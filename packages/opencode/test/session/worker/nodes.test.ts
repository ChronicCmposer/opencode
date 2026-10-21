// TDD steps 34–35 — the flag-gated graph-node helper.
//
// `sessionWorkerNodes` is the single seam the production app graphs use to decide
// whether the session-hub node is a member of `LayerNode.group([...])`. It is pure:
// it never spawns a worker, never reads the environment unless the caller omits an
// explicit gate, and returns a fresh array each call. The default gate mirrors
// `RuntimeFlags`'s `enabledByExperimental` precedence (runtime-flags.ts:12-16), so the
// graph decision and the runtime flag cannot drift.

import { describe, expect, test } from "bun:test"
import { Context, Effect, Layer } from "effect"
import type { ConfigV1 } from "@opencode-ai/core/v1/config/config"
import { ModelsDev } from "@opencode-ai/core/models-dev"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Auth } from "@/auth"
import { Config } from "@/config/config"
import { Permission } from "@/permission"
import { Question } from "@/question"
import { SessionStatus } from "@/session/status"
import { InstanceRef } from "@/effect/instance-ref"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Provider } from "@/provider/provider"
import { collectNodeNames } from "@/session/worker/node-table"
import {
  SessionHubService,
  createLazyExecutorWorkerPool,
  productionSessionHubNode,
  type ProductionSessionHubNodeOptions,
  type ProductionPoolFactory,
} from "@/session/worker/hub"
import {
  createExecutorWorker,
  type ExecutorTransport,
  type SessionExecutorInbound,
  type SessionExecutorOutbound,
} from "@/session/worker/execute"
import { sid } from "../../lib/async"
import {
  SessionWorkerNodes,
  sessionWorkerNodes,
  sessionWorkersEnabled,
  type SessionWorkerNodesOptions,
} from "@/session/worker/nodes"

const hubNode = LayerNode.make({
  name: "@opencode/SessionHub",
  layer: Layer.empty as Layer.Layer<never>,
  deps: [] as const,
})

describe("sessionWorkersEnabled — enabledByExperimental precedence", () => {
  test("unset dedicated and umbrella is off", () => {
    expect(sessionWorkersEnabled({})).toBe(false)
  })

  test("umbrella alone enables the dedicated flag", () => {
    expect(sessionWorkersEnabled({ OPENCODE_EXPERIMENTAL: "true" })).toBe(true)
  })

  test("dedicated flag alone enables it", () => {
    expect(sessionWorkersEnabled({ OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "true" })).toBe(true)
  })

  test("dedicated false overrides a true umbrella", () => {
    expect(
      sessionWorkersEnabled({
        OPENCODE_EXPERIMENTAL: "true",
        OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "false",
      }),
    ).toBe(false)
  })

  test("dedicated true overrides a false umbrella", () => {
    expect(
      sessionWorkersEnabled({
        OPENCODE_EXPERIMENTAL: "false",
        OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "true",
      }),
    ).toBe(true)
  })

  test("only the exact string 'true' counts", () => {
    expect(sessionWorkersEnabled({ OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "1" })).toBe(false)
    expect(sessionWorkersEnabled({ OPENCODE_EXPERIMENTAL: "yes" })).toBe(false)
  })
})

describe("sessionWorkerNodes", () => {
  test("returns an empty list when the explicit gate is off", () => {
    expect(sessionWorkerNodes({ hubNode, enabled: false })).toEqual([])
  })

  test("returns the hub node when the explicit gate is on", () => {
    expect(sessionWorkerNodes({ hubNode, enabled: true })).toEqual([hubNode])
  })

  test("the explicit gate wins over the environment", () => {
    const env = { OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "true" }
    expect(sessionWorkerNodes({ hubNode, enabled: false, env })).toEqual([])
    expect(sessionWorkerNodes({ hubNode, enabled: true, env: {} })).toEqual([hubNode])
  })

  test("defaults to the environment precedence", () => {
    expect(sessionWorkerNodes({ hubNode, env: {} })).toEqual([])
    expect(sessionWorkerNodes({ hubNode, env: { OPENCODE_EXPERIMENTAL: "true" } })).toEqual([hubNode])
    expect(
      sessionWorkerNodes({
        hubNode,
        env: { OPENCODE_EXPERIMENTAL: "true", OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "false" },
      }),
    ).toEqual([])
  })

  test("is pure — a fresh array per call, never the same reference", () => {
    const first = sessionWorkerNodes({ hubNode, enabled: true })
    const second = sessionWorkerNodes({ hubNode, enabled: true })
    expect(first).not.toBe(second)
    expect(first).toEqual(second)
    expect(first[0]).toBe(hubNode)
  })

  test("its result spreads into a LayerNode.group without spawning anything", () => {
    const enabled = LayerNode.group([...sessionWorkerNodes({ hubNode, enabled: true })])
    expect(enabled.dependencies).toEqual([hubNode])
    const disabled = LayerNode.group([...sessionWorkerNodes({ hubNode, enabled: false })])
    expect(disabled.dependencies).toEqual([])
  })

  test("preserves the caller's node type (generic, decoupled from the hub)", () => {
    const options: SessionWorkerNodesOptions<typeof hubNode> = { hubNode, enabled: true }
    expect(sessionWorkerNodes(options)).toEqual([hubNode])
    expect(SessionWorkerNodes.sessionWorkerNodes).toBe(sessionWorkerNodes)
  })
})

// ---------------------------------------------------------------------------
// Steps 34–35 — the production node's graph membership + lazy activation.
// ---------------------------------------------------------------------------

/** The dependency-light executor fixture (speaks the executor protocol). */
const EXECUTOR_FIXTURE = new URL("../../fixture/session-execute-worker.ts", import.meta.url)

const instanceCtx = {
  directory: "/tmp/opencode-worker-activation",
  worktree: "/tmp/opencode-worker-activation",
  project: {} as never,
}

const providerInfo = {
  id: "test",
  name: "Test",
  source: "api",
  env: [],
  options: { baseURL: "https://api.example.test/v1", headers: { "x-provider": "1" } },
  models: {},
} as unknown as Provider.Info

const providerModel = {
  id: "model",
  providerID: "test",
  api: { id: "model-api", url: "https://fallback.example.test/v1", npm: "@ai-sdk/openai-compatible" },
  name: "Model",
  capabilities: {},
  cost: {},
  limit: { context: 1_000, output: 1_000 },
  status: "active",
  options: {},
  headers: { "x-model": "1" },
  release_date: "2024-01-01",
} as unknown as Provider.Model

/** Replace the node's declared service deps with dependency-free mocks. */
const productionReplacements: LayerNode.Replacements = [
  [Config.node, Layer.mock(Config.Service, { get: () => Effect.succeed({ model: "test/model" } as ConfigV1.Info) })],
  [
    Auth.node,
    Layer.mock(Auth.Service, {
      get: () => Effect.succeed(new Auth.Api({ type: "api", key: "sk-test" })),
      all: () => Effect.succeed({ test: new Auth.Api({ type: "api", key: "sk-test" }) }),
    }),
  ],
  [ModelsDev.node, Layer.mock(ModelsDev.Service, { get: () => Effect.succeed({}) })],
  [
    Provider.node,
    Layer.mock(Provider.Service, {
      defaultModel: () =>
        Effect.succeed({ providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("model") }),
      getProvider: () => Effect.succeed(providerInfo),
      getModel: () => Effect.succeed(providerModel),
    }),
  ],
  [RuntimeFlags.node, RuntimeFlags.layer({ experimentalSessionWorkers: true })],
  // Hub-proxy authorities (SessionStatus/Permission/Question) are the production
  // node's wire authorities; mocked so the node builds dependency-free.
  [SessionStatus.node, Layer.mock(SessionStatus.Service, {})],
  [Permission.node, Layer.mock(Permission.Service, {})],
  [Question.node, Layer.mock(Question.Service, {})],
]

/** An executor transport that settles every request immediately. */
function instantTransport(): ExecutorTransport {
  let alive = true
  let pending: ((message: SessionExecutorOutbound) => void) | undefined
  return {
    post(message: SessionExecutorInbound) {
      const reply: SessionExecutorOutbound =
        message.type === "run" || message.type === "startShell"
          ? { type: "done", id: message.id, result: { sessionID: message.sessionID } }
          : message.type === "ping"
            ? { type: "pong", id: message.id }
            : message.type === "cancel"
              ? { type: "cancelled", id: message.id }
              : { type: "busy", id: message.id, busy: false }
      queueMicrotask(() => pending?.(reply))
    },
    waitFor() {
      return new Promise<SessionExecutorOutbound>((resolve) => {
        pending = resolve
      })
    },
    get alive() {
      return alive
    },
    async terminate() {
      alive = false
      return 0
    },
  }
}

const buildProductionNode = (options: ProductionSessionHubNodeOptions) =>
  AppNodeBuilder.build(productionSessionHubNode(options), productionReplacements)

const provideInstance = <A, E>(effect: Effect.Effect<A, E>) =>
  effect.pipe(Effect.provideService(InstanceRef, instanceCtx))

describe("34 graph membership", () => {
  test("flag-off excludes the hub node (empty spread)", () => {
    const off = LayerNode.group([...sessionWorkerNodes({ hubNode: productionSessionHubNode(), enabled: false })])
    expect(off.dependencies).toEqual([])
    expect(collectNodeNames(off).has("@opencode/SessionHub")).toBe(false)
  })

  test("flag-on includes the hub node, which provides SessionHubService", () => {
    const on = LayerNode.group([...sessionWorkerNodes({ hubNode: productionSessionHubNode(), enabled: true })])
    expect(collectNodeNames(on).has("@opencode/SessionHub")).toBe(true)
  })

  test("the production node declares its service deps (init + hub-proxy authorities)", () => {
    const names = productionSessionHubNode().dependencies.map((dependency) => dependency.name)
    expect(names).toEqual([
      "@opencode/Config",
      "@opencode/Auth",
      "@opencode/ModelsDev",
      "@opencode/Provider",
      "@opencode/RuntimeFlags",
      "@opencode/SessionStatus",
      "@opencode/Permission",
      "@opencode/Question",
      "@opencode/EventV2Bridge",
    ])
  })
})

describe("34a lazy production activation", () => {
  test("building the node provides SessionHubService, spawns 0 workers until first dispatch", async () => {
    const created: number[] = []
    const poolFactory: ProductionPoolFactory = (resolveInit, _sinks) =>
      createLazyExecutorWorkerPool({
        k: 1,
        create: async (id) => {
          await resolveInit()
          created.push(id)
          return createExecutorWorker({ id, transport: instantTransport() })
        },
      })

    await Effect.runPromise(
      provideInstance(
        Effect.scoped(
          Effect.gen(function* () {
            const context = yield* Layer.build(
              buildProductionNode({
                count: 1,
                entry: EXECUTOR_FIXTURE,
                workKind: "instant",
                timeoutMs: 15_000,
                poolFactory,
              }),
            )
            const hub = Context.get(context, SessionHubService)
            // Building the layer (and resolving the init) spawns no isolate.
            expect(created).toEqual([])

            const result = yield* Effect.promise(() => hub.ensureRunning(sid("ses_activation")))
            expect(result.sessionID).toBe("ses_activation")
            // The first dispatch resolves the init and materialises K=1 worker.
            expect(created).toEqual([1])
          }),
        ),
      ),
    )
  })

  test("flag-on init advertises sessionWorkerPorts when port backends are supplied", async () => {
    let initCapabilities: unknown
    const poolFactory: ProductionPoolFactory = (resolveInit, _sinks) =>
      createLazyExecutorWorkerPool({
        k: 1,
        create: async (id) => {
          initCapabilities = (await resolveInit()).capabilities
          return createExecutorWorker({ id, transport: instantTransport() })
        },
      })
    const backends = {
      writer: { query: async () => [], publish: async () => ({ seq: null, committed: false }) },
      broker: {} as never,
    }

    await Effect.runPromise(
      provideInstance(
        Effect.scoped(
          Effect.gen(function* () {
            const context = yield* Layer.build(
              buildProductionNode({ count: 1, entry: EXECUTOR_FIXTURE, workKind: "instant", poolFactory, ports: backends }),
            )
            const hub = Context.get(context, SessionHubService)
            yield* Effect.promise(() => hub.ensureRunning(sid("ses_caps")))
          }),
        ),
      ),
    )

    expect(initCapabilities).toEqual({ sessionWorkerPorts: true })
  })
})
