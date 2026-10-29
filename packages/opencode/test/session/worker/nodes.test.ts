// TDD steps 34–35 — the flag-gated graph-node helper.
//
// `sessionWorkerNodes` is the single seam the production app graphs use to decide
// whether the session-hub node is a member of `LayerNode.group([...])`. It is pure:
// it never spawns a worker, never reads the environment unless the caller omits an
// explicit gate, and returns a fresh array each call. The default gate delegates to
// the canonical `RuntimeFlags.experimentalSessionWorkersFromEnv` (runtime-flags.ts),
// so the graph decision and the runtime flag cannot drift.

import { describe, expect, test } from "bun:test"
import { ConfigProvider, Context, Effect, Layer } from "effect"
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
import { processRole } from "@/effect/process-role"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Provider } from "@/provider/provider"
import { collectNodeNames } from "@/session/worker/node-table"
import {
  SessionHubService,
  productionSessionHubNode,
  type ProductionSessionHubNodeOptions,
  type ProductionPoolFactory,
} from "@/session/worker/hub"
import { createLazyExecutorWorkerPool } from "../../lib/lazy-worker-pool"
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

describe("sessionWorkersEnabled — canonical experimentalSessionWorkersFromEnv precedence", () => {
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

  test("invalid dedicated value falls back to the umbrella", () => {
    expect(
      sessionWorkersEnabled({
        OPENCODE_EXPERIMENTAL: "true",
        OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "nonsense",
      }),
    ).toBe(true)
  })

  test("invalid umbrella value is treated as absent", () => {
    expect(sessionWorkersEnabled({ OPENCODE_EXPERIMENTAL: "nonsense" })).toBe(false)
  })

  test("undefined values are treated as absent", () => {
    expect(
      sessionWorkersEnabled({
        OPENCODE_EXPERIMENTAL: undefined,
        OPENCODE_EXPERIMENTAL_SESSION_WORKERS: undefined,
      }),
    ).toBe(false)
  })

  test("accepts every boolean true synonym on the dedicated flag", () => {
    for (const value of ["true", "yes", "on", "1", "y"]) {
      expect(sessionWorkersEnabled({ OPENCODE_EXPERIMENTAL_SESSION_WORKERS: value })).toBe(true)
    }
  })

  test("accepts every boolean false synonym on the dedicated flag", () => {
    for (const value of ["false", "no", "off", "0", "n"]) {
      expect(sessionWorkersEnabled({ OPENCODE_EXPERIMENTAL_SESSION_WORKERS: value })).toBe(false)
    }
  })

  test("accepts every boolean synonym on the umbrella flag", () => {
    for (const value of ["true", "yes", "on", "1", "y"]) {
      expect(sessionWorkersEnabled({ OPENCODE_EXPERIMENTAL: value })).toBe(true)
    }
    for (const value of ["false", "no", "off", "0", "n"]) {
      expect(sessionWorkersEnabled({ OPENCODE_EXPERIMENTAL: value })).toBe(false)
    }
  })

  test("boolean parsing is case-sensitive like Effect Config.boolean", () => {
    expect(sessionWorkersEnabled({ OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "TRUE" })).toBe(false)
    expect(sessionWorkersEnabled({ OPENCODE_EXPERIMENTAL: "TRUE" })).toBe(false)
  })
})

describe("sessionWorkersEnabled agrees with runtime-flags for the same env", () => {
  const envs: Record<string, string | undefined>[] = [
    {},
    { OPENCODE_EXPERIMENTAL: "true" },
    { OPENCODE_EXPERIMENTAL: "false" },
    { OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "true" },
    { OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "false" },
    { OPENCODE_EXPERIMENTAL: "true", OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "false" },
    { OPENCODE_EXPERIMENTAL: "false", OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "true" },
    { OPENCODE_EXPERIMENTAL: "yes", OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "on" },
    { OPENCODE_EXPERIMENTAL: "off", OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "n" },
    { OPENCODE_EXPERIMENTAL: "nonsense", OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "1" },
    { OPENCODE_EXPERIMENTAL: "nonsense", OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "nonsense" },
    { OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "TRUE" },
    { OPENCODE_EXPERIMENTAL: undefined, OPENCODE_EXPERIMENTAL_SESSION_WORKERS: undefined },
  ]

  test("nodes.ts and runtime-flags.ts return the same boolean for every env", () => {
    for (const env of envs) {
      expect(sessionWorkersEnabled(env)).toBe(RuntimeFlags.experimentalSessionWorkersFromEnv(env))
    }
  })

  test("the graph predicate IS the single resolver exported by runtime-flags", () => {
    // One function, not two implementations that happen to agree.
    expect(RuntimeFlags.resolveSessionWorkersFlag).toBe(RuntimeFlags.experimentalSessionWorkersFromEnv)
  })
})

// The graph decision reads `process.env` at module-eval time, while the runtime
// delegation gate resolves `RuntimeFlags.experimentalSessionWorkers` through the
// ambient ConfigProvider. They must still agree, so both parse through the same
// canonical predicate. This resolves the real service from a provider built out
// of the same env (`fromUnknown`, i.e. not `process.env`) and compares.
describe("sessionWorkersEnabled agrees with the ConfigProvider-resolved runtime flag", () => {
  /** `"ConfigError"` marks the pre-fix failure mode where the Config path threw. */
  const resolveRuntimeFlag = (env: Record<string, unknown>): boolean | "ConfigError" => {
    const layer = AppNodeBuilder.build(RuntimeFlags.node).pipe(
      Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown(env))),
    )
    try {
      return Effect.runSync(
        RuntimeFlags.Service.useSync((flags) => flags.experimentalSessionWorkers).pipe(Effect.provide(layer)),
      )
    } catch {
      return "ConfigError"
    }
  }

  const driftEnvs: Record<string, unknown>[] = [
    {},
    { OPENCODE_EXPERIMENTAL: "true" },
    { OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "true" },
    { OPENCODE_EXPERIMENTAL: "false", OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "true" },
    { OPENCODE_EXPERIMENTAL: "true", OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "false" },
    // The drift cases — the provider (not `process.env`) supplies a dedicated
    // value the pure predicate recovers from but the `Config.boolean` path used
    // to reject with a fatal `ConfigError`.
    { OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "nonsense" },
    { OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "TRUE" },
    { OPENCODE_EXPERIMENTAL: "true", OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "nonsense" },
    { OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "yes" },
  ]

  test("both resolvers return the same boolean for every env", () => {
    for (const env of driftEnvs) {
      expect(resolveRuntimeFlag(env)).toBe(sessionWorkersEnabled(env as Record<string, string | undefined>))
    }
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

  // The hub must never enter a worker-hosted graph. A session worker's reduced
  // layer graph excludes it, but the module graph dynamically pulls in the app
  // runtime; when plugins are configured that builds a second Server whose graph
  // would spread this helper. Gating on the process role keeps the hub out of any
  // worker-hosted graph (role `session`) while the process main graph (`hub`)
  // stays unchanged. This is the marker for the deleted worker-hosted-hub fixture:
  // a worker graph contributes no hub, so no nested hub can clobber its thread name.
  test("returns an empty list for a non-hub process role even when enabled", () => {
    expect(sessionWorkerNodes({ hubNode, enabled: true, role: "session" })).toEqual([])
    // An omitted role delegates to the ambient role, which is not necessarily
    // "unset": the test process imports the hub bootstrap, so it is `hub`.
    expect(sessionWorkerNodes({ hubNode, enabled: true, role: undefined })).toEqual(
      sessionWorkerNodes({ hubNode, enabled: true, role: processRole() }),
    )
  })

  test("returns the hub node for the hub role when enabled", () => {
    expect(sessionWorkerNodes({ hubNode, enabled: true, role: "hub" })).toEqual([hubNode])
  })

  test("a disabled gate stays empty regardless of role", () => {
    expect(sessionWorkerNodes({ hubNode, enabled: false, role: "session" })).toEqual([])
    expect(sessionWorkerNodes({ hubNode, enabled: false, role: "hub" })).toEqual([])
  })

  test("defaults to the process role, so an undeclared role keeps the gate closed", () => {
    expect(sessionWorkerNodes({ hubNode, enabled: true })).toEqual(processRole() === "hub" ? [hubNode] : [])
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
  [
    Config.node,
    Layer.mock(Config.Service, {
      get: () => Effect.succeed({ model: "test/model" } as ConfigV1.Info),
      directories: () => Effect.succeed(["/cfg/opencode"]),
    }),
  ],
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
      "@opencode/RunBarrier",
    ])
  })
})

describe("34a lazy production activation", () => {
  test("building the node provides SessionHubService, spawns 0 workers until first dispatch", async () => {
    const created: number[] = []
    const poolFactory: ProductionPoolFactory = (resolveInit, _sinks) =>
      createLazyExecutorWorkerPool({
        // Per-session dispatch materialises the worker on first use.
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
                count: 0,
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
            // The first dispatch resolves the init and materialises the session's
            // one worker (id 1; no warm slot consumed it).
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
        create: async (id) => {
          initCapabilities = (await resolveInit()).capabilities
          return createExecutorWorker({ id, transport: instantTransport() })
        },
      })
    // No broker or writer authority is served; the capability is still advertised.
    const backends = {}

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
