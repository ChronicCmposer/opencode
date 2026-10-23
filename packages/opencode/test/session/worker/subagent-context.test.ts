// Regression (Fix 1 follow-up) — a subagent child dispatched through the
// hub-proxy `dispatchSubagent` path must inherit the PARENT's per-session
// instance/context.
//
// The bug: `hub.ensureRunning`/`startShell` capture the dispatching session's
// `InstanceRef`/runtime context via `onDispatch`, but a subagent child is
// dispatched by the raw scheduler (`subagent.dispatchSubagent` →
// `scheduler.dispatch`), which never calls `onDispatch`. So the child's
// `resolveInit` fell through to `process.cwd()`/`Project.ID.global` (H1) and the
// child worker's per-worker hub-proxy handlers ran with no `InstanceRef` and
// died (H2).
//
// This suite exercises the UNCOVERED path: it drives `dispatchSubagent` over a
// hub-proxy control port (not `hub.ensureRunning`) and asserts the child
// inherits the parent's directory and a context in which a hub-side
// `SessionStatus.set` succeeds.

import { describe, expect, test } from "bun:test"
import { Context, Effect, Layer, Option } from "effect"
import type { SessionID } from "@/session/schema"
import { InstanceRef } from "@/effect/instance-ref"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Session } from "@/session/session"
import { SessionStatus } from "@/session/status"
import { Permission } from "@/permission"
import { Question } from "@/question"
import { Config } from "@/config/config"
import { Auth } from "@/auth"
import { ModelsDev } from "@opencode-ai/core/models-dev"
import { Provider } from "@/provider/provider"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import type { ConfigV1 } from "@opencode-ai/core/v1/config/config"
import { EventV2Bridge } from "@/event-v2-bridge"
import {
  createExecutorWorkerPool,
  type ExecutorRun,
  type ExecutorWorker,
  type SessionExecutionResult,
} from "@/session/worker/execute"
import {
  createLazyExecutorWorkerPool,
  createSessionHub,
  productionSessionHubLayer,
  SessionHubService,
  type ProductionPoolFactory,
} from "@/session/worker/hub"
import type { SessionWorkerInit } from "@/session/worker/session-worker"
import { createHubProxyClient } from "@/session/worker/layer"
import { capture, flush, sid } from "../../lib/async"

// ---------------------------------------------------------------------------
// (a) unit: `createSessionHub` invokes the propagation hook with (child, parent).
// ---------------------------------------------------------------------------

/** An in-memory executor worker that completes instantly and keeps its result. */
class BridgingWorker implements ExecutorWorker {
  alive = true
  readonly runs: ExecutorRun[] = []
  private readonly results = new Map<SessionID, SessionExecutionResult>()

  constructor(readonly id: number) {}

  async execute(run: ExecutorRun): Promise<void> {
    this.runs.push(run)
    this.results.set(run.sessionID, { sessionID: run.sessionID, value: null })
  }
  async cancel(): Promise<void> {}
  async assertNotBusy(): Promise<boolean> {
    return false
  }
  resultFor(sessionID: SessionID): SessionExecutionResult | undefined {
    return this.results.get(sessionID)
  }
  async terminate(): Promise<void> {
    this.alive = false
  }
}

describe("subagent dispatch propagates the parent's per-session context (unit)", () => {
  test("the hub calls onSubagentDispatch with (childID, parentID) over the dispatchSubagent RPC", async () => {
    const pool = createExecutorWorkerPool({ k: 1, spawn: (id) => new BridgingWorker(id) })
    const propagated: Array<{ childID: SessionID; parentID: SessionID }> = []
    const hub = createSessionHub({
      pool,
      subagentDepth: 2,
      onSubagentDispatch: (childID, parentID) => propagated.push({ childID, parentID }),
      ports: { writer: {} as never, broker: {} as never, hubProxy: {} },
    })
    const channel = new MessageChannel()
    const dispose = hub.attachHubProxyPort(1, channel.port1)
    const client = createHubProxyClient(channel.port2)

    await client.dispatchSubagent!({
      parentID: sid("ses_parent"),
      childID: sid("ses_child"),
      type: "task",
      depth: 1,
    })
    await flush()

    expect(propagated).toEqual([{ childID: sid("ses_child"), parentID: sid("ses_parent") }])

    dispose()
    client.dispose()
    await hub.terminateAll()
  })
})

// ---------------------------------------------------------------------------
// (b) integration: through `productionSessionHubLayer`, the child inherits the
// parent's ambient directory (H1) and a working hub-proxy context (H2).
// ---------------------------------------------------------------------------

describe("subagent dispatch inherits the parent's instance/context (production layer)", () => {
  const ambient = (directory: string) => ({ directory, worktree: directory, project: {} as never })

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

  // `SessionStatus.set` reads the ambient `InstanceRef` and dies without it, so a
  // successful child-port call proves the child's hub-proxy handlers run under a
  // real instance context (H2).
  const deps = Layer.mergeAll(
    Layer.mock(Config.Service, {
      get: () => Effect.succeed({ model: "config/default" } as ConfigV1.Info),
      directories: () => Effect.succeed(["/cfg/opencode"]),
    }),
    Layer.mock(Auth.Service, {
      all: () => Effect.succeed({}),
      get: () => Effect.succeed(new Auth.Api({ type: "api", key: "sk-test" })),
    }),
    Layer.mock(ModelsDev.Service, { get: () => Effect.succeed({}) }),
    Layer.mock(Provider.Service, {
      defaultModel: () =>
        Effect.succeed({ providerID: ProviderV2.ID.make("default"), modelID: ModelV2.ID.make("default") }),
      getProvider: () => Effect.succeed(providerInfo),
      getModel: () => Effect.succeed(providerModel),
    }),
    RuntimeFlags.layer({ experimentalSessionWorkers: true }),
    Layer.mock(SessionStatus.Service, {
      get: () => Effect.succeed({ type: "idle" } as SessionStatus.Info),
      list: () => Effect.succeed(new Map()),
      set: () =>
        Effect.gen(function* () {
          const ctx = yield* InstanceRef
          if (!ctx) return yield* Effect.die(new Error("InstanceRef not provided to the hub-proxy handler"))
        }),
    }),
    Layer.mock(Permission.Service, {}),
    Layer.mock(Question.Service, {}),
    Layer.mock(EventV2Bridge.Service, { publishLive: () => Effect.void }),
  )

  /** A `Session` stub whose `get` records every session it was asked about. */
  const sessionLayer = (directoryBySession: Map<SessionID, string>, queried: SessionID[]) =>
    Layer.succeed(
      Session.Service,
      Session.Service.of({
        get: (sessionID: SessionID) => {
          queried.push(sessionID)
          return Effect.succeed({
            id: sessionID,
            directory: directoryBySession.get(sessionID) ?? "/persisted/unknown",
            projectID: "proj-test",
            model: { id: "m", providerID: "p" },
          } as unknown as Session.Info)
        },
        findMessage: () => Effect.succeed(Option.none()),
      } as unknown as Session.Interface),
    )

  /**
   * A pool whose single worker records the init resolved for the session it is
   * asked to run AND transfers a hub-proxy control port, so the test can drive
   * the real `dispatchSubagent`/`sessionStatus.set` handlers per worker.
   */
  const recordingPortPool =
    (
      captured: Array<{ sessionID: SessionID; init: SessionWorkerInit }>,
      hubProxyPorts: Map<number, MessagePort>,
    ): ProductionPoolFactory =>
    (resolveInit, sinks) => {
      const results = new Map<SessionID, SessionExecutionResult>()
      return createLazyExecutorWorkerPool({
        k: 1,
        create: async (id) => {
          const channel = new MessageChannel()
          sinks.onHubProxyPort(id, channel.port2)
          hubProxyPorts.set(id, channel.port1)
          return {
            id,
            alive: true,
            execute: async (run) => {
              captured.push({ sessionID: run.sessionID, init: await resolveInit(run.sessionID) })
              results.set(run.sessionID, { sessionID: run.sessionID, value: null })
            },
            cancel: async () => {},
            assertNotBusy: async () => false,
            resultFor: (sessionID) => results.get(sessionID),
            terminate: async () => {},
          } satisfies ExecutorWorker
        },
      })
    }

  test("the child resolves under the parent's directory and its hub-proxy context is live", async () => {
    const captured: Array<{ sessionID: SessionID; init: SessionWorkerInit }> = []
    const hubProxyPorts = new Map<number, MessagePort>()
    const queried: SessionID[] = []
    const layer = productionSessionHubLayer({
      count: 1,
      poolFactory: recordingPortPool(captured, hubProxyPorts),
    }).pipe(Layer.provide(deps))

    const parentID = sid("ses_parent_ctx")
    const childID = sid("ses_child_ctx")
    const sessions = sessionLayer(new Map([[parentID, "/persisted/parent"]]), queried)

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(layer)
          const hub = Context.get(context, SessionHubService)

          // The parent dispatch is what populates the per-session instance/context.
          yield* Effect.promise(() => hub.ensureRunning(parentID)).pipe(
            Effect.provide(sessions),
            Effect.provideService(InstanceRef, ambient("/ambient/parent")),
          )

          const parentPort = hubProxyPorts.get(1)
          expect(parentPort).toBeDefined()
          const client = createHubProxyClient(parentPort!)

          // Drive the CHILD through the hub-proxy path — NOT `ensureRunning`.
          const info = yield* Effect.promise(() =>
            client.dispatchSubagent!({ parentID, childID, type: "task", depth: 1 }),
          )
          expect(info.status).toBe("completed")

          // H2: the child worker's own control port serves `sessionStatus.set`
          // under a live instance context.
          const childPort = hubProxyPorts.get(2)
          expect(childPort).toBeDefined()
          const childClient = createHubProxyClient(childPort!)
          const status = yield* Effect.promise(() =>
            capture(childClient.call("sessionStatus.set", [childID, { type: "busy" }])),
          )
          expect(status.ok).toBe(true)

          childClient.dispose()
          client.dispose()
        }),
      ),
    )

    // H1: the child booted under the parent's directory, never `process.cwd()`.
    const childInit = captured.find((entry) => entry.sessionID === childID)?.init
    expect(childInit?.snapshot.directory).toBe("/ambient/parent")
    // The persisted-directory fallback was reachable for the child: the hub
    // queried the child session through the inherited context.
    expect(queried).toContain(childID)
  })
})
