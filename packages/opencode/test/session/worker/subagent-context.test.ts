// Per-session context after a child's own entry is released/reaped.
//
// The old ancestor-chain fallback (and its `onSubagentDispatch` copy) was dead
// once the job registry was removed: every dispatch records the session's own
// ambient instance/runtime context synchronously before the pool materializes
// its worker (`onDispatch` -> `recordDispatch`). `resolveInit` then reads the
// session's PERSISTED directory/project from that context. This suite proves a
// delegate-shaped child (`parentID` + raw `session.prompt`) and an
// auto-continuation child (no ambient `InstanceRef`) still boot under the
// parent's directory after their per-session entry was released by a reap.

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
import { type ExecutorWorker, type SessionExecutionResult } from "@/session/worker/execute"
import {
  createLazyExecutorWorkerPool,
  productionSessionHubLayer,
  SessionHubService,
  type ProductionPoolFactory,
} from "@/session/worker/hub"
import type { SessionWorkerInit } from "@/session/worker/session-worker"
import { sid } from "../../lib/async"

const PARENT_ID = sid("ses_parent")
const CHILD_ID = sid("ses_child")
const PARENT_DIR = "/work/parent"

const ambient = (directory: string) => ({
  directory,
  worktree: directory,
  project: { id: "proj-parent" } as never,
})

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

const deps = () =>
  Layer.mergeAll(
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
      set: () => Effect.void,
    }),
    Layer.mock(Permission.Service, {}),
    Layer.mock(Question.Service, {}),
    Layer.mock(EventV2Bridge.Service, { publishLive: () => Effect.void }),
  )

// Both sessions persist the SAME directory (a subagent shares the parent's
// working directory). The child carries `parentID`, which is what a
// delegate-shaped caller relies on.
const sessionLayer = Layer.succeed(
  Session.Service,
  Session.Service.of({
    get: (sessionID: SessionID) =>
      Effect.succeed({
        id: sessionID,
        parentID: sessionID === CHILD_ID ? PARENT_ID : undefined,
        projectID: "proj-parent",
        directory: PARENT_DIR,
        model: { id: "m", providerID: "p" },
      } as unknown as Session.Info),
    findMessage: () => Effect.succeed(Option.none()),
  } as unknown as Session.Interface),
)

/** A pool whose workers record the init resolved for the session they run. */
const recordingPool =
  (captured: Array<{ sessionID: SessionID; init: SessionWorkerInit }>): ProductionPoolFactory =>
  (resolveInit) => {
    const results = new Map<SessionID, SessionExecutionResult>()
    return createLazyExecutorWorkerPool({
      create: async (id) => ({
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
      }) satisfies ExecutorWorker,
    })
  }

/** Build the production hub once per test with the recording pool. */
const buildHub = (captured: Array<{ sessionID: SessionID; init: SessionWorkerInit }>) =>
  productionSessionHubLayer({ count: 1, poolFactory: recordingPool(captured) }).pipe(Layer.provide(deps()))

const childInits = (captured: Array<{ sessionID: SessionID; init: SessionWorkerInit }>) =>
  captured.filter((entry) => entry.sessionID === CHILD_ID).map((entry) => entry.init)

describe("per-session context survives a released child entry", () => {
  test("a delegate-shaped child re-boots under the parent's directory after a reap", async () => {
    const captured: Array<{ sessionID: SessionID; init: SessionWorkerInit }> = []

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(buildHub(captured))
          const hub = Context.get(context, SessionHubService)

          // The parent records its ambient instance.
          yield* Effect.promise(() => hub.ensureRunning(PARENT_ID)).pipe(
            Effect.provide(sessionLayer),
            Effect.provideService(InstanceRef, ambient(PARENT_DIR)),
          )
          // Raw `session.prompt` on the child: the child records the ambient
          // instance/context, and `resolveInit` reads its persisted directory.
          yield* Effect.promise(() => hub.ensureRunning(CHILD_ID)).pipe(
            Effect.provide(sessionLayer),
            Effect.provideService(InstanceRef, ambient(PARENT_DIR)),
          )
          // Reap the child's worker: the hub releases its per-session entry.
          yield* Effect.promise(() => hub.cancel(CHILD_ID))
          // Auto-continuation: no ambient `InstanceRef`, only the app's Session
          // context. The persisted directory (not the hub cwd) must win.
          yield* Effect.promise(() => hub.ensureRunning(CHILD_ID)).pipe(Effect.provide(sessionLayer))
        }),
      ),
    )

    const inits = childInits(captured)
    expect(inits).toHaveLength(2)
    expect(inits[0]?.snapshot.directory).toBe(PARENT_DIR)
    expect(inits[0]?.snapshot.projectId).toBe("proj-parent")
    // The boot after the reaped entry has no ambient instance to inherit.
    expect(inits[1]?.snapshot.directory).toBe(PARENT_DIR)
    expect(inits[1]?.snapshot.projectId).toBe("proj-parent")
    expect(inits[1]?.snapshot.directory).not.toBe(process.cwd())
  })

  test("an auto-continuation child with no ambient instance boots under its persisted directory", async () => {
    const captured: Array<{ sessionID: SessionID; init: SessionWorkerInit }> = []

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(buildHub(captured))
          const hub = Context.get(context, SessionHubService)

          // The very first child dispatch carries no ambient InstanceRef: this is
          // the post-reap auto-continuation shape. Its persisted row supplies the
          // directory, so it never falls back to the hub cwd.
          yield* Effect.promise(() => hub.ensureRunning(CHILD_ID)).pipe(Effect.provide(sessionLayer))
        }),
      ),
    )

    const inits = childInits(captured)
    expect(inits).toHaveLength(1)
    expect(inits[0]?.snapshot.directory).toBe(PARENT_DIR)
    expect(inits[0]?.snapshot.projectId).toBe("proj-parent")
    expect(inits[0]?.snapshot.directory).not.toBe(process.cwd())
  })
})
