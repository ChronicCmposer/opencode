// Regression (review follow-up) — the per-session instance/context fallback for a
// session that has NO own entry must resolve through its ANCESTOR CHAIN in the
// hub job registry, never through a last-writer-wins latch.
//
// The prior safety net latched the most recent `onDispatch` instance/context in
// `lastInstance`/`lastContext`. Under concurrent multi-project dispatch that is
// last-writer-wins: a child admitted without its own entry could inherit a
// DIFFERENT project's instance. This suite drives the production layer so that:
//
//   1. an ancestor session captures `/ambient/ancestor`,
//   2. a decoy session from ANOTHER project captures `/ambient/decoy` LAST (so a
//      last-writer-wins latch would hold the decoy),
//   3. a child session is registered in the job registry as the ancestor's child
//      but has NO own `instanceBySession` entry and NO persisted directory,
//   4. the child is dispatched with no ambient `InstanceRef`.
//
// The only correct resolution is the ancestor's directory. The primary
// `onSubagentDispatch` copy is deliberately NOT exercised (no `dispatchSubagent`
// RPC), so this proves the ancestor fallback in isolation.

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
import { createWorkerEgressClient } from "@/session/worker/egress-client"
import type { SessionWorkerInit } from "@/session/worker/session-worker"
import { sid } from "../../lib/async"
import { pollUntil } from "../../lib/poll"

/** The subset of a published event payload this suite inspects (its stamped location). */
interface PublishedEvent {
  readonly location?: { readonly directory?: string }
}

// A real `project.id`/`worktree` are required by `instanceLocation`, so the
// event-location test can assert the stamped directory.
const ambient = (directory: string) => ({
  directory,
  worktree: directory,
  project: { id: "proj-ambient" } as never,
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

const deps = (published: PublishedEvent[] = []) =>
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
    // Capture every live-only republish so a test can inspect the location the
    // hub stamped on a worker's event.
    Layer.mock(EventV2Bridge.Service, {
      publishLive: (payload) => {
        published.push(payload)
        return Effect.void
      },
    }),
  )

// A `Session` stub that persists NO directory for any session, so
// `instanceFromSession` can never mask the ancestor lookup: the child's init
// directory must come from the parent chain or not at all.
const sessionLayer = Layer.succeed(
  Session.Service,
  Session.Service.of({
    get: (sessionID: SessionID) =>
      Effect.succeed({
        id: sessionID,
        projectID: "proj-test",
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
      k: 1,
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

describe("subagent context falls back to the ancestor chain, never a last-dispatch latch", () => {
  test("a session with no own entry resolves under its registered ancestor, not another project", async () => {
    const captured: Array<{ sessionID: SessionID; init: SessionWorkerInit }> = []
    const layer = productionSessionHubLayer({ count: 1, poolFactory: recordingPool(captured) }).pipe(
      Layer.provide(deps()),
    )

    const ancestorID = sid("ses_ancestor")
    const decoyID = sid("ses_decoy")
    const childID = sid("ses_child_fallback")

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(layer)
          const hub = Context.get(context, SessionHubService)

          // The ancestor captures its own instance under its session id.
          yield* Effect.promise(() => hub.ensureRunning(ancestorID)).pipe(
            Effect.provide(sessionLayer),
            Effect.provideService(InstanceRef, ambient("/ambient/ancestor")),
          )
          // A decoy from ANOTHER project dispatches LAST. A last-writer-wins latch
          // would now hold the decoy's instance.
          yield* Effect.promise(() => hub.ensureRunning(decoyID)).pipe(
            Effect.provide(sessionLayer),
            Effect.provideService(InstanceRef, ambient("/ambient/decoy")),
          )
          // The child is a registered subagent of the ancestor — but NO instance/
          // context was copied onto it (no `dispatchSubagent` RPC, so the primary
          // `onSubagentDispatch` copy never ran).
          hub.jobs.start({ id: childID, type: "task", parentID: ancestorID, depth: 1 })
          // Drive the child with NO ambient `InstanceRef`; its only route to a
          // project is the ancestor chain in the job registry.
          yield* Effect.promise(() => hub.ensureRunning(childID)).pipe(Effect.provide(sessionLayer))
        }),
      ),
    )

    const childInit = captured.find((entry) => entry.sessionID === childID)?.init
    expect(childInit?.snapshot.directory).toBe("/ambient/ancestor")
  })

  test("a session with no own entry stamps its worker's events with the ancestor's location", async () => {
    const published: PublishedEvent[] = []
    const captured: Array<{ sessionID: SessionID; init: SessionWorkerInit }> = []
    const ancestorID = sid("ses_ancestor_evt")
    const decoyID = sid("ses_decoy_evt")
    const childID = sid("ses_child_evt")
    let egressPort: MessagePort | undefined

    // Transfer the worker's egress port WHILE it is bound to the child, so the hub
    // resolves the per-worker publisher from the child's session. The child has no
    // own `instanceBySession` entry, so the ONLY location the publisher can stamp
    // is the ancestor's — reached through the job registry, never a last-dispatch
    // latch (which would hold the decoy from another project).
    const poolFactory: ProductionPoolFactory = (resolveInit, sinks) => {
      const results = new Map<SessionID, SessionExecutionResult>()
      return createLazyExecutorWorkerPool({
        k: 1,
        create: async (id) => ({
          id,
          alive: true,
          execute: async (run) => {
            captured.push({ sessionID: run.sessionID, init: await resolveInit(run.sessionID) })
            if (run.sessionID === childID) {
              const channel = new MessageChannel()
              sinks.onWorkerPorts(id, { egress: channel.port2 })
              egressPort = channel.port1
            }
            results.set(run.sessionID, { sessionID: run.sessionID, value: null })
          },
          cancel: async () => {},
          assertNotBusy: async () => false,
          resultFor: (sessionID) => results.get(sessionID),
          terminate: async () => {},
        }) satisfies ExecutorWorker,
      })
    }

    const layer = productionSessionHubLayer({
      count: 1,
      poolFactory,
      // Injected writer/broker keep the DEFAULT egress backend (which resolves the
      // per-worker publisher via `eventsForWorker`) while spawning no isolates.
      backends: { writer: {} as never, broker: {} as never },
    }).pipe(Layer.provide(deps(published)))

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(layer)
          const hub = Context.get(context, SessionHubService)

          // The ancestor captures `/ambient/ancestor` under its session id.
          yield* Effect.promise(() => hub.ensureRunning(ancestorID)).pipe(
            Effect.provide(sessionLayer),
            Effect.provideService(InstanceRef, ambient("/ambient/ancestor")),
          )
          // A decoy from ANOTHER project dispatches LAST: a last-writer-wins latch
          // would stamp the child's events with this location instead.
          yield* Effect.promise(() => hub.ensureRunning(decoyID)).pipe(
            Effect.provide(sessionLayer),
            Effect.provideService(InstanceRef, ambient("/ambient/decoy")),
          )
          hub.jobs.start({ id: childID, type: "task", parentID: ancestorID, depth: 1 })
          yield* Effect.promise(() => hub.ensureRunning(childID)).pipe(Effect.provide(sessionLayer))

          // Emit INSIDE the hub's scope: closing it disposes the port servers, so
          // an emit afterwards would reach no receiver.
          expect(egressPort).toBeDefined()
          const egress = createWorkerEgressClient(egressPort!, { windowMs: 0 })
          egress.emit({ id: "evt_fallback", type: "session.text", properties: { text: "hi" } })
          egress.end()
          yield* Effect.promise(() =>
            pollUntil(() => published.some((event) => event.location !== undefined), {
              message: "worker event was never republished with a location",
            }),
          )
          egress.dispose()
        }),
      ),
    )

    const stamped = published.find((event) => event.location !== undefined)
    expect(stamped?.location?.directory).toBe("/ambient/ancestor")
  })
})
