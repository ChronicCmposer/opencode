// R2-M4 — the production hub must supply the synchronous child edge the
// dispatcher's crash/timeout cascade reads.
//
// The bug: `createProductionSessionHub` fell back to `childrenOf: () => []`
// because the production layer never supplied one. A crashed parent therefore
// left its `task` child running on its own worker forever. This test drives the
// REAL production layer (only the services and the pool are fakes) and a REAL
// `session.spawn` hub-proxy request, so it fails if the wiring is removed.

import { describe, expect, test } from "bun:test"
import { Context, Effect, Layer, Option } from "effect"
import type { ConfigV1 } from "@opencode-ai/core/v1/config/config"
import { ModelsDev } from "@opencode-ai/core/models-dev"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { Auth } from "@/auth"
import { Config } from "@/config/config"
import { Permission } from "@/permission"
import { Provider } from "@/provider/provider"
import { Question } from "@/question"
import { SessionStatus } from "@/session/status"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { EventV2Bridge } from "@/event-v2-bridge"
import { InstanceRef } from "@/effect/instance-ref"
import { MessageID } from "@/session/schema"
import { Session } from "@/session/session"
import {
  createExecutorWorkerPool,
  type ExecutorRun,
  type ExecutorWorker,
  type SessionExecutionResult,
} from "@/session/worker/execute"
import { SessionCancelledError, WorkerCrashedError } from "@/session/worker/dispatch"
import { createHubProxyClient } from "@/session/worker/hub-proxy-wire"
import { productionSessionHubLayer, SessionHubService, createSessionChildrenIndex } from "@/session/worker/hub"
import type { SessionSpawnRequest } from "@/session/worker/subagent"
import { pollUntil } from "../../lib/poll"
import { testEffect } from "../../lib/effect"
import { sid } from "../../lib/async"

const it = testEffect(Layer.empty)

const instanceCtx = { directory: "/tmp/ocx-cascade", worktree: "/tmp/ocx-cascade", project: {} as never }

const providerInfo = {
  id: "test",
  name: "Test",
  source: "api",
  env: [],
  options: {},
  models: {},
} as unknown as Provider.Info

const providerModel = {
  id: "model",
  providerID: "test",
  api: { id: "model-api", url: "https://example.test/v1", npm: "@ai-sdk/openai-compatible" },
  name: "Model",
  capabilities: {},
} as unknown as Provider.Model

const deps = Layer.mergeAll(
  Layer.mock(Config.Service, {
    get: () => Effect.succeed({} as ConfigV1.Info),
    directories: () => Effect.succeed(["/tmp/ocx-cascade"]),
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

/** An executor worker whose run stays parked until the test crashes/terminates it. */
class CascadeWorker implements ExecutorWorker {
  alive = true
  readonly results = new Map<string, SessionExecutionResult>()
  private pending:
    | { readonly sessionID: string; readonly resolve: () => void; readonly reject: (error: unknown) => void }
    | undefined

  constructor(readonly id: number) {}

  execute(run: ExecutorRun): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.pending = { sessionID: run.sessionID, resolve, reject }
    })
  }

  crash(): void {
    this.alive = false
    const pending = this.pending
    this.pending = undefined
    pending?.reject(new WorkerCrashedError({ message: `worker ${this.id} crashed`, workerID: this.id }))
  }

  async cancel(): Promise<void> {}
  async assertNotBusy(): Promise<boolean> {
    return this.pending !== undefined
  }
  resultFor(sessionID: string): SessionExecutionResult | undefined {
    return this.results.get(sessionID)
  }
  async terminate(): Promise<void> {
    this.alive = false
    const pending = this.pending
    this.pending = undefined
    pending?.reject(new SessionCancelledError({ message: "worker terminated", sessionID: pending.sessionID as never }))
  }
}

class CascadePool {
  readonly workers: CascadeWorker[] = []
  readonly spawned: CascadeWorker[] = []
  readonly pool = createExecutorWorkerPool({
    spawn: (id) => {
      const worker = new CascadeWorker(id)
      this.spawned.push(worker)
      return worker
    },
  })

  byID(id: number): CascadeWorker | undefined {
    return this.spawned.find((worker) => worker.id === id)
  }
}

const spawnRequest = (parentID: string, childID: string): SessionSpawnRequest => ({
  parentID: parentID as never,
  childID: childID as never,
  type: "task",
  title: "cascade child",
  prompt: {
    messageID: MessageID.ascending(),
    sessionID: childID as never,
    agent: "general",
    parts: [{ type: "text", text: "do the thing" }],
  },
})

const sessionIdle = (sessionID: string, parentID?: string) => ({
  id: sessionID,
  parentID,
  metadata: {},
})

describe("R2-M4 production crash cascade", () => {
  test("the child index removes a released child from its parent", () => {
    const index = createSessionChildrenIndex()
    const parent = sid("ses_index_parent")
    const child = sid("ses_index_child")
    const sibling = sid("ses_index_sibling")
    const grandchild = sid("ses_index_grandchild")

    index.register(parent, child)
    index.register(parent, sibling)
    index.register(child, grandchild)
    expect(index.childrenOf(parent)).toEqual([child, sibling])
    expect(index.childrenOf(child)).toEqual([grandchild])

    index.release(child)
    expect(index.childrenOf(parent)).toEqual([sibling])
    // The descendant edge stays until the grandchild itself is released, so a
    // re-dispatched parent can still cascade to a live grandchild.
    expect(index.childrenOf(child)).toEqual([grandchild])

    index.release(sibling)
    expect(index.childrenOf(parent)).toEqual([])
    index.release(grandchild)
    expect(index.childrenOf(child)).toEqual([])

    index.release(parent)
    index.clear()
    expect(index.childrenOf(parent)).toEqual([])
  })

  it.live("a crashed parent cancels the task child on its own worker", () =>
    Effect.gen(function* () {
      const parentID = sid("ses_cascade_parent")
      const childID = sid("ses_cascade_child")
      const fakes = new CascadePool()
      const poolFactory = () => fakes.pool
      const layer = productionSessionHubLayer({ count: 1, poolFactory }).pipe(Layer.provide(deps))

      yield* Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(layer)
          const hub = Context.get(context, SessionHubService)
          const sessions = Layer.succeed(
            Session.Service,
            Session.Service.of({
              // The spawn authority requires a supplied childID to be a direct
              // child; model the persisted edge the production DB always has.
              get: (id: never) =>
                Effect.succeed(sessionIdle(String(id), String(id) === childID ? String(parentID) : undefined) as never),
              findMessage: () => Effect.succeed(Option.none()),
              spawn: (input: { readonly childID?: string }) =>
                Effect.succeed({
                  childID: (input.childID ?? "ses_spawned") as never,
                  result: { info: {}, parts: [] } as never,
                }),
            } as unknown as Session.Interface),
          )
          const provide = <A, E>(effect: Effect.Effect<A, E>) =>
            effect.pipe(Effect.provide(sessions), Effect.provideService(InstanceRef, instanceCtx))

          // The parent owns worker 1 and stays parked: its crash is the trigger.
          yield* provide(Effect.promise(() => hub.ensureRunning(parentID))).pipe(Effect.exit, Effect.forkChild)
          yield* Effect.promise(() =>
            pollUntil(() => hub.dispatcher.registry.get(parentID)?.state === "running", {
              message: "parent never started",
            }),
          )
          const parentWorkerID = hub.dispatcher.registry.get(parentID)!.workerID!
          const parentWorker = fakes.byID(parentWorkerID)!
          expect(parentWorker).toBeDefined()

          // Run the real hub-proxy authority path the task tool uses: this is
          // what registers the parent -> child edge in production.
          const channel = new MessageChannel()
          const disposeProxy = hub.attachHubProxyPort(parentWorkerID, channel.port1)
          const client = createHubProxyClient(channel.port2)
          try {
            const spawned = (yield* Effect.promise(() =>
              client.call("session.spawn", [spawnRequest(String(parentID), String(childID))]),
            )) as { readonly childID: string }
            expect(spawned.childID).toBe(childID)

            // The child is materialised on its own worker and stays parked.
            yield* provide(Effect.promise(() => hub.ensureRunning(childID))).pipe(Effect.exit, Effect.forkChild)
            yield* Effect.promise(() =>
              pollUntil(() => hub.dispatcher.registry.get(childID)?.state === "running", {
                message: "child never started on its own worker",
              }),
            )
            const childWorkerID = hub.dispatcher.registry.get(childID)!.workerID!
            expect(childWorkerID).not.toBe(parentWorkerID)
            expect(fakes.pool.byID(childWorkerID)).toBeDefined()

            // Crash the parent's isolate. The dispatcher must read childrenOf and
            // terminate the child's worker; without the production index the
            // child stays running and this poll fails.
            parentWorker.crash()
            yield* Effect.promise(() =>
              pollUntil(() => !hub.dispatcher.registry.has(childID) && fakes.pool.byID(childWorkerID) === undefined, {
                message: "child worker was not cancelled/retired after the parent crash",
              }),
            )
            expect(fakes.pool.byID(parentWorkerID)).toBeUndefined()
          } finally {
            client.dispose()
            disposeProxy()
            channel.port1.close()
            channel.port2.close()
          }
        }),
      )
    }),
  )
})
