// A mid-session model switch retires ONE worker generation. The live session
// keeps its hub-owned context/instance/children so the successor isolate can
// still resolve permissions, questions, plugins and `SessionPrompt`.
//
// The bug this pins: `releaseGeneration` used to run the session-release hook,
// deleting `contextBySession` while the successor was already running, so a
// permission request from the next turn found no `InstanceRef` directory.

import { createSessionBoundExecutorWorker } from "./lib/lazy-worker-pool"
import { describe, expect, test } from "bun:test"
import { Context, Deferred, Effect, Fiber, Layer, Option } from "effect"
import type { ConfigV1 } from "@opencode-ai/core/v1/config/config"
import { ModelsDev } from "@opencode-ai/core/models-dev"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { Auth } from "@/auth"
import { Config } from "@/config/config"
import { Permission } from "@/permission"
import { Provider } from "@/provider/provider"
import { Question } from "@/question"
import { SessionStatus } from "@/session/status"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { EventV2Bridge } from "@/event-v2-bridge"
import { InstanceRef } from "@/effect/instance-ref"
import { Session } from "@/session/session"
import { SessionCancelledError } from "@/session/worker/dispatch"
import {
  createExecutorWorkerPool,
  type ExecutorRun,
  type ExecutorWorker,
  type SessionExecutionResult,
  type ExecutorWorkerPool,
} from "@/session/worker/execute"
import {
  createProductionSessionHub,
  productionSessionHubLayer,
  SessionHubService,
  type ProductionPoolFactory,
  type SessionBoundSpawner,
  type SessionHub,
  type WorkerPorts,
} from "@/session/worker/hub"
import { RunBarrier } from "@/session/run-barrier"
import { createTestBarrier } from "./lib/run-barrier-test"
import { createHubProxyClient } from "@/session/worker/hub-proxy-wire"
import { sessionWorkerInit } from "./lib/session-worker-init"
import { testEffect } from "./lib/effect"
import { pollUntil } from "./lib/poll"
import { sid } from "./lib/async"

const it = testEffect(Layer.empty)

const instanceCtx = {
  directory: "/tmp/ocx-model-switch-context",
  worktree: "/tmp/ocx-model-switch-context",
  project: { id: "prj_model_switch" } as never,
}

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
  cost: {},
  limit: { context: 1_000, output: 1_000 },
  status: "active",
  options: {},
  headers: {},
  release_date: "2024-01-01",
} as unknown as Provider.Model

type PermissionPending = {
  readonly info: PermissionV1.Request
  readonly deferred: Deferred.Deferred<void, PermissionV1.RejectedError | PermissionV1.CorrectedError>
}

/** A self-contained pending-permission authority that records the ambient directory. */
function permissionService(state: {
  readonly pending: Map<string, PermissionPending>
  readonly directories: string[]
  readonly cancelled: string[]
  readonly interrupted: string[]
}): Permission.Interface {
  const ask: Permission.Interface["ask"] = (input) =>
    Effect.gen(function* () {
      const ctx = yield* InstanceRef
      state.directories.push(ctx?.directory ?? "<missing>")
      const id = input.id ?? `perm_${state.pending.size + 1}`
      const deferred = yield* Deferred.make<void, PermissionV1.RejectedError | PermissionV1.CorrectedError>()
      state.pending.set(id, {
        info: {
          id,
          sessionID: input.sessionID,
          permission: input.permission,
          patterns: input.patterns,
          metadata: input.metadata,
          always: input.always,
          tool: input.tool,
        } as PermissionV1.Request,
        deferred,
      })
      // Mirror the real service: a retired generation's blocked ask is
      // interrupted and its pending entry never outlives it.
      return yield* Effect.ensuring(
        Deferred.await(deferred),
        Effect.sync(() => {
          state.pending.delete(id)
        }),
      ).pipe(
        Effect.onInterrupt(() =>
          Effect.sync(() => {
            state.interrupted.push(id)
          }),
        ),
      )
    })
  const reply: Permission.Interface["reply"] = (input) =>
    Effect.gen(function* () {
      const entry = state.pending.get(input.requestID)
      if (entry === undefined) return yield* Effect.fail(new PermissionV1.NotFoundError({ requestID: input.requestID }))
      state.pending.delete(input.requestID)
      yield* Deferred.succeed(entry.deferred, undefined)
    })
  const cancelSession: NonNullable<Permission.Interface["cancelSession"]> = (sessionID) =>
    Effect.gen(function* () {
      state.cancelled.push(String(sessionID))
      for (const [id, entry] of state.pending) {
        if (entry.info.sessionID !== sessionID) continue
        state.pending.delete(id)
        yield* Deferred.fail(entry.deferred, new PermissionV1.RejectedError())
      }
    })
  return {
    ask,
    reply,
    list: () => Effect.succeed([...state.pending.values()].map((entry) => entry.info)),
    cancelSession,
  } as Permission.Interface
}

function questionService(cancelled: string[]): Question.Interface {
  return {
    ask: () => Effect.die(new Error("question.ask is not used in this test")),
    reply: () => Effect.void,
    reject: () => Effect.void,
    list: () => Effect.succeed([]),
    cancelSession: (sessionID: string) =>
      Effect.sync(() => {
        cancelled.push(sessionID)
      }),
  } as unknown as Question.Interface
}

/** A fake isolate with controllable run parking and termination. */
class ParkedIsolate implements ExecutorWorker {
  alive = true
  private pending:
    | { readonly run: ExecutorRun; readonly resolve: () => void; readonly reject: (error: unknown) => void }
    | undefined
  constructor(
    readonly id: number,
    private readonly park: boolean,
    private readonly results: Map<string, SessionExecutionResult>,
  ) {}

  execute(run: ExecutorRun): Promise<void> {
    if (!this.park) {
      this.results.set(run.sessionID, { sessionID: run.sessionID, value: { parked: false } } as SessionExecutionResult)
      return Promise.resolve()
    }
    return new Promise<void>((resolve, reject) => {
      this.pending = { run, resolve, reject }
    })
  }
  /** Settle the currently parked run successfully (a normal turn boundary). */
  resolvePending(): void {
    const pending = this.pending
    this.pending = undefined
    if (pending === undefined) return
    this.results.set(pending.run.sessionID, {
      sessionID: pending.run.sessionID,
      value: { parked: true },
    } as SessionExecutionResult)
    pending.resolve()
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
    pending?.reject(new SessionCancelledError({ message: "worker terminated", sessionID: "ses_unknown" as never }))
  }
}

describe("model switch keeps the live session's hub state", () => {
  it.live("the next turn's permission request resolves under the session's directory", () =>
    Effect.gen(function* () {
      const parentID = sid("ses_switch_parent")
      const childID = sid("ses_switch_child")
      const parents = new Map<string, string>([[String(childID), String(parentID)]])
      let model = "model-a"
      const permission = {
        pending: new Map<string, PermissionPending>(),
        directories: [] as string[],
        cancelled: [] as string[],
        interrupted: [] as string[],
      }
      const questionCancelled: string[] = []
      let hubRef: SessionHub | undefined
      let workerPorts: WorkerPorts | undefined
      const executions = new Map<string, number>()
      const results = new Map<string, SessionExecutionResult>()
      const spawnedModels: Array<string | undefined> = []
      let firstParentIsolate: ParkedIsolate | undefined
      const sessionLayer = Layer.succeed(
        Session.Service,
        Session.Service.of({
          get: (sessionID: never) => {
            const id = String(sessionID)
            return Effect.succeed({
              id,
              parentID: parents.get(id),
              model: { providerID: model, id: model },
              metadata: {},
            } as never)
          },
          findMessage: () => Effect.succeed(Option.none()),
        } as unknown as Session.Interface),
      )
      const deps = Layer.mergeAll(
        RunBarrier.layer,
        Layer.mock(Config.Service, {
          get: () => Effect.succeed({} as ConfigV1.Info),
          directories: () => Effect.succeed([instanceCtx.directory]),
        }),
        Layer.mock(Auth.Service, {
          all: () => Effect.succeed({}),
          get: () => Effect.succeed(new Auth.Api({ type: "api", key: "sk-test" })),
        }),
        Layer.mock(ModelsDev.Service, { get: () => Effect.succeed({}) }),
        Layer.mock(Provider.Service, {
          defaultModel: () =>
            Effect.succeed({ providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("model") }),
          getProvider: () => Effect.succeed(providerInfo),
          getModel: () => Effect.succeed(providerModel),
        }),
        RuntimeFlags.layer({ experimentalSessionWorkers: true }),
        Layer.mock(SessionStatus.Service, {
          get: () => Effect.succeed({ type: "idle" } as SessionStatus.Info),
          list: () => Effect.succeed(new Map()),
          set: () => Effect.void,
        }),
        Layer.succeed(Permission.Service, permissionService(permission)),
        Layer.succeed(Question.Service, questionService(questionCancelled)),
        Layer.mock(EventV2Bridge.Service, { publishLive: () => Effect.void }),
      )
      const poolFactory: ProductionPoolFactory = (resolveInit) => {
        const spawnFor: SessionBoundSpawner = async (id, sessionID) => {
          // Resolving the init is what registers the persisted parent edge and
          // reads the session's PERSISTED model through the captured context.
          const init = await resolveInit(sessionID)
          spawnedModels.push(String(init.model.modelID))
          const count = (executions.get(sessionID) ?? 0) + 1
          executions.set(sessionID, count)
          // Every turn parks so the test controls each boundary. Turn 1 is
          // released explicitly after generation A's permission ask is pending.
          const isolate = new ParkedIsolate(id, true, results)
          if (String(sessionID) === String(parentID) && count === 1) firstParentIsolate = isolate
          return isolate
        }
        return createExecutorWorkerPool({
          spawn: (id) =>
            createSessionBoundExecutorWorker(
              id,
              spawnFor,
              undefined,
              undefined,
              undefined,
              async () => ({ generation: model }),
              (sessionID, workerID) => hubRef!.dispatcher.retireGeneration(sessionID, workerID),
            ),
        })
      }
      const layer = productionSessionHubLayer({
        count: 1,
        poolFactory,
        onServices: (services) => {
          workerPorts = services.workerPorts
        },
      }).pipe(Layer.provide(deps))

      yield* Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(layer)
          const hub = Context.get(context, SessionHubService)
          hubRef = hub
          const provide = <A, E>(effect: Effect.Effect<A, E>) =>
            effect.pipe(Effect.provide(sessionLayer), Effect.provideService(InstanceRef, instanceCtx))

          // Turn 1 on model A. It parks so generation A's hub-proxy port is
          // attached while the worker is still bound to its session; the run is
          // released after the old generation's ask is pending.
          const firstRun = yield* provide(Effect.promise(() => hub.ensureRunning(parentID))).pipe(Effect.forkChild)
          yield* Effect.promise(() =>
            pollUntil(() => hub.dispatcher.registry.get(parentID)?.state === "running", {
              message: "the first parent turn never started",
            }),
          )
          const workerID = hub.dispatcher.registry.entry(parentID)!.workerID
          expect(workerID).toBeDefined()

          // A child registered before the switch, so the cascade can prove the
          // parent's children index survived the generation retirement.
          const childFiber = yield* provide(Effect.promise(() => hub.ensureRunning(childID))).pipe(Effect.forkChild)
          yield* Effect.promise(() =>
            pollUntil(() => hub.dispatcher.registry.get(childID)?.state === "running", {
              message: "the child worker never started",
            }),
          )

          // Attach a proxy port to the old generation so its disposal is visible.
          const oldChannel = new MessageChannel()
          const disposeOld = hub.attachHubProxyPort(workerID, oldChannel.port1)
          const oldClient = createHubProxyClient(oldChannel.port2)
          expect(Effect.runSync(workerPorts!.attached()).hubProxy).toContain(workerID)

          // Generation A blocks on a permission ask before the switch. That ask
          // must be failed by A's retirement, never carried into generation B.
          const oldAsk = oldClient.call("permission.ask", [
            {
              id: "perm_old",
              sessionID: parentID,
              permission: "bash",
              patterns: ["*"],
              always: [],
            },
          ])
          oldAsk.catch(() => undefined)
          yield* Effect.promise(() =>
            pollUntil(() => permission.pending.has("perm_old"), {
              message: "generation A's ask never reached the hub authority",
            }),
          )
          // Let turn 1 reach its normal boundary. The hub-side ask stays pending
          // across the boundary; only the generation retirement may fail it.
          expect(firstParentIsolate).toBeDefined()
          firstParentIsolate!.resolvePending()
          yield* Fiber.join(firstRun)

          // Turn 2 after the user switches the model: the pool replaces the
          // isolate and routes the old generation through the dispatcher.
          model = "model-b"
          const successor = yield* provide(Effect.promise(() => hub.ensureRunning(parentID))).pipe(
            Effect.forkChild,
          )
          yield* Effect.promise(() =>
            pollUntil(() => spawnedModels.at(-1) === "model-b", {
              message: "the successor isolate never resolved model-b",
            }),
          )

          // The old generation's port server was disposed and its blocked ask
          // was failed by the generation-scoped retirement. Crucially this is
          // NOT a session-wide cancel: generation B's asks must survive.
          expect(Effect.runSync(workerPorts!.attached()).hubProxy).not.toContain(workerID)
          expect(permission.cancelled).toEqual([])
          expect(questionCancelled).toEqual([])
          yield* Effect.promise(() =>
            pollUntil(() => !permission.pending.has("perm_old"), {
              message: "generation A's blocked ask outlived its retirement",
            }),
          )
          expect(permission.interrupted).toContain("perm_old")
          permission.directories.length = 0

          // The next turn requests a permission through the real hub-proxy path
          // under the successor's worker id. Its handler runs in the session's
          // captured context, so it sees the session directory.
          const channel = new MessageChannel()
          hub.attachHubProxyPort(workerID, channel.port1)
          const client = createHubProxyClient(channel.port2)
          try {
            const ask = client.call("permission.ask", [
              {
                id: "perm_switch",
                sessionID: parentID,
                permission: "bash",
                patterns: ["*"],
                always: [],
              },
            ])
            yield* Effect.promise(() =>
              pollUntil(() => permission.directories.length === 1, {
                message: "the permission request never reached the hub authority",
              }),
            )
            // The successor's ask is live: retiring A did not cancel it.
            expect(permission.pending.has("perm_switch")).toBe(true)
            expect(permission.directories[0]).toBe(instanceCtx.directory)
            yield* Effect.promise(() =>
              client.call("permission.reply", [{ requestID: "perm_switch", reply: "once" }]),
            )
            yield* Effect.promise(() => ask)
          } finally {
            client.dispose()
            channel.port1.close()
            channel.port2.close()
            oldClient.dispose()
            disposeOld()
            oldChannel.port1.close()
            oldChannel.port2.close()
          }

          // A cancel now cascades through the parent's child index. If the
          // generation retirement had dropped the session index, the child would
          // be left running.
          const report = yield* Effect.promise(() => hub.cancelWithReport(parentID))
          expect(report.descendants.map((descendant) => String(descendant.sessionID))).toContain(String(childID))
          yield* Effect.promise(() =>
            pollUntil(() => hub.dispatcher.registry.entry(childID) === undefined, {
              message: "the child worker survived the parent cancel",
            }),
          )
          // The parked successor run and child were cancelled by the same cascade.
          yield* Fiber.join(successor).pipe(Effect.exit)
          yield* Fiber.join(childFiber).pipe(Effect.exit)
        }),
      )
    }),
  )
})

describe("generation retirement is generation-scoped", () => {
  test("releases the old lease and never runs the session hook or marks the successor lost", async () => {
    const sessionID = sid("ses_generation_scope")
    const released: string[] = []
    const generationReleased: string[] = []
    const zeroed: Array<ReadonlyArray<unknown>> = []
    let terminations = 0
    let owner = ""
    const results = new Map<string, SessionExecutionResult>()
    const workers: ExecutorWorker[] = []
    const makeWorker = (id: number): ExecutorWorker => ({
      id,
      alive: true,
      async execute(run) {
        results.set(run.sessionID, { sessionID: run.sessionID, value: { execution: 1 } })
      },
      async cancel() {},
      async assertNotBusy() {
        return false
      },
      resultFor: (session) => results.get(session),
      async terminate() {
        terminations += 1
      },
    })
    const pool: ExecutorWorkerPool = {
      get workers() {
        return workers
      },
      spawn: (id) => {
        const created = makeWorker(id ?? 1)
        workers.push(created)
        return created
      },
      terminateAll: async () => {},
      byID: (id) => workers.find((candidate) => candidate.id === id),
      resultFor: (session) => results.get(session),
    }
    const poolFactory: ProductionPoolFactory = () => pool
    const hub = createProductionSessionHub({ ...{ barrier: createTestBarrier() },
      init: () => sessionWorkerInit(),
      poolFactory,
      db: {
        threadId: 0,
        query: async (sql: string) => (sql.includes("event_sequence") ? [{ worker_owner_id: owner }] : []),
        run: async (sql: string, params?: ReadonlyArray<unknown>) => {
          if (sql.includes("worker_expires_at = 0") && params !== undefined) zeroed.push([...params])
          return { changes: 1, lastInsertRowid: 0 }
        },
        transaction: async () => [],
        close: async () => {},
      } as never,
      onGenerationReleased: (id) => {
        generationReleased.push(String(id))
      },
      onSessionReleased: (id) => {
        released.push(String(id))
      },
      onWorkerCrashed: () => {},
    })
    try {
      await hub.ensureRunning(sessionID)
      const workerID = hub.dispatcher.registry.entry(sessionID)!.workerID
      owner = `worker-${workerID}-old`
      await hub.dispatcher.retireGeneration(sessionID, workerID)

      // Generation hook ran; the session hook did not. The isolate (the
      // successor slot) was never terminated.
      expect(generationReleased).toEqual([String(sessionID)])
      expect(released).toEqual([])
      expect(terminations).toBe(0)
      // The retired generation's exact lease was zeroed, never a successor's.
      expect(zeroed).toEqual([[String(sessionID), owner]])

      // The successor generation is NOT treated as lost: an indeterminate call
      // it starts survives the next run start instead of being force-released.
      hub.lifecycle.toolStarted(sessionID, "call_successor", true)
      await hub.ensureRunning(sessionID)
      expect(hub.lifecycle.indeterminate(sessionID).map((call) => call.callID)).toEqual(["call_successor"])
    } finally {
      await hub.terminateAll()
    }
  }, 30_000)
})
