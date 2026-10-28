// Flag-on hub wiring: `run-state.ts` forwards `startShell` and
// `cancel` to the hub, and the hub routes UI stop through the
// cooperative `cancelWithGrace` path.
//
// The flag-on/flag-off `ensureRunning` delegation matrix lives in
// `rollout/flag-gating.test.ts`. What is unique here is the
// control wiring: `startShell` opens the caller's `ready` latch (the hub cannot
// carry it), maps a hub busy rejection to `Session.BusyError`, and `cancel`
// reaches the hub. The hub runs over an in-memory executor pool so the wiring is
// tested deterministically without spawning isolates.

import { describe, expect, test } from "bun:test"
import { Context, Deferred, Effect, Fiber, Latch, Layer, Logger, Scope } from "effect"
import { ModelsDev } from "@opencode-ai/core/models-dev"
import { Auth } from "@/auth"
import { Config } from "@/config/config"
import type { SessionID } from "@/session/schema"
import { BackgroundJob } from "@/background/job"
import { Session } from "@/session/session"
import { Permission } from "@/permission"
import { Provider } from "@/provider/provider"
import { Question } from "@/question"
import { SessionStatus } from "@/session/status"
import { SessionRunState } from "@/session/run-state"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { EventV2Bridge, type LiveEvent, type LiveEventPublisher } from "@/event-v2-bridge"
import {
  createProductionSessionHub,
  createSessionHub,
  productionSessionHubLayer,
  productionSessionHubNode,
  SessionHubService,
  type ProductionHubServices,
  type SessionHub,
} from "@/session/worker/hub"
import { workerLogMessage, type WorkerLogger } from "@/session/worker/log"
import { reapSupport } from "@/session/worker/pid-registry"
import { sessionWorkerInit } from "../../lib/session-worker-init"
import { pollUntil } from "../../lib/poll"
import { SessionBusyError, type ExecutorWorkerPool } from "@/session/worker/execute"
import { withTmpdirInstance } from "../../fixture/fixture"
import { pollWithTimeout, testEffect } from "../../lib/effect"
import { flush, sid } from "../../lib/async"
import { createRecordingScheduler } from "../../lib/scheduler"
import { withParts } from "../../lib/session-parts"
import { createFakeExecutorPool, FakeExecutorWorker } from "../../lib/fake-worker-pool"
import { createLazyExecutorWorkerPool } from "../../lib/lazy-worker-pool"
import { fromPool } from "@/session/worker/worker-pools"
import type { WorkerPorts } from "@/session/worker/worker-ports"

const it = testEffect(Layer.empty)

const runStateLayer = SessionRunState.layer.pipe(
  Layer.provide(
    Layer.mergeAll(
      // The flag-on cancel branch now cancels background jobs too, so `list`
      // must be callable even when no jobs are running.
      Layer.mock(BackgroundJob.Service, { list: () => Effect.succeed([]) }),
      Layer.mock(SessionStatus.Service, { set: () => Effect.void }),
    ),
  ),
)

/** A status set call, recorded in order so a test can assert the exact sequence. */
interface StatusCall {
  readonly sessionID: SessionID
  readonly status: SessionStatus.Info
}

/**
 * `SessionRunState.layer` with a recording `SessionStatus` and a `BackgroundJob`
 * stub whose `list` is callable (the flag-off cancel branch cancels background
 * jobs before touching the runner table). Used to pin the status transitions a
 * cancel produces, flag on and flag off.
 */
const recordingRunStateLayer = (calls: StatusCall[]) =>
  SessionRunState.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(BackgroundJob.Service, { list: () => Effect.succeed([]) }),
        Layer.mock(SessionStatus.Service, {
          set: (sessionID, status) => Effect.sync(() => void calls.push({ sessionID, status })),
        }),
      ),
    ),
  )

/**
 * `SessionRunState.layer` with a recording `SessionStatus` AND a `BackgroundJob`
 * stub that reports a fixed set of running jobs and records every `cancel`. Used
 * to pin the layer contract: cancel must cancel background subagents/jobs
 * (flag on and flag off) as well as reach the single terminal idle.
 */
const recordingBackgroundRunStateLayer = (calls: StatusCall[], jobs: BackgroundJob.Info[], cancelled: string[]) =>
  SessionRunState.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(BackgroundJob.Service, {
          list: () => Effect.succeed(jobs),
          cancel: (id) =>
            Effect.sync(() => {
              cancelled.push(id)
              return undefined
            }),
        }),
        Layer.mock(SessionStatus.Service, {
          set: (sessionID, status) => Effect.sync(() => void calls.push({ sessionID, status })),
        }),
      ),
    ),
  )

/** A running background job owned by `parentSessionID` (a background subagent). */
const backgroundJobOf = (id: string, parentSessionID: SessionID): BackgroundJob.Info => ({
  id,
  type: "task",
  status: "running",
  started_at: 0,
  metadata: { parentSessionId: parentSessionID },
})

const flagsOn = RuntimeFlags.layer({ experimentalSessionWorkers: true })

describe("12 — flag-on hub wiring (startShell / cancel delegation)", () => {
  it.live("startShell calls the hub's startShell and opens the caller's ready latch", () => {
    const pool = createFakeExecutorPool(1, { value: withParts(sid("ses_shell")) })
    const base = createSessionHub({ pool })
    let shellCalls = 0
    const hub: SessionHub = {
      ...base,
      startShell: async (sessionID) => {
        shellCalls += 1
        return base.startShell(sessionID)
      },
    }
    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        const ready = yield* Latch.make()
        const result = yield* state.startShell(
          sid("ses_shell"),
          Effect.succeed(withParts(sid("ses_shell"))),
          Effect.succeed(withParts(sid("ses_shell"))),
          ready,
        )
        expect(result).toEqual(withParts(sid("ses_shell")))
        expect(shellCalls).toBe(1)
        // The hub cannot carry the caller's latch, so run-state must open it:
        // awaiting would suspend forever (and time the test out) otherwise.
        yield* ready.await
      }).pipe(Effect.provide(runStateLayer), Effect.provideService(SessionHubService, hub), Effect.provide(flagsOn)),
    )
  })

  it.live("startShell maps a hub busy rejection to Session.BusyError", () => {
    const pool = createFakeExecutorPool(1)
    const base = createSessionHub({ pool })
    const hub: SessionHub = {
      ...base,
      startShell: async (sessionID) => {
        throw new SessionBusyError({ sessionID })
      },
    }
    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        const error = yield* state
          .startShell(
            sid("ses_busy"),
            Effect.succeed(withParts(sid("ses_busy"))),
            Effect.succeed(withParts(sid("ses_busy"))),
          )
          .pipe(Effect.flip)
        expect(error).toBeInstanceOf(Session.BusyError)
      }).pipe(Effect.provide(runStateLayer), Effect.provideService(SessionHubService, hub), Effect.provide(flagsOn)),
    )
  })

  it.live("cancel delegates to the hub cancel path", () => {
    const pool = createFakeExecutorPool(1)
    const base = createSessionHub({ pool })
    const cancelled: SessionID[] = []
    const hub: SessionHub = {
      ...base,
      cancelWithReport: async (sessionID) => {
        cancelled.push(sessionID)
        return base.cancelWithReport(sessionID)
      },
    }
    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        yield* state.cancel(sid("ses_none"))
        expect(cancelled).toEqual([sid("ses_none")])
      }).pipe(Effect.provide(runStateLayer), Effect.provideService(SessionHubService, hub), Effect.provide(flagsOn)),
    )
  })

  it.live("cancel emits session.status idle after the hub cancel resolves", () => {
    const pool = createFakeExecutorPool(1)
    const base = createSessionHub({ pool })
    const order: string[] = []
    const hub: SessionHub = {
      ...base,
      cancelWithReport: async (sessionID) => {
        order.push("hub-cancel")
        return base.cancelWithReport(sessionID)
      },
    }
    const calls: StatusCall[] = []
    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        yield* state.cancel(sid("ses_none"))
        // The hub cancel ran, and run-state published exactly one idle for the
        // session — the same event/fields the flag-off branch emits.
        expect(order).toEqual(["hub-cancel"])
        expect(calls).toEqual([{ sessionID: sid("ses_none"), status: { type: "idle" } }])
      }).pipe(
        Effect.provide(recordingRunStateLayer(calls)),
        Effect.provideService(SessionHubService, hub),
        Effect.provide(flagsOn),
      ),
    )
  })

  it.live("flag-on cancel cancels background jobs and emits exactly one idle, matching flag-off", () => {
    const sessionID = sid("ses_bg")
    const job = backgroundJobOf("job_bg_1", sessionID)

    const flagOnCalls: StatusCall[] = []
    const flagOffCalls: StatusCall[] = []
    const flagOnCancelled: string[] = []
    const flagOffCancelled: string[] = []

    const flagOnPool = createFakeExecutorPool(1)
    const flagOnHub = createSessionHub({ pool: flagOnPool })
    const flagOn = withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        yield* state.cancel(sessionID)
      }).pipe(
        Effect.provide(recordingBackgroundRunStateLayer(flagOnCalls, [job], flagOnCancelled)),
        Effect.provideService(SessionHubService, flagOnHub),
        Effect.provide(flagsOn),
      ),
    )

    const flagOff = withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        yield* state.cancel(sessionID)
      }).pipe(Effect.provide(recordingBackgroundRunStateLayer(flagOffCalls, [job], flagOffCancelled))),
    )

    return Effect.all([flagOn, flagOff]).pipe(
      Effect.map(() => {
        // The background subagent is cancelled on both paths — the
        // flag-on branch must not let it outlive the cancel.
        expect(flagOnCancelled).toEqual([job.id])
        expect(flagOffCancelled).toEqual([job.id])
        // Exactly one terminal idle, and the same idle sequence as flag off.
        expect(idleOf(flagOnCalls)).toEqual([{ sessionID, status: { type: "idle" } }])
        expect(idleOf(flagOnCalls)).toEqual(idleOf(flagOffCalls))
      }),
    )
  })

  it.live("flag-on cancel reaches idle exactly like the flag-off sequence", () => {
    const flagOnCalls: StatusCall[] = []
    const flagOffCalls: StatusCall[] = []

    const flagOnPool = createFakeExecutorPool(1)
    const flagOnHub = createSessionHub({ pool: flagOnPool })
    const flagOn = withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        yield* state.cancel(sid("ses_parity"))
      }).pipe(
        Effect.provide(recordingRunStateLayer(flagOnCalls)),
        Effect.provideService(SessionHubService, flagOnHub),
        Effect.provide(flagsOn),
      ),
    )

    // Flag off: no hub service, so `SessionRunState.cancel` uses the in-process
    // branch; a session that is not busy goes straight to `status.set(idle)`.
    const flagOff = withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        yield* state.cancel(sid("ses_parity"))
      }).pipe(Effect.provide(recordingRunStateLayer(flagOffCalls))),
    )

    return Effect.all([flagOn, flagOff]).pipe(
      Effect.map(() => {
        expect(idleOf(flagOnCalls)).toEqual([{ sessionID: sid("ses_parity"), status: { type: "idle" } }])
        expect(idleOf(flagOnCalls)).toEqual(idleOf(flagOffCalls))
      }),
    )
  })
})

// ---------------------------------------------------------------------------
// Integrated flag-on cancel — one scenario must observe the hub cancel outcome,
// exactly one `session.status: idle`, AND the `session.cancelled` event.
//
// These facts were covered by two separate
// tests (`hub-wiring.test.ts` idle; `hub.test.ts` outcome+event) with no single
// integrated run. The shared blocking executor fake stays running until
// cancelled and exposes a cooperative cancel port, so the dispatcher's
// `cancelWithGrace` resolves on the cooperative path instead of terminating.
// ---------------------------------------------------------------------------

/** A `LiveEventPublisher` spy that records every published event. */
const recordingPublisher = () => {
  const published: LiveEvent[] = []
  const publisher: LiveEventPublisher = {
    publish: (event) => void published.push(event),
    publishDefinition: (definition, data) => void published.push({ type: definition.type, data }),
  }
  return { publisher, published }
}

const idleOf = (calls: StatusCall[]) => calls.filter((call) => call.status.type === "idle")

/** Whether the hub currently hosts `sessionID` on a running worker. */
const runningState = (hub: SessionHub, sessionID: SessionID) => (): true | undefined =>
  hub.dispatcher.registry.get(sessionID)?.state === "running" ? true : undefined

describe("12 — integrated flag-on cancel (outcome + idle + session.cancelled)", () => {
  it.live("one flag-on cancel observes the hub outcome, exactly one idle, and session.cancelled", () => {
    const sessionID = sid("ses_integrated")
    const flagOnCalls: StatusCall[] = []
    const flagOffCalls: StatusCall[] = []
    // The executor cancel reaches the worker-local run-state (table B),
    // whose `onIdle` publishes the terminal idle through the hub-proxy. The pool
    // simulates that emit; the hub-side `SessionRunState.cancel` must not add a
    // second one.
    const pool = createFakeExecutorPool(1, {
      mode: "blocking",
      value: withParts(sessionID),
      onCancel: (id) => flagOnCalls.push({ sessionID: id, status: { type: "idle" } }),
    })
    const { publisher, published } = recordingPublisher()
    const hub = createSessionHub({
      pool,
      events: publisher,
      cancellation: () => ({ portFor: (worker) => pool.byID(worker.id)?.cancelPort, graceMs: 5_000 }),
    })

    // Flag on: a worker session that stays busy until `state.cancel` reaches it.
    const flagOn = withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        const running = yield* state
          .ensureRunning(sessionID, Effect.succeed(withParts(sessionID)), Effect.succeed(withParts(sessionID)))
          .pipe(Effect.exit, Effect.forkChild)
        yield* pollWithTimeout(
          Effect.sync(() => (hub.dispatcher.registry.get(sessionID)?.state === "running" ? true : undefined)),
          "flag-on session never reached running",
        )
        const outcome = yield* state.cancel(sessionID)
        yield* Fiber.join(running)
        return outcome
      }).pipe(
        Effect.provide(recordingRunStateLayer(flagOnCalls)),
        Effect.provideService(SessionHubService, hub),
        Effect.provide(flagsOn),
      ),
    )

    // Flag off: the in-process runner, busy on a work effect that signals
    // readiness then never settles — the same "cancel a live run" shape.
    const flagOff = withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        const started = yield* Deferred.make<void>()
        const work = Effect.gen(function* () {
          yield* Deferred.succeed(started, undefined)
          return yield* Effect.never
        })
        const running = yield* state
          .ensureRunning(sessionID, Effect.succeed(withParts(sessionID)), work)
          .pipe(Effect.exit, Effect.forkChild)
        yield* pollWithTimeout(
          Effect.gen(function* () {
            return (yield* Deferred.isDone(started)) ? true : undefined
          }),
          "flag-off session never started",
        )
        const outcome = yield* state.cancel(sessionID)
        yield* Fiber.join(running)
        return outcome
      }).pipe(Effect.provide(recordingRunStateLayer(flagOffCalls))),
    )

    return Effect.gen(function* () {
      const on = yield* flagOn
      const off = yield* flagOff
      if (on === undefined) throw new Error("expected a flag-on cancel outcome")

      // The cancel outcome (mode / winner / acknowledged). The worker ack is
      // what makes the flag-on path report `acknowledged: true`.
      expect(on).toMatchObject({ sessionID, mode: "cooperative", winner: "cancel", acknowledged: true })
      expect(typeof on.durationMs).toBe("number")

      // `session.status: idle` exactly once.
      expect(idleOf(flagOnCalls)).toEqual([{ sessionID, status: { type: "idle" } }])

      // The flag-off busy cancel reaches the same single idle and outcome
      // (acknowledged differs by design: only a worker ack sets it true).
      expect(off).toMatchObject({ sessionID, mode: "cooperative", winner: "cancel", acknowledged: false })
      expect(idleOf(flagOffCalls)).toEqual(idleOf(flagOnCalls))

      // `session.cancelled` with the right fields, published once.
      expect(published.map((event) => event.type)).toEqual(["session.cancelled"])
      expect(published[0]?.data).toEqual({
        sessionID,
        mode: "cooperative",
        durationMs: on.durationMs,
        acknowledged: true,
      })
    })
  })

  it.live("cancel with nothing running is a no-op-safe idle with a sensible outcome", () => {
    const sessionID = sid("ses_noop")
    const pool = createFakeExecutorPool(1, { mode: "blocking", value: withParts(sessionID) })
    const { publisher, published } = recordingPublisher()
    const hub = createSessionHub({
      pool,
      events: publisher,
      cancellation: () => ({ portFor: (worker) => pool.byID(worker.id)?.cancelPort }),
    })

    const flagOnCalls: StatusCall[] = []
    const flagOffCalls: StatusCall[] = []

    const flagOn = withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        return yield* state.cancel(sessionID)
      }).pipe(
        Effect.provide(recordingRunStateLayer(flagOnCalls)),
        Effect.provideService(SessionHubService, hub),
        Effect.provide(flagsOn),
      ),
    )

    const flagOff = withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        return yield* state.cancel(sessionID)
      }).pipe(Effect.provide(recordingRunStateLayer(flagOffCalls))),
    )

    return Effect.gen(function* () {
      const on = yield* flagOn
      const off = yield* flagOff

      // Flag on: nothing was admitted, so the hub reports no outcome — a safe
      // no-op. Run-state still emits the terminal idle exactly once.
      expect(on).toBeUndefined()
      expect(idleOf(flagOnCalls)).toEqual([{ sessionID, status: { type: "idle" } }])
      // No outcome means no `session.cancelled` was published.
      expect(published).toEqual([])

      // Flag off: the in-process branch reports the completed outcome.
      expect(off).toEqual({ sessionID, mode: "completed", winner: "completion", acknowledged: false, durationMs: 0 })

      // Both arms emit the same single terminal idle: the no-op is safe.
      expect(idleOf(flagOffCalls)).toEqual(idleOf(flagOnCalls))
    })
  })
})

// ---------------------------------------------------------------------------
// Exactly ONE idle per flag-ON cancel, in every case.
//
// The executor cancel now routes into the worker-local run-state (table B),
// whose `onIdle` publishes `session.status: idle` through the hub-proxy. The
// hub-side `SessionRunState.cancel` therefore emits the fallback idle ONLY when
// no worker could have published it: a queued session, an idle no-op, or a
// grace-expired terminate. These three cases pin that contract, each against
// flag-off.
// ---------------------------------------------------------------------------

describe("12 — exactly one flag-on cancel idle per case (ON ≡ OFF)", () => {
  it.live("busy/dispatched: the worker publishes the idle, the hub does not double-emit", () => {
    const sessionID = sid("ses_busy_one")
    const onCalls: StatusCall[] = []
    const offCalls: StatusCall[] = []
    // The pool simulates table B publishing idle through the hub-proxy on cancel.
    const pool = createFakeExecutorPool(1, {
      mode: "blocking",
      value: withParts(sessionID),
      onCancel: (id) => onCalls.push({ sessionID: id, status: { type: "idle" } }),
    })
    const hub = createSessionHub({
      pool,
      cancellation: () => ({ portFor: (worker) => pool.byID(worker.id)?.cancelPort, graceMs: 5_000 }),
    })

    const flagOn = withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        const running = yield* state
          .ensureRunning(sessionID, Effect.succeed(withParts(sessionID)), Effect.succeed(withParts(sessionID)))
          .pipe(Effect.exit, Effect.forkChild)
        yield* pollWithTimeout(Effect.sync(runningState(hub, sessionID)), "flag-on session never reached running")
        yield* state.cancel(sessionID)
        yield* Fiber.join(running)
      }).pipe(
        Effect.provide(recordingRunStateLayer(onCalls)),
        Effect.provideService(SessionHubService, hub),
        Effect.provide(flagsOn),
      ),
    )

    const flagOff = withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        const started = yield* Deferred.make<void>()
        const work = Effect.gen(function* () {
          yield* Deferred.succeed(started, undefined)
          return yield* Effect.never
        })
        const running = yield* state
          .ensureRunning(sessionID, Effect.succeed(withParts(sessionID)), work)
          .pipe(Effect.exit, Effect.forkChild)
        yield* pollWithTimeout(
          Effect.gen(function* () {
            return (yield* Deferred.isDone(started)) ? true : undefined
          }),
          "flag-off session never started",
        )
        yield* state.cancel(sessionID)
        yield* Fiber.join(running)
      }).pipe(Effect.provide(recordingRunStateLayer(offCalls))),
    )

    return Effect.all([flagOn, flagOff]).pipe(
      Effect.map(() => {
        // One idle, from the worker's hub-proxy emit — not two.
        expect(idleOf(onCalls)).toEqual([{ sessionID, status: { type: "idle" } }])
        expect(idleOf(offCalls)).toEqual([{ sessionID, status: { type: "idle" } }])
        expect(idleOf(onCalls)).toEqual(idleOf(offCalls))
      }),
    )
  })

  it.live("idle/no-op: the hub publishes the single idle, matching flag-off", () => {
    const sessionID = sid("ses_idle_one")
    const onCalls: StatusCall[] = []
    const offCalls: StatusCall[] = []
    const pool = createFakeExecutorPool(1, { mode: "blocking", value: withParts(sessionID) })
    const hub = createSessionHub({
      pool,
      cancellation: () => ({ portFor: (worker) => pool.byID(worker.id)?.cancelPort, graceMs: 5_000 }),
    })

    const flagOn = withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        return yield* state.cancel(sessionID)
      }).pipe(
        Effect.provide(recordingRunStateLayer(onCalls)),
        Effect.provideService(SessionHubService, hub),
        Effect.provide(flagsOn),
      ),
    )

    const flagOff = withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        return yield* state.cancel(sessionID)
      }).pipe(Effect.provide(recordingRunStateLayer(offCalls))),
    )

    return Effect.all([flagOn, flagOff]).pipe(
      Effect.map(([on]) => {
        // Nothing was admitted and nothing ran: the hub-side emits the idle.
        expect(on).toBeUndefined()
        expect(idleOf(onCalls)).toEqual([{ sessionID, status: { type: "idle" } }])
        expect(idleOf(offCalls)).toEqual(idleOf(onCalls))
      }),
    )
  })
})

// ---------------------------------------------------------------------------
// Parent cancel must settle every subagent child to exactly one idle.
//
// The user bug: `SessionRunState.cancel` wrote the fallback idle only for the
// requested session, so a child the hub cascade could not settle (never
// admitted, queued, or grace-terminated) never reached `SessionStatus = idle`.
// The hub now reports its cascade's descendants and whether each worker
// published its own idle; run-state writes the fallback for exactly the rest.
// ---------------------------------------------------------------------------

describe("parent cancel settles every child to exactly one idle", () => {
  const idleCalls = (calls: StatusCall[]) => calls.filter((call) => call.status.type === "idle")
  const runningState = (hub: SessionHub, sessionID: SessionID) => (): true | undefined =>
    hub.dispatcher.registry.get(sessionID)?.state === "running" ? true : undefined

  /** A work/onInterrupt pair a test never inspects; `ensureRunning` needs both. */
  const work = (sessionID: SessionID) => Effect.succeed(withParts(sessionID))

  it.live("a linked child the hub never dispatched still reaches one idle", () => {
    const parentID = sid("ses_w4_parent")
    const childID = sid("ses_w4_child")
    const calls: StatusCall[] = []
    const { publisher, published } = recordingPublisher()
    // Only the parent runs on a worker; the child is linked but never
    // dispatched, so no worker can publish its idle.
    const pool = createFakeExecutorPool(1, {
      mode: "blocking",
      value: withParts(parentID),
      onCancel: (id) => {
        if (id === parentID) calls.push({ sessionID: id, status: { type: "idle" } })
      },
    })
    const hub = createSessionHub({
      pool,
      events: publisher,
      cancellation: () => ({
        childrenOf: (id) => (id === parentID ? [childID] : []),
        portFor: (worker) => pool.byID(worker.id)?.cancelPort,
        graceMs: 5_000,
      }),
    })

    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        const running = yield* state
          .ensureRunning(parentID, work(parentID), work(parentID))
          .pipe(Effect.exit, Effect.forkChild)
        yield* pollWithTimeout(Effect.sync(runningState(hub, parentID)), "parent never reached running")
        yield* state.cancel(parentID)
        yield* Fiber.join(running)
      }).pipe(
        Effect.provide(recordingRunStateLayer(calls)),
        Effect.provideService(SessionHubService, hub),
        Effect.provide(flagsOn),
      ),
    ).pipe(
      Effect.map(() => {
        const idles = idleCalls(calls)
        // The parent's worker published its own idle; no fallback double-emit.
        expect(idles.filter((call) => call.sessionID === parentID)).toBeArrayOfSize(1)
        // The child never ran, so run-state's cascade fallback is its only idle.
        expect(idles.filter((call) => call.sessionID === childID)).toBeArrayOfSize(1)
        // The parent's cancel is reported once; the child's fallback idle is
        // the only thing the hub reports for it.
        expect(published.map((event) => event.type)).toEqual(["session.cancelled"])
      }),
    )
  })

  it.live("a child the hub registry never linked is swept via Session.children", () => {
    const parentID = sid("ses_w4_sweep_parent")
    const childID = sid("ses_w4_sweep_child")
    const calls: StatusCall[] = []
    // The parent runs; the child is only persisted (no hub registry link).
    const pool = createFakeExecutorPool(1, {
      mode: "blocking",
      value: withParts(parentID),
      onCancel: (id) => {
        if (id === parentID) calls.push({ sessionID: id, status: { type: "idle" } })
      },
    })
    const hub = createSessionHub({
      pool,
      cancellation: () => ({ portFor: (worker) => pool.byID(worker.id)?.cancelPort, graceMs: 5_000 }),
    })
    const sessions = Session.Service.of({
      children: (id: SessionID) => Effect.succeed(id === parentID ? [{ id: childID } as Session.Info] : []),
    } as unknown as Session.Interface)

    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        const running = yield* state
          .ensureRunning(parentID, work(parentID), work(parentID))
          .pipe(Effect.exit, Effect.forkChild)
        yield* pollWithTimeout(Effect.sync(runningState(hub, parentID)), "parent never reached running")
        yield* state.cancel(parentID)
        yield* Fiber.join(running)
      }).pipe(
        Effect.provide(recordingRunStateLayer(calls)),
        Effect.provideService(SessionHubService, hub),
        Effect.provide(flagsOn),
        Effect.provideService(Session.Service, sessions),
      ),
    ).pipe(
      Effect.map(() => {
        const idles = idleCalls(calls)
        expect(idles.filter((call) => call.sessionID === parentID)).toBeArrayOfSize(1)
        // The sweep cancelled the persisted child through the hub and wrote its idle.
        expect(idles.filter((call) => call.sessionID === childID)).toBeArrayOfSize(1)
      }),
    )
  })
})

// ---------------------------------------------------------------------------
// Hub observability: the production hub logs pool/entry/reap resolution and
// pid reports through the injected `log` seam.
// ---------------------------------------------------------------------------

interface RecordedLogLine {
  readonly message: string
  readonly fields: Record<string, unknown>
}

const recordingLog = (): { readonly log: WorkerLogger; readonly lines: RecordedLogLine[] } => {
  const lines: RecordedLogLine[] = []
  return { log: (message, fields) => void lines.push({ message, fields }), lines }
}

const lineFor = (lines: RecordedLogLine[], event: string): RecordedLogLine => {
  const line = lines.find((entry) => entry.message === workerLogMessage(event))
  if (line === undefined) throw new Error(`no log line for "${event}"; saw ${JSON.stringify(lines.map((l) => l.message))}`)
  return line
}

/** A production hub over an in-memory pool: no isolate is ever spawned. */
const productionHubWithLog = (options: {
  readonly count?: number
  readonly env?: Record<string, string | undefined>
  readonly entry?: string | URL
  readonly log: WorkerLogger
}): SessionHub =>
  createProductionSessionHub({
    init: sessionWorkerInit(),
    ...(options.count === undefined ? {} : { count: options.count }),
    poolFactory: () => createFakeExecutorPool(options.count ?? 1),
    log: options.log,
    ...(options.env === undefined ? {} : { env: options.env }),
    ...(options.entry === undefined ? {} : { entry: options.entry }),
  })

describe("hub construction resolution", () => {
  test("logs the resolved pool with count and parallelism (no warm headroom)", async () => {
    const { log, lines } = recordingLog()
    const hub = productionHubWithLog({ count: 4, env: {}, log })
    try {
      const line = lineFor(lines, "pool resolved")
      expect(line.fields.workerCount).toBe(4)
      expect(typeof line.fields.availableParallelism).toBe("number")
      expect(line.fields.availableParallelism as number).toBeGreaterThan(0)
      // The removed env-count knob must not reappear as a stale diagnostic field.
      expect("override" in line.fields).toBe(false)
      // v2 has no headroom reserve: per-session workers materialize on demand.
      expect("headroom" in line.fields).toBe(false)
    } finally {
      await hub.terminateAll()
    }
  })

  test("logs workerCount 0 when no count is configured (diagnostic only)", async () => {
    const { log, lines } = recordingLog()
    const hub = productionHubWithLog({ env: {}, log })
    try {
      const line = lineFor(lines, "pool resolved")
      // `workerCount` is diagnostic, not a prewarm: no configured count means 0.
      expect(line.fields.workerCount).toBe(0)
      expect("override" in line.fields).toBe(false)
    } finally {
      await hub.terminateAll()
    }
  })

  test("logs the resolved worker entry and its source (explicit path)", async () => {
    const { log, lines } = recordingLog()
    const hub = productionHubWithLog({ count: 1, env: {}, entry: "/opt/session-worker.js", log })
    try {
      const line = lineFor(lines, "worker entry resolved")
      expect(line.fields.entry).toBe("/opt/session-worker.js")
      expect(line.fields.source).toBe("definedPath")
    } finally {
      await hub.terminateAll()
    }
  })

  test("reports the env source when the entry comes from the environment", async () => {
    const { log, lines } = recordingLog()
    const hub = productionHubWithLog({ count: 1, env: { OPENCODE_SESSION_WORKER_PATH: "/env/worker.js" }, log })
    try {
      const line = lineFor(lines, "worker entry resolved")
      expect(line.fields.entry).toBe("/env/worker.js")
      expect(line.fields.source).toBe("env")
    } finally {
      await hub.terminateAll()
    }
  })

  test("logs reap support for this platform", async () => {
    const support = reapSupport()
    const { log, lines } = recordingLog()
    const hub = productionHubWithLog({ count: 1, env: {}, log })
    try {
      const line = lineFor(lines, "reap support")
      expect(line.fields).toEqual({ level: "INFO", supported: support.supported, reason: support.reason })
    } finally {
      await hub.terminateAll()
    }
  })

  test("logs a pid report with the mirrored entry count", async () => {
    const { log, lines } = recordingLog()
    const hub = productionHubWithLog({ count: 1, env: {}, log })
    const channel = new MessageChannel()
    try {
      hub.attachHubProxyPort(7, channel.port1)
      channel.port2.postMessage({
        type: "pid-report",
        workerID: 7,
        entries: [
          { pid: 4242, pgid: 4242, label: "shell", workerID: 7 },
          { pid: 4343, pgid: 4343, label: "shell", workerID: 7 },
        ],
      })

      await pollUntil(() => lines.some((entry) => entry.message === workerLogMessage("pid report")), {
        message: "pid report never logged",
      })
      expect(lineFor(lines, "pid report").fields).toEqual({ level: "INFO", workerID: 7, entries: 2 })
    } finally {
      channel.port1.close()
      channel.port2.close()
      await hub.terminateAll()
    }
  })
})

// ---------------------------------------------------------------------------
// The hub-path narrative must emit through the
// PRODUCTION layer, not only the test-only `createSessionHubLayer`.
//
// The verification agent found `productionSessionHubLayer` never created a
// bridge logger, so `options.log` defaulted to `noopWorkerLogger` and the bring-up lines
// (and every transitively forwarded event) were dormant on the real app graphs
// (`app-runtime.ts`, `server/routes/instance/httpapi/server.ts`). This builds
// that exact layer, provides the nine services it needs as mocks, injects a
// capturing Effect `Logger`, and asserts the bring-up line emits.
// ---------------------------------------------------------------------------

/** The nine services `productionSessionHubLayer` requires; none is called at build. */
const productionWiringServices = Layer.mergeAll(
  Layer.mock(Config.Service, {}),
  Layer.mock(Auth.Service, {}),
  Layer.mock(ModelsDev.Service, {}),
  Layer.mock(Provider.Service, {}),
  RuntimeFlags.layer({ experimentalSessionWorkers: true }),
  Layer.mock(SessionStatus.Service, {}),
  Layer.mock(Permission.Service, {}),
  Layer.mock(Question.Service, {}),
  Layer.mock(EventV2Bridge.Service, {}),
)

describe("the production hub layer emits the narrative", () => {
  test("productionSessionHubLayer creates a bridge logger so the bring-up line emits (not the noop)", async () => {
    // The bridge logger forks `Effect.logInfo`; capture the raw Effect messages.
    const messages: unknown[] = []
    const logger = Logger.make<unknown, void>((entry) => {
      messages.push(entry.message)
    })
    const emitted = () =>
      messages.some((message) => Array.isArray(message) && message[0] === workerLogMessage("pool resolved"))

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(
            // The production layer (the same one `productionSessionHubNode` wraps),
            // over an in-memory pool so no isolate is spawned at build.
            productionSessionHubLayer({ count: 2, poolFactory: () => createFakeExecutorPool(2) }).pipe(
              Layer.provide(productionWiringServices),
            ),
          )
          const hub = Context.get(context, SessionHubService)
          expect(hub).toBeDefined()
          // The bridge forks the line asynchronously; poll the captured messages.
          yield* Effect.promise(() =>
            pollUntil(emitted, { message: "production layer never logged 'pool resolved'" }),
          )
        }),
      ).pipe(Effect.provide(Logger.layer([logger]))),
    )
  })
})

// ---------------------------------------------------------------------------
// Production service wiring — the layer owns the Wave-6 `WorkerPorts` and
// `WorkerPools` services: building it spawns no isolate, a worker's ports attach
// through the provided `WorkerPorts` service, and teardown disposes ports before
// terminating the pool. The graph node keeps its nine declared deps.
// ---------------------------------------------------------------------------

describe("production hub service wiring", () => {
  test("no isolate at build, ports attach through WorkerPorts, ports dispose before pool terminate", async () => {
    const order: string[] = []
    const created: number[] = []
    const disposed: number[] = []
    const lazy = createLazyExecutorWorkerPool({
      create: async (id) => {
        created.push(id)
        return new FakeExecutorWorker(id)
      },
    })
    const pool: ExecutorWorkerPool = {
      get workers() {
        return lazy.workers
      },
      spawn: (id) => lazy.spawn(id),
      releaseSession: (sessionID, workerID) => lazy.releaseSession?.(sessionID, workerID),
      terminateAll: async () => {
        order.push("pool-terminate")
        await lazy.terminateAll()
      },
      byID: (id) => lazy.byID(id),
      resultFor: (sessionID) => lazy.resultFor(sessionID),
    }
    let services: ProductionHubServices | undefined
    const channel = new MessageChannel()

    try {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const context = yield* Layer.build(
              productionSessionHubLayer({
                count: 1,
                poolFactory: () => pool,
                ports: {
                  egress: (workerID, port) => {
                    port.start()
                    return () => {
                      order.push("ports-dispose")
                      disposed.push(workerID)
                    }
                  },
                },
                onServices: (resolved) => {
                  services = resolved
                },
              }).pipe(Layer.provide(productionWiringServices)),
            )
            const hub = Context.get(context, SessionHubService)

            // Building the layer (and resolving the pool/port services) spawns
            // no isolate: materialization happens only on dispatch.
            expect(created).toEqual([])
            expect(services?.workerPorts).toBeDefined()
            expect(services?.workerPools.executorPool).toBe(pool)

            // The attach reaches the layer-owned service, not a private registry.
            hub.attachWorkerPorts(7, { egress: channel.port1 })
            expect(disposed).toEqual([])
            expect(yield* services!.workerPorts!.attached()).toEqual({ workerPorts: [7], hubProxy: [] })

            yield* Effect.promise(() => hub.terminateAll())
            expect(disposed).toEqual([7])
            expect(order).toEqual(["ports-dispose", "pool-terminate"])
          }),
        ),
      )
    } finally {
      channel.port1.close()
      channel.port2.close()
    }
  })

  test("a transient backend build failure is not cached; the next attach retries and succeeds", async () => {
    // H/M1: `sharedPortSource` used to mark the build resolved BEFORE invoking
    // the source, so one throw became a permanent "no backends" cache. The first
    // attach must fail loud; the second must rebuild and attach the worker's port.
    let builds = 0
    const disposed: number[] = []
    const pool = createFakeExecutorPool(1)
    const hub = createProductionSessionHub({
      init: sessionWorkerInit(),
      poolFactory: () => pool,
      ports: () => {
        builds += 1
        if (builds === 1) throw new Error("transient backend build failure")
        return {
          writer: {} as never,
          egress: (workerID, port) => {
            port.start()
            return () => disposed.push(workerID)
          },
        }
      },
    })
    const channel = new MessageChannel()
    try {
      expect(() => hub.attachWorkerPorts(1, { egress: channel.port1 })).toThrow("transient backend build failure")
      expect(builds).toBe(1)

      // The failure must not be cached: the retry builds again and attaches.
      hub.attachWorkerPorts(1, { egress: channel.port1 })
      expect(builds).toBe(2)
      expect(disposed).toEqual([])

      await hub.terminateAll()
      expect(disposed).toEqual([1])
    } finally {
      channel.port1.close()
      channel.port2.close()
    }
  })

  test("layer scope terminates the pool exactly once, after disposing ports", async () => {
    // H/M2: the redundant scope finalizer C terminated the pool a second time
    // after the hub's own finalizer. Post-scope, the count must be exactly one and
    // the ports must already be disposed when the pool terminate runs.
    const order: string[] = []
    const lazy = createLazyExecutorWorkerPool({
      create: async (id) => new FakeExecutorWorker(id),
    })
    const pool: ExecutorWorkerPool = {
      get workers() {
        return lazy.workers
      },
      spawn: (id) => lazy.spawn(id),
      releaseSession: (sessionID, workerID) => lazy.releaseSession?.(sessionID, workerID),
      terminateAll: async () => {
        order.push("pool-terminate")
        await lazy.terminateAll()
      },
      byID: (id) => lazy.byID(id),
      resultFor: (sessionID) => lazy.resultFor(sessionID),
    }
    const channel = new MessageChannel()

    try {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const context = yield* Layer.build(
              productionSessionHubLayer({
                count: 1,
                poolFactory: () => pool,
                ports: {
                  egress: (workerID, port) => {
                    port.start()
                    return () => order.push("ports-dispose")
                  },
                },
              }).pipe(Layer.provide(productionWiringServices)),
            )
            const hub = Context.get(context, SessionHubService)
            hub.attachWorkerPorts(7, { egress: channel.port1 })
            // Intentionally no manual terminate: the layer scope owns teardown.
          }),
        ),
      )

      expect(order.filter((event) => event === "pool-terminate")).toHaveLength(1)
      expect(order).toEqual(["ports-dispose", "pool-terminate"])
      expect(order.indexOf("ports-dispose")).toBeLessThan(order.indexOf("pool-terminate"))
    } finally {
      channel.port1.close()
      channel.port2.close()
    }
  })

  test("a caller-provided scope does not finalize caller-owned port/pool services", async () => {
    // H/M3: `options.scope` owns only services the hub creates internally. A
    // pre-built `workerPorts`/`workerPools` stays caller-owned, so closing the
    // passed scope must not dispose them.
    let portDisposals = 0
    let poolTerminations = 0
    const ports: WorkerPorts = {
      attachWorkerPorts: () => Effect.succeed(() => {}),
      attachHubProxyPort: () => Effect.succeed(() => {}),
      disposeWorker: () => Effect.void,
      disposeAll: () => Effect.sync(() => void (portDisposals += 1)),
      attached: () => Effect.succeed({ workerPorts: [], hubProxy: [] }),
    }
    const base = createFakeExecutorPool(1)
    const pool: ExecutorWorkerPool = {
      get workers() {
        return base.workers
      },
      spawn: (id) => base.spawn(id),
      terminateAll: async () => {
        poolTerminations += 1
      },
      byID: (id) => base.byID(id),
      resultFor: (sessionID) => base.resultFor(sessionID),
    }

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const scope = yield* Scope.Scope
          createProductionSessionHub({
            init: sessionWorkerInit(),
            scope,
            workerPorts: ports,
            workerPools: fromPool(pool),
          })
        }),
      ),
    )

    expect(portDisposals).toBe(0)
    expect(poolTerminations).toBe(0)
  })

  test("the production node still declares exactly its nine service deps", () => {
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

// ---------------------------------------------------------------------------
// Retired worker port disposal — a reap/cancel retires the worker AND tears down
// its hub-side port servers; only `terminateAll` used to dispose them.
// ---------------------------------------------------------------------------

describe("retired worker hub-side port disposal", () => {
  /** Minimal backends: only the egress disposer matters to these tests. */
  const egressBackends = (disposed: number[]) => ({
    writer: {} as never,
    egress: (workerID: number, port: MessagePort) => {
      port.start()
      return () => disposed.push(workerID)
    },
  })

  test("cancel disposes the worker's egress server", async () => {
    const pool = createFakeExecutorPool(1, { mode: "blocking" })
    const disposed: number[] = []
    const hub = createSessionHub({ pool, ports: egressBackends(disposed) })
    const sessionID = sid("ses_dispose_cancel")
    const channel = new MessageChannel()
    try {
      const running = hub.ensureRunning(sessionID)
      await flush()
      const worker = pool.spawned[0]
      hub.attachWorkerPorts(worker.id, { egress: channel.port1 })
      expect(disposed).toEqual([])

      await hub.cancel(sessionID)
      await expect(running).rejects.toBeDefined()

      expect(disposed).toEqual([worker.id])
    } finally {
      channel.port1.close()
      channel.port2.close()
      await hub.terminateAll()
    }
  })

  test("reap disposes the worker's egress server", async () => {
    const pool = createFakeExecutorPool(1)
    const scheduler = createRecordingScheduler()
    const disposed: number[] = []
    const hub = createSessionHub({
      pool,
      ports: egressBackends(disposed),
      schedule: scheduler.schedule,
      reapMs: 1_000,
    })
    const sessionID = sid("ses_dispose_reap")
    const channel = new MessageChannel()
    try {
      await hub.ensureRunning(sessionID)
      await flush()
      const worker = pool.spawned[0]
      hub.attachWorkerPorts(worker.id, { egress: channel.port1 })

      scheduler.pending().find((timer) => timer.delayMs === 1_000)!.run()
      await flush()

      expect(disposed).toEqual([worker.id])
    } finally {
      channel.port1.close()
      channel.port2.close()
      await hub.terminateAll()
    }
  })
})
