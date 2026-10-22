// Phase 12 — flag-on hub wiring: `run-state.ts` forwards `startShell` and
// `cancel` to the hub, and the hub routes UI stop through the Phase 11
// cooperative `cancelWithGrace` path.
//
// The flag-on/flag-off `ensureRunning` delegation matrix lives in
// `rollout/flag-gating.test.ts` (finding #186). What is unique here is the
// control wiring: `startShell` opens the caller's `ready` latch (the hub cannot
// carry it), maps a hub busy rejection to `Session.BusyError`, and `cancel`
// reaches the hub. The hub runs over an in-memory executor pool so the wiring is
// tested deterministically without spawning isolates.

import { describe, expect } from "bun:test"
import { Deferred, Effect, Fiber, Latch, Layer } from "effect"
import type { SessionID } from "@/session/schema"
import { BackgroundJob } from "@/background/job"
import { Session } from "@/session/session"
import { SessionStatus } from "@/session/status"
import { SessionRunState } from "@/session/run-state"
import { RuntimeFlags } from "@/effect/runtime-flags"
import type { LiveEvent, LiveEventPublisher } from "@/event-v2-bridge"
import type { CancelFrame, CancelPort } from "@/session/worker/cancel"
import { createSessionHub, SessionHubService, type SessionHub } from "@/session/worker/hub"
import {
  SessionBusyError,
  type ExecutorRun,
  type ExecutorSessionWorker,
  type ExecutorWorker,
  type ExecutorWorkerPool,
  type SessionExecutionResult,
} from "@/session/worker/execute"
import { withTmpdirInstance } from "../../fixture/fixture"
import { pollWithTimeout, testEffect } from "../../lib/effect"
import { sid } from "../../lib/async"
import { withParts } from "../../lib/session-parts"

const it = testEffect(Layer.empty)

class FakeWorker implements ExecutorWorker {
  busy = false
  constructor(
    readonly id: number,
    private readonly onExecute: (run: ExecutorRun) => void,
  ) {}
  get alive(): boolean {
    return true
  }
  async execute(run: ExecutorRun): Promise<void> {
    this.onExecute(run)
  }
  async cancel(): Promise<void> {}
  async assertNotBusy(): Promise<boolean> {
    return this.busy
  }
  resultFor(): SessionExecutionResult | undefined {
    return undefined
  }
  async terminate(): Promise<void> {}
}

class FakePool implements ExecutorWorkerPool {
  readonly workers: FakeWorker[]
  readonly spawned: FakeWorker[] = []
  private readonly results = new Map<SessionID, SessionExecutionResult>()
  private nextSpawnId = 100

  constructor(
    k: number,
    private readonly value: unknown,
  ) {
    this.workers = Array.from({ length: k }, (_, index) => this.make(index + 1))
  }

  private make(id: number): FakeWorker {
    return new FakeWorker(id, (run) => {
      this.results.set(run.sessionID, { sessionID: run.sessionID, value: this.value })
    })
  }

  spawn(): ExecutorSessionWorker {
    const worker = this.make(this.nextSpawnId++)
    this.spawned.push(worker)
    return worker
  }

  async terminateAll(): Promise<void> {}

  byID(id: number): ExecutorWorker | undefined {
    return [...this.workers, ...this.spawned].find((worker) => worker.id === id)
  }

  resultFor(sessionID: SessionID): SessionExecutionResult | undefined {
    return this.results.get(sessionID)
  }
}

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
 * to pin the Phase 6.1 contract: cancel must cancel background subagents/jobs
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
    const pool = new FakePool(1, withParts(sid("ses_shell")))
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
    const pool = new FakePool(1, undefined)
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
    const pool = new FakePool(1, undefined)
    const base = createSessionHub({ pool })
    const cancelled: SessionID[] = []
    const hub: SessionHub = {
      ...base,
      cancel: async (sessionID) => {
        cancelled.push(sessionID)
        return base.cancel(sessionID)
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
    const pool = new FakePool(1, undefined)
    const base = createSessionHub({ pool })
    const order: string[] = []
    const hub: SessionHub = {
      ...base,
      cancel: async (sessionID) => {
        order.push("hub-cancel")
        return base.cancel(sessionID)
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

    const flagOnPool = new FakePool(1, undefined)
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
        // Phase 6.1: the background subagent is cancelled on both paths — the
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

    const flagOnPool = new FakePool(1, undefined)
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
        const idleOf = (calls: StatusCall[]) => calls.filter((call) => call.status.type === "idle")
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
// The audit (`o1-w4-audit.md` §5) found these facts covered by two separate
// tests (`hub-wiring.test.ts` idle; `hub.test.ts` outcome+event) with no single
// integrated run. The worker below stays running until cancelled and exposes a
// cooperative cancel port, so the dispatcher's `cancelWithGrace` resolves on the
// cooperative path instead of terminating.
// ---------------------------------------------------------------------------

/** A worker that stays running until cancelled, with a cooperative cancel port. */
class BlockingWorker implements ExecutorWorker {
  readonly cancelPort: CancelPort
  private readonly finish = new Map<string, () => void>()
  private readonly results = new Map<SessionID, SessionExecutionResult>()
  private readonly ackHandlers = new Set<(frame: CancelFrame) => void>()

  constructor(
    readonly id: number,
    private readonly value: unknown,
    /**
     * Simulates the worker-local run-state (table B) publishing the terminal
     * `session.status: idle` through the hub-proxy before it acknowledges the
     * cancel. Phase 4 routes the executor cancel into that inner runner, so a
     * dispatched run emits its own idle; the hub-side `SessionRunState.cancel`
     * must not emit a second one.
     */
    private readonly onCancelIdle?: (sessionID: SessionID) => void,
  ) {
    this.cancelPort = {
      notify: (frame) => {
        if (frame.type !== "cancel") return
        // table B publishes idle on unwind, BEFORE the ack. Then acknowledge:
        // `cancelWithGrace` reclassifies a settle with no ack to a natural
        // completion, so the ack must land before the run settles.
        this.onCancelIdle?.(frame.sessionID as SessionID)
        for (const handler of this.ackHandlers) {
          handler({ type: "cancel-ack", sessionID: frame.sessionID, accepted: true })
        }
        this.finish.get(frame.sessionID)?.()
      },
      onMessage: (handler) => {
        this.ackHandlers.add(handler)
        return () => void this.ackHandlers.delete(handler)
      },
    }
  }

  get alive(): boolean {
    return true
  }

  execute(run: ExecutorRun): Promise<void> {
    return new Promise<void>((resolve) => {
      this.finish.set(run.sessionID, () => {
        this.results.set(run.sessionID, { sessionID: run.sessionID, value: this.value })
        resolve()
      })
    })
  }

  async cancel(sessionID: SessionID): Promise<void> {
    this.finish.get(sessionID)?.()
  }

  async assertNotBusy(sessionID: SessionID): Promise<boolean> {
    return this.finish.has(sessionID)
  }

  resultFor(sessionID: SessionID): SessionExecutionResult | undefined {
    return this.results.get(sessionID)
  }

  async terminate(): Promise<void> {}
}

class BlockingPool implements ExecutorWorkerPool {
  readonly workers: BlockingWorker[]
  readonly spawned: BlockingWorker[] = []
  private nextSpawnId = 100

  constructor(
    k: number,
    private readonly value: unknown,
    private readonly onCancelIdle?: (sessionID: SessionID) => void,
  ) {
    this.workers = Array.from({ length: k }, (_, index) => new BlockingWorker(index + 1, this.value, onCancelIdle))
  }

  spawn(): ExecutorSessionWorker {
    const worker = new BlockingWorker(this.nextSpawnId++, this.value, this.onCancelIdle)
    this.spawned.push(worker)
    return worker
  }

  async terminateAll(): Promise<void> {}

  byID(id: number): ExecutorWorker | undefined {
    return [...this.workers, ...this.spawned].find((worker) => worker.id === id)
  }

  resultFor(sessionID: SessionID): SessionExecutionResult | undefined {
    for (const worker of [...this.workers, ...this.spawned]) {
      const result = worker.resultFor(sessionID)
      if (result !== undefined) return result
    }
    return undefined
  }
}

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

describe("12 — integrated flag-on cancel (outcome + idle + session.cancelled)", () => {
  it.live("one flag-on cancel observes the hub outcome, exactly one idle, and session.cancelled", () => {
    const sessionID = sid("ses_integrated")
    const flagOnCalls: StatusCall[] = []
    const flagOffCalls: StatusCall[] = []
    // Phase 4: the executor cancel reaches the worker-local run-state (table B),
    // whose `onIdle` publishes the terminal idle through the hub-proxy. The pool
    // simulates that emit; the hub-side `SessionRunState.cancel` must not add a
    // second one.
    const pool = new BlockingPool(1, withParts(sessionID), (id) =>
      flagOnCalls.push({ sessionID: id, status: { type: "idle" } }),
    )
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
    const pool = new BlockingPool(1, withParts(sessionID))
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
// Phase 4 consolidation — exactly ONE idle per flag-ON cancel, in every case.
//
// The executor cancel now routes into the worker-local run-state (table B),
// whose `onIdle` publishes `session.status: idle` through the hub-proxy. The
// hub-side `SessionRunState.cancel` therefore emits the fallback idle ONLY when
// no worker could have published it: a queued session, an idle no-op, or a
// grace-expired terminate. These three cases pin that contract, each against
// flag-off.
// ---------------------------------------------------------------------------

describe("12 — exactly one flag-on cancel idle per case (ON ≡ OFF)", () => {
  /** Whether the hub currently hosts `sessionID` on a running worker. */
  const runningState = (hub: SessionHub, sessionID: SessionID) => (): true | undefined =>
    hub.dispatcher.registry.get(sessionID)?.state === "running" ? true : undefined

  it.live("busy/dispatched: the worker publishes the idle, the hub does not double-emit", () => {
    const sessionID = sid("ses_busy_one")
    const onCalls: StatusCall[] = []
    const offCalls: StatusCall[] = []
    // The pool simulates table B publishing idle through the hub-proxy on cancel.
    const pool = new BlockingPool(1, withParts(sessionID), (id) =>
      onCalls.push({ sessionID: id, status: { type: "idle" } }),
    )
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

  it.live("queued-but-not-dispatched: the hub publishes the single idle, matching flag-off", () => {
    const holderID = sid("ses_holder")
    const queuedID = sid("ses_queued")
    const onCalls: StatusCall[] = []
    const offCalls: StatusCall[] = []
    // One live worker holds `holderID`; `queuedID` is admitted with no worker.
    const pool = new BlockingPool(1, withParts(holderID))
    const hub = createSessionHub({
      pool,
      cancellation: () => ({ portFor: (worker) => pool.byID(worker.id)?.cancelPort, graceMs: 5_000 }),
    })

    const flagOn = withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        const holder = yield* state
          .ensureRunning(holderID, Effect.succeed(withParts(holderID)), Effect.succeed(withParts(holderID)))
          .pipe(Effect.exit, Effect.forkChild)
        yield* pollWithTimeout(Effect.sync(runningState(hub, holderID)), "holder session never reached running")
        const queued = yield* state
          .ensureRunning(queuedID, Effect.succeed(withParts(queuedID)), Effect.succeed(withParts(queuedID)))
          .pipe(Effect.exit, Effect.forkChild)
        yield* pollWithTimeout(
          Effect.sync(() => (hub.dispatcher.queuedCount === 1 ? true : undefined)),
          "session was never queued",
        )
        const outcome = yield* state.cancel(queuedID)
        yield* Fiber.join(queued)
        // Release the holder so the scope can close.
        yield* state.cancel(holderID)
        yield* Fiber.join(holder)
        return outcome
      }).pipe(
        Effect.provide(recordingRunStateLayer(onCalls)),
        Effect.provideService(SessionHubService, hub),
        Effect.provide(flagsOn),
      ),
    )

    const flagOff = withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        yield* state.cancel(queuedID)
      }).pipe(Effect.provide(recordingRunStateLayer(offCalls))),
    )

    return Effect.all([flagOn, flagOff]).pipe(
      Effect.map(([outcome]) => {
        // No worker ran the queued session, so the hub-side is the only emitter.
        expect(outcome).toMatchObject({
          sessionID: queuedID,
          mode: "terminated",
          winner: "cancel",
          acknowledged: false,
        })
        expect(idleOf(onCalls)).toEqual([{ sessionID: queuedID, status: { type: "idle" } }])
        expect(idleOf(offCalls)).toEqual(idleOf(onCalls))
      }),
    )
  })

  it.live("idle/no-op: the hub publishes the single idle, matching flag-off", () => {
    const sessionID = sid("ses_idle_one")
    const onCalls: StatusCall[] = []
    const offCalls: StatusCall[] = []
    const pool = new BlockingPool(1, withParts(sessionID))
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
