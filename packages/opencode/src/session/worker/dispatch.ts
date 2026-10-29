// v2 per-session session worker dispatch core.
//
// The hub-side scheduler is now unbounded and per-session: every session gets
// ONE lazily-materialized worker on its first dispatch, reused for every later
// turn until it is idle-reaped or terminal-cancelled. There is no admission
// queue, no headroom reserve and no software cap — the OS (OOM) is the only
// bound. A session still has one active run; a different run dispatched while
// that run is in flight parks in a per-session (unbounded) waiter list.
//
// The scheduler depends on the `WorkerPool` seam (not on Bun workers directly),
// so scheduling stays deterministic in tests. The production adapter lives in
// `hub.ts` (`createProductionWorkerPool`).

import { Cause, Deferred, Effect, Exit, Fiber, Ref, Schema, SynchronizedRef } from "effect"
import type { SessionID } from "@/session/schema"
import { isWorkerCrashPhase, workerCrashed, type WorkerCrashPhase, type WorkerCrashed } from "@/session/ledger/crash"
import type { AutoResumeDecision } from "@/session/ledger/resume"
import type { PromptInput, ShellInput } from "@/session/prompt"
import { defined } from "@/util/object"
import { createSessionWorkerRegistry, type SessionWorkerRegistry } from "./session-registry"
import { defaultSchedule, errorTagOf, messageOf } from "./port"
import { resolveReapMs } from "./pool"
import { noopWorkerLogger, workerLogMessage, type WorkerLogger } from "./log"
import { SessionWorkerReplyTimeoutError } from "./execute"
import type { ExecutorRun, ExecutorSessionWorker, SessionRunMode } from "./execute"
import {
  createCancelController,
  createOutcomeGate,
  type CancelController,
  type CancelOutcome,
  type CancelPort,
  type CancelScheduler,
  type Outcome,
  type OutcomeGate,
} from "./cancel"

/** Default deadline for an asynchronous crash resume decision. */
const DEFAULT_RESUME_DECISION_TIMEOUT_MS = 5_000

/** Default deadline for a retiring generation's confirmed exit. */
const DEFAULT_TERMINATE_TIMEOUT_MS = 5_000

/** One session run: the key the worker executes, its subagent depth and its frame mode. */
export type SessionRun = ExecutorRun

/** A worker that executes exactly one session run at a time. */
export type SessionWorker = ExecutorSessionWorker

/** The scheduling seam: materializes one worker per session. */
export interface WorkerPool {
  /**
   * Create one fresh worker. The dispatcher passes the registry's monotonic
   * worker id so the session map and the isolate agree; a pool that owns its own
   * ids may ignore it.
   */
  spawn(id?: number): SessionWorker
  /**
   * Release a retired worker: drop the pool's entry for `workerID` (when given)
   * and any per-session pool state. Called once per retirement (reap/cancel/crash).
   */
  releaseSession?(sessionID: SessionID, workerID?: number): void
  terminateAll(): Promise<void>
}

/**
 * Raised when a session has failed because its worker crashed. A crashed run
 * rejects with this and is never requeued; the session's worker is retired and
 * the next dispatch lazily spawns a fresh one.
 */
export class WorkerCrashedError extends Schema.TaggedErrorClass<WorkerCrashedError>()("WorkerCrashedError", {
  message: Schema.String,
  workerID: Schema.Number,
  /** The crash phase from the D8 payload, when the isolate reported one. */
  sessionID: Schema.optional(Schema.String),
  phase: Schema.optional(Schema.String),
  lastCommittedMessageID: Schema.optional(Schema.NullOr(Schema.String)),
  inFlightCallIDs: Schema.optional(Schema.Array(Schema.String)),
  reason: Schema.optional(Schema.String),
  /** The underlying isolate failure, when one was captured. */
  cause: Schema.optional(Schema.Defect()),
}) {
  /** A crash is never retried on the same run. */
  readonly retryable = false as const

  /** The typed D8 payload, present when the crash carried full context. */
  get payload(): WorkerCrashed | undefined {
    if (this.sessionID === undefined || !isWorkerCrashPhase(this.phase)) return undefined
    return workerCrashed({
      sessionID: this.sessionID,
      workerID: this.workerID,
      phase: this.phase,
      lastCommittedMessageID: this.lastCommittedMessageID ?? null,
      inFlightCallIDs: this.inFlightCallIDs ?? [],
      reason: this.reason ?? this.message,
    })
  }
}

/** The underlying failure's text, embedded in a crash error so its cause is never lost. */
function describeFailure(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

/** Raised to the caller when a session is cancelled. */
export class SessionCancelledError extends Schema.TaggedErrorClass<SessionCancelledError>()("SessionCancelledError", {
  message: Schema.String,
  sessionID: Schema.String,
}) {}

/**
 * A child whose parent worker crashed. It is a cancellation (never a resume) and
 * remains a `SessionCancelledError` so existing cancellation callers keep working.
 */
export class ParentCrashedError extends SessionCancelledError {
  readonly reason = "parent-crashed" as const
  constructor(
    readonly parentSessionID: SessionID,
    readonly childSessionID: SessionID,
  ) {
    super({ message: `parent session ${parentSessionID} crashed; child ${childSessionID} was cancelled`, sessionID: childSessionID })
    this.name = "ParentCrashedError"
  }
}

/** Every terminal mode a cancelled run can report. There is no admission queue. */
export const CANCELLED_MODES = ["cooperative", "terminated", "completed", "boot"] as const

/** The terminal mode reported when a session is cancelled. */
export type CancelledMode = (typeof CANCELLED_MODES)[number]

/** One session visited by a cascade, with the mode it settled in (if it had a run). */
export interface CascadeDescendant {
  readonly sessionID: SessionID
  readonly mode: CancelledMode | undefined
}

/**
 * The ordered settlement of a cancel request and its subagent tree.
 *
 * The report is the root's own outcome (so existing `CancelOutcome` consumers
 * keep working) plus a depth-first, children-before-parent list of every
 * descendant visited exactly once. A linked descendant that was never admitted
 * is still reported, with `mode: undefined`.
 */
export interface CascadeReport extends CancelOutcome {
  readonly sessionID: SessionID
  readonly outcome: CancelOutcome
  readonly descendants: ReadonlyArray<CascadeDescendant>
}

/** Emitted once when a session reaches its terminal cancelled state. */
export interface CancelledEvent {
  readonly sessionID: SessionID
  readonly mode: CancelledMode
  readonly workerID: number | undefined
  readonly resumeFromDb: true
}

/** Additive cancellation wiring. */
export interface CancellationConfig {
  readonly portFor?: (worker: SessionWorker) => CancelPort | undefined
  readonly graceMs?: number
  readonly schedule?: CancelScheduler
  readonly reap?: (worker: SessionWorker) => void
  readonly reapAll?: () => void
  readonly childrenOf?: (sessionID: SessionID) => ReadonlyArray<SessionID>
  readonly onCancelled?: (event: CancelledEvent) => void
}

export type Scheduler = (delayMs: number, run: () => void) => () => void

export interface DispatchRequest {
  readonly sessionID: SessionID
  /** 0 = root; ≥1 = subagent child. Carried on the run; no scheduling split. */
  readonly depth?: number
  readonly mode?: SessionRunMode
  readonly prompt?: PromptInput
  readonly shell?: ShellInput
  /**
   * The model this run was requested with (the prompt's model). A session worker
   * is bound to one model, so a different request selects a new generation even
   * when the persisted session row still names the previous model.
   */
  readonly model?: { readonly providerID: string; readonly modelID: string }
}

export interface DispatcherOptions {
  readonly pool: WorkerPool
  readonly schedule?: Scheduler
  /** Idle-reap window; defaults to `OPENCODE_SESSION_WORKER_REAP_MS` (300000). */
  readonly reapMs?: number
  readonly cancellation?: CancellationConfig
  readonly log?: WorkerLogger
  /**
   * Per-session write barrier: a completed run's worker is not released for its
   * next run until the barrier resolves, so the next run never overtakes the
   * prior run's durable egress write.
   */
  readonly writeBarrier?: (sessionID: SessionID) => Promise<void> | undefined
  /**
   * Refuse a run whose bundle can no longer be admitted (e.g. a compiled
   * `BuildMismatch`). Checked before every waiter promotion, not only at hub
   * entry: a run that parks behind a crash must not respawn the stale bundle.
   */
  readonly assertAdmissible?: (sessionID: SessionID) => void
  /** Hub-side cleanup when a session's worker is retired (reap/cancel/crash). */
  readonly onSessionReleased?: (sessionID: SessionID, workerID?: number) => unknown
  /** Invoked synchronously when a run actually starts (not when it parks). */
  readonly onRunStart?: (sessionID: SessionID) => void
  /** Persist the D8 payload before the crash is returned to the caller. */
  readonly recordCrash?: (payload: WorkerCrashed) => void
  /** Ledger context for a crash whose isolate could not report it. */
  readonly crashState?: (sessionID: SessionID) => {
    readonly phase?: WorkerCrashPhase
    readonly lastCommittedMessageID: string | null
    readonly inFlightCallIDs: readonly string[]
  }
  /** Observe every typed crash payload after it is recorded. */
  readonly onCrash?: (payload: WorkerCrashed) => void
  /**
   * Apply the ledger/cap/cancel guard to a crash payload. A promise defers the
   * terminal rejection until the durable ledger read finishes, so callers never
   * observe a crash before its resume decision.
   */
  readonly resumeDecision?: (payload: WorkerCrashed) => AutoResumeDecision | Promise<AutoResumeDecision>
  /**
   * How long a crash waits for an asynchronous resume decision. An unresolved
   * decision must not pin the session's run ownership (a new dispatch would
   * attach to the dead run forever), so after this deadline the run settles with
   * no resume and the parked successor starts. Defaults to 5s; `0` disables it.
   */
  readonly resumeDecisionTimeoutMs?: number
  /**
   * How long a successor waits for a retiring generation's confirmed exit (and
   * its lease release) before proceeding. A terminate that never settles must not
   * park the session forever. Defaults to 5s.
   */
  readonly terminateTimeoutMs?: number
  /**
   * Observe an allowed auto-resume; re-dispatch stays with the hub owner. The
   * returned promise is the resumed run: the original crashed caller waits for
   * it instead of observing `WorkerCrashedError` while its replacement runs.
   */
  readonly onAutoResume?: (
    decision: Extract<AutoResumeDecision, { resume: true }>,
    payload: WorkerCrashed,
  ) => unknown
}

export interface Dispatcher extends AsyncDisposable {
  readonly busySlotCount: number
  readonly queuedCount: number
  readonly registry: SessionWorkerRegistry<SessionWorker>
  /** The worker currently hosting `sessionID`'s active run, or `undefined` when idle. */
  workerForSession(sessionID: SessionID): number | undefined
  dispatch(request: DispatchRequest): Promise<void>
  cancel(sessionID: SessionID): Promise<void>
  cancelWithGrace(sessionID: SessionID): Promise<CascadeReport | undefined>
  /**
   * Retire the session's generation: drop its registry entry, terminate the
   * isolate, and run the release callback/lease release. Used by the pool's idle
   * cap, which evicts a worker the dispatcher would otherwise keep idle.
   */
  retire(sessionID: SessionID): Promise<void>
  /**
   * Release barrier: resolves once this session's in-flight retirement has
   * confirmed the isolate exit and finished its `onSessionReleased` callback
   * (including the retired lease release). Resolves immediately when nothing is
   * retiring. Awaiting only the run settlement does not observe the release.
   */
  awaitRelease(sessionID: SessionID): Promise<void>
  terminateAll(): Promise<void>
  dispose(): Promise<void>
}

/** The mutable settlement phase of one run. Every field is written synchronously. */
interface PendingPhase {
  /** True once the run reached a terminal observation (completion/failure). */
  settled: boolean
  completed: boolean
  startedAt: number | undefined
  workerID: number | undefined
  cancelPending: boolean
}

interface Pending {
  readonly sessionID: SessionID
  readonly depth: number
  readonly mode: SessionRunMode
  readonly prompt: PromptInput | undefined
  readonly shell: ShellInput | undefined
  readonly model: { readonly providerID: string; readonly modelID: string } | undefined
  /** The caller's settlement cell; completed exactly once. */
  readonly done: Deferred.Deferred<void, unknown>
  /** Synchronous Promise adapter for `done`, settled by the same call. */
  readonly doneBridge: SettlementBridge<void>
  /** The cancellation controller's "the run completed" probe. */
  readonly completion: Deferred.Deferred<void>
  /** Synchronous Promise adapter for `completion`, settled by the same call. */
  readonly completionBridge: SettlementBridge<void>
  /** The caller's promise, bridged from `done` at admission. */
  readonly promise: Promise<void>
  /** The completion probe, bridged from `completion` at admission. */
  readonly completionPromise: Promise<void>
  readonly phase: Ref.Ref<PendingPhase>
  readonly gate: OutcomeGate
}

/**
 * A Promise viewer over a `Deferred`. `settle` runs only after the Deferred is
 * completed, and resolves/rejects the Promise in the same synchronous tick, so
 * the dispatcher's long-standing microtask ordering is preserved.
 */
interface SettlementBridge<A> {
  readonly promise: Promise<A>
  readonly settle: () => void
}

/**
 * The session ownership table. `runs` is the admitted run per session (the old
 * `byKey`), `waiters` is the unbounded parked-run list per session (the old
 * `waitersByKey`), and `terminated` is the shutdown latch. Every transition is
 * serialized through one `SynchronizedRef`, so a check and the state write it
 * guards can never be split by another ownership transition.
 */
interface OwnershipState {
  readonly runs: ReadonlyMap<SessionID, Pending>
  readonly waiters: ReadonlyMap<SessionID, ReadonlyArray<Pending>>
  readonly terminated: boolean
}

/** A new state with `pending` as the session's admitted run. */
const withRun = (state: OwnershipState, sessionID: SessionID, pending: Pending): OwnershipState => {
  const runs = new Map(state.runs)
  runs.set(sessionID, pending)
  return { ...state, runs }
}

/** A new state with no admitted run for the session (an absent run is a no-op). */
const withoutRun = (state: OwnershipState, sessionID: SessionID): OwnershipState => {
  if (!state.runs.has(sessionID)) return state
  const runs = new Map(state.runs)
  runs.delete(sessionID)
  return { ...state, runs }
}

/** A new state with exactly `waiters` parked for the session (empty removes it). */
const withWaiters = (state: OwnershipState, sessionID: SessionID, waiters: ReadonlyArray<Pending>): OwnershipState => {
  const next = new Map(state.waiters)
  if (waiters.length === 0) next.delete(sessionID)
  else next.set(sessionID, waiters)
  return { ...state, waiters: next }
}

/** A new state with the session's parked waiters removed. */
const withoutWaiters = (state: OwnershipState, sessionID: SessionID): OwnershipState => {
  if (!state.waiters.has(sessionID)) return state
  const next = new Map(state.waiters)
  next.delete(sessionID)
  return { ...state, waiters: next }
}

/** How an admitted run may begin after its build/shutdown preflight. */
type StartAdmission =
  | { readonly kind: "terminated" }
  | { readonly kind: "blocked"; readonly error: unknown }
  | { readonly kind: "open" }

/** What `nextWaiter` must do after atomically inspecting the session slot. */
type NextWaiterDecision =
  | { readonly kind: "owned" }
  | { readonly kind: "idle" }
  | { readonly kind: "blocked"; readonly error: unknown; readonly waiters: ReadonlyArray<Pending> }
  | { readonly kind: "promote"; readonly next: Pending; readonly remaining: number }

/** How `dispatch` admits, joins, or parks a request. */
type DispatchDecision =
  | { readonly kind: "reject" }
  | { readonly kind: "join"; readonly promise: Promise<void> }
  | { readonly kind: "park"; readonly pending: Pending }
  | { readonly kind: "admit"; readonly pending: Pending }

/** How a failed run's ownership and gate state resolve. */
type FailOwnershipDecision =
  | { readonly kind: "superseded" }
  | { readonly kind: "gate-cancel" }
  | { readonly kind: "owned" }

const phaseOf = (pending: Pending): PendingPhase => Ref.getUnsafe(pending.phase)

const updatePhase = (pending: Pending, update: (phase: PendingPhase) => PendingPhase): void => {
  Effect.runSync(Ref.update(pending.phase, update))
}

/** Bridge a `Deferred` to the synchronous Promise façade at the adapter boundary. */
const bridgeDeferred = <A, E>(deferred: Deferred.Deferred<A, E>): SettlementBridge<A> => {
  let deliver!: () => void
  const promise = new Promise<A>((resolve, reject) => {
    deliver = () => {
      const exit = Effect.runSyncExit(Deferred.await(deferred))
      if (Exit.isSuccess(exit)) resolve(exit.value)
      else reject(Cause.squash(exit.cause))
    }
  })
  return { promise, settle: deliver }
}

/** Complete the caller's run exactly once; a later settlement is ignored. */
const resolveRun = (pending: Pending): void => {
  if (Deferred.doneUnsafe(pending.done, Effect.void)) pending.doneBridge.settle()
}

/** Fail the caller's run exactly once; a later settlement is ignored. */
const rejectRun = (pending: Pending, error: unknown): void => {
  if (Deferred.doneUnsafe(pending.done, Effect.fail(error))) pending.doneBridge.settle()
}

/** Record the terminal observation once and wake the cancellation controller. */
const markSettled = (pending: Pending): void => {
  updatePhase(pending, (phase) => (phase.settled ? phase : { ...phase, settled: true }))
  if (Deferred.doneUnsafe(pending.completion, Effect.void)) pending.completionBridge.settle()
}

function createPending(
  sessionID: SessionID,
  depth: number,
  mode: SessionRunMode,
  prompt: PromptInput | undefined,
  shell: ShellInput | undefined,
  model: { readonly providerID: string; readonly modelID: string } | undefined,
): Pending {
  const done = Deferred.makeUnsafe<void, unknown>()
  const doneBridge = bridgeDeferred(done)
  const completion = Deferred.makeUnsafe<void>()
  const completionBridge = bridgeDeferred(completion)
  return {
    sessionID,
    depth,
    mode,
    prompt,
    shell,
    model,
    done,
    doneBridge,
    completion,
    completionBridge,
    promise: doneBridge.promise,
    completionPromise: completionBridge.promise,
    phase: Ref.makeUnsafe<PendingPhase>({
      settled: false,
      completed: false,
      startedAt: undefined,
      workerID: undefined,
      cancelPending: false,
    }),
    gate: createOutcomeGate(),
  }
}

export function createDispatcher(options: DispatcherOptions): Dispatcher {
  const pool = options.pool
  const cancellation = options.cancellation
  const log = options.log ?? noopWorkerLogger
  const writeBarrier = options.writeBarrier
  const schedule = options.schedule ?? defaultSchedule
  const reapMs = options.reapMs ?? resolveReapMs()
  const resumeDecisionTimeoutMs = options.resumeDecisionTimeoutMs ?? DEFAULT_RESUME_DECISION_TIMEOUT_MS
  const terminateTimeoutMs = options.terminateTimeoutMs ?? DEFAULT_TERMINATE_TIMEOUT_MS

  // The session→worker map. Workers outlive runs (reuse across turns) until
  // idle-reap or terminal cancel disposes them.
  const workers = createSessionWorkerRegistry<SessionWorker>()
  // Run ownership, parked waiters, and the shutdown latch are one atomic state.
  const ownership = SynchronizedRef.makeUnsafe<OwnershipState>({
    runs: new Map(),
    waiters: new Map(),
    terminated: false,
  })
  /**
   * One armed reap timer per idle session. The fiber owns the delay and the
   * probe, so clearing it interrupts both instead of relying on the host timer
   * cancel to withdraw an already-queued callback.
   */
  const idleTimers = new Map<SessionID, Fiber.Fiber<void>>()
  const controllers = new Map<number, CancelController>()
  /**
   * The confirmed-exit signal for a retiring generation. A successor must not
   * start until the old isolate is gone and its lease release has landed.
   */
  const terminating = new Map<SessionID, Deferred.Deferred<void>>()

  /** The current ownership state, read without acquiring the transition lock. */
  const stateOf = (): OwnershipState => SynchronizedRef.getUnsafe(ownership)

  /**
   * Apply one pure check-then-act transition atomically. The transition body must
   * be synchronous and must never transition ownership again, so the effect can
   * be run to completion inside the SynchronizedRef permit.
   */
  const ownershipTransition = Effect.fnUntraced(function* <A>(
    update: (state: OwnershipState) => readonly [A, OwnershipState],
  ) {
    return yield* SynchronizedRef.modifyEffect(ownership, (state) => Effect.sync(() => update(state)))
  })

  const atomically = <A>(update: (state: OwnershipState) => readonly [A, OwnershipState]): A =>
    Effect.runSync(ownershipTransition(update))

  const cancelled = (sessionID: SessionID): SessionCancelledError =>
    new SessionCancelledError({ message: `session ${sessionID} was cancelled`, sessionID })

  const clearIdle = (sessionID: SessionID): void => {
    const timer = idleTimers.get(sessionID)
    if (timer === undefined) return
    idleTimers.delete(sessionID)
    // Interrupting the fiber also runs the scheduler cancel through the
    // callback's cleanup, so a callback the host already queued cannot reap.
    Effect.runFork(Fiber.interrupt(timer))
  }

  const hasWaiters = (sessionID: SessionID): boolean => (stateOf().waiters.get(sessionID)?.length ?? 0) > 0

  const hasRun = (sessionID: SessionID): boolean => stateOf().runs.has(sessionID)

  /**
   * The typed refusal when the session's build is no longer admissible (or
   * `undefined` when it is). A defect in the guard itself must not admit work.
   */
  const inadmissible = (sessionID: SessionID): unknown | undefined => {
    if (options.assertAdmissible === undefined) return undefined
    try {
      options.assertAdmissible(sessionID)
      return undefined
    } catch (error) {
      return error
    }
  }

  /**
   * Whether a worker still owns work that must outlive an idle turn. The probe
   * is optional on the seam; a worker that cannot answer (or fails) is treated
   * as having none, so a dead isolate is never pinned forever. Background work
   * is deliberately NOT part of `runs`/waiters: it must not make a new prompt
   * fail `assertNotBusy`.
   */
  const hasActiveWork = async (worker: SessionWorker, sessionID: SessionID): Promise<boolean> => {
    if (worker.hasActiveWork === undefined) return false
    try {
      return await worker.hasActiveWork(sessionID)
    } catch (error) {
      log(workerLogMessage("background probe failed"), {
        level: "warn",
        sessionID,
        workerID: worker.id,
        reason: messageOf(error),
      })
      return false
    }
  }

  /**
   * Reap one idle session after the window. The background-work probe crosses
   * the worker protocol, so every guard is re-checked after the await: a new run
   * or parked waiter must win, and a retired/replaced worker must not be touched
   * by a stale timer.
   */
  const reapIfIdle = (sessionID: SessionID): Effect.Effect<void> =>
    Effect.gen(function* () {
      const timerID = yield* Effect.fiberId
      // A callback the host queued before `clearIdle` interrupted its fiber must
      // be inert: only the timer still installed for this session may reap.
      if (idleTimers.get(sessionID)?.id !== timerID) return
      idleTimers.delete(sessionID)
      if (stateOf().terminated || hasRun(sessionID) || hasWaiters(sessionID)) return
      const worker = workers.worker(sessionID)
      if (worker === undefined) return
      const active = yield* Effect.promise(() => hasActiveWork(worker, sessionID))
      // Re-check after the await: a new run or parked waiter must win, and a
      // retired/replaced worker must not be touched by this stale timer.
      if (stateOf().terminated || hasRun(sessionID) || hasWaiters(sessionID)) return
      if (workers.worker(sessionID) !== worker) return
      // A newer idle window may have been armed while the probe was pending.
      const newerTimer = idleTimers.get(sessionID)
      if (newerTimer !== undefined && newerTimer.id !== timerID) return
      if (active) {
        // A running background job still owns this isolate; arm a fresh window
        // instead of retiring it. Its own completion is observed by the next probe.
        scheduleReap(sessionID)
        return
      }
      retireWorker(sessionID)
      log(workerLogMessage("session worker reaped"), {
        level: "INFO",
        sessionID,
        workerID: worker.id,
        idleMs: reapMs,
      })
    })

  /**
   * The fenced delay for one reap. The scheduler seam stays callback-based, but
   * its cancel runs as the callback's cleanup when the fiber is interrupted.
   */
  const reapAfterDelay = (sessionID: SessionID): Effect.Effect<void> =>
    Effect.gen(function* () {
      yield* Effect.callback<void>((resume) => {
        const cancel = schedule(reapMs, () => resume(Effect.void))
        return Effect.sync(cancel)
      })
      yield* reapIfIdle(sessionID)
    })

  const scheduleReap = (sessionID: SessionID): void => {
    clearIdle(sessionID)
    if (stateOf().terminated || workers.worker(sessionID) === undefined) return
    idleTimers.set(sessionID, Effect.runFork(reapAfterDelay(sessionID)))
  }

  const disposeController = (workerID: number): void => {
    const controller = controllers.get(workerID)
    if (controller === undefined) return
    controller.dispose()
    controllers.delete(workerID)
  }

  /**
   * Terminal worker retirement: drop the session→worker entry, dispose its
   * cancel controller and terminate the isolate. Idempotent, so a crash that
   * already killed the worker is harmless.
   */
  const retireWorker = (
    sessionID: SessionID,
    retireOptions: { readonly keepController?: boolean } = {},
  ): SessionWorker | undefined => {
    clearIdle(sessionID)
    const worker = workers.dispose(sessionID)
    if (worker === undefined) return undefined
    if (retireOptions.keepController !== true) disposeController(worker.id)
    // Retirement is reported only after the isolate has actually exited. A
    // still-live old generation could otherwise write after its lease was
    // zeroed and re-stamp the expiry, starving the successor. The callback may
    // be async (the hub's lease release is an executor round-trip); await it in
    // the same chain so a successor waits for the release too.
    const exit = Deferred.makeUnsafe<void>()
    terminating.set(sessionID, exit)
    Effect.runFork(
      Effect.gen(function* () {
        // Retirement is reported only after the isolate has actually exited.
        yield* Effect.promise(() => worker.terminate().catch(() => undefined))
        // The callback may be async (the hub's lease release is an executor
        // round-trip); await it before signalling the successor.
        yield* Effect.promise(async () => {
          try {
            pool.releaseSession?.(sessionID, worker.id)
            await options.onSessionReleased?.(sessionID, worker.id)
          } catch (error) {
            // A callback that throws must not leave the exit signal failed (a
            // successor may be waiting) nor park it: log and treat it as settled.
            log(workerLogMessage("session release callback failed"), {
              level: "WARN",
              sessionID,
              workerID: worker.id,
              reason: messageOf(error),
            })
          }
        })
      }).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            if (terminating.get(sessionID) === exit) terminating.delete(sessionID)
            // Exactly once, even if an unexpected defect escaped the release.
            Deferred.doneUnsafe(exit, Effect.void)
          }),
        ),
      ),
    )
    return worker
  }

  const controllerFor = (config: CancellationConfig, worker: SessionWorker, port: CancelPort): CancelController => {
    const existing = controllers.get(worker.id)
    if (existing !== undefined) return existing
    const controller = createCancelController({
      port,
      terminate: () => worker.terminate().then(() => undefined),
      ...defined({ graceMs: config.graceMs, schedule: config.schedule }),
      reap: () => config.reap?.(worker),
      log,
      workerID: worker.id,
    })
    controllers.set(worker.id, controller)
    return controller
  }

  /**
   * A worker kept idle for reuse can die between turns (the isolate exits on its
   * own or its process is reaped externally). Reusing its handle would run the
   * next turn on a dead isolate, so a dead idle worker is retired and its handle
   * closed before the registry materializes a fresh one.
   */
  const replaceDeadIdleWorker = (sessionID: SessionID): void => {
    const entry = workers.entry(sessionID)
    if (entry === undefined || entry.state !== "idle" || entry.worker.alive) return
    log(workerLogMessage("dead idle worker replaced"), {
      level: "WARN",
      sessionID,
      workerID: entry.id,
    })
    retireWorker(sessionID)
  }

  /**
   * Wait for a retiring generation's exit, but never forever: a rejected or
   * wedged terminate settles at the deadline so the successor can start instead
   * of parking the session. A rejection is logged and treated as settled.
   */
  const awaitTerminating = (sessionID: SessionID, exit: Deferred.Deferred<void>): Effect.Effect<void> =>
    Deferred.await(exit).pipe(
      Effect.catchCause((cause) =>
        Effect.sync(() => {
          log(workerLogMessage("retirement exit failed"), {
            level: "WARN",
            sessionID,
            reason: messageOf(Cause.squash(cause)),
          })
        }),
      ),
      Effect.timeoutOrElse({
        duration: terminateTimeoutMs,
        orElse: () =>
          Effect.sync(() => {
            log(workerLogMessage("retirement exit timed out"), {
              level: "WARN",
              sessionID,
              timeoutMs: terminateTimeoutMs,
              reason: "the retiring generation never confirmed its exit; starting the successor",
            })
          }),
      }),
    )

  /**
   * Park a run until the previous generation's confirmed exit (or the bounded
   * deadline). The same helper is used both for an already-retiring generation
   * and for the retirement a dead-idle replacement just started, so a successor
   * can never materialize against a live old isolate.
   */
  const deferUntilRetired = (pending: Pending, exit: Deferred.Deferred<void>): void => {
    Effect.runFork(
      Effect.gen(function* () {
        // A successor must re-observe ownership after this turn, never re-enter
        // the caller's stack synchronously when the exit signal is already set.
        yield* Effect.yieldNow
        yield* awaitTerminating(pending.sessionID, exit)
        if (terminating.get(pending.sessionID) === exit) terminating.delete(pending.sessionID)
        if (stateOf().runs.get(pending.sessionID) !== pending) return
        if (stateOf().terminated) {
          pending.gate.claim("cancel")
          rejectRun(pending, cancelled(pending.sessionID))
          return
        }
        start(pending)
      }),
    )
  }

  /** The Promise façade for a retirement's confirmed-exit signal. */
  const retirementBarrier = (sessionID: SessionID): Promise<void> => {
    const exit = terminating.get(sessionID)
    return exit === undefined ? Promise.resolve() : Effect.runPromise(Deferred.await(exit))
  }

  const retire = (sessionID: SessionID): Promise<void> => {
    retireWorker(sessionID)
    return retirementBarrier(sessionID)
  }

  /** The explicit release barrier for a retirement that is already in flight. */
  const awaitRelease = (sessionID: SessionID): Promise<void> => retirementBarrier(sessionID)

  const start = (pending: Pending): void => {
    // The build guard is user code: run it before taking the ownership permit,
    // so a guard that re-enters dispatch/terminateAll cannot find the lock held.
    // Shutdown still wins because the transition below re-checks the latch.
    const blocked = stateOf().terminated ? undefined : inadmissible(pending.sessionID)
    const admission = atomically<StartAdmission>((state) => {
      if (state.terminated) return [{ kind: "terminated" }, state]
      // A build that became inadmissible while this run waited (a crash blocked
      // the bundle) must reject here too, before any isolate is materialized.
      if (blocked === undefined) return [{ kind: "open" }, state]
      const owns = state.runs.get(pending.sessionID) === pending
      return [{ kind: "blocked", error: blocked }, owns ? withoutRun(state, pending.sessionID) : state]
    })
    if (admission.kind === "terminated") {
      pending.gate.claim("cancel")
      rejectRun(pending, cancelled(pending.sessionID))
      return
    }
    if (admission.kind === "blocked") {
      if (pending.gate.claim("cancel")) rejectRun(pending, admission.error)
      nextWaiter(pending.sessionID)
      return
    }
    const exiting = terminating.get(pending.sessionID)
    if (exiting !== undefined) {
      deferUntilRetired(pending, exiting)
      return
    }
    clearIdle(pending.sessionID)
    replaceDeadIdleWorker(pending.sessionID)
    // `replaceDeadIdleWorker` may have just started an orderly retirement. The
    // check above happened before it ran, so re-check before spawning: the
    // successor must wait for the old close and lease release.
    const replaced = terminating.get(pending.sessionID)
    if (replaced !== undefined) {
      deferUntilRetired(pending, replaced)
      return
    }
    options.onRunStart?.(pending.sessionID)
    const worker = workers.getOrCreate(pending.sessionID, (id) => pool.spawn(id))
    updatePhase(pending, (phase) => ({ ...phase, startedAt: Date.now(), workerID: worker.id }))
    workers.markRunning(pending.sessionID)
    log(workerLogMessage("session admitted"), {
      level: "INFO",
      sessionID: pending.sessionID,
      workerID: worker.id,
      depth: pending.depth,
      queueDepth: stateOf().waiters.get(pending.sessionID)?.length ?? 0,
    })
    const run: SessionRun = {
      sessionID: pending.sessionID,
      depth: pending.depth,
      ...(pending.mode === "startShell" ? { mode: pending.mode } : {}),
      ...(pending.prompt === undefined ? {} : { prompt: pending.prompt }),
      ...(pending.shell === undefined ? {} : { shell: pending.shell }),
      ...(pending.model === undefined ? {} : { model: pending.model }),
    }
    let execution: Promise<void>
    try {
      execution = worker.execute(run)
    } catch (error) {
      execution = Promise.reject(error)
    }
    execution.then(
      () => complete(pending),
      (error) => fail(pending, error),
    )
  }

  /** Release a completed run once the per-session write barrier (if any) resolves. */
  const complete = (pending: Pending): void => {
    updatePhase(pending, (phase) => ({ ...phase, completed: true }))
    markSettled(pending)
    // A run whose cancellation owns the terminal decision, or whose session a
    // successor now owns, must not touch the successor's write barrier.
    const { cancelPending } = phaseOf(pending)
    const barrier = cancelPending || !ownsSession(pending) ? undefined : writeBarrier?.(pending.sessionID)
    if (barrier === undefined) {
      finalizeCompletion(pending)
      return
    }
    void barrier.then(
      () => finalizeCompletion(pending),
      () => finalizeCompletion(pending),
    )
  }

  /**
   * Whether a run still owns its session. A cancelled or superseded run may
   * settle its worker protocol arbitrarily late; only the run `runs` points at
   * may mutate the session maps, reservations, waiters, or worker.
   */
  const ownsSession = (pending: Pending): boolean => stateOf().runs.get(pending.sessionID) === pending

  /**
   * Atomically drop `pending` from run ownership. Returns false when a different
   * run (or no run) owns the session, so a stale settlement can never delete a
   * successor's entry.
   */
  const removeOwnedRun = (pending: Pending): boolean =>
    atomically((state) => {
      if (state.runs.get(pending.sessionID) !== pending) return [false, state] as const
      return [true, withoutRun(state, pending.sessionID)] as const
    })

  /**
   * Settle a superseded run's own promise without touching shared session state.
   * Its gate is claimed first so the stale run never hangs: if a cancel already
   * claimed it the promise is settled, otherwise it is settled as cancelled.
   */
  const settleSuperseded = (pending: Pending): void => {
    if (pending.gate.claim("cancel")) rejectRun(pending, cancelled(pending.sessionID))
  }

  const finalizeCompletion = (pending: Pending): void => {
    // A cancel owns this run's terminal decision: keep the run visible in `runs`
    // so a prompt arriving mid-grace parks instead of starting on the worker the
    // cancel is about to retire. `cancelWithGrace` releases the exact run.
    if (phaseOf(pending).cancelPending) return
    // A newer run owns the session (cancel + immediate re-dispatch): settling this
    // stale run must not release the successor's reservation, delete its run
    // entry, retire its worker, or promote its waiters.
    const released = removeOwnedRun(pending)
    if (!released) {
      settleSuperseded(pending)
      return
    }
    workers.release(pending.sessionID)
    const { startedAt, workerID } = phaseOf(pending)
    log(workerLogMessage("session completed"), {
      level: "INFO",
      sessionID: pending.sessionID,
      workerID,
      durationMs: startedAt === undefined ? 0 : Date.now() - startedAt,
    })
    if (pending.gate.claim("completion")) resolveRun(pending)
    else rejectRun(pending, cancelled(pending.sessionID))
    nextWaiter(pending.sessionID)
  }

  /** The typed D8 payload for one crash, with the error's own context winning. */
  const crashPayloadFor = (pending: Pending, error: unknown): WorkerCrashed => {
    const reported = error instanceof WorkerCrashedError ? error : undefined
    const state = options.crashState?.(pending.sessionID)
    const phase = isWorkerCrashPhase(reported?.phase) ? reported.phase : (state?.phase ?? "stream")
    const payload = workerCrashed({
      sessionID: pending.sessionID,
      workerID: reported?.workerID ?? workers.worker(pending.sessionID)?.id ?? phaseOf(pending).workerID ?? -1,
      phase,
      lastCommittedMessageID: reported?.lastCommittedMessageID ?? state?.lastCommittedMessageID ?? null,
      inFlightCallIDs: reported?.inFlightCallIDs ?? state?.inFlightCallIDs ?? [],
      reason: reported?.reason ?? describeFailure(error),
    })
    // Carry the typed underlying failure (e.g. `BuildMismatch`) on the in-memory
    // payload without changing its observable/persisted shape. The resume
    // decision reads it to return the typed cause as-is instead of re-classifying
    // a string reason.
    const cause = reported?.cause ?? error
    if (cause instanceof Error) {
      Object.defineProperty(payload, "cause", { value: cause, enumerable: false, configurable: false })
    }
    return payload
  }

  /**
   * A parent crash cancels its subagent children immediately: the dead parent can
   * never resume them, so each child rejects with `ParentCrashedError`. Recursion
   * uses one visited set, matching the explicit-cancel cascade.
   */
  function crashChildren(parentSessionID: SessionID, seen: Set<SessionID> = new Set()): void {
    if (seen.has(parentSessionID)) return
    seen.add(parentSessionID)
    for (const childSessionID of cancellation?.childrenOf?.(parentSessionID) ?? []) {
      if (seen.has(childSessionID)) continue
      const failure = new ParentCrashedError(parentSessionID, childSessionID)
      rejectWaiters(childSessionID, failure)
      const owned = atomically((state) => {
        const run = state.runs.get(childSessionID)
        if (run === undefined) return [undefined, state] as const
        return [run, withoutRun(state, childSessionID)] as const
      })
      const worker = workers.worker(childSessionID)
      if (owned !== undefined) {
        owned.gate.claim("cancel")
        rejectRun(owned, failure)
        workers.release(childSessionID)
      }
      if (worker !== undefined) {
        cancellation?.reap?.(worker)
        retireWorker(childSessionID)
        cancellation?.onCancelled?.({
          sessionID: childSessionID,
          mode: "terminated",
          workerID: worker.id,
          resumeFromDb: true,
        })
      }
      crashChildren(childSessionID, seen)
    }
  }

  /**
   * Settle one crash after its resume decision is known. The decision is part of
   * the crash outcome: a durable ledger read must finish before the run rejects,
   * otherwise a caller could re-dispatch and race the decision.
   *
   * While that read is pending the run keeps run/worker ownership, so a
   * dispatch parks instead of starting a concurrent run. If a cancel (or a
   * partial settlement) took the session over in the meantime, this stale crash
   * must only settle its own promise: never release or promote over the new run.
   */
  const settleCrash = (
    pending: Pending,
    failure: WorkerCrashedError | Error,
    payload: WorkerCrashed,
    decision: AutoResumeDecision | undefined,
  ): void => {
    if (!removeOwnedRun(pending)) {
      rejectRun(pending, failure)
      return
    }
    workers.release(pending.sessionID)
    if (decision?.resume === true) {
      // The caller's run was resumed, so its promise must reflect the resumed
      // run, not the crash that triggered it. When the owner re-dispatches (the
      // production hub returns that promise), settle this caller with its
      // outcome; without a promise, resolve as resumed so no caller observes
      // `WorkerCrashedError` for a run that is still healthy.
      const resumed = options.onAutoResume?.(decision, payload)
      if (resumed instanceof Promise) {
        resumed.then(
          () => {
            if (pending.gate.claim("completion")) resolveRun(pending)
            else rejectRun(pending, cancelled(pending.sessionID))
          },
          (error: unknown) => {
            rejectRun(pending, error)
            nextWaiter(pending.sessionID)
          },
        )
        return
      }
      // A resume owner that returns no run promise cannot deliver its outcome:
      // the caller still observes the typed crash (never a silent success).
    }
    rejectRun(pending, failure)
    nextWaiter(pending.sessionID)
  }

  /**
   * Retire a worker that died while a cancel still owned its run. The cancel
   * controller keeps the graceful settlement (`keepController`), but retirement
   * and the child cascade belong to this crash; otherwise the next turn reuses a
   * dead worker. This is the one dead-worker retirement path on the cancel side.
   */
  const retireDeadCancelledWorker = (sessionID: SessionID): void => {
    const dead = workers.worker(sessionID)
    if (dead === undefined || dead.alive) return
    crashChildren(sessionID)
    retireWorker(sessionID, { keepController: true })
  }

  const fail = (pending: Pending, error: unknown): void => {
    markSettled(pending)
    // A dead isolate must never survive a cancel that owns this run: the cancel
    // settles the terminal decision, but retirement and the child cascade still
    // belong to this crash.
    if (phaseOf(pending).cancelPending) {
      retireDeadCancelledWorker(pending.sessionID)
      return
    }
    // A late rejection from a cancelled/superseded run must not settle the
    // successor that now owns this session. The ownership read, gate check, and
    // run removal are one atomic transition.
    const outcome = atomically<FailOwnershipDecision>((state) => {
      if (state.runs.get(pending.sessionID) !== pending) return [{ kind: "superseded" }, state]
      if (pending.gate.winner === "cancel") {
        return [{ kind: "gate-cancel" }, withoutRun(state, pending.sessionID)]
      }
      return [{ kind: "owned" }, state]
    })
    if (outcome.kind === "superseded") {
      settleSuperseded(pending)
      return
    }
    if (outcome.kind === "gate-cancel") {
      workers.release(pending.sessionID)
      retireDeadCancelledWorker(pending.sessionID)
      rejectRun(pending, cancelled(pending.sessionID))
      nextWaiter(pending.sessionID)
      return
    }
    const timedOut = error instanceof SessionWorkerReplyTimeoutError
    const worker = workers.worker(pending.sessionID)
    const crashed = error instanceof WorkerCrashedError || (worker !== undefined && !worker.alive)
    if (timedOut) {
      // A wedged isolate is retired, never requeued; cascade to its subagents.
      workers.release(pending.sessionID)
      removeOwnedRun(pending)
      rejectRun(pending, error)
      void cancelWithGrace(pending.sessionID).catch(() => undefined)
      retireWorker(pending.sessionID)
    } else if (crashed) {
      const payload = crashPayloadFor(pending, error)
      log(workerLogMessage("worker crashed"), {
        level: "WARN",
        sessionID: pending.sessionID,
        workerID: payload.workerID,
        phase: payload.phase,
        reason: payload.reason,
      })
      options.recordCrash?.(payload)
      options.onCrash?.(payload)
      const cause = error instanceof WorkerCrashedError ? (error.cause ?? error) : error
      const failure =
        errorTagOf(cause) === "BuildMismatch" && cause instanceof Error
          ? // A stale bundle is a typed, non-retryable failure: return it as-is
            // (the hub's resume decision also blocks a respawn until it changes).
            cause
          : new WorkerCrashedError({
              message:
                error instanceof WorkerCrashedError
                  ? error.message
                  : `worker ${payload.workerID} crashed: ${payload.reason}`,
              workerID: payload.workerID,
              sessionID: payload.sessionID,
              phase: payload.phase,
              lastCommittedMessageID: payload.lastCommittedMessageID,
              inFlightCallIDs: payload.inFlightCallIDs,
              reason: payload.reason,
              cause,
            })
      crashChildren(pending.sessionID)
      retireWorker(pending.sessionID)
      const decision = options.resumeDecision?.(payload)
      if (decision instanceof Promise) {
        // An async decision owns the terminal state only for a bounded window.
        // If it never resolves, the session would stay owned by a dead run and
        // every new `ensureRunning` would attach to that dead promise. Settle
        // without a resume at the deadline; a late answer is ignored.
        let settled = false
        const settleOnce = (resolved: AutoResumeDecision | undefined): void => {
          if (settled) return
          settled = true
          if (timer !== undefined) clearTimeout(timer)
          settleCrash(pending, failure, payload, resolved)
        }
        const timer =
          resumeDecisionTimeoutMs > 0
            ? setTimeout(() => {
                log(workerLogMessage("resume decision timed out"), {
                  level: "WARN",
                  sessionID: pending.sessionID,
                  workerID: payload.workerID,
                  timeoutMs: resumeDecisionTimeoutMs,
                })
                settleOnce(undefined)
              }, resumeDecisionTimeoutMs)
            : undefined
        timer?.unref?.()
        void decision.then(
          (resolved) => settleOnce(resolved),
          () => settleOnce(undefined),
        )
        return
      }
      settleCrash(pending, failure, payload, decision)
      return
    } else {
      log(workerLogMessage("worker run failed"), {
        level: "ERROR",
        sessionID: pending.sessionID,
        workerID: phaseOf(pending).workerID,
        error: describeFailure(error),
        cause: error,
      })
      workers.release(pending.sessionID)
      removeOwnedRun(pending)
      rejectRun(pending, error)
    }
    nextWaiter(pending.sessionID)
  }

  /** Start the next parked run for a session, or arm the idle-reap timer. */
  const nextWaiter = (sessionID: SessionID): void => {
    // The admission guard is user code and must run outside the ownership
    // permit. Snapshot first so an owned session or an empty queue short-
    // circuits without invoking it.
    const snapshot = stateOf()
    const willAdmit = !snapshot.runs.has(sessionID) && (snapshot.waiters.get(sessionID)?.length ?? 0) > 0
    const blocked = willAdmit ? inadmissible(sessionID) : undefined
    // The live-owner guard, the admission check, the waiter list drain, and the
    // promotion are one atomic transition. A settlement can admit a successor as
    // a side effect (auto-resume dispatches synchronously), so a parked waiter
    // must never overwrite that live owner.
    const decision = atomically<NextWaiterDecision>((state) => {
      if (state.runs.has(sessionID)) return [{ kind: "owned" }, state]
      const waiters = state.waiters.get(sessionID)
      if (waiters === undefined || waiters.length === 0) return [{ kind: "idle" }, state]
      // A waiter promoted after a crash must not respawn a bundle that is no
      // longer admissible. The typed refusal is the caller's outcome, not a run.
      if (blocked !== undefined) {
        return [{ kind: "blocked", error: blocked, waiters }, withoutWaiters(state, sessionID)]
      }
      const next = waiters[0]
      const remaining = waiters.slice(1)
      return [
        { kind: "promote", next, remaining: remaining.length },
        withWaiters(withRun(state, sessionID, next), sessionID, remaining),
      ]
    })
    switch (decision.kind) {
      case "owned":
        return
      case "idle":
        scheduleReap(sessionID)
        return
      case "blocked":
        for (const waiter of decision.waiters) {
          if (waiter.gate.claim("cancel")) rejectRun(waiter, decision.error)
        }
        scheduleReap(sessionID)
        return
      case "promote":
        log(workerLogMessage("waiter promoted"), {
          level: "INFO",
          sessionID,
          waiterQueueDepth: decision.remaining,
        })
        start(decision.next)
        return
    }
  }

  const rejectWaiters = (sessionID: SessionID, error: unknown): void => {
    const waiters = atomically((state) => {
      const current = state.waiters.get(sessionID)
      if (current === undefined) return [undefined, state] as const
      return [current, withoutWaiters(state, sessionID)] as const
    })
    if (waiters === undefined) return
    for (const waiter of waiters) {
      waiter.gate.claim("cancel")
      rejectRun(waiter, error)
    }
  }

  /** How one cancelled run's terminal state is settled. */
  interface CancelSettlement {
    readonly mode: CancelledMode
    readonly winner: Outcome
    readonly worker: SessionWorker | undefined
    /** Terminate and release the worker (the forced/immediate paths). */
    readonly retires: boolean
    /** Reap mirrored PGIDs when this settlement, not the controller, terminated. */
    readonly reaps: boolean
  }

  /**
   * The single settlement path for a cancelled run. `owned` is the exact run the
   * request captured; shared state is only touched while it still owns the run
   * slot. Parked waiters are drained by the caller exactly once before any await,
   * so a waiter that arrives during the grace window survives and is promoted
   * here.
   */
  const settleCancelled = (sessionID: SessionID, owned: Pending | undefined, settlement: CancelSettlement): void => {
    const { mode, winner, worker, retires, reaps } = settlement
    // The ownership check and the run-slot removal are one transition: a cancel
    // can never delete a successor's entry or release its worker.
    const owns =
      owned !== undefined &&
      atomically((state) => {
        if (state.runs.get(sessionID) !== owned) return [false, state] as const
        return [true, withoutRun(state, sessionID)] as const
      })

    if (owns) {
      if (workers.has(sessionID)) {
        workers.markCancelling(sessionID)
        workers.release(sessionID)
      }
    }

    if (owns && retires) {
      if (reaps && worker !== undefined) cancellation?.reap?.(worker)
      retireWorker(sessionID)
    }

    if (owned !== undefined) {
      if (winner === "completion" && owned.gate.claim("completion")) resolveRun(owned)
      else {
        owned.gate.claim("cancel")
        rejectRun(owned, cancelled(sessionID))
      }
    }

    if (owned !== undefined) {
      cancellation?.onCancelled?.({ sessionID, mode, workerID: worker?.id, resumeFromDb: true })
    }

    if (!hasRun(sessionID)) nextWaiter(sessionID)
  }

  /** Whether an in-flight run is the exact same run as a new request. */
  const isSameRun = (
    admitted: Pending,
    depth: number,
    mode: SessionRunMode,
    prompt: PromptInput | undefined,
    shell: ShellInput | undefined,
    model: { readonly providerID: string; readonly modelID: string } | undefined,
  ): boolean =>
    !phaseOf(admitted).settled &&
    admitted.depth === depth &&
    admitted.mode === mode &&
    admitted.prompt === prompt &&
    admitted.shell === shell &&
    admitted.model?.providerID === model?.providerID &&
    admitted.model?.modelID === model?.modelID

  const dispatch = (request: DispatchRequest): Promise<void> => {
    // Shutdown owns the terminal state: a prompt racing `terminateAll`/`dispose`
    // must never construct an isolate after the pool and backends are gone. The
    // atomic transition re-checks the latch, so the fast path cannot race it.
    if (stateOf().terminated) return Promise.reject(cancelled(request.sessionID))
    const depth = request.depth ?? 0
    if (!Number.isInteger(depth) || depth < 0) {
      return Promise.reject(new Error(`invalid dispatch depth: ${depth} (expected a non-negative integer)`))
    }
    const mode: SessionRunMode = request.mode ?? "run"
    const sessionID = request.sessionID
    const prompt = request.prompt
    const shell = request.shell
    const model = request.model ?? request.prompt?.model

    // Join vs park vs admit is decided together with the ownership write.
    const decision = atomically<DispatchDecision>((state) => {
      if (state.terminated) return [{ kind: "reject" }, state]
      const admitted = state.runs.get(sessionID)
      if (admitted !== undefined) {
        if (isSameRun(admitted, depth, mode, prompt, shell, model)) {
          return [{ kind: "join", promise: admitted.promise }, state]
        }
        // Different run identity: park behind the in-flight run. Unbounded.
        const pending = createPending(sessionID, depth, mode, prompt, shell, model)
        const waiters = state.waiters.get(sessionID) ?? []
        return [{ kind: "park", pending }, withWaiters(state, sessionID, [...waiters, pending])]
      }
      const pending = createPending(sessionID, depth, mode, prompt, shell, model)
      return [{ kind: "admit", pending }, withRun(state, sessionID, pending)]
    })
    if (decision.kind === "reject") return Promise.reject(cancelled(sessionID))
    if (decision.kind === "join") return decision.promise
    if (decision.kind === "park") {
      clearIdle(sessionID)
      return decision.pending.promise
    }
    start(decision.pending)
    return decision.pending.promise
  }

  const cancel = async (sessionID: SessionID): Promise<void> => {
    const owned = stateOf().runs.get(sessionID)
    // Parked waiters belong to this session's terminal decision: drain them once,
    // before the admitted run's worker settles.
    rejectWaiters(sessionID, cancelled(sessionID))
    if (owned === undefined) return
    if (workers.has(sessionID)) workers.markCancelling(sessionID)
    const worker = workers.worker(sessionID)
    // A run still in its first boot has no fiber to terminate: the boot lifecycle
    // aborts the partial isolate, releases the permit, and reports the typed boot
    // outcome. Only a run that already started takes the terminate-only path.
    const boot = await worker?.cancelBoot?.()
    if (boot !== undefined) {
      settleCancelled(sessionID, owned, {
        mode: "boot",
        winner: "cancel",
        worker,
        retires: true,
        reaps: false,
      })
      return
    }
    settleCancelled(sessionID, owned, {
      mode: "terminated",
      winner: "cancel",
      worker,
      retires: true,
      reaps: true,
    })
  }

  interface CascadeAccumulator {
    readonly seen: Set<SessionID>
    readonly descendants: CascadeDescendant[]
  }

  /**
   * Cascade one node: children first, then this session. `seen` is the single
   * cycle guard, so a self-parent or a diamond edge visits each session once.
   */
  const cancelOne = async (sessionID: SessionID, cascade: CascadeAccumulator): Promise<CancelOutcome | undefined> => {
    if (cascade.seen.has(sessionID)) return undefined
    cascade.seen.add(sessionID)

    const config = cancellation
    if (config === undefined) {
      await cancel(sessionID)
      return undefined
    }

    // Capture the exact run this request owns BEFORE any await: a newer run may
    // be admitted while children unwind or the grace window is open. The
    // `Pending` identity is the generation; only this run may be cancelled,
    // released, or have its worker retired.
    const owned = stateOf().runs.get(sessionID)

    for (const child of config.childrenOf?.(sessionID) ?? []) {
      if (cascade.seen.has(child)) continue
      const childOutcome = await cancelOne(child, cascade)
      cascade.descendants.push({ sessionID: child, mode: childOutcome?.mode })
    }

    if (owned === undefined) return undefined

    // The run we meant to cancel already settled and something else owns the
    // session now: never cancel the newcomer. The owner check and the waiter
    // drain are one atomic transition, so a waiter cannot be left behind.
    const drained = atomically((state) => {
      if (state.runs.get(sessionID) !== owned) return [undefined, state] as const
      const waiters = state.waiters.get(sessionID)
      if (waiters === undefined) return [[] as ReadonlyArray<Pending>, state] as const
      return [waiters, withoutWaiters(state, sessionID)] as const
    })
    if (drained === undefined) return undefined

    // Drain waiters that existed at request time exactly once. A prompt that
    // parks during the grace window is NOT drained here; `settleCancelled`
    // promotes it onto a fresh worker after this cancel no longer owns it.
    for (const waiter of drained) {
      waiter.gate.claim("cancel")
      rejectRun(waiter, cancelled(sessionID))
    }

    if (workers.has(sessionID)) workers.markCancelling(sessionID)
    const worker = workers.worker(sessionID)
    // The first boot owns the spawn permit and has no run fiber yet: abort the
    // boot interval and report `boot` instead of sending a cancel frame to a
    // worker that cannot answer it. The boot path releases the permit exactly
    // once; a terminate-only settlement would hide that typed outcome.
    if (worker !== undefined) {
      const started = Date.now()
      const boot = await worker.cancelBoot?.()
      if (boot !== undefined) {
        settleCancelled(sessionID, owned, {
          mode: "boot",
          winner: "cancel",
          worker,
          retires: true,
          reaps: false,
        })
        // A boot cancel settles a partial isolate that never started a run, so
        // no worker published the terminal idle. `acknowledged: false` forces the
        // caller's fallback idle; otherwise an auto-resume boot cancel would
        // leave the session busy forever.
        return { sessionID, mode: "boot", winner: "cancel", acknowledged: false, durationMs: Date.now() - started }
      }
    }
    const port = worker === undefined ? undefined : config.portFor?.(worker)

    if (worker === undefined || port === undefined) {
      settleCancelled(sessionID, owned, {
        mode: "terminated",
        winner: "cancel",
        worker,
        retires: true,
        reaps: true,
      })
      return { sessionID, mode: "terminated", winner: "cancel", acknowledged: false, durationMs: 0 }
    }

    updatePhase(owned, (phase) => ({ ...phase, cancelPending: true }))
    let resolved: CancelOutcome = {
      sessionID,
      mode: "terminated",
      winner: "cancel",
      acknowledged: false,
      durationMs: 0,
    }
    try {
      const outcome = await controllerFor(config, worker, port).cancel(sessionID, {
        complete: owned.completionPromise,
      })
      resolved =
        outcome.mode === "cooperative" && !outcome.acknowledged && phaseOf(owned).completed
          ? { ...outcome, mode: "completed", winner: "completion" }
          : outcome
      return resolved
    } finally {
      updatePhase(owned, (phase) => ({ ...phase, cancelPending: false }))
      // The controller terminates/reaps on the forced path; a cooperative or
      // completed outcome keeps the worker. Only the captured run is released.
      settleCancelled(sessionID, owned, {
        mode: resolved.mode,
        winner: resolved.winner,
        worker,
        retires: resolved.mode === "terminated" || resolved.mode === "boot",
        reaps: false,
      })
    }
  }

  const cancelWithGrace = async (sessionID: SessionID): Promise<CascadeReport | undefined> => {
    const cascade: CascadeAccumulator = { seen: new Set<SessionID>(), descendants: [] }
    const outcome = await cancelOne(sessionID, cascade)
    if (outcome === undefined) return undefined
    return {
      ...outcome,
      sessionID,
      outcome,
      descendants: cascade.descendants,
    }
  }

  const terminateAll = async (): Promise<void> => {
    // The shutdown latch, the run table, and the waiter table flip together:
    // a dispatch can either observe the pre-shutdown state or the latch, never a
    // cleared table that still admits work.
    const shutdown = atomically((state) => {
      if (state.terminated) return [undefined, state] as const
      const runs = [...state.runs.values()]
      const waiters = [...state.waiters.values()].flat()
      return [
        { runs, waiters },
        {
          runs: new Map<SessionID, Pending>(),
          waiters: new Map<SessionID, ReadonlyArray<Pending>>(),
          terminated: true,
        },
      ] as const
    })
    if (shutdown === undefined) return
    for (const sessionID of [...idleTimers.keys()]) clearIdle(sessionID)
    for (const pending of shutdown.runs) rejectRun(pending, cancelled(pending.sessionID))
    for (const pending of shutdown.waiters) rejectRun(pending, cancelled(pending.sessionID))
    for (const controller of controllers.values()) controller.dispose()
    controllers.clear()
    const workersToTerminate = workers.entries().map((entry) => entry.worker)
    workers.clear()
    await Promise.all(workersToTerminate.map((worker) => worker.terminate().catch(() => undefined)))
    await pool.terminateAll().catch(() => undefined)
    cancellation?.reapAll?.()
  }

  return {
    get busySlotCount() {
      return stateOf().runs.size
    },
    get queuedCount() {
      let count = 0
      for (const waiters of stateOf().waiters.values()) count += waiters.length
      return count
    },
    registry: workers,
    workerForSession: (sessionID) => workers.get(sessionID)?.workerID,
    dispatch,
    cancel,
    cancelWithGrace,
    retire,
    awaitRelease,
    terminateAll,
    dispose: terminateAll,
    [Symbol.asyncDispose]: terminateAll,
  }
}

export * as SessionDispatch from "./dispatch"
