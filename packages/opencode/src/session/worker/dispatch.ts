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
// `hub.ts` (the scoped production pool).

import {
  Cause,
  Context,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  FiberMap,
  FiberSet,
  Layer,
  Schema,
  Scope,
} from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import type { SessionID } from "@/session/schema"
import { isWorkerCrashPhase, workerCrashed, type WorkerCrashPhase, type WorkerCrashed } from "@/session/ledger/crash"
import type { AutoResumeDecision } from "@/session/ledger/resume"
import type { PromptInput, ShellInput } from "@/session/prompt"
import { defined } from "@/util/object"
import { createSessionWorkerRegistry, type SessionWorkerRegistry } from "./session-registry"
import { defaultSchedule, errorTagOf, messageOf } from "./port"
import { resolveReapMs } from "./pool"
import type { EffectBridge } from "@/effect/bridge"
import { workerLogMessage } from "./worker-logging"
import { SessionWorkerReplyTimeoutError } from "./execute"
import type { ExecutorRun, ExecutorSessionWorker, SessionExecutionResult, SessionRunMode } from "./execute"
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

/** Default deadline for a completed run's durable write barrier. */
const DEFAULT_WRITE_BARRIER_TIMEOUT_MS = 5_000

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
  /** The worker's last `done` reply, when the pool retains it. */
  resultFor?(sessionID: SessionID): SessionExecutionResult | undefined
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

/**
 * The classification of a run whose fiber exited because it was interrupted.
 * Derived from `Cause.hasInterruptsOnly` (never from an error string or exit
 * code), mirroring `Runner.Cancelled`.
 */
export class Cancelled extends Schema.TaggedErrorClass<Cancelled>()("SessionDispatchCancelled", {}) {}

/** Classify a run exit cause as a clean cancellation, or `undefined` otherwise. */
export const cancelledFromCause = (cause: Cause.Cause<unknown>): Cancelled | undefined =>
  Cause.hasInterruptsOnly(cause) ? new Cancelled() : undefined

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
    super({
      message: `parent session ${parentSessionID} crashed; child ${childSessionID} was cancelled`,
      sessionID: childSessionID,
    })
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

/** The one commit outcome a run's `done` reply and the barrier agree on (F11). */
export interface CommitOutcome {
  readonly committed: string | null
}

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
  readonly bridge?: EffectBridge.Shape
  /**
   * Per-session write barrier: a completed run's worker is not released for its
   * next run until the barrier resolves, so the next run never overtakes the
   * prior run's durable egress write.
   */
  readonly writeBarrier?: (sessionID: SessionID, outcome?: CommitOutcome) => Promise<void> | undefined
  /**
   * How long a completed run waits for its write barrier before releasing the
   * worker as if the barrier failed. Defaults to 5s; `0` or less fires the
   * deadline immediately (the run never waits for durability).
   */
  readonly writeBarrierTimeoutMs?: number
  /**
   * Refuse a run that can no longer be admitted. Checked before every waiter
   * promotion, not only at hub entry, so a run parked behind a crash is
   * re-checked before it spawns.
   */
  readonly assertAdmissible?: (sessionID: SessionID) => void
  /** Hub-side cleanup when a session's worker is retired (reap/cancel/crash). */
  readonly onSessionReleased?: (sessionID: SessionID, workerID?: number) => unknown
  /**
   * Worker/generation-scoped cleanup for an in-place generation replacement
   * (a mid-session model switch). It runs INSTEAD OF `onSessionReleased`, so the
   * session's own hub state survives while the old generation's ports, pending
   * asks and lease are released.
   */
  readonly onGenerationRetired?: (sessionID: SessionID, workerID?: number) => unknown
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
   * park the session forever. Defaults to 5s; `0` or less fires the deadline
   * immediately (no confirmed-exit wait).
   */
  readonly terminateTimeoutMs?: number
  /**
   * Observe an allowed auto-resume; re-dispatch stays with the hub owner. The
   * returned promise is the resumed run: the original crashed caller waits for
   * it instead of observing `WorkerCrashedError` while its replacement runs.
   */
  readonly onAutoResume?: (decision: Extract<AutoResumeDecision, { resume: true }>, payload: WorkerCrashed) => unknown
  /**
   * The hub's authoritative commit outcome for a completed run, read before the
   * hub deletes its active-run record. Carried into the barrier and the run's
   * `done` reply (F11).
   */
  readonly commitOutcome?: (sessionID: SessionID) => CommitOutcome | undefined
}

export interface Dispatcher extends AsyncDisposable {
  readonly busySlotCount: number
  readonly queuedCount: number
  /** Retiring generations still waiting for their confirmed exit. */
  readonly retiringCount: number
  readonly registry: SessionWorkerRegistry<SessionWorker>
  /** The worker currently hosting `sessionID`'s active run, or `undefined` when idle. */
  workerForSession(sessionID: SessionID): number | undefined
  dispatch(request: DispatchRequest): Promise<void>
  /** Effect-native admission used by the hub-side dispatcher adapter. */
  dispatchEffect(request: DispatchRequest): Effect.Effect<void, unknown>
  cancel(sessionID: SessionID): Promise<void>
  cancelWithGrace(sessionID: SessionID): Promise<CascadeReport | undefined>
  /**
   * Retire the session's generation: drop its registry entry, terminate the
   * isolate, and run the release callback/lease release. Used by the pool's idle
   * cap, which evicts a worker the dispatcher would otherwise keep idle.
   */
  retire(sessionID: SessionID): Promise<void>
  /**
   * Retire one generation of a session's worker that the pool replaced in place
   * (a mid-session model switch). Runs the same release gate and confirmed-exit
   * barrier as `retire` but never terminates or drops the successor isolate: the
   * pool reuses the same worker slot for the new generation.
   */
  retireGeneration(sessionID: SessionID, workerID?: number): Promise<void>
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
  /**
   * True while a completed run's write barrier is still pending. That barrier
   * owns the single release/promotion for the run: a cancel settlement must
   * settle the caller without releasing the worker or promoting waiters until
   * the barrier's `barrierDone` fires.
   */
  completionBarrierInFlight: boolean
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
  /** The cancellation controller's "the run completed" probe. */
  readonly completion: Deferred.Deferred<void>
  /** Resolved by the barrier's finish path once `completionBarrierInFlight` clears. */
  readonly barrierDone: Deferred.Deferred<void>
  /** The caller's promise, derived from `done` at admission. */
  readonly promise: Promise<void>
  /** The completion probe, derived from `completion` at admission. */
  readonly completionPromise: Promise<void>
  /** The barrier probe, derived from `barrierDone` at admission. */
  readonly barrierDonePromise: Promise<void>
  phase: PendingPhase
  readonly gate: OutcomeGate
}

/**
 * The dispatcher's scoped Effect engine. `fork` owns every background fiber;
 * `reaping`/`retiring` are session-keyed so a replacement timer or retirement
 * interrupts the one it supersedes. A plain Map holds run ownership because JS
 * transitions are synchronous inside one Effect step.
 */
interface Engine {
  readonly fork: <A, E>(effect: Effect.Effect<A, E>) => Fiber.Fiber<A, E>
  readonly background: FiberSet.FiberSet<void>
  readonly reaping: FiberMap.FiberMap<SessionID, void>
  readonly runReap: (key: SessionID, effect: Effect.Effect<void>) => Fiber.Fiber<void>
  readonly retiring: FiberMap.FiberMap<SessionID, void>
  readonly runRetire: (key: SessionID, effect: Effect.Effect<void>) => Fiber.Fiber<void>
}

const makeEngine: Effect.Effect<Engine, never, Scope.Scope> = Effect.gen(function* () {
  const background = yield* FiberSet.make<void>()
  const fork = (yield* FiberSet.runtime(background)()) as <A, E>(effect: Effect.Effect<A, E>) => Fiber.Fiber<A, E>
  const reaping = yield* FiberMap.make<SessionID, void>()
  const runReap = yield* FiberMap.runtime(reaping)()
  const retiring = yield* FiberMap.make<SessionID, void>()
  const runRetire = yield* FiberMap.runtime(retiring)()
  return { fork, background, reaping, runReap, retiring, runRetire }
})

/** How an admitted run may begin after its build/shutdown preflight. */
type StartAdmission =
  | { readonly kind: "terminated" }
  | { readonly kind: "blocked"; readonly error: unknown }
  | { readonly kind: "open" }

/** What `nextWaiter` must do after inspecting the session slot. */
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

const phaseOf = (pending: Pending): PendingPhase => pending.phase

/** Complete the caller's run exactly once; a later settlement is ignored. */
const resolveRun = (pending: Pending): void => {
  Deferred.doneUnsafe(pending.done, Effect.void)
}

/** Fail the caller's run exactly once; a later settlement is ignored. */
const rejectRun = (pending: Pending, error: unknown): void => {
  Deferred.doneUnsafe(pending.done, Effect.fail(error))
}

/** Record the terminal observation once and wake the cancellation controller. */
const markSettled = (pending: Pending): void => {
  if (!pending.phase.settled) pending.phase = { ...pending.phase, settled: true }
  Deferred.doneUnsafe(pending.completion, Effect.void)
}

function createPending(
  promiseOf: <A, E>(deferred: Deferred.Deferred<A, E>) => Promise<A>,
  sessionID: SessionID,
  depth: number,
  mode: SessionRunMode,
  prompt: PromptInput | undefined,
  shell: ShellInput | undefined,
  model: { readonly providerID: string; readonly modelID: string } | undefined,
): Pending {
  const done = Deferred.makeUnsafe<void, unknown>()
  const completion = Deferred.makeUnsafe<void>()
  const barrierDone = Deferred.makeUnsafe<void>()
  return {
    sessionID,
    depth,
    mode,
    prompt,
    shell,
    model,
    done,
    completion,
    barrierDone,
    promise: promiseOf(done),
    // The completion and barrier probes are internal; a runtime shutdown may
    // interrupt a pending await, so they never surface an unhandled rejection.
    completionPromise: promiseOf(completion).catch(() => undefined),
    barrierDonePromise: promiseOf(barrierDone).catch(() => undefined),
    phase: {
      settled: false,
      completed: false,
      startedAt: undefined,
      workerID: undefined,
      cancelPending: false,
      completionBarrierInFlight: false,
    },
    gate: createOutcomeGate(),
  }
}

function buildDispatcher(
  options: DispatcherOptions,
  engine: Engine,
  runPromise: <A, E>(effect: Effect.Effect<A, E>) => Promise<A>,
  scope: Scope.Scope,
): Dispatcher {
  const pool = options.pool
  const cancellation = options.cancellation
  const bridge = options.bridge
  const writeBarrier = options.writeBarrier
  const schedule = options.schedule ?? defaultSchedule
  const reapMs = options.reapMs ?? resolveReapMs()
  const resumeDecisionTimeoutMs = options.resumeDecisionTimeoutMs ?? DEFAULT_RESUME_DECISION_TIMEOUT_MS
  const terminateTimeoutMs = options.terminateTimeoutMs ?? DEFAULT_TERMINATE_TIMEOUT_MS
  const writeBarrierTimeoutMs = options.writeBarrierTimeoutMs ?? DEFAULT_WRITE_BARRIER_TIMEOUT_MS
  const promiseOf = <A, E>(deferred: Deferred.Deferred<A, E>): Promise<A> => runPromise(Deferred.await(deferred))

  // Plain single-owner state. Every transition below is synchronous, so a
  // check and the write it guards cannot interleave in JS's run-to-completion.
  const runs = new Map<SessionID, Pending>()
  const waiters = new Map<SessionID, Pending[]>()
  let terminated = false
  // The session→worker map. Workers outlive runs (reuse across turns) until
  // idle-reap or terminal cancel disposes them.
  const workers = createSessionWorkerRegistry<SessionWorker>()
  const controllers = new Map<number, CancelController>()
  /**
   * The retired-run signal for a session. Completed ONLY by the retirement
   * fiber's `onExit` after its release settles; a successor awaits it.
   */
  const retiringDone = new Map<SessionID, Deferred.Deferred<void>>()
  let disposed = false

  /** Create one run's settlement cells and Promise facade. */
  const create = (
    sessionID: SessionID,
    depth: number,
    mode: SessionRunMode,
    prompt: PromptInput | undefined,
    shell: ShellInput | undefined,
    model: { readonly providerID: string; readonly modelID: string } | undefined,
  ): Pending => createPending(promiseOf, sessionID, depth, mode, prompt, shell, model)

  /**
   * Bound a Promise with a deadline. A promise that settles is forwarded as-is;
   * only one that never settles pays the timeout. A timeout fires `onTimeout`
   * once and yields `undefined`, so the caller always resumes. When the deadline
   * has already elapsed the source is still observed, so its rejection can never
   * surface unhandled.
   *
   * The deadline is Effect's interruptible clock, not a raw host timer: the
   * timeout fiber is interrupted the moment the source settles.
   */
  const boundedPromise = <A>(promise: Promise<A>, timeoutMs: number, onTimeout: () => void): Promise<A | undefined> => {
    /**
     * Run the deadline diagnostic without letting it escape. From an Effect
     * timeout callback a throw would be a defect that still resolves the caller
     * wrong; the failure itself is reported as one more diagnostic, guarded
     * because the logger is the seam that may have failed.
     */
    const reportTimeout = (): void => {
      try {
        onTimeout()
      } catch (error) {
        try {
          void bridge?.fork(
            Effect.logWarning(workerLogMessage("timeout diagnostic failed"), { reason: messageOf(error) }),
          )
        } catch {
          // No reporter left: settling the deadline is the only safe outcome.
        }
      }
    }
    if (timeoutMs <= 0) {
      // The deadline already expired: observe the source so a rejection is never
      // unhandled, but never wait for it.
      promise.catch(() => undefined)
      reportTimeout()
      return Promise.resolve(undefined)
    }
    return runPromise(
      Effect.tryPromise({ try: () => promise, catch: (error) => error }).pipe(
        Effect.timeoutOrElse({
          duration: Duration.millis(timeoutMs),
          orElse: () =>
            Effect.sync(() => {
              reportTimeout()
              return undefined
            }),
        }),
      ),
    )
  }

  const cancelled = (sessionID: SessionID): SessionCancelledError =>
    new SessionCancelledError({ message: `session ${sessionID} was cancelled`, sessionID })

  /**
   * A new turn under a session key interrupts any pending reap timer: the
   * FiberMap replacement is the identity fence, so no token bookkeeping remains.
   */
  const clearIdle = (sessionID: SessionID): void => {
    engine.runReap(sessionID, Effect.void)
  }

  const hasWaiters = (sessionID: SessionID): boolean => (waiters.get(sessionID)?.length ?? 0) > 0

  const hasRun = (sessionID: SessionID): boolean => runs.has(sessionID)

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
      void bridge?.fork(
        Effect.logWarning(workerLogMessage("background probe failed"), {
          sessionID,
          workerID: worker.id,
          reason: messageOf(error),
        }),
      )
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
      if (terminated || hasRun(sessionID) || hasWaiters(sessionID)) return
      const worker = workers.worker(sessionID)
      if (worker === undefined) return
      const active = yield* Effect.tryPromise({
        try: () => hasActiveWork(worker, sessionID),
        catch: (error) => error,
      }).pipe(Effect.orElseSucceed(() => false))
      // Re-check after the await: a new run or parked waiter must win, and a
      // retired/replaced worker must not be touched by this stale timer.
      if (terminated || hasRun(sessionID) || hasWaiters(sessionID)) return
      if (workers.worker(sessionID) !== worker) return
      if (active) {
        // A running background job still owns this isolate; replace this timer
        // with a fresh window under the same key.
        scheduleReap(sessionID)
        return
      }
      retireWorker(sessionID)
      void bridge?.fork(
        Effect.logInfo(workerLogMessage("session worker reaped"), {
          sessionID,
          workerID: worker.id,
          idleMs: reapMs,
        }),
      )
    })

  /**
   * The delay for one reap. The scheduler seam stays callback-based, but its
   * cancel runs as the callback's cleanup when the fiber is interrupted.
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
    if (terminated || workers.worker(sessionID) === undefined) return
    engine.runReap(sessionID, reapAfterDelay(sessionID))
  }

  const disposeController = (workerID: number): void => {
    const controller = controllers.get(workerID)
    if (controller === undefined) return
    controller.dispose()
    controllers.delete(workerID)
  }

  /**
   * Detach the controller a replaced generation used. An in-flight cancel keeps
   * its controller until it settles, then the cancel path disposes it (the
   * successor run may be the one it is cancelling); later cancels get a fresh controller for the new generation.
   */
  const detachController = (workerID: number): void => {
    const controller = controllers.get(workerID)
    if (controller === undefined) return
    controllers.delete(workerID)
    if (controller.pending === 0) controller.dispose()
  }

  /**
   * The shared retirement release gate. `scope` selects which lifetime the hook
   * releases: a terminal retirement ("session": crash/reap/cancel/idle eviction)
   * also runs the pool's session cleanup and the session-release hook, while an
   * in-place generation replacement ("generation": a mid-session model switch)
   * runs only the generation-scoped hook. Both signal the same confirmed-exit
   * barrier AFTER the release lands, so a successor never starts against a live
   * old generation.
   */
  const beginRetirement = (
    sessionID: SessionID,
    worker: SessionWorker,
    scope: "session" | "generation",
  ): Promise<void> => {
    // Retirement is reported only after the isolate has actually exited. A
    // still-live old generation could otherwise write after its lease was
    // zeroed and re-stamp the expiry, starving the successor. The callback may
    // be async (the hub's lease release is an executor round-trip); await it in
    // the same fiber so a successor waits for the release too.
    const exit = Deferred.makeUnsafe<void>()
    retiringDone.set(sessionID, exit)
    let releasing: Promise<void> | undefined
    /** Run the release hook exactly once, even if the terminate watchdog races. */
    const release = (): Promise<void> => {
      if (releasing !== undefined) return releasing
      releasing = (async () => {
        // The pool's session state (init cache, idle-cap bookkeeping) belongs to
        // the session, not the generation. Only a terminal retirement drops it;
        // a generation switch leaves it for the successor on the same slot.
        if (scope === "session") {
          try {
            pool.releaseSession?.(sessionID, worker.id)
          } catch (error) {
            void bridge?.fork(
              Effect.logWarning(workerLogMessage("session release callback failed"), {
                sessionID,
                workerID: worker.id,
                reason: messageOf(error),
              }),
            )
          }
        }
        const hook = scope === "generation" ? options.onGenerationRetired : options.onSessionReleased
        try {
          // The callback may be async (the hub's lease release is an executor
          // round-trip); await it before signalling the successor.
          const released = hook?.(sessionID, worker.id) as PromiseLike<unknown> | undefined
          if (released !== undefined && typeof released.then === "function") {
            // The synchronous epoch advance and exact-owner capture happen when
            // the hook is called, before the successor is admitted; only the
            // async tail is bounded so a wedged release cannot hold the slot.
            await boundedPromise(
              Promise.resolve(released),
              terminateTimeoutMs,
              () =>
                void bridge?.fork(
                  Effect.logWarning(workerLogMessage("release callback timed out"), {
                    sessionID,
                    workerID: worker.id,
                    timeoutMs: terminateTimeoutMs,
                  }),
                ),
            )
          }
        } catch (error) {
          // A callback that throws must not leave the exit signal failed (a
          // successor may be waiting) nor park it: log and treat it as settled.
          void bridge?.fork(
            Effect.logWarning(workerLogMessage("release callback failed"), {
              sessionID,
              workerID: worker.id,
              reason: messageOf(error),
            }),
          )
        }
      })()
      return releasing
    }
    /**
     * Complete the retired Deferred exactly once, and only from the retirement
     * fiber's `onExit` after the idempotent release has settled (or failed).
     */
    const completeRetired = (): void => {
      if (retiringDone.get(sessionID) === exit) retiringDone.delete(sessionID)
      Deferred.doneUnsafe(exit, Effect.void)
    }
    const releaseEffect: Effect.Effect<void> = Effect.promise(() => release()).pipe(Effect.ignoreCause)
    // The pool terminated the old isolate before it reported the generation
    // change, so there is nothing left to watch: run the release directly (the
    // pool awaits the retired Deferred before spawning the successor).
    const body =
      scope === "generation"
        ? releaseEffect
        : Effect.tryPromise({ try: () => worker.terminate().catch(() => undefined), catch: (error) => error }).pipe(
            Effect.timeoutOrElse({
              duration: terminateTimeoutMs,
              orElse: () =>
                Effect.sync(() => {
                  void bridge?.fork(
                    Effect.logWarning(workerLogMessage("retirement terminate timed out"), {
                      sessionID,
                      workerID: worker.id,
                      timeoutMs: terminateTimeoutMs,
                    }),
                  )
                }),
            }),
            Effect.ignore,
            Effect.andThen(releaseEffect),
          )
    // A terminal retirement exposes its bounded barrier through `retire`/
    // `awaitRelease`; the generation path awaits the same completed Deferred so
    // the pool can gate the successor on the release.
    engine.runRetire(
      sessionID,
      body.pipe(
        // One annotation at the per-session/per-worker fiber boundary, so every
        // Effect log inside the retirement carries the ids the forwarding logger
        // renders into the user-visible prefix.
        Effect.annotateLogs({ sessionID, workerID: worker.id }),
        Effect.onExit(() => Effect.uninterruptible(releaseEffect.pipe(Effect.andThen(Effect.sync(completeRetired))))),
      ),
    )
    return scope === "generation" ? runPromise(Deferred.await(exit)) : Promise.resolve()
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
    beginRetirement(sessionID, worker, "session")
    return worker
  }

  /**
   * Retire one generation the pool replaced in place. The session→worker entry
   * and the successor isolate are untouched: the same slot serves the new
   * generation, so only the old generation's controller, ports, pending asks and
   * lease are released.
   */
  const retireGeneration = (sessionID: SessionID, workerID?: number): Promise<void> => {
    const worker = workers.worker(sessionID)
    if (worker === undefined) {
      // The registry entry can already be gone when a concurrent terminal reap
      // drops it before the pool reports the in-place generation change. The
      // pool has terminated the old isolate by then, so run the generation
      // release directly: skipping it would leave the retired lease live and
      // the old epoch unadvanced. This runs synchronously, before any successor
      // dispatch can create a new entry.
      if (workerID === undefined) return Promise.resolve()
      clearIdle(sessionID)
      detachController(workerID)
      return invokeGenerationRelease(sessionID, workerID)
    }
    if (workerID !== undefined && worker.id !== workerID) {
      // A different live worker already owns the session: the old generation
      // was retired when its registry entry was replaced. Running the
      // generation hook now would advance the CURRENT generation's ask epoch
      // and cancel the successor's asks, so this stale report is a no-op.
      return Promise.resolve()
    }
    // A terminal retirement already owns this session's release gate; join its
    // release rather than starting a second, competing one.
    const retiring = retiringDone.get(sessionID)
    if (retiring !== undefined) return runPromise(Deferred.await(retiring))
    clearIdle(sessionID)
    detachController(worker.id)
    return beginRetirement(sessionID, worker, "generation")
  }

  /** Run the generation-scoped release with no dispatcher record to gate it. */
  const invokeGenerationRelease = (sessionID: SessionID, workerID: number): Promise<void> => {
    const hook = options.onGenerationRetired
    if (hook === undefined) return Promise.resolve()
    try {
      return Promise.resolve(hook(sessionID, workerID)).then(() => undefined)
    } catch (error) {
      void bridge?.fork(
        Effect.logWarning(workerLogMessage("generation release callback failed"), {
          sessionID,
          workerID,
          reason: messageOf(error),
        }),
      )
      return Promise.resolve()
    }
  }

  const controllerFor = (config: CancellationConfig, worker: SessionWorker, port: CancelPort): CancelController => {
    const existing = controllers.get(worker.id)
    if (existing !== undefined) return existing
    const controller = createCancelController({
      port,
      terminate: () => worker.terminate().then(() => undefined),
      ...defined({ graceMs: config.graceMs, schedule: config.schedule }),
      reap: () => config.reap?.(worker),
      ...(bridge === undefined ? {} : { bridge }),
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
    void bridge?.fork(
      Effect.logWarning(workerLogMessage("dead idle worker replaced"), { sessionID, workerID: entry.id }),
    )
    retireWorker(sessionID)
  }

  /**
   * Wait for a retiring generation's exit, but never forever: a rejected or
   * wedged terminate settles at the deadline so the successor can start instead
   * of parking the session. Returns whether the exit actually settled, so a
   * timed-out (still-live) retirement stays visible in diagnostics.
   */
  const awaitTerminating = (sessionID: SessionID, exit: Deferred.Deferred<void>): Effect.Effect<boolean> =>
    Deferred.await(exit).pipe(
      Effect.map(() => true),
      Effect.catchCause((cause) =>
        Effect.sync(() => {
          void bridge?.fork(
            Effect.logWarning(workerLogMessage("retirement exit failed"), {
              sessionID,
              reason: messageOf(Cause.squash(cause)),
            }),
          )
          return true
        }),
      ),
      Effect.timeoutOrElse({
        duration: terminateTimeoutMs,
        orElse: () =>
          Effect.sync(() => {
            void bridge?.fork(
              Effect.logWarning(workerLogMessage("retirement exit timed out"), {
                sessionID,
                timeoutMs: terminateTimeoutMs,
                reason: "the retiring generation never confirmed its exit; starting the successor",
              }),
            )
            return false
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
    engine.fork(
      Effect.gen(function* () {
        // A successor must re-observe ownership after this turn, never re-enter
        // the caller's stack synchronously when the exit signal is already set.
        yield* Effect.yieldNow
        const settled = yield* awaitTerminating(pending.sessionID, exit)
        // A timed-out retirement is still live. Keep it in `retiringDone` so
        // `retiringCount`/`awaitRelease` remain accurate; the successor still
        // starts (bounded), but through the explicit bypass below instead of
        // deleting evidence of the live generation.
        if (settled && retiringDone.get(pending.sessionID) === exit) retiringDone.delete(pending.sessionID)
        if (runs.get(pending.sessionID) !== pending) return
        if (terminated) {
          pending.gate.claim("cancel")
          rejectRun(pending, cancelled(pending.sessionID))
          return
        }
        start(pending, { permitLiveRetirement: !settled })
      }),
    )
  }

  /**
   * The Promise façade for a retirement's confirmed-exit signal. Bounded by the
   * same deadline as successor admission: a wedged terminate or release callback
   * must not pin `retire`/`awaitRelease` (and therefore idle-cap eviction) forever.
   */
  const retirementBarrier = (sessionID: SessionID): Promise<void> => {
    const exit = retiringDone.get(sessionID)
    if (exit === undefined) return Promise.resolve()
    return runPromise(
      Deferred.await(exit).pipe(
        Effect.timeoutOrElse({
          duration: terminateTimeoutMs,
          orElse: () =>
            Effect.sync(
              () =>
                void bridge?.fork(
                  Effect.logWarning(workerLogMessage("retirement barrier timed out"), {
                    sessionID,
                    timeoutMs: terminateTimeoutMs,
                  }),
                ),
            ),
        }),
      ),
    )
  }

  const retire = (sessionID: SessionID): Promise<void> => {
    retireWorker(sessionID)
    return retirementBarrier(sessionID)
  }

  /** The explicit release barrier for a retirement that is already in flight. */
  const awaitRelease = (sessionID: SessionID): Promise<void> => retirementBarrier(sessionID)

  /** Settle a run whose supervised fiber was interrupted as a clean cancel. */
  const cancelInterrupted = (pending: Pending): void => {
    pending.gate.claim("cancel")
    rejectRun(pending, cancelled(pending.sessionID))
  }

  const start = (pending: Pending, startOptions: { readonly permitLiveRetirement?: boolean } = {}): void => {
    // The build guard is user code: run it before mutating ownership, so a guard
    // that re-enters dispatch/terminateAll cannot observe a half-written state.
    const blocked = terminated ? undefined : inadmissible(pending.sessionID)
    const admission: StartAdmission = terminated
      ? { kind: "terminated" }
      : blocked === undefined
        ? { kind: "open" }
        : { kind: "blocked", error: blocked }
    if (admission.kind === "terminated") {
      pending.gate.claim("cancel")
      rejectRun(pending, cancelled(pending.sessionID))
      return
    }
    if (admission.kind === "blocked") {
      // A build that became inadmissible while this run waited (a crash blocked
      // the bundle) must reject here too, before any isolate is materialized.
      if (runs.get(pending.sessionID) === pending) runs.delete(pending.sessionID)
      if (pending.gate.claim("cancel")) rejectRun(pending, admission.error)
      nextWaiter(pending.sessionID)
      return
    }
    const exiting = startOptions.permitLiveRetirement === true ? undefined : retiringDone.get(pending.sessionID)
    if (exiting !== undefined) {
      deferUntilRetired(pending, exiting)
      return
    }
    clearIdle(pending.sessionID)
    replaceDeadIdleWorker(pending.sessionID)
    // `replaceDeadIdleWorker` may have just started an orderly retirement. The
    // check above happened before it ran, so re-check before spawning: the
    // successor must wait for the old close and lease release. A retirement that
    // already timed out is bypassed explicitly (it stays visible for diagnostics).
    const replaced = startOptions.permitLiveRetirement === true ? undefined : retiringDone.get(pending.sessionID)
    if (replaced !== undefined) {
      deferUntilRetired(pending, replaced)
      return
    }
    options.onRunStart?.(pending.sessionID)
    const worker = workers.getOrCreate(pending.sessionID, (id) => pool.spawn(id))
    pending.phase = { ...pending.phase, startedAt: Date.now(), workerID: worker.id }
    workers.markRunning(pending.sessionID)
    void bridge?.fork(
      Effect.logInfo(workerLogMessage("session admitted"), {
        sessionID: pending.sessionID,
        workerID: worker.id,
        depth: pending.depth,
        queueDepth: waiters.get(pending.sessionID)?.length ?? 0,
      }),
    )
    const run: SessionRun = {
      sessionID: pending.sessionID,
      depth: pending.depth,
      ...(pending.mode === "startShell" ? { mode: pending.mode } : {}),
      ...(pending.prompt === undefined ? {} : { prompt: pending.prompt }),
      ...(pending.shell === undefined ? {} : { shell: pending.shell }),
      ...(pending.model === undefined ? {} : { model: pending.model }),
    }
    const execution = Effect.tryPromise({
      try: () => worker.execute(run),
      catch: (error) => error,
    }).pipe(
      Effect.exit,
      Effect.flatMap((exit) => {
        if (Exit.isSuccess(exit)) return Effect.sync(() => complete(pending))
        const cause = exit.cause
        if (cancelledFromCause(cause) !== undefined) return Effect.sync(() => cancelInterrupted(pending))
        return Effect.sync(() => fail(pending, Cause.squash(cause)))
      }),
    )
    engine.fork(execution)
  }

  /** Release a completed run once the per-session write barrier (if any) resolves. */
  const complete = (pending: Pending): void => {
    pending.phase = { ...pending.phase, completed: true }
    markSettled(pending)
    // A cancel that already claimed the run owns the terminal decision; a run
    // whose session a successor now owns must not touch its write barrier.
    // A cooperative cancel still in grace has NOT claimed the run yet, so the
    // completion must still own the durable barrier before the cancel settles.
    const cancelOwned = pending.gate.winner === "cancel"
    if (cancelOwned || !ownsSession(pending)) {
      finalizeCompletion(pending)
      return
    }
    // Mark ownership BEFORE invoking the barrier: a cancel settlement observes
    // the flag and must leave release/promotion to this barrier's single
    // completion. Every path below clears the flag exactly once.
    pending.phase = { ...pending.phase, completionBarrierInFlight: true }
    let finalized = false
    const finish = (): void => {
      if (finalized) return
      finalized = true
      pending.phase = { ...pending.phase, completionBarrierInFlight: false }
      // A cancel that already claimed this run owns release/retire/promotion
      // once the barrier resolves, so it must not be finalized as a completion.
      // A completion still owns its own finalization here.
      if (pending.gate.winner !== "cancel") finalizeCompletion(pending)
      Deferred.doneUnsafe(pending.barrierDone, Effect.void)
    }
    const reported = pool.resultFor?.(pending.sessionID)?.committed
    const carried =
      options.commitOutcome?.(pending.sessionID) ?? (reported === undefined ? undefined : { committed: reported })
    let barrier: Promise<void> | undefined
    try {
      barrier = writeBarrier?.(pending.sessionID, carried)
    } catch (error) {
      // A throwing seam is treated as a resolved barrier so the run can never
      // park its worker forever.
      void bridge?.fork(
        Effect.logWarning(workerLogMessage("session write barrier failed"), {
          sessionID: pending.sessionID,
          reason: messageOf(error),
        }),
      )
      finish()
      return
    }
    if (barrier === undefined) {
      finish()
      return
    }
    void boundedPromise(
      barrier,
      writeBarrierTimeoutMs,
      () =>
        void bridge?.fork(
          Effect.logWarning(workerLogMessage("session write barrier timed out"), {
            sessionID: pending.sessionID,
            timeoutMs: writeBarrierTimeoutMs,
          }),
        ),
    ).then(finish, finish)
  }

  /**
   * Whether a run still owns its session. A cancelled or superseded run may
   * settle its worker protocol arbitrarily late; only the run `runs` points at
   * may mutate the session maps, reservations, waiters, or worker.
   */
  const ownsSession = (pending: Pending): boolean => runs.get(pending.sessionID) === pending

  /**
   * Atomically drop `pending` from run ownership. Returns false when a different
   * run (or no run) owns the session, so a stale settlement can never delete a
   * successor's entry.
   */
  const removeOwnedRun = (pending: Pending): boolean => {
    if (runs.get(pending.sessionID) !== pending) return false
    runs.delete(pending.sessionID)
    return true
  }

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
    void bridge?.fork(
      Effect.logInfo(workerLogMessage("session completed"), {
        sessionID: pending.sessionID,
        workerID,
        durationMs: startedAt === undefined ? 0 : Date.now() - startedAt,
      }),
    )
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
    // Carry the typed underlying failure on the in-memory payload without
    // changing its observable/persisted shape.
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
      const owned = runs.get(childSessionID)
      if (owned !== undefined) runs.delete(childSessionID)
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
    const outcome: FailOwnershipDecision =
      runs.get(pending.sessionID) !== pending
        ? { kind: "superseded" }
        : pending.gate.winner === "cancel"
          ? { kind: "gate-cancel" }
          : { kind: "owned" }
    if (outcome.kind === "gate-cancel") runs.delete(pending.sessionID)
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
      void bridge?.fork(
        Effect.logWarning(workerLogMessage("worker crashed"), {
          sessionID: pending.sessionID,
          workerID: payload.workerID,
          phase: payload.phase,
          reason: payload.reason,
        }),
      )
      options.recordCrash?.(payload)
      options.onCrash?.(payload)
      const cause = error instanceof WorkerCrashedError ? (error.cause ?? error) : error
      const failure = new WorkerCrashedError({
        message:
          error instanceof WorkerCrashedError ? error.message : `worker ${payload.workerID} crashed: ${payload.reason}`,
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
          settleCrash(pending, failure, payload, resolved)
        }
        // `0` disables the deadline (wait for the decision); otherwise the
        // Effect clock bounds it and a late answer cannot settle twice.
        const decisionWindow =
          resumeDecisionTimeoutMs > 0
            ? boundedPromise(
                decision,
                resumeDecisionTimeoutMs,
                () =>
                  void bridge?.fork(
                    Effect.logWarning(workerLogMessage("resume decision timed out"), {
                      sessionID: pending.sessionID,
                      workerID: payload.workerID,
                      timeoutMs: resumeDecisionTimeoutMs,
                    }),
                  ),
              )
            : decision
        void decisionWindow.then(
          (resolved) => settleOnce(resolved),
          () => settleOnce(undefined),
        )
        return
      }
      settleCrash(pending, failure, payload, decision)
      return
    } else {
      void bridge?.fork(
        Effect.logError(workerLogMessage("worker run failed"), {
          sessionID: pending.sessionID,
          workerID: phaseOf(pending).workerID,
          error: describeFailure(error),
          cause: error,
        }),
      )
      workers.release(pending.sessionID)
      removeOwnedRun(pending)
      rejectRun(pending, error)
    }
    nextWaiter(pending.sessionID)
  }

  /** Start the next parked run for a session, or arm the idle-reap timer. */
  const nextWaiter = (sessionID: SessionID): void => {
    // The admission guard is user code and must run outside the ownership
    // transition. Snapshot first so an owned session or an empty queue short-
    // circuits without invoking it.
    const willAdmit = !runs.has(sessionID) && (waiters.get(sessionID)?.length ?? 0) > 0
    const blocked = willAdmit ? inadmissible(sessionID) : undefined
    // A settlement can admit a successor as a side effect (auto-resume dispatches
    // synchronously), so a parked waiter must never overwrite a live owner.
    const parked = runs.has(sessionID) ? undefined : waiters.get(sessionID)
    const decision: NextWaiterDecision = runs.has(sessionID)
      ? { kind: "owned" }
      : parked === undefined || parked.length === 0
        ? { kind: "idle" }
        : blocked !== undefined
          ? { kind: "blocked", error: blocked, waiters: parked }
          : { kind: "promote", next: parked[0], remaining: parked.length - 1 }
    if (decision.kind === "blocked") waiters.delete(sessionID)
    if (decision.kind === "promote") {
      const remaining = parked!.slice(1)
      if (remaining.length === 0) waiters.delete(sessionID)
      else waiters.set(sessionID, remaining)
      runs.set(sessionID, decision.next)
    }
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
        void bridge?.fork(
          Effect.logInfo(workerLogMessage("waiter promoted"), {
            sessionID,
            waiterQueueDepth: decision.remaining,
          }),
        )
        start(decision.next)
        return
    }
  }

  const rejectWaiters = (sessionID: SessionID, error: unknown): void => {
    const parked = waiters.get(sessionID)
    if (parked === undefined) return
    waiters.delete(sessionID)
    for (const waiter of parked) {
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
  const settleCancelled = async (
    sessionID: SessionID,
    owned: Pending | undefined,
    settlement: CancelSettlement,
  ): Promise<void> => {
    const { mode, winner, worker, retires, reaps } = settlement
    // A completion that raced this cancel may have installed a write barrier.
    // The barrier owns the single release/promotion for that run, so the worker
    // must not be released (nor a waiter promoted) until its bounded finish runs.
    const barrierInFlight = owned !== undefined && phaseOf(owned).completionBarrierInFlight

    // Settle the caller before any durability wait: a cancel request never waits
    // on the write barrier.
    if (owned !== undefined) {
      if (winner === "completion" && owned.gate.claim("completion")) resolveRun(owned)
      else {
        owned.gate.claim("cancel")
        rejectRun(owned, cancelled(sessionID))
      }
      cancellation?.onCancelled?.({ sessionID, mode, workerID: worker?.id, resumeFromDb: true })
    }

    if (barrierInFlight && owned !== undefined) {
      await boundedPromise(
        owned.barrierDonePromise,
        writeBarrierTimeoutMs,
        () =>
          void bridge?.fork(
            Effect.logWarning(workerLogMessage("session write barrier timed out"), {
              sessionID,
              timeoutMs: writeBarrierTimeoutMs,
            }),
          ),
      )
    }

    // A completion-winner whose barrier was in flight is finalized by the
    // barrier's own finish path, which we just awaited. Every other path settles
    // ownership here, exactly once.
    const barrierOwnsFinalization = winner === "completion" && barrierInFlight
    // The ownership check and the run-slot removal are one transition: a cancel
    // can never delete a successor's entry or release its worker.
    const owns = owned !== undefined && !barrierOwnsFinalization && runs.get(sessionID) === owned
    if (owns) runs.delete(sessionID)

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

    // When the completion barrier owns the release, `finalizeCompletion` is the
    // one place that promotes the parked waiter after the barrier resolves.
    if (!barrierOwnsFinalization && !hasRun(sessionID)) nextWaiter(sessionID)
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
    if (terminated) return Promise.reject(cancelled(request.sessionID))
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
    const admitted = terminated ? undefined : runs.get(sessionID)
    const decision: DispatchDecision = terminated
      ? { kind: "reject" }
      : admitted !== undefined
        ? isSameRun(admitted, depth, mode, prompt, shell, model)
          ? { kind: "join", promise: admitted.promise }
          : { kind: "park", pending: create(sessionID, depth, mode, prompt, shell, model) }
        : { kind: "admit", pending: create(sessionID, depth, mode, prompt, shell, model) }
    if (decision.kind === "park") {
      const parked = waiters.get(sessionID) ?? []
      parked.push(decision.pending)
      waiters.set(sessionID, parked)
    }
    if (decision.kind === "admit") runs.set(sessionID, decision.pending)
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
    const owned = runs.get(sessionID)
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
      await settleCancelled(sessionID, owned, {
        mode: "boot",
        winner: "cancel",
        worker,
        retires: true,
        reaps: false,
      })
      return
    }
    await settleCancelled(sessionID, owned, {
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
    const owned = runs.get(sessionID)

    for (const child of config.childrenOf?.(sessionID) ?? []) {
      if (cascade.seen.has(child)) continue
      const childOutcome = await cancelOne(child, cascade)
      cascade.descendants.push({ sessionID: child, mode: childOutcome?.mode })
    }

    if (owned === undefined) return undefined

    // The run we meant to cancel already settled and something else owns the
    // session now: never cancel the newcomer. The owner check and the waiter
    // drain are one synchronous transition, so a waiter cannot be left behind.
    if (runs.get(sessionID) !== owned) return undefined
    const drained = waiters.get(sessionID) ?? []
    waiters.delete(sessionID)

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
        await settleCancelled(sessionID, owned, {
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
      await settleCancelled(sessionID, owned, {
        mode: "terminated",
        winner: "cancel",
        worker,
        retires: true,
        reaps: true,
      })
      return { sessionID, mode: "terminated", winner: "cancel", acknowledged: false, durationMs: 0 }
    }

    owned.phase = { ...owned.phase, cancelPending: true }
    const controller = controllerFor(config, worker, port)
    let resolved: CancelOutcome = {
      sessionID,
      mode: "terminated",
      winner: "cancel",
      acknowledged: false,
      durationMs: 0,
    }
    try {
      const outcome = await controller.cancel(sessionID, {
        complete: owned.completionPromise,
      })
      resolved =
        outcome.mode === "cooperative" && !outcome.acknowledged && phaseOf(owned).completed
          ? { ...outcome, mode: "completed", winner: "completion" }
          : outcome
      return resolved
    } finally {
      // A controller detached mid-cancel is disposed once its last cancel settles.
      if (controllers.get(worker.id) !== controller && controller.pending === 0) controller.dispose()
      owned.phase = { ...owned.phase, cancelPending: false }
      // The controller terminates/reaps on the forced path; a cooperative or
      // completed outcome keeps the worker. Only the captured run is released.
      await settleCancelled(sessionID, owned, {
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

  let shutdown: Promise<void> | undefined
  const terminateAll = (): Promise<void> => {
    shutdown ??= runShutdown()
    return shutdown
  }

  const runShutdown = async (): Promise<void> => {
    // The shutdown latch, the run table, and the waiter table flip together:
    // a dispatch can either observe the pre-shutdown state or the latch, never a
    // cleared table that still admits work.
    terminated = true
    const pendingRuns = [...runs.values()]
    const pendingWaiters = [...waiters.values()].flat()
    runs.clear()
    waiters.clear()
    const reaping = [...engine.reaping].map(([sessionID]) => sessionID)
    for (const sessionID of reaping) clearIdle(sessionID)
    for (const pending of pendingRuns) rejectRun(pending, cancelled(pending.sessionID))
    for (const pending of pendingWaiters) rejectRun(pending, cancelled(pending.sessionID))
    for (const controller of controllers.values()) controller.dispose()
    controllers.clear()
    const workersToTerminate = workers.entries().map((entry) => entry.worker)
    workers.clear()
    // Every direct terminate stays bounded; a wedged isolate must not hang
    // shutdown, because the pool and backends are torn down after this.
    await runPromise(
      Effect.forEach(
        workersToTerminate,
        (worker) =>
          Effect.tryPromise({ try: () => worker.terminate().catch(() => undefined), catch: (error) => error }).pipe(
            Effect.timeoutOrElse({ duration: terminateTimeoutMs, orElse: () => Effect.void }),
          ),
        { concurrency: "unbounded", discard: true },
      ),
    )
    // A retirement's Deferred completes ONLY in its fiber's `onExit`. Interrupt
    // the fiber first so a wedged terminate stops waiting, then let the
    // idempotent release settle; the same owner completes the Deferred.
    for (const [, fiber] of engine.retiring) fiber.interruptUnsafe()
    const exits = [...retiringDone.values()]
    await runPromise(
      Effect.forEach(
        exits,
        (exit) =>
          Deferred.await(exit).pipe(Effect.timeoutOrElse({ duration: terminateTimeoutMs, orElse: () => Effect.void })),
        { concurrency: "unbounded", discard: true },
      ),
    )
    retiringDone.clear()
    // Release timers and standby fibers; a wedged release is bounded by the
    // timeout rather than awaited forever.
    await runPromise(
      Effect.all([FiberMap.clear(engine.reaping), FiberSet.clear(engine.background)], {
        concurrency: "unbounded",
        discard: true,
      }).pipe(Effect.timeoutOrElse({ duration: terminateTimeoutMs, orElse: () => Effect.void })),
    )
    await pool.terminateAll().catch(() => undefined)
    cancellation?.reapAll?.()
    disposed = true
    await runPromise(Scope.close(scope, Exit.void))
  }

  return {
    get busySlotCount() {
      return runs.size
    },
    get queuedCount() {
      let count = 0
      for (const parked of waiters.values()) count += parked.length
      return count
    },
    get retiringCount() {
      return retiringDone.size
    },
    registry: workers,
    workerForSession: (sessionID) => workers.get(sessionID)?.workerID,
    dispatch,
    dispatchEffect: (request) => Effect.tryPromise({ try: () => dispatch(request), catch: (error) => error }),
    cancel,
    cancelWithGrace,
    retire,
    retireGeneration,
    awaitRelease,
    terminateAll,
    dispose: terminateAll,
    [Symbol.asyncDispose]: terminateAll,
  }
}

export interface Interface {
  readonly dispatcher: Dispatcher
  readonly ensureRunning: (request: DispatchRequest) => Effect.Effect<void, unknown>
  readonly cancel: (sessionID: SessionID) => Effect.Effect<CascadeReport | undefined, unknown>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionDispatch") {}

export const make = (options: DispatcherOptions): Effect.Effect<Dispatcher, never, Scope.Scope> =>
  Effect.gen(function* () {
    const scope = yield* Scope.Scope
    const engine = yield* makeEngine
    const runPromise = yield* FiberSet.makeRuntimePromise()
    return buildDispatcher(options, engine, runPromise, scope)
  })

export const layer = (options: DispatcherOptions): Layer.Layer<Service> =>
  Layer.effect(
    Service,
    Effect.gen(function* () {
      const dispatcher = yield* make(options)
      yield* Effect.addFinalizer(() =>
        Effect.tryPromise({ try: () => dispatcher.terminateAll(), catch: (cause) => cause }).pipe(Effect.ignore),
      )
      return Service.of({
        dispatcher,
        ensureRunning: (request) => dispatcher.dispatchEffect(request),
        cancel: (sessionID) =>
          Effect.tryPromise({ try: () => dispatcher.cancelWithGrace(sessionID), catch: (cause) => cause }),
      })
    }),
  )

export const node = (options: DispatcherOptions) =>
  LayerNode.make({ service: Service, layer: layer(options), deps: [] })

export * as SessionDispatch from "./dispatch"
