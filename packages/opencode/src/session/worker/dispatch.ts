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

import { Schema } from "effect"
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

interface Pending {
  readonly sessionID: SessionID
  readonly depth: number
  readonly mode: SessionRunMode
  readonly prompt: PromptInput | undefined
  readonly shell: ShellInput | undefined
  readonly model: { readonly providerID: string; readonly modelID: string } | undefined
  readonly promise: Promise<void>
  readonly resolve: () => void
  readonly reject: (error: unknown) => void
  readonly settled: Promise<void>
  readonly markSettled: () => void
  /** True once the run has reached a terminal observation (completion/failure). */
  settledFlag: boolean
  completed: boolean
  readonly gate: OutcomeGate
  startedAt: number | undefined
  workerID: number | undefined
  cancelPending: boolean
}

function createPending(
  sessionID: SessionID,
  depth: number,
  mode: SessionRunMode,
  prompt: PromptInput | undefined,
  shell: ShellInput | undefined,
  model: { readonly providerID: string; readonly modelID: string } | undefined,
): Pending {
  let resolve!: () => void
  let reject!: (error: unknown) => void
  const promise = new Promise<void>((res, rej) => {
    resolve = res
    reject = rej
  })
  let markSettled!: () => void
  const settled = new Promise<void>((res) => {
    markSettled = res
  })
  const pending: Pending = {
    sessionID,
    depth,
    mode,
    prompt,
    shell,
    model,
    promise,
    resolve,
    reject,
    settled,
    markSettled: () => {
      pending.settledFlag = true
      markSettled()
    },
    settledFlag: false,
    completed: false,
    gate: createOutcomeGate(),
    startedAt: undefined,
    workerID: undefined,
    cancelPending: false,
  }
  return pending
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
  const byKey = new Map<SessionID, Pending>()
  const waitersByKey = new Map<SessionID, Pending[]>()
  const idleTimers = new Map<SessionID, () => void>()
  const controllers = new Map<number, CancelController>()
  /**
   * The confirmed-exit promise for a retiring generation. A successor must not
   * start until the old isolate is gone and its lease release has landed.
   */
  const terminating = new Map<SessionID, Promise<void>>()
  let terminated = false

  const cancelled = (sessionID: SessionID): SessionCancelledError =>
    new SessionCancelledError({ message: `session ${sessionID} was cancelled`, sessionID })

  const clearIdle = (sessionID: SessionID): void => {
    const cancel = idleTimers.get(sessionID)
    if (cancel === undefined) return
    cancel()
    idleTimers.delete(sessionID)
  }

  const hasWaiters = (sessionID: SessionID): boolean => (waitersByKey.get(sessionID)?.length ?? 0) > 0

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
   * is deliberately NOT part of `byKey`/waiters: it must not make a new prompt
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
  const reapIfIdle = async (sessionID: SessionID): Promise<void> => {
    idleTimers.delete(sessionID)
    if (terminated || byKey.has(sessionID) || hasWaiters(sessionID)) return
    const worker = workers.worker(sessionID)
    if (worker === undefined) return
    const active = await hasActiveWork(worker, sessionID)
    // Re-check after the await: a new run or parked waiter must win, and a
    // retired/replaced worker must not be touched by this stale timer.
    if (terminated || byKey.has(sessionID) || hasWaiters(sessionID)) return
    if (workers.worker(sessionID) !== worker) return
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
  }

  const scheduleReap = (sessionID: SessionID): void => {
    clearIdle(sessionID)
    if (terminated || workers.worker(sessionID) === undefined) return
    idleTimers.set(
      sessionID,
      schedule(reapMs, () => {
        void reapIfIdle(sessionID)
      }),
    )
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
    let exit!: Promise<void>
    exit = worker
      .terminate()
      .catch(() => undefined)
      .then(async () => {
        try {
          pool.releaseSession?.(sessionID, worker.id)
          await options.onSessionReleased?.(sessionID, worker.id)
        } catch (error) {
          // A callback that throws must not leave `exit` rejected (nobody may be
          // awaiting it) nor park the successor: log and treat it as settled.
          log(workerLogMessage("session release callback failed"), {
            level: "WARN",
            sessionID,
            workerID: worker.id,
            reason: messageOf(error),
          })
        }
      })
      .finally(() => {
        if (terminating.get(sessionID) === exit) terminating.delete(sessionID)
      })
    terminating.set(sessionID, exit)
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
  const awaitTerminating = async (sessionID: SessionID, exit: Promise<void>): Promise<void> => {
    let timer: ReturnType<typeof setTimeout> | undefined
    const deadline = new Promise<void>((resolve) => {
      timer = setTimeout(() => {
        log(workerLogMessage("retirement exit timed out"), {
          level: "WARN",
          sessionID,
          timeoutMs: terminateTimeoutMs,
          reason: "the retiring generation never confirmed its exit; starting the successor",
        })
        resolve()
      }, terminateTimeoutMs)
      timer.unref?.()
    })
    try {
      await Promise.race([
        exit.catch((error: unknown) => {
          log(workerLogMessage("retirement exit failed"), {
            level: "WARN",
            sessionID,
            reason: messageOf(error),
          })
        }),
        deadline,
      ])
    } finally {
      if (timer !== undefined) clearTimeout(timer)
    }
  }

  /**
   * Park a run until the previous generation's confirmed exit (or the bounded
   * deadline). The same helper is used both for an already-retiring generation
   * and for the retirement a dead-idle replacement just started, so a successor
   * can never materialize against a live old isolate.
   */
  const deferUntilRetired = (pending: Pending, exit: Promise<void>): void => {
    void (async () => {
      await awaitTerminating(pending.sessionID, exit)
      if (terminating.get(pending.sessionID) === exit) terminating.delete(pending.sessionID)
      if (byKey.get(pending.sessionID) !== pending) return
      if (terminated) {
        pending.gate.claim("cancel")
        pending.reject(cancelled(pending.sessionID))
        return
      }
      start(pending)
    })()
  }

  /**
   * Retire one session's generation on behalf of the pool's idle cap. Idempotent:
   * a session with no registered worker resolves immediately.
   */
  const retire = (sessionID: SessionID): Promise<void> => {
    retireWorker(sessionID)
    return terminating.get(sessionID) ?? Promise.resolve()
  }

  /** The explicit release barrier for a retirement that is already in flight. */
  const awaitRelease = (sessionID: SessionID): Promise<void> => terminating.get(sessionID) ?? Promise.resolve()

  const start = (pending: Pending): void => {
    if (terminated) {
      pending.gate.claim("cancel")
      pending.reject(cancelled(pending.sessionID))
      return
    }
    // A build that became inadmissible while this run waited (a crash blocked
    // the bundle) must reject here too, before any isolate is materialized.
    const blocked = inadmissible(pending.sessionID)
    if (blocked !== undefined) {
      if (ownsSession(pending)) byKey.delete(pending.sessionID)
      if (pending.gate.claim("cancel")) pending.reject(blocked)
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
    pending.startedAt = Date.now()
    pending.workerID = worker.id
    workers.markRunning(pending.sessionID)
    log(workerLogMessage("session admitted"), {
      level: "INFO",
      sessionID: pending.sessionID,
      workerID: worker.id,
      depth: pending.depth,
      queueDepth: waitersByKey.get(pending.sessionID)?.length ?? 0,
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
    pending.completed = true
    pending.markSettled()
    const barrier = pending.cancelPending ? undefined : writeBarrier?.(pending.sessionID)
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
   * settle its worker protocol arbitrarily late; only the run `byKey` points at
   * may mutate the session maps, reservations, waiters, or worker.
   */
  const ownsSession = (pending: Pending): boolean => byKey.get(pending.sessionID) === pending

  /**
   * Settle a superseded run's own promise without touching shared session state.
   * Its gate is claimed first so the stale run never hangs: if a cancel already
   * claimed it the promise is settled, otherwise it is settled as cancelled.
   */
  const settleSuperseded = (pending: Pending): void => {
    if (pending.gate.claim("cancel")) pending.reject(cancelled(pending.sessionID))
  }

  const finalizeCompletion = (pending: Pending): void => {
    // A cancel owns this run's terminal decision: keep the run visible in `byKey`
    // so a prompt arriving mid-grace parks instead of starting on the worker the
    // cancel is about to retire. `cancelWithGrace` releases the exact run.
    if (pending.cancelPending) return
    // A newer run owns the session (cancel + immediate re-dispatch): settling this
    // stale run must not release the successor's reservation, delete its `byKey`
    // entry, retire its worker, or promote its waiters.
    if (!ownsSession(pending)) {
      settleSuperseded(pending)
      return
    }
    workers.release(pending.sessionID)
    byKey.delete(pending.sessionID)
    log(workerLogMessage("session completed"), {
      level: "INFO",
      sessionID: pending.sessionID,
      workerID: pending.workerID,
      durationMs: pending.startedAt === undefined ? 0 : Date.now() - pending.startedAt,
    })
    if (pending.gate.claim("completion")) pending.resolve()
    else pending.reject(cancelled(pending.sessionID))
    nextWaiter(pending.sessionID)
  }

  /** The typed D8 payload for one crash, with the error's own context winning. */
  const crashPayloadFor = (pending: Pending, error: unknown): WorkerCrashed => {
    const reported = error instanceof WorkerCrashedError ? error : undefined
    const state = options.crashState?.(pending.sessionID)
    const phase = isWorkerCrashPhase(reported?.phase) ? reported.phase : (state?.phase ?? "stream")
    const payload = workerCrashed({
      sessionID: pending.sessionID,
      workerID: reported?.workerID ?? workers.worker(pending.sessionID)?.id ?? pending.workerID ?? -1,
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
      const owned = byKey.get(childSessionID)
      const worker = workers.worker(childSessionID)
      if (owned !== undefined) {
        owned.gate.claim("cancel")
        owned.reject(failure)
        byKey.delete(childSessionID)
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
   * While that read is pending the run keeps `byKey`/worker ownership, so a
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
    if (!ownsSession(pending)) {
      pending.reject(failure)
      return
    }
    workers.release(pending.sessionID)
    byKey.delete(pending.sessionID)
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
            if (pending.gate.claim("completion")) pending.resolve()
            else pending.reject(cancelled(pending.sessionID))
          },
          (error: unknown) => {
            pending.reject(error)
            nextWaiter(pending.sessionID)
          },
        )
        return
      }
      // A resume owner that returns no run promise cannot deliver its outcome:
      // the caller still observes the typed crash (never a silent success).
    }
    pending.reject(failure)
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
    pending.markSettled()
    // A dead isolate must never survive a cancel that owns this run: the cancel
    // settles the terminal decision, but retirement and the child cascade still
    // belong to this crash.
    if (pending.cancelPending) {
      retireDeadCancelledWorker(pending.sessionID)
      return
    }
    // A late rejection from a cancelled/superseded run must not settle the
    // successor that now owns this session.
    if (!ownsSession(pending)) {
      settleSuperseded(pending)
      return
    }
    if (pending.gate.winner === "cancel") {
      workers.release(pending.sessionID)
      byKey.delete(pending.sessionID)
      retireDeadCancelledWorker(pending.sessionID)
      pending.reject(cancelled(pending.sessionID))
      nextWaiter(pending.sessionID)
      return
    }
    const timedOut = error instanceof SessionWorkerReplyTimeoutError
    const worker = workers.worker(pending.sessionID)
    const crashed = error instanceof WorkerCrashedError || (worker !== undefined && !worker.alive)
    if (timedOut) {
      // A wedged isolate is retired, never requeued; cascade to its subagents.
      workers.release(pending.sessionID)
      byKey.delete(pending.sessionID)
      pending.reject(error)
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
        workerID: pending.workerID,
        error: describeFailure(error),
        cause: error,
      })
      workers.release(pending.sessionID)
      byKey.delete(pending.sessionID)
      pending.reject(error)
    }
    nextWaiter(pending.sessionID)
  }

  /** Start the next parked run for a session, or arm the idle-reap timer. */
  const nextWaiter = (sessionID: SessionID): void => {
    // A settlement can admit a successor as a side effect (auto-resume dispatches
    // synchronously). Never overwrite that live owner with a parked waiter.
    if (byKey.has(sessionID)) return
    const waiters = waitersByKey.get(sessionID)
    if (waiters === undefined || waiters.length === 0) {
      scheduleReap(sessionID)
      return
    }
    // A waiter promoted after a crash must not respawn a bundle that is no
    // longer admissible. The typed refusal is the caller's outcome, not a run.
    const blocked = inadmissible(sessionID)
    if (blocked !== undefined) {
      waitersByKey.delete(sessionID)
      for (const waiter of waiters) {
        if (waiter.gate.claim("cancel")) waiter.reject(blocked)
      }
      scheduleReap(sessionID)
      return
    }
    const next = waiters.shift()!
    log(workerLogMessage("waiter promoted"), {
      level: "INFO",
      sessionID,
      waiterQueueDepth: waiters.length,
    })
    if (waiters.length === 0) waitersByKey.delete(sessionID)
    byKey.set(sessionID, next)
    start(next)
  }

  const rejectWaiters = (sessionID: SessionID, error: unknown): void => {
    const waiters = waitersByKey.get(sessionID)
    if (waiters === undefined) return
    waitersByKey.delete(sessionID)
    for (const waiter of waiters) {
      waiter.gate.claim("cancel")
      waiter.reject(error)
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
   * request captured; shared state is only touched while it still owns `byKey`.
   * Parked waiters are drained by the caller exactly once before any await, so a
   * waiter that arrives during the grace window survives and is promoted here.
   */
  const settleCancelled = (
    sessionID: SessionID,
    owned: Pending | undefined,
    settlement: CancelSettlement,
  ): void => {
    const { mode, winner, worker, retires, reaps } = settlement
    const owns = owned !== undefined && ownsSession(owned)

    if (owns) {
      byKey.delete(sessionID)
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
      if (winner === "completion" && owned.gate.claim("completion")) owned.resolve()
      else {
        owned.gate.claim("cancel")
        owned.reject(cancelled(sessionID))
      }
    }

    if (owned !== undefined) {
      cancellation?.onCancelled?.({ sessionID, mode, workerID: worker?.id, resumeFromDb: true })
    }

    if (!byKey.has(sessionID)) nextWaiter(sessionID)
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
    !admitted.settledFlag &&
    admitted.depth === depth &&
    admitted.mode === mode &&
    admitted.prompt === prompt &&
    admitted.shell === shell &&
    admitted.model?.providerID === model?.providerID &&
    admitted.model?.modelID === model?.modelID

  const dispatch = (request: DispatchRequest): Promise<void> => {
    // Shutdown owns the terminal state: a prompt racing `terminateAll`/`dispose`
    // must never construct an isolate after the pool and backends are gone.
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

    const admitted = byKey.get(sessionID)
    if (admitted !== undefined) {
      if (isSameRun(admitted, depth, mode, prompt, shell, model)) return admitted.promise
      // Different run identity: park behind the in-flight run. Unbounded.
      clearIdle(sessionID)
      const pending = createPending(sessionID, depth, mode, prompt, shell, model)
      const waiters = waitersByKey.get(sessionID)
      if (waiters === undefined) waitersByKey.set(sessionID, [pending])
      else waiters.push(pending)
      return pending.promise
    }

    const pending = createPending(sessionID, depth, mode, prompt, shell, model)
    byKey.set(sessionID, pending)
    start(pending)
    return pending.promise
  }

  const cancel = async (sessionID: SessionID): Promise<void> => {
    const owned = byKey.get(sessionID)
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
  const cancelOne = async (
    sessionID: SessionID,
    cascade: CascadeAccumulator,
  ): Promise<CancelOutcome | undefined> => {
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
    const owned = byKey.get(sessionID)

    for (const child of config.childrenOf?.(sessionID) ?? []) {
      if (cascade.seen.has(child)) continue
      const childOutcome = await cancelOne(child, cascade)
      cascade.descendants.push({ sessionID: child, mode: childOutcome?.mode })
    }

    // The run we meant to cancel already settled and something else owns the
    // session now: never cancel the newcomer.
    if (byKey.get(sessionID) !== owned) return undefined
    if (owned === undefined) return undefined

    // Drain waiters that exist at request time exactly once. A prompt that parks
    // during the grace window is NOT drained here; `settleCancelled` promotes it
    // onto a fresh worker after this cancel no longer owns the session.
    rejectWaiters(sessionID, cancelled(sessionID))

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

    owned.cancelPending = true
    let resolved: CancelOutcome = {
      sessionID,
      mode: "terminated",
      winner: "cancel",
      acknowledged: false,
      durationMs: 0,
    }
    try {
      const outcome = await controllerFor(config, worker, port).cancel(sessionID, { complete: owned.settled })
      resolved =
        outcome.mode === "cooperative" && !outcome.acknowledged && owned.completed
          ? { ...outcome, mode: "completed", winner: "completion" }
          : outcome
      return resolved
    } finally {
      owned.cancelPending = false
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
    if (terminated) return
    terminated = true
    for (const cancel of idleTimers.values()) cancel()
    idleTimers.clear()
    for (const pending of byKey.values()) pending.reject(cancelled(pending.sessionID))
    for (const waiters of waitersByKey.values()) {
      for (const pending of waiters) pending.reject(cancelled(pending.sessionID))
    }
    byKey.clear()
    waitersByKey.clear()
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
      return byKey.size
    },
    get queuedCount() {
      let count = 0
      for (const waiters of waitersByKey.values()) count += waiters.length
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
