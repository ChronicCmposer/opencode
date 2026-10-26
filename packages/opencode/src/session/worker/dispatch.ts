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
import type { PromptInput, ShellInput } from "@/session/prompt"
import { defined } from "@/util/object"
import { createSessionWorkerRegistry } from "./session-registry"
import { createSessionRegistry, type SessionRegistry } from "./registry"
import { defaultSchedule, messageOf } from "./port"
import { DEFAULT_REAP_MS, REAP_MS_ENV, resolveReapMs } from "./pool"
import { noopWorkerLogger, workerLogMessage, type WorkerLogger } from "./log"
import { SessionWorkerReplyTimeoutError } from "./execute"
import type { ExecutorRun, ExecutorSessionWorker, SessionRunMode } from "./execute"
import {
  createCancelController,
  createOutcomeGate,
  type CancelController,
  type CancelMode,
  type CancelOutcome,
  type CancelPort,
  type CancelScheduler,
  type OutcomeGate,
} from "./cancel"

/** Approved idle-reap window for a session worker. */
export const DEFAULT_SESSION_WORKER_REAP_MS = DEFAULT_REAP_MS

/** Environment override for {@link DEFAULT_SESSION_WORKER_REAP_MS}. */
export const SESSION_WORKER_REAP_ENV = REAP_MS_ENV

/** Resolve the idle-reap window via the shared pool resolver (one source of truth). */
export function resolveSessionWorkerReapMs(
  options: { readonly env?: Record<string, string | undefined> } = {},
): number {
  return resolveReapMs(options.env ?? process.env)
}

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
  /** The underlying isolate failure, when one was captured. */
  cause: Schema.optional(Schema.Defect()),
}) {}

/** The underlying failure's text, embedded in a crash error so its cause is never lost. */
function describeFailure(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

/** Raised to the caller when a session is cancelled. */
export class SessionCancelledError extends Schema.TaggedErrorClass<SessionCancelledError>()("SessionCancelledError", {
  message: Schema.String,
  sessionID: Schema.String,
}) {}

/** The terminal mode reported when a session is cancelled. */
export type CancelledMode = CancelMode | "queued"

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
  /** Hub-side cleanup when a session's worker is retired (reap/cancel/crash). */
  readonly onSessionReleased?: (sessionID: SessionID, workerID?: number) => void
}

export interface Dispatcher extends AsyncDisposable {
  readonly busySlotCount: number
  readonly queuedCount: number
  readonly registry: SessionRegistry
  /** The worker currently hosting `sessionID`'s active run, or `undefined` when idle. */
  workerForSession(sessionID: SessionID): number | undefined
  dispatch(request: DispatchRequest): Promise<void>
  cancel(sessionID: SessionID): Promise<void>
  cancelWithGrace(sessionID: SessionID): Promise<CancelOutcome | undefined>
  terminateAll(): Promise<void>
  dispose(): Promise<void>
}

interface Pending {
  readonly sessionID: SessionID
  readonly depth: number
  readonly mode: SessionRunMode
  readonly prompt: PromptInput | undefined
  readonly shell: ShellInput | undefined
  readonly promise: Promise<void>
  readonly resolve: () => void
  readonly reject: (error: unknown) => void
  readonly settled: Promise<void>
  readonly markSettled: () => void
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
  return {
    sessionID,
    depth,
    mode,
    prompt,
    shell,
    promise,
    resolve,
    reject,
    settled,
    markSettled,
    completed: false,
    gate: createOutcomeGate(),
    startedAt: undefined,
    workerID: undefined,
    cancelPending: false,
  }
}

export function createDispatcher(options: DispatcherOptions): Dispatcher {
  const pool = options.pool
  const cancellation = options.cancellation
  const log = options.log ?? noopWorkerLogger
  const writeBarrier = options.writeBarrier
  const schedule = options.schedule ?? defaultSchedule
  const reapMs = options.reapMs ?? resolveSessionWorkerReapMs()

  // The session→worker map. Workers outlive runs (reuse across turns) until
  // idle-reap or terminal cancel disposes them.
  const workers = createSessionWorkerRegistry<SessionWorker>()
  const reservations = createSessionRegistry()
  const byKey = new Map<SessionID, Pending>()
  const waitersByKey = new Map<SessionID, Pending[]>()
  const idleTimers = new Map<SessionID, () => void>()
  const controllers = new Map<number, CancelController>()
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
    const worker = workers.get(sessionID)
    if (worker === undefined) return
    const active = await hasActiveWork(worker, sessionID)
    // Re-check after the await: a new run or parked waiter must win, and a
    // retired/replaced worker must not be touched by this stale timer.
    if (terminated || byKey.has(sessionID) || hasWaiters(sessionID)) return
    if (workers.get(sessionID) !== worker) return
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
    if (terminated || workers.get(sessionID) === undefined) return
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
  const retireWorker = (sessionID: SessionID): SessionWorker | undefined => {
    clearIdle(sessionID)
    const worker = workers.dispose(sessionID)
    if (worker === undefined) return undefined
    disposeController(worker.id)
    void worker.terminate().catch(() => undefined)
    pool.releaseSession?.(sessionID, worker.id)
    options.onSessionReleased?.(sessionID, worker.id)
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

  const start = (pending: Pending): void => {
    if (terminated) {
      pending.gate.claim("cancel")
      pending.reject(cancelled(pending.sessionID))
      return
    }
    clearIdle(pending.sessionID)
    const worker = workers.getOrCreate(pending.sessionID, (id) => pool.spawn(id))
    pending.startedAt = Date.now()
    pending.workerID = worker.id
    reservations.reserve(pending.sessionID, worker.id)
    reservations.markRunning(pending.sessionID)
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

  const finalizeCompletion = (pending: Pending): void => {
    // A cancel owns this run's terminal decision: keep the run visible in `byKey`
    // so a prompt arriving mid-grace parks instead of starting on the worker the
    // cancel is about to retire. `cancelWithGrace` releases the exact run.
    if (pending.cancelPending) return
    reservations.release(pending.sessionID)
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

  const fail = (pending: Pending, error: unknown): void => {
    pending.markSettled()
    // Same ownership rule as `finalizeCompletion`: the cancel settles this run.
    if (pending.cancelPending) return
    reservations.release(pending.sessionID)
    byKey.delete(pending.sessionID)
    if (pending.gate.winner === "cancel") {
      pending.reject(cancelled(pending.sessionID))
      nextWaiter(pending.sessionID)
      return
    }
    const timedOut = error instanceof SessionWorkerReplyTimeoutError
    const worker = workers.get(pending.sessionID)
    const crashed = error instanceof WorkerCrashedError || (worker !== undefined && !worker.alive)
    if (timedOut) {
      // A wedged isolate is retired, never requeued; cascade to its subagents.
      pending.reject(error)
      void cancelWithGrace(pending.sessionID).catch(() => undefined)
      retireWorker(pending.sessionID)
    } else if (crashed) {
      log(workerLogMessage("worker crashed"), {
        level: "WARN",
        sessionID: pending.sessionID,
        workerID: worker?.id ?? pending.workerID,
        reason: "crash",
      })
      pending.reject(
        error instanceof WorkerCrashedError
          ? error
          : new WorkerCrashedError({
              message: `worker ${worker?.id ?? pending.workerID ?? -1} crashed: ${describeFailure(error)}`,
              workerID: worker?.id ?? pending.workerID ?? -1,
              cause: error,
            }),
      )
      // A parent crash is an implicit cancel of its subagent children.
      void cancelWithGrace(pending.sessionID).catch(() => undefined)
      retireWorker(pending.sessionID)
    } else {
      log(workerLogMessage("worker run failed"), {
        level: "ERROR",
        sessionID: pending.sessionID,
        workerID: pending.workerID,
        error: describeFailure(error),
        cause: error,
      })
      pending.reject(error)
    }
    nextWaiter(pending.sessionID)
  }

  /** Start the next parked run for a session, or arm the idle-reap timer. */
  const nextWaiter = (sessionID: SessionID): void => {
    const waiters = waitersByKey.get(sessionID)
    if (waiters === undefined || waiters.length === 0) {
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

  /** Whether an in-flight run is the exact same run as a new request. */
  const isSameRun = (
    admitted: Pending,
    depth: number,
    mode: SessionRunMode,
    prompt: PromptInput | undefined,
    shell: ShellInput | undefined,
  ): boolean =>
    admitted.depth === depth &&
    admitted.mode === mode &&
    admitted.prompt === prompt &&
    admitted.shell === shell

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

    const admitted = byKey.get(sessionID)
    if (admitted !== undefined) {
      if (isSameRun(admitted, depth, mode, prompt, shell)) return admitted.promise
      // Different run identity: park behind the in-flight run. Unbounded.
      clearIdle(sessionID)
      const pending = createPending(sessionID, depth, mode, prompt, shell)
      const waiters = waitersByKey.get(sessionID)
      if (waiters === undefined) waitersByKey.set(sessionID, [pending])
      else waiters.push(pending)
      return pending.promise
    }

    const pending = createPending(sessionID, depth, mode, prompt, shell)
    byKey.set(sessionID, pending)
    start(pending)
    return pending.promise
  }

  /** Remove one still-parked waiter for `sessionID` (never a running run). */
  const removeQueued = (sessionID: SessionID): Pending | undefined => {
    const waiters = waitersByKey.get(sessionID)
    if (waiters === undefined || waiters.length === 0) return undefined
    const [queued] = waiters.splice(0, 1)
    if (waiters.length === 0) waitersByKey.delete(sessionID)
    queued.gate.claim("cancel")
    queued.reject(cancelled(sessionID))
    return queued
  }

  const cancel = async (sessionID: SessionID): Promise<void> => {
    const error = cancelled(sessionID)
    // A session's parked waiters are all cancelled with it; the running turn (if
    // any) is cancelled below rather than left orphaned.
    removeQueued(sessionID)
    rejectWaiters(sessionID, error)

    const admitted = byKey.get(sessionID)
    if (admitted === undefined) return
    reservations.markCancelling(sessionID)
    // Claim the gate BEFORE terminating so the terminate-induced rejection is a
    // cancellation, never a crash or a spurious run-failure line.
    admitted.gate.claim("cancel")
    rejectWaiters(sessionID, error)
    byKey.delete(sessionID)
    reservations.release(sessionID)
    retireWorker(sessionID)
    admitted.reject(error)
  }

  const cancelWithGrace = async (
    sessionID: SessionID,
    visited: ReadonlySet<SessionID> = new Set<SessionID>(),
  ): Promise<CancelOutcome | undefined> => {
    if (visited.has(sessionID)) return undefined
    const seen = new Set(visited).add(sessionID)

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

    for (const child of config.childrenOf?.(sessionID) ?? []) await cancelWithGrace(child, seen)

    // The run we meant to cancel already settled and something else owns the
    // session now: never cancel the newcomer.
    if (byKey.get(sessionID) !== owned) return undefined

    const queued = removeQueued(sessionID)
    rejectWaiters(sessionID, cancelled(sessionID))
    const admitted = owned
    if (admitted === undefined) {
      if (queued === undefined) return undefined
      config.onCancelled?.({ sessionID, mode: "queued", workerID: undefined, resumeFromDb: true })
      return { sessionID, mode: "terminated", winner: "cancel", acknowledged: false, durationMs: 0 }
    }

    reservations.markCancelling(sessionID)
    const worker = workers.get(sessionID)
    const port = worker === undefined ? undefined : config.portFor?.(worker)

    rejectWaiters(sessionID, cancelled(sessionID))

    if (worker === undefined || port === undefined) {
      admitted.gate.claim("cancel")
      byKey.delete(sessionID)
      reservations.release(sessionID)
      if (worker !== undefined) cancellation?.reap?.(worker)
      retireWorker(sessionID)
      admitted.reject(cancelled(sessionID))
      config.onCancelled?.({ sessionID, mode: "terminated", workerID: worker?.id, resumeFromDb: true })
      return { sessionID, mode: "terminated", winner: "cancel", acknowledged: false, durationMs: 0 }
    }

    admitted.cancelPending = true
    let resolved: CancelOutcome = {
      sessionID,
      mode: "terminated",
      winner: "cancel",
      acknowledged: false,
      durationMs: 0,
    }
    try {
      const outcome = await controllerFor(config, worker, port).cancel(sessionID, { complete: admitted.settled })
      resolved =
        outcome.mode === "cooperative" && !outcome.acknowledged && admitted.completed
          ? { ...outcome, mode: "completed", winner: "completion" }
          : outcome

      // Only release/retire the run captured before the await. A prompt that
      // parked during the grace is resumed by `nextWaiter` below, on a fresh
      // worker when the terminated path retired this one; a run that already
      // owns `byKey` is left alone.
      if (byKey.get(sessionID) === admitted) {
        byKey.delete(sessionID)
        reservations.release(sessionID)
        if (resolved.mode === "terminated") retireWorker(sessionID)
      }
      config.onCancelled?.({ sessionID, mode: resolved.mode, workerID: worker.id, resumeFromDb: true })
      return resolved
    } finally {
      admitted.cancelPending = false
      if (resolved.winner === "completion") admitted.resolve()
      else admitted.reject(cancelled(sessionID))
      // Resume a prompt that arrived during the grace now that this cancel no
      // longer owns the terminal decision. `nextWaiter` re-arms idle reaping
      // when nothing is parked. If a newer run already owns `byKey`, leave it.
      if (!byKey.has(sessionID)) nextWaiter(sessionID)
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
    reservations.clear()
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
    registry: reservations,
    workerForSession: (sessionID) => reservations.get(sessionID)?.workerID,
    dispatch,
    cancel,
    cancelWithGrace,
    terminateAll,
    dispose: terminateAll,
    [Symbol.asyncDispose]: terminateAll,
  }
}

export * as SessionDispatch from "./dispatch"
