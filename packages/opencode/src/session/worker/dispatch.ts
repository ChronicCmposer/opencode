// Phase 4.3–4.9 — session worker dispatch core (plan tasks 4.3–4.9).
//
// The hub-side scheduler. It owns:
//   • one active session per worker (no multiplexing);
//   • a bounded FIFO admission queue for foreground sessions beyond K;
//   • on-demand headroom (max(2, ceil(K/2))) used ONLY by depth-1 children,
//     with a bounded child queue behind it;
//   • at-most-once crash handling: reject the caller, never requeue, respawn
//     with backoff, and degrade the slot after a crash loop;
//   • the key→worker reservation map lifecycle;
//   • idle-reap of headroom workers.
//
// The scheduler depends on the `WorkerPool` interface (task 4.9), not on Bun
// workers directly, so scheduling is deterministic in tests. The production
// `ExecutorWorkerPool`/`WorkerPool` adapter lives in `hub.ts`
// (`createProductionWorkerPool`).

import { Schema } from "effect"
import type { SessionID } from "@/session/schema"
import type { PromptInput, ShellInput } from "@/session/prompt"
import { defined } from "@/util/object"
import { parsePositiveInt } from "@/util/parse"
import {
  ADMISSION_QUEUE_CAPACITY,
  CHILD_QUEUE_CAPACITY,
  WAITER_QUEUE_CAPACITY,
  SessionQueueFullError,
  createBoundedQueue,
  type BoundedQueue,
} from "./queue"
import { createSessionRegistry, type SessionRegistry } from "./registry"
import { createSupervision, type Supervision } from "./supervision"
import { defaultSchedule } from "./port"
import { noopWorkerLogger, workerLogMessage, type WorkerLogger } from "./log"
import { SessionWorkerReplyTimeoutError } from "./execute"
import type {
  ExecutorRun,
  ExecutorSessionWorker,
  ExecutorWorkerKind,
  ExecutorWorkerPool,
  SessionRunMode,
} from "./execute"
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

export { SessionQueueFullError } from "./queue"

/** Approved minimum headroom (plan decision line 71). */
export const HEADROOM_MIN = 2

/** Approved headroom idle-reap window (plan decision line 71). */
export const DEFAULT_HEADROOM_IDLE_REAP_MS = 60_000

/**
 * Headroom size for the foreground pool: `max(HEADROOM_MIN, ceil(K/2))`, where
 * K = nproc × F (F = 2) is the spec's "total worker threads" target for the
 * FOREGROUND pool.
 *
 * Headroom is ADDITIVE and TRANSIENT: it is spawned on demand for depth ≥ 1
 * children and idle-reaped, so it is never part of K. Read "total worker
 * threads" as the foreground target, not as a hard cap that includes headroom —
 * the process may briefly hold K plus this many workers while children run.
 */
export function deriveHeadroom(workerCount: number): number {
  parsePositiveInt(workerCount, "worker count")
  return Math.max(HEADROOM_MIN, Math.ceil(workerCount / 2))
}

/** Scheduling kind: foreground roots or on-demand depth-1 children (execute.ts owns the shape). */
export type WorkerKind = ExecutorWorkerKind

/**
 * The scheduling kind a run's depth selects: roots (depth 0) are foreground,
 * every nested child (depth ≥ 1) needs on-demand headroom. Encoding the rule as
 * one function keeps the scheduler's capacity checks from drifting from its
 * admission rule.
 */
export function workerKindForDepth(depth: number): WorkerKind {
  return depth >= 1 ? "headroom" : "foreground"
}

/** One session run: the key the worker executes, its subagent depth and its frame mode. */
export type SessionRun = ExecutorRun

/** A worker that executes exactly one session run at a time. */
export interface SessionWorker extends ExecutorSessionWorker {}

/** The scheduling seam (task 4.9): supplies and replaces workers. */
export interface WorkerPool {
  /** The K foreground workers. */
  readonly workers: ReadonlyArray<SessionWorker>
  /** Create one fresh worker of `kind` (headroom on demand, or crash respawn). */
  spawn(kind: WorkerKind): SessionWorker
  terminateAll(): Promise<void>
}

/**
 * Compile-time proof that the executor pool (`execute.ts`) satisfies the
 * scheduling seam: `createExecutorWorkerPool`'s result is a valid `WorkerPool`.
 */
type Assignable<From, To> = [From] extends [To] ? true : never
type ExecutorPoolFitsWorkerPool = Assignable<ExecutorWorkerPool, WorkerPool>

/**
 * Raised when a session has failed because its worker crashed (plan 11.7). It is
 * the single crash error: a crashed run rejects with this, and callers match it
 * by tag (`Effect.catchTag("WorkerCrashedError")`) or structurally. There is no
 * separate session-level crash class to keep in sync, so no prototype surgery.
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

/**
 * Raised when a depth ≥ 1 child cannot be scheduled: the physical headroom pool
 * is at its bound and the bounded child queue has no room, so the child is
 * refused loud rather than admitted past the physical limit (decision line 60).
 */
export class HeadroomFullError extends Schema.TaggedErrorClass<HeadroomFullError>()("HeadroomFullError", {
  message: Schema.String,
}) {}

/** The terminal mode reported when a session is cancelled (plan 11.10 adds `queued`). */
export type CancelledMode = CancelMode | "queued"

/** Emitted once when a session reaches its terminal cancelled state (plan 11.6). */
export interface CancelledEvent {
  readonly sessionID: SessionID
  readonly mode: CancelledMode
  readonly workerID: number | undefined
  /** Resume is user-triggered from the last durable cursor; the hub never requeues (plan 11.6). */
  readonly resumeFromDb: true
}

/**
 * Additive cancellation wiring (plan Phase 11). Supplying it upgrades
 * `cancelWithGrace` from the Phase-4 immediate-terminate path to cooperative
 * checkpoints + a grace window, with hub-side PGID reaping and subagent cascade.
 */
export interface CancellationConfig {
  /** The worker's control port, when it exposes one (cancel + ack; plan 11.2). */
  readonly portFor?: (worker: SessionWorker) => CancelPort | undefined
  /** Grace before unconditional terminate; defaults to `OPENCODE_SESSION_CANCEL_GRACE_MS`. */
  readonly graceMs?: number
  readonly schedule?: CancelScheduler
  /** Hub reaps the worker's mirrored PGIDs after a terminate (plan 11.5). */
  readonly reap?: (worker: SessionWorker) => void
  /** Hub reaps every mirrored PGID on shutdown, after the workers are gone (plan 11.5). */
  readonly reapAll?: () => void
  /** Subagent children of a session, cancelled children-first (plan 11.8). */
  readonly childrenOf?: (sessionID: SessionID) => ReadonlyArray<SessionID>
  readonly onCancelled?: (event: CancelledEvent) => void
}

export type Scheduler = (delayMs: number, run: () => void) => () => void

export interface DispatchRequest {
  readonly sessionID: SessionID
  /** 0 = foreground/root; ≥1 = subagent child. Defaults to 0. */
  readonly depth?: number
  /** Executor frame to run; defaults to `"run"` (a `"startShell"` takes the shell path). */
  readonly mode?: SessionRunMode
  /**
   * The sub-agent prompt the dispatched worker persists before looping
   * (Solution 1 / B2). Absent keeps a plain run byte-for-byte unchanged.
   */
  readonly prompt?: PromptInput
  /** The shell command for a `"startShell"` dispatch; absent for a plain run. */
  readonly shell?: ShellInput
}

export interface DispatcherOptions {
  readonly pool: WorkerPool
  /** Headroom bound; defaults to `deriveHeadroom(pool.workers.length)`. */
  readonly headroom?: number
  readonly admissionCapacity?: number
  readonly childCapacity?: number
  /** Bound on different-kind waiters parked behind one key; defaults to `WAITER_QUEUE_CAPACITY`. */
  readonly waiterCapacity?: number
  readonly headroomIdleReapMs?: number
  readonly supervision?: Supervision
  /** Timer seam; defaults to `setTimeout` (unref'd so it never blocks exit). */
  readonly schedule?: Scheduler
  readonly onDegraded?: (workerID: number) => void
  /** Phase 11 additive cancellation wiring; absent keeps the Phase-4 terminate path. */
  readonly cancellation?: CancellationConfig
  /** Optional structured logger seam; absent defaults to a no-op (flag-off behavior). */
  readonly log?: WorkerLogger
  /**
   * Per-session write barrier (R4#16). When supplied, a completed run is not
   * released (its slot freed, its waiters promoted) until the barrier for that
   * session resolves — the hub wires it to the worker's durable-egress ack, so
   * the next run never overtakes the prior run's egress write.
   */
  readonly writeBarrier?: (sessionID: SessionID) => Promise<void> | undefined
}

export interface Dispatcher extends AsyncDisposable {
  readonly busySlotCount: number
  readonly queuedCount: number
  readonly headroomSlotCount: number
  /** Physical headroom bound (Phase 9 admission seam): `headroomSlotCount >= headroomCapacity` is full. */
  readonly headroomCapacity: number
  readonly degradedCount: number
  readonly registry: SessionRegistry
  /** The worker currently hosting `sessionID`, or `undefined` when it is not admitted. */
  workerForSession(sessionID: SessionID): number | undefined
  dispatch(request: DispatchRequest): Promise<void>
  cancel(sessionID: SessionID): Promise<void>
  /**
   * Phase 11 cancel: cooperative checkpoint first, then terminate after the
   * grace window; reaps mirrored PGIDs and cascades to subagent children.
   * Returns `undefined` when no cancellation config is wired.
   */
  cancelWithGrace(sessionID: SessionID): Promise<CancelOutcome | undefined>
  terminateAll(): Promise<void>
  dispose(): Promise<void>
}

interface Slot {
  readonly key: number
  readonly kind: WorkerKind
  worker: SessionWorker
  busy: SessionID | null
  /**
   * True while the slot's isolate is being retired (a crash or a reply timeout):
   * the slot must never be handed another run until its replacement is spawned.
   * Guarding on this closes the window before `terminate()` resolves, when the
   * wedged isolate could otherwise still read as `alive`.
   */
  retiring: boolean
  /** Cancels the idle-reap timer (it holds a timer's canceller, not a cancel controller). */
  cancelIdleReap: (() => void) | undefined
  respawnCancel: (() => void) | undefined
}

interface Pending {
  readonly sessionID: SessionID
  readonly depth: number
  readonly kind: WorkerKind
  /** The executor frame this run produces; only `"startShell"` is carried on the run. */
  readonly mode: SessionRunMode
  /** The sub-agent prompt the run carries to the worker (Solution 1). */
  readonly prompt: PromptInput | undefined
  /** The shell command the run carries to the worker for a `"startShell"` run. */
  readonly shell: ShellInput | undefined
  readonly promise: Promise<void>
  readonly resolve: () => void
  readonly reject: (error: unknown) => void
  /**
   * Resolves when the underlying run settles, independent of the caller's
   * `promise`. A graceful cancel detects a settle through this, so deferring the
   * caller's settlement (plan 11.9) never hides the run's own completion.
   */
  readonly settled: Promise<void>
  readonly markSettled: () => void
  /**
   * True once the run finished NATURALLY (its executor promise resolved); stays
   * false when the run settled by rejecting (an interrupt/failure). The graceful
   * cancel reads it to tell a completion that won the race from a cancel that
   * unwound the run — a distinction the worker's `cancel-ack` cannot make alone,
   * because a real worker only acks after the run has already settled.
   */
  completed: boolean
  /** First-writer-wins between a cancel and a natural completion (plan 11.9). */
  readonly gate: OutcomeGate
  slot: Slot | undefined
  /** Wall-clock start, stamped when the run begins, for the completion duration line. */
  startedAt: number | undefined
  /** The worker id hosting the run, captured at admission so it survives slot detach. */
  workerID: number | undefined
  /**
   * True while a graceful cancel owns this run's settlement (plan 11.9): a
   * completion or failure must not settle the caller itself, because the
   * controller's winner decides whether the caller resolves or rejects. This is
   * what keeps the caller consistent with the reported outcome.
   */
  cancelPending: boolean
}

function createPending(
  sessionID: SessionID,
  depth: number,
  kind: WorkerKind,
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
    kind,
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
    slot: undefined,
    startedAt: undefined,
    workerID: undefined,
    cancelPending: false,
  }
}

export function createDispatcher(options: DispatcherOptions): Dispatcher {
  const pool = options.pool
  const supervision = options.supervision ?? createSupervision()
  const registry = createSessionRegistry()
  const schedule = options.schedule ?? defaultSchedule
  const headroomMax = options.headroom ?? deriveHeadroom(pool.workers.length)
  const admissionCapacity = options.admissionCapacity ?? ADMISSION_QUEUE_CAPACITY
  const childCapacity = options.childCapacity ?? CHILD_QUEUE_CAPACITY
  const waiterCapacity = options.waiterCapacity ?? WAITER_QUEUE_CAPACITY
  const headroomIdleReapMs = options.headroomIdleReapMs ?? DEFAULT_HEADROOM_IDLE_REAP_MS
  const onDegraded = options.onDegraded
  const cancellation = options.cancellation
  const log = options.log ?? noopWorkerLogger
  const writeBarrier = options.writeBarrier

  const admission: BoundedQueue<Pending> = createBoundedQueue("admission", admissionCapacity)
  const children: BoundedQueue<Pending> = createBoundedQueue("child", childCapacity)
  const byKey = new Map<SessionID, Pending>()
  const waitersByKey = new Map<SessionID, BoundedQueue<Pending>>()
  const controllers = new Map<number, CancelController>()
  const slots: Slot[] = []
  let nextSlotKey = 1
  let terminated = false

  const emptySlot = (kind: WorkerKind, worker: SessionWorker): Slot => ({
    key: nextSlotKey++,
    kind,
    worker,
    busy: null,
    retiring: false,
    cancelIdleReap: undefined,
    respawnCancel: undefined,
  })

  for (const worker of pool.workers) {
    slots.push(emptySlot("foreground", worker))
  }

  const freeSlot = (kind: WorkerKind): Slot | undefined =>
    slots.find((slot) => slot.kind === kind && slot.busy === null && !slot.retiring && slot.worker.alive)

  // Capacity counts only slots that can actually take a run: a dead or retiring
  // headroom slot must not read as free capacity (that would falsely refuse a
  // child as "headroom full" while its replacement is still spawning).
  const countHeadroomSlots = (): number =>
    slots.filter((slot) => slot.kind === "headroom" && !slot.retiring && slot.worker.alive).length
  const countBusySlots = (): number => slots.filter((slot) => slot.busy !== null).length

  /** The queue a run of `kind` waits in; its size is the depth reported at admission. */
  const queueFor = (kind: WorkerKind): BoundedQueue<Pending> => (kind === "foreground" ? admission : children)

  /**
   * A bounded-queue rejection is reported once, at the dispatcher's catch site:
   * the queue is a generic FIFO and cannot know the session it refused, but the
   * typed `SessionQueueFullError` carries the queue name and capacity.
   */
  const logQueueFull = (sessionID: SessionID, error: unknown): void => {
    if (!(error instanceof SessionQueueFullError)) return
    log(workerLogMessage("queue full"), {
      level: "WARN",
      sessionID,
      queue: error.queue,
      capacity: error.capacity,
    })
  }

  const addSlot = (kind: WorkerKind, worker: SessionWorker): Slot => {
    const slot = emptySlot(kind, worker)
    slots.push(slot)
    return slot
  }

  const clearIdleReap = (slot: Slot): void => {
    if (slot.cancelIdleReap === undefined) return
    slot.cancelIdleReap()
    slot.cancelIdleReap = undefined
  }

  const clearRespawn = (slot: Slot): void => {
    if (slot.respawnCancel === undefined) return
    slot.respawnCancel()
    slot.respawnCancel = undefined
  }

  /**
   * Detach `slot` from the scheduler AND retire its isolate: removing a slot is
   * the scheduler's one "this worker is gone" transition, so the terminate lives
   * here rather than being repeated at every call site. `terminate` is
   * idempotent, so a slot already killed (a crash) is harmless.
   */
  const removeSlot = (slot: Slot, options: { forget?: boolean } = {}): void => {
    clearIdleReap(slot)
    clearRespawn(slot)
    // Release the slot's supervision entry by default so churned slots do not
    // leak backoff state. The degraded branch opts out: its entry is what keeps
    // `degradedCount` reflecting the permanently-reduced pool.
    if (options.forget ?? true) supervision.forget(slot.key)
    const controller = controllers.get(slot.key)
    if (controller !== undefined) {
      controller.dispose()
      controllers.delete(slot.key)
    }
    const index = slots.indexOf(slot)
    if (index >= 0) slots.splice(index, 1)
    void slot.worker.terminate().catch(() => undefined)
  }

  const cancelled = (sessionID: SessionID): SessionCancelledError =>
    new SessionCancelledError({ message: `session ${sessionID} was cancelled`, sessionID })

  /** Lazily create the per-worker cancel controller (one control port per worker). */
  const controllerFor = (config: CancellationConfig, slot: Slot, port: CancelPort): CancelController => {
    const existing = controllers.get(slot.key)
    if (existing !== undefined) return existing
    const controller = createCancelController({
      port,
      terminate: () => slot.worker.terminate().then(() => undefined),
      ...defined({ graceMs: config.graceMs, schedule: config.schedule }),
      reap: () => config.reap?.(slot.worker),
      // Thread the dispatcher's logger and the owning worker id into the
      // controller so its D1 `cancel requested` line fires in production, where
      // the controller is built at this call site.
      log,
      workerID: slot.worker.id,
    })
    controllers.set(slot.key, controller)
    return controller
  }

  /**
   * Retire `slot` for `sessionID`: detach the pending, release its reservation,
   * reject the caller, terminate the isolate (idempotent), optionally reap its
   * mirrored PGIDs, then replace the slot on the same kind unless shutting down.
   */
  const terminateSlot = async (
    sessionID: SessionID,
    pending: Pending,
    slot: Slot | undefined,
    options: { reap: boolean; onRetired?: () => void },
  ): Promise<void> => {
    pending.slot = undefined
    registry.release(sessionID)
    byKey.delete(sessionID)
    pending.reject(cancelled(sessionID))
    if (slot !== undefined) {
      slot.busy = null
      // `removeSlot` retires the isolate; reap its mirrored PGIDs before the
      // replacement slot is spawned.
      removeSlot(slot)
      if (options.reap) cancellation?.reap?.(slot.worker)
      if (!terminated) addSlot(slot.kind, pool.spawn(slot.kind))
    }
    // Fired in the terminal transition's own tick (before control returns to the
    // caller), so an observer of the caller's rejection already sees the event.
    options.onRetired?.()
  }

  const start = (pending: Pending, slot: Slot): void => {
    slot.busy = pending.sessionID
    pending.slot = slot
    pending.startedAt = Date.now()
    pending.workerID = slot.worker.id
    clearIdleReap(slot)
    registry.reserve(pending.sessionID, slot.worker.id)
    registry.markRunning(pending.sessionID)
    log(workerLogMessage("session admitted"), {
      level: "INFO",
      sessionID: pending.sessionID,
      workerID: slot.worker.id,
      kind: pending.kind,
      depth: pending.depth,
      queueDepth: queueFor(pending.kind).size,
    })
    const run: SessionRun = {
      sessionID: pending.sessionID,
      depth: pending.depth,
      // Carry the mode only when it deviates from the default so a plain run
      // keeps its minimal `{sessionID, depth}` shape.
      ...(pending.mode === "startShell" ? { mode: pending.mode } : {}),
      // A subagent prompt rides the run frame only when present, so a plain run
      // keeps its existing shape.
      ...(pending.prompt === undefined ? {} : { prompt: pending.prompt }),
      // The shell command rides a `startShell` run only when present.
      ...(pending.shell === undefined ? {} : { shell: pending.shell }),
    }
    let execution: Promise<void>
    try {
      execution = slot.worker.execute(run)
    } catch (error) {
      execution = Promise.reject(error)
    }
    execution.then(
      () => complete(pending),
      (error) => fail(pending, error),
    )
  }

  const maybeReap = (slot: Slot): void => {
    if (slot.kind !== "headroom" || slot.busy !== null) return
    clearIdleReap(slot)
    slot.cancelIdleReap = schedule(headroomIdleReapMs, () => {
      slot.cancelIdleReap = undefined
      if (terminated || slot.busy !== null) return
      removeSlot(slot)
      log(workerLogMessage("headroom reaped"), {
        level: "INFO",
        workerID: slot.worker.id,
        idleMs: headroomIdleReapMs,
      })
    })
  }

  /**
   * Release a completed run. The release is held behind the per-session write
   * barrier (R4#16) so the run's durable egress write is acked before the slot
   * is freed and the session's next run is promoted — the next run can never
   * overtake the prior run's egress. A graceful cancel in flight owns settlement
   * (`cancelPending`) and skips the barrier, since its controller reconciles the
   * caller with the reported winner.
   */
  const complete = (pending: Pending): void => {
    // The run itself has settled: signal the graceful-cancel controller before any
    // barrier or deferred caller settlement, so it can never miss the settle. The
    // run resolved (not rejected), so record it as a NATURAL completion.
    pending.completed = true
    pending.markSettled()
    const barrier = pending.cancelPending ? undefined : writeBarrier?.(pending.sessionID)
    if (barrier === undefined) {
      finalizeCompletion(pending)
      return
    }
    // A barrier rejection is not a run failure: the run already completed, so
    // release either way rather than wedging the slot forever.
    void barrier.then(
      () => finalizeCompletion(pending),
      () => finalizeCompletion(pending),
    )
  }

  const finalizeCompletion = (pending: Pending): void => {
    const slot = pending.slot
    registry.release(pending.sessionID)
    byKey.delete(pending.sessionID)
    if (slot !== undefined) {
      slot.busy = null
      supervision.recordHealthy(slot.key)
      maybeReap(slot)
    }
    log(workerLogMessage("session completed"), {
      level: "INFO",
      sessionID: pending.sessionID,
      workerID: pending.workerID,
      durationMs: pending.startedAt === undefined ? 0 : Date.now() - pending.startedAt,
    })
    // First-writer-wins: a cancel claimed before this completion rejects instead.
    // A graceful cancel in flight defers settlement to its controller so the
    // caller always matches the reported winner.
    if (!pending.cancelPending) {
      if (pending.gate.claim("completion")) pending.resolve()
      else pending.reject(cancelled(pending.sessionID))
    }
    pump()
    nextWaiter(pending.sessionID)
  }

  const fail = (pending: Pending, error: unknown): void => {
    // The run has settled (failed): signal the graceful-cancel controller so a
    // cancel racing a failure resolves instead of waiting out its grace window.
    pending.markSettled()
    const slot = pending.slot
    registry.release(pending.sessionID)
    byKey.delete(pending.sessionID)
    if (slot !== undefined) slot.busy = null
    // (a) A cancelled run's rejection is a cancellation, not a crash (no respawn).
    if (pending.gate.winner === "cancel") {
      pending.reject(cancelled(pending.sessionID))
      if (slot !== undefined) maybeReap(slot)
      pump()
      nextWaiter(pending.sessionID)
      return
    }
    // (a') A graceful cancel in flight owns settlement: a terminate-induced (or
    // raced) rejection must not log a run failure or reject the caller here — the
    // controller reconciles the caller with its winner. The slot is already
    // detached by the cancel, so there is no crash to retire.
    if (pending.cancelPending) {
      if (slot !== undefined) maybeReap(slot)
      pump()
      nextWaiter(pending.sessionID)
      return
    }
    const timedOut = error instanceof SessionWorkerReplyTimeoutError
    const crashed = error instanceof WorkerCrashedError || (slot !== undefined && !slot.worker.alive)
    if (timedOut && slot !== undefined) {
      // (b) A reply timeout is a wedged isolate: reject with the timeout itself
      // (not a cancellation), cascade to the subagents it would otherwise orphan,
      // then terminate + retire under the same supervised backoff as a crash.
      pending.reject(error)
      void cancelWithGrace(pending.sessionID).catch(() => undefined)
      retire(slot, pending.sessionID, "timeout")
    } else if (crashed && slot !== undefined) {
      // (c) A crashed isolate: reject with a crash error, cascade, then terminate
      // (idempotent — a crash that already killed it is fine) + retire.
      pending.reject(
        error instanceof WorkerCrashedError
          ? error
          : new WorkerCrashedError({
              message: `worker ${slot.worker.id} crashed: ${describeFailure(error)}`,
              workerID: slot.worker.id,
              cause: error,
            }),
      )
      // Plan 9.6: a parent CRASH is an implicit cancel of its subagent children.
      // The parent's own reservation is already released above, so cascading
      // through `cancelWithGrace` only reaches descendants — children-first.
      void cancelWithGrace(pending.sessionID).catch(() => undefined)
      retire(slot, pending.sessionID, "crash")
    } else {
      // (d) A plain session failure with a live worker: release it, but surface
      // the run's own error first so a defect (e.g. "No user message found in
      // stream") is never silent. `pending.workerID` is captured at admission,
      // so it survives a detach; the error text keeps the cause visible.
      log(workerLogMessage("worker run failed"), {
        level: "ERROR",
        sessionID: pending.sessionID,
        workerID: pending.workerID,
        error: describeFailure(error),
        cause: error,
      })
      pending.reject(error)
      if (slot !== undefined) maybeReap(slot)
      pump()
    }
    nextWaiter(pending.sessionID)
  }

  /**
   * Retire a slot whose run failed (`reason: "crash"`) or wedged
   * (`reason: "timeout"`), then replace it under the same supervised backoff.
   *
   * The old isolate is terminated FIRST: `terminate` is idempotent, so a crash
   * that already killed it is harmless, while a timed-out isolate that is still
   * running is stopped before it can issue more writer transactions. The slot is
   * marked `retiring` for the whole window so `freeSlot` never hands it a run.
   * After a crash loop the slot is removed and reported degraded (`K−1`) rather
   * than respawned forever.
   */
  const retire = (slot: Slot, sessionID: SessionID, reason: "crash" | "timeout"): void => {
    if (terminated) return
    slot.retiring = true
    void slot.worker.terminate().catch(() => undefined)
    const decision = supervision.recordCrash(slot.key)
    log(workerLogMessage("worker crashed"), {
      level: "WARN",
      workerID: decision.workerID,
      sessionID,
      reason,
      attempts: decision.attempts,
      backoffMs: decision.delayMs,
      degraded: decision.degraded,
    })
    if (decision.degraded) {
      // Keep the supervision entry so `degradedCount` records the K−1 pool.
      removeSlot(slot, { forget: false })
      onDegraded?.(slot.key)
      log(workerLogMessage("slot degraded"), {
        level: "ERROR",
        workerID: slot.key,
        degradedCount: supervision.degradedCount,
      })
      pump()
      return
    }
    clearRespawn(slot)
    slot.respawnCancel = schedule(decision.delayMs, () => {
      slot.respawnCancel = undefined
      if (terminated) return
      slot.worker = pool.spawn(slot.kind)
      // The fresh isolate can take runs again.
      slot.retiring = false
      pump()
    })
    // Drain whatever OTHER free slots can take now; the retiring slot itself
    // rejoins capacity only when its respawn fires.
    pump()
  }

  const pump = (): void => {
    if (terminated) return
    let foreground = freeSlot("foreground")
    while (foreground !== undefined && admission.size > 0) {
      start(admission.dequeue()!, foreground)
      foreground = freeSlot("foreground")
    }
    let headroom = freeSlot("headroom")
    while (headroom !== undefined && children.size > 0) {
      start(children.dequeue()!, headroom)
      headroom = freeSlot("headroom")
    }
    // Occupancy is measured once per pump; each spawned headroom slot bumps it,
    // instead of re-filtering the whole slot list on every iteration.
    let headroomSlots = countHeadroomSlots()
    while (children.size > 0 && headroomSlots < headroomMax) {
      start(children.dequeue()!, addSlot("headroom", pool.spawn("headroom")))
      headroomSlots += 1
    }
  }

  const nextWaiter = (sessionID: SessionID): void => {
    const waiters = waitersByKey.get(sessionID)
    if (waiters === undefined || waiters.size === 0) return
    const next = waiters.dequeue()!
    log(workerLogMessage("waiter promoted"), {
      level: "INFO",
      sessionID,
      waiterQueueDepth: waiters.size,
    })
    if (waiters.size === 0) waitersByKey.delete(sessionID)
    byKey.set(sessionID, next)
    startOrEnqueue(next)
  }

  /**
   * Reject `pending` and every different-kind waiter parked behind its key when
   * the queue refuses it. The whole wait list fails together: a head-of-line
   * admission failure must not leave the waiters behind it orphaned.
   */
  const failEnqueue = (pending: Pending, error: unknown): void => {
    byKey.delete(pending.sessionID)
    pending.reject(error)
    const waiters = waitersByKey.get(pending.sessionID)
    if (waiters === undefined) return
    waitersByKey.delete(pending.sessionID)
    for (const waiter of waiters.items) waiter.reject(error)
  }

  /**
   * Drain every different-run waiter parked behind a session whose run is being
   * cancelled. A cancelled session never promotes a waiter, so each is rejected
   * with the same cancellation instead of being left parked forever.
   */
  const rejectWaiters = (sessionID: SessionID, error: unknown): void => {
    const waiters = waitersByKey.get(sessionID)
    if (waiters === undefined) return
    waitersByKey.delete(sessionID)
    for (const waiter of waiters.items) {
      waiter.gate.claim("cancel")
      waiter.reject(error)
    }
  }

  /** A depth ≥ 1 child refused because the physical headroom is saturated. */
  const headroomFull = (): HeadroomFullError =>
    new HeadroomFullError({
      message: `headroom is full (${countHeadroomSlots()} of ${headroomMax} slots) and the child queue is at capacity (${childCapacity})`,
    })

  const enqueue = (pending: Pending): void => {
    const queue = pending.kind === "foreground" ? admission : children
    try {
      queue.enqueue(pending)
    } catch (error) {
      logQueueFull(pending.sessionID, error)
      // A child-queue overflow is really "headroom is full": the physical pool is
      // saturated and the bounded child queue has no room, so surface the
      // semantic error instead of the raw queue-full failure (decision line 60).
      failEnqueue(pending, pending.kind === "headroom" ? headroomFull() : error)
      return
    }
    log(workerLogMessage("session queued"), {
      level: "INFO",
      sessionID: pending.sessionID,
      queue: queue.name,
      depth: pending.depth,
      capacity: queue.capacity,
      size: queue.size,
    })
  }

  const startOrEnqueue = (pending: Pending): void => {
    const slot = freeSlot(pending.kind)
    if (slot !== undefined) {
      start(pending, slot)
      return
    }
    if (pending.kind === "headroom" && countHeadroomSlots() < headroomMax) {
      start(pending, addSlot("headroom", pool.spawn("headroom")))
      return
    }
    enqueue(pending)
  }

  /**
   * Whether an in-flight run is the exact same run as a new request. The dedup
   * key is the full run identity — kind, frame mode, EXACT depth and the prompt
   * and shell payloads by reference — so a same-session dispatch at a different
   * depth (or carrying a different prompt) parks as a waiter instead of silently
   * joining a run it does not describe.
   */
  const isSameRun = (
    admitted: Pending,
    depth: number,
    kind: WorkerKind,
    mode: SessionRunMode,
    prompt: PromptInput | undefined,
    shell: ShellInput | undefined,
  ): boolean =>
    admitted.depth === depth &&
    admitted.kind === kind &&
    admitted.mode === mode &&
    admitted.prompt === prompt &&
    admitted.shell === shell

  const dispatch = (request: DispatchRequest): Promise<void> => {
    const depth = request.depth ?? 0
    if (!Number.isInteger(depth) || depth < 0) {
      // Async validation: reject the returned promise rather than throwing out of
      // a function whose contract is a promise.
      return Promise.reject(new Error(`invalid dispatch depth: ${depth} (expected a non-negative integer)`))
    }
    const kind: WorkerKind = workerKindForDepth(depth)
    const mode: SessionRunMode = request.mode ?? "run"
    const sessionID = request.sessionID
    const prompt = request.prompt
    const shell = request.shell

    const admitted = byKey.get(sessionID)
    if (admitted !== undefined) {
      if (isSameRun(admitted, depth, kind, mode, prompt, shell)) return admitted.promise
      // Different run identity: park behind the in-flight run, bounded per key.
      const pending = createPending(sessionID, depth, kind, mode, prompt, shell)
      const waiters = waitersByKey.get(sessionID) ?? createBoundedQueue<Pending>("waiter", waiterCapacity)
      if (!waitersByKey.has(sessionID)) waitersByKey.set(sessionID, waiters)
      try {
        waiters.enqueue(pending)
      } catch (error) {
        logQueueFull(sessionID, error)
        if (waiters.size === 0) waitersByKey.delete(sessionID)
        pending.reject(error)
      }
      return pending.promise
    }

    const pending = createPending(sessionID, depth, kind, mode, prompt, shell)
    byKey.set(sessionID, pending)
    startOrEnqueue(pending)
    return pending.promise
  }

  const cancel = async (sessionID: SessionID): Promise<void> => {
    const error = cancelled(sessionID)
    const queued =
      admission.remove((pending) => pending.sessionID === sessionID) ??
      children.remove((pending) => pending.sessionID === sessionID)
    if (queued !== undefined) {
      queued.gate.claim("cancel")
      byKey.delete(sessionID)
      queued.reject(error)
      rejectWaiters(sessionID, error)
      return
    }

    const admitted = byKey.get(sessionID)
    if (admitted === undefined) {
      rejectWaiters(sessionID, error)
      return
    }
    registry.markCancelling(sessionID)
    // Claim the gate BEFORE terminating: the terminate-induced rejection is then
    // reported as a cancellation (`winner:"cancel"`) instead of a crash or a
    // spurious "worker run failed" ERROR (the slot is detached by terminateSlot,
    // so `fail` would otherwise fall through to its plain-failure branch).
    admitted.gate.claim("cancel")
    // Drain the waiters BEFORE terminating: the terminate-induced rejection runs
    // `fail`, whose `nextWaiter` would otherwise promote a waiter for a session
    // that is being cancelled.
    rejectWaiters(sessionID, error)
    // `terminateSlot` detaches the slot first, so the terminate-induced rejection
    // is not mistaken for a crash (which would schedule a spurious respawn).
    await terminateSlot(sessionID, admitted, admitted.slot, { reap: false })
    pump()
    nextWaiter(sessionID)
  }

  const cancelWithGrace = async (sessionID: SessionID): Promise<CancelOutcome | undefined> => {
    const config = cancellation
    if (config === undefined) {
      await cancel(sessionID)
      return undefined
    }

    // 11.8: cascade to subagents first, each bounded by the same grace window.
    for (const child of config.childrenOf?.(sessionID) ?? []) await cancelWithGrace(child)

    // 11.10: a queued session is removed from the FIFO and terminated in place.
    const queued =
      admission.remove((pending) => pending.sessionID === sessionID) ??
      children.remove((pending) => pending.sessionID === sessionID)
    if (queued !== undefined) {
      queued.gate.claim("cancel")
      byKey.delete(sessionID)
      queued.reject(cancelled(sessionID))
      rejectWaiters(sessionID, cancelled(sessionID))
      config.onCancelled?.({ sessionID, mode: "queued", workerID: undefined, resumeFromDb: true })
      return { sessionID, mode: "terminated", winner: "cancel", acknowledged: false, durationMs: 0 }
    }

    const admitted = byKey.get(sessionID)
    if (admitted === undefined) return undefined

    registry.markCancelling(sessionID)
    const slot = admitted.slot
    const port = slot === undefined ? undefined : config.portFor?.(slot.worker)

    // No control channel: claim the cancel and terminate in place. Claiming the
    // gate first keeps the terminate-induced rejection a cancellation.
    if (slot === undefined || port === undefined) {
      admitted.gate.claim("cancel")
      // Drain waiters before terminating, so the terminate-induced `fail` cannot
      // promote one for a session that is being cancelled.
      rejectWaiters(sessionID, cancelled(sessionID))
      await terminateSlot(sessionID, admitted, slot, {
        reap: true,
        onRetired: () =>
          config.onCancelled?.({ sessionID, mode: "terminated", workerID: slot?.worker.id, resumeFromDb: true }),
      })
      pump()
      nextWaiter(sessionID)
      return { sessionID, mode: "terminated", winner: "cancel", acknowledged: false, durationMs: 0 }
    }

    // Detach the slot so the terminate-induced rejection is not misread as a crash.
    admitted.slot = undefined
    // The graceful cancel owns the caller's settlement: `complete`/`fail` must not
    // settle it while this runs, because the controller's winner decides whether
    // the caller resolves (completion) or rejects (cancel). This is the
    // reconciliation that keeps the caller consistent with the reported outcome.
    admitted.cancelPending = true
    // Drain waiters up-front: `complete`/`fail` still call `nextWaiter` while the
    // controller runs, and a cancelled session must never promote a waiter.
    rejectWaiters(sessionID, cancelled(sessionID))
    let resolved: CancelOutcome = {
      sessionID,
      mode: "terminated",
      winner: "cancel",
      acknowledged: false,
      durationMs: 0,
    }
    try {
      const outcome = await controllerFor(config, slot, port).cancel(sessionID, { complete: admitted.settled })
      // Q11 — completion wins, decided by how the run settled rather than by the
      // worker's ack alone (a real worker acks only after the run has settled, so
      // the ack is often not visible yet). A cooperative settle of a run that
      // resolved naturally, with no accepted cancel, is a completion; a run that
      // settled by rejecting is a cancel even without an ack.
      resolved =
        outcome.mode === "cooperative" && !outcome.acknowledged && admitted.completed
          ? { ...outcome, mode: "completed", winner: "completion" }
          : outcome

      // A `cooperative` outcome means the worker's run settled at a checkpoint;
      // the live worker is reused (11.4). A grace *expiry* is unambiguous: the
      // controller terminated + reaped, and this releases the slot. A `completed`
      // outcome means the run finished on its own; the live worker is reused too.
      if (resolved.mode === "terminated") {
        await terminateSlot(sessionID, admitted, slot, { reap: false })
        pump()
        nextWaiter(sessionID)
      } else {
        slot.busy = null
        maybeReap(slot)
        pump()
      }
      config.onCancelled?.({ sessionID, mode: resolved.mode, workerID: slot.worker.id, resumeFromDb: true })
      return resolved
    } finally {
      admitted.cancelPending = false
      // Reconcile the caller with the reported winner. `terminateSlot` already
      // rejected it on the terminated path, so the extra settle is a no-op there.
      if (resolved.winner === "completion") admitted.resolve()
      else admitted.reject(cancelled(sessionID))
      rejectWaiters(sessionID, cancelled(sessionID))
    }
  }

  const terminateAll = async (): Promise<void> => {
    if (terminated) return
    terminated = true
    for (const pending of byKey.values()) pending.reject(cancelled(pending.sessionID))
    for (const waiters of waitersByKey.values()) {
      for (const pending of waiters.items) pending.reject(cancelled(pending.sessionID))
    }
    admission.clear()
    children.clear()
    byKey.clear()
    waitersByKey.clear()
    registry.clear()
    for (const controller of controllers.values()) controller.dispose()
    controllers.clear()
    const workers = slots.map((slot) => slot.worker)
    for (const slot of slots) {
      clearIdleReap(slot)
      clearRespawn(slot)
    }
    slots.length = 0
    await Promise.all(workers.map((worker) => worker.terminate().catch(() => undefined)))
    await pool.terminateAll().catch(() => undefined)
    // Phase 11.5: once every worker isolate is gone, reap any mirrored PGIDs the
    // per-worker terminate did not cover (e.g. a worker that never got a slot).
    cancellation?.reapAll?.()
  }

  return {
    get busySlotCount() {
      return countBusySlots()
    },
    get queuedCount() {
      return admission.size + children.size
    },
    get headroomSlotCount() {
      return countHeadroomSlots()
    },
    get headroomCapacity() {
      return headroomMax
    },
    get degradedCount() {
      return supervision.degradedCount
    },
    registry,
    workerForSession: (sessionID) => registry.get(sessionID)?.workerID,
    dispatch,
    cancel,
    cancelWithGrace,
    terminateAll,
    dispose: terminateAll,
    [Symbol.asyncDispose]: terminateAll,
  }
}

export * as SessionDispatch from "./dispatch"
