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
// workers directly, so scheduling is deterministic in tests. `createBunWorkerPool`
// is the production adapter over `pool.ts`.

import { Schema } from "effect"
import type { SessionID } from "@/session/schema"
import { defined } from "@/util/object"
import { parsePositiveInt } from "@/util/parse"
import {
  ADMISSION_QUEUE_CAPACITY,
  CHILD_QUEUE_CAPACITY,
  WAITER_QUEUE_CAPACITY,
  createBoundedQueue,
  type BoundedQueue,
} from "./queue"
import { createSessionRegistry, type SessionRegistry } from "./registry"
import { createSupervision, type Supervision } from "./supervision"
import { resolveWorkerCount, spawnPoolWorker, type Env, type PoolWorker } from "./pool"
import { defaultSchedule } from "./port"
import type {
  ExecutorRun,
  ExecutorSessionWorker,
  ExecutorWorkerKind,
  ExecutorWorkerPool,
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

/** Headroom size: `max(2, ceil(K/2))`. */
export function deriveHeadroom(workerCount: number): number {
  parsePositiveInt(workerCount, "worker count")
  return Math.max(HEADROOM_MIN, Math.ceil(workerCount / 2))
}

/** Scheduling kind: foreground roots or on-demand depth-1 children (execute.ts owns the shape). */
export type WorkerKind = ExecutorWorkerKind

/** One session run: the key the worker executes and its subagent depth. */
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

/** Raised when a session has failed because its worker crashed (plan 11.7). */
export class SessionCrashedError extends Schema.TaggedErrorClass<SessionCrashedError>()("SessionCrashedError", {
  message: Schema.String,
  workerID: Schema.Number,
}) {}

/**
 * Worker-level crash. It is also a `SessionCrashedError` (the session failed),
 * so a caller may catch either the worker- or the session-level error. It keeps
 * its own `_tag` for `Effect.catchTag("WorkerCrashedError")`.
 */
export class WorkerCrashedError extends Schema.TaggedErrorClass<WorkerCrashedError>()("WorkerCrashedError", {
  message: Schema.String,
  workerID: Schema.Number,
}) {}

// A crashed session failed, so `WorkerCrashedError` is also a
// `SessionCrashedError`: re-parent its prototype to keep `instanceof
// SessionCrashedError` true while its own `_tag` stays "WorkerCrashedError".
Object.setPrototypeOf(WorkerCrashedError.prototype, SessionCrashedError.prototype)

/** Raised to the caller when a session is cancelled. */
export class SessionCancelledError extends Schema.TaggedErrorClass<SessionCancelledError>()("SessionCancelledError", {
  message: Schema.String,
  sessionID: Schema.String,
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
  /** Subagent children of a session, cancelled children-first (plan 11.8). */
  readonly childrenOf?: (sessionID: SessionID) => ReadonlyArray<SessionID>
  readonly onCancelled?: (event: CancelledEvent) => void
}

export type Scheduler = (delayMs: number, run: () => void) => () => void

export interface DispatchRequest {
  readonly sessionID: SessionID
  /** 0 = foreground/root; ≥1 = subagent child. Defaults to 0. */
  readonly depth?: number
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
  /** Cancels the idle-reap timer (it holds a timer's canceller, not a cancel controller). */
  cancelIdleReap: (() => void) | undefined
  respawnCancel: (() => void) | undefined
}

interface Pending {
  readonly sessionID: SessionID
  readonly depth: number
  readonly kind: WorkerKind
  readonly promise: Promise<void>
  readonly resolve: () => void
  readonly reject: (error: unknown) => void
  /** First-writer-wins between a cancel and a natural completion (plan 11.9). */
  readonly gate: OutcomeGate
  slot: Slot | undefined
}

function createPending(sessionID: SessionID, depth: number, kind: WorkerKind): Pending {
  let resolve!: () => void
  let reject!: (error: unknown) => void
  const promise = new Promise<void>((res, rej) => {
    resolve = res
    reject = rej
  })
  return { sessionID, depth, kind, promise, resolve, reject, gate: createOutcomeGate(), slot: undefined }
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
    cancelIdleReap: undefined,
    respawnCancel: undefined,
  })

  for (const worker of pool.workers) {
    slots.push(emptySlot("foreground", worker))
  }

  const freeSlot = (kind: WorkerKind): Slot | undefined =>
    slots.find((slot) => slot.kind === kind && slot.busy === null && slot.worker.alive)

  const countHeadroomSlots = (): number => slots.filter((slot) => slot.kind === "headroom").length
  const countBusySlots = (): number => slots.filter((slot) => slot.busy !== null).length

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
      removeSlot(slot)
      await slot.worker.terminate().catch(() => undefined)
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
    clearIdleReap(slot)
    registry.reserve(pending.sessionID, slot.worker.id)
    registry.markRunning(pending.sessionID)
    const run: SessionRun = { sessionID: pending.sessionID, depth: pending.depth }
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
      void slot.worker.terminate().catch(() => undefined)
    })
  }

  const complete = (pending: Pending): void => {
    const slot = pending.slot
    registry.release(pending.sessionID)
    byKey.delete(pending.sessionID)
    if (slot !== undefined) {
      slot.busy = null
      supervision.recordHealthy(slot.key)
      maybeReap(slot)
    }
    // First-writer-wins: a cancel claimed before this completion rejects instead.
    if (pending.gate.claim("completion")) pending.resolve()
    else pending.reject(cancelled(pending.sessionID))
    pump()
    nextWaiter(pending.sessionID)
  }

  const fail = (pending: Pending, error: unknown): void => {
    const slot = pending.slot
    registry.release(pending.sessionID)
    byKey.delete(pending.sessionID)
    if (slot !== undefined) slot.busy = null
    // A cancelled run's rejection is a cancellation, not a crash (no respawn).
    if (pending.gate.winner === "cancel") {
      pending.reject(cancelled(pending.sessionID))
      if (slot !== undefined) maybeReap(slot)
      pump()
      nextWaiter(pending.sessionID)
      return
    }
    const crashed = error instanceof WorkerCrashedError || (slot !== undefined && !slot.worker.alive)
    if (crashed && slot !== undefined) {
      pending.reject(
        error instanceof WorkerCrashedError
          ? error
          : new WorkerCrashedError({ message: `worker ${slot.worker.id} crashed`, workerID: slot.worker.id }),
      )
      // Plan 9.6: a parent CRASH is an implicit cancel of its subagent children.
      // The parent's own reservation is already released above, so cascading
      // through `cancelWithGrace` only reaches descendants — children-first.
      void cancelWithGrace(pending.sessionID).catch(() => undefined)
      crash(slot)
    } else {
      pending.reject(error)
      if (slot !== undefined) maybeReap(slot)
      pump()
    }
    nextWaiter(pending.sessionID)
  }

  const crash = (slot: Slot): void => {
    if (terminated) return
    const decision = supervision.recordCrash(slot.key)
    if (decision.degraded) {
      // Keep the supervision entry so `degradedCount` records the K−1 pool.
      removeSlot(slot, { forget: false })
      onDegraded?.(slot.key)
      void slot.worker.terminate().catch(() => undefined)
      pump()
      return
    }
    clearRespawn(slot)
    slot.respawnCancel = schedule(decision.delayMs, () => {
      slot.respawnCancel = undefined
      if (terminated) return
      slot.worker = pool.spawn(slot.kind)
      pump()
    })
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

  const enqueue = (pending: Pending): void => {
    const queue = pending.kind === "foreground" ? admission : children
    try {
      queue.enqueue(pending)
    } catch (error) {
      failEnqueue(pending, error)
    }
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

  const dispatch = (request: DispatchRequest): Promise<void> => {
    const depth = request.depth ?? 0
    if (!Number.isInteger(depth) || depth < 0) {
      // Async validation: reject the returned promise rather than throwing out of
      // a function whose contract is a promise.
      return Promise.reject(new Error(`invalid dispatch depth: ${depth} (expected a non-negative integer)`))
    }
    const kind: WorkerKind = depth >= 1 ? "headroom" : "foreground"
    const sessionID = request.sessionID

    const admitted = byKey.get(sessionID)
    if (admitted !== undefined) {
      if (admitted.kind === kind) return admitted.promise
      // Different kind: park behind the in-flight run, bounded per key.
      const pending = createPending(sessionID, depth, kind)
      const waiters = waitersByKey.get(sessionID) ?? createBoundedQueue<Pending>("waiter", waiterCapacity)
      if (!waitersByKey.has(sessionID)) waitersByKey.set(sessionID, waiters)
      try {
        waiters.enqueue(pending)
      } catch (error) {
        if (waiters.size === 0) waitersByKey.delete(sessionID)
        pending.reject(error)
      }
      return pending.promise
    }

    const pending = createPending(sessionID, depth, kind)
    byKey.set(sessionID, pending)
    startOrEnqueue(pending)
    return pending.promise
  }

  const cancel = async (sessionID: SessionID): Promise<void> => {
    const queued =
      admission.remove((pending) => pending.sessionID === sessionID) ??
      children.remove((pending) => pending.sessionID === sessionID)
    if (queued !== undefined) {
      byKey.delete(sessionID)
      queued.reject(cancelled(sessionID))
      return
    }

    const admitted = byKey.get(sessionID)
    if (admitted === undefined) return
    registry.markCancelling(sessionID)
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
      byKey.delete(sessionID)
      queued.gate.claim("cancel")
      queued.reject(cancelled(sessionID))
      config.onCancelled?.({ sessionID, mode: "queued", workerID: undefined, resumeFromDb: true })
      return { sessionID, mode: "terminated", winner: "cancel", acknowledged: false, durationMs: 0 }
    }

    const admitted = byKey.get(sessionID)
    if (admitted === undefined) return undefined

    admitted.gate.claim("cancel")
    registry.markCancelling(sessionID)
    const slot = admitted.slot
    const port = slot === undefined ? undefined : config.portFor?.(slot.worker)

    // No control channel: fall straight through to the terminate path.
    if (slot === undefined || port === undefined) {
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
    const outcome = await controllerFor(config, slot, port).cancel(sessionID, { complete: admitted.promise })
    // Q11 — completion wins. The dispatcher claims the cancel gate before the
    // controller runs, so a run that settles on its own during the grace window
    // is reported by the controller as `cooperative`. The worker's `cancel-ack`
    // is the discriminator: a settle with no ack means the run completed
    // naturally, so reclassify the outcome to `completed`/`completion`. The ack
    // already reflects the worker's real reply and is preserved verbatim.
    const resolved: CancelOutcome =
      outcome.mode === "cooperative" && !outcome.acknowledged
        ? { ...outcome, mode: "completed", winner: "completion" }
        : outcome

    if (resolved.mode === "terminated") {
      // The controller already terminated the isolate and reaped its PGIDs;
      // terminate again is idempotent, only release the reservation + slot.
      await terminateSlot(sessionID, admitted, slot, { reap: false })
      pump()
      nextWaiter(sessionID)
    } else {
      // Cooperative or completed: the run settled at a checkpoint; reuse the live
      // worker (11.4).
      slot.busy = null
      maybeReap(slot)
      pump()
    }
    config.onCancelled?.({ sessionID, mode: resolved.mode, workerID: slot.worker.id, resumeFromDb: true })
    return resolved
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

export interface BunWorkerPoolOptions {
  readonly k?: number
  readonly entry?: string | URL
  readonly env?: Env
  readonly timeoutMs?: number
  readonly availableParallelism?: () => number
  /** Executor-protocol driver; defaults to the Phase-4 bootstrap placeholder. */
  readonly drive?: PoolWorkerDriver
}

/**
 * Drive one session run on a pool worker. The production executor transport
 * lives in `hub.ts` (`createBunExecutorWorkerPool`); this is the seam the
 * bootstrap-backed adapter is built on.
 */
export type PoolWorkerDriver = (worker: PoolWorker, run: SessionRun) => Promise<void>

/**
 * Phase-4 bootstrap placeholder. It boots the trivial Effect, but does NOT yet
 * carry `run.sessionID`/`run.depth` across the isolate — the executor-protocol
 * transport replaces it when the bootstrap pool is unified with the hub pool.
 */
export const driveBootstrapPlaceholder: PoolWorkerDriver = async (worker) => {
  await worker.ready()
  await worker.run(1)
}

/**
 * Adapt one `PoolWorker` to the scheduling seam. The run is forwarded to
 * `drive` (production: the executor protocol) rather than hard-coded, so the
 * adapter never silently drops `run.sessionID`/`run.depth`.
 */
export function adaptPoolWorker(
  worker: PoolWorker,
  drive: PoolWorkerDriver = driveBootstrapPlaceholder,
): SessionWorker {
  return {
    get id() {
      return worker.id
    },
    get alive() {
      return worker.alive
    },
    execute: (run: SessionRun): Promise<void> => drive(worker, run),
    terminate: () => worker.terminate().then(() => undefined),
  }
}

export interface BunWorkerPool extends WorkerPool, AsyncDisposable {
  dispose(): Promise<void>
}

/** Production `WorkerPool`: adapts the Bun worker pool to the scheduling seam. */
export function createBunWorkerPool(options: BunWorkerPoolOptions = {}): BunWorkerPool {
  const k =
    options.k ?? resolveWorkerCount({ availableParallelism: options.availableParallelism, env: options.env })
  const created: PoolWorker[] = []
  const drive = options.drive ?? driveBootstrapPlaceholder

  const spawnWorker = (): PoolWorker => {
    const worker = spawnPoolWorker({
      id: created.length + 1,
      entry: options.entry,
      env: options.env,
      timeoutMs: options.timeoutMs,
    })
    created.push(worker)
    return worker
  }

  const foreground = Array.from({ length: k }, () => adaptPoolWorker(spawnWorker(), drive))

  const terminateAll = async (): Promise<void> => {
    await Promise.all(created.map((worker) => worker.terminate().catch(() => undefined)))
  }

  return {
    workers: foreground,
    spawn: () => adaptPoolWorker(spawnWorker(), drive),
    terminateAll,
    dispose: terminateAll,
    [Symbol.asyncDispose]: terminateAll,
  }
}

export * as SessionDispatch from "./dispatch"
