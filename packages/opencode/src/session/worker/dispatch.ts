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
import {
  ADMISSION_QUEUE_CAPACITY,
  CHILD_QUEUE_CAPACITY,
  createBoundedQueue,
  type BoundedQueue,
} from "./queue"
import { createSessionRegistry, type SessionRegistry } from "./registry"
import { createSupervision, type Supervision } from "./supervision"
import { resolveWorkerCount, spawnPoolWorker, type Env, type PoolWorker } from "./pool"
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
  if (!Number.isInteger(workerCount) || workerCount < 1) {
    throw new Error(`invalid worker count: ${workerCount} (expected a positive integer)`)
  }
  return Math.max(HEADROOM_MIN, Math.ceil(workerCount / 2))
}

export type WorkerKind = "foreground" | "headroom"

export interface SessionRun {
  readonly sessionID: SessionID
  readonly depth: number
}

/** A worker that executes exactly one session run at a time. */
export interface SessionWorker {
  readonly id: number
  readonly alive: boolean
  /** Execute `run`; rejects on crash (and on a session failure). */
  execute(run: SessionRun): Promise<void>
  /** Terminate the isolate; idempotent. */
  terminate(): Promise<void>
}

/** The scheduling seam (task 4.9): supplies and replaces workers. */
export interface WorkerPool {
  /** The K foreground workers. */
  readonly workers: ReadonlyArray<SessionWorker>
  /** Create one fresh worker of `kind` (headroom on demand, or crash respawn). */
  spawn(kind: WorkerKind): SessionWorker
  terminateAll(): Promise<void>
}

/** Raised when a session has failed because its worker crashed (plan 11.7). */
export class SessionCrashedError extends Schema.TaggedErrorClass<SessionCrashedError>()("SessionCrashedError", {
  message: Schema.String,
  workerID: Schema.Number,
}) {}

/**
 * Worker-level crash. It is also a `SessionCrashedError` (the session failed),
 * so a caller may catch either the worker- or the session-level error.
 */
export class WorkerCrashedError extends SessionCrashedError {}

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
  readonly headroomIdleReapMs?: number
  readonly supervision?: Supervision
  /** Timer seam; defaults to `setTimeout` (unref'd so it never blocks exit). */
  readonly schedule?: Scheduler
  readonly onDegraded?: (workerID: number) => void
  /** Phase 11 additive cancellation wiring; absent keeps the Phase-4 terminate path. */
  readonly cancellation?: CancellationConfig
}

export interface Dispatcher extends AsyncDisposable {
  readonly activeCount: number
  readonly queuedCount: number
  readonly headroomCount: number
  /** Physical headroom bound (Phase 9 admission seam): `headroomCount >= headroomCapacity` is full. */
  readonly headroomCapacity: number
  readonly degradedCount: number
  readonly registry: SessionRegistry
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

const defaultSchedule: Scheduler = (delayMs, run) => {
  const timer = setTimeout(run, delayMs)
  ;(timer as { unref?: () => void }).unref?.()
  return () => clearTimeout(timer)
}

interface Slot {
  readonly key: number
  readonly kind: WorkerKind
  worker: SessionWorker
  busy: SessionID | null
  reapCancel: (() => void) | undefined
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
  const headroomIdleReapMs = options.headroomIdleReapMs ?? DEFAULT_HEADROOM_IDLE_REAP_MS
  const onDegraded = options.onDegraded
  const cancellation = options.cancellation

  const admission: BoundedQueue<Pending> = createBoundedQueue("admission", admissionCapacity)
  const children: BoundedQueue<Pending> = createBoundedQueue("child", childCapacity)
  const byKey = new Map<SessionID, Pending>()
  const waitersByKey = new Map<SessionID, Pending[]>()
  const controllers = new Map<number, CancelController>()
  const slots: Slot[] = []
  let nextSlotKey = 1
  let terminated = false

  for (const worker of pool.workers) {
    slots.push({ key: nextSlotKey++, kind: "foreground", worker, busy: null, reapCancel: undefined })
  }

  const freeSlot = (kind: WorkerKind): Slot | undefined =>
    slots.find((slot) => slot.kind === kind && slot.busy === null && slot.worker.alive)

  const headroomCount = (): number => slots.filter((slot) => slot.kind === "headroom").length

  const addSlot = (kind: WorkerKind, worker: SessionWorker): Slot => {
    const slot: Slot = { key: nextSlotKey++, kind, worker, busy: null, reapCancel: undefined }
    slots.push(slot)
    return slot
  }

  const clearReap = (slot: Slot): void => {
    if (slot.reapCancel === undefined) return
    slot.reapCancel()
    slot.reapCancel = undefined
  }

  const removeSlot = (slot: Slot): void => {
    clearReap(slot)
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
  const controllerFor = (slot: Slot, port: CancelPort): CancelController => {
    const existing = controllers.get(slot.key)
    if (existing !== undefined) return existing
    const config = cancellation!
    const controller = createCancelController({
      port,
      terminate: () => slot.worker.terminate().then(() => undefined),
      ...(config.graceMs === undefined ? {} : { graceMs: config.graceMs }),
      ...(config.schedule === undefined ? {} : { schedule: config.schedule }),
      reap: () => config.reap?.(slot.worker),
    })
    controllers.set(slot.key, controller)
    return controller
  }

  const start = (pending: Pending, slot: Slot): void => {
    slot.busy = pending.sessionID
    pending.slot = slot
    clearReap(slot)
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
    clearReap(slot)
    slot.reapCancel = schedule(headroomIdleReapMs, () => {
      slot.reapCancel = undefined
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
      removeSlot(slot)
      onDegraded?.(slot.key)
      void slot.worker.terminate().catch(() => undefined)
      pump()
      return
    }
    schedule(decision.delayMs, () => {
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
    while (children.size > 0 && headroomCount() < headroomMax) {
      start(children.dequeue()!, addSlot("headroom", pool.spawn("headroom")))
    }
  }

  const nextWaiter = (sessionID: SessionID): void => {
    const waiters = waitersByKey.get(sessionID)
    if (waiters === undefined || waiters.length === 0) return
    const next = waiters.shift()!
    if (waiters.length === 0) waitersByKey.delete(sessionID)
    byKey.set(sessionID, next)
    startOrEnqueue(next)
  }

  const enqueue = (pending: Pending): void => {
    const queue = pending.kind === "foreground" ? admission : children
    try {
      queue.enqueue(pending)
    } catch (error) {
      byKey.delete(pending.sessionID)
      pending.reject(error)
    }
  }

  const startOrEnqueue = (pending: Pending): void => {
    const slot = freeSlot(pending.kind)
    if (slot !== undefined) {
      start(pending, slot)
      return
    }
    if (pending.kind === "headroom" && headroomCount() < headroomMax) {
      start(pending, addSlot("headroom", pool.spawn("headroom")))
      return
    }
    enqueue(pending)
  }

  const dispatch = (request: DispatchRequest): Promise<void> => {
    const depth = request.depth ?? 0
    if (!Number.isInteger(depth) || depth < 0) {
      throw new Error(`invalid dispatch depth: ${depth} (expected a non-negative integer)`)
    }
    const kind: WorkerKind = depth >= 1 ? "headroom" : "foreground"
    const sessionID = request.sessionID

    const admitted = byKey.get(sessionID)
    if (admitted !== undefined) {
      if (admitted.kind === kind) return admitted.promise
      const pending = createPending(sessionID, depth, kind)
      const waiters = waitersByKey.get(sessionID) ?? []
      waiters.push(pending)
      waitersByKey.set(sessionID, waiters)
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
    const slot = admitted.slot
    // Detach the slot so the terminate-induced rejection is not mistaken for a
    // crash (which would schedule a spurious second respawn).
    admitted.slot = undefined
    registry.release(sessionID)
    byKey.delete(sessionID)
    admitted.reject(cancelled(sessionID))
    if (slot !== undefined) {
      slot.busy = null
      removeSlot(slot)
      await slot.worker.terminate().catch(() => undefined)
      if (!terminated) addSlot(slot.kind, pool.spawn(slot.kind))
    }
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
      admitted.slot = undefined
      registry.release(sessionID)
      byKey.delete(sessionID)
      admitted.reject(cancelled(sessionID))
      if (slot !== undefined) {
        slot.busy = null
        removeSlot(slot)
        await slot.worker.terminate().catch(() => undefined)
        config.reap?.(slot.worker)
        if (!terminated) addSlot(slot.kind, pool.spawn(slot.kind))
      }
      config.onCancelled?.({ sessionID, mode: "terminated", workerID: slot?.worker.id, resumeFromDb: true })
      pump()
      nextWaiter(sessionID)
      return { sessionID, mode: "terminated", winner: "cancel", acknowledged: false, durationMs: 0 }
    }

    // Detach the slot so the terminate-induced rejection is not misread as a crash.
    admitted.slot = undefined
    const outcome = await controllerFor(slot, port).cancel(sessionID, { complete: admitted.promise })

    if (outcome.mode === "terminated") {
      // The controller already terminated the isolate and reaped its PGIDs; only replace the slot.
      registry.release(sessionID)
      byKey.delete(sessionID)
      admitted.reject(cancelled(sessionID))
      slot.busy = null
      removeSlot(slot)
      if (!terminated) addSlot(slot.kind, pool.spawn(slot.kind))
      pump()
      nextWaiter(sessionID)
    } else {
      // Cooperative: the run settled at a checkpoint; reuse the live worker (11.4).
      slot.busy = null
      maybeReap(slot)
      pump()
    }
    // The dispatcher claimed cancel first, so a terminal event is never "completed".
    config.onCancelled?.({
      sessionID,
      mode: outcome.mode === "terminated" ? "terminated" : "cooperative",
      workerID: slot.worker.id,
      resumeFromDb: true,
    })
    return outcome
  }

  const terminateAll = async (): Promise<void> => {
    if (terminated) return
    terminated = true
    for (const pending of byKey.values()) pending.reject(cancelled(pending.sessionID))
    for (const waiters of waitersByKey.values()) {
      for (const pending of waiters) pending.reject(cancelled(pending.sessionID))
    }
    admission.clear()
    children.clear()
    byKey.clear()
    waitersByKey.clear()
    registry.clear()
    for (const controller of controllers.values()) controller.dispose()
    controllers.clear()
    const workers = slots.map((slot) => slot.worker)
    for (const slot of slots) clearReap(slot)
    slots.length = 0
    await Promise.all(workers.map((worker) => worker.terminate().catch(() => undefined)))
    await pool.terminateAll().catch(() => undefined)
  }

  return {
    get activeCount() {
      return slots.filter((slot) => slot.busy !== null).length
    },
    get queuedCount() {
      return admission.size + children.size
    },
    get headroomCount() {
      return headroomCount()
    },
    get headroomCapacity() {
      return headroomMax
    },
    get degradedCount() {
      return supervision.degradedCount
    },
    registry,
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
}

function adaptPoolWorker(worker: PoolWorker): SessionWorker {
  return {
    get id() {
      return worker.id
    },
    get alive() {
      return worker.alive
    },
    async execute(_run: SessionRun): Promise<void> {
      await worker.ready()
      await worker.run(1)
    },
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

  const foreground = Array.from({ length: k }, () => adaptPoolWorker(spawnWorker()))

  const terminateAll = async (): Promise<void> => {
    await Promise.all(created.map((worker) => worker.terminate().catch(() => undefined)))
  }

  return {
    workers: foreground,
    spawn: () => adaptPoolWorker(spawnWorker()),
    terminateAll,
    dispose: terminateAll,
    [Symbol.asyncDispose]: terminateAll,
  }
}

export * as SessionDispatch from "./dispatch"
