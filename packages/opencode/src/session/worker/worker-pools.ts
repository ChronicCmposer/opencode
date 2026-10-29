// Executor transport, worker cap, and session-bound pool factories.
import os from "node:os"
import path from "node:path"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import {
  Context,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Queue,
  Scope,
  ScopedCache,
  Schema,
  Semaphore,
} from "effect"
import type { SessionID } from "@/session/schema"
import type { EffectBridge } from "@/effect/bridge"
import { defined } from "@/util/object"
import {
  createExecutorCancelPort,
  createExecutorWorker,
  createExecutorWorkerPool,
  SessionWorkerReplyTimeoutError,
  type ExecutorSessionWorker,
  type ExecutorTransport,
  type ExecutorWorker,
  type ExecutorWorkerPool,
  type SessionExecutionResult,
  type SessionExecutorInbound,
  type SessionExecutorOutbound,
  type WorkerSpawnReason,
} from "./execute"
import {
  BOOT_CANCELLED,
  createBootLifecycle,
  type BootCancelResult,
  type BootLifecycle,
  type BootTelemetry,
} from "./cancel"
import { SESSION_WORKER_IDLE_TIMEOUT_ENV } from "./pool"
import { mergeEnv, type WorkerModelSelection } from "./init"
import { BuildMismatch, verifyReadyBuild } from "./build-hash"
import type { SessionWorkerInit } from "./session-worker"
import { workerLogMessage } from "./worker-logging"
import { sessionThreadName } from "@/session/worker/thread-name"
import { WorkerRuntime } from "./runtime-edge"

import type { WorkerTransferredPorts } from "./worker-ports"

// ---------------------------------------------------------------------------
// Spawn admission (one Effect `Semaphore`, not a hand-built Promise queue).
// ---------------------------------------------------------------------------

/** Why a permit was released. Diagnostic only; both outcomes free the slot. */
export type SpawnOutcome = "ready" | "failed"

/** A boot slot held by one in-flight spawn. */
export interface SpawnPermit {
  /** Release the permit exactly once; repeated releases are a no-op. */
  release(outcome?: SpawnOutcome, error?: unknown): void
}

export interface SpawnGateOptions {
  /** How long an acquisition may wait for a slot before the spawn fails. */
  readonly timeoutMs: number
  /** How many boots may run concurrently; defaults to 4. */
  readonly capacity?: number
}

export interface SpawnGateHandle {
  acquire(signal?: AbortSignal): Promise<SpawnPermit>
  /** Interrupt every pending holder fiber; later acquisitions are not supported. */
  close?(): Promise<void>
  /** Whether at least one boot slot is currently held. */
  readonly held?: boolean
  /** Exact count of currently held boot slots. */
  readonly active?: number
  /** The maximum number of concurrent boots. */
  readonly capacity?: number
  /** Number of acquisitions still queued for a slot. */
  readonly waiting?: number
}

/** A spawn could not obtain a boot slot within the configured timeout. */
export class SpawnGateTimeoutError extends Schema.TaggedErrorClass<SpawnGateTimeoutError>("SpawnGateTimeoutError")(
  "WorkerPools.SpawnGateTimeout",
  { timeoutMs: Schema.Number },
) {
  constructor(override readonly timeoutMs: number) {
    super({ timeoutMs })
    this.name = "SpawnGateTimeoutError"
  }
  override get message() {
    return `spawn gate: no worker spawn permit became available within ${this.timeoutMs}ms`
  }
}

/** A boot slot acquisition was aborted before it was granted. */
export class SpawnGateAbortedError extends Schema.TaggedErrorClass<SpawnGateAbortedError>("SpawnGateAbortedError")(
  "WorkerPools.SpawnGateAborted",
  {},
) {
  constructor() {
    super({})
    this.name = "SpawnGateAbortedError"
  }
  override get message() {
    return "spawn gate: acquisition aborted before a worker spawn permit was granted"
  }
}

const DEFAULT_SPAWN_CAPACITY = 4

function requirePositive(name: string, value: number): number {
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`spawn gate: ${name} must be a positive finite number, received ${value}`)
  }
  return value
}

function requirePositiveInteger(name: string, value: number): number {
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`spawn gate: ${name} must be a positive integer, received ${value}`)
  }
  return value
}

/**
 * The one global spawn admission. A `Semaphore` owns the permits (capacity 4 by
 * default); `withPermits(1)` is the acquisition primitive, so an interrupted
 * waiter leaves the queue without consuming a permit and every failed or
 * abandoned spawn releases exactly once.
 */
export function makeSpawnGate(options: SpawnGateOptions): SpawnGateHandle {
  const timeoutMs = requirePositive("timeoutMs", options.timeoutMs)
  const capacity = requirePositiveInteger("capacity", options.capacity ?? DEFAULT_SPAWN_CAPACITY)
  const semaphore = Semaphore.makeUnsafe(capacity)
  const scope = Scope.makeUnsafe()
  // Plain counters: every mutation below is synchronous, so JavaScript's single
  // thread already gives the gate the atomicity it needs.
  let activeCount = 0
  let waitingCount = 0

  const acquire = (signal?: AbortSignal): Promise<SpawnPermit> => {
    if (signal?.aborted === true) return Promise.reject(new SpawnGateAbortedError())

    const granted = Deferred.makeUnsafe<void>()
    const released = Deferred.makeUnsafe<void>()
    const aborted = Deferred.makeUnsafe<void>()
    let waitingCounted = false
    let holding = false

    const onAbort = (): void => {
      Deferred.doneUnsafe(aborted, Effect.void)
    }
    signal?.addEventListener("abort", onAbort, { once: true })
    let detached = false
    const detach = (): void => {
      if (detached) return
      detached = true
      signal?.removeEventListener("abort", onAbort)
    }

    const hold = semaphore.withPermits(1)(
      Effect.gen(function* () {
        waitingCount = Math.max(0, waitingCount - 1)
        waitingCounted = false
        activeCount += 1
        holding = true
        yield* Deferred.succeed(granted, undefined)
        yield* Deferred.await(released)
      }),
    )

    // Count the waiter before the fiber can run, so `waiting` is observable the
    // moment `acquire` is called (and never double-counted on abort).
    waitingCounted = true
    waitingCount += 1

    const permit = (): SpawnPermit => {
      let releasedOnce = false
      return {
        release: () => {
          if (releasedOnce) return
          releasedOnce = true
          Deferred.doneUnsafe(released, Effect.void)
        },
      }
    }

    const cleanup = Effect.sync(() => {
      detach()
      if (waitingCounted) {
        waitingCounted = false
        waitingCount = Math.max(0, waitingCount - 1)
      }
      if (holding) {
        holding = false
        activeCount = Math.max(0, activeCount - 1)
      }
    })

    const holder = Effect.raceFirst(
      hold,
      Deferred.await(aborted).pipe(Effect.andThen(Effect.interrupt)),
    ).pipe(Effect.catchCause(() => Effect.void), Effect.ensuring(cleanup))

    // The holder lives in the gate's scope so it outlives this call while held;
    // a failed or interrupted acquisition interrupts it so it can never
    // construct afterwards.
    return WorkerRuntime.runPromise(
      Effect.gen(function* () {
        const fiber = yield* Effect.forkIn(holder, scope)
        return yield* Effect.raceFirst(
          Deferred.await(granted).pipe(Effect.map(permit)),
          Deferred.await(aborted).pipe(Effect.andThen(Effect.fail(new SpawnGateAbortedError()))),
        ).pipe(
          Effect.timeoutOrElse({
            duration: timeoutMs,
            orElse: () => Effect.fail(new SpawnGateTimeoutError(timeoutMs)),
          }),
          Effect.onError(() => Fiber.interrupt(fiber).pipe(Effect.andThen(cleanup))),
        )
      }),
    )
  }

  return {
    acquire,
    close: () => WorkerRuntime.runPromise(Scope.close(scope, Exit.void)),
    get held() {
      return activeCount > 0
    },
    get active() {
      return activeCount
    },
    get capacity() {
      return capacity
    },
    get waiting() {
      return waitingCount
    },
  }
}

// ---------------------------------------------------------------------------
// Bun executor transport + pool (production adapters over `execute.ts` frames).
// ---------------------------------------------------------------------------

export interface BunExecutorTransportOptions {
  readonly entry: string | URL
  readonly env?: Record<string, string | undefined>
  readonly workerData?: unknown
  /** The SQLite file this isolate uses, for the live-worker/schema gate. */
  readonly databasePath?: string
  /**
   * OS thread name (`WorkerOptions.name`) applied at spawn, so the isolate is
   * born with the right Linux `comm` before it self-names. Absent leaves Bun's
   * default (`Worker`). Fail-soft: a name never affects spawning.
   */
  readonly name?: string
  readonly timeoutMs?: number
  /** Maximum buffered outbound frames kept for late `waitFor` matches. */
  readonly bufferCapacity?: number
  /** Called with the worker's transferred egress ports. */
  readonly onWorkerPorts?: (ports: WorkerTransferredPorts) => void
  /** Called with the worker's transferred hub-proxy control port. */
  readonly onHubProxyPort?: (port: MessagePort) => void
  /** Optional re-entry bridge for the worker lifecycle lines. */
  readonly bridge?: EffectBridge.Shape
  /** Boot-cancellation telemetry: a cancel win or a death during abort. */
  readonly onBootTelemetry?: (event: BootTelemetry) => void
  /**
   * Boot-stage phase reporting: `boot` before the isolate is constructed,
   * `materialize` when its validated `ready` handshake acknowledges the real
   * graph. The session worker then owns stream/tool/commit phases.
   */
  readonly onPhase?: (phase: "boot" | "materialize") => void
  /** Hub slot id for diagnostics (a boot/import failure is reported under it). */
  readonly workerID?: number
  /**
   * The one global spawn permit. When supplied, the isolate is constructed only
   * after `acquire()` resolves and the permit is released only on the worker's
   * validated `ready` handshake frame (port transfers arrive earlier). Absent
   * keeps the legacy eager construction (direct transport tests and
   * non-production callers).
   */
  readonly spawnGate?: SpawnGateHandle
  /**
   * Deadline for the boot-ready handshake after the worker is constructed. On
   * expiry the spawn fails alone (the permit is released and the in-flight wait
   * rejects); later sessions may spawn. Defaults to the transport `timeoutMs`.
   */
  readonly bootTimeoutMs?: number
}

const DEFAULT_TRANSPORT_TIMEOUT_MS = 30_000

/** Default deadline for waiting on the one global spawn permit. */
export const DEFAULT_SPAWN_GATE_TIMEOUT_MS = 30_000

/** Cap on buffered frames, so a transport nobody waits on cannot grow unbounded. */
export const DEFAULT_TRANSPORT_BUFFER_CAPACITY = 1024

/**
 * How long an isolate may take to acknowledge the orderly-shutdown frame before
 * the hub terminates it anyway. The worker's SQLite connections use a 250 ms
 * busy timeout (`WORKER_BUSY_TIMEOUT_MS`), so a synchronous statement reaches a
 * safepoint well inside this budget. The 1 s bound is deliberately ~4x that
 * timeout: tightening it to 250 ms would raw-terminate a healthy worker that is
 * merely finishing one blocked statement. The budget only covers a genuinely
 * wedged event loop; the raw-terminate fallback is logged and counted.
 */
const SHUTDOWN_ACK_TIMEOUT_MS = 1_000

/**
 * How long a slot terminate waits for an in-flight boot/init before detaching.
 * A wedged init resolver must not make `pool.terminateAll()` hang: the join is
 * abandoned at the deadline and the cache key is invalidated, so a late boot
 * result is discarded instead of published.
 */
const MATERIALIZE_JOIN_TIMEOUT_MS = 1_000

/** Raw terminates (an isolate that never acknowledged the shutdown frame). */
let rawTerminateCount = 0

/** Observe how many isolates had to be terminated without an orderly ack. */
export function workerShutdownStats(): { readonly rawTerminates: number } {
  return { rawTerminates: rawTerminateCount }
}

/** Workers already asked to close, so a second retire never re-posts `shutdown`. */
const shutdownRequested = new WeakSet<object>()

interface WorkerShutdownOptions {
  readonly bridge?: EffectBridge.Shape
  readonly workerID?: number
}

/**
 * Ask the isolate to close its native WAL handle and release its bus lease
 * before the hub calls `Worker.terminate()`. Terminating without this leaves the
 * bun:sqlite handle (and its WAL lock) alive until process exit. Bounded: a
 * wedged or half-booted worker is still terminated after the timeout, and the
 * raw terminate is logged/counted so the fallback is observable.
 */
async function requestWorkerShutdown(worker: Worker, options: WorkerShutdownOptions = {}): Promise<void> {
  // A fake/partial worker without event plumbing can never deliver the ack.
  // Post the close frame (recording fakes assert on it) and return without the
  // bounded wait, so every test retire does not stall for the full budget.
  if (typeof worker.addEventListener !== "function") {
    try {
      worker.postMessage({ type: "shutdown", id: 0 })
    } catch {
      // The isolate is already gone; there is nothing left to close.
    }
    return
  }
  if (shutdownRequested.has(worker)) return
  shutdownRequested.add(worker)
  let acknowledged = false
  await new Promise<void>((resolve) => {
    let settled = false
    let cancelTimer: (() => void) | undefined
    const finish = (): void => {
      if (settled) return
      settled = true
      cancelTimer?.()
      worker.removeEventListener?.("message", onMessage)
      resolve()
    }
    const onMessage = (event: MessageEvent): void => {
      const frame = event.data as { readonly type?: unknown } | undefined
      if (frame?.type !== "shutdown-ack") return
      acknowledged = true
      finish()
    }
    cancelTimer = scheduleTimeout(SHUTDOWN_ACK_TIMEOUT_MS, finish)
    worker.addEventListener?.("message", onMessage)
    try {
      worker.postMessage({ type: "shutdown", id: 0 })
    } catch {
      finish()
    }
  })
  if (acknowledged) return
  rawTerminateCount += 1
  void options.bridge?.fork(
    Effect.logWarning(workerLogMessage("worker shutdown ack timed out"), {
      ...(options.workerID === undefined ? {} : { workerID: options.workerID }),
      timeoutMs: SHUTDOWN_ACK_TIMEOUT_MS,
      reason: "the isolate reached no safepoint within the shutdown budget; terminating it raw",
    }),
  )
}

/** A validated boot/port frame; `invalid` fails the spawn loud. */
type BootFrame =
  | { readonly kind: "ready" }
  | { readonly kind: "worker-ports"; readonly ports: WorkerTransferredPorts }
  | { readonly kind: "hub-proxy"; readonly port: MessagePort }
  /**
   * `error` preserves a typed failure (e.g. `BuildMismatch`) for the caller.
   * Every invalid frame's teardown is orderly, because even a pre-ready isolate
   * may already have opened SQLite.
   */
  | { readonly kind: "invalid"; readonly reason: string; readonly error?: Error }

const asMessagePort = (value: unknown): MessagePort | undefined =>
  typeof MessagePort !== "undefined" && value instanceof MessagePort ? value : undefined

const TRANSFERRED_PORT_FIELDS = ["egress", "egressControl"] as const

/**
 * Parse a worker boot/port frame at the boundary. A frame with a boot/port
 * `type` is never silently cast or ignored: a port field that is not a real
 * `MessagePort` (or a `ready` without a numeric thread id) is `invalid`, so the
 * transport fails the spawn loud instead of hanging on a half-booted worker.
 * Any other shape is an executor frame (`undefined`).
 */
function parseBootFrame(data: unknown): BootFrame | undefined {
  if (typeof data !== "object" || data === null) return undefined
  const frame = data as {
    readonly type?: unknown
    readonly threadId?: unknown
    readonly port?: unknown
    readonly egress?: unknown
    readonly egressControl?: unknown
  }
  if (frame.type === "ready") {
    // Any `ready` frame proves the isolate opened its DB/WAL: even a rejected
    // one must close through the orderly shutdown path, never terminate-only.
    if (typeof frame.threadId !== "number") {
      return { kind: "invalid", reason: "boot-ready frame without a numeric threadId" }
    }
    try {
      verifyReadyBuild(frame)
    } catch (error) {
      if (error instanceof BuildMismatch) return { kind: "invalid", reason: error.message, error }
      return { kind: "invalid", reason: "boot-ready frame failed build-hash verification" }
    }
    return { kind: "ready" }
  }
  if (frame.type === "ports") {
    const ports: { -readonly [K in keyof WorkerTransferredPorts]: MessagePort | undefined } = {
      egress: undefined,
      egressControl: undefined,
    }
    for (const name of TRANSFERRED_PORT_FIELDS) {
      const value = frame[name]
      if (value === undefined) continue
      const port = asMessagePort(value)
      if (port === undefined) {
        return { kind: "invalid", reason: `ports frame field "${name}" is not a MessagePort` }
      }
      ports[name] = port
    }
    return { kind: "worker-ports", ports }
  }
  if (frame.type === "hub-proxy") {
    const port = asMessagePort(frame.port)
    return port === undefined
      ? { kind: "invalid", reason: "hub-proxy frame without a MessagePort" }
      : { kind: "hub-proxy", port }
  }
  return undefined
}

interface PendingWaiter {
  readonly predicate: (message: SessionExecutorOutbound) => boolean
  readonly resolve: (message: SessionExecutorOutbound) => void
  readonly reject: (error: Error) => void
  /** Cancels the deadline; absent when the wait has no deadline (`timeoutMs <= 0`). */
  readonly cancelTimeout: (() => void) | undefined
}

/**
 * Process-wide live executor isolates, keyed by the database file their env
 * points at. The schema gate must observe actual spawns, not just the
 * single-flight permit: a worker releases the permit when it becomes boot-ready
 * but stays alive (and writes) for the rest of its generation. `spawnGate.held`
 * therefore cannot answer "is any worker live?". Every hub in one process shares
 * this registry; a terminated or failed isolate removes itself.
 *
 * The schema gate is per database file, so a worker only blocks a migration of
 * the database it actually uses: unrelated hubs in the same process (tests, a
 * second project) must not refuse each other.
 */
const liveExecutorWorkers = new Map<object, string | undefined>()

/**
 * Spawns that hold a global permit but have not constructed their isolate yet.
 * The schema gate must refuse a migration for a database that an in-flight
 * spawn is about to open; the constructed-worker registry cannot see that
 * window. The token is removed the moment `new Worker` runs.
 */
const pendingSpawns = new Map<object, string | undefined>()

function beginPendingSpawn(databasePath: string | undefined): object {
  const token = {}
  pendingSpawns.set(token, databasePath)
  return token
}

function endPendingSpawn(token: object | undefined): void {
  if (token === undefined) return
  pendingSpawns.delete(token)
}

/** How many session-worker isolates live on `databasePath` (all files when omitted). */
export function liveSessionWorkerCount(databasePath?: string): number {
  const matches = (workerPath: string | undefined): boolean =>
    databasePath === undefined || (workerPath !== undefined && path.resolve(workerPath) === path.resolve(databasePath))
  let count = 0
  for (const workerPath of liveExecutorWorkers.values()) if (matches(workerPath)) count += 1
  for (const workerPath of pendingSpawns.values()) if (matches(workerPath)) count += 1
  return count
}

/**
 * Schedule a callback with Effect's clock instead of a raw timer. The
 * returned cancel function interrupts the sleep fiber, so a settled watchdog
 * never keeps the process alive. Used only for the native-worker boundaries
 * (promise APIs) that still need a deadline; Effect paths use `timeoutOrElse`.
 */
function scheduleTimeout(ms: number, fire: () => void): () => void {
  const fiber = WorkerRuntime.runFork(
    Effect.sleep(Duration.millis(ms)).pipe(Effect.andThen(Effect.sync(fire))),
  )
  let cancelled = false
  return () => {
    if (cancelled) return
    cancelled = true
    fiber.interruptUnsafe()
  }
}

/**
 * A `MessagePort`-free `ExecutorTransport` over a real Bun worker: buffers every
 * outbound frame so a `waitFor` may match one that already arrived, and fails
 * loud on timeout or isolate error.
 */
export function createBunExecutorTransport(options: BunExecutorTransportOptions): ExecutorTransport {
  const bridge = options.bridge
  const timeoutMs = options.timeoutMs ?? DEFAULT_TRANSPORT_TIMEOUT_MS
  const bufferCapacity = options.bufferCapacity ?? DEFAULT_TRANSPORT_BUFFER_CAPACITY
  const gate = options.spawnGate
  const bootTimeoutMs = options.bootTimeoutMs ?? timeoutMs
  const messages: SessionExecutorOutbound[] = []
  const waiters = new Set<PendingWaiter>()
  let alive = true
  // Set by `close`/`terminate`; a later `terminate` still destroys the thread
  // even though `alive` is already false (so a concurrent run sees it dead).
  let closing = false
  let worker: Worker | undefined
  let booting: Promise<void> | undefined
  /** The schema-gate reservation for a permit held before construction. */
  let reservedSpawn: object | undefined
  // Aborts the pending spawn-permit acquisition when the boot is cancelled or
  // terminated, so a cancelled spawn never waits out the gate deadline.
  let bootAcquisition: AbortController | undefined
  let bootTimer: (() => void) | undefined
  let bootLifecycle: BootLifecycle<Worker> | undefined
  // The one raw thread-destroy gate. Every teardown path (boot failure, boot
  // cancel, normal retire) closes the isolate orderly first and then destroys
  // the thread through this flag, so a second authority calls no second
  // `Worker.terminate()` for the same isolate.
  let threadDestroyed = false
  // A cancel that arrives while the boot is still waiting for the global permit.
  // The cancel resolves immediately; the flag tells the pending acquisition to
  // release its eventual permit without constructing the isolate.
  let waitingCancelled = false
  let waitingCancel:
    | { readonly promise: Promise<BootCancelResult>; readonly resolve: (result: BootCancelResult) => void }
    | undefined
  // Posts that arrived before the gate granted a permit; flushed in order once
  // the isolate exists so a caller's `post`/`waitFor` pair is never reordered.
  const queued: SessionExecutorInbound[] = []
  // The error a boot/termination raised. A worker can die before a caller's
  // `waitFor` registers (an isolate that throws at import is that fast), so the
  // terminal error is remembered and replayed to a late waiter instead of
  // leaving it to time out.
  let terminal: Error | undefined
  const failWaiters = (error: Error): void => {
    terminal = error
    for (const waiter of waiters) {
      waiter.cancelTimeout?.()
      waiters.delete(waiter)
      waiter.reject(error)
    }
  }

  const constructWorker = (): Worker => {
    // The isolate is about to exist: the registry (below) takes over the
    // schema-gate count, so the pre-construction reservation ends here.
    endPendingSpawn(reservedSpawn)
    reservedSpawn = undefined
    const created = new Worker(options.entry, {
      env: mergeEnv(options.env),
      // `smol` keeps each isolate's heap small; a per-session worker does not
      // need a full-size JS heap, and the cap only bounds concurrently running
      // workers, not the idle isolates a long session list can retain.
      smol: true,
      ...defined({ workerData: options.workerData, name: options.name }),
    })
    liveExecutorWorkers.set(created, options.databasePath ?? options.env?.OPENCODE_DB)
    return created
  }

  const clearBootTimer = (): void => {
    bootTimer?.()
    bootTimer = undefined
  }

  /**
   * Cancel a boot that is still waiting for the global permit. The caller gets
   * the typed cancel outcome NOW: the permit may be held by another worker for
   * its whole boot, and a cancel must not wait for it. The pending acquisition
   * later observes `waitingCancelled`, releases its permit, and never constructs.
   */
  const cancelWhileWaiting = (): Promise<BootCancelResult> => {
    waitingCancelled = true
    bootAcquisition?.abort()
    if (waitingCancel === undefined) {
      let resolve!: (result: BootCancelResult) => void
      const promise = new Promise<BootCancelResult>((settle) => {
        resolve = settle
      })
      waitingCancel = { promise, resolve }
      resolve(BOOT_CANCELLED)
    }
    return waitingCancel.promise
  }

  /**
   * Cancel the active boot interval. A boot that already started (the run was
   * handed the worker) yields to normal cancellation, so this resolves `undefined`.
   */
  const cancelBoot = (): Promise<BootCancelResult | undefined> => {
    if (bootLifecycle !== undefined) return bootLifecycle.cancel()
    if (booting !== undefined) return cancelWhileWaiting()
    return Promise.resolve(undefined)
  }

  /** Remove and return the first buffered frame matching `predicate`, if any. */
  const takeBuffered = (
    predicate: (message: SessionExecutorOutbound) => boolean,
  ): SessionExecutorOutbound | undefined => {
    const index = messages.findIndex(predicate)
    if (index === -1) return undefined
    return messages.splice(index, 1)[0]
  }

  /**
   * Destroy the raw thread exactly once, whichever teardown path reaches it.
   * The orderly shutdown frame is attempted by each caller before this; the
   * flag is what keeps a persisted cache release from terminating an isolate a
   * failed-boot teardown already destroyed.
   */
  const destroyThread = (target: Worker): Promise<number | void> => {
    if (threadDestroyed) return Promise.resolve(0)
    threadDestroyed = true
    return Promise.resolve(target.terminate())
  }

  /**
   * Tear down the isolate that owns the active boot interval. Every path is an
   * orderly close: a booting/ready lifecycle owns the permit and its terminate
   * callback releases it once, while a started worker is closed here. A worker
   * that was never constructed (`partial === undefined`) has nothing to close.
   */
  const teardownBooted = (created: Worker): void => {
    liveExecutorWorkers.delete(created)
    if (bootLifecycle !== undefined && (bootLifecycle.state === "booting" || bootLifecycle.state === "ready")) {
      void bootLifecycle.cancel().catch(() => undefined)
      return
    }
    void requestWorkerShutdown(created, { ...(bridge === undefined ? {} : { bridge }), workerID: options.workerID })
      .then(() => destroyThread(created))
      .catch(() => undefined)
  }

  const connect = (created: Worker): void => {
    /**
     * Fail one spawn loud on a malformed boot/port frame, then tear the isolate
     * down. A typed cause (a `BuildMismatch` from a stale bundle) is preserved so
     * the dispatcher can refuse to retry it instead of crash-looping. The
     * teardown is always orderly: a rejected `ready` isolate already opened its
     * DB/WAL, and even a pre-ready frame may have opened SQLite before `ports`.
     */
    const failProtocol = (reason: string, cause?: Error): void => {
      const error = cause ?? new Error(`session worker protocol error: ${reason}`)
      alive = false
      created.onmessage = null
      created.onerror = null
      void bridge?.fork(
        Effect.logError(workerLogMessage("worker boot failed"), {
          workerID: options.workerID,
          message: error.message,
        }),
      )
      failWaiters(error)
      teardownBooted(created)
    }

    created.onmessage = (event: MessageEvent) => {
      // Production sends `ports`/`hub-proxy` before the executor announces
      // `ready`, so only the validated ready handshake releases the permit.
      // Releasing there (not on construction or the first frame) is what
      // serializes the expensive cold boot; the boot timer stays armed until then.
      const bootFrame = parseBootFrame(event.data)
      if (bootFrame?.kind === "invalid") {
        failProtocol(bootFrame.reason, bootFrame.error)
        return
      }
      if (bootFrame?.kind === "worker-ports") {
        options.onWorkerPorts?.(bootFrame.ports)
        return
      }
      if (bootFrame?.kind === "hub-proxy") {
        options.onHubProxyPort?.(bootFrame.port)
        return
      }
      if (bootFrame?.kind === "ready") {
        options.onPhase?.("materialize")
        // The permit is released as boot-ready only through the lifecycle's
        // `onRun`; a cancelled boot refuses the start and drops the frame.
        if (bootLifecycle !== undefined && !bootLifecycle.start(created)) return
      }
      const message = event.data as SessionExecutorOutbound
      // Offer the frame to a waiting caller BEFORE buffering, so a matching reply
      // can never be evicted by the capacity cap. A frame is consumed by exactly
      // one waiter; only a frame no waiter wants is buffered (and the buffer only
      // ever drops oldest frames that no waiter has claimed).
      for (const waiter of [...waiters]) {
        if (!waiter.predicate(message)) continue
        waiter.cancelTimeout?.()
        waiters.delete(waiter)
        waiter.resolve(message)
        return
      }
      messages.push(message)
      if (messages.length > bufferCapacity) messages.shift()
    }

    created.onerror = (event: ErrorEvent) => {
      alive = false
      // Detach so a terminating worker cannot re-enter this handler, then fail
      // every waiter and tear the isolate down.
      created.onmessage = null
      created.onerror = null
      // A boot/import failure often reports an empty `message`; keep the
      // originating error's text and chain it as `cause` so the caller is not
      // left with a generic "worker crashed".
      const cause = event.error
      const detail =
        cause instanceof Error && cause.message !== "" && cause.message !== event.message ? `: ${cause.message}` : ""
      const message = `session worker error: ${event.message}${detail}`
      // A boot/import failure is the loudest signal a worker could not start;
      // report it before the waiters fail so the operator sees why.
      void bridge?.fork(
        Effect.logError(workerLogMessage("worker boot failed"), {
          workerID: options.workerID,
          message,
        }),
      )
      failWaiters(new Error(message, cause === undefined ? undefined : { cause }))
      teardownBooted(created)
    }

    // A worker that exits on its own (a hard crash, `process.exit`, an OOM kill)
    // reports `close`, not `error`. Without this a dead isolate would only be
    // noticed when the run reply times out, losing the crash phase truth.
    created.addEventListener?.("close", (event: Event) => {
      if (!alive) return
      alive = false
      created.onmessage = null
      created.onerror = null
      const code = (event as Event & { readonly code?: number }).code
      failWaiters(new Error(`session worker exited with code ${code ?? "unknown"}`))
      teardownBooted(created)
    })
  }

  /**
   * Own one boot interval: the acquired permit is held until the validated
   * `ready` frame starts the run, or released exactly once when the boot is
   * cancelled/failed. The lifecycle terminates a partially materialized isolate
   * and reports the typed boot outcome to the cancel caller.
   */
  const startBoot = (acquired: SpawnPermit): void => {
    let created: Worker | undefined
    const lifecycle = createBootLifecycle<Worker>({
      permit: acquired,
      materialize: async (signal) => {
        if (signal.aborted) throw new Error("session worker boot aborted")
        // Arm the deadline before construction so a synchronous `new Worker`
        // throw still releases the global permit (the rejection path below).
        bootTimer = scheduleTimeout(bootTimeoutMs, () => {
          bootTimer = undefined
          const error = new Error(`session worker boot timeout after ${bootTimeoutMs}ms`)
          alive = false
          queued.length = 0
          failWaiters(error)
          void lifecycle.cancel().catch(() => undefined)
        })
        options.onPhase?.("boot")
        created = constructWorker()
        worker = created
        connect(created)
        for (const message of queued.splice(0)) created.postMessage(message)
        return created
      },
      terminate: async (partial) => {
        worker = undefined
        if (partial === undefined) return
        liveExecutorWorkers.delete(partial)
        // Every boot teardown is an orderly close. The worker opens SQLite
        // before it sends `ports`, so even a pre-ready deadline or malformed
        // frame can hold the WAL lock: always attempt the bounded shutdown ack
        // before the thread is destroyed. A wedged isolate still falls back to
        // the raw terminate after the ack budget, logged and counted.
        await requestWorkerShutdown(partial, { ...(bridge === undefined ? {} : { bridge }), workerID: options.workerID })
        await destroyThread(partial)
      },
      onRun: () => {
        // The validated ready handshake is the one boot-ready permit release.
        clearBootTimer()
        acquired.release("ready")
      },
      onTelemetry: options.onBootTelemetry,
    })
    bootLifecycle = lifecycle
    void lifecycle.ready.then(
      (result) => {
        if (result === BOOT_CANCELLED) {
          // A cancelled boot is terminal for this transport: the run it was
          // materializing must settle instead of waiting out its deadline.
          alive = false
          queued.length = 0
          failWaiters(new Error("session worker boot cancelled"))
        }
      },
      (error: unknown) => {
        const failure = error instanceof Error ? error : new Error(String(error))
        clearBootTimer()
        endPendingSpawn(reservedSpawn)
        reservedSpawn = undefined
        alive = false
        queued.length = 0
        failWaiters(failure)
      },
    )
  }

  /**
   * Acquire the permit, construct the isolate and flush queued posts. Only used
   * when a spawn gate is configured; without one the worker is built eagerly.
   */
  const boot = (): void => {
    if (booting !== undefined || worker !== undefined || gate === undefined || !alive) return
    const acquisition = new AbortController()
    bootAcquisition = acquisition
    booting = (async () => {
      const acquired = await gate.acquire(acquisition.signal)
      // The cancel already resolved to its caller; release the permit the
      // acquisition granted and never construct the isolate. A terminate that
      // landed while waiting takes the same path without a cancel outcome.
      bootAcquisition = undefined
      if (!alive || waitingCancelled) {
        endPendingSpawn(reservedSpawn)
        reservedSpawn = undefined
        const cancelled = waitingCancelled
        acquired.release("failed", cancelled ? undefined : new Error("session worker terminated before boot"))
        alive = false
        queued.length = 0
        failWaiters(new Error(cancelled ? "session worker boot cancelled" : "session worker terminated before boot"))
        if (cancelled) options.onBootTelemetry?.({ type: "boot-cancelled", phase: "boot" })
        if (waitingCancel !== undefined) {
          const waiting = waitingCancel
          waitingCancel = undefined
          waiting.resolve(BOOT_CANCELLED)
        }
        return
      }
      // Hold the schema-gate reservation across the pre-construction window
      // (permit granted, `new Worker` not yet run).
      reservedSpawn = beginPendingSpawn(options.databasePath ?? options.env?.OPENCODE_DB)
      startBoot(acquired)
    })().catch((error) => {
      bootAcquisition = undefined
      endPendingSpawn(reservedSpawn)
      reservedSpawn = undefined
      const failure = error instanceof Error ? error : new Error(String(error))
      // A cancelled waiter must settle even when the permit acquisition failed.
      if (waitingCancel !== undefined) {
        const waiting = waitingCancel
        waitingCancel = undefined
        waiting.resolve(BOOT_CANCELLED)
      }
      if (!alive) return
      alive = false
      queued.length = 0
      failWaiters(failure)
    })
  }

  if (gate === undefined) {
    worker = constructWorker()
    connect(worker)
  }

  return {
    post(message: SessionExecutorInbound) {
      if (!alive) throw new Error("cannot post to a terminated session worker")
      if (worker !== undefined) {
        worker.postMessage(message)
        return
      }
      queued.push(message)
      boot()
    },
    waitFor(predicate, waitOptions) {
      const buffered = takeBuffered(predicate)
      if (buffered !== undefined) return Promise.resolve(buffered)
      // A buffered frame still wins (it arrived before the failure); otherwise an
      // already-dead isolate fails the wait immediately rather than after the
      // full deadline.
      if (terminal !== undefined) return Promise.reject(terminal)
      // A transport observed before its first `post` still boots eagerly, so the
      // gate never depends on the caller's post/wait ordering.
      boot()
      const waitMs = waitOptions?.timeoutMs ?? timeoutMs
      return new Promise<SessionExecutorOutbound>((resolve, reject) => {
        // `timeoutMs <= 0` means "no deadline": a run/startShell reply is a whole
        // LLM turn that can outlast any fixed budget, so it never gets a timer.
        // The configured deadline applies only to the short control frames.
        const cancelTimeout =
          waitMs > 0
            ? scheduleTimeout(waitMs, () => {
                waiters.delete(waiter)
                reject(new SessionWorkerReplyTimeoutError(waitMs))
              })
            : undefined
        const waiter: PendingWaiter = { predicate, resolve, reject, cancelTimeout }
        waiters.add(waiter)
        // A caller that abandons the wait (the liveness probe on a resolved run)
        // aborts, so the waiter does not linger until its own deadline.
        waitOptions?.signal?.addEventListener(
          "abort",
          () => {
            if (!waiters.delete(waiter)) return
            cancelTimeout?.()
            reject(new Error("session worker wait aborted"))
          },
          { once: true },
        )
      })
    },
    get alive() {
      return alive
    },
    cancelBoot,
    /**
     * Close the isolate's durable resources while its realm still runs. Exposed
     * to the executor so every retire (including a materializing/rebind path)
     * goes through the orderly shutdown frame before `terminate()`.
     */
    async close(): Promise<void> {
      if (closing) return
      closing = true
      // The run is terminal the moment retirement starts: mark the isolate dead
      // and fail in-flight waits now, so a slow turn cannot complete during the
      // bounded shutdown-ack wait and the dispatcher classifies a dead worker.
      alive = false
      queued.length = 0
      failWaiters(new Error("session worker terminated"))
      const current = worker
      if (current === undefined) return
      await requestWorkerShutdown(current, { ...(bridge === undefined ? {} : { bridge }), workerID: options.workerID })
    },
    async terminate(code = 0): Promise<number> {
      // A `close()` already marked the isolate dead; the thread still exists and
      // must be destroyed, so only an isolate already destroyed (or one that
      // died without a close) short-circuits.
      if (threadDestroyed || (!alive && !closing)) return code
      closing = true
      alive = false
      queued.length = 0
      // Fail every in-flight run before the bounded shutdown wait, so callers
      // observe the rejection immediately while the isolate closes its handle.
      failWaiters(new Error("session worker terminated"))
      // A booting/ready interval still owns the permit: cancelling it terminates
      // the partial isolate and releases the permit exactly once. A boot still
      // waiting for the permit resolves its waiter so the next acquisition frees.
      if (bootLifecycle !== undefined && (bootLifecycle.state === "booting" || bootLifecycle.state === "ready")) {
        await bootLifecycle.cancel().catch(() => undefined)
        return code
      }
      if (bootLifecycle === undefined && booting !== undefined) {
        bootAcquisition?.abort()
        void cancelWhileWaiting()
        return code
      }
      const current = worker
      worker = undefined
      if (current === undefined) return code
      liveExecutorWorkers.delete(current)
      // Close the isolate's SQLite handle/lease while its realm still runs, then
      // destroy the thread. `terminate()` alone would strand the WAL lock, so
      // this applies to an in-flight run too: a forced cancel mid-tool must not
      // keep the write lock until process exit. The wait is bounded, so a wedged
      // worker is still terminated.
      await requestWorkerShutdown(current, { ...(bridge === undefined ? {} : { bridge }), workerID: options.workerID })
      const result = await destroyThread(current)
      return typeof result === "number" ? result : code
    },
  }
}

export interface BunExecutorWorkerPoolOptions {
  readonly entry: string | URL
  readonly env?: Record<string, string | undefined>
  readonly workerData?: unknown
  /** The SQLite file this isolate uses, for the live-worker/schema gate. */
  readonly databasePath?: string
  readonly timeoutMs?: number
  /** Run-reply liveness budget; absent uses the default-on budget, `0` disables it. */
  readonly runIdleTimeoutMs?: number
  readonly workKind?: string
  /**
   * Per-session durable-egress barrier: `execute` holds the
   * run until this resolves, so a result is never recorded before the run's
   * durable egress write is acked. Absent makes the result immediate.
   */
  readonly egressBarrier?: (sessionID: SessionID) => Promise<void> | undefined
  /** Receive each worker's transferred egress ports. */
  readonly onWorkerPorts?: (workerID: number, ports: WorkerTransferredPorts) => void
  /** Receive each worker's transferred hub-proxy control port. */
  readonly onHubProxyPort?: (workerID: number, port: MessagePort) => void
  /** The session the isolate is bound to, when known at spawn. */
  readonly sessionID?: SessionID
  /** Why this isolate is spawning; defaults to `"initial"`. */
  readonly reason?: WorkerSpawnReason
  /** Optional re-entry bridge for the worker lifecycle lines. */
  readonly bridge?: EffectBridge.Shape
  /** Boot-cancellation telemetry: a cancel win or a death during abort. */
  readonly onBootTelemetry?: (event: BootTelemetry) => void
  /** Boot-stage phase reporting for the isolate. */
  readonly onPhase?: (phase: "boot" | "materialize") => void
  /** One global spawn permit; forwarded to the transport so cold spawns serialize. */
  readonly spawnGate?: SpawnGateHandle
  /** Boot-ready deadline; see {@link BunExecutorTransportOptions.bootTimeoutMs}. */
  readonly bootTimeoutMs?: number
  /** Idle isolates retained by the pool; defaults to {@link resolveIdleWorkerCapacity}. */
  readonly maxIdleWorkers?: number
}

/**
 * Create one eager Bun executor worker speaking the frame protocol. When a
 * spawn gate is supplied, the isolate is constructed only after the global
 * permit is granted and the permit is released on the worker's `ready` frame.
 */
export function spawnBunExecutorWorker(id: number, options: BunExecutorWorkerPoolOptions): ExecutorWorker {
  const onWorkerPorts = options.onWorkerPorts
  const onHubProxyPort = options.onHubProxyPort
  // The options doc promises a default-on budget even for a direct spawn. The
  // pool factories resolve once and pass a concrete value, so this is a no-op
  // for them (and never double-warns).
  const runIdleTimeoutMs = resolveRunIdleTimeoutMs(options)
  logRunIdleTimeoutWarning(options.bridge, options)
  const transport = createBunExecutorTransport({
    entry: options.entry,
    ...defined({
      env: options.env,
      databasePath: options.databasePath,
      workerData: options.workerData,
      // Born named for its bound session (or the legacy slot id when unknown)
      // so the isolate is observable from creation, not only once it self-names.
      // TODO(N-slice): route through thread-naming.ts once its seam lands.
      name: sessionThreadName(options.sessionID ?? String(id)),
      timeoutMs: options.timeoutMs,
      ...(options.bridge === undefined ? {} : { bridge: options.bridge }),
      workerID: id,
      spawnGate: options.spawnGate,
      bootTimeoutMs: options.bootTimeoutMs,
      onBootTelemetry: options.onBootTelemetry,
      ...(options.onPhase === undefined ? {} : { onPhase: options.onPhase }),
      onWorkerPorts:
        onWorkerPorts === undefined ? undefined : (ports: WorkerTransferredPorts) => onWorkerPorts(id, ports),
      onHubProxyPort: onHubProxyPort === undefined ? undefined : (port: MessagePort) => onHubProxyPort(id, port),
    }),
  })
  return createExecutorWorker({
    id,
    transport,
    ...defined({
      workKind: options.workKind,
      timeoutMs: options.timeoutMs,
      runIdleTimeoutMs,
      sessionID: options.sessionID,
      reason: options.reason,
      egressBarrier: options.egressBarrier,
      // Forward the bridge so the `worker spawning` line fires in production,
      // where this helper (not a test's direct `createExecutorWorker`) builds
      // the executor.
      ...(options.bridge === undefined ? {} : { bridge: options.bridge }),
    }),
  })
}

/** Production `ExecutorWorkerPool`: spawns frame-protocol workers on demand. */
export function createBunExecutorWorkerPool(options: BunExecutorWorkerPoolOptions): IdleBoundedExecutorWorkerPool {
  // Resolve the default-on watchdog HERE: this factory is the documented entry
  // point, so an absent option must still arm the budget (an explicit `0`
  // disables it) rather than silently reaching the executor as `undefined`.
  const runIdleTimeoutMs = resolveRunIdleTimeoutMs(options)
  logRunIdleTimeoutWarning(options.bridge, options)
  let pool: ExecutorWorkerPool | undefined
  const idleCap = createIdleIsolateCap({
    capacity: options.maxIdleWorkers ?? resolveIdleWorkerCapacity(),
    ...(options.bridge === undefined ? {} : { bridge: options.bridge }),
    onEvict: (entry) => pool?.releaseSession?.(entry.sessionID, entry.worker.id),
  })
  pool = createExecutorWorkerPool({
    spawn: (id) => idleCap.track(spawnBunExecutorWorker(id, { ...options, runIdleTimeoutMs })),
  })
  return withIdleIsolateCap(pool, idleCap)
}

// ---------------------------------------------------------------------------
// Worker cap, idle retention, and materialization.
// ---------------------------------------------------------------------------

/** Minimum reserved child-headroom slots per nesting depth (D23). */
const WORKER_CAP_MIN_HEADROOM = 2

/**
 * A FIFO concurrency cap for running workers. At most `max` foreground runs
 * (depth 0) execute at once; the rest wait in arrival order. A parent that
 * awaits a foreground `task` child keeps its own slot, and the depth-1 child
 * runs in bounded headroom instead of deadlocking on the same budget. Each
 * nesting depth gets its own headroom, so a depth-N child can always start once
 * its ancestors are blocked.
 *
 * This is scheduling state only — it never rejects work and never owns
 * admission (the dispatcher does).
 */
export interface WorkerCap {
  /** Acquire one slot for a run at `depth` (0/absent = foreground). */
  acquire(depth?: number): Promise<() => void>
  readonly running: number
  readonly queued: number
}

interface WorkerCapHeadroom {
  readonly running: number
  readonly waiters: ReadonlyArray<number>
}

/**
 * The whole cap is one immutable value. `active` is the set of tickets that
 * currently own a slot; releasing a ticket that is absent is a no-op, which is
 * what makes the synchronous release idempotent.
 */
interface WorkerCapState {
  readonly foreground: number
  readonly foregroundWaiters: ReadonlyArray<number>
  readonly depths: ReadonlyMap<number, WorkerCapHeadroom>
  readonly active: ReadonlySet<number>
  readonly nextTicket: number
}

interface CapAdmission {
  readonly state: WorkerCapState
  readonly ticket: number
  readonly admitted: boolean
}

interface CapRelease {
  readonly state: WorkerCapState
  readonly granted?: { readonly ticket: number; readonly level: number }
}

const initialWorkerCapState = (): WorkerCapState => ({
  foreground: 0,
  foregroundWaiters: [],
  depths: new Map(),
  active: new Set(),
  nextTicket: 1,
})

const headroomOf = (state: WorkerCapState, depth: number): WorkerCapHeadroom =>
  state.depths.get(depth) ?? { running: 0, waiters: [] }

const withHeadroom = (state: WorkerCapState, depth: number, headroom: WorkerCapHeadroom): WorkerCapState => {
  const depths = new Map(state.depths)
  depths.set(depth, headroom)
  return { ...state, depths }
}

/** Admit a fresh ticket when its budget has room, otherwise append it to FIFO. */
const requestSlot = (state: WorkerCapState, level: number, max: number, headroom: number): CapAdmission => {
  const ticket = state.nextTicket
  const next = { ...state, nextTicket: ticket + 1 }
  if (level === 0) {
    if (state.foreground < max) {
      return {
        state: { ...next, foreground: state.foreground + 1, active: new Set(state.active).add(ticket) },
        ticket,
        admitted: true,
      }
    }
    return { state: { ...next, foregroundWaiters: [...state.foregroundWaiters, ticket] }, ticket, admitted: false }
  }
  const budget = headroomOf(state, level)
  if (budget.running < headroom) {
    return {
      state: {
        ...withHeadroom(next, level, { running: budget.running + 1, waiters: budget.waiters }),
        active: new Set(state.active).add(ticket),
      },
      ticket,
      admitted: true,
    }
  }
  return {
    state: withHeadroom(next, level, { running: budget.running, waiters: [...budget.waiters, ticket] }),
    ticket,
    admitted: false,
  }
}

/**
 * Free one ticket's slot and promote the oldest waiter in the same budget.
 * Promotion keeps `running` unchanged because the waiter takes the freed slot.
 */
const releaseSlot = (state: WorkerCapState, level: number, ticket: number): CapRelease => {
  if (!state.active.has(ticket)) return { state }
  const active = new Set(state.active)
  active.delete(ticket)
  if (level === 0) {
    const [nextTicket, ...rest] = state.foregroundWaiters
    if (nextTicket === undefined) return { state: { ...state, foreground: state.foreground - 1, active } }
    return {
      state: { ...state, foregroundWaiters: rest, active: new Set(active).add(nextTicket) },
      granted: { ticket: nextTicket, level: 0 },
    }
  }
  const budget = headroomOf(state, level)
  const [nextTicket, ...rest] = budget.waiters
  if (nextTicket === undefined) {
    return { state: { ...withHeadroom(state, level, { running: budget.running - 1, waiters: [] }), active } }
  }
  return {
    state: {
      ...withHeadroom(state, level, { running: budget.running, waiters: rest }),
      active: new Set(active).add(nextTicket),
    },
    granted: { ticket: nextTicket, level },
  }
}

export function createWorkerCap(max: number): WorkerCap {
  if (!Number.isInteger(max) || max < 1) {
    throw new Error(`worker cap must be a positive integer, received ${String(max)}`)
  }
  // Additive, transient headroom per nesting depth: bounded so child fan-out
  // cannot oversubscribe the machine, but never part of the foreground `max`.
  const headroom = Math.max(WORKER_CAP_MIN_HEADROOM, Math.ceil(max / 2))
  let state = initialWorkerCapState()
  const waiters = new Map<number, (release: () => void) => void>()

  const releaseFor = (ticket: number, level: number): (() => void) => () => {
    const released = releaseSlot(state, level, ticket)
    state = released.state
    const granted = released.granted
    if (granted === undefined) return
    const resolve = waiters.get(granted.ticket)
    if (resolve === undefined) return
    waiters.delete(granted.ticket)
    resolve(releaseFor(granted.ticket, granted.level))
  }

  const acquire = (depth = 0): Promise<() => void> => {
    // A malformed depth is a foreground run, matching the executor frame parse.
    const level = Number.isInteger(depth) && depth > 0 ? depth : 0
    const admission = requestSlot(state, level, max, headroom)
    state = admission.state
    if (admission.admitted) return Promise.resolve(releaseFor(admission.ticket, level))
    return new Promise<() => void>((resolve) => {
      waiters.set(admission.ticket, resolve)
    })
  }

  return {
    get running() {
      let total = state.foreground
      for (const budget of state.depths.values()) total += budget.running
      return total
    },
    get queued() {
      let total = state.foregroundWaiters.length
      for (const budget of state.depths.values()) total += budget.waiters.length
      return total
    },
    acquire,
  }
}

/**
 * Default idle-isolate retention: two per available core. Reuse across turns is
 * valuable, but a process that has dispatched thousands of sessions must not
 * retain one idle isolate for every session ever seen. Two per core matches the
 * oversubscription factor already used for in-flight work while keeping the
 * retained footprint legible.
 */
export function resolveIdleWorkerCapacity(cores: number = os.availableParallelism()): number {
  const available = Number.isInteger(cores) && cores > 0 ? cores : 1
  return Math.max(1, available * 2)
}

/** An idle isolate selected for eviction (never one with an in-flight run). */
export interface IdleWorkerEntry {
  readonly worker: ExecutorWorker
  readonly sessionID: SessionID
}

/**
 * Default deadline for one idle-eviction step (the retirement hook and the
 * isolate terminate). A wedged hook/terminate must not pin the single-flight
 * and let idle isolates grow past the cap.
 */
const DEFAULT_IDLE_EVICT_TIMEOUT_MS = 5_000

export interface IdleIsolateCapOptions {
  /** Idle isolates retained; defaults to {@link resolveIdleWorkerCapacity}. */
  readonly capacity?: number
  /**
   * Called before the idle isolate is terminated. The production pool routes it
   * through the dispatcher's full retirement (hub ports, pending asks, lease) and
   * awaits it, so eviction is not complete until the old generation is really
   * gone.
   */
  readonly onEvict: (entry: IdleWorkerEntry) => unknown
  /** Deadline for one eviction step; defaults to 5s. */
  readonly evictTimeoutMs?: number
  readonly bridge?: EffectBridge.Shape
}

/**
 * Cap the number of idle isolates by LRU, never touching a worker with an
 * in-flight run (active) or one still materializing/generating (also inside
 * `execute`). The tracker sits ABOVE a pool worker: `execute` marks it active,
 * its settlement marks it idle, and the oldest idle worker above the cap is
 * retired. Eviction goes through the same orderly `terminate` a reap would use.
 */
export interface IdleIsolateCap {
  readonly capacity: number
  /** Idle isolates currently retained. */
  readonly idle: number
  readonly evictions: number
  /** Wrap a pool worker so its runs update the active/idle LRU. */
  track(worker: ExecutorWorker): ExecutorWorker
  /** Stop tracking a worker a dispatcher/pool has already retired. */
  forget(workerID: number): void
  /** Stop tracking every worker without terminating (pool teardown does that). */
  clear(): void
  /**
   * Ask for another enforcement pass. Atomic with the flight lifecycle: when a
   * flight is in progress the request becomes a recheck instead of being lost.
   */
  requestEnforce(): void
}

export function createIdleIsolateCap(options: IdleIsolateCapOptions): IdleIsolateCap {
  const capacity = options.capacity ?? resolveIdleWorkerCapacity()
  if (!Number.isInteger(capacity) || capacity < 1) {
    throw new Error(`idle worker cap must be a positive integer, received ${String(capacity)}`)
  }
  interface Tracked {
    readonly worker: ExecutorWorker
    sessionID: SessionID | undefined
    terminated: boolean
  }
  interface IdleCapState {
    readonly tracked: ReadonlyMap<number, Tracked>
    readonly idle: ReadonlyMap<number, Tracked>
    readonly active: ReadonlySet<number>
    readonly evictions: number
  }
  let state: IdleCapState = {
    tracked: new Map(),
    idle: new Map(),
    active: new Set(),
    evictions: 0,
  }

  /**
   * Apply one synchronous transition from a Promise-facing call site. The body
   * never suspends and never re-enters the state, so applying it directly is
   * safe.
   */
  const mutate = <A>(change: (current: IdleCapState) => readonly [A, IdleCapState]): A => {
    const [value, next] = change(state)
    state = next
    return value
  }

  const evictTimeoutMs = options.evictTimeoutMs ?? DEFAULT_IDLE_EVICT_TIMEOUT_MS
  const boundedEvict = <A>(effect: Effect.Effect<A, unknown>, message: string, entry: Tracked) =>
    effect.pipe(
      Effect.timeoutOrElse({
        duration: evictTimeoutMs,
        orElse: () =>
          Effect.sync(() => {
            void options.bridge?.fork(
              Effect.logWarning(workerLogMessage(message), {
                workerID: entry.worker.id,
                sessionID: entry.sessionID,
                timeoutMs: evictTimeoutMs,
              }),
            )
          }),
      }),
      Effect.catchCause(() => Effect.void),
    )

  const evict = Effect.fnUntraced(function* (entry: Tracked) {
    mutate((current) => {
      entry.terminated = true
      const tracked = new Map(current.tracked)
      tracked.delete(entry.worker.id)
      const idle = new Map(current.idle)
      idle.delete(entry.worker.id)
      const active = new Set(current.active)
      active.delete(entry.worker.id)
      return [undefined, { ...current, tracked, idle, active, evictions: current.evictions + 1 }] as const
    })
    const sessionID = entry.sessionID
    if (sessionID !== undefined) {
      // Await the retirement so a successor is never admitted while the old
      // generation's ports/lease are still live. Both the hook and the close are
      // bounded: a wedged eviction must not pin the single-flight forever.
      yield* boundedEvict(
        Effect.tryPromise(() => Promise.resolve(options.onEvict({ worker: entry.worker, sessionID }))),
        "idle worker eviction timed out",
        entry,
      )
    }
    void options.bridge?.fork(
      Effect.logInfo(workerLogMessage("idle worker evicted"), {
        workerID: entry.worker.id,
        sessionID,
        capacity,
      }),
    )
    // Await the same in-flight close a later retire will join: eviction is not
    // complete until the isolate has exited (or the deadline elapses).
    yield* boundedEvict(
      Effect.tryPromise(() => entry.worker.terminate()),
      "idle worker terminate timed out",
      entry,
    )
  })

  /**
   * One eviction pass down to capacity. Guarded so a wedged probe/hook never
   * pins the loop: every awaited eviction step is bounded, and the caller
   * (`runEnforcement`) catches any defect and keeps serving the queue.
   */
  const enforceOnce = Effect.fnUntraced(function* () {
    while (true) {
      const next = mutate((current) => {
        if (current.idle.size <= capacity) return [undefined, current] as const
        const oldest = current.idle.entries().next()
        if (oldest.done) return [undefined, current] as const
        const [workerID, entry] = oldest.value
        const idle = new Map(current.idle)
        idle.delete(workerID)
        return [[workerID, entry] as const, { ...current, idle }] as const
      })
      if (next === undefined) return
      const [workerID, entry] = next
      if (entry.terminated || mutate((current) => [current.active.has(workerID), current])) continue
      // Background work must outlive an idle turn; keep one worker alive even
      // above the cap rather than killing a running job. Its next settlement
      // (or the dispatcher's idle reap) revisits the cap.
      const sessionID = entry.sessionID
      if (sessionID !== undefined && entry.worker.hasActiveWork !== undefined) {
        const hasActiveWork = entry.worker.hasActiveWork
        const background = yield* Effect.tryPromise(() => hasActiveWork(sessionID)).pipe(
          Effect.catchCause(() => Effect.succeed(false)),
        )
        if (background) {
          mutate((current) => {
            if (entry.terminated || current.active.has(workerID)) return [undefined, current] as const
            const idle = new Map(current.idle)
            idle.set(workerID, entry)
            return [undefined, { ...current, idle }] as const
          })
          return
        }
        // A new run may have started while the probe was in flight.
        if (entry.terminated || mutate((current) => [current.active.has(workerID), current])) continue
      }
      yield* evict(entry)
    }
  })

  /**
   * The single scoped consumer. Requests coalesce in the queue: a burst of
   * settlements wakes the loop once, and the non-blocking drain collapses every
   * request that arrived during the previous pass into one more pass. A failed
   * pass is logged and the loop continues; closing the scope interrupts it.
   */
  const requests = WorkerRuntime.runSync(Queue.unbounded<void>())
  const scope = Scope.makeUnsafe()
  WorkerRuntime.runFork(
    Effect.forkIn(
      Effect.gen(function* () {
        while (true) {
          yield* Queue.take(requests)
          // Drain every request that coalesced while the last pass ran. `poll`
          // is non-blocking (unlike `takeAll`, which waits for another item).
          while (Option.isSome(yield* Queue.poll(requests))) {}
          yield* enforceOnce().pipe(
            Effect.catchCause((cause) =>
              Effect.sync(() => {
                void options.bridge?.fork(
                  Effect.logWarning(workerLogMessage("idle worker enforcement failed"), { reason: String(cause) }),
                )
              }),
            ),
          )
        }
      }),
      scope,
    ),
  )

  const requestEnforce = (): void => {
    Queue.offerUnsafe(requests, undefined)
  }

  const track = (worker: ExecutorWorker): ExecutorWorker => {
    const entry: Tracked = { worker, sessionID: undefined, terminated: false }
    const wrapped: ExecutorWorker = {
      get id() {
        return worker.id
      },
      get alive() {
        return !entry.terminated && worker.alive
      },
      execute: async (run) => {
        const started = mutate((current) => {
          if (entry.terminated) return [false, current] as const
          const active = new Set(current.active)
          active.add(worker.id)
          const idle = new Map(current.idle)
          idle.delete(worker.id)
          return [true, { ...current, active, idle }] as const
        })
        if (!started) throw new Error(`executor worker ${worker.id} was evicted while idle`)
        try {
          return await worker.execute(run)
        } finally {
          mutate((current) => {
            const active = new Set(current.active)
            active.delete(worker.id)
            if (entry.terminated) return [undefined, { ...current, active }] as const
            entry.sessionID = run.sessionID
            const idle = new Map(current.idle)
            idle.delete(worker.id)
            idle.set(worker.id, entry)
            return [undefined, { ...current, active, idle }] as const
          })
          requestEnforce()
        }
      },
      cancel: (sessionID) => worker.cancel(sessionID),
      assertNotBusy: (sessionID) => worker.assertNotBusy(sessionID),
      resultFor: (sessionID) => worker.resultFor(sessionID),
      terminate: async () => {
        mutate((current) => {
          if (entry.terminated) return [undefined, current] as const
          entry.terminated = true
          const tracked = new Map(current.tracked)
          tracked.delete(worker.id)
          const idle = new Map(current.idle)
          idle.delete(worker.id)
          const active = new Set(current.active)
          active.delete(worker.id)
          return [undefined, { ...current, tracked, idle, active }] as const
        })
        await worker.terminate()
      },
      ...(worker.hasActiveWork === undefined
        ? {}
        : { hasActiveWork: (sessionID: SessionID) => worker.hasActiveWork!(sessionID) }),
      ...(worker.cancelBoot === undefined ? {} : { cancelBoot: () => worker.cancelBoot!() }),
      ...(worker.cancelPort === undefined ? {} : { cancelPort: worker.cancelPort }),
      ...(worker.awaitEgress === undefined
        ? {}
        : { awaitEgress: (sessionID: SessionID) => worker.awaitEgress!(sessionID) }),
    }
    mutate((current) => {
      const tracked = new Map(current.tracked)
      tracked.set(worker.id, entry)
      return [undefined, { ...current, tracked }] as const
    })
    return wrapped
  }

  return {
    capacity,
    get idle() {
      return state.idle.size
    },
    get evictions() {
      return state.evictions
    },
    track,
    requestEnforce,
    forget: (workerID) => {
      mutate((current) => {
        const tracked = new Map(current.tracked)
        tracked.delete(workerID)
        const idle = new Map(current.idle)
        idle.delete(workerID)
        const active = new Set(current.active)
        active.delete(workerID)
        return [undefined, { ...current, tracked, idle, active }] as const
      })
    },
    clear: () => {
      mutate((current) => [undefined, { ...current, tracked: new Map(), idle: new Map(), active: new Set() }] as const)
      // Closing the scope interrupts and joins the one enforcement consumer.
      WorkerRuntime.runSync(Scope.close(scope, Exit.void))
    },
  }
}

/** A pool bounded at {@link resolveIdleWorkerCapacity} idle isolates by default. */
export interface IdleBoundedExecutorWorkerPool extends ExecutorWorkerPool {
  readonly idleWorkers: number
  /** Alias for {@link idleWorkers}, for callers that read the count directly. */
  readonly idleCount: number
  readonly idleCapacity: number
  readonly idleEvictions: number
}

/** Publish the idle-cap counters without changing the pool's worker surface. */
function withIdleIsolateCap(pool: ExecutorWorkerPool, cap: IdleIsolateCap): IdleBoundedExecutorWorkerPool {
  return {
    get workers() {
      return pool.workers
    },
    spawn: (id?: number) => pool.spawn(id),
    releaseSession: (sessionID, workerID) => {
      if (workerID !== undefined) cap.forget(workerID)
      pool.releaseSession?.(sessionID, workerID)
    },
    terminateAll: async () => {
      cap.clear()
      await pool.terminateAll()
    },
    byID: (id) => pool.byID(id),
    resultFor: (sessionID) => pool.resultFor(sessionID),
    get idleWorkers() {
      return cap.idle
    },
    get idleCount() {
      return cap.idle
    },
    get idleCapacity() {
      return cap.capacity
    },
    get idleEvictions() {
      return cap.evictions
    },
  }
}

// ---------------------------------------------------------------------------
// Production pool + hub factories (steps 10 + 13 + 14).
// ---------------------------------------------------------------------------

declare global {
  /**
   * Compile-time path to the bundled session-worker isolate, injected by
   * `script/build.ts` (mirrors `OPENCODE_WORKER_PATH`). Absent in dev/test.
   */
  const OPENCODE_SESSION_WORKER_PATH: string | undefined
}

/** The mechanism a resolved session-worker entry came from. */
export type SessionWorkerEntrySource = "env" | "definedPath" | "default"

/** A resolved session-worker entry and where it came from. */
export interface DescribedSessionWorkerEntry {
  readonly entry: string | URL
  readonly source: SessionWorkerEntrySource
}

/**
 * Resolve the production session-worker entry AND the mechanism it came from:
 * the runtime env override (`"env"`), the build-time define / an explicit path
 * (`"definedPath"`), or the source-relative isolate (`"default"`). Pure, so the
 * resolution (and its diagnostics) are testable without re-importing the module.
 */
export function describeSessionWorkerEntry(
  options: { readonly env?: Record<string, string | undefined>; readonly definedPath?: string } = {},
): DescribedSessionWorkerEntry {
  const defined =
    options.definedPath ??
    (typeof OPENCODE_SESSION_WORKER_PATH !== "undefined" ? OPENCODE_SESSION_WORKER_PATH : undefined)
  const fromEnv = (options.env ?? process.env).OPENCODE_SESSION_WORKER_PATH
  // The env override wins when set (even an empty string, which falls through to
  // the default — matching `resolveSessionWorkerEntry`'s original precedence).
  const resolved = fromEnv ?? defined
  if (resolved === undefined || resolved === "") {
    return { entry: new URL("./session-worker.ts", import.meta.url), source: "default" }
  }
  return { entry: resolved, source: fromEnv !== undefined ? "env" : "definedPath" }
}

/**
 * Resolve the production session-worker entry: the runtime env override, else
 * the build-time define, else the source-relative isolate.
 */
export function resolveSessionWorkerEntry(
  options: { readonly env?: Record<string, string | undefined>; readonly definedPath?: string } = {},
): string | URL {
  return describeSessionWorkerEntry(options).entry
}

/** The production session-worker entry (the reduced-layer isolate). */
export const SESSION_WORKER_ENTRY: string | URL = resolveSessionWorkerEntry()

/**
 * Default run-reply watchdog budget. The watchdog pings the worker between run
 * frames, so a healthy long turn survives however long it takes; only a wedged
 * event loop (no `pong` for this window) times out. A value of `0` disables it.
 */
export const DEFAULT_RUN_IDLE_TIMEOUT_MS = 60_000

/**
 * Resolve the run-reply watchdog budget, defaulting it ON. An explicit option
 * wins (including `0` to disable). Otherwise an environment override is honored:
 * `0` disables it explicitly, a positive integer sets the window, and any
 * malformed value (negative, fractional, non-numeric) falls back to
 * {@link DEFAULT_RUN_IDLE_TIMEOUT_MS} with a WARN from
 * {@link runIdleTimeoutWarning}. An absent/empty variable uses the default.
 */
export function resolveRunIdleTimeoutMs(options: {
  readonly runIdleTimeoutMs?: number
  readonly env?: Record<string, string | undefined>
}): number {
  if (options.runIdleTimeoutMs !== undefined) return options.runIdleTimeoutMs
  const parsed = parseRunIdleTimeoutEnv((options.env ?? process.env)[SESSION_WORKER_IDLE_TIMEOUT_ENV])
  return parsed.kind === "value" ? parsed.ms : parsed.kind === "disabled" ? 0 : DEFAULT_RUN_IDLE_TIMEOUT_MS
}

/**
 * Why the watchdog budget fell back to the default, or `undefined` when the knob
 * is absent, empty, `0`, or a valid value. Callers emit this at WARN at startup.
 */
export function runIdleTimeoutWarning(options: {
  readonly runIdleTimeoutMs?: number
  readonly env?: Record<string, string | undefined>
}): string | undefined {
  if (options.runIdleTimeoutMs !== undefined) return undefined
  const parsed = parseRunIdleTimeoutEnv((options.env ?? process.env)[SESSION_WORKER_IDLE_TIMEOUT_ENV])
  if (parsed.kind !== "invalid") return undefined
  return `invalid ${SESSION_WORKER_IDLE_TIMEOUT_ENV}: ${parsed.raw} (expected a positive integer or 0); using default ${DEFAULT_RUN_IDLE_TIMEOUT_MS} ms`
}

type ParsedRunIdleTimeout =
  | { readonly kind: "absent" }
  | { readonly kind: "disabled" }
  | { readonly kind: "value"; readonly ms: number }
  | { readonly kind: "invalid"; readonly raw: string }

function parseRunIdleTimeoutEnv(raw: string | undefined): ParsedRunIdleTimeout {
  if (raw === undefined || raw.trim() === "") return { kind: "absent" }
  if (raw.trim() === "0") return { kind: "disabled" }
  const parsed = Number(raw)
  if (!Number.isInteger(parsed) || parsed <= 0) return { kind: "invalid", raw }
  return { kind: "value", ms: parsed }
}

/** Emit the malformed-watchdog warning once per resolved pool/hub. */
export function logRunIdleTimeoutWarning(
  bridge: EffectBridge.Shape | undefined,
  options: {
    readonly runIdleTimeoutMs?: number
    readonly env?: Record<string, string | undefined>
  },
): void {
  const warning = runIdleTimeoutWarning(options)
  if (warning === undefined) return
  void bridge?.fork(Effect.logWarning(workerLogMessage("run idle timeout invalid"), { reason: warning }))
}

/**
 * A worker `workerData` payload, or a (possibly async) resolver for one. The
 * resolver receives the session being dispatched, so the production hub resolves
 * the session's own model rather than reusing the first session's.
 */
export type SessionWorkerInitSource =
  | SessionWorkerInit
  | ((sessionID?: SessionID, model?: WorkerModelSelection) => SessionWorkerInit | Promise<SessionWorkerInit>)

export interface ProductionWorkerPoolOptions {
  readonly init: SessionWorkerInitSource
  /** Worker entry; defaults to {@link SESSION_WORKER_ENTRY} (overridable for tests). */
  readonly entry?: string | URL
  readonly env?: Record<string, string | undefined>
  /** The SQLite file this pool's isolates use, for the live-worker/schema gate. */
  readonly databasePath?: string
  readonly timeoutMs?: number
  /** Run-reply liveness budget; absent uses the default-on budget, `0` disables it. */
  readonly runIdleTimeoutMs?: number
  readonly workKind?: string
  /** Per-session durable-egress barrier. */
  readonly egressBarrier?: (sessionID: SessionID) => Promise<void> | undefined
  readonly onWorkerPorts?: (workerID: number, ports: WorkerTransferredPorts) => void
  readonly onHubProxyPort?: (workerID: number, port: MessagePort) => void
  /**
   * Fired when a worker rebinds away from a session (its isolate is retired and
   * respawned for a different session). The production hub drops that session's
   * per-session instance/context so those maps cannot grow without bound.
   */
  readonly onSessionRebound?: (sessionID: SessionID) => void
  /**
   * Full retirement for a same-session generation change (a mid-session model
   * switch). The pool replaces the isolate in place, so the dispatcher's
   * `retireWorker` never sees it; this hook runs the hub's authoritative
   * retirement (lost in-flight calls, ports, pending permission/question
   * requests, and the retired generation's lease release) before the successor
   * boots.
   */
  readonly onGenerationRetired?: (sessionID: SessionID, workerID?: number) => void | Promise<void>
  /** Pool-side cleanup when a session's worker is retired (init cache eviction). */
  readonly onSessionReleased?: (sessionID: SessionID) => void
  /**
   * Cheap generation probe: the model identity a session will run, read without
   * building the full init. When absent, the init is resolved to learn the model
   * (one expensive read per dispatch); production supplies this so an unchanged
   * generation reuses the cached init.
   */
  readonly generationOf?: (sessionID: SessionID, model?: WorkerModelSelection) => string | Promise<string>
  /** One global spawn permit shared by every isolate this pool spawns/re-spawns. */
  readonly spawnGate?: SpawnGateHandle
  /** Boot-ready deadline; see {@link BunExecutorTransportOptions.bootTimeoutMs}. */
  readonly bootTimeoutMs?: number
  /** Optional re-entry bridge for the worker lifecycle lines. */
  readonly bridge?: EffectBridge.Shape
  /** Boot-cancellation telemetry: a cancel win or a death during abort. */
  readonly onBootTelemetry?: (event: BootTelemetry) => void
  /** Boot-stage phase reporting for the session this isolate is bound to. */
  readonly onPhase?: (sessionID: SessionID, phase: "boot" | "materialize") => void
  /** Concurrent-running cap; defaults to the CPU-core count. */
  readonly maxRunning?: number
  /** Idle isolates retained by the pool; defaults to {@link resolveIdleWorkerCapacity}. */
  readonly maxIdleWorkers?: number
  /**
   * Run the full retirement for an idle isolate the cap evicts. The production
   * hub wires this to the dispatcher: dropping only the pool's local session
   * state would leave the worker's hub ports, pending asks, and durable lease
   * live behind an exit. Awaited before the eviction terminates the isolate.
   */
  readonly onIdleEvicted?: (sessionID: SessionID, workerID: number) => void | Promise<void>
  /**
   * Awaited before each isolate spawn (the hub's off-loop schema gate). It must
   * run before `bun:sqlite` opens the worker's connection, including when a
   * caller reaches `dispatcher.dispatch` directly instead of `hub.ensureRunning`.
   */
  readonly beforeSpawn?: () => Promise<void>
}

/** One resolved worker generation: its identity key and the init that boots it. */
export interface WorkerGeneration {
  readonly generation: string
  /**
   * The init resolved for this generation. Present when the generation probe
   * resolved it; a probe that only derives a cheap key may omit it, in which
   * case the spawner resolves the init itself.
   */
  readonly init?: SessionWorkerInit
}

/**
 * Build one isolate for `sessionID` under a fixed worker id. The generation's
 * resolved init rides along so the spawn uses exactly the init the generation
 * probe keyed on (never a second read that could have moved).
 */
export type SessionBoundSpawner = (
  id: number,
  sessionID: SessionID,
  init?: SessionWorkerInit,
) => Promise<ExecutorWorker>

/** One init build shared by every concurrent caller for the same key. */
interface InitFlight {
  readonly deferred: Deferred.Deferred<SessionWorkerInit, unknown>
  readonly promise: Promise<SessionWorkerInit>
}

/**
 * The production pool's init bookkeeping. Every read and modification is
 * synchronous, so the scoped cache and the init cache observe one order without
 * an intermediate Effect synchronization layer.
 */
interface ProductionInitState {
  inFlight(key: string): InitFlight | undefined
  startInFlight(key: string, flight: InitFlight): void
  finishInFlight(key: string, flight: InitFlight): void
  clearInFlight(sessionID: SessionID): void
  cached(sessionID: SessionID): WorkerGeneration | undefined
  cache(sessionID: SessionID, generation: WorkerGeneration): void
  forget(sessionID: SessionID): void
}

/** The port-transfer sinks a production pool routes worker messages to. */
export interface WorkerPoolSinks {
  readonly onWorkerPorts: (workerID: number, ports: WorkerTransferredPorts) => void
  readonly onHubProxyPort: (workerID: number, port: MessagePort) => void
}

/** Builds the executor pool from a lazy init resolver and port sinks (test seam). */
export type ProductionPoolFactory = (
  resolveInit: (sessionID?: SessionID) => Promise<SessionWorkerInit>,
  sinks: WorkerPoolSinks,
  /**
   * The dispatch cap resolved from `maxRunningWorkers` (or the CPU default). A
   * factory that builds a lazy pool should use it so the test/production cap is
   * the one actually applied.
   */
  runtime: { readonly maxRunning: number },
) => ExecutorWorkerPool

/** Mark a generation as serving the worker egress ports. */
export function withSessionWorkerPorts(init: SessionWorkerInit): SessionWorkerInit {
  return { ...init, capabilities: { ...init.capabilities, sessionWorkerPorts: true } }
}

// ---------------------------------------------------------------------------
// WorkerPools service (Wave-6 additive adapter over the Promise pool).
// ---------------------------------------------------------------------------

/** The counters {@link Interface.stats} publishes without changing the pool. */
export interface WorkerPoolsStats {
  readonly workers: number
  readonly idleWorkers: number
  readonly idleCapacity: number
  readonly idleEvictions: number
}

/**
 * The Effect-facing pool surface. `executorPool` stays the structural Promise
 * pool the dispatcher and hub already consume; the scoped cache owns every
 * isolate and `terminateAll` is the one shutdown wave.
 */
export interface Interface {
  readonly executorPool: ExecutorWorkerPool
  readonly spawn: (id?: number) => Effect.Effect<ExecutorSessionWorker>
  readonly releaseSession: (sessionID: SessionID, workerID?: number) => Effect.Effect<void>
  readonly byID: (id: number) => Effect.Effect<ExecutorWorker | undefined>
  readonly resultFor: (sessionID: SessionID) => Effect.Effect<SessionExecutionResult | undefined>
  readonly workers: Effect.Effect<ReadonlyArray<ExecutorSessionWorker>>
  readonly terminateAll: () => Effect.Effect<void>
  readonly stats: Effect.Effect<WorkerPoolsStats>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/WorkerPools") {}

type ProductionInitMaps = {
  readonly inFlight: ReadonlyMap<string, InitFlight>
  readonly cache: ReadonlyMap<SessionID, WorkerGeneration>
}

/**
 * Effect-owned production init state. Reads and pure modifications stay
 * synchronous, so the adapter observes exactly the ordering the plain-map
 * factory has.
 */
function productionInitState(): ProductionInitState {
  let maps: ProductionInitMaps = { inFlight: new Map(), cache: new Map() }
  const update = (change: (maps: ProductionInitMaps) => ProductionInitMaps): void => {
    maps = change(maps)
  }
  const current = (): ProductionInitMaps => maps
  return {
    inFlight: (key) => current().inFlight.get(key),
    startInFlight: (key, flight) =>
      update((maps) => ({ ...maps, inFlight: new Map(maps.inFlight).set(key, flight) })),
    finishInFlight: (key, flight) =>
      update((maps) => {
        if (maps.inFlight.get(key) !== flight) return maps
        const inFlight = new Map(maps.inFlight)
        inFlight.delete(key)
        return { ...maps, inFlight }
      }),
    clearInFlight: (sessionID) =>
      update((maps) => {
        const inFlight = new Map(maps.inFlight)
        for (const key of inFlight.keys()) if (key.startsWith(`${sessionID}:`)) inFlight.delete(key)
        return { ...maps, inFlight }
      }),
    cached: (sessionID) => current().cache.get(sessionID),
    cache: (sessionID, generation) =>
      update((maps) => ({ ...maps, cache: new Map(maps.cache).set(sessionID, generation) })),
    forget: (sessionID) =>
      update((maps) => {
        const cache = new Map(maps.cache)
        cache.delete(sessionID)
        return { ...maps, cache }
      }),
  }
}

/** The cache key: one isolate per `(sessionID, generation)`. */
function generationKey(sessionID: SessionID, generation: string): string {
  return `${sessionID}\u0000${generation}`
}

interface GenerationLookup {
  readonly sessionID: SessionID
  readonly workerID: number
  readonly init?: SessionWorkerInit
}

const promiseEffect = <A>(run: () => Promise<A>): Effect.Effect<A> =>
  Effect.tryPromise({ try: run, catch: (error) => error }).pipe(Effect.orDie)

/**
 * Join an in-flight promise for at most `timeoutMs`, then detach. Effect's
 * interruptible clock bounds the wait; a rejected or wedged source is reported
 * as a settled join because the caller is tearing the resource down anyway.
 */
const joinWithin = (promise: Promise<unknown>, timeoutMs: number): Promise<void> =>
  WorkerRuntime.runPromise(
    Effect.tryPromise({ try: () => promise, catch: (error) => error }).pipe(
      Effect.timeoutOrElse({
        duration: Duration.millis(timeoutMs),
        orElse: () => Effect.void,
      }),
      Effect.asVoid,
    ),
  ).catch(() => undefined)

/**
 * Build the production {@link Interface}. The isolate is an
 * `Effect.acquireRelease` resource in a `ScopedCache` keyed by
 * `(sessionID, generation)`; acquiring boots the isolate and the release is the
 * ONLY isolate-terminate authority. Retirement (crash, cancel, reap, model
 * switch, idle eviction, shutdown) is `ScopedCache.invalidate`, so every path
 * runs the same orderly DB/WAL close exactly once.
 */
export function makeProduction(options: ProductionWorkerPoolOptions): Effect.Effect<Interface, never, Scope.Scope> {
  return Effect.gen(function* () {
    const bridge = options.bridge
    const runIdleTimeoutMs = resolveRunIdleTimeoutMs(options)
    logRunIdleTimeoutWarning(bridge, options)
    const state = productionInitState()
    const resolveSource: (
      sessionID?: SessionID,
      model?: WorkerModelSelection,
    ) => SessionWorkerInit | Promise<SessionWorkerInit> =
      typeof options.init === "function" ? options.init : () => options.init as SessionWorkerInit
    const modelKey = (model: WorkerModelSelection): string => `${model.providerID}/${model.modelID}`
    // In-flight dedup is keyed by the exact generation request, so two concurrent
    // first turns for one model share one build while a model switch never joins
    // the old build.
    const resolveInit = (sessionID: SessionID, model?: WorkerModelSelection): Promise<SessionWorkerInit> => {
      const key = `${sessionID}:${model === undefined ? "" : modelKey(model)}`
      const existing = state.inFlight(key)
      if (existing !== undefined) return existing.promise
      const deferred = Deferred.makeUnsafe<SessionWorkerInit, unknown>()
      const flight: InitFlight = { deferred, promise: WorkerRuntime.runPromise(Deferred.await(deferred)) }
      state.startInFlight(key, flight)
      Promise.resolve()
        .then(() => resolveSource(sessionID, model))
        .then(
          (init) => {
            Deferred.doneUnsafe(deferred, Effect.succeed(init))
            state.finishInFlight(key, flight)
          },
          (error) => {
            // Remove the failed flight so the next turn retries instead of
            // observing a poisoned cache entry.
            Deferred.doneUnsafe(deferred, Effect.fail(error))
            state.finishInFlight(key, flight)
          },
        )
      return flight.promise
    }
    // `(sessionID, generation)` is the init cache: a repeated turn on the same
    // generation never rebuilds the graph/plugin/model bootstrap.
    const generationOf = async (
      sessionID: SessionID,
      requested?: import("./execute").WorkerRunModel,
    ): Promise<WorkerGeneration> => {
      const cached = state.cached(sessionID)
      const requestedKey = requested === undefined ? undefined : modelKey(requested)
      // The requested model is the authoritative generation key: it is the model
      // this run will actually use, even before the session row catches up.
      if (requestedKey !== undefined) {
        if (cached?.generation === requestedKey && cached.init !== undefined) return cached
        const init = await resolveInit(sessionID, requested)
        const entry: WorkerGeneration = { generation: requestedKey, init }
        state.cache(sessionID, entry)
        return entry
      }
      // No requested model: use the cheap probe when supplied (production). It
      // returns the persisted/current model identity without rebuilding the init.
      if (options.generationOf !== undefined) {
        const generation = await options.generationOf(sessionID)
        if (cached?.generation === generation && cached.init !== undefined) return cached
        const init = await resolveInit(sessionID)
        const entry: WorkerGeneration = { generation, init }
        state.cache(sessionID, entry)
        return entry
      }
      // Last resort for a caller that supplies neither: resolve the init and key
      // on its resolved model (correct, but one build per dispatch).
      const init = await resolveInit(sessionID)
      const generation = modelKey(init.model)
      if (cached?.generation === generation) return cached
      const entry: WorkerGeneration = { generation, init }
      state.cache(sessionID, entry)
      return entry
    }
    const spawnFor: SessionBoundSpawner = async (id, sessionID, resolvedInit) => {
      const init = resolvedInit ?? (await resolveInit(sessionID))
      return spawnBunExecutorWorker(id, {
        entry: options.entry ?? SESSION_WORKER_ENTRY,
        // Ship the pool id so the worker's `pid-report` frames are keyed to this
        // slot out of the box (the hub still treats its port association as authoritative).
        // The pool only spawns session workers, so the role is pinned `session`.
        workerData: { ...init, workerID: id, role: "session", sessionID },
        ...defined({
          env: options.env,
          databasePath: options.databasePath,
          timeoutMs: options.timeoutMs,
          runIdleTimeoutMs,
          workKind: options.workKind,
          egressBarrier: options.egressBarrier,
          ...(bridge === undefined ? {} : { bridge }),
          // The session this isolate is bound to, so the `worker spawning`
          // line names it (the init was already resolved for the same session).
          sessionID,
          spawnGate: options.spawnGate,
          bootTimeoutMs: options.bootTimeoutMs,
          onBootTelemetry: options.onBootTelemetry,
          ...(options.onPhase === undefined
            ? {}
            : { onPhase: (phase: "boot" | "materialize") => options.onPhase?.(sessionID, phase) }),
          onWorkerPorts: options.onWorkerPorts,
          onHubProxyPort: options.onHubProxyPort,
        }),
      })
    }
    const cap = createWorkerCap(options.maxRunning ?? os.availableParallelism())
    // The scope that owns the isolate cache; closing it invalidates every entry
    // through the same release. Kept separate so `terminateAll` can close it
    // exactly once even while the outer layer scope lives on.
    const cacheScope = yield* Scope.make()
    const lookups = new Map<string, GenerationLookup>()
    const cache = yield* Scope.provide(cacheScope)(
      ScopedCache.make<string, ExecutorWorker>({
        // The idle cap, not the cache capacity, bounds retention.
        capacity: Number.MAX_SAFE_INTEGER,
        lookup: (key) =>
          Effect.acquireRelease(
            Effect.gen(function* () {
              const lookup = lookups.get(key)
              if (lookup === undefined) throw new Error(`session worker generation ${key} disappeared before boot`)
              // The schema gate must run before the isolate opens SQLite.
              yield* Effect.tryPromise({
                try: () => Promise.resolve(options.beforeSpawn?.()),
                catch: (error) => error,
              }).pipe(Effect.orDie)
              const init = lookup.init ?? (yield* promiseEffect(() => resolveInit(lookup.sessionID)))
              return yield* promiseEffect(() => spawnFor(lookup.workerID, lookup.sessionID, init))
            }),
            (worker) =>
              Effect.tryPromise({
                try: () => worker.terminate(),
                catch: (error) => error,
              }).pipe(
                Effect.catchCause((cause) =>
                  Effect.sync(() => {
                    void bridge?.fork(
                      Effect.logWarning(workerLogMessage("worker release failed"), {
                        workerID: worker.id,
                        reason: String(cause),
                      }),
                    )
                  }),
                ),
              ),
          ),
      }),
    )
    const invalidate = (key: string): Promise<void> => {
      lookups.delete(key)
      return WorkerRuntime.runPromise(ScopedCache.invalidate(cache, key)).catch(() => undefined)
    }
    const idleCap = createIdleIsolateCap({
      capacity: options.maxIdleWorkers ?? resolveIdleWorkerCapacity(),
      ...(bridge === undefined ? {} : { bridge }),
      // An eviction is a generation retirement, not a pool-local cache drop. When
      // the hub supplies the hook it owns the whole retirement (including the
      // dispatcher's registry entry and the retired lease); otherwise keep the
      // local release for a standalone pool.
      onEvict: (entry) =>
        options.onIdleEvicted === undefined
          ? basePool?.releaseSession?.(entry.sessionID, entry.worker.id)
          : options.onIdleEvicted(entry.sessionID, entry.worker.id),
    })
    const slots = new Map<number, ExecutorWorker>()
    let nextId = 1
    let basePool: ExecutorWorkerPool | undefined
    let terminationAll: Promise<void> | undefined

    const createSlot = (id: number): ExecutorWorker => {
      let boundSession: SessionID | undefined
      let boundKey: string | undefined
      let current: ExecutorWorker | undefined
      let terminated = false
      // A slot whose isolate died is observably dead even after the bound key is
      // invalidated and `current` cleared. Dispatch classifies a raw transport
      // exit as a crash from this flag; a fresh isolate clears it.
      let isolateDead = false
      let termination: Promise<void> | undefined
      let materializing: Promise<void> | undefined

      const invalidateBound = async (deadIsolate = false): Promise<void> => {
        if (deadIsolate) isolateDead = true
        const key = boundKey
        boundKey = undefined
        boundSession = undefined
        current = undefined
        if (key !== undefined) await invalidate(key)
      }

      const worker: ExecutorWorker = {
        id,
        get alive() {
          return !terminated && !isolateDead && (current === undefined || current.alive)
        },
        execute: async (run) => {
          const release = await cap.acquire(run.depth)
          try {
            if (terminated) throw new Error(`executor worker ${id} is terminated`)
            const materialized = (async () => {
              const generation = await generationOf(run.sessionID, run.model)
              if (terminated) throw new Error(`executor worker ${id} is terminated`)
              const key = generationKey(run.sessionID, generation.generation)
              if (boundKey !== undefined && boundKey !== key) {
                const previousSession = boundSession
                await invalidateBound()
                try {
                  if (previousSession !== undefined && previousSession !== run.sessionID) {
                    await options.onSessionRebound?.(previousSession)
                  } else if (previousSession !== undefined) {
                    await options.onGenerationRetired?.(previousSession, id)
                  }
                } catch (error) {
                  // Cleanup must never fail the new run; the successor still boots.
                  void bridge?.fork(
                    Effect.logWarning(workerLogMessage("worker retirement callback failed"), {
                      workerID: id,
                      sessionID: previousSession,
                      error: error instanceof Error ? error.message : String(error),
                    }),
                  )
                }
              }
              boundSession = run.sessionID
              boundKey = key
              lookups.set(key, { sessionID: run.sessionID, workerID: id, init: generation.init })
              if (current === undefined) {
                try {
                  current = await WorkerRuntime.runPromise(ScopedCache.get(cache, key))
                } catch (error) {
                  // A failed lookup is cached by the cache's TTL; drop it so the
                  // next turn retries the init/spawn instead of replaying it.
                  await invalidate(key)
                  throw error
                }
              }
              const isolate = current
              if (isolate === undefined) throw new Error(`executor worker ${id} did not materialize`)
              isolateDead = false
              return { key, worker: isolate }
            })()
            materializing = materialized.then(
              () => undefined,
              () => undefined,
            )
            const { key, worker: isolate } = await materialized
            if (terminated) {
              await invalidate(key)
              throw new Error(`executor worker ${id} is terminated`)
            }
            try {
              return await isolate.execute(run)
            } catch (error) {
              // A first-run boot failure surfaces here, not in `ScopedCache.get`
              // (the transport constructs on the first post). A dead isolate must
              // stay observably dead (so dispatch classifies the crash) AND have
              // its key dropped so the release runs and the next turn boots a
              // fresh isolate instead of replaying the dead cached entry.
              if (!isolate.alive && boundKey === key) await invalidateBound(true)
              throw error
            }
          } finally {
            release?.()
          }
        },
        cancel: (sessionID) => (current === undefined ? Promise.resolve() : current.cancel(sessionID)),
        cancelBoot: () => current?.cancelBoot?.() ?? Promise.resolve(undefined),
        assertNotBusy: (sessionID) =>
          current === undefined ? Promise.resolve(false) : current.assertNotBusy(sessionID),
        resultFor: (sessionID) => current?.resultFor(sessionID),
        hasActiveWork: (sessionID) => current?.hasActiveWork?.(sessionID) ?? Promise.resolve(false),
        awaitEgress: (sessionID) => current?.awaitEgress?.(sessionID) ?? Promise.resolve(),
        cancelPort: createExecutorCancelPort({ cancel: (sessionID) => worker.cancel(sessionID), ...(bridge === undefined ? {} : { bridge }) }),
        terminate: () => {
          if (termination !== undefined) return termination
          terminated = true
          const pending = materializing
          termination = (async () => {
            // A terminate during the first materialization joins that same
            // close: the fresh isolate is discarded, never published. A wedged
            // boot/init resolution is bounded so shutdown cannot park on it.
            if (pending !== undefined) await joinWithin(pending, MATERIALIZE_JOIN_TIMEOUT_MS)
            await invalidateBound()
          })()
          return termination
        },
      }
      return worker
    }

    const spawn = (id?: number): ExecutorSessionWorker => {
      // Honour a dispatcher-supplied id (the registry owns the monotonic source
      // there); a direct caller keeps the pool's own counter.
      const workerID = id === undefined || slots.has(id) ? nextId : id
      nextId = Math.max(nextId, workerID + 1)
      const slot = idleCap.track(createSlot(workerID))
      slots.set(workerID, slot)
      return slot
    }

    const terminateAll = (): Promise<void> => {
      if (terminationAll !== undefined) return terminationAll
      const workers = [...slots.values()]
      slots.clear()
      terminationAll = (async () => {
        await Promise.all(workers.map((worker) => worker.terminate().catch(() => undefined)))
        // Every slot join above is bounded; the cache scope close is bounded too
        // so a lingering lookup cannot park shutdown after its deadline.
        await WorkerRuntime.runPromise(
          Scope.close(cacheScope, Exit.void).pipe(
            Effect.timeoutOrElse({
              duration: Duration.millis(MATERIALIZE_JOIN_TIMEOUT_MS),
              orElse: () => Effect.void,
            }),
          ),
        ).catch(() => undefined)
      })()
      return terminationAll
    }

    const poolBase: ExecutorWorkerPool = {
      get workers() {
        return [...slots.values()]
      },
      spawn,
      releaseSession: (sessionID, workerID) => {
        if (workerID !== undefined) slots.delete(workerID)
        state.clearInFlight(sessionID)
        state.forget(sessionID)
        options.onSessionReleased?.(sessionID)
      },
      terminateAll,
      byID: (id) => slots.get(id),
      resultFor: (sessionID) => {
        for (const worker of slots.values()) {
          const result = worker.resultFor(sessionID)
          if (result !== undefined) return result
        }
        return undefined
      },
    }
    basePool = poolBase
    const pool = withIdleIsolateCap(poolBase, idleCap)

    const stats = (): WorkerPoolsStats => ({
      workers: pool.workers.length,
      idleWorkers: pool.idleWorkers,
      idleCapacity: pool.idleCapacity,
      idleEvictions: pool.idleEvictions,
    })
    const pools: Interface = {
      executorPool: pool,
      spawn: (id) => Effect.sync(() => pool.spawn(id)),
      releaseSession: (sessionID, workerID) =>
        Effect.sync(() => {
          pool.releaseSession?.(sessionID, workerID)
        }),
      byID: (id) => Effect.sync(() => pool.byID(id)),
      resultFor: (sessionID) => Effect.sync(() => pool.resultFor(sessionID)),
      workers: Effect.sync(() => pool.workers),
      terminateAll: () => Effect.promise(() => terminateAll()),
      stats: Effect.sync(stats),
    }
    yield* Effect.addFinalizer(() => Effect.promise(() => terminateAll()))
    return pools
  })
}

/** Install a production `WorkerPools` implementation into the context. */
export function layer(options: ProductionWorkerPoolOptions): Layer.Layer<Service> {
  return Layer.effect(Service, makeProduction(options))
}

/** The unbound graph node for {@link Service}; wave 6 does not wire a consumer. */
export function node(options: ProductionWorkerPoolOptions): LayerNode.Node<Service, never, undefined> {
  return LayerNode.make({ service: Service, layer: layer(options), deps: [] })
}

export * as WorkerPools from "./worker-pools"
