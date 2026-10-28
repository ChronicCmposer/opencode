// Executor transport and lazy/session-bound worker-pool factories.
import os from "node:os"
import path from "node:path"
import type { SessionID } from "@/session/schema"
import { defined } from "@/util/object"
import { createExecutorCancelPort, createExecutorWorker, createExecutorWorkerPool, SessionWorkerReplyTimeoutError, type ExecutorTransport, type ExecutorWorker, type ExecutorWorkerPool, type SessionExecutorInbound, type SessionExecutorOutbound, type WorkerSpawnReason } from "./execute"
import { BOOT_CANCELLED, createBootLifecycle, type BootCancelResult, type BootLifecycle, type BootTelemetry } from "./cancel"
import { type SpawnGateHandle, type SpawnPermit } from "./spawn-gate"
import { SESSION_WORKER_IDLE_TIMEOUT_ENV } from "./pool"
import { mergeEnv } from "./init"
import { BuildMismatch, verifyReadyBuild } from "./build-hash"
import type { SessionWorkerInit } from "./session-worker"
import { noopWorkerLogger, workerLogMessage, type WorkerLogger } from "./log"
import { sessionThreadName } from "@/session/worker/thread-name"

import type { WorkerTransferredPorts } from "./worker-ports"
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
  /** Optional structured logger seam; absent defaults to a no-op (flag-off behavior). */
  readonly log?: WorkerLogger
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
 * the hub terminates it anyway. Long enough for a `SQLite.close()` + lease
 * release; short enough that a wedged worker cannot stall retirement.
 */
const SHUTDOWN_ACK_TIMEOUT_MS = 1_000

/**
 * Ask the isolate to close its native WAL handle and release its bus lease
 * before the hub calls `Worker.terminate()`. Terminating without this leaves the
 * bun:sqlite handle (and its WAL lock) alive until process exit. Bounded: a
 * wedged or half-booted worker is still terminated after the timeout.
 */
async function requestWorkerShutdown(worker: Worker): Promise<void> {
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
  await new Promise<void>((resolve) => {
    let settled = false
    let timer: ReturnType<typeof setTimeout>
    const finish = (): void => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      worker.removeEventListener?.("message", onMessage)
      resolve()
    }
    const onMessage = (event: MessageEvent): void => {
      const frame = event.data as { readonly type?: unknown } | undefined
      if (frame?.type === "shutdown-ack") finish()
    }
    timer = setTimeout(finish, SHUTDOWN_ACK_TIMEOUT_MS)
    timer.unref?.()
    worker.addEventListener?.("message", onMessage)
    try {
      worker.postMessage({ type: "shutdown", id: 0 })
    } catch {
      finish()
    }
  })
}

/** A validated boot/port frame; `invalid` fails the spawn loud. */
type BootFrame =
  | { readonly kind: "ready" }
  | { readonly kind: "worker-ports"; readonly ports: WorkerTransferredPorts }
  | { readonly kind: "hub-proxy"; readonly port: MessagePort }
  /**
   * `error` preserves a typed failure (e.g. `BuildMismatch`) for the caller.
   * `booted` marks a `ready` frame that the worker actually emitted: the isolate
   * completed its DB/WAL boot, so its teardown must be orderly even though the
   * hub rejected it. A malformed frame before `ready` is not booted.
   */
  | { readonly kind: "invalid"; readonly reason: string; readonly error?: Error; readonly booted?: boolean }

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
      return { kind: "invalid", reason: "boot-ready frame without a numeric threadId", booted: true }
    }
    try {
      verifyReadyBuild(frame)
    } catch (error) {
      if (error instanceof BuildMismatch) return { kind: "invalid", reason: error.message, error, booted: true }
      return { kind: "invalid", reason: "boot-ready frame failed build-hash verification", booted: true }
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
  /** Absent when the wait has no deadline (`timeoutMs <= 0`). */
  readonly timer: ReturnType<typeof setTimeout> | undefined
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

/** How many session-worker isolates live on `databasePath` (all files when omitted). */
export function liveSessionWorkerCount(databasePath?: string): number {
  if (databasePath === undefined) return liveExecutorWorkers.size
  const target = path.resolve(databasePath)
  let count = 0
  for (const workerPath of liveExecutorWorkers.values()) {
    if (workerPath !== undefined && path.resolve(workerPath) === target) count += 1
  }
  return count
}

/**
 * A `MessagePort`-free `ExecutorTransport` over a real Bun worker: buffers every
 * outbound frame so a `waitFor` may match one that already arrived, and fails
 * loud on timeout or isolate error.
 */
export function createBunExecutorTransport(options: BunExecutorTransportOptions): ExecutorTransport {
  const log = options.log ?? noopWorkerLogger
  const timeoutMs = options.timeoutMs ?? DEFAULT_TRANSPORT_TIMEOUT_MS
  const bufferCapacity = options.bufferCapacity ?? DEFAULT_TRANSPORT_BUFFER_CAPACITY
  const gate = options.spawnGate
  const bootTimeoutMs = options.bootTimeoutMs ?? timeoutMs
  const messages: SessionExecutorOutbound[] = []
  const waiters = new Set<PendingWaiter>()
  let alive = true
  let worker: Worker | undefined
  let booting: Promise<void> | undefined
  let bootTimer: ReturnType<typeof setTimeout> | undefined
  let bootLifecycle: BootLifecycle<Worker> | undefined
  // A boot that failed on its own (protocol error, deadline, isolate death) never
  // completed the handshake, so its teardown is terminate-only: a wedged isolate
  // cannot ack the orderly-shutdown frame. An explicit boot cancel stays orderly.
  let bootFailed = false
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
      if (waiter.timer !== undefined) clearTimeout(waiter.timer)
      waiters.delete(waiter)
      waiter.reject(error)
    }
  }

  const constructWorker = (): Worker => {
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
    if (bootTimer !== undefined) clearTimeout(bootTimer)
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
   * Tear down the isolate that owns the active boot interval. A booting/ready
   * lifecycle terminates the partial isolate itself (releasing the permit once);
   * a started worker is terminated directly.
   */
  const teardownBooted = (created: Worker): void => {
    liveExecutorWorkers.delete(created)
    if (bootLifecycle !== undefined && (bootLifecycle.state === "booting" || bootLifecycle.state === "ready")) {
      void bootLifecycle.cancel().catch(() => undefined)
      return
    }
    void created.terminate()
  }

  const connect = (created: Worker): void => {
    /**
     * Fail one spawn loud on a malformed boot/port frame, then tear the isolate
     * down. A typed cause (a `BuildMismatch` from a stale bundle) is preserved so
     * the dispatcher can refuse to retry it instead of crash-looping.
     * `booted` distinguishes a rejected `ready` (the isolate opened its DB/WAL,
     * so it needs the orderly close) from a frame before ready (never usable;
     * terminate-only).
     */
    const failProtocol = (reason: string, cause?: Error, booted = false): void => {
      const error = cause ?? new Error(`session worker protocol error: ${reason}`)
      bootFailed = !booted
      alive = false
      created.onmessage = null
      created.onerror = null
      log(workerLogMessage("worker boot failed"), {
        level: "ERROR",
        workerID: options.workerID,
        message: error.message,
      })
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
        failProtocol(bootFrame.reason, bootFrame.error, bootFrame.booted === true)
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
        if (waiter.timer !== undefined) clearTimeout(waiter.timer)
        waiters.delete(waiter)
        waiter.resolve(message)
        return
      }
      messages.push(message)
      if (messages.length > bufferCapacity) messages.shift()
    }

    created.onerror = (event: ErrorEvent) => {
      bootFailed = true
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
      log(workerLogMessage("worker boot failed"), {
        level: "ERROR",
        workerID: options.workerID,
        message,
      })
      failWaiters(new Error(message, cause === undefined ? undefined : { cause }))
      teardownBooted(created)
    }

    // A worker that exits on its own (a hard crash, `process.exit`, an OOM kill)
    // reports `close`, not `error`. Without this a dead isolate would only be
    // noticed when the run reply times out, losing the crash phase truth.
    created.addEventListener?.("close", (event: Event) => {
      if (!alive) return
      bootFailed = true
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
        bootTimer = setTimeout(() => {
          bootTimer = undefined
          const error = new Error(`session worker boot timeout after ${bootTimeoutMs}ms`)
          bootFailed = true
          alive = false
          queued.length = 0
          failWaiters(error)
          void lifecycle.cancel().catch(() => undefined)
        }, bootTimeoutMs)
        bootTimer.unref?.()
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
        // A failed boot has no acknowledged generation to drain, and the isolate
        // that missed its deadline or sent a malformed frame cannot answer the
        // shutdown frame; waiting the full ack budget would stall the released
        // permit for a worker that never became usable. Terminate it directly.
        if (bootFailed) {
          await partial.terminate()
          return
        }
        // A cancelled boot is still an orderly close: let the isolate release its
        // native WAL handle/lease through the shutdown frame before the thread is
        // destroyed. Terminate-only would strand the lock; `requestWorkerShutdown`
        // is the shared close primitive (owned by the cap/close section).
        await requestWorkerShutdown(partial)
        await partial.terminate()
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
    booting = (async () => {
      const acquired = await gate.acquire()
      // The cancel already resolved to its caller; release the permit the
      // acquisition granted and never construct the isolate. A terminate that
      // landed while waiting takes the same path without a cancel outcome.
      if (!alive || waitingCancelled) {
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
      startBoot(acquired)
    })().catch((error) => {
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
        const timer =
          waitMs > 0
            ? setTimeout(() => {
                waiters.delete(waiter)
                reject(new SessionWorkerReplyTimeoutError(waitMs))
              }, waitMs)
            : undefined
        const waiter: PendingWaiter = { predicate, resolve, reject, timer }
        waiters.add(waiter)
        // A caller that abandons the wait (the liveness probe on a resolved run)
        // aborts, so the waiter does not linger until its own deadline.
        waitOptions?.signal?.addEventListener(
          "abort",
          () => {
            if (!waiters.delete(waiter)) return
            if (timer !== undefined) clearTimeout(timer)
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
    async terminate(code = 0): Promise<number> {
      if (!alive) return code
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
      await requestWorkerShutdown(current)
      const result = await current.terminate()
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
  /** Optional structured logger seam; absent defaults to a no-op (flag-off behavior). */
  readonly log?: WorkerLogger
  /** Boot-cancellation telemetry: a cancel win or a death during abort. */
  readonly onBootTelemetry?: (event: BootTelemetry) => void
  /** Boot-stage phase reporting for the isolate. */
  readonly onPhase?: (phase: "boot" | "materialize") => void
  /** One global spawn permit; forwarded to the transport so cold spawns serialize. */
  readonly spawnGate?: SpawnGateHandle
  /** Boot-ready deadline; see {@link BunExecutorTransportOptions.bootTimeoutMs}. */
  readonly bootTimeoutMs?: number
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
  logRunIdleTimeoutWarning(options.log ?? noopWorkerLogger, options)
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
      log: options.log,
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
      // Forward the resolved logger so the `worker spawning` line fires in
      // production, where this helper (not a test's direct `createExecutorWorker`)
      // builds the executor.
      log: options.log,
    }),
  })
}

/** Production `ExecutorWorkerPool`: spawns frame-protocol workers on demand. */
export function createBunExecutorWorkerPool(options: BunExecutorWorkerPoolOptions): ExecutorWorkerPool {
  // Resolve the default-on watchdog HERE: this factory is the documented entry
  // point, so an absent option must still arm the budget (an explicit `0`
  // disables it) rather than silently reaching the executor as `undefined`.
  const runIdleTimeoutMs = resolveRunIdleTimeoutMs(options)
  if (options.log !== undefined) logRunIdleTimeoutWarning(options.log, options)
  return createExecutorWorkerPool({
    spawn: (id) => spawnBunExecutorWorker(id, { ...options, runIdleTimeoutMs }),
    ...(options.log === undefined ? {} : { log: options.log }),
  })
}

// ---------------------------------------------------------------------------
// Lazy pool: no isolate is spawned until a worker is first used.
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

export function createWorkerCap(max: number): WorkerCap {
  if (!Number.isInteger(max) || max < 1) {
    throw new Error(`worker cap must be a positive integer, received ${String(max)}`)
  }
  // Additive, transient headroom per nesting depth: bounded so child fan-out
  // cannot oversubscribe the machine, but never part of the foreground `max`.
  const headroom = Math.max(WORKER_CAP_MIN_HEADROOM, Math.ceil(max / 2))
  let foreground = 0
  const foregroundWaiters: Array<() => void> = []
  interface Headroom {
    running: number
    readonly waiters: Array<() => void>
  }
  const depths = new Map<number, Headroom>()
  const headroomAt = (depth: number): Headroom => {
    const existing = depths.get(depth)
    if (existing !== undefined) return existing
    const created: Headroom = { running: 0, waiters: [] }
    depths.set(depth, created)
    return created
  }

  /** A release frees exactly one slot, and only its own budget (idempotent). */
  const releaseOf = (budget: "foreground" | Headroom): (() => void) => {
    let released = false
    return () => {
      if (released) return
      released = true
      if (budget === "foreground") {
        foreground -= 1
        foregroundWaiters.shift()?.()
        return
      }
      budget.running -= 1
      budget.waiters.shift()?.()
    }
  }

  const acquire = (depth = 0): Promise<() => void> => {
    // A malformed depth is a foreground run, matching the executor frame parse.
    const level = Number.isInteger(depth) && depth > 0 ? depth : 0
    return new Promise<() => void>((resolve) => {
      if (level === 0) {
        if (foreground < max) {
          foreground += 1
          resolve(releaseOf("foreground"))
          return
        }
        foregroundWaiters.push(() => {
          foreground += 1
          resolve(releaseOf("foreground"))
        })
        return
      }
      const budget = headroomAt(level)
      if (budget.running < headroom) {
        budget.running += 1
        resolve(releaseOf(budget))
        return
      }
      budget.waiters.push(() => {
        budget.running += 1
        resolve(releaseOf(budget))
      })
    })
  }

  return {
    get running() {
      let total = foreground
      for (const budget of depths.values()) total += budget.running
      return total
    },
    get queued() {
      let total = foregroundWaiters.length
      for (const budget of depths.values()) total += budget.waiters.length
      return total
    },
    acquire,
  }
}

/**
 * The mutable materialization state shared by every worker whose isolate is
 * created on first use. The `materialize` policy owns the *why* (a lazy factory
 * ignores the session; a session-bound factory rebinds); this helper owns the
 * worker surface and the terminate race.
 */
interface MaterializingWorkerState {
  /** The live isolate, or `undefined` before the first materialization. */
  inner: ExecutorWorker | undefined
  /** Set once `terminate` runs; a late materialization must never publish. */
  terminated: boolean
  /** The in-flight materialization, so concurrent executes share one isolate. */
  materializing: Promise<ExecutorWorker> | undefined
}

/**
 * Build the `ExecutorWorker` surface around a `materialize` policy, which
 * resolves the isolate for a run's session and writes it back to `state.inner`.
 * The helper enforces the terminate race: a terminate during materialization
 * discards the fresh isolate rather than publishing a live worker. Before the
 * first materialization, `cancel`/`assertNotBusy`/`resultFor` are no-ops, which
 * is correct because no session can run on a worker that was never started.
 */
function createMaterializingExecutorWorker(
  id: number,
  materialize: (sessionID: SessionID, state: MaterializingWorkerState) => Promise<ExecutorWorker>,
  log: WorkerLogger = noopWorkerLogger,
  cap?: WorkerCap,
): ExecutorWorker {
  const state: MaterializingWorkerState = { inner: undefined, terminated: false, materializing: undefined }
  const worker: ExecutorWorker = {
    id,
    get alive() {
      return !state.terminated && (state.inner === undefined || state.inner.alive)
    },
    execute: async (run) => {
      // Cap the number of concurrently *running* foreground workers; extra
      // sessions queue here in FIFO order instead of oversubscribing the CPU.
      // A depth >= 1 child takes headroom at its own depth, so a parent that
      // awaits a foreground `task` child never deadlocks on the parent's slot.
      const release = await cap?.acquire(run.depth)
      try {
        return await (await materialize(run.sessionID, state)).execute(run)
      } finally {
        release?.()
      }
    },
    cancel: (sessionID) => (state.inner === undefined ? Promise.resolve() : state.inner.cancel(sessionID)),
    cancelBoot: () => state.inner?.cancelBoot?.() ?? Promise.resolve(undefined),
    assertNotBusy: (sessionID) =>
      state.inner === undefined ? Promise.resolve(false) : state.inner.assertNotBusy(sessionID),
    resultFor: (sessionID) => state.inner?.resultFor(sessionID),
    cancelPort: createExecutorCancelPort({ cancel: (sessionID) => worker.cancel(sessionID), log }),
    terminate: async () => {
      state.terminated = true
      state.materializing = undefined
      const current = state.inner
      state.inner = undefined
      if (current !== undefined) await current.terminate()
    },
  }
  return worker
}

/**
 * Wrap a worker factory so the underlying isolate is created on first use. The
 * dispatcher reads `alive`/builds slots at construction, so a lazy worker reports
 * itself alive (and free) before it exists; the isolate appears on the first
 * `execute`. The factory may be async (the production init is resolved on first
 * dispatch).
 */
export function createLazyExecutorWorker(
  id: number,
  create: LazyExecutorWorkerFactory,
  log: WorkerLogger = noopWorkerLogger,
  cap?: WorkerCap,
): ExecutorWorker {
  return createMaterializingExecutorWorker(
    id,
    async (_sessionID, state) => {
      if (state.terminated) throw new Error(`executor worker ${id} is terminated`)
      if (state.inner !== undefined) return state.inner
      state.materializing ??= Promise.resolve()
        .then(create)
        .then((worker) => {
          // A terminate raced the materialisation: never publish a live worker.
          if (state.terminated) {
            void worker.terminate()
            throw new Error(`executor worker ${id} is terminated`)
          }
          state.inner = worker
          return worker
        })
      return state.materializing
    },
    log,
    cap,
  )
}

/** Build one worker on first use; may be async (the production init is resolved on dispatch). */
export type LazyExecutorWorkerFactory = () => ExecutorWorker | Promise<ExecutorWorker>

export interface LazyExecutorWorkerPoolOptions {
  /** Build one worker on first use. */
  readonly create: (id: number) => ExecutorWorker | Promise<ExecutorWorker>
  /** Optional structured logger seam; absent defaults to a no-op (flag-off behavior). */
  readonly log?: WorkerLogger
  /** Concurrent-running cap; defaults to the CPU-core count. */
  readonly maxRunning?: number
}

/** A pool whose workers spawn on first use (per session). */
export function createLazyExecutorWorkerPool(options: LazyExecutorWorkerPoolOptions): ExecutorWorkerPool {
  const cap = createWorkerCap(options.maxRunning ?? os.availableParallelism())
  return createExecutorWorkerPool({
    spawn: (id) => createLazyExecutorWorker(id, () => options.create(id), options.log, cap),
  })
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
  log: WorkerLogger,
  options: {
    readonly runIdleTimeoutMs?: number
    readonly env?: Record<string, string | undefined>
  },
): void {
  const warning = runIdleTimeoutWarning(options)
  if (warning === undefined) return
  log(workerLogMessage("run idle timeout invalid"), { level: "WARN", reason: warning })
}

/**
 * A worker `workerData` payload, or a (possibly async) resolver for one. The
 * resolver receives the session being dispatched, so the production hub resolves
 * the session's own model rather than reusing the first session's.
 */
export type SessionWorkerInitSource =
  | SessionWorkerInit
  | ((sessionID?: SessionID) => SessionWorkerInit | Promise<SessionWorkerInit>)

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
  /** Pool-side cleanup when a session's worker is retired (init cache eviction). */
  readonly onSessionReleased?: (sessionID: SessionID) => void
  /** One global spawn permit shared by every isolate this pool spawns/re-spawns. */
  readonly spawnGate?: SpawnGateHandle
  /** Boot-ready deadline; see {@link BunExecutorTransportOptions.bootTimeoutMs}. */
  readonly bootTimeoutMs?: number
  /** Optional structured logger seam; absent defaults to a no-op (flag-off behavior). */
  readonly log?: WorkerLogger
  /** Boot-cancellation telemetry: a cancel win or a death during abort. */
  readonly onBootTelemetry?: (event: BootTelemetry) => void
  /** Boot-stage phase reporting for the session this isolate is bound to. */
  readonly onPhase?: (sessionID: SessionID, phase: "boot" | "materialize") => void
  /** Concurrent-running cap; defaults to the CPU-core count. */
  readonly maxRunning?: number
  /**
   * Awaited before each isolate spawn (the hub's off-loop schema gate). It must
   * run before `bun:sqlite` opens the worker's connection, including when a
   * caller reaches `dispatcher.dispatch` directly instead of `hub.ensureRunning`.
   */
  readonly beforeSpawn?: () => Promise<void>
}

/** Build one isolate for `sessionID` under a fixed worker id. */
export type SessionBoundSpawner = (id: number, sessionID: SessionID) => Promise<ExecutorWorker>

/**
 * A worker whose isolate is bound to ONE dispatched session. A worker isolate
 * owns exactly one serialized model (`reducedModelProviderLayer` rejects any
 * other with `WorkerModelMismatchError`), so when the dispatcher hands this
 * worker a *different* session it terminates the old isolate and respawns under
 * the same id with that session's init. Reuse across turns of the same session
 * (the common case) keeps the isolate.
 */
export function createSessionBoundExecutorWorker(
  id: number,
  spawnFor: SessionBoundSpawner,
  log: WorkerLogger = noopWorkerLogger,
  onRetire?: (sessionID: SessionID) => void,
  cap?: WorkerCap,
  /**
   * The generation key for a dispatch. A changed key (the session's persisted
   * model changed) retires the isolate even though the session id is the same,
   * so a legitimate mid-session model switch rebuilds the worker instead of
   * surfacing `WorkerModelMismatchError` on the next turn.
   */
  generationOf?: (sessionID: SessionID) => Promise<string>,
): ExecutorWorker {
  let boundSession: SessionID | undefined
  let boundGeneration: string | undefined
  return createMaterializingExecutorWorker(
    id,
    async (sessionID, state) => {
      if (state.terminated) throw new Error(`executor worker ${id} is terminated`)
      const generation = generationOf === undefined ? sessionID : await generationOf(sessionID)
      if (state.inner !== undefined && boundSession === sessionID && boundGeneration === generation) {
        return state.inner
      }
      const previousSession = boundSession
      const previous = state.inner
      state.inner = undefined
      boundSession = sessionID
      boundGeneration = generation
      // A worker rebinding from one session to another is the observable signal
      // that the isolate is being retired and rebuilt for a different model.
      if (previousSession !== undefined && previousSession !== sessionID) {
        // The old session's isolate is being retired; let the hub release its
        // per-session state before the new session's init is resolved.
        onRetire?.(previousSession)
        log(workerLogMessage("worker rebound"), {
          level: "INFO",
          workerID: id,
          fromSession: previousSession,
          toSession: sessionID,
          // The rebound-to session is always defined here (the branch guards
          // `previousSession !== undefined`), so the log seam hoists it into the
          // message prefix. `fromSession`/`toSession` stay for backward compat.
          sessionID,
        })
      }
      state.materializing = Promise.resolve()
        // Retire the old isolate first so two models never share one worker.
        .then(() => previous?.terminate().catch(() => undefined))
        .then(() => spawnFor(id, sessionID))
        .then((worker) => {
          if (state.terminated) {
            void worker.terminate()
            throw new Error(`executor worker ${id} is terminated`)
          }
          state.inner = worker
          return worker
        })
      return state.materializing
    },
    log,
    cap,
  )
}

/**
 * Production `ExecutorWorkerPool` over the session-worker entry. Lazy: building
 * the pool spawns no isolates. Each worker resolves its init for the session it
 * is asked to run (and rebinds when that session changes), so multiple sessions
 * with distinct models share a generation without a `WorkerModelMismatchError`.
 */
export function createProductionWorkerPool(options: ProductionWorkerPoolOptions): ExecutorWorkerPool {
  const log = options.log ?? noopWorkerLogger
  // Default the liveness watchdog ON unless the caller or env explicitly opts out.
  const runIdleTimeoutMs = resolveRunIdleTimeoutMs(options)
  logRunIdleTimeoutWarning(log, options)
  const resolveSource: (sessionID?: SessionID) => SessionWorkerInit | Promise<SessionWorkerInit> =
    typeof options.init === "function" ? options.init : () => options.init as SessionWorkerInit
  // Re-resolve the init on every dispatch (in-flight deduped): the persisted
  // model may change mid-session, and a stale boot model would raise
  // `WorkerModelMismatchError` on the next turn.
  const initInFlight = new Map<string, Promise<SessionWorkerInit>>()
  const resolveInit = (sessionID: SessionID): Promise<SessionWorkerInit> => {
    const existing = initInFlight.get(sessionID)
    if (existing !== undefined) return existing
    const resolved = Promise.resolve().then(() => resolveSource(sessionID))
    initInFlight.set(sessionID, resolved)
    resolved.then(
      () => initInFlight.delete(sessionID),
      () => initInFlight.delete(sessionID),
    )
    return resolved
  }
  // The init resolved by the generation probe, consumed by the spawn in the same
  // materialization so the persisted model is read once per dispatch.
  const pendingInit = new Map<string, SessionWorkerInit>()
  const generationOf = async (sessionID: SessionID): Promise<string> => {
    const init = await resolveInit(sessionID)
    pendingInit.set(sessionID, init)
    return `${init.model.providerID}/${init.model.modelID}`
  }
  const spawnFor: SessionBoundSpawner = async (id, sessionID) => {
    await options.beforeSpawn?.()
    const init = pendingInit.get(sessionID) ?? (await resolveInit(sessionID))
    pendingInit.delete(sessionID)
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
        log,
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
  return createExecutorWorkerPool({
    spawn: (id) => createSessionBoundExecutorWorker(id, spawnFor, log, options.onSessionRebound, cap, generationOf),
    onSessionReleased: (sessionID) => {
      initInFlight.delete(sessionID)
      pendingInit.delete(sessionID)
      options.onSessionReleased?.(sessionID)
    },
    log,
  })
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
