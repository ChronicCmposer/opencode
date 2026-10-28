// Cap-bounded parallel spawn gate for per-session workers.
//
// A Bun `Worker` costs a thread and boots asynchronously. A burst of first
// prompts must not spawn every session at once, but a single global permit also
// serializes every cold boot behind one slow graph build. This gate is a
// counting semaphore: up to `capacity` boots run concurrently and the rest queue
// FIFO. A permit is released only when the spawned worker has booted (its first
// frame / ready handshake), so a stalled boot occupies one slot, not the gate.
//
// Integration (owned by the hub/execute wiring, not this module):
//
//   const permit = await gate.acquire()
//   try {
//     worker = new Worker(...)
//     await workerReady
//     permit.release("ready")
//   } catch (error) {
//     permit.release("failed", error)
//   }
//
// `acquire` resolves before the worker is constructed, so the caller already
// holds the acquisition when it releases. A failed spawn releases the permit
// too (the failure travels through the caller's `catch`); the gate never
// rejects a granted acquisition. `outcome`/`error` are diagnostic only — they
// never change the release semantics — and releasing twice is a no-op.

export interface SpawnGateOptions {
  /**
   * How long an `acquire` may wait for a slot before its spawn attempt fails.
   * A positive, finite number of milliseconds.
   */
  readonly timeoutMs: number
  /**
   * How many boots may run concurrently. Defaults to 4; `1` preserves the old
   * fully-serialized behavior. A positive integer.
   */
  readonly capacity?: number
}

/** Why a permit was released. Diagnostic only; both outcomes free the slot. */
export type SpawnOutcome = "ready" | "failed"

export interface SpawnPermit {
  /**
   * Release the permit exactly once. Omit `outcome` (or pass `"ready"`) after a
   * successful boot; pass `"failed"` (with the spawn `error`) after a failed
   * spawn. Both paths free the next waiter; `error` is kept for diagnostics and
   * is never re-thrown by the gate.
   */
  release(outcome?: SpawnOutcome, error?: unknown): void
}

export interface SpawnGateHandle {
  /**
   * Wait for a boot slot, resolving in FIFO order. An aborted `signal` removes
   * this waiter without granting or consuming a permit, so a cancelled boot
   * never waits out the acquisition deadline or leaves a phantom slot behind.
   */
  acquire(signal?: AbortSignal): Promise<SpawnPermit>
  /** Whether at least one boot slot is currently held (permit-count probes/tests). */
  readonly held?: boolean
  /** How many boot slots are currently held. */
  readonly active?: number
  /** The maximum number of concurrent boots. */
  readonly capacity?: number
  /** Number of FIFO waiters not yet granted a slot. */
  readonly waiting?: number
}

/** A spawn could not obtain a boot slot within the configured timeout. */
export class SpawnGateTimeoutError extends Error {
  readonly timeoutMs: number

  constructor(timeoutMs: number) {
    super(`spawn gate: no worker spawn permit became available within ${timeoutMs}ms`)
    this.name = "SpawnGateTimeoutError"
    this.timeoutMs = timeoutMs
  }
}

/** A boot slot acquisition was aborted before it was granted. */
export class SpawnGateAbortedError extends Error {
  constructor() {
    super("spawn gate: acquisition aborted before a worker spawn permit was granted")
    this.name = "SpawnGateAbortedError"
  }
}

interface Waiter {
  readonly resolve: (permit: SpawnPermit) => void
  readonly reject: (error: Error) => void
  readonly timer: ReturnType<typeof setTimeout>
  readonly signal: AbortSignal | undefined
  readonly onAbort: (() => void) | undefined
}

const DEFAULT_CAPACITY = 4

export function createSpawnGate(options: SpawnGateOptions): SpawnGateHandle {
  const timeoutMs = requirePositive("timeoutMs", options.timeoutMs)
  const capacity = requirePositiveInteger("capacity", options.capacity ?? DEFAULT_CAPACITY)
  const waiters: Waiter[] = []
  let active = 0

  const permit = (): SpawnPermit => {
    let released = false
    const release = (): void => {
      if (released) return
      released = true
      active -= 1
      grantNext()
    }
    return { release }
  }

  // Admit waiters while capacity remains. A grant creates the permit only after
  // the slot is counted, so a grant can never race a second grant.
  const grantNext = (): void => {
    while (active < capacity) {
      const waiter = waiters.shift()
      if (waiter === undefined) return
      clearTimeout(waiter.timer)
      if (waiter.signal !== undefined && waiter.onAbort !== undefined) {
        waiter.signal.removeEventListener("abort", waiter.onAbort)
      }
      active += 1
      waiter.resolve(permit())
    }
  }

  /** Remove a waiter that was aborted; its abort listener is already running. */
  const abortWaiter = (waiter: Waiter, reject: (error: Error) => void): void => {
    const index = waiters.indexOf(waiter)
    if (index === -1) return
    waiters.splice(index, 1)
    clearTimeout(waiter.timer)
    reject(new SpawnGateAbortedError())
  }

  return {
    get held() {
      return active > 0
    },
    get active() {
      return active
    },
    get capacity() {
      return capacity
    },
    get waiting() {
      return waiters.length
    },
    acquire(signal?: AbortSignal) {
      if (signal?.aborted === true) return Promise.reject(new SpawnGateAbortedError())
      if (active < capacity) {
        active += 1
        return Promise.resolve(permit())
      }
      return new Promise<SpawnPermit>((resolve, reject) => {
        const timer = setTimeout(() => {
          const index = waiters.indexOf(waiter)
          if (index === -1) return
          waiters.splice(index, 1)
          // The deadline ends the acquisition: detach the abort listener so a
          // long-lived signal does not retain every timed-out waiter.
          if (signal !== undefined && onAbort !== undefined) {
            signal.removeEventListener("abort", onAbort)
          }
          reject(new SpawnGateTimeoutError(timeoutMs))
        }, timeoutMs)
        // The acquisition deadline is a shutdown guard, not work: never keep the
        // process alive waiting for a slot nobody will release.
        timer.unref?.()
        const onAbort = signal === undefined ? undefined : () => abortWaiter(waiter, reject)
        const waiter: Waiter = { resolve, reject, timer, signal, onAbort }
        if (signal !== undefined && onAbort !== undefined) signal.addEventListener("abort", onAbort, { once: true })
        waiters.push(waiter)
      })
    },
  }
}

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

export * as SpawnGate from "./spawn-gate"
