// One global spawn permit for per-session workers.
//
// A Bun `Worker` costs a thread and boots asynchronously. To keep a burst of
// first prompts from spawning every session at once, the hub serializes
// `new Worker` behind this single permit and releases it only when the spawned
// worker has booted (its first frame / ready handshake). Waiters queue FIFO and
// are never rejected by the gate while they wait; the only rejection is the
// acquisition timeout, which fails that one spawn instead of the session.
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
   * How long an `acquire` may wait for the permit before its spawn attempt
   * fails. A positive, finite number of milliseconds.
   */
  readonly timeoutMs: number
}

/** Why a permit was released. Diagnostic only; both outcomes free the gate. */
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
  /** Wait for the one global permit, resolving in FIFO order. */
  acquire(): Promise<SpawnPermit>
  /** Whether the single permit is currently held (permit-count probes/tests). */
  readonly held?: boolean
  /** Number of FIFO waiters not yet granted the permit. */
  readonly waiting?: number
}

/** A spawn could not obtain the global permit within the configured timeout. */
export class SpawnGateTimeoutError extends Error {
  readonly timeoutMs: number

  constructor(timeoutMs: number) {
    super(`spawn gate: no worker spawn permit became available within ${timeoutMs}ms`)
    this.name = "SpawnGateTimeoutError"
    this.timeoutMs = timeoutMs
  }
}

interface Waiter {
  readonly resolve: (permit: SpawnPermit) => void
  readonly reject: (error: Error) => void
  readonly timer: ReturnType<typeof setTimeout>
}

export function createSpawnGate(options: SpawnGateOptions): SpawnGateHandle {
  const timeoutMs = requireTimeout(options.timeoutMs)
  const waiters: Waiter[] = []
  let held = false

  const permit = (): SpawnPermit => {
    let released = false
    const release = (): void => {
      if (released) return
      released = true
      held = false
      const waiter = waiters.shift()
      if (waiter === undefined) return
      clearTimeout(waiter.timer)
      granted(waiter.resolve)
    }
    return { release }
  }

  // Admit one waiter as the new holder. The permit is created only after
  // `held` flips, so a grant can never race a second grant.
  const granted = (resolve: (permit: SpawnPermit) => void): void => {
    held = true
    resolve(permit())
  }

  return {
    get held() {
      return held
    },
    get waiting() {
      return waiters.length
    },
    acquire() {
      if (!held) {
        held = true
        return Promise.resolve(permit())
      }
      return new Promise<SpawnPermit>((resolve, reject) => {
        const timer = setTimeout(() => {
          const index = waiters.indexOf(waiter)
          if (index === -1) return
          waiters.splice(index, 1)
          reject(new SpawnGateTimeoutError(timeoutMs))
        }, timeoutMs)
        // The acquisition deadline is a shutdown guard, not work: never keep the
        // process alive waiting for a permit nobody will release.
        timer.unref?.()
        const waiter: Waiter = { resolve, reject, timer }
        waiters.push(waiter)
      })
    },
  }
}

function requireTimeout(value: number): number {
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`spawn gate: timeoutMs must be a positive finite number, received ${value}`)
  }
  return value
}

export * as SpawnGate from "./spawn-gate"
