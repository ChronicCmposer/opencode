// W2.5 — per-Session run barrier (D18).
//
// A run is not finished when the model loop stops. Before the next run may be
// admitted, one Session must observe four things:
//
//   1. its durable commits landed,
//   2. the projectors ran (inside the committing transaction),
//   3. tools settled, and
//   4. the background-promotion decision was made.
//
// Delta acknowledgements are deliberately absent: deltas are advisory and may be
// dropped (D17), so they can never be a correctness barrier.
//
// The barrier is per Session and bounded at 5 s. Expiry raises the typed
// `RunBarrierTimeout` and leaves the Session in `timed-out`, so the next run is
// refused until an explicit `reset`. Cancellation wins: once `cancel` is called
// the barrier settles as cancelled even if every phase has already resolved.

/** The hard bound every per-Session barrier is subject to. */
export const RUN_BARRIER_TIMEOUT_MS = 5000

/** The four commit-side phases a run must settle before the next run starts. */
export interface RunBarrierPhases {
  readonly commits: Promise<unknown>
  readonly projectors: Promise<unknown>
  readonly tools: Promise<unknown>
  readonly promotion: Promise<unknown>
}

export interface RunBarrierOutcome {
  readonly sessionID: string
  readonly status: "drained" | "cancelled"
}

export type RunBarrierState = "idle" | "draining" | "drained" | "timed-out" | "cancelled"

/** Injectable timer seam, so the bound is deterministic in tests. */
export interface RunBarrierScheduler {
  schedule(callback: () => void, delayMs: number): unknown
  cancel(handle: unknown): void
}

const defaultScheduler: RunBarrierScheduler = {
  schedule: (callback, delayMs) => setTimeout(callback, delayMs),
  cancel: (handle) => {
    if (typeof handle === "number" || (typeof handle === "object" && handle !== null)) {
      clearTimeout(handle as ReturnType<typeof setTimeout>)
    }
  },
}

export class RunBarrierError extends Error {
  readonly tag: string = "RunBarrierError"
  constructor(message: string) {
    super(message)
    this.name = "RunBarrierError"
  }
}

/** Raised when the commit-side phases did not settle within the 5 s bound. */
export class RunBarrierTimeout extends RunBarrierError {
  override readonly tag = "RunBarrierTimeout"
  constructor(
    readonly sessionID: string,
    readonly timeoutMs: number,
  ) {
    super(`run barrier for session ${sessionID} timed out after ${timeoutMs}ms`)
    this.name = "RunBarrierTimeout"
  }
}

/** A second barrier for a Session whose previous barrier is still draining. */
export class RunBarrierBusyError extends RunBarrierError {
  override readonly tag = "RunBarrierBusyError"
  constructor(readonly sessionID: string) {
    super(`run barrier for session ${sessionID} is still draining`)
    this.name = "RunBarrierBusyError"
  }
}

export interface RunBarrierOptions {
  /** Barrier bound; defaults to {@link RUN_BARRIER_TIMEOUT_MS}. */
  readonly timeoutMs?: number
  readonly scheduler?: RunBarrierScheduler
}

export interface RunBarrier {
  /** Admit the next run only after all four commit-side phases settle. */
  wait(sessionID: string, phases: RunBarrierPhases): Promise<RunBarrierOutcome>
  /** Cancel the active barrier (or the next one) for `sessionID`. */
  cancel(sessionID: string): void
  state(sessionID: string): RunBarrierState
  /** Clear a timed-out/cancelled/drained state so a later run may be admitted. */
  reset(sessionID: string): void
}

interface ActiveBarrier {
  settleCancelled(): void
}

export function createRunBarrier(options: RunBarrierOptions = {}): RunBarrier {
  const timeoutMs = options.timeoutMs ?? RUN_BARRIER_TIMEOUT_MS
  if (!Number.isInteger(timeoutMs) || timeoutMs < 1) {
    throw new Error(`invalid run barrier timeout: ${timeoutMs} (expected a positive integer)`)
  }
  const scheduler = options.scheduler ?? defaultScheduler

  const states = new Map<string, RunBarrierState>()
  const active = new Map<string, ActiveBarrier>()

  const stateOf = (sessionID: string): RunBarrierState => states.get(sessionID) ?? "idle"

  return {
    wait(sessionID, phases) {
      if (sessionID.length === 0) throw new RunBarrierError("run barrier requires a non-empty sessionID")

      const state = stateOf(sessionID)
      if (state === "timed-out") return Promise.reject(new RunBarrierTimeout(sessionID, timeoutMs))
      // A cancelled Session does not start a next run either; reset re-admits it.
      if (state === "cancelled") return Promise.resolve({ sessionID, status: "cancelled" })
      if (state === "draining") return Promise.reject(new RunBarrierBusyError(sessionID))

      states.set(sessionID, "draining")

      return new Promise<RunBarrierOutcome>((resolve, reject) => {
        let settled = false
        let timer: unknown

        const finish = (outcome: RunBarrierOutcome): void => {
          if (settled) return
          settled = true
          if (timer !== undefined) scheduler.cancel(timer)
          timer = undefined
          active.delete(sessionID)
          states.set(sessionID, outcome.status)
          resolve(outcome)
        }

        const fail = (error: Error): void => {
          if (settled) return
          settled = true
          if (timer !== undefined) scheduler.cancel(timer)
          timer = undefined
          active.delete(sessionID)
          states.set(sessionID, "timed-out")
          reject(error)
        }

        const settleCancelled = (): void => finish({ sessionID, status: "cancelled" })
        active.set(sessionID, { settleCancelled })

        timer = scheduler.schedule(() => fail(new RunBarrierTimeout(sessionID, timeoutMs)), timeoutMs)

        Promise.all([phases.commits, phases.projectors, phases.tools, phases.promotion]).then(
          () => {
            // Cancellation wins: `cancel` ran before this continuation, so the
            // active entry is either gone (already cancelled) or this check sees
            // the cancelled state.
            if (settled || stateOf(sessionID) === "cancelled") {
              settleCancelled()
              return
            }
            finish({ sessionID, status: "drained" })
          },
          (error: unknown) => {
            // Phase failures are caller bugs and must not be swallowed; keep the
            // barrier closed so the next run is not admitted after a broken drain.
            if (settled) return
            fail(error instanceof Error ? error : new RunBarrierError(String(error)))
          },
        )
      })
    },
    cancel(sessionID) {
      const current = active.get(sessionID)
      if (current) {
        // `settleCancelled` is idempotent via the `settled` flag in `wait`.
        states.set(sessionID, "cancelled")
        current.settleCancelled()
        return
      }
      states.set(sessionID, "cancelled")
    },
    state: stateOf,
    reset(sessionID) {
      if (active.has(sessionID)) throw new RunBarrierBusyError(sessionID)
      states.delete(sessionID)
    },
  }
}

export * as SessionRunBarrier from "./run-barrier"
