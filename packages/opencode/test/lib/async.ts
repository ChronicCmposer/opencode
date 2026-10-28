// Shared async test primitives.
//
// Everything a test needs to observe queued work, bound a promise, or wait for
// an abort lives here. `poll.ts` re-exports {@link pollUntil} so older imports
// keep working while new tests have a single async toolkit.
//
// Design: each primitive settles to a plain value or fails with a descriptive
// error; none of them silently hangs. Timers are always cleared on every exit
// path so a test process never keeps a stray handle alive.

import type { SessionID } from "@/session/schema"

/** Resolve after one macrotask, flushing pending microtasks and timer callbacks. */
export function flush(): Promise<void> {
  return new Promise<void>((resolve) => setTimeout(resolve, 0))
}

/** The outcome of a captured promise: either a value or the rejection reason. */
export type Settled<T> = { readonly ok: true; readonly value: T } | { readonly ok: false; readonly error: unknown }

/**
 * Await `promise` and settle it into a plain value. Never rejects, so a test can
 * assert on both the success and failure paths uniformly (Law of Atomic
 * Predictability: same input, a predictable data result — no throw).
 */
export async function settleOf<T>(promise: Promise<T>): Promise<Settled<T>> {
  try {
    return { ok: true, value: await promise }
  } catch (error) {
    return { ok: false, error }
  }
}

/** Existing name for {@link settleOf}, kept so current suites keep importing it. */
export const capture = settleOf

/** Parse a plain string into a trusted `SessionID` at the test boundary. */
export function sid(value: string): SessionID {
  return value as SessionID
}

/** Options for {@link withDeadline}. */
export interface DeadlineOptions {
  /** Total time to wait before rejecting. */
  readonly timeoutMs: number
  /** Failure message; defaults to a descriptive timeout notice. */
  readonly message?: string
  /** Runs exactly once if (and only if) the deadline fires. */
  readonly onTimeout?: () => void
}

/** Parse the two accepted call shapes into one options object. */
function deadlineOptions(
  options: number | DeadlineOptions,
  message?: string,
  onTimeout?: () => void,
): DeadlineOptions {
  if (typeof options === "number") {
    return {
      timeoutMs: options,
      ...(message === undefined ? {} : { message }),
      ...(onTimeout === undefined ? {} : { onTimeout }),
    }
  }
  return options
}

/**
 * Bound `promise` by `timeoutMs`. A fast success/failure passes through
 * unchanged; a deadline fires `onTimeout` once and rejects with `message`.
 *
 * `onTimeout` is the seam a caller uses to abort the underlying work (e.g.
 * `() => controller.abort()`), so a timed-out test does not leave it running.
 */
export function withDeadline<T>(
  promise: Promise<T>,
  options: number | DeadlineOptions,
  message?: string,
  onTimeout?: () => void,
): Promise<T> {
  const config = deadlineOptions(options, message, onTimeout)
  return new Promise<T>((resolve, reject) => {
    let settled = false
    const timer = setTimeout(() => {
      if (settled) return
      settled = true
      config.onTimeout?.()
      reject(new Error(config.message ?? `operation timed out after ${config.timeoutMs}ms`))
    }, config.timeoutMs)

    promise.then(
      (value) => {
        if (settled) return
        settled = true
        clearTimeout(timer)
        resolve(value)
      },
      (error: unknown) => {
        if (settled) return
        settled = true
        clearTimeout(timer)
        reject(error)
      },
    )
  })
}

/** Options for {@link awaitSignal}. */
export interface AwaitSignalOptions {
  /** Optional deadline; without it the wait is unbounded. */
  readonly timeoutMs?: number
  /** Failure message used when the optional deadline fires. */
  readonly message?: string
}

/**
 * Resolve once `signal` aborts (immediately when it already has). With a
 * `timeoutMs`, reject descriptively if the abort never arrives.
 *
 * The abort listener is removed on every exit path — timeout included — so a
 * short-lived test does not retain a listener on a long-lived signal.
 */
export function awaitSignal(signal: AbortSignal, options: AwaitSignalOptions = {}): Promise<void> {
  if (signal.aborted) return Promise.resolve()

  return new Promise<void>((resolve, reject) => {
    const timeoutMs = options.timeoutMs
    const timer =
      timeoutMs === undefined
        ? undefined
        : setTimeout(() => {
            signal.removeEventListener("abort", onAbort)
            reject(new Error(options.message ?? `signal was not aborted within ${timeoutMs}ms`))
          }, timeoutMs)

    const onAbort = () => {
      if (timer !== undefined) clearTimeout(timer)
      resolve()
    }
    signal.addEventListener("abort", onAbort, { once: true })
  })
}

// ---------------------------------------------------------------------------
// Polling
// ---------------------------------------------------------------------------

export interface PollUntilOptions {
  /** Total time to keep polling before failing. Defaults to 2000 ms. */
  readonly timeoutMs?: number
  /** Delay between attempts. Defaults to 10 ms. */
  readonly intervalMs?: number
  /** Failure message used when the predicate never becomes true. */
  readonly message?: string
}

/**
 * Resolve once `predicate()` is `true`, retrying every `intervalMs` until
 * `timeoutMs` elapses. The predicate may be sync or async.
 */
export async function pollUntil(
  predicate: () => boolean | Promise<boolean>,
  options: PollUntilOptions = {},
): Promise<void> {
  const timeoutMs = options.timeoutMs ?? 2000
  const intervalMs = options.intervalMs ?? 10
  const deadline = Date.now() + timeoutMs

  for (;;) {
    if (await predicate()) return
    if (Date.now() >= deadline) {
      throw new Error(options.message ?? `pollUntil timed out after ${timeoutMs}ms`)
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs))
  }
}
