// Per-session crash supervision.
//
// A Bun isolate crash is not retried on the same run: the dispatcher rejects the
// in-flight run at-most-once and retires the worker, so the next dispatch lazily
// spawns a fresh one. This module decides when that next dispatch is allowed:
//
// - five consecutive crashes on one session mark it dead (background/auto
//   dispatch then fails fast instead of spawning forever);
// - crashes back off exponentially from 1 s, capped at 30 s;
// - a healthy run resets the counter;
// - a user-initiated dispatch always resets the session, because the user asked
//   for it explicitly and the dispatcher will spawn a fresh worker.
//
// The clock and timer are injected so the policy is deterministic in tests and
// never depends on wall-clock time (`Law of Atomic Predictability`).

import type { DispatchOrigin } from "@/session/origin"

export const CRASH_BACKOFF_INITIAL_MS = 1_000
export const CRASH_BACKOFF_MAX_MS = 30_000
export const CRASH_ATTEMPTS_BEFORE_DEAD = 5

/**
 * Hard bound on retained per-session crash state. A session that crashes and is
 * then abandoned never succeeds, so it is never cleared by `recordHealthy`; the
 * cap evicts the least-recently-used history under churn instead of leaking one
 * entry per crashed session for the process lifetime.
 */
export const MAX_SUPERVISION_SESSIONS = 1_024

/** Re-exported so worker-side callers keep one supervision import. */
export type { DispatchOrigin }

export type DispatchDecision =
  | { readonly kind: "allow" }
  | { readonly kind: "backoff"; readonly delayMs: number }
  | { readonly kind: "dead" }

export interface CrashDecision {
  readonly sessionID: string
  readonly attempts: number
  readonly delayMs: number
  readonly dead: boolean
}

export interface SupervisionOptions {
  /** Monotonic-ish clock in milliseconds; defaults to `Date.now`. */
  readonly now?: () => number
  /** Timer seam returning a cancel handle; defaults to `setTimeout`/`clearTimeout`. */
  readonly schedule?: (delayMs: number, run: () => void) => () => void
}

export interface Supervision {
  /** Number of sessions with retained crash history (bounded). */
  readonly size: number
  /** Decide whether this dispatch may proceed, applying reset/backoff/dead policy. */
  decideDispatch(sessionID: string, origin: DispatchOrigin): DispatchDecision
  recordCrash(sessionID: string): CrashDecision
  recordHealthy(sessionID: string): void
  isDead(sessionID: string): boolean
  /** Drop all state for a session that no longer exists. */
  forget(sessionID: string): void
}

/** Raised by the hub-facing gate when a background/auto dispatch targets a dead session. */
export class SessionWorkerDeadError extends Error {
  constructor(readonly sessionID: string) {
    super(`session ${sessionID} is dead after repeated worker crashes; retry requires a user-initiated dispatch`)
    this.name = "SessionWorkerDeadError"
  }
}

/**
 * Backoff for the Nth consecutive crash: `1s · 2^(N−1)`, capped at 30 s.
 * The exponent is capped so a long-lived session can never overflow to
 * `Infinity`; a non-finite or sub-one count maps to no backoff.
 */
export function crashBackoffMs(attempts: number): number {
  if (!Number.isFinite(attempts) || attempts < 1) return 0
  return Math.min(CRASH_BACKOFF_MAX_MS, CRASH_BACKOFF_INITIAL_MS * 2 ** Math.min(attempts - 1, 30))
}

/**
 * Reject a non-integer or negative pool slot id. Shared with `pid-registry.ts`
 * so both the worker-id mirror and any slot-keyed caller validate ids identically.
 */
export function assertWorkerID(workerID: number): void {
  if (!Number.isInteger(workerID) || workerID < 0) {
    throw new Error(`invalid workerID: ${workerID} (expected a non-negative integer)`)
  }
}

interface SessionCrashState {
  attempts: number
  dead: boolean
  /** When the backoff window ends; 0 means no backoff is pending. */
  retryAt: number
  cancel: (() => void) | undefined
}

export function createSupervision(options: SupervisionOptions = {}): Supervision {
  const now = options.now ?? Date.now
  const schedule = options.schedule ?? defaultSchedule
  const states = new Map<string, SessionCrashState>()

  const requireSessionID = (sessionID: string): void => {
    if (typeof sessionID !== "string" || sessionID.length === 0) {
      throw new Error(`invalid sessionID: ${String(sessionID)} (expected a non-empty string)`)
    }
  }

  const clearTimer = (state: SessionCrashState): void => {
    state.cancel?.()
    state.cancel = undefined
  }

  const reset = (sessionID: string): void => {
    const state = states.get(sessionID)
    if (state !== undefined) clearTimer(state)
    states.delete(sessionID)
  }

  /**
   * Evict the least-recently-crashed session when a new one would exceed the
   * cap. `Map` preserves insertion order and `recordCrash` re-inserts, so the
   * first key is the oldest history. A pending backoff timer is canceled with
   * the entry so eviction cannot leave a timer behind.
   */
  const evictOldest = (): void => {
    const oldest = states.keys().next().value
    if (oldest === undefined) return
    const state = states.get(oldest)
    if (state !== undefined) clearTimer(state)
    states.delete(oldest)
  }

  return {
    get size() {
      return states.size
    },
    decideDispatch(sessionID, origin) {
      requireSessionID(sessionID)
      if (origin === "user") {
        reset(sessionID)
        return { kind: "allow" }
      }
      const state = states.get(sessionID)
      if (state === undefined) return { kind: "allow" }
      if (state.dead) return { kind: "dead" }
      const remaining = state.retryAt - now()
      if (remaining <= 0) {
        clearTimer(state)
        state.retryAt = 0
        return { kind: "allow" }
      }
      return { kind: "backoff", delayMs: remaining }
    },
    recordCrash(sessionID) {
      requireSessionID(sessionID)
      const existing = states.get(sessionID)
      if (existing === undefined && states.size >= MAX_SUPERVISION_SESSIONS) evictOldest()
      const state = existing ?? { attempts: 0, dead: false, retryAt: 0, cancel: undefined }
      const attempts = state.attempts + 1
      const delayMs = crashBackoffMs(attempts)
      state.attempts = attempts
      state.dead = attempts >= CRASH_ATTEMPTS_BEFORE_DEAD
      clearTimer(state)
      if (state.dead) {
        state.retryAt = 0
      } else {
        state.retryAt = now() + delayMs
        state.cancel = schedule(delayMs, () => {
          state.retryAt = 0
          state.cancel = undefined
        })
      }
      // Re-insert so the session becomes the most recent entry (LRU order).
      states.delete(sessionID)
      states.set(sessionID, state)
      return { sessionID, attempts, delayMs, dead: state.dead }
    },
    recordHealthy(sessionID) {
      requireSessionID(sessionID)
      reset(sessionID)
    },
    isDead(sessionID) {
      requireSessionID(sessionID)
      return states.get(sessionID)?.dead ?? false
    },
    forget(sessionID) {
      requireSessionID(sessionID)
      reset(sessionID)
    },
  }
}

function defaultSchedule(delayMs: number, run: () => void): () => void {
  const timer = setTimeout(run, delayMs)
  timer.unref?.()
  return () => clearTimeout(timer)
}

export * as SupervisionModule from "./supervision"
