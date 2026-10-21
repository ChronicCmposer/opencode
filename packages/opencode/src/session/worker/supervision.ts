// Phase 4.7 — crash-loop supervision (plan task 4.7, plan line 71).
//
// A crashed worker is respawned with exponential backoff (1 s doubling to a
// 30 s cap). After five consecutive crashes on one slot the slot is degraded:
// the pool runs at K−1 rather than respawning forever. A healthy run resets the
// counter. This module is pure bookkeeping; the dispatcher owns the timers and
// the actual spawn.

export const CRASH_BACKOFF_INITIAL_MS = 1_000
export const CRASH_BACKOFF_MAX_MS = 30_000
export const CRASH_ATTEMPTS_BEFORE_DEGRADED = 5

export interface CrashDecision {
  readonly workerID: number
  readonly attempts: number
  readonly delayMs: number
  readonly degraded: boolean
}

export interface Supervision {
  readonly degradedCount: number
  recordCrash(workerID: number): CrashDecision
  recordHealthy(workerID: number): void
  isDegraded(workerID: number): boolean
}

/** Backoff for the Nth consecutive crash: `1s · 2^(N−1)`, capped at 30 s. */
export function crashBackoffMs(attempts: number): number {
  if (attempts < 1) return 0
  return Math.min(CRASH_BACKOFF_MAX_MS, CRASH_BACKOFF_INITIAL_MS * 2 ** (attempts - 1))
}

export function createSupervision(): Supervision {
  const attemptsByWorker = new Map<number, number>()

  return {
    get degradedCount() {
      let count = 0
      for (const attempts of attemptsByWorker.values()) {
        if (attempts >= CRASH_ATTEMPTS_BEFORE_DEGRADED) count++
      }
      return count
    },
    recordCrash(workerID) {
      const attempts = (attemptsByWorker.get(workerID) ?? 0) + 1
      attemptsByWorker.set(workerID, attempts)
      return {
        workerID,
        attempts,
        delayMs: crashBackoffMs(attempts),
        degraded: attempts >= CRASH_ATTEMPTS_BEFORE_DEGRADED,
      }
    },
    recordHealthy(workerID) {
      attemptsByWorker.delete(workerID)
    },
    isDegraded(workerID) {
      return (attemptsByWorker.get(workerID) ?? 0) >= CRASH_ATTEMPTS_BEFORE_DEGRADED
    },
  }
}

export * as SupervisionModule from "./supervision"
