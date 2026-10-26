// Per-session worker registry — the single source of session ownership.
//
// The v2 scheduler is unbounded and per-session: a session's worker is created
// on its first dispatch and reused for every later turn until it is reaped or
// its run terminal-cancels. This registry owns BOTH the session→worker map and
// the reservation state (one entry, one monotonic id counter), so the worker id,
// the active run's worker id and the session's state can never drift apart.
//
// State machine: `reserved → running → cancelling`, then either `idle` (the
// worker is kept for reuse) or removal (`dispose`, the worker is retired).
// Absence and `idle` are distinct: `idle` means "a reusable worker exists but is
// not owned by any run". `get`/`size` expose only owned (non-idle) entries;
// `entry`/`worker`/`entries` see every materialized worker.

import type { SessionID } from "@/session/schema"

export type ReservationState = "idle" | "reserved" | "running" | "cancelling"

export interface SessionWorkerEntry<W> {
  /** The worker's monotonic id (canonical). */
  readonly id: number
  /** Alias of {@link id}; the reservation surface historically used this name. */
  readonly workerID: number
  readonly sessionID: SessionID
  readonly worker: W
  readonly state: ReservationState
}

export interface SessionWorkerRegistry<W> {
  /** Number of sessions currently owned by a run (idle workers excluded). */
  readonly size: number
  /** Whether `sessionID` is currently owned by a run. */
  has(sessionID: SessionID): boolean
  /** The owned entry for `sessionID`, or `undefined` when idle/absent. */
  get(sessionID: SessionID): SessionWorkerEntry<W> | undefined
  /** The entry for `sessionID` even while idle, or `undefined` when absent. */
  entry(sessionID: SessionID): SessionWorkerEntry<W> | undefined
  /** The materialized worker for `sessionID` (idle or owned), if any. */
  worker(sessionID: SessionID): W | undefined
  /** Get the session's worker, materializing it under a fresh monotonic id on first use. */
  getOrCreate(sessionID: SessionID, create: (id: number) => W): W
  markRunning(sessionID: SessionID): SessionWorkerEntry<W>
  markCancelling(sessionID: SessionID): SessionWorkerEntry<W>
  /** Drop the run's ownership but keep the worker for reuse (`idle`); idempotent. */
  release(sessionID: SessionID): void
  /** Remove and return the session's worker (the caller terminates it). */
  dispose(sessionID: SessionID): W | undefined
  /** Active session ids currently handled by `workerID`. */
  sessionsOn(workerID: number): ReadonlyArray<SessionID>
  /** Every materialized entry, including idle workers (terminate walks this). */
  entries(): ReadonlyArray<SessionWorkerEntry<W>>
  /** The next worker id this registry will assign. */
  nextID(): number
  clear(): void
}

export interface SessionWorkerRegistryOptions {
  /** First worker id; defaults to 1. */
  readonly firstID?: number
}

export function createSessionWorkerRegistry<W>(
  options: SessionWorkerRegistryOptions = {},
): SessionWorkerRegistry<W> {
  const workers = new Map<SessionID, SessionWorkerEntry<W>>()
  let nextID = options.firstID ?? 1

  const require = (sessionID: SessionID): SessionWorkerEntry<W> => {
    const entry = workers.get(sessionID)
    if (entry === undefined) throw new Error(`no worker entry for session ${sessionID}`)
    return entry
  }

  /** Rewrite the entry's state (and optionally its worker) without a second map. */
  const transition = (sessionID: SessionID, state: ReservationState): SessionWorkerEntry<W> => {
    const current = require(sessionID)
    const next: SessionWorkerEntry<W> = { ...current, state }
    workers.set(sessionID, next)
    return next
  }

  return {
    get size() {
      let count = 0
      for (const entry of workers.values()) if (entry.state !== "idle") count += 1
      return count
    },
    has: (sessionID) => {
      const entry = workers.get(sessionID)
      return entry !== undefined && entry.state !== "idle"
    },
    get: (sessionID) => {
      const entry = workers.get(sessionID)
      return entry === undefined || entry.state === "idle" ? undefined : entry
    },
    entry: (sessionID) => workers.get(sessionID),
    worker: (sessionID) => workers.get(sessionID)?.worker,
    getOrCreate(sessionID, create) {
      const existing = workers.get(sessionID)
      if (existing !== undefined) {
        if (existing.state === "idle") transition(sessionID, "reserved")
        return existing.worker
      }
      const id = nextID
      nextID += 1
      const worker = create(id)
      workers.set(sessionID, { id, workerID: id, sessionID, worker, state: "reserved" })
      return worker
    },
    markRunning: (sessionID) => transition(sessionID, "running"),
    markCancelling: (sessionID) => transition(sessionID, "cancelling"),
    release: (sessionID) => {
      const existing = workers.get(sessionID)
      if (existing === undefined || existing.state === "idle") return
      workers.set(sessionID, { ...existing, state: "idle" })
    },
    dispose(sessionID) {
      const entry = workers.get(sessionID)
      if (entry === undefined) return undefined
      workers.delete(sessionID)
      return entry.worker
    },
    sessionsOn: (workerID) =>
      [...workers.values()]
        .filter((entry) => entry.id === workerID && entry.state !== "idle")
        .map((entry) => entry.sessionID),
    entries: () => [...workers.values()],
    nextID: () => nextID,
    clear: () => {
      workers.clear()
    },
  }
}

export * as SessionWorkerRegistryModule from "./session-registry"
