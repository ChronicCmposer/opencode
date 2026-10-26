// Per-session worker registry — one lazily-materialized worker per session.
//
// The v2 scheduler is unbounded and per-session: a session's worker is created
// on its first dispatch and reused for every later turn until it is reaped or
// its run terminal-cancels. This registry is the session→worker map; it owns the
// monotonic worker id source so a replacement never reuses an id.

import type { SessionID } from "@/session/schema"

export interface SessionWorkerEntry<W> {
  readonly id: number
  readonly sessionID: SessionID
  readonly worker: W
}

export interface SessionWorkerRegistry<W> {
  readonly size: number
  has(sessionID: SessionID): boolean
  get(sessionID: SessionID): W | undefined
  entry(sessionID: SessionID): SessionWorkerEntry<W> | undefined
  /** Get the session's worker, materializing it under a fresh monotonic id on first use. */
  getOrCreate(sessionID: SessionID, create: (id: number) => W): W
  /** Remove and return the session's worker (the caller terminates it). */
  dispose(sessionID: SessionID): W | undefined
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

  return {
    get size() {
      return workers.size
    },
    has: (sessionID) => workers.has(sessionID),
    get: (sessionID) => workers.get(sessionID)?.worker,
    entry: (sessionID) => workers.get(sessionID),
    getOrCreate(sessionID, create) {
      const existing = workers.get(sessionID)
      if (existing !== undefined) return existing.worker
      const id = nextID
      nextID += 1
      const worker = create(id)
      workers.set(sessionID, { id, sessionID, worker })
      return worker
    },
    dispose(sessionID) {
      const entry = workers.get(sessionID)
      if (entry === undefined) return undefined
      workers.delete(sessionID)
      return entry.worker
    },
    entries: () => [...workers.values()],
    nextID: () => nextID,
    clear: () => {
      workers.clear()
    },
  }
}

export * as SessionWorkerRegistryModule from "./session-registry"
