// Phase 4.8 — key→worker map lifecycle (plan task 4.8, plan line 61).
//
// The hub's only per-key state: a `SessionID`→worker reservation. The state
// machine is `reserved → running → cancelling`; the terminal `idle`/`released`
// states are represented by absence (there is no mapping). Insert on dispatch,
// delete on completion/cancel/crash, clear on terminate.

import type { SessionID } from "@/session/schema"

export type ReservationState = "reserved" | "running" | "cancelling"

export interface Reservation {
  readonly sessionID: SessionID
  readonly state: ReservationState
  readonly workerID: number | undefined
}

export interface SessionRegistry {
  readonly size: number
  has(sessionID: SessionID): boolean
  get(sessionID: SessionID): Reservation | undefined
  /** Insert a new reservation in the `reserved` state; fails loud on a duplicate. */
  reserve(sessionID: SessionID, workerID: number): Reservation
  markRunning(sessionID: SessionID): Reservation
  markCancelling(sessionID: SessionID): Reservation
  /** Delete the mapping (completion/cancel/crash); idempotent. */
  release(sessionID: SessionID): void
  sessionsOn(workerID: number): ReadonlyArray<SessionID>
  entries(): ReadonlyArray<Reservation>
  /** Wipe every mapping (terminate). */
  clear(): void
}

export function createSessionRegistry(): SessionRegistry {
  const reservations = new Map<SessionID, Reservation>()

  const require = (sessionID: SessionID): Reservation => {
    const reservation = reservations.get(sessionID)
    if (reservation === undefined) throw new Error(`no reservation for session ${sessionID}`)
    return reservation
  }

  const transition = (sessionID: SessionID, state: ReservationState): Reservation => {
    const current = require(sessionID)
    const next: Reservation = { sessionID, state, workerID: current.workerID }
    reservations.set(sessionID, next)
    return next
  }

  return {
    get size() {
      return reservations.size
    },
    has: (sessionID) => reservations.has(sessionID),
    get: (sessionID) => reservations.get(sessionID),
    reserve(sessionID, workerID) {
      if (reservations.has(sessionID)) throw new Error(`session ${sessionID} is already reserved`)
      const reservation: Reservation = { sessionID, state: "reserved", workerID }
      reservations.set(sessionID, reservation)
      return reservation
    },
    markRunning: (sessionID) => transition(sessionID, "running"),
    markCancelling: (sessionID) => transition(sessionID, "cancelling"),
    release: (sessionID) => {
      reservations.delete(sessionID)
    },
    sessionsOn: (workerID) =>
      [...reservations.values()].filter((reservation) => reservation.workerID === workerID).map((r) => r.sessionID),
    entries: () => [...reservations.values()],
    clear: () => {
      reservations.clear()
    },
  }
}

export * as SessionRegistryModule from "./registry"
