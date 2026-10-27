// The explicit per-isolate process role.
//
// The process role is a property of the ISOLATE, not of a graph. The process
// main thread is the `hub`; each session-worker isolate adopts the role its
// `workerData` carries (`setProcessRole(init.role)` at boot in
// `session-worker.ts`). Module state is per-isolate — a worker gets its own copy
// of this module — so a worker's role never leaks into the hub and vice versa.
//
// This replaces the ambient `isMainThread` check that used to decide session-hub
// graph membership: a session worker is a `session`, so a worker-hosted graph
// never contributes the hub node (and never builds a nested hub).
//
// The role is FAIL CLOSED: an isolate that never declares one has NO authority.
// The hub declares itself explicitly in `project/bootstrap.ts`; a worker
// declares itself at its entry. Callers (e.g. graph membership) must treat an
// unset role as "not hub", never as an implicit hub.

import type { Role } from "@/session/worker/node-table"

/** The canonical role list; the runtime guard at every parse boundary. */
export const ROLES: readonly Role[] = ["session", "hub"]

let currentRole: Role | undefined

/**
 * The calling isolate's process role, or `undefined` when it has not declared
 * one. Graph membership treats `undefined` as a non-hub and stays closed.
 */
export function processRole(): Role | undefined {
  return currentRole
}

/** The calling isolate's role, failing loud when it never declared one. */
export function requireProcessRole(): Role {
  if (currentRole === undefined) {
    throw new Error("process role is not set: declare it at the process entry before reading it")
  }
  return currentRole
}

/** Whether `value` is one of the declared {@link Role}s (a runtime boundary guard). */
export function isRole(value: unknown): value is Role {
  return typeof value === "string" && ROLES.includes(value as Role)
}

/**
 * Adopt `role` for the calling isolate. Set-once: the first call wins, a repeat
 * of the SAME role is an idempotent no-op, and a CONFLICTING re-set fails loud —
 * two authorities disagreeing about what this process is cannot both be right.
 */
export function setProcessRole(role: Role): void {
  if (currentRole === undefined) {
    currentRole = role
    return
  }
  if (currentRole !== role) {
    throw new Error(`process role is already "${currentRole}"; refusing to re-set it to "${role}"`)
  }
}

export * as ProcessRole from "./process-role"
