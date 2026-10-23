// Session-worker thread-name grammar for human observability.
//
// A worker thread advertises which session slot it serves via its OS thread
// name (see `@/util/thread-name` for the write path). The grammar is:
//
//   idle    `ocs<slot>`
//   running `ocs<slot>:<last 6 of sessionID>`
//
// Prefix `oc` marks opencode, `s` marks a session slot, no dash, no
// zero-padding. The id suffix is a short, human-recognisable fingerprint — the
// last 6 chars of the session id — so `ps -T` distinguishes concurrent runs.
// The result is clipped to the kernel's 15-byte `comm` limit.

import { threadNameWithinLimit } from "@/util/thread-name"

/**
 * Build the OS thread name for the session worker in `slot`. Without a
 * `sessionID` the slot is idle (`ocs<slot>`); with one it is running and appends
 * the last 6 chars of the id. Always clipped to the kernel's `comm` limit.
 */
export function sessionThreadName(slot: number, sessionID?: string): string {
  const idle = `ocs${slot}`
  if (!sessionID) return threadNameWithinLimit(idle)
  return threadNameWithinLimit(`${idle}:${sessionID.slice(-6)}`)
}

export * as SessionThreadName from "./thread-name"
