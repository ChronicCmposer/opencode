// Session-worker thread-name grammar for human observability.
//
// A worker advertises its bound session via its OS thread name (see
// `@/util/thread-name` for the write path). The grammar is:
//
//   idle    `ocs:<last 6 of sessionID>`
//   running `ocs:<last 6 of sessionID>!`
//
// Prefix `ocs:` marks an opencode session worker and the trailing `!` marks a
// live run, so `ps -T` shows at a glance which worker is busy. The id suffix is
// a short, human-recognisable fingerprint — the last 6 chars of the session id —
// so concurrent workers remain distinguishable. The result is clipped to the
// kernel's 15-byte `comm` limit.

import { threadNameWithinLimit } from "@/util/thread-name"

export interface SessionThreadNameOptions {
  /** Append the live-run marker (`!`) while a run is in flight. */
  readonly running?: boolean
}

/**
 * Build the OS thread name for a worker bound to `sessionID`. The idle form is
 * `ocs:<last6>`; `running: true` appends `!`. Always clipped to the kernel's
 * `comm` limit.
 */
export function sessionThreadName(sessionID: string, options?: SessionThreadNameOptions): string {
  // An unbound/empty id is still a valid observation name (`ocs:`); naming is
  // best-effort diagnostics and must never throw at boot.
  const idle = `ocs:${sessionID.slice(-6)}`
  return threadNameWithinLimit(options?.running === true ? `${idle}!` : idle)
}

export * as SessionThreadName from "./thread-name"
