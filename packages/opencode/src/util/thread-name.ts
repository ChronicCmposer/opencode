// Best-effort OS thread naming for human observability.
//
// Linux exposes a per-thread name at `/proc/thread-self/comm`. Each opencode
// isolate (hub `och`, session workers by session id) calls `setThreadName` from
// inside its own thread so `ps -T`, `top -H` and friends label the thread
// instead of showing it as the process name. `/proc/thread-self` is a symlink
// to the CALLING thread's `/proc/<pid>/task/<tid>`; `/proc/self` would name the
// process leader and every isolate would fight over one shared name.
//
// This is deliberately fail-soft: the file is absent on non-Linux hosts (macOS,
// Windows) and may be unwritable in a hardened sandbox. A failed name write
// must never break a worker, so every error is swallowed. Callers ignore the
// result — the name is a diagnostic, not a control-flow signal.

import { writeFileSync } from "node:fs"

const THREAD_COMM_PATH = "/proc/thread-self/comm"

/** The kernel's `comm` field holds at most 15 bytes. */
const THREAD_COMM_MAX_BYTES = 15

/**
 * Clip `name` to the kernel's 15-byte `comm` limit, counting UTF-8 bytes and
 * keeping whole code points. Clipping stops before the first code point that
 * would exceed the budget, so a surrogate pair is never split (a lone surrogate
 * would be written as U+FFFD) and the kernel never rejects the name for length.
 * Pure and total, so callers can compose names freely (see `sessionThreadName`).
 */
export function threadNameWithinLimit(name: string): string {
  // `toWellFormed` removes lone surrogates from the input itself, so the result
  // is always a well-formed UTF-16 string safe for `writeFileSync`.
  let clipped = ""
  for (const char of name.toWellFormed()) {
    if (Buffer.byteLength(clipped + char, "utf8") > THREAD_COMM_MAX_BYTES) break
    clipped += char
  }
  return clipped
}

/**
 * Name the current OS thread `name` on Linux. No-op (never throws) elsewhere or
 * when the kernel rejects the write, since thread naming is best-effort
 * observability. The name is clipped to the kernel's 15-byte `comm` limit
 * before writing.
 */
export function setThreadName(name: string): void {
  try {
    writeFileSync(THREAD_COMM_PATH, threadNameWithinLimit(name))
  } catch {
    // Non-Linux host or unwritable /proc: leave the default name in place.
  }
}

export * as ThreadName from "./thread-name"
