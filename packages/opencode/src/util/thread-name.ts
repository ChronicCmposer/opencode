// Best-effort OS thread naming for human observability.
//
// Linux exposes a per-thread name at `/proc/thread-self/comm`. Each opencode
// isolate (writer `ocw`, broker `ocb`, hub `och`) calls `setThreadName` from
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
const THREAD_COMM_MAX = 15

/**
 * Clip `name` to the kernel's 15-byte `comm` limit. Pure and total, so callers
 * can compose names freely (see `sessionThreadName`) without re-checking the
 * limit at every call site.
 */
export function threadNameWithinLimit(name: string): string {
  return name.slice(0, THREAD_COMM_MAX)
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
