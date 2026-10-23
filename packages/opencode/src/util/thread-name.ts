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

/**
 * Name the current OS thread `name` on Linux. No-op (never throws) elsewhere or
 * when the kernel rejects the write, since thread naming is best-effort
 * observability. Names longer than the kernel's 15-byte `comm` limit are
 * truncated by the kernel, not by us.
 */
export function setThreadName(name: string): void {
  try {
    writeFileSync(THREAD_COMM_PATH, name)
  } catch {
    // Non-Linux host or unwritable /proc: leave the default name in place.
  }
}

export * as ThreadName from "./thread-name"
