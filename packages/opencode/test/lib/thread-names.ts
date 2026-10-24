// OS thread-name inspection over the Linux `/proc` filesystem.
//
// The thread-name lifecycle tests must observe a worker thread's REAL OS name
// (`/proc/self/task/<tid>/comm`), not a value the fixture reports. Reading that
// file is inherently racy: a thread can exit between the directory listing and
// the per-thread read, so each read is skipped rather than failing the test.
//
// `waitFor*` poll through the shared `pollUntil`, so a name that never appears
// fails loudly with the names actually seen instead of hanging.

import { existsSync, readFileSync, readdirSync } from "node:fs"
import { pollUntil } from "./poll"

/** The Linux per-process task directory. */
export const TASK_DIR = "/proc/self/task"

/** Whether this process exposes Linux `/proc` thread information. */
export const procAvailable = existsSync("/proc/thread-self") || existsSync(TASK_DIR)

const DEFAULT_TIMEOUT_MS = 3000

/** The OS thread name of `tid`, or `undefined` if the thread is gone. */
export function threadName(tid: number): string | undefined {
  try {
    return readFileSync(`${TASK_DIR}/${tid}/comm`, "utf8").trim()
  } catch {
    return undefined
  }
}

/** Every thread name currently visible for this process, race-safely. */
export function threadNames(): string[] {
  let tids: string[]
  try {
    tids = readdirSync(TASK_DIR)
  } catch {
    return []
  }
  const names: string[] = []
  for (const tid of tids) {
    try {
      names.push(readFileSync(`${TASK_DIR}/${tid}/comm`, "utf8").trim())
    } catch {
      // The thread exited between listing and reading; skip it.
    }
  }
  return names
}

/** Resolve once a thread with `name` exists anywhere in the process. */
export function waitForAnyThreadNamed(name: string, timeoutMs: number = DEFAULT_TIMEOUT_MS): Promise<void> {
  return pollUntil(() => threadNames().includes(name), {
    timeoutMs,
    message: `no thread named "${name}" appeared in ${TASK_DIR}; saw ${JSON.stringify(threadNames())}`,
  })
}

/** Resolve once thread `tid`'s OS name is exactly `name`. */
export function waitForThreadNamed(tid: number, name: string, timeoutMs: number = DEFAULT_TIMEOUT_MS): Promise<void> {
  return pollUntil(() => threadName(tid) === name, {
    timeoutMs,
    message: `thread ${tid} never became "${name}"; saw "${threadName(tid)}"`,
  })
}
