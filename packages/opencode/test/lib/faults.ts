// Test-facing entry point for the `OPENCODE_TEST_FAULTS` fault-injection seam
//
// The seam itself lives in `src/testing/faults.ts`: production call sites
// (writer, pool) import it directly, it is gated by `OPENCODE_TEST_FAULTS`, and
// it is inert unless armed. This module is the single test-side import:
//
//   - it re-exports the seam, so tests never reach across into `src/`, and
//   - it pins the crash-worker fixture path and offers `withFaults`, a scoped
//     "arm the gate for this block, always restore it" helper.

import path from "path"
import { FAULTS_ENV } from "@/testing/faults"

export * from "@/testing/faults"

/** Absolute path to the worker fixture that exercises `crashWorker`. */
export const faultWorkerEntry = path.join(import.meta.dir, "../fixture/fault-worker.ts")

/** Set (or clear, when `undefined`) the fault gate for the current process. */
export function serveFaults(spec: string | undefined): void {
  if (spec === undefined) delete process.env[FAULTS_ENV]
  else process.env[FAULTS_ENV] = spec
}

/**
 * Run `body` with the fault gate armed to `spec`, restoring the previous value
 * afterwards even if `body` throws. The scoped counterpart to `serveFaults`.
 */
export async function withFaults<T>(spec: string | undefined, body: () => T | Promise<T>): Promise<T> {
  const previous = process.env[FAULTS_ENV]
  serveFaults(spec)
  try {
    return await body()
  } finally {
    serveFaults(previous)
  }
}
