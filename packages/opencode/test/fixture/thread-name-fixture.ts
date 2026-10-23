// WS5 — session-worker thread-name lifecycle fixture.
//
// Boots the frozen Phase 5 executor with a trivial work kind and mirrors the
// production entry's dynamic thread naming:
//
//   boot       `ocs<slot>`
//   run start  `ocs<slot>:<last 6 of sessionID>`  (executor `onRunStart`)
//   terminal   `ocs<slot>`                        (executor `onIdle`: settle,
//                                                  cooperative cancel, interrupt)
//
// `workerData.workerID` carries the pool slot (defaulting to 1) so headroom
// workers, which share this entry, name themselves with their own slot. The
// `ready` frame reports this thread's REAL OS tid (not `node:worker_threads`'
// logical id), so the test can read `/proc/self/task/<tid>/comm` directly.

import { workerData } from "node:worker_threads"
import { Effect } from "effect"
import {
  DEFAULT_WORK_KIND,
  installSessionExecutor,
  type SessionExecutionResult,
  type SessionWork,
  type SessionWorkFactory,
} from "@/session/worker/execute"
import { sessionThreadName } from "@/session/worker/thread-name"
import { setThreadName } from "@/util/thread-name"
import { currentThreadId } from "../lib/loop-gap"

interface WorkerScope {
  onmessage: ((event: { data: unknown }) => void) | null
  postMessage(message: unknown, transfer?: readonly Transferable[]): void
}

const scope = globalThis as unknown as WorkerScope

/** The pool slot this worker serves; `workerData.workerID` when the pool shipped one. */
const slot = (workerData as { readonly workerID?: number } | null | undefined)?.workerID ?? 1

/**
 * A work kind whose run is long enough to observe the running name on the
 * thread. It sleeps (interruptible, so a cooperative cancel can cut it short),
 * so the test can poll `/proc` mid-run without racing the settle.
 */
const RUN_MS = 250

const work: SessionWorkFactory = (kind) => {
  if (kind !== DEFAULT_WORK_KIND) throw new Error(`unknown work kind: ${kind}`)
  const run: SessionWork = (sessionID) =>
    Effect.gen(function* () {
      yield* Effect.sleep(`${RUN_MS} millis`)
      return { sessionID, value: { ok: true } } satisfies SessionExecutionResult
    })
  return run
}

// Boot the idle name before announcing `ready`, exactly as the production entry
// names its slot at module load.
setThreadName(sessionThreadName(slot))

installSessionExecutor(scope, {
  work,
  threadId: currentThreadId() ?? 0,
  onRunStart: (sessionID) => setThreadName(sessionThreadName(slot, sessionID)),
  // `onIdle` fires on every terminal (settle, cooperative cancel, interrupt), so
  // the thread always returns to its idle slot name.
  onIdle: () => Effect.sync(() => setThreadName(sessionThreadName(slot))),
})
