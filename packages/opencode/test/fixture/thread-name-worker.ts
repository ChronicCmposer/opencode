// Session-worker thread-name lifecycle fixture.
//
// Boots the frozen executor with a trivial work kind and mirrors the production
// entry's lifecycle through the injectable `createThreadNamer` seam:
//
//   boot/bind  `ocs:<last 6 of sessionID>`
//   run start  `ocs:<last 6 of sessionID>!`  (executor `onRunStart`)
//   terminal   `ocs:<last 6 of sessionID>`   (executor `onIdle`: settle,
//                                             cooperative cancel, interrupt)
//
// `workerData.sessionID` carries the bound session. When absent, a stable
// fixture id is synthesized from the pool slot so a direct boot stays
// observable. The `ready` frame reports this thread's REAL OS tid (not
// `node:worker_threads`' logical id), so a test can read
// `/proc/self/task/<tid>/comm` directly.

import { workerData } from "node:worker_threads"
import { Effect } from "effect"
import {
  DEFAULT_WORK_KIND,
  installSessionExecutor,
  type SessionExecutionResult,
  type SessionWork,
  type SessionWorkFactory,
} from "@/session/worker/execute"
import { createThreadNamer } from "@/session/worker/thread-naming"
import { setThreadName } from "@/util/thread-name"
import { currentThreadId } from "../lib/loop-gap"
import { scope } from "./worker-scope"

/** The pool slot this worker serves, used only to synthesize a fixture session id. */
const slot = (workerData as { readonly workerID?: number } | null | undefined)?.workerID ?? 1
const sessionID =
  (workerData as { readonly sessionID?: string } | null | undefined)?.sessionID ?? `ses_fixture_${slot}`

/**
 * A work kind whose run is long enough to observe the running name on the
 * thread. It sleeps (interruptible, so a cooperative cancel can cut it short),
 * so the test can poll `/proc` mid-run without racing the settle.
 */
const RUN_MS = 250

const work: SessionWorkFactory = (kind) => {
  if (kind !== DEFAULT_WORK_KIND) throw new Error(`unknown work kind: ${kind}`)
  const run: SessionWork = (runSessionID) =>
    Effect.gen(function* () {
      yield* Effect.sleep(`${RUN_MS} millis`)
      return { sessionID: runSessionID, value: { ok: true } } satisfies SessionExecutionResult
    })
  return run
}

// Bind and advertise the idle name before announcing `ready`, exactly as the
// production entry does from `init.sessionID`.
const namer = createThreadNamer({ setName: setThreadName })
namer.onBind(sessionID)

installSessionExecutor(scope, {
  work,
  threadId: currentThreadId() ?? 0,
  onRunStart: (runSessionID) => namer.onRunStart(runSessionID),
  onIdle: () => Effect.sync(() => namer.onIdle()),
})
