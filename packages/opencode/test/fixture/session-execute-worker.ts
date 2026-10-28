// Phase 5 — session-executor worker fixture.
//
// A dependency-light worker entry that installs the Phase 5 executor with a
// controllable work factory. The hub sends only a work *kind* (a string), never
// a closure. Durations let the tests observe the hub loop staying idle and two
// sessions running concurrently on two isolates.

import { threadId } from "node:worker_threads"
import { Effect } from "effect"
import {
  installSessionExecutor,
  type SessionExecutionResult,
  type SessionWork,
  type SessionWorkFactory,
} from "@/session/worker/execute"
import { scope } from "./worker-scope"

const DURATIONS: Record<string, number> = {
  instant: 0,
  noop: 10,
  slow: 200,
  // Long enough that a main-thread timer has an unambiguous window to fire even
  // on a heavily oversubscribed host (event-loop liveness test).
  verySlow: 2_000,
}

let executions = 0

const work: SessionWorkFactory = (kind) => {
  const delayMs = DURATIONS[kind]
  if (delayMs === undefined) throw new Error(`unknown work kind: ${kind}`)
  const run: SessionWork = (sessionID) =>
    Effect.gen(function* () {
      if (delayMs > 0) yield* Effect.sleep(`${delayMs} millis`)
      executions += 1
      return { sessionID, value: { execution: executions, kind } } satisfies SessionExecutionResult
    })
  return run
}

installSessionExecutor(scope, { work, threadId })
