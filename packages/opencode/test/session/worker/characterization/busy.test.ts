// Characterization: one active run per session.
//
// `SessionRunMachine` is the portable run-state table used both in the hub
// (flag off) and inside a session worker (flag on). The refactor to one worker
// per session must preserve its observable contract:
//   * two concurrent `ensureRunning` calls for the SAME session join one work
//     execution (no duplicate turn);
//   * `assertNotBusy` rejects with `SessionBusyError` while that run is active;
//   * the session becomes idle again once the run settles.
//
// Synchronization uses promise gates, not sleeps.

import { describe, expect, test } from "bun:test"
import { Effect, Exit, Scope } from "effect"
import { createSessionRunMachine, SessionBusyError } from "@/session/worker/execute"
import { sid } from "../../../lib/async"
import { withParts } from "../../../lib/session-parts"

/** Run `body` against a fresh scope, closing it so forked fibers never leak. */
const withScope = <A>(body: (scope: Scope.Scope) => Promise<A>): Promise<A> =>
  Effect.gen(function* () {
    const scope = yield* Effect.acquireRelease(Scope.make(), (scope) => Scope.close(scope, Exit.void))
    return yield* Effect.promise(() => body(scope))
  }).pipe(Effect.scoped, Effect.runPromise)

/** A promise gate plus a one-shot "started" signal. */
function gate() {
  let open!: () => void
  const opened = new Promise<void>((resolve) => {
    open = resolve
  })
  let started!: () => void
  const startedPromise = new Promise<void>((resolve) => {
    started = resolve
  })
  return { opened, open, startedPromise, started }
}

describe("one active run per session", () => {
  test("concurrent ensureRunning joins one execution and assertNotBusy rejects while busy", () =>
    withScope(async (scope) => {
      const machine = createSessionRunMachine({ scope })
      const sessionID = sid("ses_one")
      const g = gate()
      let executions = 0

      const work = Effect.gen(function* () {
        executions += 1
        yield* Effect.sync(() => g.started())
        yield* Effect.promise(() => g.opened)
        return { sessionID, value: withParts(sessionID) }
      })

      const first = Effect.runPromise(machine.ensureRunning(sessionID, work))
      const second = Effect.runPromise(machine.ensureRunning(sessionID, work))
      await g.startedPromise

      expect(executions).toBe(1)
      expect(machine.busy(sessionID)).toBe(true)
      await expect(Effect.runPromise(machine.assertNotBusy(sessionID))).rejects.toBeInstanceOf(SessionBusyError)

      g.open()
      const [a, b] = await Promise.all([first, second])

      expect(a).toEqual(b)
      expect(executions).toBe(1)
      expect(machine.busy(sessionID)).toBe(false)
      await Effect.runPromise(machine.assertNotBusy(sessionID))
    }))
})
