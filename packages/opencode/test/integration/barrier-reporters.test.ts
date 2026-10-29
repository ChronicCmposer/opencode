// The run barrier must observe all four commit-side phases. A started run is
// not evidence that commits, projectors, tools, or the promotion decision
// actually settled.
//
// These tests wire the lifecycle ledger (the real tool/commit observation
// surface) to the real `RunBarrier` service and prove:
//   - every phase must be reported by its own observation,
//   - a missing reporter is not fabricated (typed timeout, next run refused),
//   - a zero-tool run settles `tools` only through an explicit `noTools()`,
//   - a real tool report settles `tools`,
//   - cancellation still wins over an incomplete report set.

import { describe, expect } from "bun:test"
import { Cause, Effect, Exit, Fiber } from "effect"
import { createLifecycleTracker } from "@/session/ledger/lifecycle"
import {
  RunBarrier,
  RunBarrierReportError,
  RunBarrierTimeout,
  type RunBarrierPromotionOutcome,
} from "@/session/run-barrier"
import { testEffect } from "../lib/effect"
import { advanceBarrierTimeout, runBarrierLayer } from "../lib/run-barrier-test"

const it = testEffect(runBarrierLayer)

type Lifecycle = ReturnType<typeof createLifecycleTracker>

/**
 * Mirrors the production wiring a hub uses: reports are emitted where the real
 * observation lands (commit handler, tool finish, projector settle), never from
 * the fact that the run started.
 */
const createObservedRun = Effect.fn("test.createObservedRun")(function* (
  barrier: RunBarrier.Interface,
  lifecycle: Lifecycle,
  sessionID: string,
) {
  const run = yield* barrier.beginRun(sessionID)
  return {
    run,
    commit(messageID: string) {
      lifecycle.commit(sessionID, messageID)
      run.commit(messageID)
    },
    projector(projector = "session-row") {
      run.projector(projector)
    },
    toolStarted(callID: string) {
      lifecycle.toolStarted(sessionID, callID)
    },
    toolFinished(callID: string) {
      lifecycle.toolFinished(sessionID, callID)
      run.tool(callID)
    },
    /**
     * The run loop finished: the promotion decision is known, and an empty tool
     * ledger is settled deliberately. A tool that started but never finished
     * leaves the `tools` phase unreported, so the barrier waits.
     */
    finish(outcome: RunBarrierPromotionOutcome = "no-job") {
      if (run.missing().includes("tools") && lifecycle.indeterminate(sessionID).length === 0) run.noTools()
      run.promotion(outcome)
    },
  }
})

/** Fork a wait, advance the test clock past the bound, and return its exit. */
const timeoutWait = (barrier: RunBarrier.Interface, sessionID: string) =>
  Effect.gen(function* () {
    const fiber = yield* barrier.wait(sessionID).pipe(Effect.exit, Effect.forkChild)
    yield* Effect.yieldNow
    yield* advanceBarrierTimeout()
    return yield* Fiber.join(fiber)
  })

describe("run barrier phase reporters", () => {
  it.effect("a full zero-tool run reports every phase and drains", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const lifecycle = createLifecycleTracker()
      const observed = yield* createObservedRun(barrier, lifecycle, "ses_reported")

      observed.commit("msg_1")
      observed.projector()
      observed.finish()

      expect(yield* barrier.wait("ses_reported")).toEqual({ sessionID: "ses_reported", status: "drained" })
      expect(barrier.state("ses_reported")).toBe("drained")
    }),
  )

  it.effect("a missing projector reporter is not inferred from the commit report", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const lifecycle = createLifecycleTracker()
      const observed = yield* createObservedRun(barrier, lifecycle, "ses_no_projector")

      observed.commit("msg_1")
      observed.finish()

      const exit = yield* timeoutWait(barrier, "ses_no_projector")
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBeInstanceOf(RunBarrierTimeout)
      expect(barrier.state("ses_no_projector")).toBe("timed-out")
      // No next run after a fabricated drain: the barrier stays refused.
      const refused = yield* barrier.wait("ses_no_projector").pipe(Effect.flip)
      expect(refused).toBeInstanceOf(RunBarrierTimeout)
    }),
  )

  it.effect("a started tool with no settle report holds the barrier", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const lifecycle = createLifecycleTracker()
      const observed = yield* createObservedRun(barrier, lifecycle, "ses_tool_stuck")

      observed.toolStarted("call_write")
      observed.commit("msg_1")
      observed.projector()
      observed.finish()

      const exit = yield* timeoutWait(barrier, "ses_tool_stuck")
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBeInstanceOf(RunBarrierTimeout)
      expect(barrier.state("ses_tool_stuck")).toBe("timed-out")
    }),
  )

  it.effect("a real tool settlement report drains a run with tools", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const lifecycle = createLifecycleTracker()
      const observed = yield* createObservedRun(barrier, lifecycle, "ses_tool_real")

      observed.toolStarted("call_write")
      observed.commit("msg_1")
      observed.projector()
      observed.toolFinished("call_write")
      observed.finish()

      expect(yield* barrier.wait("ses_tool_real")).toEqual({ sessionID: "ses_tool_real", status: "drained" })
      expect(observed.run.reported("tools")).toBe(true)
    }),
  )

  it.effect("a report for the wrong observation cannot stand in for a projector", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const lifecycle = createLifecycleTracker()
      const observed = yield* createObservedRun(barrier, lifecycle, "ses_wrong_kind")

      observed.commit("msg_1")
      expect(() =>
        observed.run.report({
          sessionID: "ses_wrong_kind",
          phase: "projectors",
          observation: { kind: "commit", messageID: "msg_1" },
        }),
      ).toThrow(RunBarrierReportError)

      const error = yield* barrier.wait("ses_wrong_kind").pipe(Effect.flip)
      expect(error).toBeInstanceOf(RunBarrierReportError)
      expect(barrier.state("ses_wrong_kind")).toBe("failed")
    }),
  )

  it.effect("cancellation wins while a reporter is still missing", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const lifecycle = createLifecycleTracker()
      const observed = yield* createObservedRun(barrier, lifecycle, "ses_cancel_missing")

      observed.commit("msg_1")
      // The projector was never reported, so the run is still open.
      observed.finish()

      const pending = yield* barrier.wait("ses_cancel_missing").pipe(Effect.forkChild)
      yield* Effect.yieldNow
      yield* barrier.cancel("ses_cancel_missing")

      expect(yield* Fiber.join(pending)).toEqual({ sessionID: "ses_cancel_missing", status: "cancelled" })
      expect(barrier.state("ses_cancel_missing")).toBe("cancelled")
      yield* advanceBarrierTimeout()
      expect(barrier.state("ses_cancel_missing")).toBe("cancelled")
    }),
  )
})
