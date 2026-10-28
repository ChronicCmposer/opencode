// The run barrier must observe all four commit-side phases. A started run is
// not evidence that commits, projectors, tools, or the promotion decision
// actually settled.
//
// These tests wire the lifecycle ledger (the real tool/commit observation
// surface) to a per-run reporter and prove:
//   - every phase must be reported by its own observation,
//   - a missing reporter is not fabricated (typed timeout, next run refused),
//   - a zero-tool run settles `tools` only through an explicit `noTools()`,
//   - a real tool report settles `tools`,
//   - cancellation still wins over an incomplete report set.

import { describe, expect, test } from "bun:test"
import { createLifecycleTracker } from "@/session/ledger/lifecycle"
import {
  RUN_BARRIER_TIMEOUT_MS,
  RunBarrierReportError,
  RunBarrierTimeout,
  createRunBarrier,
  type RunBarrier,
  type RunBarrierPromotionOutcome,
  type RunBarrierScheduler,
} from "@/session/run-barrier"

function fakeClock() {
  let now = 0
  let nextID = 1
  const timers = new Map<number, { readonly at: number; readonly run: () => void }>()
  return {
    scheduler: {
      schedule: (run: () => void, delayMs: number) => {
        const id = nextID++
        timers.set(id, { at: now + delayMs, run })
        return id
      },
      cancel: (handle: unknown) => void timers.delete(handle as number),
    } satisfies RunBarrierScheduler,
    advance(ms: number) {
      now += ms
      for (const [id, timer] of [...timers.entries()]) {
        if (timer.at <= now) {
          timers.delete(id)
          timer.run()
        }
      }
    },
  }
}

type Lifecycle = ReturnType<typeof createLifecycleTracker>

/**
 * Mirrors the production wiring a hub uses: reports are emitted where the real
 * observation lands (commit handler, tool finish, projector settle), never from
 * the fact that the run started.
 */
function createObservedRun(barrier: RunBarrier, lifecycle: Lifecycle, sessionID: string) {
  const run = barrier.beginRun(sessionID)
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
}

describe("run barrier phase reporters", () => {
  test("a full zero-tool run reports every phase and drains", async () => {
    const clock = fakeClock()
    const lifecycle = createLifecycleTracker()
    const barrier = createRunBarrier({ scheduler: clock.scheduler })
    const observed = createObservedRun(barrier, lifecycle, "ses_reported")

    observed.commit("msg_1")
    observed.projector()
    observed.finish()

    expect(await barrier.wait("ses_reported")).toEqual({ sessionID: "ses_reported", status: "drained" })
    expect(barrier.state("ses_reported")).toBe("drained")
  })

  test("a missing projector reporter is not inferred from the commit report", async () => {
    const clock = fakeClock()
    const lifecycle = createLifecycleTracker()
    const barrier = createRunBarrier({ scheduler: clock.scheduler })
    const observed = createObservedRun(barrier, lifecycle, "ses_no_projector")

    observed.commit("msg_1")
    observed.finish()

    const pending = barrier.wait("ses_no_projector")
    clock.advance(RUN_BARRIER_TIMEOUT_MS)

    await expect(pending).rejects.toBeInstanceOf(RunBarrierTimeout)
    expect(barrier.state("ses_no_projector")).toBe("timed-out")
    // No next run after a fabricated drain: the barrier stays refused.
    await expect(barrier.wait("ses_no_projector")).rejects.toBeInstanceOf(RunBarrierTimeout)
  })

  test("a started tool with no settle report holds the barrier", async () => {
    const clock = fakeClock()
    const lifecycle = createLifecycleTracker()
    const barrier = createRunBarrier({ scheduler: clock.scheduler })
    const observed = createObservedRun(barrier, lifecycle, "ses_tool_stuck")

    observed.toolStarted("call_write")
    observed.commit("msg_1")
    observed.projector()
    observed.finish()

    const pending = barrier.wait("ses_tool_stuck")
    clock.advance(RUN_BARRIER_TIMEOUT_MS)
    await expect(pending).rejects.toBeInstanceOf(RunBarrierTimeout)
    expect(barrier.state("ses_tool_stuck")).toBe("timed-out")
  })

  test("a real tool settlement report drains a run with tools", async () => {
    const clock = fakeClock()
    const lifecycle = createLifecycleTracker()
    const barrier = createRunBarrier({ scheduler: clock.scheduler })
    const observed = createObservedRun(barrier, lifecycle, "ses_tool_real")

    observed.toolStarted("call_write")
    observed.commit("msg_1")
    observed.projector()
    observed.toolFinished("call_write")
    observed.finish()

    expect(await barrier.wait("ses_tool_real")).toEqual({ sessionID: "ses_tool_real", status: "drained" })
    expect(observed.run.reported("tools")).toBe(true)
  })

  test("a report for the wrong observation cannot stand in for a projector", async () => {
    const clock = fakeClock()
    const lifecycle = createLifecycleTracker()
    const barrier = createRunBarrier({ scheduler: clock.scheduler })
    const observed = createObservedRun(barrier, lifecycle, "ses_wrong_kind")

    observed.commit("msg_1")
    expect(() =>
      observed.run.report({
        sessionID: "ses_wrong_kind",
        phase: "projectors",
        observation: { kind: "commit", messageID: "msg_1" },
      }),
    ).toThrow(RunBarrierReportError)

    const pending = barrier.wait("ses_wrong_kind")
    clock.advance(RUN_BARRIER_TIMEOUT_MS)
    await expect(pending).rejects.toBeInstanceOf(RunBarrierReportError)
    expect(barrier.state("ses_wrong_kind")).toBe("failed")
  })

  test("cancellation wins while a reporter is still missing", async () => {
    const clock = fakeClock()
    const lifecycle = createLifecycleTracker()
    const barrier = createRunBarrier({ scheduler: clock.scheduler })
    const observed = createObservedRun(barrier, lifecycle, "ses_cancel_missing")

    observed.commit("msg_1")
    observed.finish()

    const pending = barrier.wait("ses_cancel_missing")
    barrier.cancel("ses_cancel_missing")

    expect(await pending).toEqual({ sessionID: "ses_cancel_missing", status: "cancelled" })
    expect(barrier.state("ses_cancel_missing")).toBe("cancelled")
    clock.advance(RUN_BARRIER_TIMEOUT_MS)
    expect(barrier.state("ses_cancel_missing")).toBe("cancelled")
  })
})
