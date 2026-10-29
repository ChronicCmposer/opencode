import { describe, expect } from "bun:test"
import { Cause, Effect, Exit, Fiber } from "effect"
import { adjust as adjustTestClock } from "effect/testing/TestClock"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"

import {
  RUN_BARRIER_PHASES,
  RUN_BARRIER_TIMEOUT_MS,
  RunBarrier,
  RunBarrierBusyError,
  RunBarrierMissingReportError,
  RunBarrierReportError,
  RunBarrierTimeout,
} from "@/session/run-barrier"
import { testEffect } from "../../lib/effect"

const it = testEffect(LayerNode.compile(LayerNode.group([RunBarrier.node])))

function reportAll(run: RunBarrier.RunBarrierRun, skip?: RunBarrier.RunBarrierPhase): void {
  if (skip !== "commits") run.commit("msg_1")
  if (skip !== "projectors") run.projector("session-row")
  if (skip !== "tools") run.noTools()
  if (skip !== "promotion") run.promotion("no-job")
}

/** Fork a wait, advance the test clock past the bound, and return its exit. */
const timeoutWait = (barrier: RunBarrier.Interface, sessionID: string) =>
  Effect.gen(function* () {
    const fiber = yield* barrier.wait(sessionID).pipe(Effect.exit, Effect.forkChild)
    yield* Effect.yieldNow
    yield* adjustTestClock(RUN_BARRIER_TIMEOUT_MS)
    return yield* Fiber.join(fiber)
  })

describe("per-Session run barrier — real phase reports", () => {
  it.effect("drains only after commits, projectors, tools, and promotion are reported", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const run = barrier.beginRun("s1")
      run.commit("msg_1")
      run.projector("session-row")
      run.promotion("no-job")

      const fiber = yield* barrier.wait("s1").pipe(Effect.forkChild)
      yield* Effect.yieldNow
      expect(barrier.state("s1")).toBe("draining")

      run.noTools()
      expect(yield* Fiber.join(fiber)).toEqual({ sessionID: "s1", status: "drained" })
      expect(barrier.state("s1")).toBe("drained")
    }),
  )

  it.effect("admits the next run after the previous barrier drained", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const first = barrier.beginRun("s1")
      reportAll(first)
      expect(yield* barrier.wait("s1")).toEqual({ sessionID: "s1", status: "drained" })

      const second = barrier.beginRun("s1")
      reportAll(second)
      expect(yield* barrier.wait("s1")).toEqual({ sessionID: "s1", status: "drained" })
      expect(barrier.state("s1")).toBe("drained")
    }),
  )

  it.effect("a missing reporter is never fabricated into a settlement", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const run = barrier.beginRun("s1")
      reportAll(run, "tools")

      const exit = yield* timeoutWait(barrier, "s1")
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBeInstanceOf(RunBarrierTimeout)
      expect(barrier.state("s1")).toBe("timed-out")

      // The next run is refused: a started run is not proof that tools settled.
      const next = yield* barrier.wait("s1").pipe(Effect.flip)
      expect(next).toBeInstanceOf(RunBarrierTimeout)
    }),
  )

  for (const missing of RUN_BARRIER_PHASES) {
    it.effect(`refuses a run whose ${missing} reporter never arrived`, () =>
      Effect.gen(function* () {
        const barrier = yield* RunBarrier.Service
        const sessionID = `ses_missing_${missing}`
        const run = barrier.beginRun(sessionID)
        reportAll(run, missing)

        const exit = yield* timeoutWait(barrier, sessionID)
        expect(Exit.isFailure(exit)).toBe(true)
        if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBeInstanceOf(RunBarrierTimeout)
        expect(barrier.state(sessionID)).toBe("timed-out")
      }),
    )
  }

  it.effect("a zero-tool run settles tools only through an explicit noTools report", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service

      const abandoned = barrier.beginRun("s1")
      reportAll(abandoned, "tools")
      const timedOut = yield* timeoutWait(barrier, "s1")
      expect(Exit.isFailure(timedOut)).toBe(true)
      barrier.reset("s1")

      const run = barrier.beginRun("s1")
      run.commit("msg_1")
      run.projector("session-row")
      run.promotion("no-job")
      run.noTools()
      expect(yield* barrier.wait("s1")).toEqual({ sessionID: "s1", status: "drained" })
    }),
  )

  it.effect("a no-commit run settles commits and projectors explicitly", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const run = barrier.beginRun("s1")
      run.noCommit()
      run.noTools()
      run.promotion("no-job")

      expect(run.reported("commits")).toBe(true)
      expect(run.reported("projectors")).toBe(true)
      expect(run.commitOutcome()).toEqual({ committed: null })
      expect(yield* barrier.wait("s1")).toEqual({ sessionID: "s1", status: "drained" })
    }),
  )

  it.effect("a real commit records its message id as the commit outcome", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const run = barrier.beginRun("s1")
      run.commit("msg_42")
      expect(run.commitOutcome()).toEqual({ committed: "msg_42" })
    }),
  )

  it.effect("a no-commit report cannot overwrite a real commit report", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const run = barrier.beginRun("s1")
      run.commit("msg_1")
      run.projector("session-row")

      expect(() => run.noCommit()).toThrow(RunBarrierReportError)
      // The conflicting observation fails loud instead of draining a broken run.
      const error = yield* barrier.wait("s1").pipe(Effect.flip)
      expect(error).toBeInstanceOf(RunBarrierReportError)
    }),
  )

  it.effect("a none observation cannot settle a phase that requires a real observation", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const run = barrier.beginRun("s1")

      expect(() => run.report({ sessionID: "s1", phase: "tools", observation: { kind: "none" } })).toThrow(
        RunBarrierReportError,
      )
    }),
  )

  it.effect("a run with an in-flight tool waits for the tool report, not for run start", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const run = barrier.beginRun("s1")
      run.commit("msg_1")
      run.projector("session-row")
      run.promotion("no-job")

      const fiber = yield* barrier.wait("s1").pipe(Effect.forkChild)
      yield* Effect.yieldNow
      expect(barrier.state("s1")).toBe("draining")

      run.tool("call_write")
      expect(yield* Fiber.join(fiber)).toEqual({ sessionID: "s1", status: "drained" })
    }),
  )

  it.effect("waiting without a begun run is a typed refusal", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const error = yield* barrier.wait("s1").pipe(Effect.flip)
      expect(error).toBeInstanceOf(RunBarrierMissingReportError)
      expect(barrier.state("s1")).toBe("failed")
    }),
  )

  it.effect("a report for the wrong phase or observation is refused", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const run = barrier.beginRun("s1")

      expect(() =>
        run.report({ sessionID: "s1", phase: "projectors", observation: { kind: "commit", messageID: "msg_1" } }),
      ).toThrow(RunBarrierReportError)

      const error = yield* barrier.wait("s1").pipe(Effect.flip)
      expect(error).toBeInstanceOf(RunBarrierReportError)
      expect(barrier.state("s1")).toBe("failed")
    }),
  )

  it.effect("a report for another session is refused", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const run = barrier.beginRun("s1")
      expect(() =>
        run.report({ sessionID: "s2", phase: "commits", observation: { kind: "commit", messageID: "msg_1" } }),
      ).toThrow(RunBarrierReportError)
    }),
  )

  it.effect("a zero-tool and a real tool report cannot both settle the same run", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const run = barrier.beginRun("s1")
      run.noTools()
      expect(() => run.tool("call_after_no_tools")).toThrow(RunBarrierReportError)
    }),
  )

  it.effect("a stale run token cannot settle the next run", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const first = barrier.beginRun("s1")
      reportAll(first)
      expect(yield* barrier.wait("s1")).toEqual({ sessionID: "s1", status: "drained" })

      const second = barrier.beginRun("s1")
      expect(first.commit("late_msg")).toBe(false)

      const exit = yield* timeoutWait(barrier, "s1")
      expect(Exit.isFailure(exit)).toBe(true)
      expect(second.commit("msg_2")).toBe(true)
    }),
  )

  it.effect("cancellation wins while the report set is incomplete", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const run = barrier.beginRun("s1")
      run.commit("msg_1")

      const fiber = yield* barrier.wait("s1").pipe(Effect.forkChild)
      yield* Effect.yieldNow
      barrier.cancel("s1")

      expect(yield* Fiber.join(fiber)).toEqual({ sessionID: "s1", status: "cancelled" })
      expect(barrier.state("s1")).toBe("cancelled")
    }),
  )

  it.effect("a begun run cannot overlap a still-draining run", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const run = barrier.beginRun("s1")
      expect(run.missing()).toEqual([...RUN_BARRIER_PHASES])

      const fiber = yield* barrier.wait("s1").pipe(Effect.forkChild)
      yield* Effect.yieldNow
      expect(() => barrier.beginRun("s1")).toThrow(RunBarrierBusyError)

      barrier.cancel("s1")
      expect(yield* Fiber.join(fiber)).toEqual({ sessionID: "s1", status: "cancelled" })
    }),
  )

  it.effect("abandon re-admits a Session whose timed-out owner is gone", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const run = barrier.beginRun("s1")
      reportAll(run, "promotion")
      const exit = yield* timeoutWait(barrier, "s1")
      expect(Exit.isFailure(exit)).toBe(true)
      expect(barrier.state("s1")).toBe("timed-out")

      barrier.abandon("s1")
      expect(barrier.state("s1")).toBe("idle")

      const next = barrier.beginRun("s1")
      reportAll(next)
      expect(yield* barrier.wait("s1")).toEqual({ sessionID: "s1", status: "drained" })
      // Idempotent: a second recovery of the same dead barrier is harmless.
      barrier.abandon("s1")
    }),
  )

  it.effect("abandon refuses a barrier that is still draining", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      barrier.beginRun("s1")
      const fiber = yield* barrier.wait("s1").pipe(Effect.forkChild)
      yield* Effect.yieldNow

      expect(() => barrier.abandon("s1")).toThrow(RunBarrierBusyError)
      expect(barrier.state("s1")).toBe("draining")

      barrier.cancel("s1")
      expect(yield* Fiber.join(fiber)).toEqual({ sessionID: "s1", status: "cancelled" })
    }),
  )

  it.effect("reset re-admits a Session after a timeout", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const run = barrier.beginRun("s1")
      reportAll(run, "promotion")
      const exit = yield* timeoutWait(barrier, "s1")
      expect(Exit.isFailure(exit)).toBe(true)

      barrier.reset("s1")
      expect(barrier.state("s1")).toBe("idle")

      const next = barrier.beginRun("s1")
      reportAll(next)
      expect(yield* barrier.wait("s1")).toEqual({ sessionID: "s1", status: "drained" })
    }),
  )

  it.effect("a timed-out Session does not block another Session's barrier", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      barrier.beginRun("s1")
      const stuck = yield* barrier.wait("s1").pipe(Effect.exit, Effect.forkChild)

      const other = barrier.beginRun("s2")
      reportAll(other)
      expect(yield* barrier.wait("s2")).toEqual({ sessionID: "s2", status: "drained" })

      yield* adjustTestClock(RUN_BARRIER_TIMEOUT_MS)
      expect(Exit.isFailure(yield* Fiber.join(stuck))).toBe(true)
      expect(barrier.state("s2")).toBe("drained")
    }),
  )
})

describe("run barrier service", () => {
  it.effect("node exposes the unbound RunBarrier service tag", () =>
    Effect.sync(() => {
      const entry = RunBarrier.node
      expect(entry.kind).toBe("layer")
      expect(entry.name).toBe("@opencode/RunBarrier")
      expect(entry.service).toBe(RunBarrier.Service)
      expect(entry.dependencies).toEqual([])
    }),
  )

  it.effect("the service reports through the real layer closure", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const run = barrier.beginRun("s1")
      expect(run.commit("msg_1")).toBe(true)
      expect(run.projector("session-row")).toBe(true)
      expect(run.noTools()).toBe(true)
      expect(run.promotion("no-job")).toBe(true)

      expect(yield* barrier.wait("s1")).toEqual({ sessionID: "s1", status: "drained" })
      expect(barrier.state("s1")).toBe("drained")
      expect(barrier.size).toBe(1)
    }),
  )
})
