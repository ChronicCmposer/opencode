// A refused barrier report is a real failure, not a timeout. The caller must see
// the typed report error and the state must stay distinct from `timed-out`.

import { describe, expect } from "bun:test"
import { Cause, Effect, Exit, Fiber } from "effect"
import { adjust as adjustTestClock } from "effect/testing/TestClock"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"

import { RUN_BARRIER_TIMEOUT_MS, RunBarrier, RunBarrierReportError, RunBarrierTimeout } from "@/session/run-barrier"
import { testEffect } from "../../lib/effect"

const it = testEffect(LayerNode.compile(LayerNode.group([RunBarrier.node])))

describe("distinct barrier failure state (contained)", () => {
  it.effect("a refused report surfaces the typed error and a `failed` state", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const run = yield* barrier.beginRun("s1")

      expect(() =>
        run.report({ sessionID: "s1", phase: "projectors", observation: { kind: "commit", messageID: "msg_1" } }),
      ).toThrow(RunBarrierReportError)

      const error = yield* barrier.wait("s1").pipe(Effect.flip)
      expect(error).toBeInstanceOf(RunBarrierReportError)
      expect(error).not.toBeInstanceOf(RunBarrierTimeout)
      expect(barrier.state("s1")).toBe("failed")
    }),
  )

  it.effect("a timed-out barrier keeps the timeout state", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const run = yield* barrier.beginRun("s2")
      run.commit("msg_1")
      run.projector("session-row")
      run.noTools()
      // promotion is missing, so the barrier stays open until the bound.

      const waiting = yield* barrier.wait("s2").pipe(Effect.exit, Effect.forkChild)
      yield* Effect.yieldNow
      yield* adjustTestClock(RUN_BARRIER_TIMEOUT_MS)
      const exit = yield* Fiber.join(waiting)

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBeInstanceOf(RunBarrierTimeout)
      expect(barrier.state("s2")).toBe("timed-out")
    }),
  )
})
