// The service contract: cancellation wins, the bound is typed, a missing run is
// refused typed, a zero-commit run records `{ committed: null }`, and the session
// map is bounded.

import { describe, expect } from "bun:test"
import { Cause, Effect, Exit, Fiber } from "effect"
import * as TestClock from "effect/testing/TestClock"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"

import {
  RUN_BARRIER_MAX_SESSIONS,
  RUN_BARRIER_TIMEOUT_MS,
  RunBarrier,
  RunBarrierMissingReportError,
  RunBarrierTimeout,
} from "@/session/run-barrier"
import { testEffect } from "../../lib/effect"

const it = testEffect(LayerNode.compile(LayerNode.group([RunBarrier.node])))

function drain(barrier: RunBarrier.Interface, sessionID: string) {
  return Effect.suspend(() => {
    const run = barrier.beginRun(sessionID)
    run.commit(`msg_${sessionID}`)
    run.projector("session-row")
    run.noTools()
    run.promotion("no-job")
    return barrier.wait(sessionID)
  })
}

describe("run barrier service contract", () => {
  it.effect("cancellation wins over an incomplete report set", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const run = barrier.beginRun("s1")
      run.commit("msg_1")

      const waiting = yield* barrier.wait("s1").pipe(Effect.forkChild)
      yield* Effect.yieldNow
      barrier.cancel("s1")

      expect(yield* Fiber.join(waiting)).toEqual({ sessionID: "s1", status: "cancelled" })
      expect(barrier.state("s1")).toBe("cancelled")
    }),
  )

  it.effect("the 5 s bound expires with a typed RunBarrierTimeout and refuses the next run", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const run = barrier.beginRun("s1")
      run.commit("msg_1")
      run.projector("session-row")
      run.noTools()
      // promotion is deliberately missing.

      const waiting = yield* barrier.wait("s1").pipe(Effect.exit, Effect.forkChild)
      yield* Effect.yieldNow
      yield* TestClock.adjust(RUN_BARRIER_TIMEOUT_MS)
      const exit = yield* Fiber.join(waiting)

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBeInstanceOf(RunBarrierTimeout)
      expect(barrier.state("s1")).toBe("timed-out")

      const refused = yield* barrier.wait("s1").pipe(Effect.flip)
      expect(refused).toBeInstanceOf(RunBarrierTimeout)
    }),
  )

  it.effect("waiting for a run that never began is a typed missing-report refusal", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const refused = yield* barrier.wait("s1").pipe(Effect.flip)
      expect(refused).toBeInstanceOf(RunBarrierMissingReportError)
      expect(barrier.state("s1")).toBe("failed")
    }),
  )

  it.effect("a no-commit run records { committed: null }", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const run = barrier.beginRun("s1")
      expect(run.commitOutcome()).toBeUndefined()
      run.noCommit()
      expect(run.commitOutcome()).toEqual({ committed: null })
    }),
  )

  it.effect("the session map retains at most the LRU bound", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      for (let index = 0; index < RUN_BARRIER_MAX_SESSIONS + 1; index++) {
        yield* drain(barrier, `s${index}`)
      }
      expect(barrier.size).toBe(RUN_BARRIER_MAX_SESSIONS)
      expect(barrier.state("s0")).toBe("idle")
      expect(barrier.state(`s${RUN_BARRIER_MAX_SESSIONS}`)).toBe("drained")
    }),
  )
})
