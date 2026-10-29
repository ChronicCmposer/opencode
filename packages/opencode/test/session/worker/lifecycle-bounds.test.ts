// Barrier and resume state must be bounded: an abandoned session can never grow
// the process maps forever. Terminal sessions evict LRU once the cap is reached.

import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"

import { RUN_BARRIER_MAX_SESSIONS, RunBarrier } from "@/session/run-barrier"
import { createAutoResumeController } from "@/session/ledger/resume"
import { createLifecycleTracker } from "@/session/ledger/lifecycle"
import { testEffect } from "../../lib/effect"

const barrierIt = testEffect(LayerNode.compile(LayerNode.group([RunBarrier.node])))

const drain = Effect.fn("test.drain")(function* (barrier: RunBarrier.Interface, sessionID: string) {
  const run = yield* barrier.beginRun(sessionID)
  run.commit(`msg_${sessionID}`)
  run.projector("session-row")
  run.noTools()
  run.promotion("no-job")
  return yield* barrier.wait(sessionID)
})

describe("bounded lifecycle maps (MINOR)", () => {
  barrierIt.effect("run-barrier keeps at most the LRU number of session states", () =>
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

  test("resume attempts are bounded per session", () => {
    const resume = createAutoResumeController({ maxSessions: 2 })
    for (const sessionID of ["s1", "s2", "s3"]) resume.record(sessionID)
    expect(resume.size).toBe(2)
    expect(resume.attempts("s1")).toBe(0)
    expect(resume.attempts("s3")).toBe(1)
  })

  test("the lifecycle tracker is bounded and forgets retired sessions", () => {
    const lifecycle = createLifecycleTracker({ maxSessions: 2 })
    for (const sessionID of ["s1", "s2", "s3"]) lifecycle.commit(sessionID, `msg_${sessionID}`)
    expect(lifecycle.size).toBe(2)
    expect(lifecycle.crashState("s1").lastCommittedMessageID).toBeNull()
    lifecycle.forget("s3")
    expect(lifecycle.size).toBe(1)
  })
})
