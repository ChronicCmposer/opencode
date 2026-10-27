// Barrier and resume state must be bounded: an abandoned session can never grow
// the process maps forever. Terminal sessions evict LRU once the cap is reached.

import { describe, expect, test } from "bun:test"
import { createRunBarrier } from "@/session/run-barrier"
import { createAutoResumeController } from "@/session/ledger/resume"
import { createLifecycleTracker } from "@/session/ledger/lifecycle"

const settled = Promise.resolve()

describe("bounded lifecycle maps (MINOR)", () => {
  test("run-barrier keeps at most the configured number of session states", async () => {
    const barrier = createRunBarrier({ maxSessions: 2 })
    for (const sessionID of ["s1", "s2", "s3"]) {
      await barrier.wait(sessionID, {
        commits: settled,
        projectors: settled,
        tools: settled,
        promotion: settled,
      })
    }
    expect(barrier.size).toBe(2)
    expect(barrier.state("s1")).toBe("idle")
    expect(barrier.state("s3")).toBe("drained")
  })

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
