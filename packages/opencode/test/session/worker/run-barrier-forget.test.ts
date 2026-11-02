// `RunBarrier.forget` is the crash/terminate escape hatch: the owning worker is
// gone, so the active wait is failed with `RunBarrierCancelled` and every
// retained map entry for the session is dropped. A successor run is admitted
// immediately, and a late report from the forgotten run can never settle it.

import { describe, expect, test } from "bun:test"
import { Effect, Fiber } from "effect"
import type { RunBarrierRun } from "@/session/run-barrier"
import { createTestBarrier } from "../../lib/run-barrier-test"

function reportSuccessor(run: RunBarrierRun): void {
  run.commit("msg_successor")
  run.projector("session-row")
  run.noTools()
  run.promotion("no-job")
}

describe("RunBarrier.forget", () => {
  test("an active wait returns cancelled and the session state is dropped", async () => {
    const barrier = createTestBarrier()
    const run = await Effect.runPromise(barrier.beginRun("ses_forget_active"))
    const waiting = Effect.runFork(barrier.wait("ses_forget_active"))

    await Effect.runPromise(Effect.yieldNow)
    expect(barrier.state("ses_forget_active")).toBe("draining")
    expect(barrier.size).toBe(1)

    await Effect.runPromise(barrier.forget("ses_forget_active"))

    expect(await Effect.runPromise(Fiber.join(waiting))).toEqual({
      sessionID: "ses_forget_active",
      status: "cancelled",
    })
    expect(barrier.size).toBe(0)
    expect(barrier.state("ses_forget_active")).toBe("idle")

    // The forgotten report handle no longer owns the session.
    expect(run.commit("msg_late")).toBe(false)
  })

  test("a successor run begins after forget and drains on its own reports", async () => {
    const barrier = createTestBarrier()
    await Effect.runPromise(barrier.beginRun("ses_forget_successor"))
    await Effect.runPromise(barrier.forget("ses_forget_successor"))

    const successor = await Effect.runPromise(barrier.beginRun("ses_forget_successor"))
    reportSuccessor(successor)

    expect(await Effect.runPromise(barrier.wait("ses_forget_successor"))).toEqual({
      sessionID: "ses_forget_successor",
      status: "drained",
    })
    expect(barrier.size).toBe(1)
  })

  test("a late report from the forgotten run cannot mark the successor drained", async () => {
    const barrier = createTestBarrier()
    const forgotten = await Effect.runPromise(barrier.beginRun("ses_forget_late"))
    await Effect.runPromise(barrier.forget("ses_forget_late"))

    const successor = await Effect.runPromise(barrier.beginRun("ses_forget_late"))
    const waiting = Effect.runFork(barrier.wait("ses_forget_late"))
    await Effect.runPromise(Effect.yieldNow)
    expect(barrier.state("ses_forget_late")).toBe("draining")

    // Every late report is rejected: the forgotten epoch no longer owns the map.
    expect(forgotten.commit("msg_forgotten")).toBe(false)
    expect(forgotten.projector("session-row")).toBe(false)
    expect(forgotten.noTools()).toBe(false)
    expect(forgotten.promotion("no-job")).toBe(false)
    await Effect.runPromise(Effect.yieldNow)
    expect(barrier.state("ses_forget_late")).toBe("draining")

    reportSuccessor(successor)
    expect(await Effect.runPromise(Fiber.join(waiting))).toEqual({
      sessionID: "ses_forget_late",
      status: "drained",
    })
  })

  test("forget with no active run is a safe no-op", async () => {
    const barrier = createTestBarrier()

    await Effect.runPromise(barrier.forget("ses_forget_unknown"))
    expect(barrier.size).toBe(0)
    expect(barrier.state("ses_forget_unknown")).toBe("idle")

    await Effect.runPromise(barrier.beginRun("ses_forget_idle"))
    await Effect.runPromise(barrier.forget("ses_forget_idle"))
    await Effect.runPromise(barrier.forget("ses_forget_idle"))
    expect(barrier.size).toBe(0)
  })
})
