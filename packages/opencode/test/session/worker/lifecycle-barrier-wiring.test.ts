// The run barrier must be drained by the production hub: the next run waits for
// the prior run's commits/projectors/tools/promotion, is bounded at 5 s, and a
// cancel wins over a still-draining barrier.

import { describe, expect, test } from "bun:test"
import { createLifecycleTracker } from "@/session/ledger/lifecycle"
import { createFakeExecutorPool } from "../../lib/fake-worker-pool"
import { flush } from "../../lib/async"
import { sid } from "../../lib/async"
import {
  RUN_BARRIER_TIMEOUT_MS,
  RunBarrierTimeout,
  createRunBarrier,
  type RunBarrierScheduler,
} from "@/session/run-barrier"
import { createSessionHub } from "@/session/worker/hub"

function fakeScheduler() {
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

describe("hub run-barrier wiring (C7 / D18)", () => {
  test("the next run waits until an in-flight tool settles", async () => {
    const lifecycle = createLifecycleTracker()
    const clock = fakeScheduler()
    const barrier = createRunBarrier({ scheduler: clock.scheduler })
    const hub = createSessionHub({ pool: createFakeExecutorPool(1), lifecycle, barrier })
    const sessionID = sid("ses_barrier_wait")
    lifecycle.toolStarted(sessionID, "call_write")

    try {
      const first = hub.ensureRunning(sessionID)
      await flush()
      const second = hub.ensureRunning(sessionID)
      await flush()
      expect(hub.dispatcher.busySlotCount).toBe(1)

      lifecycle.toolFinished(sessionID, "call_write")
      await first
      await second
      expect(hub.barrier.state(sessionID)).toBe("drained")
    } finally {
      await hub.terminateAll()
    }
  })

  test("expiry is a typed RunBarrierTimeout that refuses the next run", async () => {
    const lifecycle = createLifecycleTracker()
    const clock = fakeScheduler()
    const barrier = createRunBarrier({ scheduler: clock.scheduler })
    const hub = createSessionHub({ pool: createFakeExecutorPool(1), lifecycle, barrier })
    const sessionID = sid("ses_barrier_timeout")
    lifecycle.toolStarted(sessionID, "call_stuck")

    try {
      const first = hub.ensureRunning(sessionID)
      await flush()
      expect(hub.barrier.state(sessionID)).toBe("draining")
      clock.advance(RUN_BARRIER_TIMEOUT_MS)
      await first
      await flush()

      await expect(hub.ensureRunning(sessionID)).rejects.toBeInstanceOf(RunBarrierTimeout)
      expect(hub.barrier.state(sessionID)).toBe("timed-out")
    } finally {
      await hub.terminateAll()
    }
  })

  test("cancel wins while the barrier is draining", async () => {
    const lifecycle = createLifecycleTracker()
    const clock = fakeScheduler()
    const barrier = createRunBarrier({ scheduler: clock.scheduler })
    const hub = createSessionHub({ pool: createFakeExecutorPool(1), lifecycle, barrier })
    const sessionID = sid("ses_barrier_cancel")
    lifecycle.toolStarted(sessionID, "call_slow")

    try {
      const first = hub.ensureRunning(sessionID).catch(() => undefined)
      await flush()
      await hub.cancel(sessionID)
      await first
      await flush()
      expect(hub.barrier.state(sessionID)).toBe("cancelled")
      expect(barrier.state(sessionID)).not.toBe("draining")
    } finally {
      await hub.terminateAll()
    }
  })
})
