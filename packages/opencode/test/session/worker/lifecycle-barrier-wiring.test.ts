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

/**
 * The explicit legacy-observer seam: these cases drive the barrier with caller
 * phases because the fake run never reports commit/projector phases itself.
 */
const settled = Promise.resolve()

function deferred() {
  let resolve!: () => void
  const promise = new Promise<void>((done) => {
    resolve = done
  })
  return { promise, resolve }
}

const phasesWithTools = (tools: Promise<unknown>) => ({
  commits: settled,
  projectors: settled,
  tools,
  promotion: settled,
})

describe("hub run-barrier wiring (D18)", () => {
  test("the next run waits until an in-flight tool settles", async () => {
    const lifecycle = createLifecycleTracker()
    const clock = fakeScheduler()
    const barrier = createRunBarrier({ scheduler: clock.scheduler })
    const tools = deferred()
    const hub = createSessionHub({
      pool: createFakeExecutorPool(1),
      lifecycle,
      barrier,
      barrierPhases: () => phasesWithTools(tools.promise),
    })
    const sessionID = sid("ses_barrier_wait")
    lifecycle.toolStarted(sessionID, "call_write")

    try {
      const first = hub.ensureRunning(sessionID)
      await flush()
      const second = hub.ensureRunning(sessionID)
      await flush()
      expect(hub.dispatcher.busySlotCount).toBe(1)
      expect(hub.barrier.state(sessionID)).toBe("draining")

      lifecycle.toolFinished(sessionID, "call_write")
      tools.resolve()
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
    const stuck = new Promise<never>(() => {})
    const hub = createSessionHub({
      pool: createFakeExecutorPool(1),
      lifecycle,
      barrier,
      barrierPhases: () => phasesWithTools(stuck),
    })
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
    const stuck = new Promise<never>(() => {})
    const hub = createSessionHub({
      pool: createFakeExecutorPool(1),
      lifecycle,
      barrier,
      barrierPhases: () => phasesWithTools(stuck),
    })
    const sessionID = sid("ses_barrier_cancel")
    lifecycle.toolStarted(sessionID, "call_slow")

    try {
      const first = hub.ensureRunning(sessionID).catch(() => undefined)
      await flush()
      expect(hub.barrier.state(sessionID)).toBe("draining")
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

// The barrier must await all FOUR commit-side phases. A phase that never reports
// (here `promotion`) is not treated as settled merely because the others are:
// the barrier stays open, expires typed, and cancel still wins.
describe("hub run-barrier four-phase coverage", () => {
  const partialPhases = () => ({
    commits: Promise.resolve(),
    projectors: Promise.resolve(),
    tools: Promise.resolve(),
    promotion: new Promise<never>(() => {}),
  })

  test("a missing promotion phase is not fabricated as settled", async () => {
    const lifecycle = createLifecycleTracker()
    const clock = fakeScheduler()
    const barrier = createRunBarrier({ scheduler: clock.scheduler })
    const hub = createSessionHub({
      pool: createFakeExecutorPool(1),
      lifecycle,
      barrier,
      barrierPhases: partialPhases,
    })
    const sessionID = sid("ses_barrier_missing_promotion")

    try {
      const first = hub.ensureRunning(sessionID).catch(() => undefined)
      await flush()
      expect(hub.barrier.state(sessionID)).toBe("draining")
      clock.advance(RUN_BARRIER_TIMEOUT_MS)
      await first
      await flush()

      expect(hub.barrier.state(sessionID)).toBe("timed-out")
      await expect(hub.ensureRunning(sessionID)).rejects.toBeInstanceOf(RunBarrierTimeout)
    } finally {
      await hub.terminateAll()
    }
  })

  test("cancel wins while promotion is missing", async () => {
    const lifecycle = createLifecycleTracker()
    const barrier = createRunBarrier()
    const hub = createSessionHub({
      pool: createFakeExecutorPool(1),
      lifecycle,
      barrier,
      barrierPhases: partialPhases,
    })
    const sessionID = sid("ses_barrier_missing_cancel")

    try {
      const first = hub.ensureRunning(sessionID).catch(() => undefined)
      await flush()
      expect(hub.barrier.state(sessionID)).toBe("draining")
      await hub.cancel(sessionID)
      await first
      await flush()
      expect(hub.barrier.state(sessionID)).toBe("cancelled")
    } finally {
      await hub.terminateAll()
    }
  })
})
