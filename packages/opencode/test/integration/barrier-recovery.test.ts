// a timed-out run barrier must be abandonable, not a permanent wedge.
//
// The red state: `admitBarrier` refused every later run once the barrier was
// `timed-out`, and the lifecycle tracker kept the crashed generation's in-flight
// calls forever. A session whose worker was already retired (crash / terminate)
// could therefore never start another run: `ensureRunning` threw
// `RunBarrierTimeout` on every attempt.
//
// The recovery is deliberately narrow: a timeout is only abandoned once the
// worker that owned it is gone (no live fiber can still settle its phases), and
// calls left in flight before a run starts cannot belong to it. A live worker
// keeps the spec's refusal (D18). Cancellation still wins over every drain.

import { describe, expect, test } from "bun:test"
import { createLifecycleTracker } from "@/session/ledger/lifecycle"
import { createFakeExecutorPool } from "../lib/fake-worker-pool"
import { flush, sid } from "../lib/async"
import {
  RUN_BARRIER_TIMEOUT_MS,
  RunBarrierTimeout,
  createRunBarrier,
  type RunBarrierScheduler,
} from "@/session/run-barrier"
import { createSessionHub } from "@/session/worker/hub"

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

const settled = Promise.resolve()

/**
 * The explicit legacy-observer seam: these cases exercise a fake run that never
 * reports commit/projector phases, so the hub's drain is driven by caller
 * phases instead of the production report set.
 */
const settledPhases = () => ({
  commits: settled,
  projectors: settled,
  tools: settled,
  promotion: settled,
})

describe("run-barrier recovery after a crash", () => {
  test("a timed-out barrier with no live worker is abandoned, including its orphaned call", async () => {
    const lifecycle = createLifecycleTracker()
    const clock = fakeClock()
    const barrier = createRunBarrier({ scheduler: clock.scheduler })
    const hub = createSessionHub({
      pool: createFakeExecutorPool(1),
      lifecycle,
      barrier,
      barrierPhases: settledPhases,
    })
    const sessionID = sid("ses_barrier_abandon")

    // A dead generation left a side-effecting call in flight (indeterminate for
    // resume) and the barrier expired: nothing can settle either now.
    lifecycle.toolStarted(sessionID, "call_from_dead_worker", true)
    const stuck = new Promise<never>(() => {})
    const timedOut = barrier.wait(sessionID, {
      commits: stuck,
      projectors: settled,
      tools: settled,
      promotion: settled,
    })
    await flush()
    clock.advance(RUN_BARRIER_TIMEOUT_MS)
    await expect(timedOut).rejects.toBeInstanceOf(RunBarrierTimeout)
    expect(barrier.state(sessionID)).toBe("timed-out")

    try {
      await expect(hub.ensureRunning(sessionID)).resolves.toBeDefined()
      expect(hub.barrier.state(sessionID)).toBe("drained")
      expect(lifecycle.indeterminate(sessionID)).toEqual([])
    } finally {
      await hub.terminateAll()
    }
  })

  test("a cancelled tool call no longer permanently poisons the session", async () => {
    const lifecycle = createLifecycleTracker()
    const clock = fakeClock()
    const barrier = createRunBarrier({ scheduler: clock.scheduler })
    const pool = createFakeExecutorPool(1, { mode: "blocking" })
    const hub = createSessionHub({ pool, lifecycle, barrier })
    const sessionID = sid("ses_barrier_cancelled_tool")

    try {
      // A real generation is running and a side-effecting tool is in flight.
      const first = hub.ensureRunning(sessionID)
      await flush()
      lifecycle.toolStarted(sessionID, "call_cancelled", true)

      // Cancelling retires that generation: its call can never settle.
      await hub.cancel(sessionID)
      await first.catch(() => undefined)
      await flush()

      // The next run must release the orphaned call at admission, not inherit
      // it and time out its own barrier.
      const second = hub.ensureRunning(sessionID)
      await flush()
      expect(lifecycle.indeterminate(sessionID)).toEqual([])
      expect(barrier.state(sessionID)).not.toBe("timed-out")

      await hub.cancel(sessionID)
      await second.catch(() => undefined)
    } finally {
      await hub.terminateAll()
    }
  })

  test("a live in-flight tool still holds the barrier (recovery is not a free pass)", async () => {
    const lifecycle = createLifecycleTracker()
    const clock = fakeClock()
    const barrier = createRunBarrier({ scheduler: clock.scheduler })
    const pool = createFakeExecutorPool(1, { mode: "blocking" })
    const hub = createSessionHub({ pool, lifecycle, barrier })
    const sessionID = sid("ses_barrier_live_tool")

    try {
      const run = hub.ensureRunning(sessionID)
      await flush()
      // The tool starts DURING the run, so it belongs to this run's barrier.
      lifecycle.toolStarted(sessionID, "call_live", true)
      const worker = pool.spawned[0] ?? pool.workers[0]
      await worker!.cancel(sessionID)
      await flush()
      expect(hub.barrier.state(sessionID)).toBe("draining")

      clock.advance(RUN_BARRIER_TIMEOUT_MS)
      await run
      expect(hub.barrier.state(sessionID)).toBe("timed-out")

      // A live worker still owns the session, so the next run is refused (D18).
      await expect(hub.ensureRunning(sessionID)).rejects.toBeInstanceOf(RunBarrierTimeout)
    } finally {
      await hub.terminateAll()
    }
  })

  test("cancel still wins over barrier completion and over a later timeout", async () => {
    const lifecycle = createLifecycleTracker()
    const clock = fakeClock()
    const barrier = createRunBarrier({ scheduler: clock.scheduler })
    const hub = createSessionHub({
      pool: createFakeExecutorPool(1),
      lifecycle,
      barrier,
      barrierPhases: settledPhases,
    })
    const sessionID = sid("ses_barrier_cancel_wins")

    try {
      const pending = barrier.wait(sessionID, {
        commits: new Promise<never>(() => {}),
        projectors: settled,
        tools: settled,
        promotion: settled,
      })
      await flush()
      expect(barrier.state(sessionID)).toBe("draining")

      await hub.cancel(sessionID)

      await expect(pending).resolves.toEqual({ sessionID, status: "cancelled" })
      expect(barrier.state(sessionID)).toBe("cancelled")
      clock.advance(RUN_BARRIER_TIMEOUT_MS)
      expect(barrier.state(sessionID)).toBe("cancelled")

      // A cancelled session is re-admitted only through the explicit reset path.
      await expect(hub.ensureRunning(sessionID)).resolves.toBeDefined()
    } finally {
      await hub.terminateAll()
    }
  })
})
