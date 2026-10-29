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

import { describe, expect } from "bun:test"
import { Effect, Exit, Fiber } from "effect"
import { createLifecycleTracker } from "@/session/ledger/lifecycle"
import { createFakeExecutorPool } from "../lib/fake-worker-pool"
import { flush, sid } from "../lib/async"
import { RunBarrier, RunBarrierTimeout } from "@/session/run-barrier"
import { createSessionHub } from "@/session/worker/hub"
import { testEffect } from "../lib/effect"
import { advanceBarrierTimeout, clockedRunBarrierLayer } from "../lib/run-barrier-test"

const it = testEffect(clockedRunBarrierLayer)

/** Let the hub's promises reach their next await without changing the clock. */
const settle = () => Effect.promise(flush)

/** Fork a hub promise so the test can observe intermediate state. */
const forkHub = <A>(run: () => Promise<A>) => Effect.promise(run).pipe(Effect.forkChild)

describe("run-barrier recovery after a crash", () => {
  it.effect("a timed-out barrier with no live worker is abandoned, including its orphaned call", () =>
    Effect.gen(function* () {
      const lifecycle = createLifecycleTracker()
      const barrier = yield* RunBarrier.Service
      const hub = createSessionHub({
        pool: createFakeExecutorPool(1),
        lifecycle,
        barrier,
      })
      const sessionID = sid("ses_barrier_abandon")

      try {
        // A dead generation left a side-effecting call in flight (indeterminate
        // for resume) and the barrier expired: nothing can settle either now.
        lifecycle.toolStarted(sessionID, "call_from_dead_worker", true)
        yield* barrier.beginRun(sessionID)
        const timedOut = yield* barrier.wait(sessionID).pipe(Effect.exit, Effect.forkChild)
        yield* Effect.yieldNow
        yield* advanceBarrierTimeout()
        expect(Exit.isFailure(yield* Fiber.join(timedOut))).toBe(true)
        expect(barrier.state(sessionID)).toBe("timed-out")

        yield* Effect.promise(() => hub.ensureRunning(sessionID))
        expect(hub.barrier.state(sessionID)).toBe("drained")
        expect(lifecycle.indeterminate(sessionID)).toEqual([])
      } finally {
        yield* Effect.promise(() => hub.terminateAll())
      }
    }),
  )

  it.effect("a cancelled tool call no longer permanently poisons the session", () =>
    Effect.gen(function* () {
      const lifecycle = createLifecycleTracker()
      const barrier = yield* RunBarrier.Service
      const pool = createFakeExecutorPool(1, { mode: "blocking" })
      const hub = createSessionHub({ pool, lifecycle, barrier })
      const sessionID = sid("ses_barrier_cancelled_tool")

      try {
        // A real generation is running and a side-effecting tool is in flight.
        const first = yield* forkHub(() => hub.ensureRunning(sessionID).catch(() => undefined))
        yield* settle()
        lifecycle.toolStarted(sessionID, "call_cancelled", true)

        // Cancelling retires that generation: its call can never settle.
        yield* Effect.promise(() => hub.cancel(sessionID))
        yield* Fiber.join(first)
        yield* settle()

        // The next run must release the orphaned call at admission, not inherit
        // it and time out its own barrier.
        const second = yield* forkHub(() => hub.ensureRunning(sessionID).catch(() => undefined))
        yield* settle()
        expect(lifecycle.indeterminate(sessionID)).toEqual([])
        expect(barrier.state(sessionID)).not.toBe("timed-out")

        yield* Effect.promise(() => hub.cancel(sessionID))
        yield* Fiber.join(second)
      } finally {
        yield* Effect.promise(() => hub.terminateAll())
      }
    }),
  )

  it.effect("a live in-flight tool still holds the barrier (recovery is not a free pass)", () =>
    Effect.gen(function* () {
      const lifecycle = createLifecycleTracker()
      const barrier = yield* RunBarrier.Service
      const pool = createFakeExecutorPool(1, { mode: "blocking" })
      const hub = createSessionHub({ pool, lifecycle, barrier })
      const sessionID = sid("ses_barrier_live_tool")

      try {
        const run = yield* forkHub(() => hub.ensureRunning(sessionID).catch(() => undefined))
        yield* settle()
        // The tool starts DURING the run, so it belongs to this run's barrier.
        lifecycle.toolStarted(sessionID, "call_live", true)
        const worker = pool.spawned[0] ?? pool.workers[0]
        yield* Effect.promise(() => worker.cancel(sessionID))
        yield* settle()
        expect(hub.barrier.state(sessionID)).toBe("draining")

        yield* advanceBarrierTimeout()
        yield* Fiber.join(run)
        expect(hub.barrier.state(sessionID)).toBe("timed-out")

        // A live worker still owns the session, so the next run is refused (D18).
        const refused = yield* Effect.tryPromise({
          try: () => hub.ensureRunning(sessionID),
          catch: (error) => error,
        }).pipe(Effect.flip)
        expect(refused).toBeInstanceOf(RunBarrierTimeout)
      } finally {
        yield* Effect.promise(() => hub.terminateAll())
      }
    }),
  )

  it.effect("cancel still wins over barrier completion and over a later timeout", () =>
    Effect.gen(function* () {
      const lifecycle = createLifecycleTracker()
      const barrier = yield* RunBarrier.Service
      const hub = createSessionHub({
        pool: createFakeExecutorPool(1),
        lifecycle,
        barrier,
      })
      const sessionID = sid("ses_barrier_cancel_wins")

      try {
        yield* barrier.beginRun(sessionID)
        const pending = yield* barrier.wait(sessionID).pipe(Effect.forkChild)
        yield* Effect.yieldNow
        expect(barrier.state(sessionID)).toBe("draining")

        yield* Effect.promise(() => hub.cancel(sessionID))

        expect(yield* Fiber.join(pending)).toEqual({ sessionID, status: "cancelled" })
        expect(barrier.state(sessionID)).toBe("cancelled")
        yield* advanceBarrierTimeout()
        expect(barrier.state(sessionID)).toBe("cancelled")

        // A cancelled session is re-admitted only through the explicit reset path.
        yield* Effect.promise(() => hub.ensureRunning(sessionID))
      } finally {
        yield* Effect.promise(() => hub.terminateAll())
      }
    }),
  )
})
