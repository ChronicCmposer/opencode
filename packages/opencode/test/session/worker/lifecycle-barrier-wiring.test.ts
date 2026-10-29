// The run barrier must be drained by the production hub: the next run waits for
// the prior run's commits/projectors/tools/promotion, is bounded at 5 s, and a
// cancel wins over a still-draining barrier.
//
// The bound is driven by `TestClock.adjust` through `clockedRunBarrierLayer`, so
// expiry is deterministic and instant; no case waits 5 s of wall-clock time.

import { describe, expect } from "bun:test"
import { Effect, Fiber } from "effect"

import { createLifecycleTracker } from "@/session/ledger/lifecycle"
import { RunBarrier, RunBarrierTimeout } from "@/session/run-barrier"
import { createSessionHub } from "@/session/worker/hub"
import { createFakeExecutorPool } from "../../lib/fake-worker-pool"
import { flush, pollUntil, sid } from "../../lib/async"
import { testEffect } from "../../lib/effect"
import { advanceBarrierTimeout, clockedRunBarrierLayer } from "../../lib/run-barrier-test"

const it = testEffect(clockedRunBarrierLayer)

interface ObservableBarrier {
  readonly barrier: RunBarrier.Interface
  run(sessionID: string): RunBarrier.RunBarrierRun | undefined
  suppressPromotion(sessionID: string): void
}

/**
 * A delegating spy over the real service: the hub drives the real reports, while
 * these cases capture the per-run handle (to settle an in-flight tool) or omit
 * the promotion report (to prove a missing phase is never fabricated).
 */
function observableBarrier(service: RunBarrier.Interface): ObservableBarrier {
  const runs = new Map<string, RunBarrier.RunBarrierRun>()
  const suppressPromotion = new Set<string>()
  return {
    barrier: {
      beginRun: (sessionID) =>
        service.beginRun(sessionID).pipe(
          Effect.map((run) => {
            const captured = suppressPromotion.has(sessionID) ? { ...run, promotion: () => false } : run
            runs.set(sessionID, captured)
            return captured
          }),
        ),
      wait: (sessionID) => service.wait(sessionID),
      cancel: (sessionID) => service.cancel(sessionID),
      state: (sessionID) => service.state(sessionID),
      reset: (sessionID) => service.reset(sessionID),
      abandon: (sessionID) => service.abandon(sessionID),
      get size() {
        return service.size
      },
    },
    run: (sessionID) => runs.get(sessionID),
    suppressPromotion: (sessionID) => suppressPromotion.add(sessionID),
  }
}

/** Wait for the hub to publish the state the case is about to act on. */
const waitForState = (hub: ReturnType<typeof createSessionHub>, sessionID: string, state: RunBarrier.RunBarrierState) =>
  Effect.promise(() =>
    pollUntil(() => hub.barrier.state(sessionID) === state, {
      message: `barrier never reached ${state}`,
    }),
  )

describe("hub run-barrier wiring (D18)", () => {
  it.effect("the next run waits until an in-flight tool settles", () =>
    Effect.gen(function* () {
      const service = yield* RunBarrier.Service
      const lifecycle = createLifecycleTracker()
      const control = observableBarrier(service)
      const hub = createSessionHub({
        pool: createFakeExecutorPool(1),
        lifecycle,
        barrier: control.barrier,
      })
      const sessionID = sid("ses_barrier_wait")
      lifecycle.toolStarted(sessionID, "call_write")

      try {
        const first = yield* Effect.promise(() => hub.ensureRunning(sessionID)).pipe(Effect.forkChild)
        yield* Effect.promise(flush)
        yield* waitForState(hub, sessionID, "draining")
        const second = yield* Effect.promise(() => hub.ensureRunning(sessionID)).pipe(Effect.forkChild)
        yield* Effect.promise(flush)
        expect(hub.dispatcher.busySlotCount).toBe(1)
        expect(hub.barrier.state(sessionID)).toBe("draining")

        lifecycle.toolFinished(sessionID, "call_write")
        control.run(sessionID)?.tool("call_write")
        yield* Fiber.join(first)
        yield* Fiber.join(second)
        expect(hub.barrier.state(sessionID)).toBe("drained")
      } finally {
        yield* Effect.promise(() => hub.terminateAll())
      }
    }),
  )

  it.effect("expiry is a typed RunBarrierTimeout that refuses the next run", () =>
    Effect.gen(function* () {
      const service = yield* RunBarrier.Service
      const lifecycle = createLifecycleTracker()
      const control = observableBarrier(service)
      const hub = createSessionHub({
        pool: createFakeExecutorPool(1),
        lifecycle,
        barrier: control.barrier,
      })
      const sessionID = sid("ses_barrier_timeout")
      lifecycle.toolStarted(sessionID, "call_stuck")

      try {
        const first = yield* Effect.promise(() => hub.ensureRunning(sessionID).catch(() => undefined)).pipe(
          Effect.forkChild,
        )
        yield* Effect.promise(flush)
        yield* waitForState(hub, sessionID, "draining")
        // The in-flight tool is never reported: advance the barrier's clock to
        // its real 5 s bound instead of sleeping for it.
        yield* advanceBarrierTimeout()
        yield* Fiber.join(first)
        yield* Effect.promise(flush)

        const refused = yield* Effect.tryPromise({
          try: () => hub.ensureRunning(sessionID),
          catch: (error) => error,
        }).pipe(Effect.flip)
        expect(refused).toBeInstanceOf(RunBarrierTimeout)
        expect(hub.barrier.state(sessionID)).toBe("timed-out")
      } finally {
        yield* Effect.promise(() => hub.terminateAll())
      }
    }),
  )

  it.effect("cancel wins while the barrier is draining", () =>
    Effect.gen(function* () {
      const service = yield* RunBarrier.Service
      const lifecycle = createLifecycleTracker()
      const control = observableBarrier(service)
      const hub = createSessionHub({
        pool: createFakeExecutorPool(1),
        lifecycle,
        barrier: control.barrier,
      })
      const sessionID = sid("ses_barrier_cancel")
      lifecycle.toolStarted(sessionID, "call_slow")

      try {
        const first = yield* Effect.promise(() => hub.ensureRunning(sessionID).catch(() => undefined)).pipe(
          Effect.forkChild,
        )
        yield* Effect.promise(flush)
        yield* waitForState(hub, sessionID, "draining")
        yield* Effect.promise(() => hub.cancel(sessionID))
        yield* Fiber.join(first)
        yield* Effect.promise(flush)
        expect(hub.barrier.state(sessionID)).toBe("cancelled")
      } finally {
        yield* Effect.promise(() => hub.terminateAll())
      }
    }),
  )
})

// The barrier must await all FOUR commit-side phases. A phase that never reports
// (here `promotion`) is not treated as settled merely because the others are:
// the barrier stays open, expires typed, and cancel still wins.
describe("hub run-barrier four-phase coverage", () => {
  it.effect("a missing promotion phase is not fabricated as settled", () =>
    Effect.gen(function* () {
      const service = yield* RunBarrier.Service
      const lifecycle = createLifecycleTracker()
      const control = observableBarrier(service)
      const sessionID = sid("ses_barrier_missing_promotion")
      control.suppressPromotion(sessionID)
      const hub = createSessionHub({
        pool: createFakeExecutorPool(1),
        lifecycle,
        barrier: control.barrier,
      })

      try {
        const first = yield* Effect.promise(() => hub.ensureRunning(sessionID).catch(() => undefined)).pipe(
          Effect.forkChild,
        )
        yield* Effect.promise(flush)
        yield* waitForState(hub, sessionID, "draining")
        yield* advanceBarrierTimeout()
        yield* Fiber.join(first)
        yield* Effect.promise(flush)

        expect(hub.barrier.state(sessionID)).toBe("timed-out")
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

  it.effect("cancel wins while promotion is missing", () =>
    Effect.gen(function* () {
      const service = yield* RunBarrier.Service
      const lifecycle = createLifecycleTracker()
      const control = observableBarrier(service)
      const sessionID = sid("ses_barrier_missing_cancel")
      control.suppressPromotion(sessionID)
      const hub = createSessionHub({
        pool: createFakeExecutorPool(1),
        lifecycle,
        barrier: control.barrier,
      })

      try {
        const first = yield* Effect.promise(() => hub.ensureRunning(sessionID).catch(() => undefined)).pipe(
          Effect.forkChild,
        )
        yield* Effect.promise(flush)
        yield* waitForState(hub, sessionID, "draining")
        yield* Effect.promise(() => hub.cancel(sessionID))
        yield* Fiber.join(first)
        yield* Effect.promise(flush)
        expect(hub.barrier.state(sessionID)).toBe("cancelled")
      } finally {
        yield* Effect.promise(() => hub.terminateAll())
      }
    }),
  )
})
