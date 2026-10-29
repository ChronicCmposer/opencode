// v2 per-session dispatch core.
//
// One unbounded, lazily-materialized worker per session; reuse across turns; one
// active run per session; an unbounded per-session waiter list for a different
// run identity; at-most-once crash rejection followed by a fresh lazy worker;
// and the key→worker reservation lifecycle.

import { describe, expect, test } from "bun:test"
import { existsSync, readdirSync, readFileSync } from "node:fs"
import path from "node:path"
import { Deferred, Effect } from "effect"
import type { SessionID } from "@/session/schema"
import type { ShellInput } from "@/session/prompt"
import { SessionCancelledError, WorkerCrashedError, CANCELLED_MODES, type Dispatcher, type DispatchRequest, type WorkerPool } from "@/session/worker/dispatch"
import { createDispatcher } from "../../lib/dispatcher"
import {
  createExecutorWorker,
  createSessionDispatch,
  SessionWorkerReplyTimeoutError,
  type ExecutorTransport,
  type ExecutorWorkerPool,
  type SessionExecutorInbound,
  type SessionScheduler,
} from "@/session/worker/execute"
import { Logger } from "effect"
import { workerLogMessage } from "@/session/worker/worker-logging"
import { recordingBridge } from "../../lib/recorders"
import { createWorkerHarness } from "../../lib/worker-harness"
import { withFaults } from "../../lib/faults"
import { capture, flush, pollUntil, sid } from "../../lib/async"
import { FaultPool, FakeSessionWorker, createFakeWorkerPool, type FakeCancelPort } from "../../lib/fake-worker-pool"
import { createRecordingScheduler } from "../../lib/scheduler"

const createTestScheduler = createRecordingScheduler

describe("v2 one worker per session", () => {
  test("concurrent sessions each run on their own worker (no admission queue)", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })
    const first = dispatcher.dispatch({ sessionID: sid("ses_1") })
    const second = dispatcher.dispatch({ sessionID: sid("ses_2") })
    const third = dispatcher.dispatch({ sessionID: sid("ses_3") })
    await flush()

    expect(pool.spawned).toHaveLength(3)
    expect(pool.spawned.map((worker) => worker.runs[0]?.sessionID)).toEqual([sid("ses_1"), sid("ses_2"), sid("ses_3")])
    expect(dispatcher.busySlotCount).toBe(3)
    expect(dispatcher.queuedCount).toBe(0)

    for (const [index, worker] of pool.spawned.entries()) worker.finish(sid(`ses_${index + 1}`))
    await Promise.all([first, second, third])
    expect(dispatcher.busySlotCount).toBe(0)
    await dispatcher.terminateAll()
  })

  test("a session reuses its worker across turns", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })

    const first = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()
    pool.spawned[0].finish(sid("ses_1"))
    await first

    const second = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()
    expect(pool.spawned).toHaveLength(1)
    expect(pool.spawned[0].runs).toHaveLength(2)
    pool.spawned[0].finish(sid("ses_1"))
    await second
    await dispatcher.terminateAll()
  })

  test("materializes each session under the registry's monotonic id", async () => {
    const seen: number[] = []
    const pool: WorkerPool = {
      spawn: (id) => {
        const workerID = id ?? -1
        seen.push(workerID)
        return new FakeSessionWorker(workerID, { mode: "finish" })
      },
      terminateAll: async () => {},
    }
    const dispatcher = createDispatcher({ pool })

    await dispatcher.dispatch({ sessionID: sid("ses_a") })
    await dispatcher.dispatch({ sessionID: sid("ses_b") })

    expect(seen).toEqual([1, 2])
    await dispatcher.terminateAll()
  })

  test("a dispatch after terminateAll rejects and never spawns", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })
    await dispatcher.terminateAll()

    await expect(dispatcher.dispatch({ sessionID: sid("ses_late") })).rejects.toBeInstanceOf(SessionCancelledError)
    expect(pool.spawned).toHaveLength(0)
    expect(dispatcher.busySlotCount).toBe(0)
  })
})

describe("terminateAll is serialized", () => {
  test("concurrent callers share one shutdown and the pool closes once", async () => {
    let poolTerminations = 0
    const pool: WorkerPool = {
      spawn: (id) => new FakeSessionWorker(id ?? -1, { mode: "finish" }),
      terminateAll: async () => {
        poolTerminations++
        await flush()
      },
    }
    const dispatcher = createDispatcher({ pool })

    await Promise.all([dispatcher.terminateAll(), dispatcher.terminateAll()])
    await dispatcher.terminateAll()

    expect(poolTerminations).toBe(1)
  })
})

describe("crash handling retires the session worker at-most-once", () => {
  test("the crashed run rejects and is not requeued; a later dispatch spawns fresh", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })

    const crashed = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()
    pool.spawned[0].crash(sid("ses_1"))
    await expect(crashed).rejects.toBeInstanceOf(WorkerCrashedError)

    expect(pool.spawned[0].runs.map((run) => run.sessionID)).toEqual([sid("ses_1")])
    expect(dispatcher.queuedCount).toBe(0)
    expect(dispatcher.registry.has(sid("ses_1"))).toBe(false)

    const healthy = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()
    expect(pool.spawned).toHaveLength(2)
    expect(pool.spawned[1].runs.map((run) => run.sessionID)).toEqual([sid("ses_1")])
    pool.spawned[1].finish(sid("ses_1"))
    await healthy
    await dispatcher.terminateAll()
  })

  test("a session failure on a live worker releases the run and reuses the worker", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })

    const failing = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()
    pool.spawned[0].fail(sid("ses_1"), new Error("boom"))
    await expect(failing).rejects.toThrow("boom")

    expect(pool.spawned[0].alive).toBe(true)
    expect(pool.spawned).toHaveLength(1)

    const next = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()
    expect(pool.spawned[0].runs.map((run) => run.sessionID)).toEqual([sid("ses_1"), sid("ses_1")])
    pool.spawned[0].finish(sid("ses_1"))
    await next
    await dispatcher.terminateAll()
  })

  test("a real isolate crash via the OPENCODE_TEST_FAULTS seam rejects at-most-once", async () => {
    await withFaults("crashWorker@dispatch:run", async () => {
      await using harness = createWorkerHarness({ cores: 1 })
      const pool = new FaultPool(harness, 1)
      const dispatcher = createDispatcher({ pool })

      const crashed = dispatcher.dispatch({ sessionID: sid("ses_crash") })
      await expect(crashed).rejects.toBeInstanceOf(WorkerCrashedError)
      expect(dispatcher.registry.has(sid("ses_crash"))).toBe(false)
      expect(dispatcher.queuedCount).toBe(0)
      await dispatcher.terminateAll()
    })
  })

  test("a reply timeout rejects with the timeout, retires the worker and replaces it lazily", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })

    const run = capture(dispatcher.dispatch({ sessionID: sid("ses_timeout") }))
    await flush()
    const wedged = pool.spawned[0]
    const timedOut = new SessionWorkerReplyTimeoutError(30_000)
    wedged.fail(sid("ses_timeout"), timedOut)

    expect(await run).toMatchObject({ ok: false, error: timedOut })
    expect(wedged.alive).toBe(false)
    expect(dispatcher.workerForSession(sid("ses_timeout"))).toBeUndefined()

    const next = capture(dispatcher.dispatch({ sessionID: sid("ses_next") }))
    await flush()
    expect(pool.spawned).toHaveLength(2)
    expect(pool.spawned[1].runs.map((record) => record.sessionID)).toEqual([sid("ses_next")])
    pool.spawned[1].finish(sid("ses_next"))
    await next
    await dispatcher.terminateAll()
  })
})

describe("key→worker map lifecycle", () => {
  test("insert on dispatch, running while executing, deleted on completion", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })
    const run = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()

    const reservation = dispatcher.registry.get(sid("ses_1"))
    expect(reservation?.state).toBe("running")
    expect(reservation?.workerID).toBe(pool.spawned[0].id)

    pool.spawned[0].finish(sid("ses_1"))
    await run
    expect(dispatcher.registry.has(sid("ses_1"))).toBe(false)
    await dispatcher.terminateAll()
  })

  test("same-kind duplicate joins instead of running twice", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })
    const first = dispatcher.dispatch({ sessionID: sid("ses_1") })
    const joined = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()

    expect(pool.spawned[0].runs).toHaveLength(1)
    expect(dispatcher.queuedCount).toBe(0)
    pool.spawned[0].finish(sid("ses_1"))
    await Promise.all([first, joined])
    await dispatcher.terminateAll()
  })

  test("cancel deletes a parked waiter", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })
    const running = dispatcher.dispatch({ sessionID: sid("ses_running") })
    const queued = dispatcher.dispatch({ sessionID: sid("ses_running"), depth: 1 })
    await flush()

    // A session cancel removes its parked waiters AND cancels the running turn.
    await dispatcher.cancel(sid("ses_running"))
    await expect(queued).rejects.toBeInstanceOf(SessionCancelledError)
    await expect(running).rejects.toBeInstanceOf(SessionCancelledError)
    expect(dispatcher.registry.has(sid("ses_running"))).toBe(false)
    expect(dispatcher.queuedCount).toBe(0)
    await dispatcher.terminateAll()
  })

  test("cancel of a running session retires its worker", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })
    const running = capture(dispatcher.dispatch({ sessionID: sid("ses_1") }))
    await flush()

    await dispatcher.cancel(sid("ses_1"))
    expect(await running).toMatchObject({ ok: false, error: expect.any(SessionCancelledError) })
    expect(dispatcher.registry.has(sid("ses_1"))).toBe(false)
    expect(pool.spawned[0].alive).toBe(false)
    await dispatcher.terminateAll()
  })

  test("a deferred rejection from a cancelled run cannot settle the successor", async () => {
    const rejections: Array<(error: unknown) => void> = []
    const terminated: number[] = []
    let nextID = 1
    const pool: WorkerPool = {
      spawn: () => {
        const id = nextID++
        return {
          id,
          alive: true,
          execute: () => new Promise<void>((_resolve, reject) => void rejections.push(reject)),
          terminate: async () => {
            terminated.push(id)
          },
        }
      },
      terminateAll: async () => {},
    }
    const dispatcher = createDispatcher({ pool })

    const first = capture(dispatcher.dispatch({ sessionID: sid("ses_1") }))
    await flush()
    await dispatcher.cancel(sid("ses_1"))
    expect((await first).ok).toBe(false)

    // The successor takes the session and spawns a fresh worker.
    const second = capture(dispatcher.dispatch({ sessionID: sid("ses_1") }))
    await flush()
    expect(dispatcher.workerForSession(sid("ses_1"))).toBe(2)
    expect(dispatcher.registry.has(sid("ses_1"))).toBe(true)

    // The cancelled run's rejection is delivered late, after the successor owns it.
    rejections[0](new Error("late failure from the cancelled run"))
    await flush()

    expect(dispatcher.workerForSession(sid("ses_1"))).toBe(2)
    expect(dispatcher.registry.has(sid("ses_1"))).toBe(true)
    expect(dispatcher.busySlotCount).toBe(1)
    expect(terminated).toEqual([1])

    rejections[1](new SessionCancelledError({ message: "cleanup", sessionID: sid("ses_1") }))
    await flush()
    expect((await second).ok).toBe(false)
    await dispatcher.terminateAll()
  })

  test("terminate clears the map and rejects in-flight sessions", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })
    const running = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()
    expect(dispatcher.registry.size).toBe(1)

    await dispatcher.terminateAll()
    expect(dispatcher.registry.size).toBe(0)
    await expect(running).rejects.toBeInstanceOf(SessionCancelledError)
  })
})

describe("Pending settlement is single-owner", () => {
  test("completion that wins the cancel race settles the caller exactly once", async () => {
    const pool = createFakeWorkerPool(1)
    const barrier = Deferred.makeUnsafe<void>()
    const barrierCalls: SessionID[] = []
    const events: string[] = []
    const dispatcher = createDispatcher({
      pool,
      writeBarrier: (sessionID) => {
        barrierCalls.push(sessionID)
        return Effect.runPromise(Deferred.await(barrier))
      },
      cancellation: {
        portFor: (worker) => (worker as FakeSessionWorker).cancelPort,
        graceMs: 500,
        schedule: createTestScheduler().schedule,
        onCancelled: (event) => events.push(event.mode),
      },
    })

    const sessionID = sid("ses_once")
    let settlements = 0
    const run = dispatcher.dispatch({ sessionID })
    void run.then(
      () => {
        settlements += 1
      },
      () => {
        settlements += 1
      },
    )
    await pollUntil(() => pool.spawned.length === 1, { message: "the run never started" })

    // Completion lands before the cancel request; the controller must observe it,
    // start the completion barrier, and the caller must settle exactly once.
    pool.spawned[0].finish(sessionID)
    await pollUntil(() => barrierCalls.length === 1, { message: "the completion barrier never started" })
    const cancelling = dispatcher.cancelWithGrace(sessionID)
    await pollUntil(() => settlements === 1, { message: "the caller never settled" })
    expect(events).toEqual(["completed"])

    // Only release/promotion waits for the barrier; the caller already settled.
    Deferred.doneUnsafe(barrier, Effect.void)
    expect(await cancelling).toMatchObject({
      mode: "completed",
      winner: "completion",
    })
    await run
    await flush()
    expect(settlements).toBe(1)
    expect(barrierCalls).toEqual([sessionID])
    await dispatcher.terminateAll()
  })

  test("a completion-winner cancel keeps a parked waiter parked until the write barrier resolves", async () => {
    const pool = createFakeWorkerPool(1)
    const barrier = Deferred.makeUnsafe<void>()
    const barrierCalls: SessionID[] = []
    const dispatcher = createDispatcher({
      pool,
      writeBarrier: (sessionID) => {
        barrierCalls.push(sessionID)
        return Effect.runPromise(Deferred.await(barrier))
      },
      cancellation: {
        portFor: (worker) => (worker as FakeSessionWorker).cancelPort,
        graceMs: 500,
        schedule: createTestScheduler().schedule,
      },
    })

    const sessionID = sid("ses_completion_barrier_waiter")
    const root = capture(dispatcher.dispatch({ sessionID }))
    await pollUntil(() => pool.spawned.length === 1, { message: "the root run never started" })

    pool.spawned[0].finish(sessionID)
    await pollUntil(() => barrierCalls.length === 1, { message: "the write barrier never started" })

    // Start the cancel first: it drains the waiters that existed at request
    // time, then suspends on the boot probe. Park a new waiter during that
    // window; completion wins the race, but this waiter must stay parked until
    // the write barrier resolves.
    const cancelling = dispatcher.cancelWithGrace(sessionID)
    const waiter = capture(dispatcher.dispatch({ sessionID, depth: 1 }))
    await pollUntil(() => dispatcher.queuedCount === 1, { message: "the waiter was never parked" })

    // The caller settles at cancel settlement; only release/promotion waits.
    expect((await root).ok).toBe(true)
    expect(dispatcher.workerForSession(sessionID)).toBe(pool.spawned[0].id)
    expect(dispatcher.busySlotCount).toBe(1)
    expect(dispatcher.queuedCount).toBe(1)
    expect(pool.spawned[0].runs).toHaveLength(1)
    expect(barrierCalls).toEqual([sessionID])

    Deferred.doneUnsafe(barrier, Effect.void)
    expect(await cancelling).toMatchObject({
      mode: "completed",
      winner: "completion",
    })
    await pollUntil(() => pool.spawned[0].runs.length === 2, {
      message: "the waiter was not promoted after the write barrier resolved",
    })
    expect(barrierCalls).toEqual([sessionID])
    pool.spawned[0].finish(sessionID)
    await pollUntil(() => barrierCalls.length === 2, {
      message: "the promoted run did not take its own write barrier",
    })
    expect(await waiter).toEqual({ ok: true, value: undefined })
    await dispatcher.terminateAll()
  })

  test("a cancel-first grace completion still runs the write barrier before release", async () => {
    const pool = createFakeWorkerPool(1, { onCancel: "ignore", acknowledge: false })
    const barrier = Deferred.makeUnsafe<void>()
    const barrierCalls: SessionID[] = []
    const dispatcher = createDispatcher({
      pool,
      writeBarrier: (sessionID) => {
        barrierCalls.push(sessionID)
        return Effect.runPromise(Deferred.await(barrier))
      },
      cancellation: {
        portFor: (worker) => (worker as FakeSessionWorker).cancelPort,
        graceMs: 500,
        schedule: createTestScheduler().schedule,
      },
    })

    const sessionID = sid("ses_cancel_first_barrier")
    const root = capture(dispatcher.dispatch({ sessionID }))
    await pollUntil(() => pool.spawned.length === 1, { message: "the root run never started" })

    // The cancel opens the grace window first; the worker ignores the frame so
    // the test can complete the run while `cancelPending` is true.
    const cancelling = dispatcher.cancelWithGrace(sessionID)
    const cancelPort = pool.spawned[0].cancelPort as FakeCancelPort
    await pollUntil(() => cancelPort.sent.some((frame) => frame.type === "cancel"), {
      message: "the cancel frame was never delivered",
    })
    const waiter = capture(dispatcher.dispatch({ sessionID, depth: 1 }))
    await pollUntil(() => dispatcher.queuedCount === 1, { message: "the waiter was never parked" })

    pool.spawned[0].finish(sessionID)
    await pollUntil(() => barrierCalls.length === 1, {
      message: "a completion during the cancel grace skipped the write barrier",
    })
    await flush()

    // The caller settles at cancel settlement; only the release/promotion waits
    // for the barrier.
    expect(await root).toEqual({ ok: true, value: undefined })
    // The barrier owns the release: the worker is retained and the waiter stays
    // parked even though the cancel request already reported completion.
    expect(dispatcher.workerForSession(sessionID)).toBe(pool.spawned[0].id)
    expect(dispatcher.busySlotCount).toBe(1)
    expect(dispatcher.queuedCount).toBe(1)
    expect(pool.spawned[0].runs).toHaveLength(1)

    Deferred.doneUnsafe(barrier, Effect.void)
    expect(await cancelling).toMatchObject({ mode: "completed", winner: "completion" })
    await pollUntil(() => pool.spawned[0].runs.length === 2, {
      message: "the waiter was not promoted after the write barrier resolved",
    })
    expect(await root).toEqual({ ok: true, value: undefined })
    pool.spawned[0].finish(sessionID)
    await pollUntil(() => barrierCalls.length === 2, {
      message: "the promoted run did not take its own write barrier",
    })
    expect(await waiter).toEqual({ ok: true, value: undefined })
    await dispatcher.terminateAll()
  })

  test("an acknowledged cancel waits for the write barrier before admitting a parked waiter", async () => {
    // Acknowledge the cancel so cancel wins, but ignore the frame so the run can
    // complete on its own and install the write barrier during the grace window.
    const pool = createFakeWorkerPool(1, { onCancel: "ignore" })
    const barrier = Deferred.makeUnsafe<void>()
    const barrierCalls: SessionID[] = []
    const dispatcher = createDispatcher({
      pool,
      writeBarrier: (sessionID) => {
        barrierCalls.push(sessionID)
        return Effect.runPromise(Deferred.await(barrier))
      },
      cancellation: {
        portFor: (worker) => (worker as FakeSessionWorker).cancelPort,
        graceMs: 500,
        schedule: createTestScheduler().schedule,
      },
    })

    const sessionID = sid("ses_ack_cancel_barrier_waiter")
    const root = capture(dispatcher.dispatch({ sessionID }))
    await pollUntil(() => pool.spawned.length === 1, { message: "the root run never started" })

    const cancelling = dispatcher.cancelWithGrace(sessionID)
    const cancelPort = pool.spawned[0].cancelPort as FakeCancelPort
    await pollUntil(() => cancelPort.sent.some((frame) => frame.type === "cancel"), {
      message: "the cancel frame was never delivered",
    })
    const waiter = capture(dispatcher.dispatch({ sessionID, depth: 1 }))
    await pollUntil(() => dispatcher.queuedCount === 1, { message: "the waiter was never parked" })

    pool.spawned[0].finish(sessionID)
    await pollUntil(() => barrierCalls.length === 1, {
      message: "a completion during the acknowledged cancel skipped the write barrier",
    })
    // The caller is settled as-is (it never waits for durability).
    expect(await root).toMatchObject({ ok: false, error: expect.any(SessionCancelledError) })
    await flush()

    // The barrier still gates admission: the worker is retained and the waiter
    // stays parked until the barrier resolves.
    expect(dispatcher.workerForSession(sessionID)).toBe(pool.spawned[0].id)
    expect(dispatcher.busySlotCount).toBe(1)
    expect(dispatcher.queuedCount).toBe(1)
    expect(pool.spawned[0].runs).toHaveLength(1)
    expect(barrierCalls).toEqual([sessionID])

    Deferred.doneUnsafe(barrier, Effect.void)
    expect(await cancelling).toMatchObject({
      mode: "cooperative",
      winner: "cancel",
      acknowledged: true,
    })
    await pollUntil(() => pool.spawned[0].runs.length === 2, {
      message: "the waiter was not promoted after the barrier resolved",
    })
    // The old barrier resolved before promotion, so it cannot delete the
    // successor's session/worker handle.
    expect(dispatcher.workerForSession(sessionID)).toBe(pool.spawned[0].id)
    expect(dispatcher.registry.has(sessionID)).toBe(true)
    expect(dispatcher.busySlotCount).toBe(1)

    pool.spawned[0].finish(sessionID)
    await pollUntil(() => barrierCalls.length === 2, {
      message: "the promoted run did not take its own write barrier",
    })
    expect(await waiter).toEqual({ ok: true, value: undefined })
    await dispatcher.terminateAll()
  })

  test("a write barrier that never settles is bounded and finalizes once", async () => {
    const pool = createFakeWorkerPool(1)
    const { bridge, lines } = recordingLogger()
    const barrierCalls: SessionID[] = []
    const dispatcher = createDispatcher({
      pool,
      bridge,
      writeBarrierTimeoutMs: 25,
      writeBarrier: (sessionID) => {
        barrierCalls.push(sessionID)
        return new Promise<void>(() => {})
      },
    })

    const sessionID = sid("ses_write_barrier_timeout")
    const run = capture(dispatcher.dispatch({ sessionID }))
    await pollUntil(() => pool.spawned.length === 1, { message: "the run never started" })
    pool.spawned[0].finish(sessionID)

    const outcome = await Promise.race([
      run.then(() => "resolved" as const),
      new Promise<"timeout">((resolve) => setTimeout(() => resolve("timeout"), 1_000)),
    ])

    expect(outcome).toBe("resolved")
    expect(await run).toEqual({ ok: true, value: undefined })
    expect(barrierCalls).toEqual([sessionID])
    expect(dispatcher.busySlotCount).toBe(0)
    expect(dispatcher.workerForSession(sessionID)).toBeUndefined()
    expect(lines.filter((line) => line.message === workerLogMessage("session write barrier timed out"))).toHaveLength(1)
    expect(lines.filter((line) => line.message === workerLogMessage("session completed"))).toHaveLength(1)
    await dispatcher.terminateAll()
  })

  test("an immediate write barrier deadline ignores a rejecting barrier without an unhandled rejection", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({
      pool,
      writeBarrierTimeoutMs: 0,
      // The deadline already elapsed, so the rejection must be observed and
      // discarded instead of surfacing as an unhandled rejection.
      writeBarrier: () => Promise.reject(new Error("barrier failed after the deadline")),
    })

    const sessionID = sid("ses_write_barrier_immediate_reject")
    const run = capture(dispatcher.dispatch({ sessionID }))
    await pollUntil(() => pool.spawned.length === 1, { message: "the run never started" })
    pool.spawned[0].finish(sessionID)

    expect(await run).toEqual({ ok: true, value: undefined })
    expect(dispatcher.busySlotCount).toBe(0)
    expect(dispatcher.workerForSession(sessionID)).toBeUndefined()
    // Let any late unhandled rejection surface before the test ends.
    await flush()
    await dispatcher.terminateAll()
  })

  test("a throwing write barrier timeout logger still resolves the run", async () => {
    const pool = createFakeWorkerPool(1)
    const sink = Logger.make<unknown, void>(({ message }) => {
      const [head] = Array.isArray(message) ? message : [message]
      if (head === workerLogMessage("session write barrier timed out")) throw new Error("logger exploded")
    })
    const dispatcher = createDispatcher({
      pool,
      bridge: recordingBridge(sink),
      writeBarrierTimeoutMs: 0,
      writeBarrier: () => new Promise<void>(() => {}),
    })

    const sessionID = sid("ses_write_barrier_throwing_log")
    const run = capture(dispatcher.dispatch({ sessionID }))
    await pollUntil(() => pool.spawned.length === 1, { message: "the run never started" })
    pool.spawned[0].finish(sessionID)

    const outcome = await Promise.race([
      run.then(() => "resolved" as const),
      new Promise<"timeout">((resolve) => setTimeout(() => resolve("timeout"), 1_000)),
    ])

    expect(outcome).toBe("resolved")
    expect(await run).toEqual({ ok: true, value: undefined })
    expect(dispatcher.busySlotCount).toBe(0)
    expect(dispatcher.workerForSession(sessionID)).toBeUndefined()
    await dispatcher.terminateAll()
  })

  test("a throwing positive write barrier timeout logger never escapes the timer", async () => {
    const pool = createFakeWorkerPool(1)
    const uncaught: unknown[] = []
    const onUncaught = (error: unknown) => uncaught.push(error)
    process.on("uncaughtException", onUncaught)
    try {
      const sink = Logger.make<unknown, void>(({ message }) => {
        const [head] = Array.isArray(message) ? message : [message]
        if (head === workerLogMessage("session write barrier timed out")) throw new Error("logger exploded")
      })
      const dispatcher = createDispatcher({
        pool,
        bridge: recordingBridge(sink),
        // Positive deadline: the diagnostic runs from the timer callback, so a
        // throw there must not surface as an uncaught host exception.
        writeBarrierTimeoutMs: 10,
        writeBarrier: () => new Promise<void>(() => {}),
      })

      const sessionID = sid("ses_write_barrier_positive_throwing_log")
      const run = capture(dispatcher.dispatch({ sessionID }))
      await pollUntil(() => pool.spawned.length === 1, { message: "the run never started" })
      pool.spawned[0].finish(sessionID)

      const outcome = await Promise.race([
        run.then(() => "resolved" as const),
        new Promise<"timeout">((resolve) => setTimeout(() => resolve("timeout"), 1_000)),
      ])

      expect(outcome).toBe("resolved")
      expect(await run).toEqual({ ok: true, value: undefined })
      expect(uncaught).toEqual([])
      expect(dispatcher.busySlotCount).toBe(0)
      expect(dispatcher.workerForSession(sessionID)).toBeUndefined()
      await dispatcher.terminateAll()
    } finally {
      process.off("uncaughtException", onUncaught)
    }
  })

  test("a superseded run's late completion cannot invoke the successor's write barrier", async () => {
    let finishCancelled!: () => void
    let finishSuccessor!: () => void
    let nextID = 1
    const spawned: number[] = []
    const terminated: number[] = []
    const pool: WorkerPool = {
      spawn: () => {
        const id = nextID++
        spawned.push(id)
        return {
          id,
          alive: true,
          execute: () =>
            new Promise<void>((resolve) => {
              if (id === 1) finishCancelled = resolve
              else finishSuccessor = resolve
            }),
          terminate: async () => {
            terminated.push(id)
          },
        }
      },
      terminateAll: async () => {},
    }
    const barrierCalls: SessionID[] = []
    const dispatcher = createDispatcher({
      pool,
      writeBarrier: (sessionID) => {
        barrierCalls.push(sessionID)
        return undefined
      },
    })

    const sessionID = sid("ses_superseded")
    const cancelledRun = capture(dispatcher.dispatch({ sessionID }))
    await pollUntil(() => spawned.length === 1, { message: "the first run never started" })
    await dispatcher.cancel(sessionID)
    expect((await cancelledRun).ok).toBe(false)

    // The successor now owns the same session on a fresh worker.
    const successor = capture(dispatcher.dispatch({ sessionID, depth: 1 }))
    await pollUntil(() => dispatcher.workerForSession(sessionID) === 2, {
      message: "the successor never took the session",
    })

    // The cancelled run's execution promise resolves arbitrarily late.
    finishCancelled()
    await flush()

    expect(barrierCalls).toEqual([])
    expect(dispatcher.workerForSession(sessionID)).toBe(2)
    expect(dispatcher.registry.has(sessionID)).toBe(true)
    expect(dispatcher.busySlotCount).toBe(1)
    expect(terminated).toEqual([1])

    finishSuccessor()
    expect(await successor).toEqual({ ok: true, value: undefined })
    await dispatcher.terminateAll()
  })

  test("a parked waiter is promoted exactly once after the admitted run settles", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })
    const sessionID = sid("ses_promote")
    let rootSettlements = 0
    const root = dispatcher.dispatch({ sessionID })
    void root.then(
      () => {
        rootSettlements += 1
      },
      () => {
        rootSettlements += 1
      },
    )
    const waiter = capture(dispatcher.dispatch({ sessionID, depth: 1 }))
    await pollUntil(() => dispatcher.queuedCount === 1, { message: "the waiter was never parked" })

    pool.spawned[0].finish(sessionID)
    await root
    await pollUntil(() => pool.spawned[0].runs.length === 2, {
      message: "the waiter was never promoted",
    })

    expect(pool.spawned[0].runs.map((run) => run.depth)).toEqual([0, 1])
    expect(dispatcher.queuedCount).toBe(0)
    expect(dispatcher.busySlotCount).toBe(1)
    expect(rootSettlements).toBe(1)

    pool.spawned[0].finish(sessionID)
    expect(await waiter).toEqual({ ok: true, value: undefined })
    await pollUntil(() => !dispatcher.registry.has(sessionID), {
      message: "the promoted run never released the session",
    })
    expect(dispatcher.queuedCount).toBe(0)
    await dispatcher.terminateAll()
  })
})

describe("crash error identity", () => {
  test("WorkerCrashedError carries its own _tag with no prototype reparenting", () => {
    const crash = new WorkerCrashedError({ message: "worker 1 crashed", workerID: 1 })
    expect(crash._tag).toBe("WorkerCrashedError")
    expect(crash).toBeInstanceOf(WorkerCrashedError)
    expect(Object.getPrototypeOf(crash)).toBe(WorkerCrashedError.prototype)
  })

  test("Effect.catchTag matches the WorkerCrashedError tag", async () => {
    const caught = await Effect.runPromise(
      Effect.fail(new WorkerCrashedError({ message: "boom", workerID: 1 })).pipe(
        Effect.catchTag("WorkerCrashedError", () => Effect.succeed("worker")),
        Effect.orElseSucceed(() => "missed"),
      ),
    )
    expect(caught).toBe("worker")
  })

  test("a worker that dies with a real error surfaces its message and cause", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })
    const running = capture(dispatcher.dispatch({ sessionID: sid("ses_crash_message") }))
    await flush()

    const cause = new Error("ModuleNotFound resolving './session-worker.ts' (entry point)")
    pool.spawned[0].crashWith(sid("ses_crash_message"), cause)

    const settledRun = await running
    if (settledRun.ok) throw new Error("expected the crashed run to reject")
    const error = settledRun.error as WorkerCrashedError
    expect(error).toBeInstanceOf(WorkerCrashedError)
    expect(error.message).toContain("ModuleNotFound resolving './session-worker.ts'")
    expect(error.cause).toBe(cause)
    await dispatcher.terminateAll()
  })
})

describe("dispatch validation and waiters", () => {
  test("an invalid depth rejects the returned promise instead of throwing", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })
    let outcome: Promise<void> | undefined
    expect(() => {
      outcome = dispatcher.dispatch({ sessionID: sid("ses_bad"), depth: -1 })
    }).not.toThrow()
    await expect(outcome!).rejects.toThrow(/invalid dispatch depth/)
    await dispatcher.terminateAll()
  })

  test("different-run waiters are unbounded and promote in arrival order", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })
    const root = capture(dispatcher.dispatch({ sessionID: sid("ses_1") }))
    const waiters = Array.from({ length: 100 }, (_, index) =>
      capture(dispatcher.dispatch({ sessionID: sid("ses_1"), depth: index + 1 })),
    )
    await flush()
    expect(dispatcher.queuedCount).toBe(100)

    pool.spawned[0].finish(sid("ses_1"))
    await root
    await flush()
    expect(pool.spawned[0].runs).toHaveLength(2)
    expect(pool.spawned[0].runs[1].depth).toBe(1)

    await dispatcher.terminateAll()
    await Promise.allSettled(waiters)
  })
})

describe("execution seam forwards depth and the startShell frame", () => {
  function recordingScheduler(): { scheduler: SessionScheduler; calls: DispatchRequest[] } {
    const calls: DispatchRequest[] = []
    const scheduler: SessionScheduler = {
      dispatch: async (request) => {
        calls.push(request)
      },
      cancel: async () => {},
      terminateAll: async () => {},
      registry: { get: () => undefined },
    }
    return { scheduler, calls }
  }

  function resultPool(): ExecutorWorkerPool {
    return {
      workers: [],
      spawn: () => {
        throw new Error("the pool must not be asked to spawn for a recorded dispatch")
      },
      terminateAll: async () => {},
      byID: () => undefined,
      resultFor: (sessionID) => ({ sessionID }),
    }
  }

  test("ensureRunning carries an explicit depth into the dispatch request", async () => {
    const { scheduler, calls } = recordingScheduler()
    const dispatch = createSessionDispatch({ dispatcher: scheduler, pool: resultPool() })
    await dispatch.ensureRunning(sid("ses_child"), 1)
    expect(calls).toEqual([{ sessionID: sid("ses_child"), depth: 1, mode: "run" }])
  })

  test("ensureRunning derives depth from depthOf when none is passed", async () => {
    const { scheduler, calls } = recordingScheduler()
    const dispatch = createSessionDispatch({
      dispatcher: scheduler,
      pool: resultPool(),
      depthOf: (sessionID) => (sessionID === "ses_child" ? 2 : 0),
    })
    await dispatch.ensureRunning(sid("ses_child"))
    expect(calls).toEqual([{ sessionID: sid("ses_child"), depth: 2, mode: "run" }])
  })

  test("startShell dispatches the startShell mode (not a plain run)", async () => {
    const { scheduler, calls } = recordingScheduler()
    const dispatch = createSessionDispatch({ dispatcher: scheduler, pool: resultPool() })
    await dispatch.startShell(sid("ses_shell"))
    expect(calls).toEqual([{ sessionID: sid("ses_shell"), depth: 0, mode: "startShell" }])
  })

  test("createExecutorWorker posts a startShell frame for a startShell-mode run", async () => {
    const posted: SessionExecutorInbound[] = []
    const transport: ExecutorTransport = {
      post: (message) => {
        posted.push(message)
      },
      waitFor: async (match) => {
        const frame = posted[posted.length - 1] as { readonly id: number; readonly sessionID?: SessionID }
        const reply = { type: "done" as const, id: frame.id, result: { sessionID: frame.sessionID ?? "ses" } }
        return match(reply) ? reply : reply
      },
      alive: true,
      terminate: async () => 0,
    }
    const worker = createExecutorWorker({ id: 1, transport, workKind: "session" })

    await worker.execute({ sessionID: sid("ses_child"), depth: 1, mode: "startShell" })
    await worker.execute({ sessionID: sid("ses_root"), depth: 0 })

    expect(posted[0]).toMatchObject({ type: "startShell", sessionID: "ses_child", kind: "session" })
    expect(posted[1]).toMatchObject({ type: "run", sessionID: "ses_root", kind: "session" })
  })
})

// ---------------------------------------------------------------------------
// Cancellation.
// ---------------------------------------------------------------------------

describe("cancelWithGrace and reaping seams", () => {
  test("the terminate path invokes the reap seam with the worker", async () => {
    const pool = createFakeWorkerPool(1)
    const reaped: number[] = []
    const dispatcher = createDispatcher({
      pool,
      cancellation: { reap: (worker) => reaped.push(worker.id) },
    })

    const running = capture(dispatcher.dispatch({ sessionID: sid("ses_reap") }))
    await flush()
    await dispatcher.cancelWithGrace(sid("ses_reap"))

    expect(await running).toMatchObject({ ok: false, error: expect.any(SessionCancelledError) })
    expect(reaped).toEqual([pool.spawned[0].id])
    await dispatcher.terminateAll()
  })

  test("terminateAll invokes the reapAll seam exactly once", async () => {
    const pool = createFakeWorkerPool(1)
    let reapAllCalls = 0
    const dispatcher = createDispatcher({
      pool,
      cancellation: {
        reapAll: () => {
          reapAllCalls += 1
        },
      },
    })

    const running = capture(dispatcher.dispatch({ sessionID: sid("ses_reap_all") }))
    await flush()
    await dispatcher.terminateAll()
    await running
    await dispatcher.terminateAll()
    expect(reapAllCalls).toBe(1)
  })

  test("a cooperative ack whose run actually unwound reuses the worker and does not reap", async () => {
    const pool = createFakeWorkerPool(1, { onCancel: "resolve" })
    const reaped: number[] = []
    const dispatcher = createDispatcher({
      pool,
      cancellation: {
        portFor: (worker) => (worker as FakeSessionWorker).cancelPort,
        graceMs: 500,
        schedule: createTestScheduler().schedule,
        reap: (worker) => reaped.push(worker.id),
      },
    })

    const running = capture(dispatcher.dispatch({ sessionID: sid("ses_unwound") }))
    await flush()
    const outcome = await dispatcher.cancelWithGrace(sid("ses_unwound"))

    expect(outcome).toMatchObject({ mode: "cooperative", winner: "cancel", acknowledged: true })
    expect(pool.spawned[0].alive).toBe(true)
    expect(reaped).toEqual([])
    expect(await running).toMatchObject({ ok: false, error: expect.any(SessionCancelledError) })
    await dispatcher.terminateAll()
  })

  test("grace expiry terminates a still-running worker and reaps it exactly once", async () => {
    const pool = createFakeWorkerPool(1, { onCancel: "ignore" })
    const scheduler = createTestScheduler()
    const reaped: number[] = []
    const dispatcher = createDispatcher({
      pool,
      cancellation: {
        portFor: (worker) => (worker as FakeSessionWorker).cancelPort,
        graceMs: 500,
        schedule: scheduler.schedule,
        reap: (worker) => reaped.push(worker.id),
      },
    })

    const running = capture(dispatcher.dispatch({ sessionID: sid("ses_grace") }))
    await flush()
    const pending = dispatcher.cancelWithGrace(sid("ses_grace"))
    await flush()

    scheduler
      .pending()
      .find((timer) => timer.delayMs === 500)
      ?.run()
    const outcome = await pending

    expect(outcome).toMatchObject({ mode: "terminated", winner: "cancel", acknowledged: true })
    expect(pool.spawned[0].alive).toBe(false)
    expect(reaped).toEqual([pool.spawned[0].id])
    expect(await running).toMatchObject({ ok: false, error: expect.any(SessionCancelledError) })
    await dispatcher.terminateAll()
  })
})

// ---------------------------------------------------------------------------
// Observability logging.
// ---------------------------------------------------------------------------

interface RecordedLog {
  readonly message: string
  readonly fields: Record<string, unknown>
  readonly level: string
}

function recordingLogger(): { bridge: ReturnType<typeof recordingBridge>; lines: RecordedLog[] } {
  const lines: RecordedLog[] = []
  const sink = Logger.make<unknown, void>(({ logLevel, message }) => {
    const [head, fields] = Array.isArray(message) ? message : [message, undefined]
    lines.push({
      message: typeof head === "string" ? head : String(head),
      fields: typeof fields === "object" && fields !== null ? (fields as Record<string, unknown>) : {},
      level: logLevel.toLowerCase(),
    })
  })
  return { bridge: recordingBridge(sink), lines }
}

describe("dispatch observability logging", () => {
  test("session admitted (INFO) carries worker and depth", async () => {
    const pool = createFakeWorkerPool(1)
    const { bridge, lines } = recordingLogger()
    const dispatcher = createDispatcher({ pool, bridge })

    const run = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()

    const admitted = lines.find((line) => line.message === workerLogMessage("session admitted"))
    expect(admitted).toBeDefined()
    expect(admitted!.level).toBe("info")
    expect(admitted!.fields).toMatchObject({
      sessionID: "ses_1",
      workerID: pool.spawned[0].id,
      depth: 0,
    })
    expect(typeof admitted!.fields.queueDepth).toBe("number")

    pool.spawned[0].finish(sid("ses_1"))
    await run
    await dispatcher.terminateAll()
  })

  test("session completed (INFO) carries the worker and a non-negative duration", async () => {
    const pool = createFakeWorkerPool(1)
    const { bridge, lines } = recordingLogger()
    const dispatcher = createDispatcher({ pool, bridge })

    const run = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()
    pool.spawned[0].finish(sid("ses_1"))
    await run

    const done = lines.find((line) => line.message === workerLogMessage("session completed"))
    expect(done).toBeDefined()
    expect(done!.level).toBe("info")
    expect(done!.fields).toMatchObject({ sessionID: "ses_1", workerID: pool.spawned[0].id })
    expect(typeof done!.fields.durationMs).toBe("number")
    await dispatcher.terminateAll()
  })

  test("waiter promoted (INFO) carries the remaining waiter depth", async () => {
    const pool = createFakeWorkerPool(1)
    const { bridge, lines } = recordingLogger()
    const dispatcher = createDispatcher({ pool, bridge })

    const root = capture(dispatcher.dispatch({ sessionID: sid("ses_1") }))
    const waiter = capture(dispatcher.dispatch({ sessionID: sid("ses_1"), depth: 1 }))
    await flush()

    pool.spawned[0].finish(sid("ses_1"))
    await root
    await flush()

    const promoted = lines.find((line) => line.message === workerLogMessage("waiter promoted"))
    expect(promoted).toBeDefined()
    expect(promoted!.level).toBe("info")
    expect(promoted!.fields).toMatchObject({ sessionID: "ses_1" })
    expect(typeof promoted!.fields.waiterQueueDepth).toBe("number")

    await dispatcher.terminateAll()
    await Promise.allSettled([waiter])
  })

  test("worker crashed (WARN) carries the crash", async () => {
    const pool = createFakeWorkerPool(1)
    const { bridge, lines } = recordingLogger()
    const dispatcher = createDispatcher({ pool, bridge })

    const crashed = capture(dispatcher.dispatch({ sessionID: sid("ses_1") }))
    await flush()
    pool.spawned[0].crash(sid("ses_1"))
    await crashed

    const line = lines.find((entry) => entry.message === workerLogMessage("worker crashed"))
    expect(line).toBeDefined()
    expect(line!.level).toBe("warn")
    expect(line!.fields).toMatchObject({ sessionID: "ses_1" })
    expect(typeof line!.fields.workerID).toBe("number")
    await dispatcher.terminateAll()
  })

  test("session worker reaped (INFO) carries the idle window", async () => {
    const pool = createFakeWorkerPool(1)
    const scheduler = createTestScheduler()
    const { bridge, lines } = recordingLogger()
    const dispatcher = createDispatcher({ pool, bridge, schedule: scheduler.schedule, reapMs: 60_000 })

    const parent = dispatcher.dispatch({ sessionID: sid("ses_parent") })
    await flush()
    pool.spawned[0].finish(sid("ses_parent"))
    await parent
    await flush()

    scheduler
      .pending()
      .find((timer) => timer.delayMs === 60_000)
      ?.run()
    await flush()

    const line = lines.find((entry) => entry.message === workerLogMessage("session worker reaped"))
    expect(line).toBeDefined()
    expect(line!.level).toBe("info")
    expect(line!.fields).toMatchObject({ idleMs: 60_000, sessionID: "ses_parent" })
    expect(typeof line!.fields.workerID).toBe("number")
    await dispatcher.terminateAll()
  })

  test("an idle timer cleared by a new turn cannot retire the live worker", async () => {
    const pool = createFakeWorkerPool(1)
    const armed: Array<() => void> = []
    const dispatcher = createDispatcher({
      pool,
      reapMs: 1_000,
      // The timer is already queued in the host: cancelling it is best-effort,
      // so the dispatcher itself must make the stale callback harmless.
      schedule: (_delayMs, run) => {
        armed.push(run)
        return () => {}
      },
    })
    const sessionID = sid("ses_stale_reap")

    const first = dispatcher.dispatch({ sessionID })
    await flush()
    pool.spawned[0].finish(sessionID)
    await first
    await flush()
    expect(armed).toHaveLength(1)

    // A new turn resets the window. The stale callback below still fires after
    // the replacement window is armed.
    const second = dispatcher.dispatch({ sessionID })
    await flush()
    pool.spawned[0].finish(sessionID)
    await second
    await flush()
    expect(armed).toHaveLength(2)

    armed[0]!()
    await flush()

    // The cleared timer was interrupted: it must not retire the worker or
    // consume the replacement window.
    expect(pool.spawned[0].alive).toBe(true)
    expect(dispatcher.registry.entry(sessionID)).toBeDefined()

    armed[1]!()
    await flush()
    expect(pool.spawned[0].alive).toBe(false)
    expect(dispatcher.registry.entry(sessionID)).toBeUndefined()
    await dispatcher.terminateAll()
  })

  test("a scheduler that resumes synchronously still arms a working reap timer", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({
      pool,
      reapMs: 1_000,
      // The scheduler seam allows a callback to run during registration. The
      // timer identity fence must already be installed before that callback.
      schedule: (_delayMs, run) => {
        run()
        return () => {}
      },
    })
    const sessionID = sid("ses_sync_reap")

    const run = dispatcher.dispatch({ sessionID })
    await flush()
    pool.spawned[0].finish(sessionID)
    await run

    await pollUntil(() => !dispatcher.registry.has(sessionID) && !pool.spawned[0].alive, {
      message: "a synchronously fired reap callback was lost",
      timeoutMs: 1_000,
    })
    expect(pool.spawned[0].alive).toBe(false)
    await dispatcher.terminateAll()
  })

  test("a plain session failure logs an ERROR 'worker run failed' line", async () => {
    const pool = createFakeWorkerPool(1)
    const { bridge, lines } = recordingLogger()
    const dispatcher = createDispatcher({ pool, bridge })

    const failing = capture(dispatcher.dispatch({ sessionID: sid("ses_1") }))
    await flush()
    pool.spawned[0].fail(sid("ses_1"), new Error("No user message found in stream"))
    await failing

    const line = lines.find((entry) => entry.message === workerLogMessage("worker run failed"))
    expect(line).toBeDefined()
    expect(line!.level).toBe("error")
    expect(line!.fields).toMatchObject({
      sessionID: "ses_1",
      workerID: pool.spawned[0].id,
    })
    expect(String(line!.fields.error)).toContain("No user message found in stream")
    await dispatcher.terminateAll()
  })

  test("cancelWithGrace emits 'cancel requested' with sessionID, workerID and graceMs", async () => {
    const pool = createFakeWorkerPool(1, { onCancel: "ignore" })
    const scheduler = createTestScheduler()
    const { bridge, lines } = recordingLogger()
    const dispatcher = createDispatcher({
      pool,
      bridge,
      cancellation: {
        portFor: (worker) => (worker as FakeSessionWorker).cancelPort,
        graceMs: 750,
        schedule: scheduler.schedule,
      },
    })

    const running = capture(dispatcher.dispatch({ sessionID: sid("ses_d1") }))
    await flush()
    const pending = dispatcher.cancelWithGrace(sid("ses_d1"))
    await flush()

    const line = lines.find((entry) => entry.message === workerLogMessage("cancel requested"))
    expect(line).toBeDefined()
    expect(line!.fields).toEqual({ sessionID: "ses_d1", workerID: pool.spawned[0].id, graceMs: 750 })

    scheduler
      .pending()
      .find((timer) => timer.delayMs === 750)
      ?.run()
    await pending
    expect(await running).toMatchObject({ ok: false, error: expect.any(SessionCancelledError) })
    await dispatcher.terminateAll()
  })
})

// ---------------------------------------------------------------------------
// Prompt / shell plumbing.
// ---------------------------------------------------------------------------

describe("prompt and shell plumbing", () => {
  test("a dispatch carrying a prompt forwards it on the executor run", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })
    const prompt = { sessionID: sid("ses_child"), agent: "build", parts: [{ type: "text" as const, text: "go" }] }

    const run = capture(dispatcher.dispatch({ sessionID: sid("ses_child"), depth: 0, prompt }))
    await flush()

    expect(pool.spawned[0].runs).toEqual([{ sessionID: sid("ses_child"), depth: 0, prompt }])
    pool.spawned[0].finish(sid("ses_child"))
    await run
    await dispatcher.terminateAll()
  })

  test("a prompt-less dispatch keeps the minimal run shape", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })

    const run = capture(dispatcher.dispatch({ sessionID: sid("ses_bare"), depth: 0 }))
    await flush()

    expect(pool.spawned[0].runs).toEqual([{ sessionID: sid("ses_bare"), depth: 0 }])
    expect("prompt" in pool.spawned[0].runs[0]).toBe(false)
    pool.spawned[0].finish(sid("ses_bare"))
    await run
    await dispatcher.terminateAll()
  })

  test("a startShell dispatch carries the shell command on the run frame", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })
    const shell = { sessionID: sid("ses_shell"), agent: "build", command: "echo hi" }

    const run = capture(dispatcher.dispatch({ sessionID: sid("ses_shell"), mode: "startShell", shell }))
    await flush()

    expect(pool.spawned[0].runs).toEqual([{ sessionID: sid("ses_shell"), depth: 0, mode: "startShell", shell }])
    pool.spawned[0].finish(sid("ses_shell"))
    await run
    await dispatcher.terminateAll()
  })

  test("a shell-less startShell dispatch omits the shell field", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })

    const run = capture(dispatcher.dispatch({ sessionID: sid("ses_shell"), mode: "startShell" }))
    await flush()

    expect(pool.spawned[0].runs).toEqual([{ sessionID: sid("ses_shell"), depth: 0, mode: "startShell" }])
    expect("shell" in pool.spawned[0].runs[0]).toBe(false)
    pool.spawned[0].finish(sid("ses_shell"))
    await run
    await dispatcher.terminateAll()
  })

  test("an identical shell reference coalesces onto the in-flight run", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })
    const shell: ShellInput = { sessionID: sid("ses_shell"), agent: "build", command: "echo hi" }

    const first = dispatcher.dispatch({ sessionID: sid("ses_shell"), mode: "startShell", shell })
    const second = dispatcher.dispatch({ sessionID: sid("ses_shell"), mode: "startShell", shell })
    await flush()

    expect(second).toBe(first)
    expect(pool.spawned[0].runs.map((run) => run.shell)).toEqual([shell])
    pool.spawned[0].finish(sid("ses_shell"))
    await first
    await dispatcher.terminateAll()
  })

  test("two different shell commands park instead of coalescing", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })
    const firstShell: ShellInput = { sessionID: sid("ses_shell"), agent: "build", command: "echo one" }
    const secondShell: ShellInput = { sessionID: sid("ses_shell"), agent: "build", command: "echo two" }

    const first = capture(dispatcher.dispatch({ sessionID: sid("ses_shell"), mode: "startShell", shell: firstShell }))
    const second = capture(dispatcher.dispatch({ sessionID: sid("ses_shell"), mode: "startShell", shell: secondShell }))
    await flush()

    expect(pool.spawned[0].runs.map((run) => run.shell)).toEqual([firstShell])
    pool.spawned[0].finish(sid("ses_shell"))
    await first
    await flush()
    expect(pool.spawned[0].runs.map((run) => run.shell)).toEqual([firstShell, secondShell])
    pool.spawned[0].finish(sid("ses_shell"))
    await second
    await dispatcher.terminateAll()
  })
})

// ---------------------------------------------------------------------------
// Gate / waiter draining / dedup identity / write barrier / cascade cycles.
// ---------------------------------------------------------------------------

describe("cancel claims the gate and drains waiters", () => {
  test("a running cancel reports a cancellation and logs no run failure", async () => {
    const pool = createFakeWorkerPool(1)
    const { bridge, lines } = recordingLogger()
    const dispatcher = createDispatcher({ pool, bridge })

    const running = capture(dispatcher.dispatch({ sessionID: sid("ses_cancel") }))
    await flush()
    await dispatcher.cancel(sid("ses_cancel"))
    await flush()

    expect(await running).toMatchObject({ ok: false, error: expect.any(SessionCancelledError) })
    expect(lines.find((line) => line.message === workerLogMessage("worker run failed"))).toBeUndefined()
    await dispatcher.terminateAll()
  })

  test("cancel rejects parked different-run waiters for the session", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })

    const root = capture(dispatcher.dispatch({ sessionID: sid("ses_1") }))
    const waiter = capture(dispatcher.dispatch({ sessionID: sid("ses_1"), depth: 1 }))
    await flush()

    await dispatcher.cancel(sid("ses_1"))
    expect(await root).toMatchObject({ ok: false, error: expect.any(SessionCancelledError) })
    expect(await waiter).toMatchObject({ ok: false, error: expect.any(SessionCancelledError) })
    await dispatcher.terminateAll()
  })
})

describe("single cancellation settlement", () => {
  test("cancel settles the admitted run once: reap, event, waiter, and registry", async () => {
    const pool = createFakeWorkerPool(1)
    const reaped: number[] = []
    const events: string[] = []
    let waiterRejections = 0
    const dispatcher = createDispatcher({
      pool,
      cancellation: {
        reap: (worker) => reaped.push(worker.id),
        onCancelled: (event) => events.push(`${event.sessionID}:${event.mode}`),
      },
    })

    const run = capture(dispatcher.dispatch({ sessionID: sid("ses_matrix") }))
    const waiter = dispatcher.dispatch({ sessionID: sid("ses_matrix"), depth: 1 })
    waiter.catch(() => {
      waiterRejections += 1
    })
    await flush()

    await dispatcher.cancel(sid("ses_matrix"))
    expect(await run).toMatchObject({ ok: false, error: expect.any(SessionCancelledError) })
    await expect(waiter).rejects.toBeInstanceOf(SessionCancelledError)
    await flush()

    expect(waiterRejections).toBe(1)
    expect(reaped).toEqual([pool.spawned[0].id])
    expect(events).toEqual(["ses_matrix:terminated"])
    expect(dispatcher.registry.size).toBe(0)
    expect(dispatcher.queuedCount).toBe(0)
    await dispatcher.terminateAll()
  })

  test("cancelWithGrace cooperative settles once without reaping", async () => {
    const pool = createFakeWorkerPool(1, { onCancel: "resolve" })
    const reaped: number[] = []
    const events: string[] = []
    let waiterRejections = 0
    const dispatcher = createDispatcher({
      pool,
      cancellation: {
        portFor: (worker) => (worker as FakeSessionWorker).cancelPort,
        graceMs: 500,
        schedule: createTestScheduler().schedule,
        reap: (worker) => reaped.push(worker.id),
        onCancelled: (event) => events.push(`${event.sessionID}:${event.mode}`),
      },
    })

    const run = capture(dispatcher.dispatch({ sessionID: sid("ses_matrix") }))
    const waiter = dispatcher.dispatch({ sessionID: sid("ses_matrix"), depth: 1 })
    waiter.catch(() => {
      waiterRejections += 1
    })
    await flush()

    expect(await dispatcher.cancelWithGrace(sid("ses_matrix"))).toMatchObject({ mode: "cooperative" })
    expect(await run).toMatchObject({ ok: false, error: expect.any(SessionCancelledError) })
    await expect(waiter).rejects.toBeInstanceOf(SessionCancelledError)
    await flush()

    expect(waiterRejections).toBe(1)
    expect(reaped).toEqual([])
    expect(events).toEqual(["ses_matrix:cooperative"])
    expect(dispatcher.registry.has(sid("ses_matrix"))).toBe(false)
    expect(dispatcher.queuedCount).toBe(0)
    await dispatcher.terminateAll()
  })

  test("cancelWithGrace forced settles once and reaps once", async () => {
    const pool = createFakeWorkerPool(1, { onCancel: "ignore" })
    const scheduler = createTestScheduler()
    const reaped: number[] = []
    const events: string[] = []
    const dispatcher = createDispatcher({
      pool,
      cancellation: {
        portFor: (worker) => (worker as FakeSessionWorker).cancelPort,
        graceMs: 500,
        schedule: scheduler.schedule,
        reap: (worker) => reaped.push(worker.id),
        onCancelled: (event) => events.push(`${event.sessionID}:${event.mode}`),
      },
    })

    const run = capture(dispatcher.dispatch({ sessionID: sid("ses_matrix") }))
    await flush()
    const pending = dispatcher.cancelWithGrace(sid("ses_matrix"))
    await flush()
    scheduler
      .pending()
      .find((timer) => timer.delayMs === 500)
      ?.run()

    expect(await pending).toMatchObject({ mode: "terminated" })
    expect(await run).toMatchObject({ ok: false, error: expect.any(SessionCancelledError) })
    expect(reaped).toEqual([pool.spawned[0].id])
    expect(events).toEqual(["ses_matrix:terminated"])
    await dispatcher.terminateAll()
  })

  test("cancelling an idle session settles nothing and emits no event", async () => {
    const pool = createFakeWorkerPool(1)
    const reaped: number[] = []
    const events: string[] = []
    const dispatcher = createDispatcher({
      pool,
      cancellation: {
        reap: (worker) => reaped.push(worker.id),
        onCancelled: (event) => events.push(`${event.sessionID}:${event.mode}`),
      },
    })

    await dispatcher.cancel(sid("ses_idle"))
    expect(await dispatcher.cancelWithGrace(sid("ses_idle"))).toBeUndefined()
    expect(events).toEqual([])
    expect(reaped).toEqual([])
    expect(dispatcher.registry.size).toBe(0)
    await dispatcher.terminateAll()
  })
})

describe("dedup key includes depth and prompt identity", () => {
  test("a same-session dispatch at a different depth parks instead of joining", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })

    const shallow = capture(dispatcher.dispatch({ sessionID: sid("ses_1"), depth: 0 }))
    const deep = capture(dispatcher.dispatch({ sessionID: sid("ses_1"), depth: 1 }))
    await flush()

    expect(pool.spawned[0].runs.map((run) => run.depth)).toEqual([0])
    pool.spawned[0].finish(sid("ses_1"))
    await shallow
    await flush()
    expect(pool.spawned[0].runs.map((run) => run.depth)).toEqual([0, 1])
    pool.spawned[0].finish(sid("ses_1"))
    await deep
    await dispatcher.terminateAll()
  })

  test("a same-session dispatch with a different prompt parks instead of joining", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })
    const firstPrompt = { sessionID: sid("ses_1"), agent: "build", parts: [{ type: "text" as const, text: "one" }] }
    const secondPrompt = { sessionID: sid("ses_1"), agent: "build", parts: [{ type: "text" as const, text: "two" }] }

    const first = capture(dispatcher.dispatch({ sessionID: sid("ses_1"), prompt: firstPrompt }))
    const second = capture(dispatcher.dispatch({ sessionID: sid("ses_1"), prompt: secondPrompt }))
    await flush()

    expect(pool.spawned[0].runs.map((run) => run.prompt)).toEqual([firstPrompt])
    pool.spawned[0].finish(sid("ses_1"))
    await first
    await flush()
    expect(pool.spawned[0].runs.map((run) => run.prompt)).toEqual([firstPrompt, secondPrompt])
    pool.spawned[0].finish(sid("ses_1"))
    await second
    await dispatcher.terminateAll()
  })
})

describe("per-session write barrier", () => {
  test("a completed run is not released until its write barrier resolves", async () => {
    const pool = createFakeWorkerPool(1)
    let releaseBarrier!: () => void
    const barrier = new Promise<void>((resolve) => {
      releaseBarrier = resolve
    })
    const dispatcher = createDispatcher({ pool, writeBarrier: () => barrier })

    const first = capture(dispatcher.dispatch({ sessionID: sid("ses_1") }))
    await flush()
    pool.spawned[0].finish(sid("ses_1"))
    await flush()

    expect(dispatcher.registry.has(sid("ses_1"))).toBe(true)
    const second = capture(dispatcher.dispatch({ sessionID: sid("ses_2") }))
    await flush()
    expect(pool.spawned[1].runs.map((run) => run.sessionID)).toEqual([sid("ses_2")])

    releaseBarrier()
    await flush()
    expect(await first).toEqual({ ok: true, value: undefined })
    expect(dispatcher.registry.has(sid("ses_1"))).toBe(false)

    pool.spawned[1].finish(sid("ses_2"))
    await second
    await dispatcher.terminateAll()
  })
})

describe("dead admission queue", () => {
  test("no source module imports the deleted worker queue", () => {
    const dir = path.resolve(import.meta.dir, "../../../src/session/worker")
    const offenders = readdirSync(dir)
      .filter((file) => file.endsWith(".ts"))
      .filter((file) =>
        /from\s+["'](?:@\/session\/worker\/|\.\/)queue["']/.test(readFileSync(path.join(dir, file), "utf8")),
      )

    expect(offenders).toEqual([])
    expect(existsSync(path.join(dir, "queue.ts"))).toBe(false)
  })

  test("the cancel vocabulary only lists reachable modes (no queued)", () => {
    expect(CANCELLED_MODES).toEqual(["cooperative", "terminated", "completed", "boot"])
  })
})

describe("cascade report", () => {
  test("a cyclic tree visits each descendant exactly once and reports children first", async () => {
    const pool = createFakeWorkerPool(3)
    const parent = sid("ses_parent")
    const child = sid("ses_child")
    const grandchild = sid("ses_grandchild")
    const events: string[] = []
    const childrenOf = (sessionID: SessionID): ReadonlyArray<SessionID> =>
      sessionID === parent
        ? [child]
        : sessionID === child
          ? [grandchild]
          : sessionID === grandchild
            ? [parent]
            : []
    const dispatcher = createDispatcher({
      pool,
      cancellation: {
        childrenOf,
        onCancelled: (event) => events.push(event.sessionID),
      },
    })

    const parentRun = capture(dispatcher.dispatch({ sessionID: parent }))
    const childRun = capture(dispatcher.dispatch({ sessionID: child }))
    const grandchildRun = capture(dispatcher.dispatch({ sessionID: grandchild }))
    await flush()

    const report = await dispatcher.cancelWithGrace(parent)

    expect(report?.sessionID).toBe(parent)
    expect(report?.mode).toBe("terminated")
    expect(report?.outcome.sessionID).toBe(parent)
    expect(report?.outcome.mode).toBe("terminated")
    expect(report?.descendants.map((entry) => entry.sessionID)).toEqual([grandchild, child])
    expect(report?.descendants.map((entry) => entry.mode)).toEqual(["terminated", "terminated"])
    expect(events).toEqual([grandchild, child, parent])
    for (const run of [parentRun, childRun, grandchildRun]) {
      expect(await run).toMatchObject({ ok: false, error: expect.any(SessionCancelledError) })
    }
    await dispatcher.terminateAll()
  })

  test("a diamond edge and an idle descendant are reported once with their modes", async () => {
    const pool = createFakeWorkerPool(2)
    const parent = sid("ses_parent")
    const childA = sid("ses_child_a")
    // childB is linked but never admitted: it still appears in the report with
    // no mode, and a cycle back to the parent must not revisit it.
    const childB = sid("ses_child_b")
    const childrenOf = (sessionID: SessionID): ReadonlyArray<SessionID> =>
      sessionID === parent
        ? [childA, childB]
        : sessionID === childA
          ? [childB, parent]
          : []
    const dispatcher = createDispatcher({ pool, cancellation: { childrenOf } })

    const parentRun = capture(dispatcher.dispatch({ sessionID: parent }))
    const childARun = capture(dispatcher.dispatch({ sessionID: childA }))
    await flush()

    const report = await dispatcher.cancelWithGrace(parent)

    expect(report?.descendants).toEqual([
      { sessionID: childB, mode: undefined },
      { sessionID: childA, mode: "terminated" },
    ])
    expect(await parentRun).toMatchObject({ ok: false, error: expect.any(SessionCancelledError) })
    expect(await childARun).toMatchObject({ ok: false, error: expect.any(SessionCancelledError) })
    await dispatcher.terminateAll()
  })

  test("a self-parenting session terminates instead of recursing forever", async () => {
    const pool = createFakeWorkerPool(1)
    const self = sid("ses_self")
    const dispatcher = createDispatcher({
      pool,
      cancellation: { childrenOf: (sessionID) => (sessionID === self ? [self] : []) },
    })

    const run = capture(dispatcher.dispatch({ sessionID: self }))
    await flush()
    const report = await dispatcher.cancelWithGrace(self)
    expect(report?.descendants).toEqual([])
    expect(await run).toMatchObject({ ok: false, error: expect.any(SessionCancelledError) })
    await dispatcher.terminateAll()
  })
})

describe("cyclic subagent cascade", () => {
  test("a childrenOf graph that reports a cycle terminates instead of recursing forever", async () => {
    const pool = createFakeWorkerPool(1)
    const parent = sid("ses_parent")
    const child = sid("ses_child")
    const childrenOf = (sessionID: SessionID): ReadonlyArray<SessionID> =>
      sessionID === parent ? [child] : sessionID === child ? [parent] : []
    const dispatcher: Dispatcher = createDispatcher({ pool, cancellation: { childrenOf } })

    const outcome = await dispatcher.cancelWithGrace(parent)
    expect(outcome).toBeUndefined()
    await dispatcher.terminateAll()
  })
})

describe("idle worker death", () => {
  test("a dead idle worker is retired and replaced before the next dispatch", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })

    const first = dispatcher.dispatch({ sessionID: sid("ses_reuse") })
    await flush()
    const idle = pool.spawned[0]
    idle.finish(sid("ses_reuse"))
    await first

    // The worker dies between turns while it is parked idle in the registry.
    idle.alive = false
    const originalTerminate = idle.terminate.bind(idle)
    let terminated = 0
    idle.terminate = async () => {
      terminated += 1
      await originalTerminate()
    }

    const second = dispatcher.dispatch({ sessionID: sid("ses_reuse") })
    await flush()

    // The dead handle is closed and a fresh worker materialized; the next run
    // never executes on the dead isolate.
    expect(terminated).toBe(1)
    expect(pool.spawned).toHaveLength(2)
    expect(pool.spawned[1]).not.toBe(idle)
    expect(pool.spawned[1].runs.map((run) => run.sessionID)).toEqual([sid("ses_reuse")])

    pool.spawned[1].finish(sid("ses_reuse"))
    await second
    await dispatcher.terminateAll()
  })
})

describe("async resume decision deadline", () => {
  test("an unresolved decision settles the crash at the deadline and frees the session", async () => {
    const pool = createFakeWorkerPool(1)
    let decisions = 0
    const dispatcher = createDispatcher({
      pool,
      resumeDecisionTimeoutMs: 30,
      resumeDecision: () => {
        decisions += 1
        // A durable ledger read that never resolves must not pin the session.
        return new Promise(() => {})
      },
    })

    const crashed = dispatcher.dispatch({ sessionID: sid("ses_resume_deadline") })
    await flush()
    pool.spawned[0].crash(sid("ses_resume_deadline"))

    const outcome = await Promise.race([
      crashed.then(
        () => ({ kind: "resolved" as const }),
        (error: unknown) => ({ kind: "rejected" as const, error }),
      ),
      new Promise<{ kind: "timeout" }>((resolve) => setTimeout(() => resolve({ kind: "timeout" }), 2_000)),
    ])
    expect(outcome.kind).toBe("rejected")
    expect(outcome.kind === "rejected" ? outcome.error : undefined).toBeInstanceOf(WorkerCrashedError)
    expect(decisions).toBe(1)
    expect(dispatcher.registry.has(sid("ses_resume_deadline"))).toBe(false)

    // The dead run no longer owns the session: a new dispatch materializes a
    // fresh worker instead of attaching to the dead promise.
    const healthy = dispatcher.dispatch({ sessionID: sid("ses_resume_deadline") })
    await flush()
    expect(pool.spawned).toHaveLength(2)
    pool.spawned[1].finish(sid("ses_resume_deadline"))
    await healthy
    await dispatcher.terminateAll()
  })
})

describe("retirement waits for a confirmed exit", () => {
  test("onSessionReleased fires only after terminate resolves, and its promise is awaited", async () => {
    let resolveTerminate!: () => void
    const terminated = new Promise<void>((resolve) => {
      resolveTerminate = resolve
    })
    let resolveRelease!: () => void
    const release = new Promise<void>((resolve) => {
      resolveRelease = resolve
    })
    const released: string[] = []
    const worker = new FakeSessionWorker(1, { mode: "cooperative" })
    const terminate = worker.terminate.bind(worker)
    worker.terminate = async () => {
      await terminated
      await terminate()
    }
    const pool: WorkerPool = { spawn: () => worker, terminateAll: async () => {} }
    const dispatcher = createDispatcher({
      pool,
      onSessionReleased: async (sessionID) => {
        await release
        released.push(sessionID)
      },
    })

    const run = dispatcher.dispatch({ sessionID: sid("ses_confirmed_exit") })
    void run.catch(() => undefined)
    await flush()
    const cancelling = dispatcher.cancel(sid("ses_confirmed_exit"))
    await flush()
    // The isolate has not exited yet: no release may be observed.
    expect(released).toEqual([])

    resolveTerminate()
    await flush()
    // Termination resolved, but the async release callback is still pending.
    expect(released).toEqual([])

    resolveRelease()
    await cancelling
    await pollUntil(() => released.length === 1, { message: "the release callback never ran" })
    expect(released).toEqual([sid("ses_confirmed_exit")])
    await dispatcher.terminateAll()
  })

  test("the terminate watchdog never signals the exit before an async release settles", async () => {
    const worker = new FakeSessionWorker(1)
    worker.terminate = () => new Promise<void>(() => {})
    let confirmRelease!: () => void
    const releaseGate = new Promise<void>((resolve) => {
      confirmRelease = resolve
    })
    const released: SessionID[] = []
    let spawnCount = 0
    const spawned: FakeSessionWorker[] = []
    const { bridge, lines } = recordingLogger()
    const pool: WorkerPool = {
      spawn: (id) => {
        spawnCount += 1
        const next = spawnCount === 1 ? worker : new FakeSessionWorker(id ?? spawnCount + 1)
        spawned.push(next)
        return next
      },
      terminateAll: async () => {},
    }
    const dispatcher = createDispatcher({
      pool,
      bridge,
      terminateTimeoutMs: 1_000,
      onSessionReleased: async (sessionID) => {
        await releaseGate
        released.push(sessionID)
      },
    })
    const sessionID = sid("ses_watchdog_release_gate")

    const first = capture(dispatcher.dispatch({ sessionID }))
    await pollUntil(() => spawned.length === 1, { message: "the first run never started" })
    spawned[0].finish(sessionID)
    await first
    await flush()

    void dispatcher.retire(sessionID)
    await pollUntil(
      () => lines.some((line) => line.message === workerLogMessage("retirement terminate timed out")),
      { message: "the terminate watchdog never fired" },
    )

    // The watchdog has fired, but the async release hook is still gated. The
    // exit signal is the successor's confirmation and must not be resolved yet.
    const successor = capture(dispatcher.dispatch({ sessionID }))
    await flush()
    expect(spawned).toHaveLength(1)
    expect(released).toEqual([])
    expect(dispatcher.retiringCount).toBe(1)

    confirmRelease()
    await pollUntil(() => spawned.length === 2, {
      message: "the successor never started after the release hook settled",
    })
    spawned[1].finish(sessionID)
    expect(await successor).toEqual({ ok: true, value: undefined })
    await pollUntil(() => released.length === 1, { message: "the release hook never ran" })
    expect(released).toEqual([sessionID])
    await dispatcher.terminateAll()
  })

  test("repeated release barriers join one retirement and observe one release", async () => {
    let confirmExit!: () => void
    const exit = new Promise<void>((resolve) => {
      confirmExit = resolve
    })
    let confirmRelease!: () => void
    const releaseGate = new Promise<void>((resolve) => {
      confirmRelease = resolve
    })
    let releaseCalls = 0
    const worker = new FakeSessionWorker(1)
    worker.terminate = async () => {
      await exit
      worker.alive = false
    }
    const pool: WorkerPool = {
      spawn: () => worker,
      releaseSession: () => {
        releaseCalls += 1
      },
      terminateAll: async () => {},
    }
    const dispatcher = createDispatcher({ pool, onSessionReleased: () => releaseGate })
    const sessionID = sid("ses_joined_release")

    const run = dispatcher.dispatch({ sessionID })
    await flush()
    worker.finish(sessionID)
    await run
    await flush()

    const retiring = dispatcher.retire(sessionID)
    const joining = dispatcher.awaitRelease(sessionID)
    let settled = 0
    void retiring.then(() => {
      settled += 1
    })
    void joining.then(() => {
      settled += 1
    })
    await flush()
    expect(releaseCalls).toBe(0)
    expect(settled).toBe(0)

    confirmExit()
    await pollUntil(() => releaseCalls === 1, { message: "releaseSession never ran after the exit" })
    await flush()
    // The isolate exited, but the async release callback is still pending.
    expect(settled).toBe(0)

    confirmRelease()
    await Promise.all([retiring, joining])
    expect(settled).toBe(2)
    expect(releaseCalls).toBe(1)
    await dispatcher.terminateAll()
  })

  test("a cancel/crash race retires once and releases once", async () => {
    const worker = new FakeSessionWorker(1)
    let releaseCalls = 0
    const pool: WorkerPool = {
      spawn: () => worker,
      releaseSession: () => {
        releaseCalls += 1
      },
      terminateAll: async () => {},
    }
    const dispatcher = createDispatcher({ pool })
    const sessionID = sid("ses_cancel_crash_race")

    const run = capture(dispatcher.dispatch({ sessionID }))
    await flush()

    // The crash rejection lands while the cancel request is still suspending on
    // its boot probe: both terminal paths target the same generation.
    const cancelling = dispatcher.cancel(sessionID)
    worker.crashWith(sessionID, new Error("race crash"))
    await Promise.all([cancelling, run])
    await dispatcher.awaitRelease(sessionID)

    expect(releaseCalls).toBe(1)
    expect(dispatcher.registry.entry(sessionID)).toBeUndefined()
    await dispatcher.terminateAll()
  })

  test("a successor waits for the retiring generation's release callback", async () => {
    let confirmRelease!: () => void
    const releaseGate = new Promise<void>((resolve) => {
      confirmRelease = resolve
    })
    const released: SessionID[] = []
    const spawned: FakeSessionWorker[] = []
    const pool: WorkerPool = {
      spawn: (id) => {
        const worker = new FakeSessionWorker(id ?? spawned.length + 1)
        spawned.push(worker)
        return worker
      },
      terminateAll: async () => {},
    }
    const dispatcher = createDispatcher({
      pool,
      onSessionReleased: async (sessionID) => {
        await releaseGate
        released.push(sessionID)
      },
    })
    const sessionID = sid("ses_successor_release")

    const first = dispatcher.dispatch({ sessionID })
    await flush()
    spawned[0].finish(sessionID)
    await first
    await flush()

    // The isolate exits at once, but the release callback is held open.
    void dispatcher.retire(sessionID)
    await flush()
    const successor = dispatcher.dispatch({ sessionID })
    await flush()
    expect(spawned).toHaveLength(1)

    confirmRelease()
    await pollUntil(() => spawned.length === 2, {
      message: "the successor started before the retirement release completed",
    })
    spawned[1].finish(sessionID)
    await successor
    expect(released).toEqual([sessionID])
    await dispatcher.terminateAll()
  })
})

describe("a detached cancel controller", () => {
  test("is disposed once its in-flight cancel settles", async () => {
    const pool = createFakeWorkerPool(1, { onCancel: "ignore" })
    let subscribed = 0
    let unsubscribed = 0
    const dispatcher = createDispatcher({
      pool,
      cancellation: {
        portFor: (worker) => {
          const port = (worker as FakeSessionWorker).cancelPort
          return {
            notify: (frame) => port.notify(frame),
            onMessage: (handler) => {
              subscribed += 1
              const off = port.onMessage(handler)
              return () => {
                unsubscribed += 1
                off()
              }
            },
          }
        },
        graceMs: 500,
        schedule: createTestScheduler().schedule,
      },
    })
    const sessionID = sid("ses_detached_controller")
    const root = capture(dispatcher.dispatch({ sessionID }))
    await pollUntil(() => pool.spawned.length === 1, { message: "the run never started" })

    const cancelling = dispatcher.cancelWithGrace(sessionID)
    await pollUntil(() => subscribed === 1, { message: "the cancel controller was never created" })

    await dispatcher.retireGeneration(sessionID, pool.spawned[0].id)
    expect(unsubscribed).toBe(0)

    pool.spawned[0].finish(sessionID)
    await cancelling
    await root
    await pollUntil(() => unsubscribed === 1, { message: "the detached controller was never disposed" })
    await dispatcher.terminateAll()
  })
})

describe("retirement exit cannot park the successor", () => {
  test("a terminate that never settles yields to the bounded successor start", async () => {
    const worker = new FakeSessionWorker(1, { mode: "cooperative" })
    let terminations = 0
    worker.terminate = () => {
      terminations += 1
      // The first retire never confirms; the teardown path still resolves.
      return terminations === 1 ? new Promise<void>(() => {}) : Promise.resolve()
    }
    const spawned: FakeSessionWorker[] = []
    const pool: WorkerPool = {
      spawn: () => {
        spawned.push(worker)
        return worker
      },
      terminateAll: async () => {},
    }
    const dispatcher = createDispatcher({ pool, terminateTimeoutMs: 25 })

    const first = dispatcher.dispatch({ sessionID: sid("ses_hung_exit") })
    void first.catch(() => undefined)
    await flush()
    await dispatcher.cancel(sid("ses_hung_exit"))

    const successor = dispatcher.dispatch({ sessionID: sid("ses_hung_exit") })
    await pollUntil(() => worker.runs.length === 2, {
      message: "the successor never started after the retirement deadline",
      timeoutMs: 1_000,
    })
    worker.finish(sid("ses_hung_exit"))
    await successor
    expect(spawned.length).toBe(2)
    await dispatcher.terminateAll()
  })

  test("a deferred successor that times out leaves a live retirement visible", async () => {
    const worker = new FakeSessionWorker(1)
    worker.terminate = () => new Promise<void>(() => {})
    let confirmRelease!: () => void
    const releaseGate = new Promise<void>((resolve) => {
      confirmRelease = resolve
    })
    const released: SessionID[] = []
    let spawnCount = 0
    const spawned: FakeSessionWorker[] = []
    const pool: WorkerPool = {
      spawn: (id) => {
        spawnCount += 1
        const next = spawnCount === 1 ? worker : new FakeSessionWorker(id ?? spawnCount + 1)
        spawned.push(next)
        return next
      },
      terminateAll: async () => {},
    }
    const dispatcher = createDispatcher({
      pool,
      terminateTimeoutMs: 25,
      onSessionReleased: async (sessionID) => {
        await releaseGate
        released.push(sessionID)
      },
    })
    const sessionID = sid("ses_deferred_timeout_live_retirement")

    const first = capture(dispatcher.dispatch({ sessionID }))
    await pollUntil(() => spawned.length === 1, { message: "the first run never started" })
    spawned[0].finish(sessionID)
    await first
    await flush()

    void dispatcher.retire(sessionID)
    const successor = capture(dispatcher.dispatch({ sessionID }))
    await pollUntil(() => spawned.length === 2, {
      message: "the successor never started after the retirement deadline",
    })

    // The successor starts at its bounded deadline, but the release hook is still
    // gated: the retirement must stay visible instead of being deleted as if it
    // had settled.
    expect(dispatcher.retiringCount).toBe(1)
    expect(released).toEqual([])

    confirmRelease()
    await pollUntil(() => dispatcher.retiringCount === 0, {
      message: "the timed-out retirement never left the retiring set",
    })
    expect(released).toEqual([sessionID])
    spawned[1].finish(sessionID)
    expect(await successor).toEqual({ ok: true, value: undefined })
    await dispatcher.terminateAll()
  })

  test("a rejected session-release callback never parks the successor", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({
      pool,
      onSessionReleased: () => {
        throw new Error("lease release failed")
      },
      terminateTimeoutMs: 25,
    })

    const first = dispatcher.dispatch({ sessionID: sid("ses_release_failure") })
    void first.catch(() => undefined)
    await flush()
    await dispatcher.cancel(sid("ses_release_failure"))

    const successor = dispatcher.dispatch({ sessionID: sid("ses_release_failure") })
    await flush()
    pool.spawned[1].finish(sid("ses_release_failure"))
    await successor
    await dispatcher.terminateAll()
  })
})

describe("bounded retirement barriers", () => {
  test("retire and awaitRelease resolve within the terminate deadline when terminate never settles", async () => {
    const worker = new FakeSessionWorker(1)
    worker.terminate = () => new Promise<void>(() => {})
    const pool: WorkerPool = { spawn: () => worker, terminateAll: async () => {} }
    const dispatcher = createDispatcher({ pool, terminateTimeoutMs: 25 })
    const sessionID = sid("ses_bounded_retire")

    const run = dispatcher.dispatch({ sessionID })
    await flush()
    worker.finish(sessionID)
    await run
    await flush()

    const retiring = dispatcher.retire(sessionID)
    const awaiting = dispatcher.awaitRelease(sessionID)
    const outcome = await Promise.race([
      Promise.all([retiring, awaiting]).then(() => "resolved" as const),
      new Promise<"timeout">((resolve) => setTimeout(() => resolve("timeout"), 1_000)),
    ])

    expect(outcome).toBe("resolved")
    expect(dispatcher.registry.has(sessionID)).toBe(false)
    await dispatcher.terminateAll()
  })

  test("terminateAll drains a wedged retirement and completes its exit exactly once", async () => {
    const worker = new FakeSessionWorker(1)
    worker.terminate = () => new Promise<void>(() => {})
    const released: SessionID[] = []
    const pool: WorkerPool = { spawn: () => worker, terminateAll: async () => {} }
    const dispatcher = createDispatcher({
      pool,
      terminateTimeoutMs: 25,
      onSessionReleased: (sessionID) => {
        released.push(sessionID)
      },
    })
    const sessionID = sid("ses_bounded_terminate_all")

    const run = dispatcher.dispatch({ sessionID })
    void run.catch(() => undefined)
    await flush()

    // Start the wedged retirement before shutdown so terminateAll has to join a
    // live retirement barrier instead of only terminating the worker directly.
    const retiring = dispatcher.retire(sessionID)
    const outcome = await Promise.race([
      dispatcher.terminateAll().then(() => "resolved" as const),
      new Promise<"timeout">((resolve) => setTimeout(() => resolve("timeout"), 1_000)),
    ])

    expect(outcome).toBe("resolved")
    await retiring
    // Shutdown settles the retirement barrier instead of leaving a suspended
    // `terminating` entry behind; the release hook still ran exactly once.
    await dispatcher.awaitRelease(sessionID)
    expect(dispatcher.retiringCount).toBe(0)
    expect(released).toEqual([sessionID])
    await expect(run).rejects.toBeInstanceOf(SessionCancelledError)
  })

  test("a wedged retirement fiber is stopped at the watchdog so shutdown does not wait again", async () => {
    const worker = new FakeSessionWorker(1)
    worker.terminate = () => new Promise<void>(() => {})
    const released: SessionID[] = []
    const pool: WorkerPool = { spawn: () => worker, terminateAll: async () => {} }
    const dispatcher = createDispatcher({
      pool,
      terminateTimeoutMs: 500,
      onSessionReleased: (sessionID) => {
        released.push(sessionID)
      },
    })
    const sessionID = sid("ses_wedged_retirement_fiber")

    const run = dispatcher.dispatch({ sessionID })
    await flush()
    worker.finish(sessionID)
    await run
    await flush()

    const retiring = dispatcher.retire(sessionID)
    const outcome = await Promise.race([
      retiring.then(() => "resolved" as const),
      new Promise<"timeout">((resolve) => setTimeout(() => resolve("timeout"), 2_000)),
    ])

    expect(outcome).toBe("resolved")
    expect(dispatcher.retiringCount).toBe(0)

    // The wedged fiber must be off the shutdown drain: if it were still tracked,
    // terminateAll would pay the full terminate deadline again.
    const started = Date.now()
    await dispatcher.terminateAll()
    expect(Date.now() - started).toBeLessThan(500)
    expect(dispatcher.retiringCount).toBe(0)
    await dispatcher.awaitRelease(sessionID)
    expect(released).toEqual([sessionID])
  })

  test("terminateAll resolves a live retirement before draining its deferred successor", async () => {
    const worker = new FakeSessionWorker(1)
    worker.terminate = () => new Promise<void>(() => {})
    let spawnCount = 0
    const spawned: FakeSessionWorker[] = []
    const pool: WorkerPool = {
      spawn: (id) => {
        spawnCount += 1
        const next = spawnCount === 1 ? worker : new FakeSessionWorker(id ?? spawnCount + 1)
        spawned.push(next)
        return next
      },
      terminateAll: async () => {},
    }
    const dispatcher = createDispatcher({ pool, terminateTimeoutMs: 800 })
    const sessionID = sid("ses_shutdown_deferred_release")

    const first = capture(dispatcher.dispatch({ sessionID }))
    await pollUntil(() => spawned.length === 1, { message: "the first run never started" })
    spawned[0].finish(sessionID)
    await first
    await flush()

    void dispatcher.retire(sessionID)
    const successor = capture(dispatcher.dispatch({ sessionID }))
    await flush()
    expect(spawned).toHaveLength(1)

    // The deferred successor is parked on the retirement exit. Shutdown must
    // resolve that exit before draining so it does not charge another full
    // terminate deadline for work it already ended.
    const started = Date.now()
    await dispatcher.terminateAll()
    expect(Date.now() - started).toBeLessThan(500)
    expect(await successor).toMatchObject({ ok: false, error: expect.any(SessionCancelledError) })
  })

  test("terminateAll drains a reap probe that never settles", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({
      pool,
      reapMs: 1_000,
      terminateTimeoutMs: 25,
      // Fire the reap callback during registration so the probe starts without
      // waiting on wall-clock time.
      schedule: (_delayMs, run) => {
        run()
        return () => {}
      },
    })
    const sessionID = sid("ses_reap_probe_never_settles")

    const run = dispatcher.dispatch({ sessionID })
    await pollUntil(() => pool.spawned.length === 1, { message: "the run never started" })
    const worker = pool.spawned[0]
    // A background probe that never answers must not wedge shutdown.
    worker.activeWorkGate = new Promise<void>(() => {})
    worker.finish(sessionID)
    await run
    await flush()

    const outcome = await Promise.race([
      dispatcher.terminateAll().then(() => "resolved" as const),
      new Promise<"timeout">((resolve) => setTimeout(() => resolve("timeout"), 1_000)),
    ])

    expect(outcome).toBe("resolved")
    expect(dispatcher.registry.entries()).toHaveLength(0)
    expect(worker.alive).toBe(false)
  })

  test("a throwing pool releaseSession still runs onSessionReleased", async () => {
    const worker = new FakeSessionWorker(1)
    const released: SessionID[] = []
    const pool: WorkerPool = {
      spawn: () => worker,
      releaseSession: () => {
        throw new Error("pool release failed")
      },
      terminateAll: async () => {},
    }
    const dispatcher = createDispatcher({
      pool,
      onSessionReleased: (releasedSessionID) => {
        released.push(releasedSessionID)
      },
    })
    const sessionID = sid("ses_throwing_pool_release")

    const run = dispatcher.dispatch({ sessionID })
    await flush()
    worker.finish(sessionID)
    await run
    await flush()

    await dispatcher.retire(sessionID)
    await dispatcher.awaitRelease(sessionID)
    expect(released).toEqual([sessionID])
    await dispatcher.terminateAll()
  })
})

describe("a settled crash never hands a dead promise to a new caller", () => {
  test("a caller arriving during the resume decision parks and starts fresh", async () => {
    const pool = createFakeWorkerPool(1)
    let decide!: (decision: { resume: false; sessionID: string; phase: "stream"; reason: "cancelled" }) => void
    const decision = new Promise<{ resume: false; sessionID: string; phase: "stream"; reason: "cancelled" }>(
      (resolve) => {
        decide = resolve
      },
    )
    const dispatcher = createDispatcher({
      pool,
      resumeDecisionTimeoutMs: 1_000,
      resumeDecision: () => decision,
    })

    const sessionID = sid("ses_settled_crash")
    const first = dispatcher.dispatch({ sessionID })
    void first.catch(() => undefined)
    await flush()
    pool.spawned[0].crash(sessionID)
    await flush()

    // The run is already terminal (settled) but its resume decision is pending.
    // A new caller must not receive the dead admitted promise.
    const second = dispatcher.dispatch({ sessionID })
    void second.catch(() => undefined)
    expect(second).not.toBe(first)

    decide({ resume: false, sessionID: String(sessionID), phase: "stream", reason: "cancelled" })
    await expect(first).rejects.toBeInstanceOf(WorkerCrashedError)
    await pollUntil(() => pool.spawned.length === 2, { message: "the parked caller never started fresh" })
    pool.spawned[1].finish(sessionID)
    await second
    await dispatcher.terminateAll()
  })
})

describe("auto-resume defers the crashed caller's settlement", () => {
  test("the original caller resolves with the resumed run instead of the crash", async () => {
    const pool = createFakeWorkerPool(1)
    let resume!: () => void
    const resumed = new Promise<void>((resolve) => {
      resume = resolve
    })
    let attempts = 0
    const dispatcher = createDispatcher({
      pool,
      resumeDecision: () => ({
        resume: true,
        sessionID: "ses_resumed",
        attempt: 1,
        phase: "stream",
        fromMessageID: "msg_committed",
      }),
      onAutoResume: () => {
        attempts += 1
        return resumed
      },
    })

    const sessionID = sid("ses_resumed")
    const first = dispatcher.dispatch({ sessionID })
    await flush()
    pool.spawned[0].crash(sessionID)
    await pollUntil(() => attempts === 1, { message: "the resume was never observed" })

    let settled = false
    void first.then(
      () => {
        settled = true
      },
      () => {
        settled = true
      },
    )
    await flush()
    expect(settled).toBe(false)

    resume()
    await expect(first).resolves.toBeUndefined()
    await dispatcher.terminateAll()
  })
})

describe("dead worker retirement", () => {
  test("a worker that dies mid-run is retired once and its children are cascaded", async () => {
    const pool = createFakeWorkerPool(2)
    const parent = sid("ses_dead_parent")
    const child = sid("ses_dead_child")
    const cancelled: SessionID[] = []
    const dispatcher = createDispatcher({
      pool,
      cancellation: {
        childrenOf: (sessionID) => (sessionID === parent ? [child] : []),
        onCancelled: (event) => cancelled.push(event.sessionID),
      },
    })

    const parentRun = capture(dispatcher.dispatch({ sessionID: parent }))
    const childRun = capture(dispatcher.dispatch({ sessionID: child }))
    await flush()

    const [parentWorker, childWorker] = pool.spawned
    const parentRetires = captureTerminations(parentWorker)
    const childRetires = captureTerminations(childWorker)

    // The isolate dies without a typed crash; the dispatcher must still treat it
    // as a crash, retire it exactly once, and never reuse the dead handle.
    parentWorker.crashWith(parent, new Error("isolate exited"))

    expect(await parentRun).toMatchObject({ ok: false, error: expect.any(WorkerCrashedError) })
    expect(await childRun).toMatchObject({ ok: false, error: expect.any(Error) })
    await flush()

    expect(parentRetires()).toBe(1)
    expect(childRetires()).toBe(1)
    expect(cancelled).toContain(child)
    expect(dispatcher.registry.has(parent)).toBe(false)
    await dispatcher.terminateAll()
  })
})

function captureTerminations(worker: FakeSessionWorker): () => number {
  let count = 0
  const terminate = worker.terminate.bind(worker)
  worker.terminate = async () => {
    count += 1
    await terminate()
  }
  return () => count
}

describe("atomic ownership transitions", () => {
  test("a dispatch during completion settlement parks instead of joining or running concurrently", async () => {
    const pool = createFakeWorkerPool(1)
    const barrier = Deferred.makeUnsafe<void>()
    const sessionID = sid("ses_settling")
    const dispatcher = createDispatcher({
      pool,
      writeBarrier: () => Effect.runPromise(Deferred.await(barrier)),
    })

    const first = capture(dispatcher.dispatch({ sessionID }))
    await pollUntil(() => pool.spawned.length === 1, { message: "the first run never started" })
    pool.spawned[0].finish(sessionID)
    await flush()

    // The run is terminal but its write barrier still owns the slot: a caller
    // must park, never join the settled promise or start a second isolate.
    expect(dispatcher.busySlotCount).toBe(1)
    const second = capture(dispatcher.dispatch({ sessionID }))
    await flush()
    expect(dispatcher.queuedCount).toBe(1)
    expect(pool.spawned[0].runs).toHaveLength(1)

    Deferred.doneUnsafe(barrier, Effect.void)
    expect(await first).toEqual({ ok: true, value: undefined })
    await pollUntil(() => pool.spawned[0].runs.length === 2, {
      message: "the parked caller was never promoted after the barrier",
    })
    expect(pool.spawned[0].runs.map((run) => run.depth)).toEqual([0, 0])
    expect(dispatcher.queuedCount).toBe(0)

    pool.spawned[0].finish(sessionID)
    expect(await second).toEqual({ ok: true, value: undefined })
    await dispatcher.terminateAll()
  })

  test("a successor admitted during crash settlement is not overwritten by a parked waiter", async () => {
    const pool = createFakeWorkerPool(1)
    const sessionID = sid("ses_crash_race")
    let successor: Promise<void> | undefined
    const dispatcher = createDispatcher({
      pool,
      resumeDecision: (payload) => ({
        resume: true,
        sessionID: payload.sessionID,
        attempt: 1,
        phase: payload.phase,
        fromMessageID: "msg_1",
      }),
      onAutoResume: () => {
        successor = dispatcher.dispatch({ sessionID })
        return undefined
      },
    })

    const crashed = capture(dispatcher.dispatch({ sessionID }))
    const waiter = capture(dispatcher.dispatch({ sessionID, depth: 1 }))
    await pollUntil(() => dispatcher.queuedCount === 1, { message: "the waiter was never parked" })

    pool.spawned[0].crash(sessionID)
    expect((await crashed).ok).toBe(false)
    await pollUntil(() => dispatcher.workerForSession(sessionID) === 2, {
      message: "the auto-resume successor never took the session",
    })

    // The successor owns the slot; the parked waiter must stay parked.
    expect(dispatcher.busySlotCount).toBe(1)
    expect(dispatcher.queuedCount).toBe(1)
    expect(pool.spawned[1]?.runs.map((run) => run.depth)).toEqual([0])

    pool.spawned[1].finish(sessionID)
    await successor!
    await pollUntil(() => pool.spawned[1].runs.length === 2, {
      message: "the parked waiter was never promoted after the successor settled",
    })
    expect(pool.spawned[1].runs.map((run) => run.depth)).toEqual([0, 1])
    expect(dispatcher.queuedCount).toBe(0)

    pool.spawned[1].finish(sessionID)
    expect(await waiter).toEqual({ ok: true, value: undefined })
    await dispatcher.terminateAll()
  })

  test("terminateAll latches shutdown and rejects the admitted run and every parked waiter", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })
    const sessionID = sid("ses_shutdown")

    const root = capture(dispatcher.dispatch({ sessionID }))
    const waiter = capture(dispatcher.dispatch({ sessionID, depth: 1 }))
    await pollUntil(() => dispatcher.queuedCount === 1, { message: "the waiter was never parked" })

    await dispatcher.terminateAll()

    expect(await root).toMatchObject({ ok: false, error: expect.any(SessionCancelledError) })
    expect(await waiter).toMatchObject({ ok: false, error: expect.any(SessionCancelledError) })
    expect(dispatcher.busySlotCount).toBe(0)
    expect(dispatcher.queuedCount).toBe(0)
    expect(dispatcher.registry.size).toBe(0)

    const spawnedBefore = pool.spawned.length
    await expect(dispatcher.dispatch({ sessionID })).rejects.toBeInstanceOf(SessionCancelledError)
    expect(pool.spawned).toHaveLength(spawnedBefore)
    await dispatcher.terminateAll()
  })

  test("cancel owns the run: a later completion cannot resolve the caller", async () => {
    let finishRun: (() => void) | undefined
    let terminations = 0
    const pool: WorkerPool = {
      spawn: () => ({
        id: 1,
        alive: true,
        execute: () =>
          new Promise<void>((resolve) => {
            finishRun = resolve
          }),
        terminate: async () => {
          terminations += 1
        },
      }),
      terminateAll: async () => {},
    }
    const dispatcher = createDispatcher({ pool })
    const sessionID = sid("ses_cancel_owns")
    let settlements = 0
    const run = dispatcher.dispatch({ sessionID })
    void run.then(
      () => {
        settlements += 1
      },
      () => {
        settlements += 1
      },
    )
    await pollUntil(() => finishRun !== undefined, { message: "the run never started" })

    await dispatcher.cancel(sessionID)
    expect(await capture(run)).toMatchObject({ ok: false, error: expect.any(SessionCancelledError) })
    expect(dispatcher.busySlotCount).toBe(0)

    // The cancelled run's execution resolves late; ownership is already gone, so
    // the caller and its gate cannot be settled a second time.
    finishRun!()
    await flush()
    expect(settlements).toBe(1)
    expect(terminations).toBe(1)
    await dispatcher.terminateAll()
  })

  test("an admission guard may re-enter dispatch for another session", async () => {
    const pool = createFakeWorkerPool(1)
    const outer = sid("ses_guard_outer")
    const inner = sid("ses_guard_inner")
    let innerDispatch: Promise<void> | undefined
    let innerThrown: unknown
    let reentered = false
    const dispatcher = createDispatcher({
      pool,
      assertAdmissible: (sessionID) => {
        if (reentered || sessionID !== outer) return
        reentered = true
        // User code must not run while the transition permit is held: a nested
        // dispatch would otherwise find the non-reentrant lock taken.
        try {
          innerDispatch = dispatcher.dispatch({ sessionID: inner })
        } catch (error) {
          innerThrown = error
        }
      },
    })

    const outerRun = capture(dispatcher.dispatch({ sessionID: outer }))
    await pollUntil(() => reentered, { message: "the admission guard never ran" })

    expect(innerThrown).toBeUndefined()
    expect(innerDispatch).toBeDefined()

    await pollUntil(() => dispatcher.busySlotCount === 2, {
      message: "the outer and inner sessions were never both admitted",
    })
    expect(dispatcher.queuedCount).toBe(0)

    const outerWorker = pool.spawned.find((worker) => worker.runs[0]?.sessionID === outer)
    const innerWorker = pool.spawned.find((worker) => worker.runs[0]?.sessionID === inner)
    expect(outerWorker).toBeDefined()
    expect(innerWorker).toBeDefined()

    outerWorker!.finish(outer)
    innerWorker!.finish(inner)
    expect(await outerRun).toEqual({ ok: true, value: undefined })
    expect(await capture(innerDispatch!)).toEqual({ ok: true, value: undefined })
    expect(dispatcher.busySlotCount).toBe(0)
    expect(dispatcher.queuedCount).toBe(0)
    await dispatcher.terminateAll()
  })
})
