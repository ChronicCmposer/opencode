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
import {
  createDispatcher,
  SessionCancelledError,
  WorkerCrashedError,
  CANCELLED_MODES,
  type Dispatcher,
  type DispatchRequest,
  type WorkerPool,
} from "@/session/worker/dispatch"
import {
  createExecutorWorker,
  createSessionDispatch,
  SessionWorkerReplyTimeoutError,
  type ExecutorTransport,
  type ExecutorWorkerPool,
  type SessionExecutorInbound,
  type SessionScheduler,
} from "@/session/worker/execute"
import { workerLogMessage, type WorkerLogger } from "@/session/worker/log"
import { createWorkerHarness } from "../../lib/worker-harness"
import { withFaults } from "../../lib/faults"
import { capture, flush, pollUntil, sid } from "../../lib/async"
import { FaultPool, FakeSessionWorker, createFakeWorkerPool } from "../../lib/fake-worker-pool"
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
    expect(await dispatcher.cancelWithGrace(sessionID)).toMatchObject({
      mode: "completed",
      winner: "completion",
    })
    expect(settlements).toBe(1)
    expect(events).toEqual(["completed"])
    expect(barrierCalls).toEqual([sessionID])

    Deferred.doneUnsafe(barrier, Effect.void)
    await run
    await flush()
    expect(settlements).toBe(1)
    await dispatcher.terminateAll()
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
}

function recordingLogger(): { log: WorkerLogger; lines: RecordedLog[] } {
  const lines: RecordedLog[] = []
  return {
    log: (message, fields) => {
      lines.push({ message, fields })
    },
    lines,
  }
}

describe("dispatch observability logging", () => {
  test("session admitted (INFO) carries worker and depth", async () => {
    const pool = createFakeWorkerPool(1)
    const { log, lines } = recordingLogger()
    const dispatcher = createDispatcher({ pool, log })

    const run = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()

    const admitted = lines.find((line) => line.message === workerLogMessage("session admitted"))
    expect(admitted).toBeDefined()
    expect(admitted!.fields).toMatchObject({
      level: "INFO",
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
    const { log, lines } = recordingLogger()
    const dispatcher = createDispatcher({ pool, log })

    const run = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()
    pool.spawned[0].finish(sid("ses_1"))
    await run

    const done = lines.find((line) => line.message === workerLogMessage("session completed"))
    expect(done).toBeDefined()
    expect(done!.fields).toMatchObject({ level: "INFO", sessionID: "ses_1", workerID: pool.spawned[0].id })
    expect(typeof done!.fields.durationMs).toBe("number")
    await dispatcher.terminateAll()
  })

  test("waiter promoted (INFO) carries the remaining waiter depth", async () => {
    const pool = createFakeWorkerPool(1)
    const { log, lines } = recordingLogger()
    const dispatcher = createDispatcher({ pool, log })

    const root = capture(dispatcher.dispatch({ sessionID: sid("ses_1") }))
    const waiter = capture(dispatcher.dispatch({ sessionID: sid("ses_1"), depth: 1 }))
    await flush()

    pool.spawned[0].finish(sid("ses_1"))
    await root
    await flush()

    const promoted = lines.find((line) => line.message === workerLogMessage("waiter promoted"))
    expect(promoted).toBeDefined()
    expect(promoted!.fields).toMatchObject({ level: "INFO", sessionID: "ses_1" })
    expect(typeof promoted!.fields.waiterQueueDepth).toBe("number")

    await dispatcher.terminateAll()
    await Promise.allSettled([waiter])
  })

  test("worker crashed (WARN) carries the crash", async () => {
    const pool = createFakeWorkerPool(1)
    const { log, lines } = recordingLogger()
    const dispatcher = createDispatcher({ pool, log })

    const crashed = capture(dispatcher.dispatch({ sessionID: sid("ses_1") }))
    await flush()
    pool.spawned[0].crash(sid("ses_1"))
    await crashed

    const line = lines.find((entry) => entry.message === workerLogMessage("worker crashed"))
    expect(line).toBeDefined()
    expect(line!.fields).toMatchObject({ level: "WARN", sessionID: "ses_1" })
    expect(typeof line!.fields.workerID).toBe("number")
    await dispatcher.terminateAll()
  })

  test("session worker reaped (INFO) carries the idle window", async () => {
    const pool = createFakeWorkerPool(1)
    const scheduler = createTestScheduler()
    const { log, lines } = recordingLogger()
    const dispatcher = createDispatcher({ pool, log, schedule: scheduler.schedule, reapMs: 60_000 })

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
    expect(line!.fields).toMatchObject({ level: "INFO", idleMs: 60_000, sessionID: "ses_parent" })
    expect(typeof line!.fields.workerID).toBe("number")
    await dispatcher.terminateAll()
  })

  test("a plain session failure logs an ERROR 'worker run failed' line", async () => {
    const pool = createFakeWorkerPool(1)
    const { log, lines } = recordingLogger()
    const dispatcher = createDispatcher({ pool, log })

    const failing = capture(dispatcher.dispatch({ sessionID: sid("ses_1") }))
    await flush()
    pool.spawned[0].fail(sid("ses_1"), new Error("No user message found in stream"))
    await failing

    const line = lines.find((entry) => entry.message === workerLogMessage("worker run failed"))
    expect(line).toBeDefined()
    expect(line!.fields).toMatchObject({
      level: "ERROR",
      sessionID: "ses_1",
      workerID: pool.spawned[0].id,
    })
    expect(String(line!.fields.error)).toContain("No user message found in stream")
    await dispatcher.terminateAll()
  })

  test("cancelWithGrace emits 'cancel requested' with sessionID, workerID and graceMs", async () => {
    const pool = createFakeWorkerPool(1, { onCancel: "ignore" })
    const scheduler = createTestScheduler()
    const { log, lines } = recordingLogger()
    const dispatcher = createDispatcher({
      pool,
      log,
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
    const { log, lines } = recordingLogger()
    const dispatcher = createDispatcher({ pool, log })

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
