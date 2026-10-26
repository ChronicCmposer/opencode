// v2 per-session dispatch core.
//
// One unbounded, lazily-materialized worker per session; reuse across turns; one
// active run per session; an unbounded per-session waiter list for a different
// run identity; at-most-once crash rejection followed by a fresh lazy worker;
// and the key→worker reservation lifecycle.

import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import type { SessionID } from "@/session/schema"
import type { ShellInput } from "@/session/prompt"
import {
  createDispatcher,
  SessionCancelledError,
  WorkerCrashedError,
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
import { capture, flush, sid } from "../../lib/async"
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
