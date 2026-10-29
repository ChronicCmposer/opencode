// v2 execute seam — `createSessionDispatch` drives the per-session registry:
// the first `ensureRunning` materializes a session's worker, later turns reuse
// it, distinct sessions get distinct workers, and `resultFor`/`assertNotBusy`/
// `cancel` keep working.

import { describe, expect, test } from "bun:test"
import {
  createExecutorWorker,
  createExecutorWorkerPool,
  createSessionDispatch,
  type ExecutorTransport,
} from "@/session/worker/execute"
import { WorkerCrashedError } from "@/session/worker/dispatch"
import { createDispatcher } from "../../lib/dispatcher"
import {
  CRASH_ATTEMPTS_BEFORE_DEAD,
  createSupervision,
  SessionWorkerDeadError,
} from "@/session/worker/supervision"
import { createFakeExecutorPool, FakeExecutorWorker } from "../../lib/fake-worker-pool"
import { createRecordingScheduler } from "../../lib/scheduler"
import { flush, sid } from "../../lib/async"
import { pollUntil } from "../../lib/poll"

/** A transport that answers every run/startShell with a done frame. */
function doneTransport(value: unknown = "ok"): ExecutorTransport {
  const posted: Array<{ readonly id: number; readonly sessionID?: string }> = []
  return {
    post: (message) => posted.push(message as { readonly id: number; readonly sessionID?: string }),
    waitFor: async (match) => {
      const last = posted[posted.length - 1]
      const reply = {
        type: "done" as const,
        id: last?.id ?? 0,
        result: { sessionID: last?.sessionID ?? "ses", value },
      }
      if (match(reply)) return reply
      throw new Error("no matching reply")
    },
    alive: true,
    terminate: async () => 0,
  }
}

describe("createSessionDispatch uses the per-session worker registry", () => {
  test("the first ensureRunning creates one worker and the second turn reuses it", async () => {
    let spawns = 0
    const pool = createExecutorWorkerPool({
      spawn: (id) => {
        spawns += 1
        const results = new Map<string, { sessionID: string; value?: unknown }>()
        return {
          id,
          alive: true,
          execute: async (run) => {
            results.set(run.sessionID, { sessionID: run.sessionID, value: `worker-${id}` })
          },
          cancel: async () => {},
          assertNotBusy: async () => false,
          resultFor: (sessionID: string) => results.get(sessionID),
          terminate: async () => {},
        }
      },
    })
    const dispatcher = createDispatcher({ pool })
    const dispatch = createSessionDispatch({ dispatcher, pool })

    await dispatch.ensureRunning(sid("ses_reuse"))
    await dispatch.ensureRunning(sid("ses_reuse"))

    expect(spawns).toBe(1)
    expect(pool.workers).toHaveLength(1)
    await dispatcher.terminateAll()
  })

  test("distinct sessions run on distinct workers and keep their own results", async () => {
    const pool = createExecutorWorkerPool({
      spawn: (id) => {
        const results = new Map<string, { sessionID: string; value?: unknown }>()
        return {
          id,
          alive: true,
          execute: async (run) => {
            results.set(run.sessionID, { sessionID: run.sessionID, value: `worker-${id}` })
          },
          cancel: async () => {},
          assertNotBusy: async () => false,
          resultFor: (sessionID: string) => results.get(sessionID),
          terminate: async () => {},
        }
      },
    })
    const dispatcher = createDispatcher({ pool })
    const dispatch = createSessionDispatch({ dispatcher, pool })

    const [a, b] = await Promise.all([
      dispatch.ensureRunning(sid("ses_a")),
      dispatch.ensureRunning(sid("ses_b")),
    ])

    expect(a).toEqual({ sessionID: "ses_a", value: "worker-1" })
    expect(b).toEqual({ sessionID: "ses_b", value: "worker-2" })
    expect(pool.workers).toHaveLength(2)
    await dispatcher.terminateAll()
  })

  test("assertNotBusy and cancel reach the owning worker", async () => {
    // The registry owns the monotonic worker id, so the pool must honour the
    // dispatcher-supplied id (the shared fake's eager-worker counter does not).
    const pool = createExecutorWorkerPool({
      spawn: (id) => new FakeExecutorWorker(id, { mode: "blocking" }),
    })
    const dispatcher = createDispatcher({ pool })
    const dispatch = createSessionDispatch({ dispatcher, pool })

    const running = dispatch.ensureRunning(sid("ses_busy"))
    // The registry records the worker only after the dispatch seam has admitted
    // the session; wait on the observable assertion rather than a fixed flush.
    await pollUntil(async () => await dispatch.assertNotBusy(sid("ses_busy")), {
      message: "session was never registered as busy",
    })
    await dispatch.cancel(sid("ses_busy"))
    await expect(running).rejects.toBeDefined()
    await pollUntil(async () => !(await dispatch.assertNotBusy(sid("ses_busy"))), {
      message: "session stayed busy after cancel",
    })
    await dispatcher.terminateAll()
  })

  test("coalesced waiters record one crash for one crashed worker", async () => {
    const sessionID = sid("ses_coalesced_crash")
    // One crash event: every coalesced waiter observes the exact same rejection.
    const crash = new WorkerCrashedError({ message: "worker 1 crashed", workerID: 1 })
    const pool = createExecutorWorkerPool({
      spawn: (id) => ({
        id,
        alive: false,
        execute: () => Promise.reject(crash),
        cancel: async () => {},
        assertNotBusy: async () => false,
        resultFor: () => undefined,
        terminate: async () => {},
      }),
    })
    const dispatcher = createDispatcher({ pool })
    const base = createSupervision()
    let recorded = 0
    const supervision = {
      ...base,
      recordCrash: (id: string) => {
        recorded += 1
        return base.recordCrash(id)
      },
    }
    const dispatch = createSessionDispatch({ dispatcher, pool, supervision })

    const [first, second] = await Promise.allSettled([
      dispatch.ensureRunning(sessionID),
      dispatch.ensureRunning(sessionID),
    ])

    expect(first.status).toBe("rejected")
    expect(second.status).toBe("rejected")
    expect(recorded).toBe(1)
    expect(base.isDead(sessionID)).toBe(false)
    await dispatcher.terminateAll()
  })

  test("startShell still carries the shell frame", async () => {
    const pool = createFakeExecutorPool(1, { value: { sessionID: "ses_shell" } })
    const dispatcher = createDispatcher({ pool })
    const dispatch = createSessionDispatch({ dispatcher, pool })

    const result = await dispatch.startShell(sid("ses_shell"), {
      sessionID: sid("ses_shell"),
      agent: "build",
      command: "echo hi",
    })

    expect(result.sessionID).toBe("ses_shell")
    await dispatcher.terminateAll()
  })
})

// ---------------------------------------------------------------------------
// Integration: two real sessions run concurrently on distinct isolates.
// ---------------------------------------------------------------------------

describe("per-session workers integration", () => {
  const EXECUTOR_FIXTURE = new URL("../../fixture/session-execute-worker.ts", import.meta.url)

  test("two sessions run concurrently on distinct workers without cross-talk", async () => {
    const { createSessionHub, createBunExecutorTransport } = await import("@/session/worker/hub")
    const pool = createExecutorWorkerPool({
      spawn: (id) =>
        createExecutorWorker({
          id,
          transport: createBunExecutorTransport({ entry: EXECUTOR_FIXTURE, workerID: id }),
          workKind: "slow",
          timeoutMs: 15_000,
        }),
    })
    const hub = createSessionHub({ ...{ barrier: createTestBarrier() }, pool })
    try {
      const [a, b] = await Promise.all([
        hub.ensureRunning(sid("ses_real_a")),
        hub.ensureRunning(sid("ses_real_b")),
      ])

      expect(a.sessionID).toBe("ses_real_a")
      expect(b.sessionID).toBe("ses_real_b")
      expect(pool.workers).toHaveLength(2)
      expect((a.value as { execution: number }).execution).toBe(1)
      expect((b.value as { execution: number }).execution).toBe(1)
    } finally {
      await hub.terminateAll()
    }
  }, 30_000)
})

// ---------------------------------------------------------------------------
// Retention: a retired worker leaves the pool, so the pool stays bounded by
// live sessions rather than every session ever dispatched.
// ---------------------------------------------------------------------------

describe("retired workers leave the executor pool", () => {
  type LifecycleOutcome = "done" | "crash" | "block"

  function lifecyclePool(outcome: LifecycleOutcome) {
    const spawned: Array<{ readonly id: number }> = []
    const pool = createExecutorWorkerPool({
      spawn: (id) => {
        let alive = true
        const results = new Map<string, { sessionID: string; value?: unknown }>()
        const worker = {
          id,
          get alive() {
            return alive
          },
          execute: async (run: { readonly sessionID: string }) => {
            if (outcome === "crash") {
              alive = false
              throw new WorkerCrashedError({ message: `worker ${id} crashed`, workerID: id })
            }
            if (outcome === "block") return new Promise<void>(() => {})
            results.set(run.sessionID, { sessionID: run.sessionID, value: id })
          },
          cancel: async () => {},
          assertNotBusy: async () => false,
          resultFor: (sessionID: string) => results.get(sessionID),
          terminate: async () => {
            alive = false
          },
        }
        spawned.push(worker)
        return worker
      },
    })
    return { pool, spawned }
  }

  test("a reaped worker is dropped from byID/resultFor and a re-dispatch spawns fresh", async () => {
    const scheduler = createRecordingScheduler()
    const { pool, spawned } = lifecyclePool("done")
    const dispatcher = createDispatcher({ pool, schedule: scheduler.schedule, reapMs: 1_000 })
    const sessionID = sid("ses_retain_reap")

    await dispatcher.dispatch({ sessionID })
    await flush()

    expect(pool.byID(1)).toBeDefined()
    expect(pool.resultFor(sessionID)).toBeDefined()

    scheduler.pending().find((timer) => timer.delayMs === 1_000)!.run()
    await flush()

    expect(pool.byID(1)).toBeUndefined()
    expect(pool.resultFor(sessionID)).toBeUndefined()
    expect(pool.workers).toHaveLength(0)

    await dispatcher.dispatch({ sessionID })
    await flush()
    expect(spawned.map((worker) => worker.id)).toEqual([1, 2])
    expect(pool.byID(2)).toBeDefined()
    await dispatcher.terminateAll()
  })

  test("a cancelled worker is dropped from the pool", async () => {
    const { pool } = lifecyclePool("block")
    const dispatcher = createDispatcher({ pool })
    const sessionID = sid("ses_retain_cancel")

    const running = dispatcher.dispatch({ sessionID })
    await flush()
    expect(pool.byID(1)).toBeDefined()

    await dispatcher.cancel(sessionID)
    await expect(running).rejects.toBeDefined()

    expect(pool.byID(1)).toBeUndefined()
    expect(pool.workers).toHaveLength(0)
    await dispatcher.terminateAll()
  })

  test("a crashed worker is dropped from the pool", async () => {
    const { pool } = lifecyclePool("crash")
    const dispatcher = createDispatcher({ pool })
    const sessionID = sid("ses_retain_crash")

    await expect(dispatcher.dispatch({ sessionID })).rejects.toBeInstanceOf(WorkerCrashedError)
    await flush()

    expect(pool.byID(1)).toBeUndefined()
    expect(pool.workers).toHaveLength(0)
    await dispatcher.terminateAll()
  })
})

// ---------------------------------------------------------------------------
// Dispatch origin: background/auto work fails fast on a dead session; an
// explicit user dispatch resets the policy and respawns a fresh worker.
// ---------------------------------------------------------------------------

describe("createSessionDispatch honors the dispatch origin", () => {
  test("auto dispatch fails fast on a dead session and spawns nothing", async () => {
    const sessionID = sid("ses_dead")
    const supervision = createSupervision()
    for (let attempt = 0; attempt < CRASH_ATTEMPTS_BEFORE_DEAD; attempt++) supervision.recordCrash(sessionID)
    const pool = createFakeExecutorPool(1)
    const dispatcher = createDispatcher({ pool })
    const dispatch = createSessionDispatch({ dispatcher, pool, supervision })

    await expect(
      dispatch.ensureRunning(sessionID, undefined, "auto"),
    ).rejects.toBeInstanceOf(SessionWorkerDeadError)
    expect(pool.spawned).toHaveLength(0)
    await dispatcher.terminateAll()
  })

  test("a user dispatch resets supervision and respawns the worker", async () => {
    const sessionID = sid("ses_dead_user")
    const supervision = createSupervision()
    for (let attempt = 0; attempt < CRASH_ATTEMPTS_BEFORE_DEAD; attempt++) supervision.recordCrash(sessionID)
    const pool = createFakeExecutorPool(1)
    const dispatcher = createDispatcher({ pool })
    const dispatch = createSessionDispatch({ dispatcher, pool, supervision })

    const result = await dispatch.ensureRunning(sessionID, undefined, "user")
    expect(result.sessionID).toBe(sessionID)
    expect(pool.spawned).toHaveLength(1)
    expect(supervision.isDead(sessionID)).toBe(false)
    await dispatcher.terminateAll()
  })
})

import { createTestBarrier } from "../../lib/run-barrier-test"
