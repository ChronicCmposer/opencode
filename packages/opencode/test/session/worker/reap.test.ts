// v2 idle reap: a session worker is terminated and dropped from the registry
// after OPENCODE_SESSION_WORKER_REAP_MS with no in-flight run and no queued
// prompt. A new dispatch lazily spawns a fresh worker.

import { describe, expect, test } from "bun:test"
import { createDispatcher } from "@/session/worker/dispatch"
import { DEFAULT_REAP_MS, REAP_MS_ENV, resolveReapMs } from "@/session/worker/pool"
import { createFakeWorkerPool } from "../../lib/fake-worker-pool"
import { createRecordingScheduler } from "../../lib/scheduler"
import { flush, sid } from "../../lib/async"

describe("session worker reap", () => {
  test("resolves the reap env inline with a 5 minute default", () => {
    expect(DEFAULT_REAP_MS).toBe(300_000)
    expect(resolveReapMs({})).toBe(300_000)
    expect(resolveReapMs({ [REAP_MS_ENV]: "1000" })).toBe(1000)
    expect(resolveReapMs({ [REAP_MS_ENV]: "0" })).toBe(300_000)
    expect(resolveReapMs({ [REAP_MS_ENV]: "nope" })).toBe(300_000)
  })

  test("an idle worker is terminated and removed after the window; re-dispatch spawns fresh", async () => {
    const pool = createFakeWorkerPool(1)
    const scheduler = createRecordingScheduler()
    const dispatcher = createDispatcher({ pool, schedule: scheduler.schedule, reapMs: 1_000 })

    const run = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()
    const worker = pool.spawned[0]
    worker.finish(sid("ses_1"))
    await run
    await flush()

    const reap = scheduler.pending().find((timer) => timer.delayMs === 1_000)
    expect(reap).toBeDefined()
    reap!.run()
    await flush()

    expect(worker.alive).toBe(false)
    expect(dispatcher.registry.has(sid("ses_1"))).toBe(false)

    const again = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()
    expect(pool.spawned).toHaveLength(2)
    expect(pool.spawned[1]).not.toBe(worker)
    pool.spawned[1].finish(sid("ses_1"))
    await again
    await dispatcher.terminateAll()
  })

  test("a worker with active background work is re-armed, then retired once the job completes", async () => {
    const pool = createFakeWorkerPool(1)
    const scheduler = createRecordingScheduler()
    const dispatcher = createDispatcher({ pool, schedule: scheduler.schedule, reapMs: 1_000 })

    const run = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()
    const worker = pool.spawned[0]
    // A background job starts during the turn and outlives it.
    worker.backgroundWork = true
    worker.finish(sid("ses_1"))
    await run
    await flush()

    scheduler.fire()
    await flush()

    // The probe kept the worker alive and armed another full window.
    expect(worker.alive).toBe(true)
    expect(scheduler.pending().filter((timer) => timer.delayMs === 1_000)).toHaveLength(1)

    // The job completes: the next window retires the now-truly-idle worker.
    worker.backgroundWork = false
    scheduler.fire()
    await flush()

    expect(worker.alive).toBe(false)
    expect(scheduler.pending()).toHaveLength(0)
    await dispatcher.terminateAll()
  })

  test("a worker whose probe reports no active work is retired on the window", async () => {
    const pool = createFakeWorkerPool(1, { activeWork: false })
    const scheduler = createRecordingScheduler()
    const dispatcher = createDispatcher({ pool, schedule: scheduler.schedule, reapMs: 1_000 })

    const run = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()
    const worker = pool.spawned[0]
    worker.finish(sid("ses_1"))
    await run
    await flush()

    scheduler.fire()
    await flush()

    expect(worker.alive).toBe(false)
    expect(scheduler.pending()).toHaveLength(0)
    await dispatcher.terminateAll()
  })

  test("a dispatch that arrives while the probe is in flight wins; the worker is not retired", async () => {
    const pool = createFakeWorkerPool(1)
    const scheduler = createRecordingScheduler()
    const dispatcher = createDispatcher({ pool, schedule: scheduler.schedule, reapMs: 1_000 })

    const first = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()
    const worker = pool.spawned[0]
    worker.finish(sid("ses_1"))
    await first
    await flush()

    // Hold the probe open, then admit a new run before it answers.
    let release!: () => void
    worker.activeWorkGate = new Promise<void>((resolve) => {
      release = resolve
    })
    worker.backgroundWork = true
    scheduler.fire()
    await flush()

    const second = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()
    release()
    await flush()

    // The stale timer must not retire an actively running worker, and it must
    // not arm a reap window while a run owns the session.
    expect(worker.alive).toBe(true)
    expect(scheduler.pending().filter((timer) => timer.delayMs === 1_000)).toHaveLength(0)

    worker.finish(sid("ses_1"))
    await second
    await flush()
    expect(scheduler.pending().filter((timer) => timer.delayMs === 1_000)).toHaveLength(1)
    await dispatcher.terminateAll()
  })


  test("a new dispatch before expiry resets the timer", async () => {
    const pool = createFakeWorkerPool(1)
    const scheduler = createRecordingScheduler()
    const dispatcher = createDispatcher({ pool, schedule: scheduler.schedule, reapMs: 1_000 })

    const first = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()
    pool.spawned[0].finish(sid("ses_1"))
    await first
    await flush()
    expect(scheduler.pending().filter((timer) => timer.delayMs === 1_000)).toHaveLength(1)

    const second = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()
    // The armed timer was cancelled when the next turn arrived.
    expect(scheduler.pending()).toHaveLength(0)

    pool.spawned[0].finish(sid("ses_1"))
    await second
    await flush()
    expect(scheduler.pending().filter((timer) => timer.delayMs === 1_000)).toHaveLength(1)
    await dispatcher.terminateAll()
  })

  test("a queued prompt keeps the worker alive (no reap timer while the session is active)", async () => {
    const pool = createFakeWorkerPool(1)
    const scheduler = createRecordingScheduler()
    const dispatcher = createDispatcher({ pool, schedule: scheduler.schedule, reapMs: 1_000 })

    const root = dispatcher.dispatch({ sessionID: sid("ses_1") })
    const waiter = dispatcher.dispatch({ sessionID: sid("ses_1"), depth: 1 })
    await flush()

    expect(scheduler.pending().filter((timer) => timer.delayMs === 1_000)).toHaveLength(0)
    pool.spawned[0].finish(sid("ses_1"))
    await root
    await flush()

    pool.spawned[0].finish(sid("ses_1"))
    await waiter
    await flush()
    expect(scheduler.pending().filter((timer) => timer.delayMs === 1_000)).toHaveLength(1)
    await dispatcher.terminateAll()
  })

  test("cancel is terminal: the worker is retired immediately and no reap timer survives", async () => {
    const pool = createFakeWorkerPool(1)
    const scheduler = createRecordingScheduler()
    const dispatcher = createDispatcher({ pool, schedule: scheduler.schedule, reapMs: 1_000 })

    const run = dispatcher.dispatch({ sessionID: sid("ses_cancel") })
    await flush()
    const worker = pool.spawned[0]
    await dispatcher.cancel(sid("ses_cancel"))
    await expect(run).rejects.toBeDefined()

    expect(worker.alive).toBe(false)
    expect(scheduler.pending()).toHaveLength(0)
    await dispatcher.terminateAll()
  })

  test("the hub cleanup seam fires when a worker is reaped or cancelled", async () => {
    const pool = createFakeWorkerPool(1)
    const scheduler = createRecordingScheduler()
    const released: string[] = []
    const dispatcher = createDispatcher({
      pool,
      schedule: scheduler.schedule,
      reapMs: 1_000,
      onSessionReleased: (sessionID) => released.push(sessionID),
    })

    const first = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()
    pool.spawned[0].finish(sid("ses_1"))
    await first
    await flush()
    scheduler.pending().find((timer) => timer.delayMs === 1_000)!.run()
    await flush()
    expect(released).toEqual(["ses_1"])

    const second = dispatcher.dispatch({ sessionID: sid("ses_2") })
    await flush()
    await dispatcher.cancel(sid("ses_2"))
    await expect(second).rejects.toBeDefined()
    expect(released).toEqual(["ses_1", "ses_2"])
    await dispatcher.terminateAll()
  })

  test("dispose cancels every armed reap timer", async () => {
    const pool = createFakeWorkerPool(1)
    const scheduler = createRecordingScheduler()
    const dispatcher = createDispatcher({ pool, schedule: scheduler.schedule, reapMs: 1_000 })

    const run = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()
    pool.spawned[0].finish(sid("ses_1"))
    await run
    await flush()
    expect(scheduler.pending()).toHaveLength(1)

    await dispatcher.terminateAll()
    expect(scheduler.pending()).toHaveLength(0)
  })
})
