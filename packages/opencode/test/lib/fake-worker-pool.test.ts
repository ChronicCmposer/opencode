import { describe, expect, test } from "bun:test"
import { SessionCancelledError, WorkerCrashedError } from "@/session/worker/dispatch"
import { capture, sid } from "./async"
import {
  FakeExecutorWorker,
  FakePool,
  FakeSessionWorker,
  createFakeExecutorPool,
  createFakeWorkerPool,
} from "./fake-worker-pool"

const run = (sessionID: string) => ({ sessionID: sid(sessionID), depth: 0 })
const execRun = (sessionID: string) => ({ sessionID: sid(sessionID), depth: 0 })

describe("FakeSessionWorker", () => {
  test("cooperative mode waits for an explicit finish", async () => {
    const worker = new FakeSessionWorker(1)
    const executing = worker.execute(run("ses_1"))
    expect(worker.pendingCount).toBe(1)

    worker.finish(sid("ses_1"))

    await expect(executing).resolves.toBeUndefined()
    expect(worker.runs).toHaveLength(1)
    expect(worker.alive).toBe(true)
  })

  test("finish mode resolves immediately", async () => {
    const worker = new FakeSessionWorker(1, { mode: "finish" })
    await expect(worker.execute(run("ses_1"))).resolves.toBeUndefined()
    expect(worker.pendingCount).toBe(0)
  })

  test("crash mode rejects a WorkerCrashedError and kills the worker", async () => {
    const worker = new FakeSessionWorker(7, { mode: "crash" })
    await expect(worker.execute(run("ses_1"))).rejects.toBeInstanceOf(WorkerCrashedError)
    expect(worker.alive).toBe(false)
  })

  test("cooperative crash rejects a WorkerCrashedError and kills the worker", async () => {
    const worker = new FakeSessionWorker(3)
    const executing = worker.execute(run("ses_1"))

    worker.crash(sid("ses_1"))

    await expect(executing).rejects.toBeInstanceOf(WorkerCrashedError)
    expect(worker.alive).toBe(false)
    expect(worker.runs[0]?.sessionID).toBe(sid("ses_1"))
  })

  test("fail mode rejects the configured error but keeps the worker alive", async () => {
    const boom = new Error("boom")
    const worker = new FakeSessionWorker(1, { mode: "fail", failure: () => boom })
    await expect(worker.execute(run("ses_1"))).rejects.toBe(boom)
    expect(worker.alive).toBe(true)
  })

  test("terminate rejects each pending run with its real SessionID", async () => {
    const worker = new FakeSessionWorker(1)
    const first = worker.execute(run("ses_1"))
    const second = worker.execute(run("ses_2"))

    await worker.terminate()

    const outcomes = await Promise.all([capture(first), capture(second)])
    for (const [index, outcome] of outcomes.entries()) {
      expect(outcome.ok).toBe(false)
      if (outcome.ok) continue
      expect(outcome.error).toBeInstanceOf(SessionCancelledError)
      expect((outcome.error as SessionCancelledError).sessionID).toBe(index === 0 ? "ses_1" : "ses_2")
    }
    expect(worker.alive).toBe(false)
    expect(worker.pendingCount).toBe(0)
  })

  test("a rejecting cancel port settles the run with a typed SessionCancelledError", async () => {
    const worker = new FakeSessionWorker(1, { onCancel: "reject" })
    const executing = worker.execute(run("ses_1"))

    worker.cancelPort.notify({ type: "cancel", sessionID: sid("ses_1") })

    const outcome = await capture(executing)
    expect(outcome.ok).toBe(false)
    if (!outcome.ok) {
      expect(outcome.error).toBeInstanceOf(SessionCancelledError)
      expect((outcome.error as SessionCancelledError).sessionID).toBe("ses_1")
    }
  })

  test("an ignoring cancel port leaves the run pending for grace expiry", async () => {
    const worker = new FakeSessionWorker(1, { onCancel: "ignore" })
    const executing = worker.execute(run("ses_1"))

    worker.cancelPort.notify({ type: "cancel", sessionID: sid("ses_1") })

    expect(worker.pendingCount).toBe(1)
    worker.finish(sid("ses_1"))
    await expect(executing).resolves.toBeUndefined()
  })
})

describe("FakeExecutorWorker", () => {
  test("auto mode records the configured result immediately", async () => {
    const worker = new FakeExecutorWorker(1, { value: "done" })
    await worker.execute(execRun("ses_1"))
    expect(worker.resultFor(sid("ses_1"))).toEqual({ sessionID: sid("ses_1"), value: "done" })
  })

  test("blocking mode stays pending until cancelled, then records the result", async () => {
    const worker = new FakeExecutorWorker(1, { mode: "blocking", value: 42 })
    const executing = worker.execute(execRun("ses_1"))

    expect(await worker.assertNotBusy(sid("ses_1"))).toBe(true)
    expect(worker.resultFor(sid("ses_1"))).toBeUndefined()

    await worker.cancel(sid("ses_1"))

    await expect(executing).resolves.toBeUndefined()
    expect(await worker.assertNotBusy(sid("ses_1"))).toBe(false)
    expect(worker.resultFor(sid("ses_1"))).toEqual({ sessionID: sid("ses_1"), value: 42 })
  })

  test("the executor pool exposes spawned workers and their results", () => {
    const pool = createFakeExecutorPool(1, { value: "x" })
    const spawned = pool.spawn()
    expect(pool.byID(spawned.id)).toBe(spawned)
    expect(pool.workers).toHaveLength(1)
  })
})

describe("FakePool", () => {
  test("creates K initial workers and logs spawned workers with increasing ids", () => {
    const pool = new FakePool(2, { firstSpawnId: 100 })
    expect(pool.workers.map((worker) => worker.id)).toEqual([1, 2])

    pool.spawn("headroom")
    pool.spawn("foreground")

    expect(pool.spawned.map((worker) => worker.id)).toEqual([100, 101])
  })

  test("modeFor parameterizes each worker by kind and origin", async () => {
    const pool = new FakePool(1, {
      modeFor: ({ kind, initial }) => (initial || kind === "foreground" ? "cooperative" : "finish"),
    })
    const headroom = pool.spawn("headroom") as FakeSessionWorker
    expect(headroom.mode).toBe("finish")
    await expect(headroom.execute(run("ses_1"))).resolves.toBeUndefined()
  })

  test("terminateAll terminates every initial worker", async () => {
    const pool = createFakeWorkerPool(2)
    await pool.terminateAll()
    expect(pool.workers.every((worker) => !worker.alive)).toBe(true)
  })

  test("rejects an invalid worker count loudly", () => {
    expect(() => new FakePool(0)).toThrow(/invalid worker count/)
    expect(() => createFakeWorkerPool(-1)).toThrow(/invalid worker count/)
  })
})
