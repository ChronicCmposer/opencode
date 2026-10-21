import { describe, expect, test } from "bun:test"
import type { SessionID } from "@/session/schema"
import { SessionCancelledError, WorkerCrashedError } from "@/session/worker/dispatch"
import { FakePool, FakeSessionWorker, createFakeWorkerPool } from "./fake-worker-pool"

const sid = (value: string) => value as SessionID
const run = (sessionID: string) => ({ sessionID: sid(sessionID), depth: 0 })

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

  test("terminate rejects pending runs with SessionCancelledError", async () => {
    const worker = new FakeSessionWorker(1)
    const executing = worker.execute(run("ses_1"))

    await worker.terminate()

    await expect(executing).rejects.toBeInstanceOf(SessionCancelledError)
    expect(worker.alive).toBe(false)
    expect(worker.pendingCount).toBe(0)
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
