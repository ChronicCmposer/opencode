// Phase 7 — writer client hardening tests (steps 12, 14, 15, 16).
//
// Uses a controllable fake isolate (`./fake-worker.ts`) so the ready handshake,
// generation echo, ready timeout, `id: -1` parse-failure sentinel and malformed
// response handling are all exercised without a real database.

import { describe, expect, test } from "bun:test"
import {
  WriterCrashedError,
  WriterRemoteError,
  WriterTimeoutError,
  createWriterClient,
  serveWriterPort,
  type HubWriter,
} from "@/session/writer/client"
import { WorkerWriterError, createWorkerWriterClient } from "@/session/worker/writer-client"
import { flush } from "../../lib/async"

const fakeEntry = new URL("./fake-worker.ts", import.meta.url)

describe("writer client: ready handshake (steps 9, 12, 29)", () => {
  test("accepts a matching ready generation and serves requests", async () => {
    await using client = createWriterClient({ entry: fakeEntry, timeoutMs: 1000 })

    const opened = await client.open("x.db")
    expect(opened).toMatchObject({ opened: true, filename: "x.db" })
    expect(client.generation).toBe(1)
    expect(client.isAlive).toBe(true)
  })

  test("a worker that exits before ready settles the ready promise, then respawns (steps 8, 10)", async () => {
    await using client = createWriterClient({
      entry: fakeEntry,
      workerData: { mode: "exit-once" },
      timeoutMs: 1000,
    })

    await expect(client.open("x.db")).rejects.toBeInstanceOf(WriterCrashedError)
    expect(client.isAlive).toBe(false)

    const opened = await client.open("x.db")
    expect(opened.opened).toBe(true)
    expect(client.generation).toBe(2)
    expect(client.isAlive).toBe(true)
  })

  test("rejects a mismatched ready generation and never marks the worker alive", async () => {
    await using client = createWriterClient({
      entry: fakeEntry,
      workerData: { readyGeneration: 99 },
      timeoutMs: 1000,
    })

    await expect(client.open("x.db")).rejects.toThrow(/generation/)
    expect(client.isAlive).toBe(false)
  })
})

describe("writer client: failure surfacing (steps 11, 14, 15, 16)", () => {
  test("times out with a tagged WriterTimeoutError when the worker never becomes ready", async () => {
    await using client = createWriterClient({
      entry: fakeEntry,
      workerData: { mode: "silent" },
      timeoutMs: 50,
    })

    const error = await client.open("x.db").then(
      () => undefined,
      (cause: unknown) => cause,
    )
    expect(error).toBeInstanceOf(WriterTimeoutError)
    expect((error as WriterTimeoutError).tag).toBe("WRITER_TIMEOUT")
  })

  test("surfaces the id:-1 parse-failure sentinel as a crash", async () => {
    const crashes: string[] = []
    await using client = createWriterClient({
      entry: fakeEntry,
      workerData: { mode: "sentinel" },
      timeoutMs: 1000,
      onCrash: (error) => crashes.push(error.message),
    })

    await expect(client.open("x.db")).rejects.toBeInstanceOf(WriterCrashedError)
    expect(client.isAlive).toBe(false)
  })

  test("crashes on a malformed worker response", async () => {
    await using client = createWriterClient({
      entry: fakeEntry,
      workerData: { mode: "garbage" },
      timeoutMs: 1000,
    })

    await expect(client.open("x.db")).rejects.toBeInstanceOf(WriterCrashedError)
    expect(client.isAlive).toBe(false)
  })
})

describe("writer port server (step 17)", () => {
  test("serves a worker query through the hub writer", async () => {
    const calls: Array<{ method: string; sql: string; params: readonly unknown[] }> = []
    const hub: HubWriter = {
      query: async (method, sql, params) => {
        calls.push({ method, sql, params })
        return [{ n: 1 }]
      },
      publish: async () => ({ seq: null, committed: false }),
    }
    const channel = new MessageChannel()
    const dispose = serveWriterPort(hub, channel.port1)
    const client = createWorkerWriterClient(channel.port2)

    const value = await client.query("all", "select 1", [7])

    expect(value).toEqual([{ n: 1 }])
    expect(calls).toEqual([{ method: "all", sql: "select 1", params: [7] }])
    client.dispose()
    dispose()
  })

  test("serves a durable publish and returns seq/committed", async () => {
    const published: Array<{ event: unknown; durable: boolean }> = []
    const hub: HubWriter = {
      query: async () => null,
      publish: async (event, options) => {
        published.push({ event, durable: options.durable })
        return { seq: 5, committed: true }
      },
    }
    const channel = new MessageChannel()
    const dispose = serveWriterPort(hub, channel.port1)
    const client = createWorkerWriterClient(channel.port2)

    const result = await client.publish({ id: "e1", type: "t", aggregateID: "a", data: { k: 1 } })

    expect(result).toEqual({ seq: 5, committed: true })
    expect(published).toEqual([
      { event: { id: "e1", type: "t", aggregateID: "a", data: { k: 1 } }, durable: true },
    ])
    client.dispose()
    dispose()
  })

  test("forwards a hub writer rejection as a typed worker error", async () => {
    const hub: HubWriter = {
      query: async () => {
        throw new WriterRemoteError("SQLITE_BUSY", "database is locked")
      },
      publish: async () => ({ seq: null, committed: false }),
    }
    const channel = new MessageChannel()
    const dispose = serveWriterPort(hub, channel.port1)
    const client = createWorkerWriterClient(channel.port2)

    const error = await client.query("all", "select 1").then(
      () => undefined,
      (cause: unknown) => cause,
    )

    expect(error).toBeInstanceOf(WorkerWriterError)
    expect((error as WorkerWriterError).tag).toBe("SQLITE_BUSY")
    expect((error as WorkerWriterError).message).toBe("database is locked")
    client.dispose()
    dispose()
  })

  test("rejects an invalid query method at the boundary", async () => {
    const hub: HubWriter = {
      query: async () => null,
      publish: async () => ({ seq: null, committed: false }),
    }
    const channel = new MessageChannel()
    const dispose = serveWriterPort(hub, channel.port1)
    const client = createWorkerWriterClient(channel.port2)

    const error = await client.query("bogus", "select 1").then(
      () => undefined,
      (cause: unknown) => cause,
    )

    expect(error).toBeInstanceOf(WorkerWriterError)
    expect((error as WorkerWriterError).tag).toBe("PROTOCOL")
    client.dispose()
    dispose()
  })

  test("does not throw when the worker end is gone before the reply", async () => {
    let release!: () => void
    const gate = new Promise<void>((resolve) => {
      release = resolve
    })
    const hub: HubWriter = {
      query: async () => {
        await gate
        return "late"
      },
      publish: async () => ({ seq: null, committed: false }),
    }
    const channel = new MessageChannel()
    const dispose = serveWriterPort(hub, channel.port1)
    const client = createWorkerWriterClient(channel.port2)

    const pending = client.query("all", "select 1").then(
      () => undefined,
      () => undefined,
    )
    await flush()
    client.dispose()
    channel.port2.close()
    release()
    await pending

    // A guarded reply to a closed port must be swallowed, not thrown.
    expect(() => dispose()).not.toThrow()
  })
})
