// Phase 7 — writer client hardening tests (steps 12, 14, 15, 16).
//
// Uses a controllable fake isolate (`./fake-worker.ts`) so the ready handshake,
// generation echo, ready timeout, `id: -1` parse-failure sentinel and malformed
// response handling are all exercised without a real database.

import { describe, expect, test } from "bun:test"
import { WriterCrashedError, WriterTimeoutError, createWriterClient } from "@/session/writer/client"

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
