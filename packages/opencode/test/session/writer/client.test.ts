// Phase 7 — writer client hardening tests (steps 12, 14, 15, 16).
//
// Uses a controllable fake isolate (`./fake-worker.ts`) so the ready handshake,
// generation echo, ready timeout, `id: -1` parse-failure sentinel and malformed
// response handling are all exercised without a real database.

import { describe, expect, test } from "bun:test"
import path from "node:path"
import {
  WriterCrashedError,
  WriterRemoteError,
  WriterTimeoutError,
  createWriterClient,
  serveWriterPort,
  type HubWriter,
} from "@/session/writer/client"
import { WorkerWriterError, createWorkerWriterClient } from "@/session/worker/writer-client"
import { FAULTS_ENV } from "@/testing/faults"
import { flush } from "../../lib/async"
import { tmpdir } from "../../fixture/fixture"
import { SCHEMA, healthyEnv } from "./fixtures"

const fakeEntry = new URL("./fake-worker.ts", import.meta.url)

/** Transaction methods for stubs whose tests never exercise them; fail loud if reached. */
const noTx: Pick<HubWriter, "begin" | "txQuery" | "commit" | "rollback"> = {
  begin: async () => {
    throw new Error("begin was not expected in this test")
  },
  txQuery: async () => {
    throw new Error("txQuery was not expected in this test")
  },
  commit: async () => {
    throw new Error("commit was not expected in this test")
  },
  rollback: async () => {
    throw new Error("rollback was not expected in this test")
  },
}

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
      ...noTx,
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
      ...noTx,
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
      ...noTx,
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
      ...noTx,
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
      ...noTx,
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

describe("writer port server: transactions over the real writer (step 17)", () => {
  const INSERT_SEQUENCE = "INSERT INTO event_sequence (aggregate_id, seq, owner_id) VALUES (?, ?, ?)"
  const SELECT_SEQUENCES = "SELECT aggregate_id FROM event_sequence ORDER BY aggregate_id"

  /** Open a real writer isolate over a temp file and serve it to a worker client. */
  async function servedWriter(tmpPath: string) {
    const writer = createWriterClient({ env: healthyEnv })
    await writer.open(path.join(tmpPath, "writer.db"))
    await writer.migrate(SCHEMA)
    const channel = new MessageChannel()
    const dispose = serveWriterPort(writer, channel.port1)
    const client = createWorkerWriterClient(channel.port2)
    return {
      writer,
      client,
      async [Symbol.asyncDispose]() {
        client.dispose()
        dispose()
        await writer.close()
      },
    }
  }

  test("begin → txQuery → commit persists, and a concurrent non-tx query queues behind the pin", async () => {
    await using tmp = await tmpdir()
    await using served = await servedWriter(tmp.path)

    const handle = await served.client.begin()
    await served.client.txQuery(handle, "run", INSERT_SEQUENCE, ["a", 0, null])

    // A non-tx read issued while the transaction is pinned must not observe the
    // uncommitted row and must not resolve until the pin clears (head-of-line).
    let observed: unknown = undefined
    const read = served.client.query("all", SELECT_SEQUENCES).then((rows) => {
      observed = rows
      return rows
    })
    await flush()
    expect(observed).toBeUndefined()

    await served.client.commit(handle)
    expect(await read).toEqual([{ aggregate_id: "a" }])
  })

  test("rollback discards the pinned transaction's writes", async () => {
    await using tmp = await tmpdir()
    await using served = await servedWriter(tmp.path)

    const handle = await served.client.begin()
    await served.client.txQuery(handle, "run", INSERT_SEQUENCE, ["b", 0, null])
    await served.client.rollback(handle)

    expect(await served.client.query("all", SELECT_SEQUENCES)).toEqual([])
  })

  test("a failing txQuery surfaces a typed error and the rollback discards partial writes", async () => {
    await using tmp = await tmpdir()
    await using served = await servedWriter(tmp.path)

    const handle = await served.client.begin()
    await served.client.txQuery(handle, "run", INSERT_SEQUENCE, ["c", 0, null])

    const error = await served.client
      .txQuery(handle, "run", "INSERT INTO missing_table VALUES (1)")
      .then(
        () => undefined,
        (cause: unknown) => cause,
      )
    expect(error).toBeInstanceOf(WorkerWriterError)
    expect((error as WorkerWriterError).message).toContain("missing_table")

    await served.client.rollback(handle)
    expect(await served.client.query("all", SELECT_SEQUENCES)).toEqual([])

    // The failed transaction released its pin: a fresh transaction can begin.
    const next = await served.client.begin()
    await served.client.rollback(next)
  })

  test("a mid-transaction abort invalidates the tx with a typed error and no hang", async () => {
    await using tmp = await tmpdir()
    const file = path.join(tmp.path, "writer.db")
    {
      await using seed = createWriterClient({ env: healthyEnv })
      await seed.open(file)
      await seed.migrate(SCHEMA)
    }

    const writer = createWriterClient({ env: { [FAULTS_ENV]: "abortTxAfter@writer:tx" } })
    await writer.open(file)
    const channel = new MessageChannel()
    const dispose = serveWriterPort(writer, channel.port1)
    const client = createWorkerWriterClient(channel.port2)

    const handle = await client.begin()
    const error = await client.txQuery(handle, "run", INSERT_SEQUENCE, ["e", 0, null]).then(
      () => undefined,
      (cause: unknown) => cause,
    )
    expect(error).toBeInstanceOf(WorkerWriterError)
    expect((error as WorkerWriterError).tag).toBe("ABORTED")

    // The aborted transaction was abandoned (no hang): a fresh one begins and the
    // abandoned write never landed.
    const next = await client.begin()
    await client.rollback(next)
    expect(await client.query("all", SELECT_SEQUENCES)).toEqual([])

    client.dispose()
    dispose()
    await writer.close()
  })

  test("a worker crash during a transaction commit surfaces a typed error and the client recovers", async () => {
    await using tmp = await tmpdir()
    const file = path.join(tmp.path, "writer.db")
    {
      await using seed = createWriterClient({ env: healthyEnv })
      await seed.open(file)
      await seed.migrate(SCHEMA)
    }

    const writer = createWriterClient({ env: { [FAULTS_ENV]: "crashWorker@writer:tx-commit" } })
    await writer.open(file)
    const channel = new MessageChannel()
    const dispose = serveWriterPort(writer, channel.port1)
    const client = createWorkerWriterClient(channel.port2)

    const handle = await client.begin()
    await client.txQuery(handle, "run", INSERT_SEQUENCE, ["d", 0, null])

    // The isolate dies at the commit boundary: the caller is rejected (never
    // retried, never observed as committed) and the request does not hang.
    const commitError = await client.commit(handle).then(
      () => undefined,
      (cause: unknown) => cause,
    )
    expect(commitError).toBeInstanceOf(WorkerWriterError)
    expect((commitError as WorkerWriterError).tag).toBe("WRITER_CRASHED")

    // The abandoned transaction left nothing partial behind, and a read respawns
    // the writer with a bumped generation.
    expect(await client.query("all", SELECT_SEQUENCES)).toEqual([])
    expect(writer.generation).toBe(2)

    // The old handle died with the isolate: it no longer names a pinned transaction.
    const staleError = await client.txQuery(handle, "run", INSERT_SEQUENCE, ["e", 0, null]).then(
      () => undefined,
      (cause: unknown) => cause,
    )
    expect(staleError).toBeInstanceOf(WorkerWriterError)
    expect((staleError as WorkerWriterError).tag).toBe("TRANSACTION")

    // A fresh transaction begins on the recovered writer.
    const next = await client.begin()
    await client.rollback(next)

    client.dispose()
    dispose()
    await writer.close()
  })
})
