// Step 16 — worker-side writer client over a `MessagePort`.
//
// `createWorkerWriterClient(port)` is the worker half of the hub writer port: it
// forwards the reduced `WriterClientHandle` (`query` / `publish`) to whichever
// process serves the hub-owned `WriterClient` on the other end. These tests drive
// it over an in-process `MessageChannel` pair with a hand-rolled responder, so no
// isolate or database is needed.

import { describe, expect, test } from "bun:test"
import { WorkerWriterError, createWorkerWriterClient, type WorkerWriterFrame } from "@/session/worker/writer-client"
import { flush } from "../../lib/async"

/** Serve raw frames on `port` with a hand-rolled responder (no client/server dep). */
function respondWith(port: MessagePort, handler: (frame: WorkerWriterFrame) => WorkerWriterFrame | undefined): void {
  port.addEventListener("message", (event) => {
    const reply = handler(event.data as WorkerWriterFrame)
    if (reply !== undefined) port.postMessage(reply)
  })
  port.start()
}

describe("worker writer client (step 16)", () => {
  test("round-trips a query over a MessagePort", async () => {
    const channel = new MessageChannel()
    const seen: WorkerWriterFrame[] = []
    respondWith(channel.port1, (frame) => {
      seen.push(frame)
      return frame.kind === "request" && frame.op === "query"
        ? { kind: "reply", id: frame.id, ok: "query", value: [{ n: 1 }] }
        : undefined
    })
    const client = createWorkerWriterClient(channel.port2)

    const value = await client.query("all", "select 1", [7])

    expect(value).toEqual([{ n: 1 }])
    expect(seen).toEqual([
      { kind: "request", id: 1, op: "query", method: "all", sql: "select 1", params: [7] },
    ])
    client.dispose()
  })

  test("round-trips a durable publish and returns seq/committed", async () => {
    const channel = new MessageChannel()
    const seen: WorkerWriterFrame[] = []
    respondWith(channel.port1, (frame) => {
      seen.push(frame)
      return frame.kind === "request" && frame.op === "publish"
        ? { kind: "reply", id: frame.id, ok: "publish", result: { seq: 3, committed: true } }
        : undefined
    })
    const client = createWorkerWriterClient(channel.port2)

    const result = await client.publish({ id: "e1", type: "t", aggregateID: "a", data: { k: 1 } })

    expect(result).toEqual({ seq: 3, committed: true })
    expect(seen).toEqual([
      { kind: "request", id: 1, op: "publish", event: { id: "e1", type: "t", aggregateID: "a", data: { k: 1 } } },
    ])
    client.dispose()
  })

  test("rejects with a typed WorkerWriterError on an error reply", async () => {
    const channel = new MessageChannel()
    respondWith(channel.port1, (frame) =>
      frame.kind === "request"
        ? { kind: "reply", id: frame.id, ok: "error", tag: "SQLITE_BUSY", message: "database is locked" }
        : undefined,
    )
    const client = createWorkerWriterClient(channel.port2)

    const error = await client.query("all", "select 1").then(
      () => undefined,
      (cause: unknown) => cause,
    )

    expect(error).toBeInstanceOf(WorkerWriterError)
    expect((error as WorkerWriterError).tag).toBe("SQLITE_BUSY")
    expect((error as WorkerWriterError).message).toBe("database is locked")
    client.dispose()
  })

  test("ignores unsolicited non-reply frames and keeps serving", async () => {
    const channel = new MessageChannel()
    respondWith(channel.port1, (frame) => {
      if (frame.kind !== "request" || frame.op !== "query") return undefined
      // A stray request: the worker client never serves requests, so it must be
      // ignored without disturbing the pending reply.
      channel.port1.postMessage({ kind: "request", id: 999, op: "query", method: "all", sql: "stray" })
      return { kind: "reply", id: frame.id, ok: "query", value: "ok" }
    })
    const client = createWorkerWriterClient(channel.port2)

    expect(await client.query("all", "select 1")).toBe("ok")
    client.dispose()
  })

  test("correlates concurrent requests by id even when replies arrive out of order", async () => {
    const channel = new MessageChannel()
    respondWith(channel.port1, (frame) => {
      if (frame.kind !== "request" || frame.op !== "query") return undefined
      if (frame.sql === "slow") {
        setTimeout(
          () => channel.port1.postMessage({ kind: "reply", id: frame.id, ok: "query", value: "slow-result" }),
          10,
        )
        return undefined
      }
      return { kind: "reply", id: frame.id, ok: "query", value: "fast-result" }
    })
    const client = createWorkerWriterClient(channel.port2)

    const [slow, fast] = await Promise.all([client.query("all", "slow"), client.query("all", "fast")])

    expect(slow).toBe("slow-result")
    expect(fast).toBe("fast-result")
    client.dispose()
  })

  test("dispose rejects every in-flight request", async () => {
    const channel = new MessageChannel()
    respondWith(channel.port1, () => undefined) // never replies
    const client = createWorkerWriterClient(channel.port2)

    const pending = client.query("all", "select 1").then(
      () => undefined,
      (cause: unknown) => cause,
    )
    await flush()
    client.dispose()

    expect(await pending).toBeInstanceOf(Error)
  })
})
