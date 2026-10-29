// Frame-aligned durable events: a durable event whose JSON body is 4–16 MiB must
// not be dropped by the 4 MiB worker frame cap. Core already offloads an
// oversized durable payload to the shared `event_blob` store before fan-out, but
// the in-memory `EventV2` payload still carries the full properties, so the
// egress frame would exceed the cap and be rejected. The events port must carry
// a small blob POSITION reference instead — one the hub can resolve from the
// shared SQLite blob — and it must never throw an unhandled `FrameTooLargeError`
// that kills the batch.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { Database, type SQLQueryBindings } from "bun:sqlite"
import { afterEach, describe, expect, test } from "bun:test"
import { DurableLog } from "@opencode-ai/core/durable-log"
import { Event } from "@opencode-ai/schema/event"
import { ENVELOPE_HEADROOM_BYTES, MAX_INLINE_EVENT_BYTES, decodeFrame, encodeFrameChunks } from "@/session/egress/frame"
import { createEgressReceiver } from "@/session/egress/receiver"
import { createEgressSender } from "@/session/egress/sender"
import { MAX_WORKER_FRAME_BYTES, eventFrame, type EventsFrame, type EventV2Event } from "@/session/worker/frame"
import { pollUntil } from "../../lib/poll"

const MiB = 1024 * 1024

interface Received {
  readonly frame: EventsFrame
  readonly text: string
}

function isBlobPositionReference(
  value: unknown,
): value is { readonly __eventBlobPosition: { readonly aggregateID: string; readonly seq: number }; readonly frameBytes: number } {
  if (typeof value !== "object" || value === null) return false
  const candidate = value as Record<string, unknown>
  const position = candidate["__eventBlobPosition"]
  return (
    typeof position === "object" &&
    position !== null &&
    typeof (position as Record<string, unknown>)["aggregateID"] === "string" &&
    typeof (position as Record<string, unknown>)["seq"] === "number" &&
    typeof candidate["frameBytes"] === "number"
  )
}

function durableEvent(id: string, sessionID: string, text: string): EventV2Event {
  return { id, type: "session.next.prompted", properties: { sessionID, text } }
}

const cleanups: Array<() => void> = []

afterEach(() => {
  while (cleanups.length > 0) cleanups.pop()?.()
})

/**
 * A live worker→hub events channel: real `MessageChannel`, real sender on the
 * worker side, real receiver on the hub side. The receiver records the exact
 * wire text it accepted, so the assertions see what actually crossed.
 */
function harness() {
  const channel = new MessageChannel()
  const received: Received[] = []
  const acks: number[] = []
  const receiver = createEgressReceiver({
    port: channel.port2,
    onFrame: (frame, text) => void received.push({ frame, text }),
    onAck: (payload) => void acks.push(payload?.count ?? 0),
  })
  const sender = createEgressSender({ port: channel.port1 })
  cleanups.push(() => {
    receiver.close()
    channel.port1.close()
    channel.port2.close()
  })
  return { sender, receiver, received, acks }
}

describe("frame-aligned durable events", () => {
  test("the default inline bound leaves envelope headroom inside the frame cap", () => {
    // Pinned exactly once: an inline payload must always fit one frame. The bound
    // is the cap MINUS the envelope, not the cap itself, or a body just under
    // 4 MiB would take the reference path with no event_blob row to resolve.
    expect(MAX_WORKER_FRAME_BYTES).toBeGreaterThan(DurableLog.DEFAULT_INLINE_PAYLOAD_LIMIT_BYTES)
    expect(DurableLog.DEFAULT_INLINE_PAYLOAD_LIMIT_BYTES).toBe(MAX_INLINE_EVENT_BYTES)
    expect(MAX_INLINE_EVENT_BYTES).toBe(MAX_WORKER_FRAME_BYTES - ENVELOPE_HEADROOM_BYTES)
  })

  test("a 5 MiB durable event crosses as a blob position reference instead of throwing", async () => {
    const { sender, receiver, received, acks } = harness()
    const big = durableEvent("evt_big", "ses_1", "x".repeat(5 * MiB))

    // The sender must substitute a reference instead of rejecting the batch.
    expect(() => sender.pushDurable(big, { aggregateID: "ses_1", seq: 1 })).not.toThrow()

    await pollUntil(() => received.length === 1, { message: "oversized durable event never reached the hub" })
    const [frame] = received
    expect(receiver.lastError).toBeUndefined()
    expect(Buffer.byteLength(frame!.text, "utf8")).toBeLessThanOrEqual(MAX_WORKER_FRAME_BYTES)

    const payload = frame!.frame.kind === "event" ? frame!.frame.payload : undefined
    expect(payload?.aggregateID).toBe("ses_1")
    expect(payload?.seq).toBe(1)
    expect(isBlobPositionReference(payload?.event.properties)).toBe(true)
    const reference = payload!.event.properties as {
      readonly __eventBlobPosition: { readonly aggregateID: string; readonly seq: number }
      readonly frameBytes: number
    }
    expect(reference.__eventBlobPosition).toEqual({ aggregateID: "ses_1", seq: 1 })
    expect(reference.frameBytes).toBeGreaterThan(MAX_WORKER_FRAME_BYTES)
    await pollUntil(() => acks.length === 1, { message: "oversized durable was not acknowledged" })
    expect(acks).toEqual([1])
  })

  test("the batch after an oversized durable still delivers (no silent drop)", async () => {
    const { sender, receiver, received, acks } = harness()
    const big = durableEvent("evt_big", "ses_1", "x".repeat(6 * MiB))
    const small = durableEvent("evt_small", "ses_1", "after")

    sender.pushDurable(big, { aggregateID: "ses_1", seq: 1 })
    sender.pushDurable(small, { aggregateID: "ses_1", seq: 2 })

    await pollUntil(() => received.length === 2, {
      message: "the durable frame after the oversized one was dropped",
    })
    expect(receiver.lastError).toBeUndefined()
    expect(receiver.received).toBe(2)
    expect(received.every(({ text }) => Buffer.byteLength(text, "utf8") <= MAX_WORKER_FRAME_BYTES)).toBe(true)
    const last = received[1]!.frame
    expect(last.kind === "event" ? last.payload.event : undefined).toEqual(small)
    await pollUntil(() => acks.length === 2)
    expect(acks).toEqual([1, 1])
  })

  test("the position reference resolves to the original body from the shared blob", async () => {
    const { sender, received } = harness()
    const big = durableEvent("evt_big", "ses_7", "y".repeat(5 * MiB))
    sender.pushDurable(big, { aggregateID: "ses_7", seq: 3 })
    await pollUntil(() => received.length === 1)

    const frame = received[0]!.frame
    if (frame.kind !== "event") throw new Error(`expected an event frame, got ${frame.kind}`)
    const reference = frame.payload.event.properties
    if (!isBlobPositionReference(reference)) throw new Error("wire payload is not a blob position reference")

    // The hub shares the worker's SQLite file. Simulate the committed row the
    // worker wrote before egress: a content-addressed file plus its blob row.
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-egress-oversize-"))
    const database = new Database(":memory:")
    cleanups.push(() => {
      database.close()
      fs.rmSync(directory, { recursive: true, force: true })
    })
    database.exec(
      "CREATE TABLE event_blob (aggregate_id text NOT NULL, seq integer NOT NULL, type text NOT NULL, hash text NOT NULL, size integer NOT NULL, data text, file_path text)",
    )
    const stored = JSON.stringify(big.properties)
    const hash = DurableLog.digest(stored)
    DurableLog.writeBlobFile(directory, hash, stored)
    database
      .query("INSERT INTO event_blob (aggregate_id, seq, type, hash, size, data, file_path) VALUES (?, ?, ?, ?, ?, NULL, ?)")
      .run("ses_7", 3, "session.next.prompted", hash, Buffer.byteLength(stored), hash)

    const connection: DurableLog.Connection = {
      all: (sql, params) =>
        database.query(sql).all(...((params ?? []) as SQLQueryBindings[])) as ReadonlyArray<Record<string, unknown>>,
      run: (sql, params) => ({
        changes: database.query(sql).run(...((params ?? []) as SQLQueryBindings[])).changes,
      }),
      exec: (sql) => database.exec(sql),
    }
    const position = reference.__eventBlobPosition
    const rows = DurableLog.loadBlobRows(connection, position.aggregateID, [position.seq])
    const row = rows.get(position.seq)
    expect(row).toBeDefined()

    const resolved = DurableLog.resolveBlobData(
      {
        id: Event.ID.make(big.id),
        type: big.type,
        seq: position.seq,
        aggregateID: position.aggregateID,
        data: { __eventBlob: String(row!["hash"]), size: Number(row!["size"]) },
      },
      row,
      { dir: directory, limitBytes: DurableLog.DEFAULT_BLOB_LIMIT_BYTES, supportsTable: true },
      new Map(),
    )
    expect(resolved).toEqual(JSON.parse(stored))
  })

  test("an oversized core blob reference crosses as a position reference without a second body serialisation", () => {
    const position = { aggregateID: "ses_offloaded", seq: 4 }
    const offloaded = { __eventBlob: DurableLog.digest("already-offloaded"), size: 5 * MiB }
    const frame = eventFrame({
      event: { id: "evt_offloaded", type: "session.next.prompted", properties: offloaded },
      ...position,
    })

    // The hub can only resolve a `__eventBlobPosition`, never the raw marker.
    const [text] = encodeFrameChunks([frame]).flat()
    const decoded = decodeFrame(text!)
    if (decoded.kind !== "event") throw new Error(`expected an event frame, got ${decoded.kind}`)
    expect(decoded.payload.event.properties).toEqual({ __eventBlobPosition: position, frameBytes: 5 * MiB })
  })

  test("a frame that already carries a position reference is forwarded unchanged", () => {
    const position = { aggregateID: "ses_offloaded", seq: 5 }
    const reference = { __eventBlobPosition: position, frameBytes: 5 * MiB }
    const frame = eventFrame({
      event: { id: "evt_reference", type: "session.next.prompted", properties: reference },
      ...position,
    })

    const [text] = encodeFrameChunks([frame]).flat()
    const decoded = decodeFrame(text!)
    if (decoded.kind !== "event") throw new Error(`expected an event frame, got ${decoded.kind}`)
    expect(decoded.payload.event.properties).toEqual(reference)
  })
})
