// Inline durable events at the frame boundary (P0, issue 4).
//
// A durable event just under 4 MiB is small enough for the D16 inline bound
// (`event.data` holds the body) but, once wrapped in the worker envelope, too
// large for one 4 MiB frame. The sender correctly substitutes a
// `{ __eventBlobPosition, frameBytes }` reference, but no `event_blob` row exists
// for an inline body — so the hub must fall back to the committed inline row at
// `(aggregateID, seq)`. Otherwise the event is silently dropped from SSE.
//
// The two halves of the defense are covered here:
//   1. the hub resolver recovers an inline body and republishes it exactly once;
//   2. the DurableLog default inline bound leaves envelope headroom, so a payload this
//      close to the cap is offloaded by reference before egress.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Schema } from "effect"
import { DurableLog } from "@opencode-ai/core/durable-log"
import { openConnection } from "@opencode-ai/core/database/connection"
import { Event } from "@opencode-ai/schema/event"
import type { LiveEvent, LiveEventPublisher } from "@/event-v2-bridge"
import { FrameTooLargeError, encodeFrame } from "@/session/egress/frame"
import { createEgressSender } from "@/session/egress/sender"
import { createEventBlobResolver, createHubEventSink, serveEventsPort } from "@/session/worker/hub"
import { eventFrame, MAX_WORKER_FRAME_BYTES, type EventV2Event } from "@/session/worker/frame"
import { pollUntil } from "../../lib/poll"

const AGGREGATE = "ses_inline_boundary"
const SEQ = 1
const NOTE_TYPE = "test.frame-inline-boundary.note"

/** JSON body size chosen to sit just below the 4 MiB cap but above one frame. */
const INLINE_BODY_BYTES = MAX_WORKER_FRAME_BYTES - 32

const Note = Event.define({
  type: NOTE_TYPE,
  durable: { version: 1, aggregate: "sessionID" },
  schema: {
    sessionID: Schema.String,
    text: Schema.String,
  },
})

const cleanups: Array<() => void> = []

afterEach(() => {
  while (cleanups.length > 0) cleanups.pop()?.()
})

/** An event whose encoded property JSON is exactly `INLINE_BODY_BYTES`. */
function inlineBoundaryEvent(): EventV2Event {
  const empty = { sessionID: AGGREGATE, text: "" }
  const overhead = Buffer.byteLength(JSON.stringify(empty))
  return {
    id: "evt_inline_boundary",
    type: NOTE_TYPE,
    properties: { sessionID: AGGREGATE, text: "x".repeat(INLINE_BODY_BYTES - overhead) },
  }
}

function legacySchema(): string {
  return `
    CREATE TABLE event_sequence (
      aggregate_id text PRIMARY KEY,
      seq integer NOT NULL,
      owner_id text
    );
    CREATE TABLE event (
      id text PRIMARY KEY,
      aggregate_id text NOT NULL,
      seq integer NOT NULL,
      type text NOT NULL,
      data text NOT NULL
    );
    CREATE UNIQUE INDEX event_aggregate_seq_idx ON event(aggregate_id, seq);
    CREATE TABLE event_blob (
      aggregate_id text NOT NULL,
      seq integer NOT NULL,
      type text NOT NULL,
      hash text NOT NULL,
      size integer NOT NULL,
      data text,
      file_path text,
      PRIMARY KEY (aggregate_id, seq)
    );
  `
}

interface Fixture {
  readonly connection: ReturnType<typeof openConnection>
  readonly blobDir: string
  readonly query: (sql: string, params: ReadonlyArray<unknown>) => Promise<ReadonlyArray<Record<string, unknown>>>
}

function fixture(): Fixture {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-inline-boundary-"))
  const connection = openConnection({ filename: path.join(directory, "opencode.db") })
  cleanups.push(() => {
    connection.close()
    fs.rmSync(directory, { recursive: true, force: true })
  })
  connection.exec(legacySchema())
  return {
    connection,
    blobDir: path.join(directory, "event-blobs"),
    query: async (sql, params) => connection.all<Record<string, unknown>>(sql, params),
  }
}

function recordingPublisher(): { readonly publisher: LiveEventPublisher; readonly published: LiveEvent[] } {
  const published: LiveEvent[] = []
  return {
    published,
    publisher: {
      publish: (event) => void published.push(event),
      publishDefinition: (definition, data) => void published.push({ type: definition.type, data }),
    },
  }
}

describe("inline durable event at the frame boundary", () => {
  test("crosses SSE and is delivered exactly once when only event.data is inline", async () => {
    const db = fixture()
    const event = inlineBoundaryEvent()
    const stored = JSON.stringify(event.properties)

    // The body is inline: under the D16 schema bound but above the headroom-
    // aware default, with no event_blob row.
    expect(Buffer.byteLength(stored)).toBeGreaterThan(DurableLog.DEFAULT_INLINE_PAYLOAD_LIMIT_BYTES)
    expect(Buffer.byteLength(stored)).toBeLessThanOrEqual(DurableLog.DEFAULT_BLOB_LIMIT_BYTES)
    db.connection.run("INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?, ?, ?, ?, ?)", [
      event.id,
      AGGREGATE,
      SEQ,
      event.type,
      stored,
    ])
    expect(db.connection.all("SELECT COUNT(*) AS count FROM event_blob")).toEqual([{ count: 0 }])

    // The envelope pushes the same body above one frame; the sender turns that
    // into the position reference the hub has to heal.
    expect(() => encodeFrame(eventFrame({ event, aggregateID: AGGREGATE, seq: SEQ }))).toThrow(FrameTooLargeError)

    const { publisher, published } = recordingPublisher()
    const sink = createHubEventSink({
      publisher,
      resolveBlob: createEventBlobResolver({ query: db.query, blobDir: db.blobDir }),
    })
    const channel = new MessageChannel()
    const dispose = serveEventsPort(channel.port1, sink)
    const sender = createEgressSender({ port: channel.port2 })
    cleanups.push(() => {
      dispose()
      channel.port1.close()
      channel.port2.close()
    })

    sender.pushDurable(event, { aggregateID: AGGREGATE, seq: SEQ })

    await pollUntil(() => sink.acks === 1, { message: "the inline durable was never acknowledged" })
    expect(published).toHaveLength(1)
    expect(sink.resyncs).toBe(0)
    const [live] = published
    expect(live?.id).toBe(event.id)
    expect(live?.type).toBe(event.type)
    expect(live?.data).toEqual(event.properties)
    expect(live?.data).not.toHaveProperty("__eventBlobPosition")
    expect(live?.data).not.toHaveProperty("__eventBlob")
    expect(live?.durable).toEqual({ aggregateID: AGGREGATE, seq: SEQ, version: 1 })
  })

  test("the DurableLog default inline bound leaves envelope headroom and offloads by reference", () => {
    // A payload this close to the 4 MiB cap would not fit once framed; the
    // default DurableLog bound must therefore reserve headroom and store it as a blob.
    expect(DurableLog.DEFAULT_INLINE_PAYLOAD_LIMIT_BYTES).toBeLessThan(MAX_WORKER_FRAME_BYTES)

    const db = fixture()
    const bus = DurableLog.create({
      connection: db.connection,
      ownerID: "gen-inline-boundary",
      definitions: [Note],
      blobDir: db.blobDir,
    })
    const text = "x".repeat(INLINE_BODY_BYTES)

    bus.publish([{ definition: Note, data: { sessionID: "s1", text } }])

    const [stored] = db.connection.all("SELECT data FROM event") as ReadonlyArray<{ data: string }>
    expect(stored?.data).toContain("__eventBlob")
    expect(db.connection.all("SELECT COUNT(*) AS count FROM event_blob")).toEqual([{ count: 1 }])
    expect(fs.readdirSync(db.blobDir)).toHaveLength(1)
    expect(bus.read({ aggregateID: "s1" }).map((payload) => (payload.data as { text: string }).text)).toEqual([text])
  })
})
