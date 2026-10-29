// The hub half of frame-aligned durable events: an oversized durable body
// crosses the worker→hub events port as a committed POSITION reference
// (`{ __eventBlobPosition, frameBytes }`). The hub must recover the body from the
// shared D16 blob before the live publisher (or the cross-isolate mesh) sees it,
// and a missing/corrupt row must surface a typed resync — never a drop and never
// the raw reference.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { DurableLog } from "@opencode-ai/core/durable-log"
import { openConnection } from "@opencode-ai/core/database/connection"
import { encodeFrame, type DurableResyncPayload } from "@/session/egress/frame"
import type { LiveEvent, LiveEventPublisher } from "@/event-v2-bridge"
import {
  buildProductionPortBackends,
  createEventBlobResolver,
  createHubEventSink,
  serveEventsPort,
  type HubEventSink,
} from "@/session/worker/hub"
import {
  blobPositionReferenceFrame,
  eventFrame,
  MAX_WORKER_FRAME_BYTES,
  type EventV2Event,
} from "@/session/worker/frame"
import { pollUntil } from "../lib/poll"

const MiB = 1024 * 1024
const AGGREGATE = "ses_blob"
const SEQ = 1

const cleanups: Array<() => void> = []

afterEach(() => {
  while (cleanups.length > 0) cleanups.pop()?.()
})

function recordingPublisher(): { publisher: LiveEventPublisher; published: LiveEvent[] } {
  const published: LiveEvent[] = []
  const publisher: LiveEventPublisher = {
    publish: (event) => void published.push(event),
    publishDefinition: (definition, data) => void published.push({ type: definition.type, data }),
  }
  return { publisher, published }
}

interface BlobFixture {
  readonly big: EventV2Event
  readonly stored: string
  readonly query: (sql: string, params: ReadonlyArray<unknown>) => Promise<ReadonlyArray<Record<string, unknown>>>
  readonly blobDir: string
  readonly connection: ReturnType<typeof openConnection>
}

/**
 * A real database file with the D16 `event_blob` row and content-addressed file
 * the worker commits before egress. The hub only shares the file; it never
 * re-commits the event.
 */
function blobFixture(): BlobFixture {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-hub-blob-"))
  const filename = path.join(directory, "opencode.db")
  const connection = openConnection({ filename })
  cleanups.push(() => {
    connection.close()
    fs.rmSync(directory, { recursive: true, force: true })
  })
  connection.exec(
    "CREATE TABLE event_blob (aggregate_id text NOT NULL, seq integer NOT NULL, type text NOT NULL, hash text NOT NULL, size integer NOT NULL, data text, file_path text)",
  )
  const big: EventV2Event = {
    id: "evt_big",
    type: "session.next.prompted",
    properties: { sessionID: AGGREGATE, text: "x".repeat(5 * MiB) },
  }
  const stored = JSON.stringify(big.properties)
  const blobDir = path.join(directory, "event-blobs")
  const hash = DurableLog.digest(stored)
  DurableLog.writeBlobFile(blobDir, hash, stored)
  connection.run(
    "INSERT INTO event_blob (aggregate_id, seq, type, hash, size, data, file_path) VALUES (?, ?, ?, ?, ?, NULL, ?)",
    [AGGREGATE, SEQ, big.type, hash, Buffer.byteLength(stored), hash],
  )
  const query = async (sql: string, params: ReadonlyArray<unknown>) => connection.all<Record<string, unknown>>(sql, params)
  return { big, stored, query, blobDir, connection }
}

function referenceFrame(big: EventV2Event) {
  const frame = blobPositionReferenceFrame(
    eventFrame({ event: big, aggregateID: AGGREGATE, seq: SEQ }),
    MAX_WORKER_FRAME_BYTES + 1,
  )
  if (frame === undefined) throw new Error("oversized frame did not produce a position reference")
  return encodeFrame(frame)
}

describe("hub resolves an oversized durable body before republish", () => {
  test("the production events port republishes the resolved 5 MiB body", async () => {
    const fixture = blobFixture()
    const { publisher, published } = recordingPublisher()
    const backends = buildProductionPortBackends({
      events: publisher,
      resolveBlob: createEventBlobResolver({ query: fixture.query, blobDir: fixture.blobDir }),
    })
    const channel = new MessageChannel()
    const dispose = backends.egress!(1, channel.port1)
    try {
      channel.port2.postMessage(referenceFrame(fixture.big))
      await pollUntil(() => published.length === 1, { message: "the oversized durable event was never republished" })

      expect(published[0]?.id).toBe("evt_big")
      expect(published[0]?.type).toBe(fixture.big.type)
      expect(published[0]?.data).toEqual(JSON.parse(fixture.stored))
      // The live surface sees the body, never the wire reference.
      expect(published[0]?.data).not.toHaveProperty("__eventBlobPosition")
    } finally {
      dispose()
      channel.port1.close()
      channel.port2.close()
      await backends.dispose()
    }
  })

  test("a missing blob row surfaces a typed resync, never a drop or raw reference", async () => {
    const fixture = blobFixture()
    fixture.connection.run("DELETE FROM event_blob")
    const { publisher, published } = recordingPublisher()
    const base = createHubEventSink({
      publisher,
      resolveBlob: createEventBlobResolver({ query: fixture.query, blobDir: fixture.blobDir }),
    })
    const resyncs: DurableResyncPayload[] = []
    const sink: HubEventSink = {
      ...(base.resolveBlob === undefined ? {} : { resolveBlob: base.resolveBlob }),
      event: (event, meta) => base.event(event, meta),
      end: () => base.end(),
      resync: (payload) => {
        resyncs.push(payload ?? {})
        base.resync(payload)
      },
      ack: (payload) => base.ack(payload),
      get ended() {
        return base.ended
      },
      get resyncs() {
        return base.resyncs
      },
      get acks() {
        return base.acks
      },
    }
    const channel = new MessageChannel()
    const dispose = serveEventsPort(channel.port1, sink)
    try {
      channel.port2.postMessage(referenceFrame(fixture.big))
      await pollUntil(() => resyncs.length === 1, { message: "a missing blob row was not surfaced as a resync" })

      expect(resyncs[0]).toEqual({ aggregateID: AGGREGATE, seq: SEQ })
      expect(sink.resyncs).toBe(1)
      expect(published).toEqual([])
    } finally {
      dispose()
      channel.port1.close()
      channel.port2.close()
    }
  })

  test("a corrupt blob row is healed by a typed resync instead of publishing garbage", async () => {
    const fixture = blobFixture()
    // The row's filename is its content hash; tamper the bytes so the DurableLog's
    // re-hash fails before any payload is trusted.
    const [row] = fixture.connection.all<{ hash: string }>("SELECT hash FROM event_blob")
    fs.writeFileSync(path.join(fixture.blobDir, row!.hash), "tampered")
    const { publisher, published } = recordingPublisher()
    const base = createHubEventSink({
      publisher,
      resolveBlob: createEventBlobResolver({ query: fixture.query, blobDir: fixture.blobDir }),
    })
    const channel = new MessageChannel()
    const dispose = serveEventsPort(channel.port1, base)
    try {
      channel.port2.postMessage(referenceFrame(fixture.big))
      await pollUntil(() => base.resyncs === 1, { message: "a corrupt blob row was not surfaced as a resync" })

      expect(published).toEqual([])
    } finally {
      dispose()
      channel.port1.close()
      channel.port2.close()
    }
  })
})
