// Hub SSE healing after a durable gap (issue 14).
//
// A worker can skip a durable sequence over the events port (overflow, a lost
// message, a crash boundary). The hub must not merely count the resync: it must
// read the missed committed events from SQLite and republish them to the same
// live surface the `/event` SSE stream listens on, in order, without blocking
// the hub loop. The real `EventV2.listen` seam below is what the SSE handler
// consumes.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Effect, Layer, ManagedRuntime } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { openConnection } from "@opencode-ai/core/database/connection"
import { EventV2 } from "@opencode-ai/core/event"
import { createLiveEventPublisher } from "@/event-v2-bridge"
import { encodeFrame } from "@/session/egress/frame"
import { buildProductionPortBackends, createDurableHealReader } from "@/session/worker/hub"
import { eventFrame } from "@/session/worker/frame"
import type { SessionID } from "@/session/schema"
import { pollUntil } from "../lib/poll"

const AGGREGATE = "ses_sse_gap"
const SESSION = AGGREGATE as SessionID

const cleanups: Array<() => void> = []

afterEach(() => {
  while (cleanups.length > 0) cleanups.pop()?.()
})

/** A real database file holding the committed aggregate log the hub must heal from. */
function eventLog(): {
  readonly query: (sql: string, params: ReadonlyArray<unknown>) => Promise<ReadonlyArray<Record<string, unknown>>>
} {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-hub-sse-gap-"))
  const connection = openConnection({ filename: path.join(directory, "opencode.db") })
  cleanups.push(() => {
    connection.close()
    fs.rmSync(directory, { recursive: true, force: true })
  })
  connection.exec(`
    CREATE TABLE event (
      id text PRIMARY KEY,
      aggregate_id text NOT NULL,
      seq integer NOT NULL,
      type text NOT NULL,
      data text NOT NULL
    )
  `)
  for (const seq of [0, 1, 2]) {
    connection.run("INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?, ?, ?, ?, ?)", [
      `evt_${seq}`,
      AGGREGATE,
      seq,
      "session.updated",
      JSON.stringify({ sessionID: AGGREGATE, title: `title ${seq}` }),
    ])
  }
  return { query: async (sql, params) => connection.all<Record<string, unknown>>(sql, params) }
}

/** The real EventV2 subscriber seam the SSE handler uses, over a DB-free live path. */
function sseRuntime() {
  const databaseStub = Layer.succeed(Database.Service, { db: {} as never })
  return ManagedRuntime.make(EventV2.layerWith().pipe(Layer.provide(databaseStub)))
}

describe("hub SSE gap healing", () => {
  test("a dropped durable frame is read from SQLite and republished to the SSE consumer", async () => {
    const log = eventLog()
    const runtime = sseRuntime()
    cleanups.push(() => void runtime.dispose())
    await runtime.runPromise(Effect.void)
    const events = runtime.runSync(EventV2.Service)
    const seen: EventV2.Payload[] = []
    runtime.runSync(events.listen((event) => Effect.sync(() => void seen.push(event))))

    const channel = new MessageChannel()
    const backends = buildProductionPortBackends({
      events: createLiveEventPublisher(events),
      sessionOfWorker: () => SESSION,
      // The production layer passes an async reader over the hub's off-loop
      // executor; this test reads the same real SQLite file through that reader.
      readDurable: createDurableHealReader({
        query: (sql, params) => log.query(sql, params),
        pageSize: 1,
      }),
    })
    const dispose = backends.egress!(1, channel.port1)
    cleanups.push(() => {
      dispose()
      channel.port1.close()
      channel.port2.close()
      void backends.dispose()
    })

    const frame = (seq: number) =>
      encodeFrame(
        eventFrame({
          event: { id: `evt_${seq}`, type: "session.updated", properties: { sessionID: AGGREGATE, title: `live ${seq}` } },
          aggregateID: AGGREGATE,
          seq,
        }),
      )

    // seq 1 is dropped in transit. The hub sees the gap at seq 2 and must heal
    // seq 1 from the committed log before delivering seq 2.
    channel.port2.postMessage(frame(0))
    channel.port2.postMessage(frame(2))

    await pollUntil(() => seen.length === 3, {
      message: "the missed durable event was counted but never republished to SSE",
    })

    expect(seen.map((event) => String(event.id))).toEqual(["evt_0", "evt_1", "evt_2"])
    expect(seen.map((event) => event.durable?.seq)).toEqual([0, 1, 2])
    expect(seen.every((event) => event.durable?.aggregateID === AGGREGATE)).toBe(true)
    // The healed body comes from the committed row, not from a wire placeholder.
    expect(seen[1]?.data).toEqual({ sessionID: AGGREGATE, title: "title 1" })
  })
})
