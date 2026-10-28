// Durable/resync integration: a mid-run rename (`Session.setTitle`) advances
// the SAME session aggregate. If the worker's durable stream observes the rename
// with a sequence gap, the session must resync from its database snapshot and
// keep streaming — never close the events port or wedge the run.
//
// This drives the real production pieces: the events port (`serveEventsPort` +
// `createHubEventSink`), the worker egress client over a real `MessagePort`, the
// durable mesh, the worker wakeup bridge (`wireDurableWakeups`), and the delta
// resync machinery (`createDeltaSubscription` + `createEventLogSnapshotReader`)
// against a real migrated SQLite database.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Effect } from "effect"
import type { EventV2 } from "@opencode-ai/core/event"
import { openConnection } from "@opencode-ai/core/database/connection"
import type { LiveEvent, LiveEventPublisher } from "@/event-v2-bridge"
import {
  createDeltaSubscription,
  createEventLogSnapshotReader,
  type DeltaResync,
  type DeltaSnapshot,
  type DurableDelivery,
} from "@/session/egress/delta"
import { encodeFrame } from "@/session/egress/frame"
import { attachMeshWorker, createEgressMesh } from "@/session/egress/mesh"
import { createHubEventSink, serveEventsPort } from "@/session/worker/hub"
import { createWorkerEgressClient } from "@/session/worker/egress-client"
import { eventFrame } from "@/session/worker/frame"
import { wireDurableWakeups } from "@/session/worker/layer"
import { migrationStatements } from "@/session/writer/migration.statements.gen"
import { pollUntil } from "../lib/poll"

const AGGREGATE = "ses_title"
const directories: string[] = []

function migratedDatabase(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-durable-resync-"))
  directories.push(directory)
  const filename = path.join(directory, "opencode.db")
  const connection = openConnection({ filename })
  try {
    for (const statement of migrationStatements) connection.exec(statement)
    connection.run(
      "INSERT INTO event_sequence (aggregate_id, seq) VALUES (?, ?)",
      [AGGREGATE, 2],
    )
    for (const [seq, title] of [
      [0, "initial title"],
      [1, "first rename"],
      [2, "mid-run rename"],
    ] as const) {
      connection.run(
        "INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?, ?, ?, ?, ?)",
        [`evt_${seq}`, AGGREGATE, seq, "session.updated", JSON.stringify({ sessionID: AGGREGATE, title })],
      )
    }
  } finally {
    connection.close()
  }
  return filename
}

afterEach(() => {
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

/** Sequences are 0-based: the first durable event is seq 0, not seq 1. */
function migratedDatabaseFromZero(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-durable-resync-zero-"))
  directories.push(directory)
  const filename = path.join(directory, "opencode.db")
  const connection = openConnection({ filename })
  try {
    for (const statement of migrationStatements) connection.exec(statement)
    connection.run("INSERT INTO event_sequence (aggregate_id, seq) VALUES (?, ?)", [AGGREGATE, 2])
    for (const [seq, title] of [
      [0, "zero title"],
      [1, "first rename"],
      [2, "second rename"],
    ] as const) {
      connection.run(
        "INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?, ?, ?, ?, ?)",
        [`evt_${seq}`, AGGREGATE, seq, "session.updated", JSON.stringify({ sessionID: AGGREGATE, title })],
      )
    }
  } finally {
    connection.close()
  }
  return filename
}

/** The durable event a v1 `session.updated` (setTitle) publishes. */
const rename = (seq: number): DurableDelivery => ({
  aggregateID: AGGREGATE,
  seq,
  event: {
    id: `evt_${seq}`,
    type: "session.updated",
    properties: { sessionID: AGGREGATE, title: `rename ${seq}` },
  },
})

describe("mid-run rename resyncs from the DB snapshot", () => {
  test("a gap between two setTitle commits emits Resync + snapshot and keeps streaming", async () => {
    const filename = migratedDatabase()
    const connection = openConnection({ filename })
    const resyncs: DeltaResync[] = []
    const snapshots: DeltaSnapshot[] = []
    const delivered: string[] = []
    try {
      const snapshot = createEventLogSnapshotReader(async (_method, sql, params) => {
        return connection.all(sql, params)
      })
      const subscription = createDeltaSubscription({
        aggregateID: AGGREGATE,
        snapshot,
        onDelta: () => {},
        onDurable: (durable) => delivered.push(durable.event.id),
        onResync: (resync) => resyncs.push(resync),
        onSnapshot: (snap) => snapshots.push(snap),
      })

      // The initial title landed, then the rename advanced the same aggregate.
      // The intermediate commit is missing on this stream.
      subscription.receiveDurable(rename(0))
      subscription.receiveDurable(rename(2))
      await pollUntil(() => snapshots.length === 1, { message: "gap never produced a DB snapshot" })

      // Heal at `seq - 1` so the skipped commit (seq 1) is part of the snapshot.
      expect(resyncs).toEqual([{ type: "resync", aggregateID: AGGREGATE, seq: 1 }])
      expect(snapshots[0]?.seq).toBe(1)
      expect(snapshots[0]?.events.map((event) => event.id)).toEqual(["evt_0", "evt_1"])

      // The stream is not closed: the next rename still streams, and the
      // already-delivered durable 2 is not snapshotted (no duplicate delivery).
      subscription.receiveDurable(rename(3))
      expect(delivered).toEqual(["evt_0", "evt_2", "evt_3"])
      expect(subscription.healing).toBe(0)
    } finally {
      connection.close()
    }
  })

  test("a first durable above zero seeds the cursor without a full-log read", async () => {
    const filename = migratedDatabaseFromZero()
    const connection = openConnection({ filename })
    const resyncs: DeltaResync[] = []
    const snapshots: DeltaSnapshot[] = []
    const delivered: string[] = []
    try {
      const snapshot = createEventLogSnapshotReader(async (_method, sql, params) => {
        return connection.all(sql, params)
      })
      const subscription = createDeltaSubscription({
        aggregateID: AGGREGATE,
        snapshot,
        onDelta: () => {},
        onDurable: (durable) => delivered.push(durable.event.id),
        onResync: (resync) => resyncs.push(resync),
        onSnapshot: (snap) => snapshots.push(snap),
      })

      // A subscription attached mid-stream observes seq 1 first. That seeds the
      // cursor: an existing aggregate may first be seen at any sequence, and its
      // earlier history is not a gap worth a full-log read or a resync.
      subscription.receiveDurable(rename(1))
      expect(subscription.healing).toBe(0)
      expect(resyncs).toEqual([])
      expect(snapshots).toEqual([])
      expect(subscription.lastDurableSeq).toBe(1)
      expect(delivered).toEqual(["evt_1"])

      // A real gap after the seed still heals at `seq - 1`, so the skipped
      // commit is in the snapshot and the gapped durable is delivered once.
      subscription.receiveDurable(rename(3))
      await pollUntil(() => snapshots.length === 1, { message: "seeded gap never snapshotted" })

      expect(resyncs).toEqual([{ type: "resync", aggregateID: AGGREGATE, seq: 2 }])
      expect(snapshots[0]?.seq).toBe(2)
      expect(snapshots[0]?.events.map((event) => event.id)).toEqual(["evt_0", "evt_1", "evt_2"])
      await pollUntil(() => delivered.length === 2, { message: "gapped durable was not delivered" })
      expect(delivered).toEqual(["evt_1", "evt_3"])
      expect(subscription.healing).toBe(0)
    } finally {
      connection.close()
    }
  })
})

describe("hub events port → worker resync over the real ports", () => {
  test("a receiver gap tells the worker to wake its aggregate and streaming continues", async () => {
    const published: LiveEvent[] = []
    const publisher: LiveEventPublisher = {
      publish: (event) => void published.push(event),
      publishDefinition: (definition, data) => void published.push({ type: definition.type, data }),
    }
    const data = new MessageChannel()
    const control = new MessageChannel()
    const mesh = createEgressMesh()
    const attachment = attachMeshWorker(mesh, "w1", control.port2)
    const client = createWorkerEgressClient(data.port1, { controlPort: control.port1 })
    const woken: string[] = []
    const republished: string[] = []
    // The production `EventV2.durable` re-read is what heals the wake: a wake for
    // the aggregate makes the isolate's durable subscribers re-read SQLite. This
    // stub records that exact seam.
    const events = {
      publishLive: (event: EventV2.Payload) => Effect.sync(() => void republished.push(event.type)),
      wakeDurable: (aggregateID: string) => Effect.sync(() => void woken.push(aggregateID)),
    }
    const disposeWakeups = wireDurableWakeups(events, client)
    const sink = createHubEventSink({ publisher })
    const disposeHub = serveEventsPort(data.port2, sink, undefined, (commit) => void mesh.publish(commit))
    try {
      await pollUntil(() => mesh.subscriptions().length === 1, { message: "worker never subscribed" })

      const frame = (seq: number) =>
        encodeFrame(
          eventFrame({
            event: { id: `evt_${seq}`, type: "session.updated", properties: { sessionID: AGGREGATE } },
            aggregateID: AGGREGATE,
            seq,
          }),
        )

      data.port1.postMessage(frame(1))
      // The rename advanced the same aggregate but the intermediate commit was
      // lost: the hub must resync, not close the port.
      data.port1.postMessage(frame(3))
      await pollUntil(() => sink.resyncs === 1, { message: "hub never saw the receiver gap" })
      await pollUntil(() => woken.includes(AGGREGATE), { message: "worker never woke the aggregate to resync" })

      // Streaming continues after the resync.
      data.port1.postMessage(frame(4))
      await pollUntil(() => published.length === 3, { message: "events stream closed after the gap" })
      expect(sink.resyncs).toBe(1)
      expect(republished).toHaveLength(3)
    } finally {
      disposeHub()
      disposeWakeups()
      client.dispose()
      attachment.dispose()
      data.port1.close()
      data.port2.close()
      control.port1.close()
      control.port2.close()
    }
  })
})
