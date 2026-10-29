// EventV2 must publish an oversized durable payload through the same D16
// content-addressed path the DurableLog uses:
//
// * the inline bound is the frame-headroom bound (4 MiB - 64 KiB), not the raw
//   4 MiB schema bound, so a stored-inline body always fits its egress frame;
// * a blob file staged before `BEGIN IMMEDIATE` is re-validated/re-created
//   inside the commit transaction, because a concurrent GC may have unlinked
//   it while the publisher waited for the write lock.
//
// The race is deterministic and real: the EventV2 commit begins only after a
// second SQLite connection deletes the last row referencing the same content
// hash and unlinks its file.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Database } from "bun:sqlite"
import { Effect, Layer, Schema } from "effect"
import * as Client from "effect/unstable/sql/SqlClient"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { Event } from "@opencode-ai/schema/event"
import { DurableLog } from "../src/durable-log"
import { Database as DatabaseService } from "../src/database/database"
import { makeSqliteClient, type SqliteNativeRunner } from "../src/database/sqlite-adapter"
import { EventV2 } from "../src/event"

const Note = EventV2.define({
  type: "test.event-blob-gc-race.note",
  durable: { version: 1, aggregate: "sessionID" },
  schema: {
    sessionID: Schema.String,
    text: Schema.String,
  },
})

const DDL = `
  CREATE TABLE event_sequence (
    aggregate_id text PRIMARY KEY,
    seq integer NOT NULL,
    owner_id text,
    worker_owner_id text,
    worker_heartbeat_at integer,
    worker_expires_at integer
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

const directories: string[] = []
const natives: Database[] = []

function connectionFrom(native: Database): DurableLog.Connection {
  return {
    all: (sql, params = []) => native.query(sql).all(...(params as never[])) as Array<Record<string, unknown>>,
    run: (sql, params = []) => ({ changes: Number(native.query(sql).run(...(params as never[])).changes) }),
    exec: (sql) => native.exec(sql),
  }
}

function open() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-event-blob-gc-race-"))
  directories.push(directory)
  const filename = path.join(directory, "race.db")
  const native = new Database(filename, { create: true })
  native.exec("PRAGMA journal_mode = WAL")
  native.exec(DDL)
  native.exec("PRAGMA foreign_keys = ON")
  natives.push(native)
  return {
    directory,
    filename,
    blobDir: path.join(directory, "event-blobs"),
    native,
    connection: connectionFrom(native),
  }
}

/** A real database layer over a native connection whose first BEGIN is hookable. */
function databaseLayer(filename: string, beforeBegin: () => void): Layer.Layer<DatabaseService.Service> {
  const native = new Database(filename, { create: true })
  native.exec("PRAGMA journal_mode = WAL")
  native.exec("PRAGMA foreign_keys = ON")
  natives.push(native)
  let armed = true
  const runner: SqliteNativeRunner = {
    all: (query, params, safeIntegers) => {
      if (armed && /^\s*begin\b/i.test(query)) {
        armed = false
        beforeBegin()
      }
      const statement = native.query(query)
      statement.safeIntegers(safeIntegers)
      return (statement.all(...(params as never[])) ?? []) as Array<Record<string, unknown>>
    },
    values: (query, params, safeIntegers) => {
      if (armed && /^\s*begin\b/i.test(query)) {
        armed = false
        beforeBegin()
      }
      const statement = native.query(query)
      statement.safeIntegers(safeIntegers)
      return (statement.values(...(params as never[])) ?? []) as ReadonlyArray<ReadonlyArray<unknown>>
    },
  }
  return Layer.effect(
    DatabaseService.Service,
    Effect.gen(function* () {
      const client = Object.assign(yield* makeSqliteClient({}, runner), { config: { filename } })
      const db = yield* EffectDrizzleSqlite.makeWithDefaults().pipe(Effect.provideService(Client.SqlClient, client))
      return { db }
    }),
  ).pipe(Layer.provide(Reactivity.layer))
}

const textOf = (event: Event.Payload) => (event.data as { text: string }).text

function durableLogLayer(filename: string): Layer.Layer<DurableLog.Service> {
  const native = new Database(filename, { create: true })
  natives.push(native)
  return DurableLog.layer({ connection: connectionFrom(native), ownerID: "event-v2-blob-gc-test" })
}

afterEach(() => {
  for (const native of natives.splice(0)) native.close()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("EventV2 blob GC race", () => {
  test("publish recreates a staged blob GC unlinked before the commit transaction", async () => {
    const database = open()
    const text = `race:${"x".repeat(DurableLog.DEFAULT_BLOB_LIMIT_BYTES + 1)}`

    // Seed the same content under the aggregate. GC will delete its only
    // reference and unlink the content-addressed file before EventV2 commits.
    const seed = DurableLog.create({
      connection: database.connection,
      ownerID: "seed",
      definitions: [Note],
      blobDir: database.blobDir,
      blobLimitBytes: 64,
    })
    seed.publish([{ definition: Note, data: { sessionID: "s1", text } }])
    const [file] = fs.readdirSync(database.blobDir)
    expect(file).toBeDefined()

    const gc = DurableLog.create({
      connection: connectionFrom(database.native),
      ownerID: "gc",
      definitions: [Note],
      blobDir: database.blobDir,
      blobLimitBytes: 64,
    })
    let collected = 0
    const dbLayer = databaseLayer(database.filename, () => {
      collected = gc.deleteAggregate("s1")
    })

    const eventLayer = EventV2.layerWith({ blobDir: database.blobDir }).pipe(
      Layer.provide(Layer.merge(dbLayer, durableLogLayer(database.filename))),
    )
    const published = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const events = yield* EventV2.Service
          return yield* events.publish(Note, { sessionID: "s1", text })
        }).pipe(Effect.provide(eventLayer)),
      ),
    )

    expect(collected).toBe(1)
    expect(published.durable?.seq).toBe(0)
    expect(fs.existsSync(path.join(database.blobDir, file!))).toBe(true)
    expect(seed.read({ aggregateID: "s1" }).map(textOf)).toEqual([text])
    expect(database.connection.all("SELECT COUNT(*) AS count FROM event_blob")).toEqual([{ count: 1 }])
  })

  test("the inline bound leaves headroom for the frame envelope", async () => {
    const database = open()
    // Between the frame-headroom bound and the raw 4 MiB schema bound: storing
    // this inline would make its frame unbounded, so it must be offloaded.
    const text = `aligned:${"x".repeat(DurableLog.DEFAULT_INLINE_PAYLOAD_LIMIT_BYTES + 1)}`
    expect(Buffer.byteLength(JSON.stringify({ sessionID: "s1", text }))).toBeLessThanOrEqual(
      DurableLog.DEFAULT_BLOB_LIMIT_BYTES,
    )

    const eventLayer = EventV2.layerWith({ blobDir: database.blobDir }).pipe(
      Layer.provide(
        Layer.merge(databaseLayer(database.filename, () => undefined), durableLogLayer(database.filename)),
      ),
    )
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const events = yield* EventV2.Service
          yield* events.publish(Note, { sessionID: "s1", text })
        }).pipe(Effect.provide(eventLayer)),
      ),
    )

    expect(database.connection.all("SELECT COUNT(*) AS count FROM event_blob")).toEqual([{ count: 1 }])
    expect(fs.readdirSync(database.blobDir)).toHaveLength(1)
    const reader = DurableLog.create({
      connection: database.connection,
      ownerID: "reader",
      definitions: [Note],
      blobDir: database.blobDir,
    })
    expect(reader.read({ aggregateID: "s1" }).map(textOf)).toEqual([text])
  })
})
