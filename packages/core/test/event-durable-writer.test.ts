// EventV2 commits through the Bus sequence/lease primitives. The observable
// contract is the SQL the production layer issues against a real SQLite file:
//
// * a new durable append reserves its position with the Bus block-reservation
//   statement (`... RETURNING seq`), so there is one sequence writer;
// * a bound worker's commit renews the lease with the same conditional update
//   the Bus publisher uses (`worker_owner_id IS NULL OR worker_owner_id = ?`)
//   and fails when it does not own the row.
//
// Before the unification EventV2 used a read-then-UPSERT and an unconditional
// lease claim, which could not report a lost renewal.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { randomUUID } from "node:crypto"
import { afterEach, describe, expect, test } from "bun:test"
import { Database } from "bun:sqlite"
import { Effect, Layer, Schema } from "effect"
import * as Client from "effect/unstable/sql/SqlClient"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { Database as DatabaseService } from "../src/database/database"
import { makeSqliteClient, type SqliteNativeRunner } from "../src/database/sqlite-adapter"
import { EventV2 } from "../src/event"

const Note = EventV2.define({
  type: "test.event-durable-writer.note",
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

function open() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-event-durable-writer-"))
  directories.push(directory)
  const filename = path.join(directory, "writer.db")
  const native = new Database(filename, { create: true })
  native.exec("PRAGMA journal_mode = WAL")
  native.exec(DDL)
  natives.push(native)
  return { filename, blobDir: path.join(directory, "event-blobs") }
}

function recordingDatabaseLayer(
  filename: string,
  ownerID: string,
  queries: string[],
): Layer.Layer<DatabaseService.Service> {
  const native = new Database(filename, { create: true })
  native.exec("PRAGMA journal_mode = WAL")
  natives.push(native)
  const runner: SqliteNativeRunner = {
    all: (query, params, safeIntegers) => {
      queries.push(query)
      const statement = native.query(query)
      statement.safeIntegers(safeIntegers)
      return (statement.all(...(params as never[])) ?? []) as Array<Record<string, unknown>>
    },
    values: (query, params, safeIntegers) => {
      queries.push(query)
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
      EventV2.bindWorkerScope(db, { sessionID: "s1", leaseOwner: ownerID })
      return { db }
    }),
  ).pipe(Layer.provide(Reactivity.layer))
}

afterEach(() => {
  for (const native of natives.splice(0)) native.close()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("EventV2 durable writer", () => {
  test("a bound worker append reserves through Bus and renews its lease conditionally", async () => {
    const database = open()
    const ownerID = `worker-1-${process.pid}-${randomUUID()}`
    const queries: string[] = []
    const eventLayer = EventV2.layerWith({ blobDir: database.blobDir }).pipe(
      Layer.provide(recordingDatabaseLayer(database.filename, ownerID, queries)),
    )

    const published = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const events = yield* EventV2.Service
          return yield* events.publish(Note, { sessionID: "s1", text: "small" })
        }).pipe(Effect.provide(eventLayer)),
      ),
    )
    expect(published.durable?.seq).toBe(0)

    // Bus block reservation, not EventV2's former read-then-UPSERT.
    expect(queries.some((query) => /returning\s+seq/i.test(query))).toBe(true)
    // Bus conditional lease renewal, not EventV2's former unconditional claim.
    expect(queries.some((query) => /worker_owner_id\s+is\s+null\s+or\s+worker_owner_id\s*=\s*\?/i.test(query))).toBe(
      true,
    )

    const admin = new Database(database.filename)
    try {
      expect(admin.query("SELECT worker_owner_id, worker_expires_at FROM event_sequence").get()).toMatchObject({
        worker_owner_id: ownerID,
      })
    } finally {
      admin.close()
    }
  })
})
