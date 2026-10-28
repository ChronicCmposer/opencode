// EventV2's durable commit is one whole transaction: `BEGIN IMMEDIATE`, the
// sequence/lease writes, the projectors, and the event row. When another
// connection holds the write lock, the adapter must not replay a single
// statement inside that transaction — the commit owner must replay the entire
// body with a fresh snapshot.
//
// These tests drive the real EventV2 layer over a real SQLite file and pin the
// write lock from a second native connection. The pre-fix path had no
// whole-transaction retry, so the commit died with `database is locked`.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { Database } from "bun:sqlite"
import { afterEach, describe, expect, test } from "bun:test"
import { Effect, Layer, Schema } from "effect"
import * as Client from "effect/unstable/sql/SqlClient"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { Database as DatabaseService } from "../src/database/database"
import { makeSqliteClient, type SqliteNativeRunner } from "../src/database/sqlite-adapter"
import { EventV2 } from "../src/event"

const Note = EventV2.define({
  type: "test.event-commit-busy.note",
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

function open(): { filename: string; blobDir: string } {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-event-commit-busy-"))
  directories.push(directory)
  const filename = path.join(directory, "commit.db")
  const native = new Database(filename, { create: true })
  native.exec("PRAGMA journal_mode = WAL")
  native.exec(DDL)
  natives.push(native)
  return { filename, blobDir: path.join(directory, "event-blobs") }
}

function databaseLayer(filename: string, queries: string[]): Layer.Layer<DatabaseService.Service> {
  const native = new Database(filename, { create: true })
  native.exec("PRAGMA journal_mode = WAL")
  // Busy policy belongs to the commit owner. Pinning the driver timeout to zero
  // makes the contention surface as SQLITE_BUSY instead of a native wait.
  native.exec("PRAGMA busy_timeout = 0")
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
      return { db }
    }),
  ).pipe(Layer.provide(Reactivity.layer))
}

afterEach(() => {
  for (const native of natives.splice(0)) native.close()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("EventV2 durable commit busy retry", () => {
  test("replays the whole commit when another connection holds the write lock", async () => {
    const database = open()
    const queries: string[] = []
    const holder = new Database(database.filename)
    try {
      holder.exec("PRAGMA busy_timeout = 0")
      holder.exec("BEGIN IMMEDIATE")
      holder.exec("INSERT INTO event_sequence (aggregate_id, seq) VALUES ('other', 0)")

      const release = setTimeout(() => holder.exec("COMMIT"), 150)
      try {
        const eventLayer = EventV2.layerWith({ blobDir: database.blobDir }).pipe(
          Layer.provide(databaseLayer(database.filename, queries)),
        )
        const published = await Effect.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              const events = yield* EventV2.Service
              return yield* events.publish(Note, { sessionID: "s1", text: "contended" })
            }).pipe(Effect.provide(eventLayer)),
          ),
        )

        expect(published.durable?.seq).toBe(0)
        // A busy begin is retried by replaying the transaction, never by
        // re-issuing one statement inside the failed transaction.
        expect(queries.filter((query) => query === "BEGIN IMMEDIATE").length).toBeGreaterThan(1)
        expect(holder.query("SELECT seq FROM event_sequence WHERE aggregate_id = 's1'").get()).toEqual({ seq: 0 })
      } finally {
        clearTimeout(release)
      }
    } finally {
      holder.close()
    }
  }, 30_000)
})
