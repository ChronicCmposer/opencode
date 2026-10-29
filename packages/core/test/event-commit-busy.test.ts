// EventV2's durable commit is one whole transaction: `BEGIN IMMEDIATE`, the
// sequence/lease writes, the projectors, and the event row. When another
// connection holds the write lock, the SQLite adapter retries the `BEGIN
// IMMEDIATE` at the transaction boundary and never replays a single statement
// inside the body (which would keep the stale snapshot).
//
// These tests drive the real EventV2 layer over a real SQLite file and pin the
// write lock from a second native connection. The pre-fix path had no
// transaction-boundary retry, so the commit died with `database is locked`.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { Database } from "bun:sqlite"
import { afterEach, describe, expect, test } from "bun:test"
import { Effect, Layer, Schema } from "effect"
import * as Client from "effect/unstable/sql/SqlClient"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { DurableLog } from "../src/durable-log"
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

function durableLogLayer(filename: string): Layer.Layer<DurableLog.Service> {
  const native = new Database(filename, { create: true })
  natives.push(native)
  return DurableLog.layer({
    connection: {
      all: (sql, params = []) => native.query(sql).all(...(params as never[])) as Array<Record<string, unknown>>,
      run: (sql, params = []) => ({ changes: Number(native.query(sql).run(...(params as never[])).changes) }),
      exec: (sql) => native.exec(sql),
    },
    ownerID: "event-v2-commit-busy-test",
  })
}

afterEach(() => {
  for (const native of natives.splice(0)) native.close()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("EventV2 durable commit busy retry", () => {
  test("retries the busy BEGIN at the boundary when another connection holds the write lock", async () => {
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
          Layer.provide(Layer.merge(databaseLayer(database.filename, queries), durableLogLayer(database.filename))),
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
        // The adapter re-issues `BEGIN IMMEDIATE` at the transaction boundary;
        // the body itself is never replayed inside the failed transaction.
        expect(queries.filter((query) => query === "BEGIN IMMEDIATE").length).toBeGreaterThan(1)
        expect(holder.query("SELECT seq FROM event_sequence WHERE aggregate_id = 's1'").get()).toEqual({ seq: 0 })
        // The body itself ran exactly once: a replay would insert a second row.
        expect(holder.query("SELECT COUNT(*) AS count FROM event WHERE aggregate_id = 's1'").get()).toEqual({
          count: 1,
        })
      } finally {
        clearTimeout(release)
      }
    } finally {
      holder.close()
    }
  }, 30_000)
})
