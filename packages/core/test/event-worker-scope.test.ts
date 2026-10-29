// Rule 11: the worker scope is a typed Context service, not an identity-keyed
// module registry. The hub graph provides no scope (explicitly scopeless); a
// worker graph provides one, and two worker databases never observe each
// other's scope even in the same process.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Database } from "bun:sqlite"
import { Effect, Layer, Schema } from "effect"
import { SqlClient } from "effect/unstable/sql/SqlClient"
import { layer as reactivityLayer } from "effect/unstable/reactivity/Reactivity"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { DurableLog } from "../src/durable-log"
import { Database as DatabaseService } from "../src/database/database"
import { makeSqliteClient, type SqliteNativeRunner } from "../src/database/sqlite-adapter"
import { EventV2 } from "../src/event"

const Note = EventV2.define({
  type: "test.event-worker-scope.note",
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
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-event-worker-scope-"))
  directories.push(directory)
  const filename = path.join(directory, "scope.db")
  const native = new Database(filename, { create: true })
  native.exec("PRAGMA journal_mode = WAL")
  native.exec(DDL)
  natives.push(native)
  return { filename, blobDir: path.join(directory, "event-blobs"), native }
}

function databaseLayer(filename: string): Layer.Layer<DatabaseService.Service> {
  const native = new Database(filename, { create: true })
  native.exec("PRAGMA journal_mode = WAL")
  natives.push(native)
  const runner: SqliteNativeRunner = {
    all: (query, params, safeIntegers) => {
      const statement = native.query(query)
      statement.safeIntegers(safeIntegers)
      return (statement.all(...(params as never[])) ?? []) as Array<Record<string, unknown>>
    },
    values: (query, params, safeIntegers) => {
      const statement = native.query(query)
      statement.safeIntegers(safeIntegers)
      return (statement.values(...(params as never[])) ?? []) as ReadonlyArray<ReadonlyArray<unknown>>
    },
  }
  return Layer.effect(
    DatabaseService.Service,
    Effect.gen(function* () {
      const client = Object.assign(yield* makeSqliteClient({}, runner), { config: { filename } })
      const db = yield* EffectDrizzleSqlite.makeWithDefaults().pipe(Effect.provideService(SqlClient, client))
      return { db }
    }),
  ).pipe(Layer.provide(reactivityLayer))
}

function durableLogLayer(filename: string): Layer.Layer<DurableLog.Service> {
  const native = new Database(filename, { create: true })
  native.exec("PRAGMA journal_mode = WAL")
  natives.push(native)
  return DurableLog.layer({
    connection: {
      all: (sql, params = []) => native.query(sql).all(...(params as never[])) as Array<Record<string, unknown>>,
      run: (sql, params = []) => ({ changes: Number(native.query(sql).run(...(params as never[])).changes) }),
      exec: (sql) => native.exec(sql),
    },
    ownerID: "event-v2-worker-scope-test",
  })
}

function workerLayer(filename: string, scope: EventV2.WorkerScope): Layer.Layer<EventV2.Service> {
  return EventV2.layerWith().pipe(
    Layer.provide(
      Layer.mergeAll(
        databaseLayer(filename),
        durableLogLayer(filename),
        Layer.succeed(EventV2.WorkerScopeService, scope),
      ),
    ),
  )
}

function publish(layer: Layer.Layer<EventV2.Service>, sessionID: string) {
  return Effect.gen(function* () {
    const events = yield* EventV2.Service
    return yield* events.publish(Note, { sessionID, text: "scoped" })
  }).pipe(Effect.provide(layer))
}

afterEach(() => {
  for (const native of natives.splice(0)) native.close()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("EventV2 worker scope service", () => {
  test("each scope owns an independent mutable lease cell", () => {
    const first = EventV2.makeWorkerScope({ sessionID: "session-a" })
    const second = EventV2.makeWorkerScope({ sessionID: "session-b" })
    const operations = { deleteAggregate: () => 1 }

    Effect.runSync(first.setLeaseOwner("owner-a"))
    first.durable = operations

    expect(first.sessionID).toBe("session-a")
    expect(second.sessionID).toBe("session-b")
    expect(Effect.runSync(first.getLeaseOwner())).toBe("owner-a")
    expect(Effect.runSync(second.getLeaseOwner())).toBeUndefined()
    expect(second.durable).toBeUndefined()

    Effect.runSync(first.clear())
    expect(first.leaseOwner).toBeUndefined()
  })

  test("the hub path has no service and stays scopeless", async () => {
    const database = open()
    const hubLayer = EventV2.layerWith({ blobDir: database.blobDir }).pipe(
      Layer.provide(Layer.merge(databaseLayer(database.filename), durableLogLayer(database.filename))),
    )

    const published = await Effect.runPromise(publish(hubLayer, "any-session"))
    expect(published.durable?.seq).toBe(0)
  })

  test("the worker path fences an aggregate outside its bound session", async () => {
    const database = open()
    const scope = EventV2.makeWorkerScope({ sessionID: "session-a" })
    const exit = await Effect.runPromise(Effect.exit(publish(workerLayer(database.filename, scope), "session-b")))

    expect(exit._tag).toBe("Failure")
    expect(String(exit)).toContain("EventV2.InvalidDurableEvent")
  })

  test("a second worker database cannot observe another worker's scope", async () => {
    const firstDatabase = open()
    const secondDatabase = open()
    const firstLayer = workerLayer(firstDatabase.filename, EventV2.makeWorkerScope({ sessionID: "session-a" }))
    const secondLayer = workerLayer(secondDatabase.filename, EventV2.makeWorkerScope({ sessionID: "session-b" }))

    // The first worker's scope cannot authorize a write to the second session...
    const foreign = await Effect.runPromise(Effect.exit(publish(firstLayer, "session-b")))
    expect(foreign._tag).toBe("Failure")
    expect(String(foreign)).toContain("EventV2.InvalidDurableEvent")

    // ...while the second worker's own scope commits normally.
    const own = await Effect.runPromise(publish(secondLayer, "session-b"))
    expect(own.durable?.seq).toBe(0)
    // A worker generation is never mirrored into the v1 sync owner column.
    expect(secondDatabase.native.query("SELECT owner_id FROM event_sequence").all()).toEqual([{ owner_id: null }])
  })
})
