// F17: a schema without the dedicated `worker_owner_id` column cannot carry a
// worker lease. It must fail with a typed error before any write, and a worker
// generation must never be repurposed into the v1 sync `owner_id` column.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Database } from "bun:sqlite"
import { Effect, Layer, Option, Schema } from "effect"
import { SqlClient } from "effect/unstable/sql/SqlClient"
import { layer as reactivityLayer } from "effect/unstable/reactivity/Reactivity"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { DurableLog } from "../src/durable-log"
import { Database as DatabaseService } from "../src/database/database"
import { makeSqliteClient, type SqliteNativeRunner } from "../src/database/sqlite-adapter"
import { EventV2 } from "../src/event"

const Note = EventV2.define({
  type: "test.worker-scope-f17.note",
  durable: { version: 1, aggregate: "sessionID" },
  schema: {
    sessionID: Schema.String,
    text: Schema.String,
  },
})

/** A pre-`add_worker_lease` journal: only the v1 sync owner column exists. */
const LEGACY_DDL = `
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
`

const directories: string[] = []
const natives: Database[] = []

function open() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-worker-scope-f17-"))
  directories.push(directory)
  const filename = path.join(directory, "legacy.db")
  const native = new Database(filename, { create: true })
  native.exec("PRAGMA journal_mode = WAL")
  native.exec(LEGACY_DDL)
  natives.push(native)
  return { filename, native }
}

function connectionFrom(native: Database): DurableLog.Connection {
  return {
    all: (sql, params = []) => native.query(sql).all(...(params as never[])) as Array<Record<string, unknown>>,
    run: (sql, params = []) => ({ changes: Number(native.query(sql).run(...(params as never[])).changes) }),
    exec: (sql) => native.exec(sql),
  }
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
  return DurableLog.layer({ connection: connectionFrom(native), ownerID: "event-v2-worker-scope-test" })
}

afterEach(() => {
  for (const native of natives.splice(0)) native.close()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("F17 legacy worker claim refusal", () => {
  test("an EventV2 worker commit fails typed and never writes owner_id", async () => {
    const database = open()
    const ownerID = "worker-legacy-generation"
    const scope = EventV2.makeWorkerScope({ sessionID: "s1", leaseOwner: ownerID })
    const eventLayer = EventV2.layerWith().pipe(
      Layer.provide(
        Layer.mergeAll(
          databaseLayer(database.filename),
          durableLogLayer(database.filename),
          Layer.succeed(EventV2.WorkerScopeService, scope),
        ),
      ),
    )

    const exit = await Effect.runPromise(
      Effect.exit(
        Effect.gen(function* () {
          const events = yield* EventV2.Service
          return yield* events.publish(Note, { sessionID: "s1", text: "legacy" })
        }).pipe(Effect.provide(eventLayer)),
      ),
    )

    expect(exit._tag).toBe("Failure")
    expect(String(exit)).toContain("DurableLog.WorkerLeaseUnsupported")
    expect(database.native.query("SELECT owner_id FROM event_sequence").all()).toEqual([])
    expect(database.native.query("SELECT COUNT(*) AS count FROM event").get()).toEqual({ count: 0 })
  })

  test("the DurableLog worker claim fails typed instead of writing the sync owner", () => {
    const database = open()
    const bus = DurableLog.create({ connection: connectionFrom(database.native), ownerID: "worker-legacy-generation" })

    expect(() => bus.claim("s1")).toThrow(DurableLog.WorkerLeaseUnsupportedError)
    expect(database.native.query("SELECT owner_id FROM event_sequence").all()).toEqual([])
  })
})

describe("worker scope presence", () => {
  test("the hub layer has no WorkerScopeService; a worker layer still does", async () => {
    const database = open()
    const hubLayer = EventV2.layerWith().pipe(
      Layer.provide(Layer.merge(databaseLayer(database.filename), durableLogLayer(database.filename))),
    )

    const hub = await Effect.runPromise(
      Effect.gen(function* () {
        const scope = yield* Effect.serviceOption(EventV2.WorkerScopeService)
        const events = yield* EventV2.Service
        const published = yield* events.publish(Note, { sessionID: "s1", text: "hub" })
        return { scope, published }
      }).pipe(Effect.provide(hubLayer)),
    )
    expect(Option.isNone(hub.scope)).toBe(true)
    expect(hub.published.durable?.seq).toBe(0)

    const workerScopeService = EventV2.makeWorkerScope({ sessionID: "s1" })
    const workerContext = Layer.merge(
      EventV2.layerWith().pipe(
        Layer.provide(
          Layer.mergeAll(
            databaseLayer(database.filename),
            durableLogLayer(database.filename),
            Layer.succeed(EventV2.WorkerScopeService, workerScopeService),
          ),
        ),
      ),
      Layer.succeed(EventV2.WorkerScopeService, workerScopeService),
    )
    const workerScope = await Effect.runPromise(
      Effect.serviceOption(EventV2.WorkerScopeService).pipe(Effect.provide(workerContext)),
    )
    expect(Option.isSome(workerScope)).toBe(true)
  })
})
