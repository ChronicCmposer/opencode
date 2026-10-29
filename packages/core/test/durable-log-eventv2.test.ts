// S2: EventV2 must consume the one DurableLog durable-write implementation.
//
// The old `event-durable-writer.test.ts` asserted SQL text with regular
// expressions. It is replaced by behavioural assertions: one append produces
// exactly one event row, one sequence row, and one conditional lease renewal;
// the EventV2 layer requires `DurableLog.Service`; and `EventV2.node` declares
// that dependency instead of silently falling back to a private copy.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { randomUUID } from "node:crypto"
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
  type: "test.durable-log-eventv2.note",
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
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-durable-log-eventv2-"))
  directories.push(directory)
  const filename = path.join(directory, "eventv2.db")
  const native = new Database(filename, { create: true })
  native.exec("PRAGMA journal_mode = WAL")
  native.exec(DDL)
  natives.push(native)
  return { filename, blobDir: path.join(directory, "event-blobs"), native }
}

function connectionFrom(native: Database): DurableLog.Connection {
  return {
    filename: ":memory:",
    all: (sql, params = []) => native.query(sql).all(...(params as never[])) as Array<Record<string, unknown>>,
    run: (sql, params = []) => ({ changes: Number(native.query(sql).run(...(params as never[])).changes) }),
    exec: (sql) => native.exec(sql),
  }
}

/**
 * Wrap the SQL client so one test can observe the call stack of the durable
 * `INSERT INTO event` statement. The shared writer is the only place EventV2 is
 * allowed to issue it; a reintroduced private row writer would show a stack
 * without `durable-log.ts`/`writePreparedEvent`.
 */
function recordingClient(client: SqlClient, onEventInsert: (stack: string) => void): SqlClient {
  const unsafe = client.unsafe.bind(client)
  return new Proxy(client, {
    get(target, property) {
      if (property === "unsafe") {
        return (sql: string, params?: ReadonlyArray<unknown>) => {
          if (sql.startsWith("INSERT INTO event ")) onEventInsert(new Error("event insert").stack ?? "")
          return unsafe(sql, params)
        }
      }
      const value = Reflect.get(target, property, target)
      // Bind ordinary methods to the target so `this` is the real client; the
      // Effect SQL service tags are callable objects without `bind`, so pass
      // them through untouched.
      return typeof value === "function" && typeof value.bind === "function" ? value.bind(target) : value
    },
  })
}

function databaseLayer(
  native: Database,
  filename: string,
  onEventInsert?: (stack: string) => void,
): Layer.Layer<DatabaseService.Service> {
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
      const base = Object.assign(yield* makeSqliteClient({}, runner), { config: { filename } })
      const client = onEventInsert === undefined ? base : recordingClient(base, onEventInsert)
      const db = yield* EffectDrizzleSqlite.makeWithDefaults().pipe(Effect.provideService(SqlClient, client))
      return { db }
    }),
  ).pipe(Layer.provide(reactivityLayer))
}

function durableLogLayer(native: Database, ownerID: string): Layer.Layer<DurableLog.Service> {
  return DurableLog.layer({
    connection: connectionFrom(native),
    ownerID,
    definitions: [Note],
  })
}

afterEach(() => {
  for (const native of natives.splice(0)) native.close()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("S2 EventV2 consumes DurableLog", () => {
  test("one append writes exactly one event row and one sequence row with one lease renewal", async () => {
    const database = open()
    const ownerID = `worker-1-${process.pid}-${randomUUID()}`
    const scope = EventV2.makeWorkerScope({ sessionID: "s1", leaseOwner: ownerID })
    const services = Layer.merge(
      databaseLayer(database.native, database.filename),
      durableLogLayer(database.native, ownerID),
    )
    const eventLayer = EventV2.layerWith({ blobDir: database.blobDir }).pipe(
      Layer.provideMerge(Layer.merge(services, Layer.succeed(EventV2.WorkerScopeService, scope))),
    )

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const events = yield* EventV2.Service
          const published = yield* events.publish(Note, { sessionID: "s1", text: "small" })
          expect(published.durable?.seq).toBe(0)
        }).pipe(Effect.provide(eventLayer)),
      ),
    )

    const events = database.native.query("SELECT aggregate_id, seq, type FROM event").all()
    const sequences = database.native
      .query("SELECT seq, owner_id, worker_owner_id FROM event_sequence")
      .all() as Array<{ seq: number; owner_id: string | null; worker_owner_id: string | null }>
    expect(events).toHaveLength(1)
    expect(sequences).toHaveLength(1)
    expect(sequences[0]).toMatchObject({ seq: 0, owner_id: null, worker_owner_id: ownerID })
  })

  test("the event row is written by the shared DurableLog writer", async () => {
    const database = open()
    const ownerID = `worker-1-${process.pid}-${randomUUID()}`
    const scope = EventV2.makeWorkerScope({ sessionID: "s1", leaseOwner: ownerID })
    const insertStacks: string[] = []
    const services = Layer.merge(
      databaseLayer(database.native, database.filename, (stack) => insertStacks.push(stack)),
      durableLogLayer(database.native, ownerID),
    )
    const eventLayer = EventV2.layerWith({ blobDir: database.blobDir }).pipe(
      Layer.provideMerge(Layer.merge(services, Layer.succeed(EventV2.WorkerScopeService, scope))),
    )

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const events = yield* EventV2.Service
          yield* events.publish(Note, { sessionID: "s1", text: "small" })
        }).pipe(Effect.provide(eventLayer)),
      ),
    )

    // EventV2 issues the roster row through the shared EffectDurableRepository
    // adapter (`appendEvent` -> `execute` -> `rows` in `durable-log.ts`), not a
    // private statement: a stand-in writer would leave that file out of the
    // observed call stack.
    expect(insertStacks).toHaveLength(1)
    expect(insertStacks[0]).toContain("durable-log.ts")
    expect(insertStacks[0]).toContain("execute")
  })

  test("a missing DurableLog.Service is a type error and a layer failure", async () => {
    const database = open()
    const onlyDatabase = EventV2.layerWith({ blobDir: database.blobDir }).pipe(
      Layer.provide(databaseLayer(database.native, database.filename)),
    )
    // The EventV2 layer's requirement set includes DurableLog.Service, so a
    // caller cannot erase it to `never`.
    // @ts-expect-error EventV2 requires DurableLog.Service
    const _requiresDurableLog: Layer.Layer<EventV2.Service> = onlyDatabase

    const exit = await Effect.runPromise(
      Effect.scoped(Effect.exit(Layer.build(onlyDatabase as Layer.Layer<EventV2.Service>))),
    )
    expect(exit._tag).toBe("Failure")
    expect(String(exit)).toContain("DurableLog")
  })

  test("EventV2.node declares DurableLog.Service as a dependency", () => {
    const names = EventV2.node.dependencies.map((dependency) => dependency.name)
    expect(names).toContain(DurableLog.Service.key)
  })
})
