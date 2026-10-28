// The worker-backed SQL client acquires its single connection through an
// asynchronous effect. `EventV2.layerWith` therefore must not probe the schema
// or load blob rows with `Effect.runSync`: construction and reads have to yield.
//
// This test uses a real SQLite client and holds its only connection while each
// EventV2 operation starts, exactly like a worker boot where another layer is
// still using the database. The layer must wait for the connection, offload an
// oversized durable payload to D16, and read it back through the durable stream.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Database } from "bun:sqlite"
import { Context, Deferred, Effect, Fiber, Layer, Schema, Scope } from "effect"
import * as Client from "effect/unstable/sql/SqlClient"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { Event } from "@opencode-ai/schema/event"
import { Bus } from "../src/bus"
import { Database as DatabaseService } from "../src/database/database"
import { makeSqliteClient, type SqliteNativeRunner } from "../src/database/sqlite-adapter"
import { EventV2 } from "../src/event"

const Note = EventV2.define({
  type: "test.event-async-client.note",
  durable: { version: 1, aggregate: "sessionID" },
  schema: {
    sessionID: Schema.String,
    text: Schema.String,
  },
})

const Manifest = { definitions: Event.durable([Note]), schema: Schema.Any }

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
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-event-async-client-"))
  directories.push(directory)
  const filename = path.join(directory, "async.db")
  const native = new Database(filename, { create: true })
  native.exec("PRAGMA journal_mode = WAL")
  native.exec(DDL)
  natives.push(native)
  return { filename, blobDir: path.join(directory, "event-blobs"), native }
}

function runnerFrom(native: Database): SqliteNativeRunner {
  return {
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
}

function databaseLayer(filename: string, client: Client.SqlClient): Layer.Layer<DatabaseService.Service> {
  return Layer.effect(
    DatabaseService.Service,
    Effect.gen(function* () {
      const db = yield* EffectDrizzleSqlite.makeWithDefaults().pipe(Effect.provideService(Client.SqlClient, client))
      return { db }
    }),
  ).pipe(Layer.provide(Reactivity.layer))
}

/**
 * Run `effect` while another fiber holds the client's only connection, releasing
 * it a scheduler tick later. This is the worker client's acquisition shape: a
 * statement that does not yield would die with `AsyncFiberError`.
 */
function duringAsyncAcquisition<A, E, R>(
  client: Client.SqlClient,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R | Scope.Scope> {
  return Effect.gen(function* () {
    const acquired = yield* Deferred.make<void>()
    const holder = yield* Effect.forkScoped(
      Effect.scoped(
        Effect.gen(function* () {
          yield* client.reserve
          yield* Deferred.succeed(acquired, undefined)
          return yield* Effect.never
        }),
      ),
    )
    yield* Deferred.await(acquired)
    const release = yield* Effect.forkScoped(Effect.sleep("5 millis").pipe(Effect.andThen(Fiber.interrupt(holder))))
    return yield* effect.pipe(Effect.ensuring(Effect.andThen(Fiber.interrupt(release), Fiber.interrupt(holder))))
  })
}

afterEach(() => {
  for (const native of natives.splice(0)) native.close()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("EventV2 async acquisition client", () => {
  test("constructs the layer and offloads/reads an oversized durable payload", async () => {
    const database = open()
    const text = `async:${"x".repeat(Bus.DEFAULT_INLINE_PAYLOAD_LIMIT_BYTES)}`
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const client = yield* makeSqliteClient({}, runnerFrom(database.native)).pipe(
            Effect.provide(Reactivity.layer),
            Effect.map((client) => Object.assign(client, { config: { filename: database.filename } })),
          )
          const eventLayer = EventV2.layerWith({ blobDir: database.blobDir }).pipe(
            Layer.provideMerge(databaseLayer(database.filename, client)),
          )
          const services = yield* duringAsyncAcquisition(client, Layer.build(eventLayer))
          const events = Context.get(services, EventV2.Service)
          const { db } = Context.get(services, DatabaseService.Service)
          const published = yield* duringAsyncAcquisition(client, events.publish(Note, { sessionID: "s1", text }))
          const read = yield* duringAsyncAcquisition(
            client,
            EventV2.readAggregate(db, { aggregateID: "s1", limit: 10, manifest: Manifest }),
          )
          return { published, read: read.events }
        }),
      ),
    )

    expect(result.published.data).toEqual({ sessionID: "s1", text })
    expect(result.read).toHaveLength(1)
    expect(result.read[0]?.data).toEqual({ sessionID: "s1", text })

    const blobRows = database.native.query("SELECT aggregate_id, seq, hash, file_path FROM event_blob").all() as Array<{
      aggregate_id: string
      seq: number
      hash: string
      file_path: string
    }>
    expect(blobRows).toHaveLength(1)
    expect(blobRows[0]?.aggregate_id).toBe("s1")
    expect(fs.existsSync(path.join(database.blobDir, blobRows[0]!.file_path))).toBe(true)
  })
})
