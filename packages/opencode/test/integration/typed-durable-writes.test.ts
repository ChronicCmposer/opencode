// Typed durable-write primitives (P0 boundary).
//
// The durable log has exactly one typed write boundary:
// `Bus.DurableRepository`. EventV2 and Bus must issue every `event`,
// `event_sequence`, and `event_blob` write through it, and the worker-facing Bus
// capability must expose only typed operations (no connection/transaction
// handles). These tests drive the real SQLite and worker paths.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Database } from "bun:sqlite"
import { Effect, Layer, Schema } from "effect"
import { Event } from "@opencode-ai/schema/event"
import { Session } from "@opencode-ai/schema/session"
import { SessionV1 } from "@opencode-ai/schema/session-v1"
import { Bus } from "@opencode-ai/core/bus"
import { Database as DatabaseService } from "@opencode-ai/core/database/database"
import { openConnection } from "@opencode-ai/core/database/connection"
import { EventV2 } from "@opencode-ai/core/event"
import { markSchemaIdentity } from "@/session/db/schema"
import { openWorkerDatabase, type WorkerDatabase } from "@/session/worker/db"
import { migrationStatements } from "@/session/writer/migration.statements.gen"

const Note = Event.define({
  type: "test.typed-durable.note",
  durable: { version: 1, aggregate: "sessionID" },
  schema: {
    sessionID: Schema.String,
    text: Schema.String,
  },
})

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
    data text NOT NULL,
    CONSTRAINT fk_event_aggregate FOREIGN KEY (aggregate_id)
      REFERENCES event_sequence(aggregate_id) ON DELETE CASCADE
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
    PRIMARY KEY (aggregate_id, seq),
    CONSTRAINT fk_event_blob_aggregate FOREIGN KEY (aggregate_id)
      REFERENCES event_sequence(aggregate_id) ON DELETE CASCADE
  );
`

const directories: string[] = []
const natives: Database[] = []
const workers: WorkerDatabase[] = []

function workspace(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-typed-durable-"))
  directories.push(directory)
  return directory
}

function connectionFrom(native: Database): Bus.Connection {
  return {
    all: (sql, params = []) => native.query(sql).all(...(params as never[])) as Array<Record<string, unknown>>,
    run: (sql, params = []) => ({ changes: Number(native.query(sql).run(...(params as never[])).changes) }),
    exec: (sql) => native.exec(sql),
  }
}

function openLegacy() {
  const directory = workspace()
  const filename = path.join(directory, "durable.db")
  const native = new Database(filename, { create: true })
  native.exec(LEGACY_DDL)
  native.exec("PRAGMA foreign_keys = ON")
  natives.push(native)
  return {
    directory,
    filename,
    blobDir: path.join(directory, "blobs"),
    native,
    connection: { ...connectionFrom(native), filename },
  } satisfies { directory: string; filename: string; blobDir: string; native: Database; connection: Bus.Connection }
}

function migratedWorkerFile(): string {
  const filename = path.join(workspace(), "worker.db")
  const connection = openConnection({ filename })
  try {
    for (const statement of migrationStatements) connection.exec(statement)
    markSchemaIdentity(connection)
    connection.run(
      "INSERT INTO project (id, name, worktree, sandboxes, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
      ["project-typed-durable", "typed", "/tmp/typed-durable", "[]", 1, 1],
    )
    connection.run(
      "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      ["session-typed", "project-typed-durable", "typed", "/tmp/typed-durable", "typed", "1", 1, 1],
    )
  } finally {
    connection.close()
  }
  return filename
}

const note = (sessionID: string, text: string) => ({ sessionID, text })
const textOf = (event: Event.Payload) => (event.data as { text: string }).text
const oversized = (seed: string) => `${seed}:${"x".repeat(4_096)}`

afterEach(async () => {
  for (const worker of workers.splice(0)) await Effect.runPromise(worker.close).catch(() => undefined)
  for (const native of natives.splice(0)) native.close()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("typed repository: durable write boundary", () => {
  test("appendSequence/appendEvent/advanceSequence write only through typed methods", () => {
    const database = openLegacy()
    const repository = Bus.createDurableRepository(database.connection)

    // No raw statement surface leaks from the boundary.
    expect(repository).not.toHaveProperty("run")
    expect(repository).not.toHaveProperty("all")
    expect(repository).not.toHaveProperty("exec")
    expect(repository.workerLease).toBe(false)
    expect(repository.blobTable).toBe(true)

    expect(repository.appendSequence({ aggregateID: "s1", block: 3, syncOwnerID: "sync-1" })).toBe(2)
    repository.appendEvent({
      id: Event.ID.create(),
      aggregateID: "s1",
      seq: 0,
      type: "test.typed-durable.note.1",
      data: note("s1", "a"),
    })
    repository.appendEvent({
      id: Event.ID.create(),
      aggregateID: "s1",
      seq: 1,
      type: "test.typed-durable.note.1",
      data: note("s1", "b"),
    })
    repository.advanceSequence({ aggregateID: "s2", seq: 4 })

    expect(repository.sequence("s1")).toEqual({
      seq: 2,
      syncOwnerID: "sync-1",
      workerOwnerID: undefined,
      workerHeartbeatAt: undefined,
      workerExpiresAt: undefined,
    })
    expect(repository.readEvents({ aggregateID: "s1" }).map((event) => event.seq)).toEqual([0, 1])
    expect(repository.readEvents({ aggregateID: "s1", after: 0 }).map((event) => event.seq)).toEqual([1])
    expect(repository.sequence("s2")?.seq).toBe(4)
    expect(database.connection.all("SELECT seq FROM event_sequence WHERE aggregate_id = ?", ["s1"])).toEqual([{ seq: 2 }])
  })

  test("appendBlob records a content-addressed row that Bus reads back", () => {
    const database = openLegacy()
    const repository = Bus.createDurableRepository(database.connection)
    const payload = JSON.stringify(note("s1", oversized("typed-blob")))
    const hash = Bus.digest(payload)
    Bus.writeBlobFile(database.blobDir, hash, payload)

    repository.appendSequence({ aggregateID: "s1", block: 1 })
    repository.appendBlob({
      aggregateID: "s1",
      seq: 0,
      type: "test.typed-durable.note.1",
      hash,
      size: Buffer.byteLength(payload),
      filePath: hash,
    })
    repository.appendEvent({
      id: Event.ID.create(),
      aggregateID: "s1",
      seq: 0,
      type: "test.typed-durable.note.1",
      data: { __eventBlob: hash, size: Buffer.byteLength(payload) },
    })

    const bus = Bus.create({
      repository,
      ownerID: "typed-blob-gen",
      definitions: [Note],
      blobDir: database.blobDir,
    })
    expect(bus.read({ aggregateID: "s1" }).map(textOf)).toEqual([oversized("typed-blob")])
    expect(repository.readBlobs({ aggregateID: "s1" })).toEqual([{ seq: 0, hash, filePath: hash }])
    expect(repository.blobReferences(hash)).toBe(1)
  })
})

describe("typed repository: lease methods fence the write path", () => {
  test("claimLease is owner-exclusive and reclaimLease bumps only at the expected sequence", () => {
    const database = openLegacy()
    const first = Bus.createDurableRepository(database.connection)
    const second = Bus.createDurableRepository({ ...database.connection, filename: database.filename })

    first.claimLease({ aggregateID: "s1", ownerID: "gen-1", leaseTimeoutMs: 0 })
    expect(() => second.claimLease({ aggregateID: "s1", ownerID: "gen-2", leaseTimeoutMs: 0 })).toThrow(
      Bus.StaleOwnerError,
    )
    expect(() =>
      second.reclaimLease({
        aggregateID: "s1",
        expectedOwnerID: "gen-1",
        expectedSeq: 1,
        nextOwnerID: "gen-2",
        leaseTimeoutMs: 0,
      }),
    ).toThrow(Bus.LeaseConflictError)
    expect(
      second.reclaimLease({
        aggregateID: "s1",
        expectedOwnerID: "gen-1",
        expectedSeq: -1,
        nextOwnerID: "gen-2",
        leaseTimeoutMs: 0,
      }),
    ).toBe(1)
    expect(first.sequence("s1")?.syncOwnerID).toBe("gen-2")
  })

  test("the worker Bus exposes only typed operations and fences a stale generation", async () => {
    const filename = migratedWorkerFile()
    const database = await Effect.runPromise(openWorkerDatabase({ filename, sessionID: "session-typed" }))
    workers.push(database)
    const blobDir = path.join(path.dirname(filename), "event-blobs")

    const handle = database.bus.create({
      ownerID: "gen-1",
      definitions: [Note],
      aggregateID: "session-typed",
      blobLimitBytes: 64,
    })
    expect(handle).not.toHaveProperty("connection")
    expect(handle).not.toHaveProperty("transaction")
    expect(handle).not.toHaveProperty("transactionAsync")

    const text = oversized("worker")
    const [first] = handle.publish([{ definition: Note, data: note("session-typed", text) }])
    expect(first?.durable?.seq).toBe(0)
    expect(fs.readdirSync(blobDir)).toHaveLength(1)
    expect(handle.read({ aggregateID: "session-typed" }).map(textOf)).toEqual([text])

    const successor = database.bus.create({
      ownerID: "gen-2",
      definitions: [Note],
      aggregateID: "session-typed",
    })
    expect(() => successor.claim("session-typed")).toThrow(Bus.StaleOwnerError)

    handle.release("session-typed")
    expect(
      successor.reclaim({
        aggregateID: "session-typed",
        expectedOwnerID: "gen-1",
        expectedSeq: 0,
        nextOwnerID: "gen-2",
      }),
    ).toEqual({ aggregateID: "session-typed", ownerID: "gen-2", seq: 0 })
  })
})

describe("typed repository: Bus injection and refcounted GC", () => {
  test("Bus.create accepts the typed repository alone and publishes through it", () => {
    const database = openLegacy()
    const base = Bus.createDurableRepository(database.connection)
    const calls = { appendSequence: 0, appendEvent: 0, appendBlob: 0, renewLease: 0 }
    const spy: Bus.DurableRepository = {
      ...base,
      appendSequence: (input) => {
        calls.appendSequence += 1
        return base.appendSequence(input)
      },
      appendEvent: (input) => {
        calls.appendEvent += 1
        base.appendEvent(input)
      },
      appendBlob: (input) => {
        calls.appendBlob += 1
        base.appendBlob(input)
      },
      renewLease: (input) => {
        calls.renewLease += 1
        return base.renewLease(input)
      },
    }
    const bus = Bus.create({
      repository: spy,
      ownerID: "gen-typed",
      definitions: [Note],
      blobDir: database.blobDir,
      blobLimitBytes: 64,
    })

    bus.publish([
      { definition: Note, data: note("s1", "small") },
      { definition: Note, data: note("s1", oversized("large")) },
    ])

    // The legacy schema has no worker-lease column, so publish performs no lease renewal.
    expect(calls).toEqual({ appendSequence: 1, appendEvent: 2, appendBlob: 1, renewLease: 0 })
    expect(bus.latest("s1")).toBe(1)
    expect(bus.read({ aggregateID: "s1" }).map(textOf)).toEqual(["small", oversized("large")])
    expect(bus.collectBlobs({ aggregateID: "s1", beforeSeq: 2 })).toBe(1)
    expect(fs.readdirSync(database.blobDir)).toEqual([])
  })

  test("the 16 MiB inline bound is the default and above it offloads by reference", () => {
    expect(Bus.DEFAULT_BLOB_LIMIT_BYTES).toBe(16 * 1024 * 1024)

    const database = openLegacy()
    const bus = Bus.create({
      connection: database.connection,
      ownerID: "gen-16mib",
      definitions: [Note],
      blobDir: database.blobDir,
    })
    expect(bus.read({ aggregateID: "s1" })).toEqual([])
    expect(Bus.isBlobReference({ __eventBlob: "0".repeat(64), size: 16 * 1024 * 1024 + 1 })).toBe(true)
    expect(Bus.isBlobReference({ __eventBlob: "0".repeat(64) })).toBe(false)
  })
})

describe("EventV2 uses the typed durable path", () => {
  test("publish, replay, and remove keep the sequence/blob rows consistent", async () => {
    const filename = migratedWorkerFile()
    const blobDir = path.join(path.dirname(filename), "event-blobs")
    const sessionID = Session.ID.create()
    const layer = EventV2.layerWith({ blobDir }).pipe(Layer.provide(DatabaseService.layerFromPath(filename)))

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const events = yield* EventV2.Service
          const published = yield* events.publish(SessionV1.Event.MessageRemoved, {
            sessionID,
            messageID: SessionV1.MessageID.ascending("msg_typed"),
          })
          expect(published.durable?.seq).toBe(0)

          yield* events.replay({
            id: Event.ID.create(),
            type: EventV2.versionedType(SessionV1.Event.MessageRemoved.type, 1),
            seq: 1,
            aggregateID: sessionID,
            data: {
              sessionID,
              messageID: SessionV1.MessageID.ascending("msg_typed_replay"),
            },
          })

          yield* events.remove(sessionID)
        }),
      ).pipe(Effect.provide(layer)),
    )

    const admin = openConnection({ filename })
    try {
      expect(admin.all("SELECT COUNT(*) AS count FROM event WHERE aggregate_id = ?", [sessionID])).toEqual([
        { count: 0 },
      ])
      expect(admin.all("SELECT COUNT(*) AS count FROM event_sequence WHERE aggregate_id = ?", [sessionID])).toEqual([
        { count: 0 },
      ])
      expect(admin.all("SELECT COUNT(*) AS count FROM event_blob WHERE aggregate_id = ?", [sessionID])).toEqual([
        { count: 0 },
      ])
    } finally {
      admin.close()
    }
  })
})
