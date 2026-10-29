// D16 event_blob integrity `event_blob`: atomic content-addressed writes, hash
// and size revalidation, refcounted dedupe GC, no inline duplication, fail-loud
// when the blob table is missing, untrusted `file_path` rejection, and batched
// blob reads with no filesystem writes inside `BEGIN IMMEDIATE`.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Database } from "bun:sqlite"
import { Effect, Schema } from "effect"
import { getTableConfig } from "drizzle-orm/sqlite-core"
import { Event } from "@opencode-ai/schema/event"
import { DurableLog } from "@opencode-ai/core/durable-log"
import { openConnection } from "@opencode-ai/core/database/connection"
import { migrations } from "@opencode-ai/core/database/migration.gen"
import { DEFAULT_WORKER_CACHE_SIZE_KIB } from "@opencode-ai/core/database/pragmas"
import { EventBlobTable } from "@opencode-ai/core/event/sql"
import { markSchemaIdentity } from "@/session/db/schema"
import { ENVELOPE_HEADROOM_BYTES, MAX_INLINE_EVENT_BYTES } from "@/session/egress/frame"
import { openWorkerDatabase } from "@/session/worker/db"
import { MAX_WORKER_FRAME_BYTES } from "@/session/worker/frame"
import { migrationStatements } from "@/session/writer/migration.statements.gen"
import { openWriterConnection, writerPragmas } from "@/session/writer/connection"

const Note = Event.define({
  type: "test.blob-integrity.note",
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

const DDL_WITHOUT_BLOBS = `
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
`

const directories: string[] = []
const natives: Database[] = []

function workspace(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-blob-integrity-"))
  directories.push(directory)
  return directory
}

function connectionFrom(native: Database): DurableLog.Connection {
  return {
    all: (sql, params = []) => native.query(sql).all(...(params as never[])) as Array<Record<string, unknown>>,
    run: (sql, params = []) => ({ changes: Number(native.query(sql).run(...(params as never[])).changes) }),
    exec: (sql) => native.exec(sql),
  }
}

function open(ddl: string = DDL) {
  const directory = workspace()
  const filename = path.join(directory, "blob.db")
  const native = new Database(filename, { create: true })
  native.exec(ddl)
  native.exec("PRAGMA foreign_keys = ON")
  natives.push(native)
  return {
    directory,
    filename,
    blobDir: path.join(directory, "blobs"),
    native,
    connection: connectionFrom(native),
  }
}

function indexNames(database: Database): string[] {
  return database
    .query("PRAGMA index_list(event_blob)")
    .all()
    .map((row) => String((row as { name: unknown }).name))
}

function indexColumns(database: Database, index: string): string[] {
  return database
    .query(`PRAGMA index_info(${index})`)
    .all()
    .map((row) => String((row as { name: unknown }).name))
}

function applyEventBlobMigration(database: Database): void {
  database.exec("CREATE TABLE event_sequence (aggregate_id text PRIMARY KEY, seq integer NOT NULL)")
  const migration = migrations.find((candidate) => candidate.id === "20261027074148_event_blob")
  if (migration === undefined) throw new Error("event_blob migration is missing")
  const tx = {
    run: (statement: unknown) => {
      database.exec(String(statement))
      return Effect.void
    },
  }
  Effect.runSync(migration.up(tx as never))
}

function migratedSchema(): Database {
  const database = new Database(":memory:")
  for (const statement of migrationStatements) database.exec(statement)
  return database
}

function migratedWorkerDatabase(): string {
  const filename = path.join(workspace(), "worker-blob.db")
  const connection = openConnection({ filename })
  try {
    for (const statement of migrationStatements) connection.exec(statement)
    markSchemaIdentity(connection)
  } finally {
    connection.close()
  }
  return filename
}

function seedSession(filename: string, sessionID: string): void {
  const connection = openConnection({ filename })
  try {
    connection.run(
      "INSERT INTO project (id, name, worktree, sandboxes, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
      ["project-blob", "blob", "/tmp/blob", "[]", 1, 1],
    )
    connection.run(
      "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      [sessionID, "project-blob", sessionID, "/tmp/blob", sessionID, "1", 1, 1],
    )
  } finally {
    connection.close()
  }
}

function createBus(input: ReturnType<typeof open>, overrides: Partial<DurableLog.Options> = {}): DurableLog.Interface {
  return DurableLog.create({
    connection: input.connection,
    ownerID: "gen-blob",
    definitions: [Note],
    blobDir: input.blobDir,
    blobLimitBytes: 64,
    ...overrides,
  })
}

const note = (sessionID: string, text: string) => ({ sessionID, text })
const textOf = (event: Event.Payload) => (event.data as { text: string }).text
const oversized = (seed: string) => `${seed}:${"x".repeat(4_096)}`

afterEach(() => {
  for (const native of natives.splice(0)) native.close()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("blob-integrity: atomic storage", () => {
  test("repairs a torn file and leaves no temp file behind", () => {
    const database = open()
    const bus = createBus(database)
    const text = oversized("torn")
    bus.publish([{ definition: Note, data: note("s1", text) }])
    const [file] = fs.readdirSync(database.blobDir)
    expect(file).toBeDefined()
    const complete = fs.readFileSync(path.join(database.blobDir, file!), "utf8")

    fs.writeFileSync(path.join(database.blobDir, file!), "truncated")
    bus.publish([{ definition: Note, data: note("s1", text) }])

    expect(fs.readdirSync(database.blobDir)).toEqual([file!])
    expect(fs.readFileSync(path.join(database.blobDir, file!), "utf8")).toBe(complete)
    expect(bus.read({ aggregateID: "s1" }).map(textOf)).toEqual([text, text])
  })

  test("a corrupted blob fails loud instead of decoding wrong data", () => {
    const database = open()
    const bus = createBus(database)
    bus.publish([{ definition: Note, data: note("s1", oversized("corrupt")) }])
    const [file] = fs.readdirSync(database.blobDir)
    const original = fs.readFileSync(path.join(database.blobDir, file!), "utf8")
    fs.writeFileSync(path.join(database.blobDir, file!), "y".repeat(original.length))

    expect(() => bus.read({ aggregateID: "s1" })).toThrow(/hash does not match its content/)
  })
})

describe("blob-integrity: write amplification", () => {
  test("inline payloads are stored once, not duplicated into event_blob", () => {
    const database = open()
    const bus = createBus(database)
    for (let index = 0; index < 5; index += 1) {
      bus.publish([{ definition: Note, data: note("s1", `small-${index}`) }])
    }

    expect(database.connection.all("SELECT COUNT(*) AS count FROM event")).toEqual([{ count: 5 }])
    expect(database.connection.all("SELECT COUNT(*) AS count FROM event_blob")).toEqual([{ count: 0 }])
    const stored = database.connection.all("SELECT data FROM event") as ReadonlyArray<{ data: string }>
    expect(stored.some((row) => row.data.includes("__eventBlob"))).toBe(false)

    bus.publish([{ definition: Note, data: note("s1", oversized("large")) }])
    expect(database.connection.all("SELECT COUNT(*) AS count FROM event_blob")).toEqual([{ count: 1 }])
  })
})

describe("blob-integrity: refcounted dedupe GC", () => {
  test("one file is shared by every row that has the same hash", () => {
    const database = open()
    const bus = createBus(database)
    const text = oversized("shared")
    bus.publish([
      { definition: Note, data: note("s1", text) },
      { definition: Note, data: note("s1", text) },
    ])

    expect(fs.readdirSync(database.blobDir)).toHaveLength(1)
    expect(database.connection.all("SELECT COUNT(*) AS rows FROM event_blob")).toEqual([{ rows: 2 }])
    expect(database.connection.all("SELECT COUNT(DISTINCT hash) AS hashes FROM event_blob")).toEqual([{ hashes: 1 }])
    expect(bus.read({ aggregateID: "s1" }).map(textOf)).toEqual([text, text])

    expect(bus.collectBlobs({ aggregateID: "s1", beforeSeq: 2 })).toBe(2)
    expect(fs.readdirSync(database.blobDir)).toEqual([])
  })

  test("a partial cursor leaves a still-referenced deduped file in place", () => {
    const database = open()
    const bus = createBus(database)
    const text = oversized("same-aggregate")
    bus.publish([
      { definition: Note, data: note("s3", text) },
      { definition: Note, data: note("s3", text) },
    ])

    expect(bus.collectBlobs({ aggregateID: "s3", beforeSeq: 1 })).toBe(1)
    expect(fs.readdirSync(database.blobDir)).toHaveLength(1)
    expect(bus.read({ aggregateID: "s3" }).map(textOf)).toEqual([text])

    expect(bus.collectBlobs({ aggregateID: "s3", beforeSeq: 2 })).toBe(1)
    expect(fs.readdirSync(database.blobDir)).toEqual([])
  })

  test("retention never leaves a live event referencing a collected blob", () => {
    const database = open()
    const bus = createBus(database)
    bus.publish([
      { definition: Note, data: note("s4", oversized("retained-away")) },
      { definition: Note, data: note("s4", "kept") },
    ])

    expect(bus.collectBlobs({ aggregateID: "s4", beforeSeq: 1 })).toBe(1)
    expect(database.connection.all("SELECT seq FROM event ORDER BY seq")).toEqual([{ seq: 1 }])
    expect(database.connection.all("SELECT COUNT(*) AS count FROM event_blob")).toEqual([{ count: 0 }])
    expect(bus.read({ aggregateID: "s4" }).map(textOf)).toEqual(["kept"])
  })

  test("aggregate deletion keeps a file another aggregate still references", () => {
    const database = open()
    const bus = createBus(database)
    bus.publish([{ definition: Note, data: note("s1", oversized("cross-aggregate")) }])
    const [file] = fs.readdirSync(database.blobDir)
    const row = (
      database.connection.all("SELECT hash, size, type FROM event_blob WHERE aggregate_id = ?", [
        "s1",
      ]) as ReadonlyArray<{
        hash: string
        size: number
        type: string
      }>
    )[0]!
    // A replayed/imported aggregate can share the content hash; the file must
    // survive until that second reference is gone.
    database.connection.run("INSERT INTO event_sequence (aggregate_id, seq) VALUES (?, 0)", ["s2"])
    database.connection.run(
      "INSERT INTO event_blob (aggregate_id, seq, type, hash, size, data, file_path) VALUES (?, 0, ?, ?, ?, NULL, ?)",
      ["s2", row.type, row.hash, row.size, row.hash],
    )
    database.connection.run("INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?, ?, 0, ?, ?)", [
      "evt-s2",
      "s2",
      row.type,
      JSON.stringify({ __eventBlob: row.hash, size: row.size }),
    ])

    expect(bus.deleteAggregate("s1")).toBe(1)
    expect(fs.readdirSync(database.blobDir)).toEqual([file!])

    expect(bus.deleteAggregate("s2")).toBe(1)
    expect(fs.readdirSync(database.blobDir)).toEqual([])
  })

  test("aggregate deletion collects its blobs and files", () => {
    const database = open()
    const bus = createBus(database)
    bus.publish([{ definition: Note, data: note("s5", oversized("aggregate")) }])
    expect(fs.readdirSync(database.blobDir)).toHaveLength(1)

    expect(bus.deleteAggregate("s5")).toBe(1)
    expect(fs.readdirSync(database.blobDir)).toEqual([])
    expect(database.connection.all("SELECT COUNT(*) AS count FROM event_blob")).toEqual([{ count: 0 }])
    expect(database.connection.all("SELECT COUNT(*) AS count FROM event")).toEqual([{ count: 0 }])
    expect(database.connection.all("SELECT COUNT(*) AS count FROM event_sequence")).toEqual([{ count: 0 }])
    expect(bus.read({ aggregateID: "s5" })).toEqual([])
  })
})

describe("blob-integrity: aggregate-scoped DurableLog", () => {
  test("collectBlobs runs its retention query under the worker scope guard", async () => {
    const filename = migratedWorkerDatabase()
    seedSession(filename, "session-a")
    const database = await Effect.runPromise(openWorkerDatabase({ filename, sessionID: "session-a" }))
    try {
      const bus = database.bus.create({
        ownerID: "gen-scoped",
        definitions: [Note],
        aggregateID: "session-a",
        blobLimitBytes: 64,
      })
      const blobDir = path.join(path.dirname(filename), "event-blobs")
      bus.publish([
        { definition: Note, data: note("session-a", oversized("retained-away")) },
        { definition: Note, data: note("session-a", "kept") },
      ])
      expect(fs.readdirSync(blobDir)).toHaveLength(1)

      expect(bus.collectBlobs({ aggregateID: "session-a", beforeSeq: 1 })).toBe(1)
      expect(
        database.connection.all("SELECT seq FROM event WHERE aggregate_id = ? ORDER BY seq", ["session-a"]),
      ).toEqual([{ seq: 1 }])
      expect(
        database.connection.all("SELECT COUNT(*) AS count FROM event_blob WHERE aggregate_id = ?", ["session-a"]),
      ).toEqual([{ count: 0 }])
      expect(fs.readdirSync(blobDir)).toEqual([])
      expect(bus.read({ aggregateID: "session-a" }).map(textOf)).toEqual(["kept"])
    } finally {
      await Effect.runPromise(database.close)
    }
  })
})

describe("blob-integrity: fail loud and path trust", () => {
  test("an oversized payload without an event_blob table is refused, not stored inline", () => {
    const database = open(DDL_WITHOUT_BLOBS)
    const bus = createBus(database)

    bus.publish([{ definition: Note, data: note("s1", "small") }])
    expect(() => bus.publish([{ definition: Note, data: note("s1", oversized("unbounded")) }])).toThrow(
      /no event_blob table/,
    )
    expect(database.connection.all("SELECT COUNT(*) AS count FROM event")).toEqual([{ count: 1 }])
  })

  test("an untrusted blob path is rejected before it can be joined", () => {
    const database = open()
    const bus = createBus(database)
    bus.publish([{ definition: Note, data: note("s6", oversized("path")) }])
    database.connection.run("UPDATE event_blob SET file_path = ? WHERE aggregate_id = ? AND seq = ?", [
      "../../escape",
      "s6",
      0,
    ])

    expect(() => bus.read({ aggregateID: "s6" })).toThrow(/not a 64-character lowercase hex content hash/)
  })
})

describe("blob-integrity: hot path", () => {
  test("one read batches all blob rows into a single query", () => {
    const database = open()
    let blobQueries = 0
    const counted: DurableLog.Connection = {
      all: (sql, params) => {
        if (sql.includes("FROM event_blob") && sql.includes("seq IN")) blobQueries += 1
        return database.connection.all(sql, params)
      },
      run: (sql, params) => database.connection.run(sql, params),
      exec: (sql) => database.connection.exec(sql),
    }
    const bus = createBus({ ...database, connection: counted })
    for (let index = 0; index < 5; index += 1) {
      bus.publish([{ definition: Note, data: note("s7", oversized(`batch-${index}`)) }])
    }

    blobQueries = 0
    expect(bus.read({ aggregateID: "s7" })).toHaveLength(5)
    expect(blobQueries).toBe(1)
  })

  test("blob files are staged before BEGIN IMMEDIATE", () => {
    const database = open()
    let stagedBeforeBegin = false
    const observed: DurableLog.Connection = {
      all: (sql, params) => database.connection.all(sql, params),
      run: (sql, params) => database.connection.run(sql, params),
      exec: (sql) => {
        if (sql === "BEGIN IMMEDIATE") {
          stagedBeforeBegin = fs.existsSync(database.blobDir) && fs.readdirSync(database.blobDir).length > 0
        }
        database.connection.exec(sql)
      },
    }
    const bus = createBus({ ...database, connection: observed })

    bus.publish([{ definition: Note, data: note("s8", oversized("staged")) }])
    expect(stagedBeforeBegin).toBe(true)
  })
})

describe("blob-integrity: frame-aligned inline bound", () => {
  test("the default inline bound leaves envelope headroom below the frame cap", () => {
    // The schema bound may reach the cap, but the DEFAULT inline bound must
    // reserve the envelope: an inline row at the cap cannot fit its frame, and
    // would cross as a position reference with no event_blob row to resolve.
    expect(DurableLog.DEFAULT_BLOB_LIMIT_BYTES).toBeLessThanOrEqual(MAX_WORKER_FRAME_BYTES)
    expect(DurableLog.DEFAULT_INLINE_PAYLOAD_LIMIT_BYTES).toBe(MAX_INLINE_EVENT_BYTES)
    expect(MAX_INLINE_EVENT_BYTES).toBe(MAX_WORKER_FRAME_BYTES - ENVELOPE_HEADROOM_BYTES)
  })

  test("a payload just under 4 MiB is blob-referenced under the default bound", () => {
    const database = open()
    const bus = DurableLog.create({
      connection: database.connection,
      ownerID: "gen-inline-headroom",
      definitions: [Note],
      blobDir: database.blobDir,
    })
    const text = `headroom:${"x".repeat(4 * 1024 * 1024 - 1_024)}`
    expect(Buffer.byteLength(JSON.stringify(note("s1", text)))).toBeGreaterThan(DurableLog.DEFAULT_INLINE_PAYLOAD_LIMIT_BYTES)
    expect(Buffer.byteLength(JSON.stringify(note("s1", text)))).toBeLessThanOrEqual(DurableLog.DEFAULT_BLOB_LIMIT_BYTES)

    bus.publish([{ definition: Note, data: note("s1", text) }])

    const [stored] = database.connection.all("SELECT data FROM event") as ReadonlyArray<{ data: string }>
    expect(stored?.data).toContain("__eventBlob")
    expect(database.connection.all("SELECT COUNT(*) AS count FROM event_blob")).toEqual([{ count: 1 }])
    expect(bus.read({ aggregateID: "s1" }).map(textOf)).toEqual([text])
  })

  test("a 4-16 MiB event is blob-referenced instead of stored inline", () => {
    const database = open()
    const bus = DurableLog.create({
      connection: database.connection,
      ownerID: "gen-frame-bound",
      definitions: [Note],
      blobDir: database.blobDir,
    })
    const text = `frame:${"x".repeat(5 * 1024 * 1024)}`

    bus.publish([{ definition: Note, data: note("s1", text) }])

    const [stored] = database.connection.all("SELECT data FROM event") as ReadonlyArray<{ data: string }>
    expect(stored?.data).toContain("__eventBlob")
    // The stored row (the shape that can cross a frame) is far below the cap.
    expect(Buffer.byteLength(stored!.data)).toBeLessThan(MAX_WORKER_FRAME_BYTES)
    expect(database.connection.all("SELECT COUNT(*) AS count FROM event_blob")).toEqual([{ count: 1 }])
    expect(fs.readdirSync(database.blobDir)).toHaveLength(1)
    expect(bus.read({ aggregateID: "s1" }).map(textOf)).toEqual([text])
  })
})

describe("blob-integrity: event_blob hash index", () => {
  const INDEX = "event_blob_hash_idx"

  test("the Drizzle schema, the migration, and the generated schema agree", () => {
    const drizzle = getTableConfig(EventBlobTable)
    expect(drizzle.indexes.map((index) => index.config.name)).toContain(INDEX)

    const migration = new Database(":memory:")
    applyEventBlobMigration(migration)
    try {
      expect(indexNames(migration)).toContain(INDEX)
      expect(indexColumns(migration, INDEX)).toEqual(["hash"])
    } finally {
      migration.close()
    }

    const generated = migratedSchema()
    try {
      expect(indexNames(generated)).toContain(INDEX)
      expect(indexColumns(generated, INDEX)).toEqual(["hash"])
    } finally {
      generated.close()
    }
  })

  test("the refcount probe searches the hash index instead of scanning", () => {
    const database = migratedSchema()
    try {
      const plan = database
        .query("EXPLAIN QUERY PLAN SELECT 1 FROM event_blob WHERE hash = ? LIMIT 1")
        .all("00".repeat(32))
        .map((row) => String((row as { detail: unknown }).detail))
        .join(" ")
      expect(plan).toContain(INDEX)
    } finally {
      database.close()
    }
  })
})

describe("blob-integrity: blob directory source", () => {
  test("a custom-file database derives its blob directory from the connection filename", () => {
    const first = open()
    const second = open()
    const text = oversized("custom-file")
    const busFor = (database: ReturnType<typeof open>) =>
      DurableLog.create({
        connection: { ...database.connection, filename: database.filename },
        ownerID: "gen-custom-file",
        definitions: [Note],
        blobLimitBytes: 64,
      })
    const firstBus = busFor(first)
    const secondBus = busFor(second)

    firstBus.publish([{ definition: Note, data: note("s1", text) }])
    secondBus.publish([{ definition: Note, data: note("s1", text) }])

    const firstDir = path.join(first.directory, "event-blobs")
    const secondDir = path.join(second.directory, "event-blobs")
    const [firstFile] = fs.readdirSync(firstDir)
    const [secondFile] = fs.readdirSync(secondDir)
    expect(firstFile).toBeDefined()
    expect(secondFile).toBeDefined()
    expect(firstDir).not.toBe(secondDir)

    firstBus.deleteAggregate("s1")
    expect(fs.existsSync(path.join(firstDir, firstFile))).toBe(false)
    expect(fs.existsSync(path.join(secondDir, secondFile))).toBe(true)
  })
})

describe("writer fallback cache budget", () => {
  test("writerPragmas applies the per-worker cache budget instead of the hub default", () => {
    expect(writerPragmas({ journalSizeLimitBytes: 131072, checkpointIntervalMs: 0 })).toContain(
      `PRAGMA cache_size = -${DEFAULT_WORKER_CACHE_SIZE_KIB}`,
    )
  })

  test("openWriterConnection's fallback handle is bounded by the worker budget", () => {
    const directory = workspace()
    const connection = openWriterConnection(path.join(directory, "writer.db"))
    try {
      expect(Math.abs(Number(connection.pragma("cache_size")))).toBeLessThanOrEqual(DEFAULT_WORKER_CACHE_SIZE_KIB)
    } finally {
      connection.close()
    }
  })
})
