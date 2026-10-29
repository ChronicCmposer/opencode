// Durable/blob settlement cases:
// 1. a legacy inline `event_blob` row (`file_path IS NULL`, `data` present) must resolve
//    through a repository-only DurableLog, not `String(undefined)`;
// 2. blob GC must commit row deletion before it unlinks files, so a rolled-back
//    transaction can never leave a live row pointing at a removed file.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Database } from "bun:sqlite"
import { Effect, Schema } from "effect"
import { Event } from "@opencode-ai/schema/event"
import { DurableLog } from "@opencode-ai/core/durable-log"
import { openConnection } from "@opencode-ai/core/database/connection"
import { markSchemaIdentity } from "@/session/db/schema"
import { openWorkerDatabase, type WorkerDatabase } from "@/session/worker/db"
import { createWorkerDatabaseWriter } from "@/session/worker/session-worker"
import { migrationStatements } from "@/session/writer/migration.statements.gen"

const Note = Event.define({
  type: "test.durable-settlement.note",
  durable: { version: 1, aggregate: "sessionID" },
  schema: {
    sessionID: Schema.String,
    text: Schema.String,
  },
})

const STORED_TYPE = "test.durable-settlement.note.1"

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

const directories: string[] = []
const natives: Database[] = []
const workers: WorkerDatabase[] = []

function workspace(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-durable-settlement-"))
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

function openLegacy() {
  const directory = workspace()
  const filename = path.join(directory, "durable.db")
  const native = new Database(filename, { create: true })
  native.exec(DDL)
  native.exec("PRAGMA foreign_keys = ON")
  natives.push(native)
  return {
    directory,
    filename,
    blobDir: path.join(directory, "blobs"),
    native,
    connection: { ...connectionFrom(native), filename },
  }
}

function migratedWorkerFile(sessionID: string): string {
  const filename = path.join(workspace(), "worker.db")
  const connection = openConnection({ filename })
  try {
    for (const statement of migrationStatements) connection.exec(statement)
    markSchemaIdentity(connection)
    connection.run(
      "INSERT INTO project (id, name, worktree, sandboxes, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
      ["project-durable-settlement", "settlement", "/tmp/durable-settlement", "[]", 1, 1],
    )
    connection.run(
      "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      [sessionID, "project-durable-settlement", sessionID, "/tmp/durable-settlement", sessionID, "1", 1, 1],
    )
  } finally {
    connection.close()
  }
  return filename
}

const note = (sessionID: string, text: string) => ({ sessionID, text })
const textOf = (event: Event.Payload) => (event.data as { text: string }).text
const oversized = (seed: string) => `${seed}:${"x".repeat(4_096)}`

function count(database: ReturnType<typeof openLegacy>, table: "event" | "event_blob"): number {
  return Number(
    (database.connection.all(`SELECT COUNT(*) AS count FROM ${table}`)[0] as { count: number }).count,
  )
}

interface TransactionHooks {
  readonly rollbackAfterWork?: boolean
  readonly afterCommit?: () => void
}

/** Wrap a real repository transaction so the test can force a rollback or observe commit. */
function withTransactionHooks(base: DurableLog.DurableRepository, hooks: TransactionHooks): DurableLog.DurableRepository {
  return {
    ...base,
    transaction: <T>(work: () => T): T => {
      const result = base.transaction(() => {
        const value = work()
        if (hooks.rollbackAfterWork === true) throw new Error("forced rollback after GC work")
        return value
      })
      hooks.afterCommit?.()
      return result
    },
  }
}

afterEach(async () => {
  for (const worker of workers.splice(0)) await Effect.runPromise(worker.close).catch(() => undefined)
  for (const native of natives.splice(0)) native.close()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("durable settlement: legacy inline blob rows", () => {
  test("a repository-only DurableLog resolves a legacy inline event_blob row", () => {
    const database = openLegacy()
    const payload = JSON.stringify(note("s1", oversized("legacy-inline")))
    const hash = DurableLog.digest(payload)
    const repository = DurableLog.createDurableRepository(database.connection)

    repository.appendSequence({ aggregateID: "s1", block: 1 })
    repository.appendEvent({
      id: Event.ID.create(),
      aggregateID: "s1",
      seq: 0,
      type: STORED_TYPE,
      data: { __eventBlob: hash, size: Buffer.byteLength(payload) },
    })
    // The pre-D16 schema wrote the payload inline; there is no file for this row.
    database.connection.run(
      "INSERT INTO event_blob (aggregate_id, seq, type, hash, size, data, file_path) VALUES (?, 0, ?, ?, ?, ?, NULL)",
      ["s1", STORED_TYPE, hash, Buffer.byteLength(payload), payload],
    )

    const bus = DurableLog.create({
      repository,
      ownerID: "gen-legacy-inline",
      definitions: [Note],
      blobDir: database.blobDir,
    })

    expect(bus.read({ aggregateID: "s1" }).map(textOf)).toEqual([note("s1", oversized("legacy-inline")).text])
  })
})

describe("durable settlement: blob GC ordering", () => {
  test("a rolled-back collect keeps both the rows and the file", () => {
    const database = openLegacy()
    const base = DurableLog.createDurableRepository(database.connection)
    const publishBus = DurableLog.create({
      repository: base,
      ownerID: "gen-gc-rollback",
      definitions: [Note],
      blobDir: database.blobDir,
      blobLimitBytes: 64,
    })
    publishBus.publish([{ definition: Note, data: note("s1", oversized("rollback")) }])
    const [file] = fs.readdirSync(database.blobDir)
    expect(file).toBeDefined()

    const gcBus = DurableLog.create({
      repository: withTransactionHooks(base, { rollbackAfterWork: true }),
      ownerID: "gen-gc-rollback",
      definitions: [Note],
      blobDir: database.blobDir,
      blobLimitBytes: 64,
    })

    expect(() => gcBus.collectBlobs({ aggregateID: "s1", beforeSeq: 1 })).toThrow(
      "forced rollback after GC work",
    )
    expect(fs.existsSync(path.join(database.blobDir, file!))).toBe(true)
    expect(count(database, "event")).toBe(1)
    expect(count(database, "event_blob")).toBe(1)
    expect(publishBus.read({ aggregateID: "s1" }).map(textOf)).toEqual([oversized("rollback")])
  })

  test("a missing blob directory fails before deleting any row", () => {
    const database = openLegacy()
    const base = DurableLog.createDurableRepository(database.connection)
    const publishBus = DurableLog.create({
      repository: base,
      ownerID: "gen-gc-no-dir",
      definitions: [Note],
      blobDir: database.blobDir,
      blobLimitBytes: 64,
    })
    publishBus.publish([{ definition: Note, data: note("s1", oversized("no-dir")) }])
    const [file] = fs.readdirSync(database.blobDir)
    expect(file).toBeDefined()

    // A repository without a filename cannot derive a blob directory; the DurableLog
    // must refuse the collection while the rows are still intact.
    const gcBus = DurableLog.create({
      repository: { ...base, filename: undefined },
      ownerID: "gen-gc-no-dir",
      definitions: [Note],
      blobLimitBytes: 64,
    })

    expect(() => gcBus.deleteAggregate("s1")).toThrow(/without a blob directory/)
    expect(count(database, "event")).toBe(1)
    expect(count(database, "event_blob")).toBe(1)
    expect(fs.existsSync(path.join(database.blobDir, file!))).toBe(true)
  })

  test("deleteAggregate unlinks only after the row deletion commits", () => {
    const database = openLegacy()
    const base = DurableLog.createDurableRepository(database.connection)
    const publishBus = DurableLog.create({
      repository: base,
      ownerID: "gen-gc-order",
      definitions: [Note],
      blobDir: database.blobDir,
      blobLimitBytes: 64,
    })
    publishBus.publish([{ definition: Note, data: note("s1", oversized("commit-order")) }])
    const [file] = fs.readdirSync(database.blobDir)
    expect(file).toBeDefined()

    let fileAtCommit: boolean | undefined
    const gcBus = DurableLog.create({
      repository: withTransactionHooks(base, {
        afterCommit: () => {
          // The row-deletion commit must still see the file: unlink happens in a
          // later transaction once the rows are durably gone. Capture the first
          // commit (subsequent GC transactions may legitimately observe it gone).
          fileAtCommit ??= fs.existsSync(path.join(database.blobDir, file!))
        },
      }),
      ownerID: "gen-gc-order",
      definitions: [Note],
      blobDir: database.blobDir,
      blobLimitBytes: 64,
    })

    expect(gcBus.deleteAggregate("s1")).toBe(1)
    expect(fileAtCommit).toBe(true)
    expect(fs.existsSync(path.join(database.blobDir, file!))).toBe(false)
    expect(count(database, "event")).toBe(0)
    expect(count(database, "event_blob")).toBe(0)
  })
})

describe("durable settlement: bound writer surface (S4)", () => {
  test("the bound writer keeps publish/query/dispose and has no transaction API", async () => {
    const sessionID = "session-settlement-tx"
    const filename = migratedWorkerFile(sessionID)
    const database = await Effect.runPromise(openWorkerDatabase({ filename, sessionID }))
    workers.push(database)
    const writer = createWorkerDatabaseWriter({
      bus: database.bus,
      connection: database.connection,
      queryConnection: database.strictConnection,
      filename: database.filename,
      workerID: 71,
      sessionID,
    })

    try {
      // Characterization: D5 makes the worker Database the transaction owner.
      // The writer seam must not grow back `begin`/`txQuery`/`commit`/`rollback`.
      expect("begin" in writer).toBe(false)
      expect("txQuery" in writer).toBe(false)
      expect("commit" in writer).toBe(false)
      expect("rollback" in writer).toBe(false)
      expect(typeof writer.publish).toBe("function")
      expect(typeof writer.dispose).toBe("function")

      const rows = await writer.query("all", "SELECT COUNT(*) AS count FROM event")
      expect(rows).toEqual([{ count: 0 }])
    } finally {
      writer.dispose()
    }
  })
})
