import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { Database as BunDatabase } from "bun:sqlite"
import { afterEach, describe, expect, test } from "bun:test"
import { Effect, Schema } from "effect"
import { Event } from "@opencode-ai/schema/event"
import { Bus } from "@opencode-ai/core/bus"
import { openConnection } from "@opencode-ai/core/database/connection"
import { migrationStatements } from "@/session/writer/migration.statements.gen"
import {
  HUB_ONLY_TABLES,
  HubOnlyTableError,
  SchemaMismatch,
  WORKER_WRITABLE_TABLES,
  WorkerStatementRejectedError,
  assertWorkerTableOwnership,
  currentSchemaIdentity,
  deriveSessionScopedTables,
  markSchemaIdentity,
  openWorkerDatabase,
  schemaIdentity,
  type WorkerDatabase,
} from "@/session/worker/db"

const temporaryDirectories: string[] = []
const openDatabases: WorkerDatabase[] = []

function temporaryDatabase(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-worker-db-"))
  temporaryDirectories.push(directory)
  return path.join(directory, "opencode.db")
}

function migrated(filename: string): void {
  const connection = openConnection({ filename })
  try {
    for (const statement of migrationStatements) connection.exec(statement)
    markSchemaIdentity(connection)
    connection.run(
      "INSERT INTO project (id, name, worktree, sandboxes, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
      ["project-1", "project", "/tmp/project", "[]", 1, 1],
    )
    connection.run(
      "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      ["session-a", "project-1", "a", "/tmp/project", "a", "1", 1, 1],
    )
    connection.run(
      "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      ["session-b", "project-1", "b", "/tmp/project", "b", "1", 1, 1],
    )
  } finally {
    connection.close()
  }
}

async function open(filename: string): Promise<WorkerDatabase> {
  const database = await Effect.runPromise(openWorkerDatabase({ filename }))
  openDatabases.push(database)
  return database
}

afterEach(async () => {
  for (const database of openDatabases.splice(0)) await Effect.runPromise(database.close)
  for (const directory of temporaryDirectories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("per-worker WAL connection (W1.1 / D5)", () => {
  test("two workers write different sessions concurrently over their own connections", async () => {
    const filename = temporaryDatabase()
    migrated(filename)

    const workerA = await open(filename)
    const workerB = await open(filename)

    const write = (database: WorkerDatabase, id: string, sessionID: string) =>
      database.run(
        "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)",
        [id, sessionID, 1, 1, "{}"],
      )

    await Effect.runPromise(
      Effect.all(
        [
          write(workerA, "message-a", "session-a"),
          write(workerB, "message-b", "session-b"),
        ],
        { concurrency: "unbounded" },
      ),
    )

    const rows = await Effect.runPromise(
      workerA.all<{ session_id: string }>("SELECT session_id FROM message ORDER BY session_id"),
    )
    expect(rows.map((row) => row.session_id)).toEqual(["session-a", "session-b"])
  })

  test("the connection is WAL and each worker has its own handle", async () => {
    const filename = temporaryDatabase()
    migrated(filename)

    const [workerA, workerB] = await Promise.all([open(filename), open(filename)])

    expect(workerA.connection.query("PRAGMA journal_mode").all()).toEqual([{ journal_mode: "wal" }])
    expect(workerB.connection.query("PRAGMA journal_mode").all()).toEqual([{ journal_mode: "wal" }])
    expect(workerA.connection).not.toBe(workerB.connection)
  })

  test("dispose closes the WAL connection exactly once", async () => {
    const filename = temporaryDatabase()
    migrated(filename)

    const database = await open(filename)
    await Effect.runPromise(database.close)
    await Effect.runPromise(database.close)

    expect(() => database.connection.query("SELECT 1").all()).toThrow()
    const native = new BunDatabase(filename)
    expect(native.query("SELECT COUNT(*) AS count FROM message").all()).toEqual([{ count: 0 }])
    native.close()
  })
})


describe("worker schema gate (W1.6 / D7)", () => {
  test("opens only when user_version and the migration journal match the build", async () => {
    const filename = temporaryDatabase()
    migrated(filename)

    const database = await open(filename)
    expect(database.identity).toEqual(currentSchemaIdentity())
  })

  test("refuses a different user_version with a typed SchemaMismatch and no state change", async () => {
    const filename = temporaryDatabase()
    migrated(filename)
    const admin = openConnection({ filename })
    const before = {
      schema: admin.all("SELECT type, name, sql FROM sqlite_master ORDER BY type, name"),
      journal: admin.all("SELECT id, time_completed FROM migration ORDER BY id"),
    }
    admin.exec("PRAGMA user_version = 999")
    admin.close()

    const error = await Effect.runPromise(openWorkerDatabase({ filename }).pipe(Effect.flip))

    expect(error).toBeInstanceOf(SchemaMismatch)
    expect(error._tag).toBe("WorkerDb.SchemaMismatch")
    expect((error as SchemaMismatch).observed.version).toBe(999)

    const after = openConnection({ filename, pragmas: [] })
    expect(after.all("SELECT type, name, sql FROM sqlite_master ORDER BY type, name")).toEqual(before.schema)
    expect(after.all("SELECT id, time_completed FROM migration ORDER BY id")).toEqual(before.journal)
    expect(Number(after.pragma("user_version"))).toBe(999)
    after.close()
  })

  test("refuses a migration-content mismatch even when the version matches", async () => {
    const filename = temporaryDatabase()
    migrated(filename)
    const admin = openConnection({ filename })
    admin.exec("DELETE FROM migration WHERE id = (SELECT id FROM migration ORDER BY id LIMIT 1)")
    admin.close()

    const error = await Effect.runPromise(openWorkerDatabase({ filename }).pipe(Effect.flip))

    expect(error).toBeInstanceOf(SchemaMismatch)
    expect((error as SchemaMismatch).expected.contentHash).not.toBe((error as SchemaMismatch).observed.contentHash)
  })
})

describe("worker table ownership (W1.5 / D6)", () => {
  test("the allowlist is exactly the session_id tables derived from the schema", () => {
    const filename = temporaryDatabase()
    migrated(filename)
    const connection = openConnection({ filename })
    expect(deriveSessionScopedTables(connection)).toEqual([...WORKER_WRITABLE_TABLES])
    expect(() => assertWorkerTableOwnership(connection)).not.toThrow()
    expect(connection.all("SELECT id FROM migration ORDER BY id").length).toBeGreaterThan(0)
    for (const table of WORKER_WRITABLE_TABLES) expect(HUB_ONLY_TABLES).not.toContain(table)
    connection.close()
  })

  test("a worker write to a hub-only table fails loud", async () => {
    const filename = temporaryDatabase()
    migrated(filename)
    const database = await open(filename)

    const error = await Effect.runPromise(
      database.run("INSERT INTO permission (id) VALUES ('nope')").pipe(Effect.flip),
    )

    expect(error).toBeInstanceOf(HubOnlyTableError)
    expect(error._tag).toBe("WorkerDb.HubOnlyTableError")
    expect((error as HubOnlyTableError).table).toBe("permission")
  })

  test("DDL is refused before it reaches SQLite", async () => {
    const filename = temporaryDatabase()
    migrated(filename)
    const database = await open(filename)

    const error = await Effect.runPromise(
      database.run("CREATE TABLE worker_owned (id text primary key)").pipe(Effect.flip),
    )

    expect(error).toBeInstanceOf(WorkerStatementRejectedError)
    expect(database.connection.query("SELECT name FROM sqlite_master WHERE name = 'worker_owned'").all()).toEqual([])
  })

  test("session.permission is hub-only while the hub connection can still write it (C9/D14)", async () => {
    const filename = temporaryDatabase()
    migrated(filename)
    const database = await open(filename)

    const rejected = await Effect.runPromise(
      database.run("UPDATE permission SET action = 'allow' WHERE id = 'missing'").pipe(Effect.flip),
    )
    expect(rejected).toBeInstanceOf(HubOnlyTableError)
    expect((rejected as HubOnlyTableError).table).toBe("permission")

    const hub = openConnection({ filename })
    try {
      hub.run(
        "INSERT INTO permission (id, project_id, action, resource, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
        ["permission-hub", "project-1", "allow", "*", 1, 1],
      )
    } finally {
      hub.close()
    }
    expect(await Effect.runPromise(database.all("SELECT id FROM permission"))).toEqual([{ id: "permission-hub" }])
  })

  test("the worker Bus publishes durable events through the guarded connection (C2)", async () => {
    const filename = temporaryDatabase()
    migrated(filename)
    const database = await open(filename)
    const Note = Event.define({
      type: "test.worker.db.note",
      durable: { version: 1, aggregate: "sessionID" },
      schema: { sessionID: Schema.String, text: Schema.String },
    })
    const bus = Bus.create({ connection: database.connection, ownerID: "gen-1", definitions: [Note] })
    expect(bus.claim("session-a")).toEqual({ aggregateID: "session-a", ownerID: "gen-1", seq: -1 })

    const published = bus.publish([{ definition: Note, data: { sessionID: "session-a", text: "durable" } }])
    expect(published[0]?.durable?.seq).toBe(0)
    expect(database.connection.all("SELECT aggregate_id, seq, owner_id FROM event_sequence")).toEqual([
      { aggregate_id: "session-a", seq: 0, owner_id: "gen-1" },
    ])
    expect(database.connection.all<{ count: number }>("SELECT COUNT(*) AS count FROM event")).toEqual([{ count: 1 }])

    const stale = Bus.create({ connection: database.connection, ownerID: "gen-2", definitions: [Note] })
    expect(() => stale.publish([{ definition: Note, data: { sessionID: "session-a", text: "stolen" } }])).toThrow(
      Bus.StaleOwnerError,
    )
  })

  test("a worker may read hub-owned state and write its own session tables", async () => {
    const filename = temporaryDatabase()
    migrated(filename)
    const database = await open(filename)

    expect(await Effect.runPromise(database.all("SELECT COUNT(*) AS count FROM session"))).toEqual([{ count: 2 }])
    await Effect.runPromise(
      database.run(
        "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)",
        ["message-z", "session-a", 2, 2, "{}"],
      ),
    )
    expect(await Effect.runPromise(database.all("SELECT id FROM message"))).toEqual([{ id: "message-z" }])
  })
})

describe("worker SQL guard bypass matrix (C9 / D14 / D21)", () => {
  const errorOf = async (effect: Effect.Effect<unknown, unknown>) =>
    Effect.runPromise(effect.pipe(Effect.flip)) as Promise<unknown>

  test("query(...).run cannot bypass the table guard", async () => {
    const filename = temporaryDatabase()
    migrated(filename)
    const database = await open(filename)
    const statement = database.connection.query("INSERT INTO permission (id) VALUES (?)")
    expect(() => statement.run("nope")).toThrow(HubOnlyTableError)
    expect(database.connection.query("SELECT COUNT(*) AS count FROM permission").all()).toEqual([{ count: 0 }])
  })

  test("a commented write cannot hide behind the comment", async () => {
    const filename = temporaryDatabase()
    migrated(filename)
    const database = await open(filename)
    expect(() =>
      database.connection.query("/* leading */ INSERT INTO permission (id) VALUES (?)").run("nope"),
    ).toThrow(HubOnlyTableError)
    expect(() =>
      database.connection.query("-- leading\nUPDATE permission SET id = 'nope'").run(),
    ).toThrow(HubOnlyTableError)
  })

  test("CTE writes are classified by their write statement", async () => {
    const filename = temporaryDatabase()
    migrated(filename)
    const database = await open(filename)

    const insert = await errorOf(
      database.run("WITH x AS (SELECT 1) INSERT INTO permission (id) SELECT 'nope' FROM x"),
    )
    expect(insert).toBeInstanceOf(HubOnlyTableError)
    expect((insert as HubOnlyTableError).table).toBe("permission")

    const update = await errorOf(database.run("WITH x AS (SELECT 1) UPDATE permission SET id = 'nope'"))
    expect(update).toBeInstanceOf(HubOnlyTableError)

    const remove = await errorOf(database.run("WITH x AS (SELECT 1) DELETE FROM permission"))
    expect(remove).toBeInstanceOf(HubOnlyTableError)
  })

  test("multiple statements are rejected even when the first is a read", async () => {
    const filename = temporaryDatabase()
    migrated(filename)
    const database = await open(filename)

    const exec = (() => {
      try {
        database.connection.exec("SELECT 1; DROP TABLE message")
      } catch (cause) {
        return cause
      }
    })()
    expect(exec).toBeInstanceOf(WorkerStatementRejectedError)
    expect(database.connection.query("SELECT name FROM sqlite_master WHERE name = 'message'").all()).toEqual([
      { name: "message" },
    ])

    const all = (() => {
      try {
        database.connection.all("SELECT 1; DROP TABLE message")
      } catch (cause) {
        return cause
      }
    })()
    expect(all).toBeInstanceOf(WorkerStatementRejectedError)
  })

  test("pragmas are read-only and never writable_schema or user_version", async () => {
    const filename = temporaryDatabase()
    migrated(filename)
    const database = await open(filename)

    expect(() => database.connection.all("PRAGMA writable_schema")).toThrow(WorkerStatementRejectedError)
    expect(() => database.connection.all("PRAGMA user_version = 5")).toThrow(WorkerStatementRejectedError)
    expect(() => database.connection.pragma("writable_schema = 1")).toThrow(WorkerStatementRejectedError)
    expect(() => database.connection.pragma("user_version = 5")).toThrow(WorkerStatementRejectedError)
    expect(Number(database.connection.pragma("user_version"))).toBe(currentSchemaIdentity().version)
  })

  test("ordinary reads, writes, and a trailing semicolon still work", async () => {
    const filename = temporaryDatabase()
    migrated(filename)
    const database = await open(filename)

    expect(await Effect.runPromise(database.all("SELECT COUNT(*) AS count FROM session;"))).toEqual([{ count: 2 }])
    await Effect.runPromise(
      database.run(
        "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)",
        ["message-guard", "session-a", 1, 1, "{}"],
      ),
    )
    expect(await Effect.runPromise(database.all("SELECT id FROM message"))).toEqual([{ id: "message-guard" }])
  })
})
