import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { Database as BunDatabase } from "bun:sqlite"
import { afterEach, describe, expect, test } from "bun:test"
import { Effect } from "effect"
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
