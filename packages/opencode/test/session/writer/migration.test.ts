// Fix C — writer migration statement coverage.
//
// The writer isolate owns the sole SQLite connection but receives no schema of
// its own: production migrates the app database in `packages/core`. The generated
// `migrationStatements` list is what the hub hands to `writer.migrate(...)` so a
// fresh writer database reaches the full application schema.

import { describe, expect, test } from "bun:test"
import { createBunSqliteConnection, type WriterConnection } from "@/session/writer/connection"
import { createWriterCore } from "@/session/writer/core"
import { migrationStatements } from "@/session/writer/migration.statements.gen"
import { ev } from "./fixtures"

const APPLICATION_TABLES: ReadonlyArray<string> = [
  "event",
  "event_sequence",
  "session",
  "message",
  "part",
  "session_message",
  "session_input",
  "project",
  "workspace",
]

function freshCore(): { connection: WriterConnection; core: ReturnType<typeof createWriterCore> } {
  const connection = createBunSqliteConnection({ filename: ":memory:" })
  const core = createWriterCore({ connection, sleep: async () => {} })
  return { connection, core }
}

function tableNames(connection: WriterConnection): ReadonlyArray<string> {
  return connection
    .all("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
    .map((row) => String(row.name))
}

describe("writer migration statements (Fix C)", () => {
  test("migrationStatements is a frozen, non-empty list of SQL statements", () => {
    expect(Object.isFrozen(migrationStatements)).toBe(true)
    expect(migrationStatements.length).toBeGreaterThan(0)
    for (const statement of migrationStatements) {
      expect(typeof statement).toBe("string")
      expect(statement.trim().length).toBeGreaterThan(0)
    }
  })

  test("migrating a fresh writer database yields the full application schema", async () => {
    const { connection, core } = freshCore()
    await core.migrate(migrationStatements)
    const names = tableNames(connection)

    for (const table of APPLICATION_TABLES) expect(names).toContain(table)
    await core.close()
  })

  test("the migration journal records the core migrations as applied", async () => {
    const { connection, core } = freshCore()
    await core.migrate(migrationStatements)

    const journal = connection.all("SELECT id FROM migration ORDER BY id")
    expect(journal.length).toBeGreaterThan(0)
    await core.close()
  })

  test("applying the migration twice is a safe no-op", async () => {
    const { connection, core } = freshCore()
    await core.migrate(migrationStatements)
    const before = tableNames(connection)

    await core.migrate(migrationStatements)

    expect(tableNames(connection)).toEqual(before)
    await core.close()
  })

  test("migrating a database the main process already migrated is a safe no-op", async () => {
    const { connection, core } = freshCore()
    // The main process migrates the app database before the writer isolate opens it.
    for (const statement of migrationStatements) connection.exec(statement)

    await core.migrate(migrationStatements)

    for (const table of APPLICATION_TABLES) expect(tableNames(connection)).toContain(table)
    await core.close()
  })

  test("re-applying the migration does not duplicate the journal", async () => {
    const { connection, core } = freshCore()
    await core.migrate(migrationStatements)
    const seeded = connection.all("SELECT id FROM migration").length

    await core.migrate(migrationStatements)

    expect(connection.all("SELECT id FROM migration").length).toBe(seeded)
    await core.close()
  })

  test("a durable publish round-trips after a re-applied migration", async () => {
    const { connection, core } = freshCore()
    await core.migrate(migrationStatements)
    await core.migrate(migrationStatements)

    const result = await core.publish({ event: ev("t", "a"), durable: true })
    expect(result).toMatchObject({ seq: 0, committed: true })
    await core.close()
  })

  test("a durable publish round-trips against the migrated schema", async () => {
    const { connection, core } = freshCore()
    await core.migrate(migrationStatements)

    const result = await core.publish({ event: ev("t", "a"), durable: true })
    expect(result).toMatchObject({ seq: 0, committed: true })
    expect(connection.all("SELECT seq FROM event_sequence WHERE aggregate_id = 'a'")).toEqual([{ seq: 0 }])
    expect(connection.all("SELECT seq FROM event WHERE aggregate_id = 'a'")).toEqual([{ seq: 0 }])
    await core.close()
  })
})
