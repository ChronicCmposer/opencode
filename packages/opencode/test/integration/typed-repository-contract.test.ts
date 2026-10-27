// Typed-repository public contract.
//
// The positive half of the matrix: a REAL bound worker writes only through the
// typed methods, the bound session is implicit, batches are real, and the raw
// SQL write surface is gone. The negative security cases live in
// `typed-repository-security.test.ts`; together they prevent a vacuous
// "everything throws" implementation.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { openConnection } from "@opencode-ai/core/database/connection"
import { markSchemaIdentity } from "@/session/db/schema"
import { openWorkerDatabase, type WorkerDatabase } from "@/session/worker/db"
import { migrationStatements } from "@/session/writer/migration.statements.gen"

const directories: string[] = []
const cleanups: Array<() => void | Promise<void>> = []

function workspace(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-typed-contract-"))
  directories.push(directory)
  return directory
}

function migratedDatabase(): string {
  const filename = path.join(workspace(), "opencode.db")
  const connection = openConnection({ filename })
  try {
    for (const statement of migrationStatements) connection.exec(statement)
    markSchemaIdentity(connection)
  } finally {
    connection.close()
  }
  return filename
}

function seedWorld(filename: string): void {
  const connection = openConnection({ filename })
  try {
    connection.run(
      "INSERT INTO project (id, name, worktree, sandboxes, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
      ["project-scope", "scope", "/tmp/scope", "[]", 1, 1],
    )
    for (const id of ["session-a", "session-b"]) {
      connection.run(
        "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [id, "project-scope", id, "/tmp/scope", id, "1", 1, 1],
      )
      connection.run("INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)", [
        `message-${id}`,
        id,
        1,
        1,
        "{}",
      ])
    }
  } finally {
    connection.close()
  }
}

async function openBound(filename: string): Promise<WorkerDatabase> {
  const database = await Effect.runPromise(openWorkerDatabase({ filename, sessionID: "session-a" }))
  cleanups.push(() => Effect.runPromise(database.close))
  return database
}

function adminAll<Row = Record<string, unknown>>(
  filename: string,
  sql: string,
  params?: ReadonlyArray<unknown>,
): ReadonlyArray<Row> {
  const connection = openConnection({ filename })
  try {
    return connection.all<Row>(sql, params as never)
  } finally {
    connection.close()
  }
}

interface RepositorySurface {
  readonly insert?: (input: unknown) => unknown
  readonly upsert?: (input: unknown) => unknown
  readonly insertMany?: (input: unknown) => unknown
  readonly update?: (table: string, values: Record<string, unknown>, where: Record<string, unknown>) => unknown
  readonly delete?: (table: string, where: Record<string, unknown>) => unknown
  readonly select?: (table: string, where?: Record<string, unknown>) => unknown
  readonly [key: string]: unknown
}

function repositoryOf(database: WorkerDatabase): RepositorySurface {
  const repository = (database as unknown as { repository?: RepositorySurface }).repository
  expect(repository, "WorkerDatabase.repository is the typed write API").toBeDefined()
  return repository!
}

interface Outcome {
  readonly ok: boolean
  readonly value?: unknown
  readonly error?: unknown
}

async function settle(operation: () => unknown): Promise<Outcome> {
  try {
    const value = operation()
    if (Effect.isEffect(value)) {
      try {
        return { ok: true, value: await Effect.runPromise(value as Effect.Effect<unknown, unknown, never>) }
      } catch (error) {
        return { ok: false, error }
      }
    }
    return { ok: true, value }
  } catch (error) {
    return { ok: false, error }
  }
}

function errorTag(error: unknown): string | undefined {
  if (error !== null && typeof error === "object" && "_tag" in error) {
    return String((error as { _tag: unknown })._tag)
  }
  return undefined
}

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("typed repository public surface", () => {
  test("only typed methods are public; no raw SQL or trusted handle is reachable", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const repository = repositoryOf(database)
    const surface = database as unknown as Record<string, unknown>

    for (const method of ["insert", "update", "delete", "upsert", "insertMany"] as const) {
      expect(typeof repository[method], `repository.${method}`).toBe("function")
    }
    for (const forbidden of ["run", "exec", "all", "query", "prepare", "sql", "connection", "raw", "transaction"]) {
      expect(repository[forbidden], `repository.${forbidden} must not be public`).toBeUndefined()
    }

    // The worker database has no public raw write surface and no raw trusted
    // Bus handle. Reads stay available through the live views.
    expect(surface.run, "WorkerDatabase.run must be gone").toBeUndefined()
    expect(surface.busConnection, "WorkerDatabase.busConnection must be gone").toBeUndefined()

    const bus = (
      database as unknown as {
        bus: { create: (input: { ownerID: string }) => Record<string, unknown> }
      }
    ).bus
    const handle = bus.create({ ownerID: "typed-contract" })
    expect(handle.connection).toBeUndefined()
    expect(handle.transaction).toBeUndefined()
    expect(handle.transactionAsync).toBeUndefined()
  })
})

describe("typed repository writes through the real bound worker", () => {
  test("insert binds the session explicitly and implicitly", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const repository = repositoryOf(database)

    const explicit = await settle(() =>
      repository.insert!({
        table: "message",
        values: { id: "typed-explicit", session_id: "session-a", time_created: 2, time_updated: 2, data: "{}" },
      }),
    )
    expect(explicit.ok, `explicit insert failed: ${String(explicit.error)}`).toBe(true)

    // The repository owns the bound scope column; a caller may omit it.
    const implicit = await settle(() =>
      repository.insert!({
        table: "message",
        values: { id: "typed-implicit", time_created: 3, time_updated: 3, data: "{}" },
      }),
    )
    expect(implicit.ok, `implicit insert failed: ${String(implicit.error)}`).toBe(true)

    expect(
      adminAll<{ id: string; session_id: string }>(filename, "SELECT id, session_id FROM message ORDER BY id"),
    ).toEqual([
      { id: "message-session-a", session_id: "session-a" },
      { id: "message-session-b", session_id: "session-b" },
      { id: "typed-explicit", session_id: "session-a" },
      { id: "typed-implicit", session_id: "session-a" },
    ])
  })

  test("insertMany writes an atomic batch of own-session rows", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const repository = repositoryOf(database)

    const outcome = await settle(() =>
      repository.insertMany!({
        table: "message",
        values: [
          { id: "batch-one", session_id: "session-a", time_created: 2, time_updated: 2, data: "{}" },
          { id: "batch-two", time_created: 3, time_updated: 3, data: "{}" },
        ],
      }),
    )
    expect(outcome.ok, `insertMany failed: ${String(outcome.error)}`).toBe(true)
    expect(
      adminAll<{ id: string; session_id: string }>(
        filename,
        "SELECT id, session_id FROM message WHERE id LIKE 'batch-%' ORDER BY id",
      ),
    ).toEqual([
      { id: "batch-one", session_id: "session-a" },
      { id: "batch-two", session_id: "session-a" },
    ])
  })

  test("update, upsert, and delete stay inside the bound session", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const repository = repositoryOf(database)

    const update = await settle(() =>
      repository.update!("message", { data: JSON.stringify({ updated: true }) }, { id: "message-session-a" }),
    )
    expect(update.ok, `update failed: ${String(update.error)}`).toBe(true)

    const upsert = await settle(() =>
      repository.upsert!({
        table: "message",
        values: {
          id: "message-session-a",
          session_id: "session-a",
          time_created: 1,
          time_updated: 4,
          data: JSON.stringify({ upserted: true }),
        },
        onConflict: { columns: ["id"], update: ["data", "time_updated"] },
      }),
    )
    expect(upsert.ok, `upsert failed: ${String(upsert.error)}`).toBe(true)

    const inserted = await settle(() =>
      repository.insert!({
        table: "message",
        values: { id: "typed-delete", session_id: "session-a", time_created: 5, time_updated: 5, data: "{}" },
      }),
    )
    expect(inserted.ok, `insert failed: ${String(inserted.error)}`).toBe(true)
    const deleted = await settle(() => repository.delete!("message", { id: "typed-delete" }))
    expect(deleted.ok, `delete failed: ${String(deleted.error)}`).toBe(true)

    expect(adminAll<{ id: string; data: string }>(filename, "SELECT id, data FROM message ORDER BY id")).toEqual([
      { id: "message-session-a", data: JSON.stringify({ upserted: true }) },
      { id: "message-session-b", data: "{}" },
    ])
  })

  test("a typed FK insert accepts only a parent in the bound session", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const repository = repositoryOf(database)

    const own = await settle(() =>
      repository.insert!({
        table: "part",
        values: {
          id: "typed-part-own",
          message_id: "message-session-a",
          session_id: "session-a",
          time_created: 1,
          time_updated: 1,
          data: "{}",
        },
      }),
    )
    expect(own.ok, `own FK insert failed: ${String(own.error)}`).toBe(true)
    expect(adminAll(filename, "SELECT id FROM part ORDER BY id")).toEqual([{ id: "typed-part-own" }])
  })

  test("typed reads are bound to the worker's session", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const repository = repositoryOf(database)

    const rows = repository.select!("message") as ReadonlyArray<{ id: string }>
    expect(rows.map((row) => row.id)).toEqual(["message-session-a"])

    const foreign = await settle(() => repository.select!("message", { session_id: "session-b" }))
    expect(foreign.ok).toBe(false)
    expect(errorTag(foreign.error)).toContain("ScopeViolationError")
  })

  test("unknown columns fail with a typed error rather than reaching SQLite", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const repository = repositoryOf(database)

    const outcome = await settle(() =>
      repository.insert!({
        table: "message",
        values: { id: "unknown-column", session_id: "session-a", not_a_column: 1 },
      }),
    )
    expect(outcome.ok).toBe(false)
    expect(errorTag(outcome.error)).toContain("UnknownColumnError")
    expect(adminAll(filename, "SELECT id FROM message WHERE id = 'unknown-column'")).toEqual([])
  })
})
