// No public raw write surface for a bound worker.
//
// The typed repository is the only write path. This suite uses a real migrated
// SQLite file and the production `openWorkerDatabase({ sessionID })` boundary,
// then exercises every connection-shaped surface a caller can still reach. A
// raw scoped-table write must be rejected even when it names the worker's own
// session, and the row must not change.

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
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-no-raw-write-"))
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

interface ConnectionSurface {
  readonly all?: (sql: string, params?: ReadonlyArray<unknown>) => unknown
  readonly values?: (sql: string, params?: ReadonlyArray<unknown>) => unknown
  readonly run?: (sql: string, params?: ReadonlyArray<unknown>) => unknown
  readonly exec?: (sql: string) => unknown
  readonly pragma?: (name: string) => unknown
  readonly query?: (sql: string) => {
    readonly all?: (...params: ReadonlyArray<unknown>) => unknown
    readonly values?: (...params: ReadonlyArray<unknown>) => unknown
    readonly get?: (...params: ReadonlyArray<unknown>) => unknown
    readonly run?: (...params: ReadonlyArray<unknown>) => unknown
  }
}

interface DatabaseSurface extends ConnectionSurface {
  readonly connection?: ConnectionSurface
  readonly strictConnection?: ConnectionSurface
  readonly drizzleConnection?: ConnectionSurface
  readonly repository?: Record<string, unknown>
  readonly busConnection?: unknown
  readonly bus?: { create: (input: { ownerID: string }) => Record<string, unknown> }
}

interface WriteSurface {
  readonly name: string
  readonly write: (sql: string) => Promise<Outcome>
}

function writeSurfaces(database: WorkerDatabase): WriteSurface[] {
  const surface = database as unknown as DatabaseSurface
  const surfaces: WriteSurface[] = []
  if (typeof surface.run === "function") {
    surfaces.push({ name: "database.run", write: (sql) => settle(() => surface.run!(sql)) })
  }
  if (typeof surface.all === "function") {
    surfaces.push({ name: "database.all", write: (sql) => settle(() => surface.all!(sql)) })
  }
  for (const name of ["connection", "strictConnection", "drizzleConnection"] as const) {
    const connection = surface[name]
    if (connection === undefined) continue
    if (typeof connection.run === "function") {
      surfaces.push({ name: `${name}.run`, write: (sql) => settle(() => connection.run!(sql)) })
    }
    if (typeof connection.all === "function") {
      surfaces.push({ name: `${name}.all`, write: (sql) => settle(() => connection.all!(sql)) })
    }
    if (typeof connection.values === "function") {
      surfaces.push({ name: `${name}.values`, write: (sql) => settle(() => connection.values!(sql)) })
    }
    if (typeof connection.exec === "function") {
      surfaces.push({ name: `${name}.exec`, write: (sql) => settle(() => connection.exec!(sql)) })
    }
    if (typeof connection.query === "function") {
      for (const mode of ["run", "all", "values", "get"] as const) {
        surfaces.push({
          name: `${name}.query().${mode}`,
          write: (sql) =>
            settle(() => {
              const statement = connection.query!(sql)
              const invoke = statement[mode]
              return invoke === undefined ? undefined : invoke()
            }),
        })
      }
    }
  }
  return surfaces
}

function readSurfaces(database: WorkerDatabase): WriteSurface[] {
  const surface = database as unknown as DatabaseSurface
  const surfaces: WriteSurface[] = []
  if (typeof surface.all === "function") {
    surfaces.push({ name: "database.all", write: (sql) => settle(() => surface.all!(sql)) })
  }
  for (const name of ["connection", "strictConnection", "drizzleConnection"] as const) {
    const connection = surface[name]
    if (connection === undefined) continue
    if (typeof connection.all === "function") {
      surfaces.push({ name: `${name}.all`, write: (sql) => settle(() => connection.all!(sql)) })
    }
    if (typeof connection.query === "function") {
      surfaces.push({
        name: `${name}.query().all`,
        write: (sql) =>
          settle(() => {
            const statement = connection.query!(sql)
            return statement.all === undefined ? undefined : statement.all()
          }),
      })
    }
  }
  return surfaces
}

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("bound worker has no public raw write API", () => {
  test("WorkerDatabase.run, repository SQL methods, and raw Bus handles are gone", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const surface = database as unknown as DatabaseSurface

    expect(surface.run, "WorkerDatabase.run is a raw write surface").toBeUndefined()
    expect(surface.busConnection, "WorkerDatabase.busConnection is a trusted raw handle").toBeUndefined()

    const repository = surface.repository
    expect(repository, "repository is the write API").toBeDefined()
    for (const forbidden of ["run", "exec", "all", "query", "prepare", "sql", "connection", "transaction"]) {
      expect(repository![forbidden], `repository.${forbidden} must not be public`).toBeUndefined()
    }
    for (const method of ["insert", "update", "delete", "upsert", "insertMany"]) {
      expect(typeof repository![method], `repository.${method}`).toBe("function")
    }

    const handle = surface.bus!.create({ ownerID: "no-raw-write" })
    expect(handle.connection).toBeUndefined()
    expect(handle.transaction).toBeUndefined()
    expect(handle.transactionAsync).toBeUndefined()
  })

  test("every connection surface rejects an own-session raw write", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const before = adminAll(filename, "SELECT id, data FROM message ORDER BY id")

    const hostile = [
      "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES ('raw-own', 'session-a', 1, 1, '{}')",
      "UPDATE message SET data = '{\"raw\":true}' WHERE id = 'message-session-a'",
      "DELETE FROM message WHERE id = 'message-session-a'",
    ]
    const surfaces = writeSurfaces(database)
    for (const sql of hostile) {
      for (const surface of surfaces) {
        const outcome = await surface.write(sql)
        expect(outcome.ok, `${sql.slice(0, 24)} was accepted by ${surface.name}`).toBe(false)
      }
    }

    expect(adminAll(filename, "SELECT id, data FROM message ORDER BY id")).toEqual(before)
  })

  test("the typed repository is the only successful write path", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)

    const outcome = await settle(() =>
      (
        database as unknown as {
          repository: {
            insert: (input: unknown) => unknown
          }
        }
      ).repository.insert({
        table: "message",
        values: { id: "typed-only", session_id: "session-a", time_created: 2, time_updated: 2, data: "{}" },
      }),
    )
    expect(outcome.ok, `typed insert failed: ${String(outcome.error)}`).toBe(true)
    expect(adminAll(filename, "SELECT id FROM message WHERE id = 'typed-only'")).toEqual([{ id: "typed-only" }])
  })

  test("DDL, ATTACH, writable pragmas, and direct main scoped reads are denied", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const surface = database as unknown as DatabaseSurface
    const connection = surface.connection!

    const ddl = await settle(() => connection.exec!("CREATE TABLE worker_owned (id text primary key)"))
    expect(ddl.ok).toBe(false)

    const attach = await settle(() => connection.exec!("ATTACH DATABASE ':memory:' AS escaped"))
    expect(attach.ok).toBe(false)

    const pragma = await settle(() => connection.pragma!("writable_schema = 1"))
    expect(pragma.ok).toBe(false)

    for (const sql of [
      "SELECT id FROM main.message",
      "SELECT id FROM main.credential",
      "SELECT value FROM credential",
    ]) {
      for (const read of readSurfaces(database)) {
        const outcome = await read.write(sql)
        expect(outcome.ok, `${sql} was readable through ${read.name}`).toBe(false)
        expect(errorTag(outcome.error), `${sql} through ${read.name}`).toBeDefined()
      }
    }

    expect(adminAll(filename, "SELECT name FROM sqlite_master WHERE name = 'worker_owned'")).toEqual([])
  })
})
