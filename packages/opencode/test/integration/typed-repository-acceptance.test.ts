// Typed-repository whole-boundary acceptance.
//
// This is the end-state contract, independent of the adversarial suites:
//
//   * a REAL migrated SQLite file + REAL bound worker (`openWorkerDatabase`);
//   * the only successful write path is the typed `WorkerRepository`;
//   * `WorkerDatabase.run`, `busConnection`, and every raw Bus handle are gone;
//   * values are bound parameters, never SQL text (an injection-shaped value is
//     stored as data and cannot delete or rewrite another row);
//   * the old SQL-text scope analyzer and TEMP mirror tables are gone, while the
//     live filtered views still see hub writes made after the worker opened and
//     still hide every foreign row.
//
// The adversarial shapes themselves (expression tails, upsert/REPLACE, row-value
// SET, FK CAST, secret reads) live in `typed-repository-security-adversarial.test.ts`.

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
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-typed-final-"))
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
      ["project-final", "final", "/tmp/final", "[]", 1, 1],
    )
    for (const id of ["session-a", "session-b"]) {
      connection.run(
        "INSERT INTO session (id, project_id, slug, directory, title, version, metadata, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [id, "project-final", id, "/tmp/final", id, "1", JSON.stringify({ depth: 1 }), 1, 1],
      )
      connection.run("INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)", [
        `message-${id}`,
        id,
        1,
        1,
        JSON.stringify({ owner: id }),
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

function admin<Row = Record<string, unknown>>(
  filename: string,
  sql: string,
  params: ReadonlyArray<unknown> = [],
): ReadonlyArray<Row> {
  const connection = openConnection({ filename })
  try {
    return connection.all<Row>(sql, params as never)
  } finally {
    connection.close()
  }
}

function adminWrite(filename: string, sql: string, params: ReadonlyArray<unknown> = []): void {
  const connection = openConnection({ filename })
  try {
    connection.run(sql, params as never)
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
  readonly query?: (sql: string) => {
    readonly all?: (...params: ReadonlyArray<unknown>) => unknown
    readonly values?: (...params: ReadonlyArray<unknown>) => unknown
    readonly get?: (...params: ReadonlyArray<unknown>) => unknown
    readonly run?: (...params: ReadonlyArray<unknown>) => unknown
  }
}

interface DatabaseSurface {
  readonly run?: (sql: string, params?: ReadonlyArray<unknown>) => unknown
  readonly all?: (sql: string, params?: ReadonlyArray<unknown>) => unknown
  readonly connection?: ConnectionSurface
  readonly strictConnection?: ConnectionSurface
  readonly drizzleConnection?: ConnectionSurface
  readonly repository?: Record<string, unknown>
  readonly busConnection?: unknown
  readonly bus?: {
    readonly connection?: unknown
    readonly transaction?: unknown
    readonly transactionAsync?: unknown
    readonly create: (input: { readonly ownerID: string }) => Record<string, unknown>
  }
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

const SOURCE = new URL("../../src/session/worker/db.ts", import.meta.url)

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("the public boundary is typed-only", () => {
  test("no raw write entry point, no raw Bus handle, and the typed methods are real", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const surface = database as unknown as DatabaseSurface

    expect(surface.run, "bound WorkerDatabase.run is a raw write API").toBeUndefined()
    expect(surface.busConnection, "WorkerDatabase.busConnection is a trusted raw handle").toBeUndefined()
    for (const forbidden of ["exec", "query", "prepare", "sql"]) {
      expect((surface as unknown as Record<string, unknown>)[forbidden], `WorkerDatabase.${forbidden}`).toBeUndefined()
    }

    const bus = surface.bus!
    expect(bus, "WorkerDatabase.bus capability").toBeDefined()
    expect(bus.connection, "the default Bus handle must not expose a connection").toBeUndefined()
    expect(bus.transaction, "the default Bus handle must not expose transaction").toBeUndefined()
    expect(bus.transactionAsync, "the default Bus handle must not expose transactionAsync").toBeUndefined()

    const handle = bus.create({ ownerID: "final-surface" })
    expect(handle.connection).toBeUndefined()
    expect(handle.transaction).toBeUndefined()
    expect(handle.transactionAsync).toBeUndefined()

    const repository = surface.repository!
    expect(repository, "WorkerDatabase.repository is the write API").toBeDefined()
    for (const method of ["insert", "insertMany", "upsert", "update", "delete", "select", "atomic"]) {
      expect(typeof repository[method], `repository.${method}`).toBe("function")
    }
    for (const forbidden of [
      "run",
      "exec",
      "all",
      "values",
      "query",
      "prepare",
      "sql",
      "connection",
      "raw",
      "transaction",
    ]) {
      expect(repository[forbidden], `repository.${forbidden} must not be public`).toBeUndefined()
    }

    // Not vacuous: the typed path still writes an own-session row.
    const insert = await settle(() =>
      (
        repository as unknown as {
          insert: (input: unknown) => unknown
        }
      ).insert({
        table: "message",
        values: { id: "final-typed", session_id: "session-a", time_created: 2, time_updated: 2, data: '{"ok":true}' },
      }),
    )
    expect(insert.ok, `typed insert failed: ${String(insert.error)}`).toBe(true)
    expect(admin(filename, "SELECT id, session_id FROM message WHERE id = 'final-typed'")).toEqual([
      { id: "final-typed", session_id: "session-a" },
    ])
  })

  test("every remaining raw write surface still rejects an own-session write", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const before = admin(filename, "SELECT id, data FROM message ORDER BY id")

    const surfaces = writeSurfaces(database)
    expect(surfaces.length, "a bound worker still has a deny-by-contract surface inventory").toBeGreaterThan(0)
    const hostile = [
      "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES ('final-raw', 'session-a', 1, 1, '{}')",
      "UPDATE message SET data = '{\"raw\":true}' WHERE id = 'message-session-a'",
      "DELETE FROM message WHERE id = 'message-session-a'",
    ]
    for (const sql of hostile) {
      for (const surface of surfaces) {
        const outcome = await surface.write(sql)
        expect(outcome.ok, `${sql.slice(0, 32)} was accepted by ${surface.name}`).toBe(false)
        expect(outcome.error, `${sql.slice(0, 32)} returned without a throw on ${surface.name}`).toBeDefined()
      }
    }

    expect(admin(filename, "SELECT id, data FROM message ORDER BY id")).toEqual(before)
  })
})

describe("values are bound parameters, never SQL text", () => {
  test("an injection-shaped value is stored as data and cannot delete or rewrite rows", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)

    const payload = "' WHERE 1 = 1; DELETE FROM message; DROP TABLE message; --"
    const outcome = await settle(() =>
      database.repository.update("message", { data: payload }, { id: "message-session-a" }),
    )
    expect(outcome.ok, `typed update failed: ${String(outcome.error)}`).toBe(true)

    expect(admin(filename, "SELECT data FROM message WHERE id = 'message-session-a'")).toEqual([{ data: payload }])
    expect(admin<{ count: number }>(filename, "SELECT COUNT(*) AS count FROM message")).toEqual([{ count: 2 }])
    expect(
      admin<{ count: number }>(filename, "SELECT COUNT(*) AS count FROM sqlite_master WHERE name = 'message'"),
    ).toEqual([{ count: 1 }])
  })

  test("a table name is not interpreted as SQL and a SQL-shaped column is rejected", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)

    const table = await settle(() => database.repository.select("message WHERE 1 = 1; DROP TABLE message" as never))
    expect(table.ok).toBe(false)
    expect(errorTag(table.error)).toContain("UnsupportedWriteError")

    const column = await settle(() =>
      database.repository.update("message", { "id = 'x' OR 1 = 1 --": "hijacked" } as never, {
        id: "message-session-a",
      }),
    )
    expect(column.ok).toBe(false)
    expect(errorTag(column.error)).toContain("UnknownColumnError")

    expect(admin<{ count: number }>(filename, "SELECT COUNT(*) AS count FROM message")).toEqual([{ count: 2 }])
  })
})

describe("live views replace parser/TEMP mirrors", () => {
  test("scoped tables are TEMP VIEWs, not TEMP mirror tables or triggers", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)

    const objects = await Effect.runPromise(
      database.all<{ readonly type: string; readonly name: string }>(
        "SELECT type, name FROM temp.sqlite_master WHERE type IN ('table', 'view') AND name IN ('session', 'message', 'part', 'session_input', 'tool_call', 'event', 'event_sequence', 'event_blob') ORDER BY name",
      ),
    )
    expect(objects.length, "the bound worker has TEMP objects for its scoped tables").toBeGreaterThan(0)
    expect(
      objects.every((object) => object.type === "view"),
      JSON.stringify(objects),
    ).toBe(true)
  })

  test("a post-open hub write is visible through the live view; a foreign row never is", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)

    // Written by another process after the worker opened its views.
    adminWrite(
      filename,
      "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)",
      ["message-live-own", "session-a", 2, 2, '{"live":true}'],
    )
    adminWrite(
      filename,
      "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)",
      ["message-live-foreign", "session-b", 2, 2, '{"live":true}'],
    )

    const view = await Effect.runPromise(database.all<{ readonly id: string }>("SELECT id FROM message ORDER BY id"))
    expect(view.map((row) => row.id)).toEqual(["message-live-own", "message-session-a"])

    const typed = database.repository.select("message")
    expect(typed.map((row) => row.id).sort()).toEqual(["message-live-own", "message-session-a"])

    // The rows really exist for the hub; the worker view is authority, not absence.
    expect(
      admin<{ id: string }>(filename, "SELECT id FROM message WHERE id LIKE 'message-live-%' ORDER BY id"),
    ).toEqual([{ id: "message-live-foreign" }, { id: "message-live-own" }])
  })

  test("the old SQL-text scope analyzer and mirror/trigger machinery are removed", () => {
    const source = fs.readFileSync(SOURCE, "utf8")
    expect(source).toContain("CREATE TEMP VIEW")
    expect(source).not.toContain("CREATE TEMP TABLE")
    expect(source).not.toContain("CREATE TRIGGER")
    expect(source).not.toMatch(/\bINSTEAD OF\b/)
    expect(source).not.toMatch(/\bassertSessionScope\b/)
    expect(source).not.toMatch(/\bscopePredicateSafe\b/)
    expect(source).not.toMatch(/TEMP mirror/i)
  })
})
