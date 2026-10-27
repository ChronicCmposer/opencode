// Typed-repository adversarial matrix.
//
// Every case runs against a REAL migrated SQLite file and a REAL bound worker
// (`openWorkerDatabase({ sessionID })`) whose authorizer/live views are the
// production ones. A hub-owned admin connection takes before/after snapshots so
// a rejected statement is proven to have had no effect, not merely to have
// returned an empty result.
//
// The contract under test:
// - the typed repository is the only write path; every raw SQL write surface is
//   denied even for the worker's own session;
// - typed methods reject immutable columns, foreign scope values, foreign FK
//   owners, hub-only tables, unknown columns, and hub-owned `metadata.depth`;
// - a rejected batch is atomic (no partial rows);
// - secret hub tables are unreadable through every read surface.

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
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-typed-security-"))
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
        "INSERT INTO session (id, project_id, slug, directory, title, version, metadata, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [id, "project-scope", id, "/tmp/scope", id, "1", JSON.stringify({ depth: 1 }), 1, 1],
      )
      connection.run("INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)", [
        `message-${id}`,
        id,
        1,
        1,
        JSON.stringify({ text: id }),
      ])
      connection.run(
        "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?)",
        [`part-${id}`, `message-${id}`, id, 1, 1, "{}"],
      )
      connection.run("INSERT INTO event_sequence (aggregate_id, seq) VALUES (?, 0)", [id])
      connection.run("INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?, ?, 0, ?, ?)", [
        `event-${id}`,
        id,
        "test.typed.security",
        "{}",
      ])
    }
    connection.run(
      "INSERT INTO permission (id, project_id, action, resource, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
      ["permission-hub", "project-scope", "allow", "*", 1, 1],
    )
    connection.run("INSERT INTO credential (id, label, value, time_created, time_updated) VALUES (?, ?, ?, ?, ?)", [
      "credential-hub",
      "hub secret",
      "super-secret",
      1,
      1,
    ])
    connection.run(
      "INSERT INTO account (id, email, url, access_token, refresh_token, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?)",
      ["account-hub", "hub@example.test", "https://example.test", "access-secret", "refresh-secret", 1, 1],
    )
    connection.run(
      "INSERT INTO control_account (email, url, access_token, refresh_token, active, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?)",
      ["control@example.test", "https://control.example.test", "control-access", "control-refresh", 1, 1, 1],
    )
    connection.run("INSERT INTO account_state (id, active_account_id) VALUES (?, ?)", [1, "account-hub"])
  } finally {
    connection.close()
  }
}

async function openBound(filename: string, sessionID = "session-a"): Promise<WorkerDatabase> {
  const database = await Effect.runPromise(openWorkerDatabase({ filename, sessionID }))
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

function snapshot(filename: string) {
  return {
    session: adminAll(
      filename,
      "SELECT id, project_id, parent_id, slug, directory, title, metadata FROM session ORDER BY id",
    ),
    message: adminAll(filename, "SELECT id, session_id, data FROM message ORDER BY id"),
    part: adminAll(filename, "SELECT id, message_id, session_id, data FROM part ORDER BY id"),
    event: adminAll(filename, "SELECT id, aggregate_id, seq, data FROM event ORDER BY id"),
    event_sequence: adminAll(filename, "SELECT aggregate_id, seq FROM event_sequence ORDER BY aggregate_id"),
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

/**
 * Require one of the named typed errors. The exact tag namespace is the
 * repository's choice; the class identity is the contract.
 */
function expectTyped(error: unknown, names: readonly string[]): void {
  const tag = errorTag(error)
  expect(tag, `expected a typed error (${names.join(" | ")}), got ${tag ?? String(error)}`).toBeDefined()
  expect(
    names.some((name) => tag!.includes(name)),
    `expected ${names.join(" | ")}, got ${tag}`,
  ).toBe(true)
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
  readonly repository?: RepositorySurface
  readonly busConnection?: unknown
}

interface RepositorySurface {
  readonly insert?: (input: unknown) => unknown
  readonly upsert?: (input: unknown) => unknown
  readonly insertMany?: (input: unknown) => unknown
  readonly update?: (table: string, values: Record<string, unknown>, where: Record<string, unknown>) => unknown
  readonly delete?: (table: string, where: Record<string, unknown>) => unknown
  readonly select?: (table: string, where?: Record<string, unknown>) => unknown
  readonly run?: unknown
  readonly exec?: unknown
  readonly all?: unknown
  readonly query?: unknown
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

interface ReadSurface {
  readonly name: string
  readonly read: (sql: string) => Promise<Outcome>
}

function readSurfaces(database: WorkerDatabase): ReadSurface[] {
  const surface = database as unknown as DatabaseSurface
  const surfaces: ReadSurface[] = []
  if (typeof surface.all === "function") {
    surfaces.push({ name: "database.all", read: (sql) => settle(() => surface.all!(sql)) })
  }
  for (const name of ["connection", "strictConnection", "drizzleConnection"] as const) {
    const connection = surface[name]
    if (connection === undefined) continue
    if (typeof connection.all === "function") {
      surfaces.push({ name: `${name}.all`, read: (sql) => settle(() => connection.all!(sql)) })
    }
    if (typeof connection.query === "function") {
      surfaces.push({
        name: `${name}.query().all`,
        read: (sql) =>
          settle(() => {
            const statement = connection.query!(sql)
            return statement.all === undefined ? undefined : statement.all()
          }),
      })
    }
  }
  return surfaces
}

function repositoryOf(database: WorkerDatabase): RepositorySurface {
  const repository = (database as unknown as DatabaseSurface).repository
  expect(repository, "WorkerDatabase.repository is the typed write API").toBeDefined()
  return repository!
}

/**
 * Every raw write surface that still exists must reject the statement. A target
 * design with no raw surface at all passes trivially; a surface that silently
 * accepts is a failure.
 */
async function expectRawDeniedEverywhere(database: WorkerDatabase, label: string, sql: string): Promise<void> {
  for (const surface of writeSurfaces(database)) {
    const outcome = await surface.write(sql)
    expect(outcome.ok, `${label} was accepted by ${surface.name}`).toBe(false)
    expect(outcome.error, `${label} returned without a throw on ${surface.name}`).toBeDefined()
  }
}

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("raw SQL cannot escape the typed-repository boundary", () => {
  test("an immutable-column expression tail cannot rewrite session identity or directory", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const before = snapshot(filename)

    await expectRawDeniedEverywhere(
      database,
      "immutable expression tail",
      "UPDATE session SET directory = '/tmp/scope' || '/../../etc' WHERE id = 'session-a'",
    )
    await expectRawDeniedEverywhere(
      database,
      "immutable slug expression",
      "UPDATE session SET slug = 'a' || '-stolen' WHERE id = 'session-a'",
    )
    await expectRawDeniedEverywhere(
      database,
      "immutable parent expression",
      "UPDATE session SET parent_id = CAST('session-b' AS TEXT) WHERE id = 'session-a'",
    )

    expect(snapshot(filename)).toEqual(before)
  })

  test("INSERT ... ON CONFLICT DO UPDATE and INSERT OR REPLACE cannot take over a session row", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const before = snapshot(filename)

    const hostile = [
      "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES ('session-b', 'project-scope', 'b', '/tmp/scope', 'hijacked', '1', 1, 1) ON CONFLICT(id) DO UPDATE SET title = 'hijacked', directory = '/tmp/scope/../../etc'",
      "INSERT OR REPLACE INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES ('session-b', 'project-scope', 'b', '/tmp/scope', 'hijacked', '1', 1, 1)",
      "INSERT OR REPLACE INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES ('session-a', 'project-scope', 'a', '/tmp/scope/../../etc', 'replaced', '1', 1, 1)",
      "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES ('session-a', 'project-scope', 'a', '/tmp/scope', 'merged', '1', 1, 1) ON CONFLICT(id) DO UPDATE SET directory = excluded.directory, title = excluded.title",
    ]
    for (const sql of hostile) await expectRawDeniedEverywhere(database, sql.slice(0, 40), sql)

    expect(snapshot(filename)).toEqual(before)
    expect(adminAll<{ title: string }>(filename, "SELECT title FROM session WHERE id = 'session-b'")).toEqual([
      { title: "session-b" },
    ])
  })

  test("row-value SET cannot move message, event, or event_sequence rows across sessions", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const before = snapshot(filename)

    await expectRawDeniedEverywhere(
      database,
      "message row value",
      "UPDATE message SET (session_id, data) = ('session-b', '{\"hijacked\":true}') WHERE id = 'message-session-a'",
    )
    await expectRawDeniedEverywhere(
      database,
      "event row value",
      "UPDATE event SET (aggregate_id, data) = ('session-b', '{\"hijacked\":true}') WHERE id = 'event-session-a'",
    )
    await expectRawDeniedEverywhere(
      database,
      "event_sequence row value",
      "UPDATE event_sequence SET (aggregate_id, seq) = ('session-b', 99) WHERE aggregate_id = 'session-a'",
    )

    expect(snapshot(filename)).toEqual(before)
  })

  test("CAST(... AS TEXT) FK expressions cannot inject a foreign transcript row", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const before = snapshot(filename)

    await expectRawDeniedEverywhere(
      database,
      "message FK cast",
      "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES ('injected-message', CAST('session-b' AS TEXT), 1, 1, '{}')",
    )
    await expectRawDeniedEverywhere(
      database,
      "part FK cast",
      "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES ('injected-part', CAST('message-session-b' AS TEXT), 'session-a', 1, 1, '{}')",
    )
    await expectRawDeniedEverywhere(
      database,
      "message PK cast onto foreign row",
      "UPDATE message SET id = CAST('message-session-b' AS TEXT) WHERE id = 'message-session-a'",
    )
    await expectRawDeniedEverywhere(
      database,
      "part FK cast update",
      "UPDATE part SET message_id = CAST('message-session-b' AS TEXT) WHERE id = 'part-session-a'",
    )

    expect(snapshot(filename)).toEqual(before)
    expect(adminAll<{ count: number }>(filename, "SELECT COUNT(*) AS count FROM part")).toEqual([{ count: 2 }])
  })

  test("metadata.depth cannot be rewritten by a worker statement", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const before = snapshot(filename)

    await expectRawDeniedEverywhere(
      database,
      "metadata depth literal",
      "UPDATE session SET metadata = '{\"depth\":0}' WHERE id = 'session-a'",
    )
    await expectRawDeniedEverywhere(
      database,
      "metadata depth expression",
      "UPDATE session SET metadata = json_set(COALESCE(metadata, '{}'), '$.depth', 0) WHERE id = 'session-a'",
    )

    expect(snapshot(filename)).toEqual(before)
  })

  test("raw writes are denied even when they target the worker's own session", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const before = snapshot(filename)

    await expectRawDeniedEverywhere(
      database,
      "own-session insert",
      "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES ('raw-own', 'session-a', 1, 1, '{}')",
    )
    await expectRawDeniedEverywhere(
      database,
      "own-session update",
      "UPDATE message SET data = '{\"raw\":true}' WHERE id = 'message-session-a'",
    )
    await expectRawDeniedEverywhere(
      database,
      "own-session delete",
      "DELETE FROM message WHERE id = 'message-session-a'",
    )

    expect(snapshot(filename)).toEqual(before)
  })
})

describe("typed repository rejects the same escapes without SQL", () => {
  test("immutable session columns are rejected on update and conflict update", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const repository = repositoryOf(database)
    const before = snapshot(filename)

    const immutable: ReadonlyArray<Record<string, unknown>> = [
      { directory: "/tmp/scope/../../etc" },
      { project_id: "project-other" },
      { slug: "stolen" },
      { parent_id: "session-b" },
      { permission: "[]" },
    ]
    for (const values of immutable) {
      const outcome = await settle(() => repository.update!("session", values, { id: "session-a" }))
      expect(outcome.ok).toBe(false)
      expectTyped(outcome.error, ["ImmutableColumnError"])
    }

    const values = {
      id: "session-a",
      project_id: "project-scope",
      slug: "a",
      directory: "/tmp/scope",
      title: "a",
      version: "1",
      time_created: 1,
      time_updated: 1,
    }
    const conflict = await settle(() =>
      repository.upsert!({
        table: "session",
        values,
        onConflict: { columns: ["id"], update: ["directory"] },
      }),
    )
    expect(conflict.ok).toBe(false)
    expectTyped(conflict.error, ["ImmutableColumnError"])

    expect(snapshot(filename)).toEqual(before)
  })

  test("session insert/upsert cannot target a foreign session id", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const repository = repositoryOf(database)
    const before = snapshot(filename)

    const values = {
      id: "session-b",
      project_id: "project-scope",
      slug: "b",
      directory: "/tmp/scope",
      title: "hijacked",
      version: "1",
      time_created: 1,
      time_updated: 1,
    }
    const insert = await settle(() => repository.insert!({ table: "session", values }))
    expect(insert.ok).toBe(false)
    expectTyped(insert.error, ["ScopeViolationError"])

    const upsert = await settle(() =>
      repository.upsert!({ table: "session", values, onConflict: { columns: ["id"], update: ["title"] } }),
    )
    expect(upsert.ok).toBe(false)
    expectTyped(upsert.error, ["ScopeViolationError"])

    expect(snapshot(filename)).toEqual(before)
  })

  test("scope columns cannot be reassigned on message, event, or event_sequence", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const repository = repositoryOf(database)
    const before = snapshot(filename)

    const cases: ReadonlyArray<readonly [string, Record<string, unknown>, Record<string, unknown>]> = [
      ["message", { session_id: "session-b" }, { id: "message-session-a" }],
      ["event", { aggregate_id: "session-b" }, { id: "event-session-a" }],
      ["event_sequence", { aggregate_id: "session-b" }, { aggregate_id: "session-a" }],
    ]
    for (const [table, values, where] of cases) {
      const outcome = await settle(() => repository.update!(table, values, where))
      expect(outcome.ok, `${table} accepted a scope-column update`).toBe(false)
      expectTyped(outcome.error, ["ScopeViolationError", "ImmutableColumnError", "UnsupportedWriteError"])
    }

    expect(snapshot(filename)).toEqual(before)
  })

  test("a foreign FK owner is rejected instead of trusting CAST SQL", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const repository = repositoryOf(database)
    const before = snapshot(filename)

    const outcome = await settle(() =>
      repository.insert!({
        table: "part",
        values: {
          id: "typed-injected-part",
          message_id: "message-session-b",
          session_id: "session-a",
          time_created: 1,
          time_updated: 1,
          data: "{}",
        },
      }),
    )
    expect(outcome.ok).toBe(false)
    expectTyped(outcome.error, ["ForeignKeyOwnershipError"])

    expect(snapshot(filename)).toEqual(before)
  })

  test("hub-owned metadata.depth is rejected on a typed session update", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const repository = repositoryOf(database)
    const before = snapshot(filename)

    const outcome = await settle(() =>
      repository.update!("session", { metadata: JSON.stringify({ depth: 0 }) }, { id: "session-a" }),
    )
    expect(outcome.ok).toBe(false)
    expectTyped(outcome.error, ["ImmutableColumnError", "ScopeViolationError", "UnsupportedWriteError"])

    expect(snapshot(filename)).toEqual(before)
  })

  test("unknown columns and hub-only tables fail with typed errors and no writes", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const repository = repositoryOf(database)
    const before = snapshot(filename)

    const unknown = await settle(() => repository.update!("message", { not_a_column: 1 }, { id: "message-session-a" }))
    expect(unknown.ok).toBe(false)
    expectTyped(unknown.error, ["UnknownColumnError"])

    const hubOnly = await settle(() =>
      repository.insert!({
        table: "permission",
        values: {
          id: "permission-worker",
          project_id: "project-scope",
          action: "allow",
          resource: "*",
          time_created: 1,
          time_updated: 1,
        },
      }),
    )
    expect(hubOnly.ok).toBe(false)
    expectTyped(hubOnly.error, ["HubOnlyWriteError"])

    expect(adminAll(filename, "SELECT id FROM permission ORDER BY id")).toEqual([{ id: "permission-hub" }])
    expect(snapshot(filename)).toEqual(before)
  })

  test("a rejected insertMany batch is atomic", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const repository = repositoryOf(database)
    const before = snapshot(filename)

    const outcome = await settle(() =>
      repository.insertMany!({
        table: "message",
        values: [
          { id: "batch-own", session_id: "session-a", time_created: 1, time_updated: 1, data: "{}" },
          { id: "batch-foreign", session_id: "session-b", time_created: 1, time_updated: 1, data: "{}" },
        ],
      }),
    )
    expect(outcome.ok).toBe(false)
    expectTyped(outcome.error, ["ScopeViolationError", "ForeignKeyOwnershipError"])

    expect(snapshot(filename)).toEqual(before)
  })
})

describe("secret hub tables stay unreadable to a bound worker", () => {
  const secretTables = ["credential", "account", "control_account", "account_state"] as const

  test("every read surface denies secret tables and direct main access", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const surfaces = readSurfaces(database)
    expect(surfaces.length, "a bound worker still has read surfaces").toBeGreaterThan(0)

    for (const table of secretTables) {
      for (const sql of [`SELECT * FROM ${table}`, `SELECT * FROM main.${table}`]) {
        for (const surface of surfaces) {
          const outcome = await surface.read(sql)
          expect(outcome.ok, `${sql} was readable through ${surface.name}`).toBe(false)
          expectTyped(outcome.error, ["HubOnlyTableError", "CrossSessionAccessError"])
        }
      }
    }

    // The typed read surface applies the same secret policy as the SQL
    // connections, so a caller cannot bypass it by choosing `repository.select`.
    const repository = repositoryOf(database)
    for (const table of secretTables) {
      const outcome = await settle(() => repository.select!(table))
      expect(outcome.ok, `repository.select(${table}) was readable`).toBe(false)
      expectTyped(outcome.error, ["UnsupportedWriteError", "HubOnlyWriteError", "HubOnlyTableError"])
    }

    // The data really exists: denial is authority, not absence.
    expect(adminAll<{ value: string }>(filename, "SELECT value FROM credential")).toEqual([{ value: "super-secret" }])
    expect(adminAll<{ access_token: string }>(filename, "SELECT access_token FROM account")).toEqual([
      { access_token: "access-secret" },
    ])
  })

  test("the worker can still read a non-secret hub table", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)

    const rows = await Effect.runPromise(database.all<{ id: string }>("SELECT id FROM permission ORDER BY id"))
    expect(rows).toEqual([{ id: "permission-hub" }])
  })
})
