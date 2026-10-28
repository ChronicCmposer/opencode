// Typed-repository adversarial acceptance.
//
// Independent of the other adversarial suites. Every case drives a REAL migrated SQLite file
// and a REAL bound worker (`openWorkerDatabase({ sessionID })`) whose authorizer,
// live views, and repository are the production ones. A hub-side admin connection
// takes before/after snapshots, so a rejected statement is proven to have had no
// effect rather than merely returned an empty result.
//
// The design claim under test: the immutable-expression / upsert / REPLACE /
// row-value / FK-CAST / INSERT-SELECT / CTE write shapes are unrepresentable at
// the public boundary, and their SQL forms are fail-closed on every surface that
// still exists.

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
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-typed-final-security-"))
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
      connection.run(
        "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?)",
        [`part-${id}`, `message-${id}`, id, 1, 1, "{}"],
      )
      connection.run("INSERT INTO event_sequence (aggregate_id, seq) VALUES (?, 0)", [id])
      connection.run("INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?, ?, 0, ?, ?)", [
        `event-${id}`,
        id,
        "test.typed.final",
        JSON.stringify({ owner: id }),
      ])
    }
    connection.run(
      "INSERT INTO permission (id, project_id, action, resource, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
      ["permission-final", "project-final", "allow", "*", 1, 1],
    )
    connection.run("INSERT INTO credential (id, label, value, time_created, time_updated) VALUES (?, ?, ?, ?, ?)", [
      "credential-final",
      "final secret",
      "super-secret-final",
      1,
      1,
    ])
    connection.run(
      "INSERT INTO account (id, email, url, access_token, refresh_token, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?)",
      ["account-final", "final@example.test", "https://example.test", "access-final", "refresh-final", 1, 1],
    )
    connection.run(
      "INSERT INTO control_account (email, url, access_token, refresh_token, active, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?)",
      ["control-final@example.test", "https://control.example.test", "control-access", "control-refresh", 1, 1, 1],
    )
    connection.run("INSERT INTO account_state (id, active_account_id) VALUES (?, ?)", [1, "account-final"])
  } finally {
    connection.close()
  }
}

function seedProjectOnly(filename: string): void {
  const connection = openConnection({ filename })
  try {
    connection.run(
      "INSERT INTO project (id, name, worktree, sandboxes, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
      ["project-final", "final", "/tmp/final", "[]", 1, 1],
    )
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

function snapshot(filename: string) {
  return {
    session: admin(
      filename,
      "SELECT id, project_id, parent_id, slug, directory, title, metadata FROM session ORDER BY id",
    ),
    message: admin(filename, "SELECT id, session_id, data FROM message ORDER BY id"),
    part: admin(filename, "SELECT id, message_id, session_id, data FROM part ORDER BY id"),
    event: admin(filename, "SELECT id, aggregate_id, seq, data FROM event ORDER BY id"),
    event_sequence: admin(filename, "SELECT aggregate_id, seq FROM event_sequence ORDER BY aggregate_id"),
    permission: admin(filename, "SELECT id, project_id, action, resource FROM permission ORDER BY id"),
    credential: admin(filename, "SELECT id, label, value FROM credential ORDER BY id"),
    account: admin(filename, "SELECT id, email, access_token FROM account ORDER BY id"),
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

async function expectRawDeniedEverywhere(database: WorkerDatabase, label: string, sql: string): Promise<void> {
  const surfaces = writeSurfaces(database)
  expect(surfaces.length, "a bound worker still has a deny-by-contract surface inventory").toBeGreaterThan(0)
  for (const surface of surfaces) {
    const outcome = await surface.write(sql)
    expect(outcome.ok, `${label} was accepted by ${surface.name}`).toBe(false)
    expect(outcome.error, `${label} returned without a throw on ${surface.name}`).toBeDefined()
  }
}

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("SQL bypass shapes are fail-closed on every public surface", () => {
  test("immutable expression tails cannot rewrite session identity", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const before = snapshot(filename)

    const hostile = [
      "UPDATE session SET directory = '/tmp/final' || '/../../etc' WHERE id = 'session-a'",
      "UPDATE session SET slug = upper('stolen') WHERE id = 'session-a'",
      "UPDATE session SET parent_id = CAST('session-b' AS TEXT) WHERE id = 'session-a'",
      "UPDATE session SET id = 'session-b' WHERE id = 'session-a'",
      "UPDATE session SET (directory, slug) = ('/tmp/evil', 'stolen') WHERE id = 'session-a'",
    ]
    for (const sql of hostile) await expectRawDeniedEverywhere(database, sql.slice(0, 44), sql)
    expect(snapshot(filename)).toEqual(before)
  })

  test("upsert, REPLACE, and conflict clauses cannot take over a row", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const before = snapshot(filename)

    const hostile = [
      "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES ('session-b', 'project-final', 'b', '/tmp/final', 'hijacked', '1', 1, 1) ON CONFLICT(id) DO UPDATE SET title = 'hijacked', directory = '/tmp/final/../../etc'",
      "INSERT OR REPLACE INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES ('session-b', 'project-final', 'b', '/tmp/final', 'hijacked', '1', 1, 1)",
      "REPLACE INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES ('session-a', 'project-final', 'a', '/tmp/final/../../etc', 'replaced', '1', 1, 1)",
      "INSERT OR IGNORE INTO message (id, session_id, time_created, time_updated, data) VALUES ('message-session-b', 'session-a', 1, 1, '{}')",
      "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES ('message-session-b', 'session-a', 1, 1, '{}') ON CONFLICT(id) DO UPDATE SET session_id = excluded.session_id",
      "UPDATE OR REPLACE message SET id = 'message-session-b', data = '{}' WHERE id = 'message-session-a'",
    ]
    for (const sql of hostile) await expectRawDeniedEverywhere(database, sql.slice(0, 44), sql)

    expect(snapshot(filename)).toEqual(before)
    expect(
      admin<{ title: string; directory: string }>(
        filename,
        "SELECT title, directory FROM session WHERE id = 'session-b'",
      ),
    ).toEqual([{ title: "session-b", directory: "/tmp/final" }])
  })

  test("row-value SET and FK CAST cannot move a row across sessions", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const before = snapshot(filename)

    const hostile = [
      "UPDATE message SET (session_id, data) = ('session-b', '{\"hijacked\":true}') WHERE id = 'message-session-a'",
      "UPDATE event SET (aggregate_id, data) = ('session-b', '{\"hijacked\":true}') WHERE id = 'event-session-a'",
      "UPDATE event_sequence SET (aggregate_id, seq) = ('session-b', 99) WHERE aggregate_id = 'session-a'",
      "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES ('final-cast', CAST('session-b' AS TEXT), 1, 1, '{}')",
      "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES ('final-part', CAST('message-session-b' AS TEXT), 'session-a', 1, 1, '{}')",
      "UPDATE part SET message_id = CAST('message-session-b' AS TEXT) WHERE id = 'part-session-a'",
      "UPDATE message SET id = CAST('message-session-b' AS TEXT) WHERE id = 'message-session-a'",
    ]
    for (const sql of hostile) await expectRawDeniedEverywhere(database, sql.slice(0, 44), sql)

    expect(snapshot(filename)).toEqual(before)
    expect(admin<{ count: number }>(filename, "SELECT COUNT(*) AS count FROM part")).toEqual([{ count: 2 }])
  })

  test("INSERT-SELECT, CTE-DML, direct main, and multi-statement writes are refused", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const before = snapshot(filename)

    const hostile = [
      "INSERT INTO message (id, session_id, time_created, time_updated, data) SELECT 'final-select', 'session-b', 1, 1, '{}'",
      "WITH forged AS (SELECT 'session-b' AS target) UPDATE message SET session_id = (SELECT target FROM forged) WHERE id = 'message-session-a'",
      "INSERT INTO main.message (id, session_id, time_created, time_updated, data) VALUES ('final-main', 'session-a', 1, 1, '{}')",
      "UPDATE main.session SET directory = '/tmp/evil' WHERE id = 'session-a'",
      "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES ('final-multi', 'session-a', 1, 1, '{}'); DROP TABLE message; --",
      "CREATE TABLE final_owned (id text primary key)",
      "ATTACH DATABASE ':memory:' AS escaped",
      "PRAGMA writable_schema = 1",
    ]
    for (const sql of hostile) await expectRawDeniedEverywhere(database, sql.slice(0, 44), sql)

    expect(snapshot(filename)).toEqual(before)
    expect(admin<{ name: string }>(filename, "SELECT name FROM sqlite_master WHERE name = 'final_owned'")).toEqual([])
  })
})

describe("typed repository rejects the same escapes without SQL", () => {
  test("immutable session identity is refused for every authority column", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const before = snapshot(filename)

    const immutable: ReadonlyArray<Record<string, unknown>> = [
      { id: "session-b" },
      { directory: "/tmp/evil" },
      { project_id: "project-other" },
      { parent_id: "session-b" },
      { permission: "[]" },
      { slug: "stolen" },
      { workspace_id: "workspace-other" },
    ]
    for (const values of immutable) {
      const outcome = await settle(() => database.repository.update("session", values, { id: "session-a" }))
      expect(outcome.ok, `session accepted ${JSON.stringify(values)}`).toBe(false)
      expectTyped(outcome.error, ["ImmutableColumnError"])
    }

    const conflict = await settle(() =>
      database.repository.upsert({
        table: "session",
        values: {
          id: "session-a",
          project_id: "project-final",
          slug: "a",
          directory: "/tmp/final",
          title: "a",
          version: "1",
          time_created: 1,
          time_updated: 1,
        },
        onConflict: { columns: ["id"], update: ["directory"] },
      }),
    )
    expect(conflict.ok).toBe(false)
    expectTyped(conflict.error, ["ImmutableColumnError"])

    expect(snapshot(filename)).toEqual(before)
  })

  test("hub-owned metadata.depth cannot change, but other metadata may", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)

    for (const metadata of [JSON.stringify({ depth: 0 }), JSON.stringify({ depth: 2 }), { depth: 0 }, "{}"]) {
      const outcome = await settle(() => database.repository.update("session", { metadata }, { id: "session-a" }))
      expect(outcome.ok, `session accepted metadata ${JSON.stringify(metadata)}`).toBe(false)
      expectTyped(outcome.error, ["ImmutableColumnError"])
    }
    expect(admin<{ metadata: string }>(filename, "SELECT metadata FROM session WHERE id = 'session-a'")).toEqual([
      { metadata: JSON.stringify({ depth: 1 }) },
    ])

    const preserved = await settle(() =>
      database.repository.update(
        "session",
        { metadata: JSON.stringify({ depth: 1, note: "kept" }) },
        { id: "session-a" },
      ),
    )
    expect(preserved.ok, `preserving depth failed: ${String(preserved.error)}`).toBe(true)
    expect(admin<{ metadata: string }>(filename, "SELECT metadata FROM session WHERE id = 'session-a'")).toEqual([
      { metadata: JSON.stringify({ depth: 1, note: "kept" }) },
    ])
  })

  test("foreign scope, FK owner, unique key, and cross-session upserts are refused", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const before = snapshot(filename)

    const cases: ReadonlyArray<readonly [string, () => unknown, readonly string[]]> = [
      [
        "insert foreign scope",
        () =>
          database.repository.insert("message", {
            id: "final-foreign-scope",
            session_id: "session-b",
            time_created: 1,
            time_updated: 1,
            data: "{}",
          }),
        ["ScopeViolationError"],
      ],
      [
        "delete immediate session",
        () => database.repository.delete("session", { id: "session-b" }),
        ["ScopeViolationError", "UnsupportedWriteError"],
      ],
      [
        "select foreign scope",
        () => database.repository.select("message", { session_id: "session-b" }),
        ["ScopeViolationError"],
      ],
      [
        "insert foreign FK parent",
        () =>
          database.repository.insert("part", {
            id: "final-foreign-part",
            message_id: "message-session-b",
            time_created: 1,
            time_updated: 1,
            data: "{}",
          }),
        ["ForeignKeyOwnershipError"],
      ],
      [
        "update foreign FK parent",
        () => database.repository.update("part", { message_id: "message-session-b" }, { id: "part-session-a" }),
        ["ForeignKeyOwnershipError"],
      ],
      [
        "upsert foreign row by PK",
        () =>
          database.repository.upsert({
            table: "message",
            values: { id: "message-session-b", session_id: "session-a", time_created: 1, time_updated: 1, data: "{}" },
            onConflict: { columns: ["id"], update: ["data"] },
          }),
        ["ScopeViolationError"],
      ],
      [
        "upsert foreign scope value",
        () =>
          database.repository.upsert({
            table: "message",
            values: { id: "final-upsert", session_id: "session-b", time_created: 1, time_updated: 1, data: "{}" },
            onConflict: { columns: ["id"], update: ["data"] },
          }),
        ["ScopeViolationError"],
      ],
    ]

    for (const [label, operation, tags] of cases) {
      const outcome = await settle(operation)
      expect(outcome.ok, `${label} was accepted`).toBe(false)
      expectTyped(outcome.error, tags)
    }

    // A foreign id in the WHERE is not trusted; the bound scope is AND-ed in, so
    // the statement is a no-op rather than a cross-session write.
    const foreignUpdate = await settle(() =>
      database.repository.update("message", { data: '{"hijacked":true}' }, { id: "message-session-b" }),
    )
    expect(foreignUpdate.ok).toBe(true)
    expect((foreignUpdate.value as { changes: number }).changes).toBe(0)
    const foreignDelete = await settle(() => database.repository.delete("message", { id: "message-session-b" }))
    expect(foreignDelete.ok).toBe(true)
    expect((foreignDelete.value as { changes: number }).changes).toBe(0)

    expect(snapshot(filename)).toEqual(before)
  })

  test("a worker cannot mint a session root when the hub row is absent", async () => {
    const filename = migratedDatabase()
    seedProjectOnly(filename)
    const database = await openBound(filename)

    // The session root is created by the hub, never by the worker's isolated
    // connection. Without this, a worker whose row is missing could recreate it
    // with depth 0 and buy headroom for descendants.
    const inserted = await settle(() =>
      database.repository.insert("session", {
        id: "session-a",
        project_id: "project-final",
        slug: "minted",
        directory: "/tmp/final",
        title: "minted",
        version: "1",
        metadata: JSON.stringify({ depth: 0 }),
        time_created: 1,
        time_updated: 1,
      }),
    )
    expect(inserted.ok, "the worker minted its own session root").toBe(false)
    expectTyped(inserted.error, ["UnsupportedWriteError", "HubOnlyWriteError", "ScopeViolationError"])
    expect(admin(filename, "SELECT id FROM session WHERE id = 'session-a'")).toEqual([])
  })

  test("the worker cannot delete its own session root and re-insert a forged depth", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const before = snapshot(filename)

    // The session root is hub authority: a worker projecting data may update the
    // row, but it may not remove and recreate it to reset metadata.depth or
    // parent_id (which would misclassify the next spawn for the depth cap).
    const deleted = await settle(() => database.repository.delete("session", { id: "session-a" }))
    expect(deleted.ok, "the worker deleted its own session root").toBe(false)
    expectTyped(deleted.error, [
      "UnsupportedWriteError",
      "HubOnlyWriteError",
      "ScopeViolationError",
      "ImmutableColumnError",
    ])

    const inserted = await settle(() =>
      database.repository.insert("session", {
        id: "session-a",
        project_id: "project-final",
        slug: "forged",
        directory: "/tmp/final",
        title: "forged",
        version: "1",
        parent_id: "session-b",
        metadata: JSON.stringify({ depth: 0 }),
        time_created: 1,
        time_updated: 1,
      }),
    )
    expect(inserted.ok, "the worker re-created its session root with a forged depth").toBe(false)

    expect(snapshot(filename)).toEqual(before)
  })

  test("hub-only, durable-log, and unknown columns fail with typed errors", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const before = snapshot(filename)

    const hub = await settle(() =>
      database.repository.insert(
        "permission" as never,
        {
          id: "permission-worker",
          project_id: "project-final",
          action: "allow",
          resource: "*",
          time_created: 1,
          time_updated: 1,
        } as never,
      ),
    )
    expect(hub.ok).toBe(false)
    expectTyped(hub.error, ["HubOnlyWriteError"])

    for (const table of ["event", "event_sequence", "event_blob"] as const) {
      const outcome = await settle(() =>
        database.repository.insert(table as never, { aggregate_id: "session-a" } as never),
      )
      expect(outcome.ok, `${table} was writable through the typed repository`).toBe(false)
      expectTyped(outcome.error, ["UnsupportedWriteError"])
    }

    const unknown = await settle(() =>
      database.repository.update("message", { not_a_column: 1 } as never, { id: "message-session-a" }),
    )
    expect(unknown.ok).toBe(false)
    expectTyped(unknown.error, ["UnknownColumnError"])

    expect(snapshot(filename)).toEqual(before)
  })

  test("a rejected insertMany batch rolls back every row", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const before = snapshot(filename)

    const outcome = await settle(() =>
      database.repository.insertMany("message", [
        { id: "final-batch-own", time_created: 1, time_updated: 1, data: "{}" },
        { id: "final-batch-foreign", session_id: "session-b", time_created: 1, time_updated: 1, data: "{}" },
      ]),
    )
    expect(outcome.ok).toBe(false)
    expectTyped(outcome.error, ["ScopeViolationError", "ForeignKeyOwnershipError"])

    expect(snapshot(filename)).toEqual(before)
    expect(admin<{ id: string }>(filename, "SELECT id FROM message WHERE id LIKE 'final-batch-%'")).toEqual([])
  })
})

describe("secret hub state stays unreadable", () => {
  test("every read surface and repository.select deny secret tables", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const surfaces = readSurfaces(database)
    expect(surfaces.length, "a bound worker still has read surfaces").toBeGreaterThan(0)

    const secrets = ["credential", "account", "control_account", "account_state"] as const
    for (const table of secrets) {
      for (const sql of [`SELECT * FROM ${table}`, `SELECT * FROM main.${table}`]) {
        for (const surface of surfaces) {
          const outcome = await surface.read(sql)
          expect(outcome.ok, `${sql} was readable through ${surface.name}`).toBe(false)
          expectTyped(outcome.error, ["HubOnlyTableError", "CrossSessionAccessError"])
        }
      }
      const typed = await settle(() => database.repository.select(table as never))
      expect(typed.ok, `repository.select(${table}) was readable`).toBe(false)
      expectTyped(typed.error, ["UnsupportedWriteError", "HubOnlyWriteError", "HubOnlyTableError"])
    }

    // Denial is authority, not absence.
    expect(admin<{ value: string }>(filename, "SELECT value FROM credential")).toEqual([
      { value: "super-secret-final" },
    ])
    expect(admin<{ access_token: string }>(filename, "SELECT access_token FROM account")).toEqual([
      { access_token: "access-final" },
    ])

    // Non-secret hub state remains readable through the live view.
    const permission = await Effect.runPromise(
      database.all<{ readonly id: string }>("SELECT id FROM permission ORDER BY id"),
    )
    expect(permission).toEqual([{ id: "permission-final" }])
  })
})
