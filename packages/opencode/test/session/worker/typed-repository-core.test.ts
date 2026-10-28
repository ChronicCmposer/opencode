// Typed repository core (RED-first).
//
// This suite drives the public typed write/read surface against a real migrated
// database and a session-bound worker. It asserts the contract the SQL-string
// write path must eventually be replaced with:
//
//   * a generated table registry validated against the live schema;
//   * typed errors instead of a statement classifier;
//   * an implicit bound session and FK ownership on every write;
//   * immutable session identity and hub-owned metadata.depth;
//   * atomic validate+write and transaction-aware savepoints;
//   * no raw SQL/repository connection on the public surface.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { openConnection } from "@opencode-ai/core/database/connection"
import { migrationStatements } from "@/session/writer/migration.statements.gen"
import { markSchemaIdentity } from "@/session/db/schema"
import { SCHEMA_TABLES } from "@/session/db/allowlist.gen"
import {
  CrossSessionAccessError,
  WorkerStatementRejectedError,
  openWorkerDatabase,
  type WorkerDatabase,
} from "@/session/worker/db"
import { TABLE_REGISTRY, assertTableRegistry, tableRegistry } from "@/session/worker/repository/registry"
import { connectedRepository } from "@/session/worker/repository/write"
import { LEDGER_SQL } from "@/session/ledger/tool-call"
import {
  ForeignKeyOwnershipError,
  HubOnlyWriteError,
  ImmutableColumnError,
  ScopeViolationError,
  TableRegistryDriftError,
  UnknownColumnError,
  UnsupportedWriteError,
} from "@/session/worker/repository/errors"

const WORKER_DB_SOURCE = path.resolve(import.meta.dir, "../../../src/session/worker/db.ts")

const directories: string[] = []
const cleanups: Array<() => void | Promise<void>> = []

function workspace(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-typed-repository-"))
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

function seed(filename: string): void {
  const connection = openConnection({ filename })
  try {
    connection.run(
      "INSERT INTO project (id, name, worktree, sandboxes, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
      ["project-1", "project", "/tmp/repository", "[]", 1, 1],
    )
    for (const id of ["session-a", "session-b"]) {
      connection.run(
        "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated, metadata) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [id, "project-1", id, "/tmp/repository", id, "1", 1, 1, id === "session-a" ? '{"depth":0}' : '{"depth":1}'],
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

async function openBound(filename: string, sessionID = "session-a"): Promise<WorkerDatabase> {
  const database = await Effect.runPromise(openWorkerDatabase({ filename, sessionID }))
  cleanups.push(() => Effect.runPromise(database.close))
  return database
}

function thrownBy(operation: () => unknown): unknown {
  try {
    operation()
    return undefined
  } catch (error) {
    return error
  }
}

function rows<T = Record<string, unknown>>(filename: string, sql: string, params: unknown[] = []): T[] {
  const connection = openConnection({ filename })
  try {
    return [...connection.all<T>(sql, params)]
  } finally {
    connection.close()
  }
}

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("generated table registry", () => {
  test("covers every schema table with structural metadata and classification", () => {
    expect(TABLE_REGISTRY.map((entry) => entry.table).sort()).toEqual([...SCHEMA_TABLES].sort())

    const message = tableRegistry("message")!
    expect(message.columns).toEqual(["id", "session_id", "time_created", "time_updated", "data"])
    expect(message.scope).toBe("session_id")
    expect(message.primaryKey).toEqual(["id"])
    expect(message.uniqueKeys).toContainEqual(["id"])
    expect(message.foreignKeys).toContainEqual({ table: "session", from: "session_id", to: "id" })
    expect(message.classification).toBe("worker")
    expect(message.readable).toBe(true)

    expect(tableRegistry("event")!.classification).toBe("durable")
    expect(tableRegistry("project")!.classification).toBe("hub")
    expect(tableRegistry("project")!.readable).toBe(true)
    expect(tableRegistry("credential")!.classification).toBe("hub")
    expect(tableRegistry("credential")!.readable).toBe(false)
  })

  test("validates against the live schema and refuses column drift", () => {
    const filename = migratedDatabase()
    const connection = openConnection({ filename })
    try {
      expect(() => assertTableRegistry(connection)).not.toThrow()
      connection.exec("ALTER TABLE message ADD COLUMN extra text")
      expect(() => assertTableRegistry(connection)).toThrow(TableRegistryDriftError)
    } finally {
      connection.close()
    }
  })
})

describe("public typed surface", () => {
  test("exposes typed methods and no raw SQL capability", async () => {
    const filename = migratedDatabase()
    seed(filename)
    const database = await openBound(filename)
    const repository = database.repository

    for (const method of ["insert", "insertMany", "upsert", "update", "delete", "select"]) {
      expect(typeof (repository as unknown as Record<string, unknown>)[method]).toBe("function")
    }
    for (const forbidden of ["run", "all", "exec", "query", "connection", "strictConnection", "drizzleConnection"]) {
      expect(repository).not.toHaveProperty(forbidden)
    }
  })

  test("the SQL write parser and the public raw write surface are gone", async () => {
    const filename = migratedDatabase()
    seed(filename)
    const database = await openBound(filename)

    // The statement analyzer/rewriter is deleted, not merely bypassed.
    const source = fs.readFileSync(WORKER_DB_SOURCE, "utf8")
    for (const parser of [
      "function executeWrite",
      "function assertWorkerStatement",
      "function injectScope",
      "function rewriteTarget",
      "function validateInsertScope",
      "function validateUpdateScope",
      "function validateForeignKeys",
    ]) {
      expect(source).not.toContain(parser)
    }
    expect(source).not.toMatch(/CREATE\s+TEMP\s+TABLE/i)
    // D6 is enforced on the trusted Drizzle path by per-connection TEMP triggers.
    expect(source).toMatch(/CREATE\s+TEMP\s+TRIGGER/i)

    // A bound handle has no raw write capability: every write is repository-only.
    const surface = database as unknown as Record<string, unknown>
    expect(surface.run).toBeUndefined()
    expect(surface.busConnection).toBeUndefined()
    for (const raw of [
      () =>
        database.connection.run(
          "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES ('raw', 'session-a', 1, 1, '{}')",
        ),
      () => database.connection.exec("DELETE FROM message"),
      () =>
        database.connection
          .query(
            "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES ('raw', 'session-a', 1, 1, '{}')",
          )
          .run(),
      () => database.strictConnection.run("DELETE FROM message"),
      () => database.drizzleConnection.run("DELETE FROM message"),
    ]) {
      expect(thrownBy(raw)).toBeInstanceOf(WorkerStatementRejectedError)
    }

    // DDL/ATTACH and direct main reads are refused by the authorizer.
    expect(thrownBy(() => database.connection.exec("CREATE TABLE raw_owned (id text)"))).toBeInstanceOf(
      WorkerStatementRejectedError,
    )
    expect(thrownBy(() => database.strictConnection.all("SELECT id FROM main.message"))).toBeInstanceOf(
      CrossSessionAccessError,
    )
  })
})

describe("typed writes bind the session implicitly", () => {
  test("insert, update, delete and select operate only on the bound session", async () => {
    const filename = migratedDatabase()
    seed(filename)
    const database = await openBound(filename)
    const repository = database.repository

    const inserted = repository.insert("message", { id: "message-new", time_created: 2, time_updated: 2, data: "{}" })
    expect(inserted.changes).toBe(1)
    expect(rows(filename, "SELECT id, session_id FROM message WHERE id = 'message-new'")).toEqual([
      { id: "message-new", session_id: "session-a" },
    ])

    expect(repository.select("message", { id: "message-new" })).toEqual([
      { id: "message-new", session_id: "session-a", time_created: 2, time_updated: 2, data: "{}" },
    ])
    // The bound view hides the other session.
    expect(
      repository
        .select("message")
        .map((row) => row.id)
        .sort(),
    ).toEqual(["message-new", "message-session-a"])

    expect(repository.update("message", { data: '{"x":1}' }, { id: "message-new" }).changes).toBe(1)
    expect(rows(filename, "SELECT data FROM message WHERE id = 'message-new'")).toEqual([{ data: '{"x":1}' }])

    expect(repository.delete("message", { id: "message-new" }).changes).toBe(1)
    expect(rows(filename, "SELECT id FROM message WHERE id = 'message-new'")).toEqual([])
  })

  test("a caller cannot provide another session's scope or target it", async () => {
    const filename = migratedDatabase()
    seed(filename)
    const database = await openBound(filename)
    const repository = database.repository

    expect(
      thrownBy(() =>
        repository.insert("message", {
          id: "message-foreign",
          session_id: "session-b",
          time_created: 1,
          time_updated: 1,
          data: "{}",
        }),
      ),
    ).toBeInstanceOf(ScopeViolationError)
    expect(thrownBy(() => repository.select("message", { session_id: "session-b" }))).toBeInstanceOf(
      ScopeViolationError,
    )
    expect(repository.update("message", { data: "x" }, { id: "message-session-b" }).changes).toBe(0)
  })
})

describe("typed validation errors", () => {
  test("unknown columns, hub-only tables and durable-log writes fail with typed errors", async () => {
    const filename = migratedDatabase()
    seed(filename)
    const database = await openBound(filename)
    const repository = database.repository

    expect(
      thrownBy(() =>
        repository.insert("message", { id: "bad", bogus: 1, time_created: 1, time_updated: 1, data: "{}" } as never),
      ),
    ).toBeInstanceOf(UnknownColumnError)

    const hub = thrownBy(() =>
      repository.insert(
        "project" as never,
        {
          id: "p",
          name: "p",
          worktree: "/tmp",
          sandboxes: "[]",
          time_created: 1,
          time_updated: 1,
        } as never,
      ),
    )
    expect(hub).toBeInstanceOf(HubOnlyWriteError)
    expect((hub as HubOnlyWriteError).table).toBe("project")

    expect(
      thrownBy(() =>
        repository.insert(
          "event" as never,
          {
            id: "e",
            aggregate_id: "session-a",
            seq: 0,
            type: "t",
            data: "{}",
          } as never,
        ),
      ),
    ).toBeInstanceOf(UnsupportedWriteError)
  })

  test("immutable session identity cannot change", async () => {
    const filename = migratedDatabase()
    seed(filename)
    const database = await openBound(filename)
    const repository = database.repository

    for (const values of [
      { slug: "stolen" },
      { id: "session-stolen" },
      { directory: "/tmp/evil" },
      { project_id: "project-stolen" },
      { parent_id: "session-b" },
    ]) {
      expect(thrownBy(() => repository.update("session", values, { id: "session-a" }))).toBeInstanceOf(
        ImmutableColumnError,
      )
    }

    // A projector may repeat the current identity value; it may still change data.
    expect(
      repository.update("session", { id: "session-a", slug: "session-a", title: "ok" }, { id: "session-a" }).changes,
    ).toBe(1)
    expect(rows(filename, "SELECT title FROM session WHERE id = 'session-a'")).toEqual([{ title: "ok" }])
  })

  test("metadata.depth is hub-owned: a worker can preserve it but never change it", async () => {
    const filename = migratedDatabase()
    seed(filename)
    const database = await openBound(filename)
    const repository = database.repository

    expect(
      thrownBy(() => repository.update("session", { metadata: '{"depth":1}' }, { id: "session-a" })),
    ).toBeInstanceOf(ImmutableColumnError)
    expect(thrownBy(() => repository.update("session", { metadata: "{}" }, { id: "session-a" }))).toBeInstanceOf(
      ImmutableColumnError,
    )

    expect(repository.update("session", { metadata: '{"depth":0,"note":"kept"}' }, { id: "session-a" }).changes).toBe(1)
    expect(rows(filename, "SELECT metadata FROM session WHERE id = 'session-a'")).toEqual([
      { metadata: '{"depth":0,"note":"kept"}' },
    ])
  })

  test("foreign-key parents must belong to the bound session", async () => {
    const filename = migratedDatabase()
    seed(filename)
    const database = await openBound(filename)
    const repository = database.repository

    const foreign = thrownBy(() =>
      repository.insert("part", {
        id: "part-foreign",
        message_id: "message-session-b",
        time_created: 1,
        time_updated: 1,
        data: "{}",
      }),
    )
    expect(foreign).toBeInstanceOf(ForeignKeyOwnershipError)
    expect(rows(filename, "SELECT id FROM part WHERE id = 'part-foreign'")).toEqual([])

    expect(
      repository.insert("part", {
        id: "part-own",
        message_id: "message-session-a",
        time_created: 1,
        time_updated: 1,
        data: "{}",
      }).changes,
    ).toBe(1)
    expect(
      thrownBy(() => repository.update("part", { message_id: "message-session-b" }, { id: "part-own" })),
    ).toBeInstanceOf(ForeignKeyOwnershipError)
  })
})

describe("upserts and batches are atomic", () => {
  test("upsert inserts its own row, updates its own row, and cannot claim a foreign key", async () => {
    const filename = migratedDatabase()
    seed(filename)
    const database = await openBound(filename)
    const repository = database.repository

    expect(
      repository.upsert(
        "message",
        { id: "message-upsert", time_created: 1, time_updated: 1, data: "{}" },
        { columns: ["id"], update: ["data"] },
      ).changes,
    ).toBe(1)
    expect(
      repository.upsert(
        "message",
        { id: "message-upsert", time_created: 1, time_updated: 2, data: '{"v":2}' },
        { columns: ["id"], update: ["data"] },
      ).changes,
    ).toBe(1)
    expect(rows(filename, "SELECT session_id, data FROM message WHERE id = 'message-upsert'")).toEqual([
      { session_id: "session-a", data: '{"v":2}' },
    ])

    expect(
      thrownBy(() =>
        repository.upsert(
          "message",
          {
            id: "message-session-b",
            session_id: "session-a",
            time_created: 1,
            time_updated: 1,
            data: "{}",
          },
          { columns: ["id"], update: ["data"] },
        ),
      ),
    ).toBeInstanceOf(ScopeViolationError)
    expect(rows(filename, "SELECT session_id, data FROM message WHERE id = 'message-session-b'")).toEqual([
      { session_id: "session-b", data: "{}" },
    ])
  })

  test("insertMany rolls back every row when one row is invalid", async () => {
    const filename = migratedDatabase()
    seed(filename)
    const database = await openBound(filename)
    const repository = database.repository

    expect(
      repository.insertMany("message", [
        { id: "message-batch-1", time_created: 1, time_updated: 1, data: "{}" },
        { id: "message-batch-2", time_created: 1, time_updated: 1, data: "{}" },
      ]).changes,
    ).toBe(2)
    expect(rows(filename, "SELECT id FROM message WHERE id LIKE 'message-batch-%' ORDER BY id")).toEqual([
      { id: "message-batch-1" },
      { id: "message-batch-2" },
    ])

    expect(
      thrownBy(() =>
        repository.insertMany("part", [
          { id: "part-batch-1", message_id: "message-session-a", time_created: 1, time_updated: 1, data: "{}" },
          { id: "part-batch-2", message_id: "missing-message", time_created: 1, time_updated: 1, data: "{}" },
        ]),
      ),
    ).toBeInstanceOf(ForeignKeyOwnershipError)
    expect(rows(filename, "SELECT id FROM part WHERE id LIKE 'part-batch-%'")).toEqual([])

    // A duplicate primary key inside one batch also rolls the whole batch back.
    expect(
      thrownBy(() =>
        repository.insertMany("message", [
          { id: "message-dup", time_created: 1, time_updated: 1, data: "{}" },
          { id: "message-dup", time_created: 2, time_updated: 2, data: "{}" },
        ]),
      ),
    ).toBeDefined()
    expect(rows(filename, "SELECT id FROM message WHERE id = 'message-dup'")).toEqual([])
  })

  test("repository writes join an open transaction through a savepoint", async () => {
    const filename = migratedDatabase()
    seed(filename)
    const database = await openBound(filename)
    const repository = database.repository

    database.connection.exec("BEGIN IMMEDIATE")
    try {
      repository.insert("message", { id: "message-tx", time_created: 1, time_updated: 1, data: "{}" })
    } finally {
      database.connection.exec("ROLLBACK")
    }
    expect(rows(filename, "SELECT id FROM message WHERE id = 'message-tx'")).toEqual([])

    repository.insert("message", { id: "message-committed", time_created: 1, time_updated: 1, data: "{}" })
    expect(rows(filename, "SELECT id FROM message WHERE id = 'message-committed'")).toEqual([
      { id: "message-committed" },
    ])
  })
})

describe("typed transaction scope and connected repository", () => {
  test("transaction commits both writes and rolls both back on failure", async () => {
    const filename = migratedDatabase()
    seed(filename)
    const database = await openBound(filename)
    const repository = database.repository

    repository.atomic(() => {
      repository.insert("message", { id: "message-tx-1", time_created: 1, time_updated: 1, data: "{}" })
      repository.insert("message", { id: "message-tx-2", time_created: 1, time_updated: 1, data: "{}" })
    })
    expect(rows(filename, "SELECT id FROM message WHERE id LIKE 'message-tx-%' ORDER BY id")).toEqual([
      { id: "message-tx-1" },
      { id: "message-tx-2" },
    ])

    expect(() =>
      repository.atomic(() => {
        repository.insert("message", { id: "message-tx-rollback", time_created: 1, time_updated: 1, data: "{}" })
        throw new Error("result write failed")
      }),
    ).toThrow("result write failed")
    expect(rows(filename, "SELECT id FROM message WHERE id = 'message-tx-rollback'")).toEqual([])
  })

  test("connectedRepository writes a ledger-only database without a session scope", () => {
    const connection = openConnection({ filename: ":memory:", pragmas: ["PRAGMA foreign_keys = OFF"] })
    try {
      connection.exec(LEDGER_SQL)
      const repository = connectedRepository(connection)
      repository.insert("tool_call", {
        call_id: "call-1",
        session_id: "ses-1",
        tool: "write",
        side_effecting: 1,
        state: "started",
        outcome: null,
        time_started: 1,
      })
      expect(repository.select("tool_call", { call_id: "call-1" })).toEqual([
        {
          call_id: "call-1",
          session_id: "ses-1",
          tool: "write",
          side_effecting: 1,
          state: "started",
          outcome: null,
          time_started: 1,
          time_finished: null,
        },
      ])
      expect(
        repository.update("tool_call", { state: "finished", outcome: "ok", time_finished: 2 }, { call_id: "call-1" })
          .changes,
      ).toBe(1)
    } finally {
      connection.close()
    }
  })
})
