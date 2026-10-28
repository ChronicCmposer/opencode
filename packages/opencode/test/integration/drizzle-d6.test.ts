// D6 on the Drizzle write path.
//
// A bound worker's in-isolate Drizzle graph (the projector and EventV2) writes
// through a private, trusted runner that routes statements to `main`. That route
// must carry the same session fence as the typed repository: a worker bound to
// session A may not delete, update, or insert session B's rows, and a deletion
// that would cascade into B's transcript must abort before any row changes.
//
// The production pieces are real: a migrated SQLite file, the production
// `openWorkerDatabase({ sessionID })` boundary, and `databaseLayer` (the Drizzle
// `Database.Service` the worker graph consumes).

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Effect, Exit } from "effect"
import { eq } from "drizzle-orm"
import { Database as DatabaseService } from "@opencode-ai/core/database/database"
import { openConnection } from "@opencode-ai/core/database/connection"
import { MessageTable, PartTable } from "@opencode-ai/core/session/sql"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { markSchemaIdentity } from "@/session/db/schema"
import { databaseLayer, openWorkerDatabase, type WorkerDatabase } from "@/session/worker/db"
import { migrationStatements } from "@/session/writer/migration.statements.gen"

const directories: string[] = []
const cleanups: Array<() => void | Promise<void>> = []

function workspace(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-drizzle-d6-"))
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
      ["project-d6", "d6", "/tmp/d6", "[]", 1, 1],
    )
    for (const id of ["session-a", "session-b"]) {
      connection.run(
        "INSERT INTO session (id, project_id, slug, directory, title, version, metadata, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [id, "project-d6", id, "/tmp/d6", id, "1", JSON.stringify({ depth: 1 }), 1, 1],
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

function snapshot(filename: string) {
  return {
    session: admin(filename, "SELECT id, title FROM session ORDER BY id"),
    message: admin(filename, "SELECT id, session_id, data FROM message ORDER BY id"),
    part: admin(filename, "SELECT id, session_id FROM part ORDER BY id"),
  }
}

/**
 * Run `work` against the worker's real Drizzle `Database.Service`, returning the
 * Exit so a forbidden statement is proven to fail rather than throw a test error.
 */
function withDrizzle<A>(database: WorkerDatabase, work: (db: DatabaseService.Interface["db"]) => Effect.Effect<A>) {
  return Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const { db } = yield* DatabaseService.Service
        return yield* work(db)
      }).pipe(Effect.provide(databaseLayer(database))),
    ),
  )
}

function expectDenied(exit: Exit.Exit<unknown, unknown>, label: string): void {
  expect(Exit.isFailure(exit), `${label} was accepted by the Drizzle write path`).toBe(true)
  const rendered = String(exit)
  expect(rendered, `${label} failed for an unrelated reason`).toContain("outside its scope")
}

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("D6 is enforced on the trusted Drizzle write path", () => {
  test("deleting another session's root aborts and cascades into nothing", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const before = snapshot(filename)

    const exit = await withDrizzle(database, (db) =>
      Effect.exit(db.delete(SessionTable).where(eq(SessionTable.id, "session-b" as never)).run()),
    )
    expectDenied(exit, "drizzle delete session-b")

    // The root and its cascaded message/part are untouched.
    expect(snapshot(filename)).toEqual(before)
    expect(admin<{ count: number }>(filename, "SELECT COUNT(*) AS count FROM message WHERE session_id = 'session-b'")).toEqual(
      [{ count: 1 }],
    )
  })

  test("delete, update, and insert against another session's rows are refused", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const before = snapshot(filename)

    const exits = await withDrizzle(database, (db) =>
      Effect.gen(function* () {
        const deleteMessage = yield* Effect.exit(
          db.delete(MessageTable).where(eq(MessageTable.id, "message-session-b" as never)).run(),
        )
        const deletePart = yield* Effect.exit(db.delete(PartTable).where(eq(PartTable.id, "part-session-b" as never)).run())
        const update = yield* Effect.exit(
          db
            .update(MessageTable)
            .set({ data: JSON.stringify({ hijacked: true }) as never })
            .where(eq(MessageTable.id, "message-session-b" as never))
            .run(),
        )
        const insert = yield* Effect.exit(
          db
            .insert(MessageTable)
            .values({ id: "message-injected", session_id: "session-b", time_created: 1, time_updated: 1, data: "{}" } as never)
            .run(),
        )
        return { deleteMessage, deletePart, update, insert }
      }),
    )

    expectDenied(exits.deleteMessage, "drizzle delete message-session-b")
    expectDenied(exits.deletePart, "drizzle delete part-session-b")
    expectDenied(exits.update, "drizzle update message-session-b")
    expectDenied(exits.insert, "drizzle insert into session-b")

    expect(snapshot(filename)).toEqual(before)
  })

  test("the worker's own-session writes still succeed through the same path", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)

    const exits = await withDrizzle(database, (db) =>
      Effect.gen(function* () {
        const update = yield* Effect.exit(
          db
            .update(MessageTable)
            .set({ data: JSON.stringify({ own: true }) as never })
            .where(eq(MessageTable.id, "message-session-a" as never))
            .run(),
        )
        const insert = yield* Effect.exit(
          db
            .insert(MessageTable)
            .values({ id: "message-own-new", session_id: "session-a", time_created: 1, time_updated: 1, data: "{}" } as never)
            .run(),
        )
        const remove = yield* Effect.exit(
          db.delete(MessageTable).where(eq(MessageTable.id, "message-session-a" as never)).run(),
        )
        return { update, insert, remove }
      }),
    )

    expect(Exit.isSuccess(exits.update), `own update failed: ${String(exits.update)}`).toBe(true)
    expect(Exit.isSuccess(exits.insert), `own insert failed: ${String(exits.insert)}`).toBe(true)
    expect(Exit.isSuccess(exits.remove), `own delete failed: ${String(exits.remove)}`).toBe(true)

    expect(admin(filename, "SELECT id, session_id FROM message ORDER BY id")).toEqual([
      { id: "message-own-new", session_id: "session-a" },
      { id: "message-session-b", session_id: "session-b" },
    ])
  })
})
