// Repository authority whitelist (typed-repository projection follow-up).
//
// RED-first against the typed repository on a REAL bound worker:
//
//   1. `session_share` is hub-owned share state. A worker may read its own row
//      but every write path (insert/upsert/update/delete/insertMany) is refused.
//   2. `path` is control-plane relocation state (Moved is hub authority), so it
//      is not in the worker session update whitelist: a changed `path` is
//      rejected, an unchanged repeat is allowed.
//   3. A session upsert is update-only: after the hub removes the root, a worker
//      upsert cannot re-mint it with a forged parent/depth.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { openConnection } from "@opencode-ai/core/database/connection"
import { markSchemaIdentity } from "@/session/db/schema"
import { openWorkerDatabase, type WorkerDatabase } from "@/session/worker/db"
import { migrationStatements } from "@/session/writer/migration.statements.gen"

const PROJECT = "project-whitelist"
const DIRECTORY = "/tmp/whitelist"
const SESSION_A = "session-whitelist-a"
const SESSION_B = "session-whitelist-b"

const directories: string[] = []
const cleanups: Array<() => void | Promise<void>> = []

function workspace(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-typed-whitelist-"))
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
      [PROJECT, "whitelist", DIRECTORY, "[]", 1, 1],
    )
    for (const id of [SESSION_A, SESSION_B]) {
      connection.run(
        "INSERT INTO session (id, project_id, slug, directory, path, title, version, metadata, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
          id,
          PROJECT,
          id,
          DIRECTORY,
          `${DIRECTORY}/${id}`,
          id,
          "1",
          JSON.stringify({ depth: id === SESSION_A ? 1 : 2 }),
          1,
          1,
        ],
      )
      connection.run(
        "INSERT INTO session_share (session_id, id, secret, url, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
        [id, `share-${id}`, `secret-${id}`, `https://share.test/${id}`, 1, 1],
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

async function openBound(filename: string, sessionID = SESSION_A): Promise<WorkerDatabase> {
  const database = await Effect.runPromise(openWorkerDatabase({ filename, sessionID }))
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

interface Outcome {
  readonly ok: boolean
  readonly value?: unknown
  readonly error?: unknown
}

function settle(operation: () => unknown): Outcome {
  try {
    return { ok: true, value: operation() }
  } catch (error) {
    return { ok: false, error }
  }
}

function shareRows(filename: string) {
  return admin(filename, "SELECT session_id, id, secret, url FROM session_share ORDER BY session_id")
}

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("session_share is hub authority", () => {
  test("every worker write path is refused and the row is untouched", async () => {
    const filename = migratedDatabase()
    seed(filename)
    const database = await openBound(filename)
    const before = shareRows(filename)

    const attempts: ReadonlyArray<[string, () => unknown]> = [
      [
        "insert",
        () =>
          database.repository.insert("session_share", {
            session_id: SESSION_A,
            id: "forged-share",
            secret: "forged-secret",
            url: "https://evil.test",
            time_created: 1,
            time_updated: 1,
          }),
      ],
      [
        "upsert",
        () =>
          database.repository.upsert(
            "session_share",
            {
              session_id: SESSION_A,
              id: "forged-share",
              secret: "forged-secret",
              url: "https://evil.test",
              time_created: 1,
              time_updated: 1,
            },
            { columns: ["session_id"], update: ["url"] },
          ),
      ],
      [
        "update",
        () => database.repository.update("session_share", { url: "https://evil.test" }, { session_id: SESSION_A }),
      ],
      ["delete", () => database.repository.delete("session_share", { session_id: SESSION_A })],
      [
        "insertMany",
        () =>
          database.repository.insertMany("session_share", [
            {
              session_id: SESSION_A,
              id: "forged-share",
              secret: "forged-secret",
              url: "https://evil.test",
              time_created: 1,
              time_updated: 1,
            },
          ]),
      ],
    ]

    for (const [operation, run] of attempts) {
      const outcome = settle(run)
      expect(outcome.ok, `session_share ${operation} was accepted`).toBe(false)
    }

    expect(shareRows(filename)).toEqual(before)

    // The own-session row stays readable: the authority is writes, not reads.
    expect(database.repository.select("session_share", { session_id: SESSION_A })).toHaveLength(1)
  })
})

describe("session path is hub relocation authority", () => {
  test("a changed path is rejected while an unchanged repeat is allowed", async () => {
    const filename = migratedDatabase()
    seed(filename)
    const database = await openBound(filename)

    const changed = settle(() => database.repository.update("session", { path: "/tmp/evil" }, { id: SESSION_A }))
    expect(changed.ok, "session update accepted a path relocation").toBe(false)
    expect(admin(filename, "SELECT path FROM session WHERE id = ?", [SESSION_A])).toEqual([
      { path: `${DIRECTORY}/${SESSION_A}` },
    ])

    const unchanged = settle(() =>
      database.repository.update("session", { path: `${DIRECTORY}/${SESSION_A}`, title: "renamed" }, { id: SESSION_A }),
    )
    expect(unchanged.ok, `unchanged path repeat failed: ${String(unchanged.error)}`).toBe(true)
    expect(admin(filename, "SELECT path, title FROM session WHERE id = ?", [SESSION_A])).toEqual([
      { path: `${DIRECTORY}/${SESSION_A}`, title: "renamed" },
    ])
  })
})

describe("session upsert is update-only", () => {
  test("a vanished root cannot be re-minted with a forged parent or depth", async () => {
    const filename = migratedDatabase()
    seed(filename)
    const database = await openBound(filename)

    // Simulate the hub removing the root after the worker opened its binding.
    admin(filename, "DELETE FROM session_share WHERE session_id = ?", [SESSION_A])
    admin(filename, "DELETE FROM message WHERE session_id = ?", [SESSION_A])
    admin(filename, "DELETE FROM session WHERE id = ?", [SESSION_A])
    expect(admin(filename, "SELECT id FROM session WHERE id = ?", [SESSION_A])).toEqual([])

    const outcome = settle(() =>
      database.repository.upsert(
        "session",
        {
          id: SESSION_A,
          project_id: PROJECT,
          slug: "minted",
          directory: DIRECTORY,
          path: `${DIRECTORY}/forged`,
          title: "minted",
          version: "1",
          parent_id: SESSION_B,
          metadata: JSON.stringify({ depth: 0 }),
          time_created: 1,
          time_updated: 1,
        },
        { columns: ["id"], update: ["title"] },
      ),
    )

    expect(outcome.ok, "a vanished root was re-minted by a worker upsert").toBe(false)
    expect(admin(filename, "SELECT id FROM session WHERE id = ?", [SESSION_A])).toEqual([])
    expect(admin(filename, "SELECT path FROM session WHERE id = ?", [SESSION_B])).toEqual([
      { path: `${DIRECTORY}/${SESSION_B}` },
    ])
  })

  test("an existing root updates only through the worker column whitelist", async () => {
    const filename = migratedDatabase()
    seed(filename)
    const database = await openBound(filename)

    const outcome = settle(() =>
      database.repository.upsert(
        "session",
        {
          id: SESSION_A,
          project_id: PROJECT,
          slug: SESSION_A,
          directory: DIRECTORY,
          title: "updated-by-upsert",
          version: "1",
          metadata: JSON.stringify({ depth: 1 }),
          time_created: 1,
          time_updated: 2,
        },
        { columns: ["id"], update: ["title", "time_updated"] },
      ),
    )

    expect(outcome.ok, `own-session upsert failed: ${String(outcome.error)}`).toBe(true)
    expect(admin(filename, "SELECT title, time_updated FROM session WHERE id = ?", [SESSION_A])).toEqual([
      { title: "updated-by-upsert", time_updated: 2 },
    ])
  })

  test("an upsert cannot rewrite a hub-owned session column", async () => {
    const filename = migratedDatabase()
    seed(filename)
    const database = await openBound(filename)

    const outcome = settle(() =>
      database.repository.upsert(
        "session",
        {
          id: SESSION_A,
          project_id: PROJECT,
          slug: SESSION_A,
          directory: DIRECTORY,
          title: SESSION_A,
          version: "1",
          metadata: JSON.stringify({ depth: 1 }),
          time_created: 1,
          time_updated: 1,
        },
        { columns: ["id"], update: { title: SESSION_A, cost: 999 } },
      ),
    )

    expect(outcome.ok, "session upsert accepted a hub-owned cost rewrite").toBe(false)
    expect(admin(filename, "SELECT cost FROM session WHERE id = ?", [SESSION_A])).toEqual([{ cost: 0 }])
  })
})
