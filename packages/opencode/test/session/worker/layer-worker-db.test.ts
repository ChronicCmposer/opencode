// W1.1 / W1.5 / W1.6 — the worker graph is backed by its own WAL connection.
//
// The per-worker DB node replaces the writer-proxied Database replacement. The
// node appears exactly once, the graph's `Database.Service` is the connection
// opened for this worker, and disposing the graph closes the handle.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import { sql } from "drizzle-orm"
import { openConnection } from "@opencode-ai/core/database/connection"
import { Session } from "@/session/session"
import { SessionID } from "@/session/schema"
import { Database } from "@opencode-ai/core/database/database"
import { migrationStatements } from "@/session/writer/migration.statements.gen"
import {
  HubOnlyTableError,
  WorkerDb,
  databaseLayer,
  markSchemaIdentity,
  openWorkerDatabase,
  type WorkerDatabase,
} from "@/session/worker/db"
import {
  ModelClient,
  WorkerDbNode,
  buildSessionWorkerLayer,
  type SessionLayerOptions,
} from "@/session/worker/layer"
import { collectNodeNames, REQUIRED_SESSION_NODE_KEYS } from "@/session/worker/node-table"
import { workerSnapshot } from "../../lib/snapshot"

const directories: string[] = []
const databases: WorkerDatabase[] = []

function temporaryDatabase(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-layer-db-"))
  directories.push(directory)
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
  } finally {
    connection.close()
  }
}

async function workerDatabase(filename: string): Promise<WorkerDatabase> {
  const database = await Effect.runPromise(openWorkerDatabase({ filename }))
  databases.push(database)
  return database
}

const options = (database: WorkerDatabase): SessionLayerOptions => ({
  snapshot: workerSnapshot({ directory: "/tmp/project" }),
  env: { values: {} },
  clients: {
    writer: { query: async () => [], publish: async () => ({ seq: null, committed: false }) },
    egress: { emit: () => {} },
    backgroundJob: { wait: async () => null, cancel: async () => {} },
    database,
  },
  modelClient: Layer.succeed(ModelClient, {
    complete: () => Effect.succeed({ text: "unused" }),
  }),
})

afterEach(async () => {
  for (const database of databases.splice(0)) await Effect.runPromise(database.close)
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("per-worker DB node (W1.1)", () => {
  test("databaseLayer alone provides Database.Service", async () => {
    const filename = temporaryDatabase()
    migrated(filename)
    const database = await workerDatabase(filename)
    const rows = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* (yield* Database.Service).db.all<{ id: string }>(sql`SELECT id FROM session`)
      }).pipe(Effect.provide(databaseLayer(database))),
    )
    expect(rows).toEqual([{ id: "session-a" }])
  })

  test("appears once and is the graph's Database", async () => {
    const filename = temporaryDatabase()
    migrated(filename)
    const database = await workerDatabase(filename)
    const built = buildSessionWorkerLayer(options(database))
    expect(built.replacements.some(([node]) => node.name === Database.node.name)).toBe(true)

    const workers = collectNodeNames(built.root)
    expect([...workers].filter((name) => name === WorkerDbNode.name)).toHaveLength(1)
    expect(workers.has(Database.node.name)).toBe(true)
    for (const key of REQUIRED_SESSION_NODE_KEYS) expect(workers.has(key)).toBe(true)

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const workerDb = yield* WorkerDb
        const session = yield* Session.Service
        const info = yield* session.get(SessionID.make("session-a"))
        return { same: workerDb === database, id: info.id }
      }).pipe(Effect.provide(built.layer)),
    )

    expect(result.same).toBe(true)
    expect(result.id).toBe(SessionID.make("session-a"))
    expect(database.connection.closed).toBe(true)
  })

  test("hub-only writes fail loud through the worker Database", async () => {
    const filename = temporaryDatabase()
    migrated(filename)
    const database = await workerDatabase(filename)
    const built = buildSessionWorkerLayer(options(database))

    const error = await Effect.runPromise(
      Effect.gen(function* () {
        const workerDb = yield* WorkerDb
        return yield* workerDb.run("INSERT INTO permission (id) VALUES ('nope')").pipe(Effect.flip)
      }).pipe(Effect.provide(built.layer)),
    )

    expect(error).toBeInstanceOf(HubOnlyTableError)
    expect((error as HubOnlyTableError).table).toBe("permission")
    expect(database.connection.closed).toBe(true)
  })
})
