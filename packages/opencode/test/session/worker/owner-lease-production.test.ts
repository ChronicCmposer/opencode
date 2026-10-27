// C8 / W1.8 — a worker generation claims (or reclaims) its session lease at
// generation start, so a stale generation can no longer write and a resumed
// generation continues the durable sequence atomically.
//
// This drives the production `createWorkerDatabaseWriter` over two real
// per-worker WAL connections to the same file, not a Bus double.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { DateTime, Effect } from "effect"
import { Event } from "@opencode-ai/schema/event"
import { SessionEvent } from "@opencode-ai/schema/session-event"
import { Bus } from "@opencode-ai/core/bus"
import { openConnection } from "@opencode-ai/core/database/connection"
import { migrationStatements } from "@/session/writer/migration.statements.gen"
import { markSchemaIdentity, openWorkerDatabase, type WorkerDatabase } from "@/session/worker/db"
import { createWorkerDatabaseWriter } from "@/session/worker/session-worker"

const durable = (text: string) => ({
  id: Event.ID.create(),
  type: SessionEvent.AgentSwitched.type,
  aggregateID: "session-lease",
  data: { sessionID: "session-lease", timestamp: DateTime.makeUnsafe(Date.now()), messageID: "msg_owner_lease", agent: text },
})

const directories: string[] = []
const databases: WorkerDatabase[] = []

function migratedDatabase(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-owner-lease-"))
  directories.push(directory)
  const filename = path.join(directory, "opencode.db")
  const connection = openConnection({ filename })
  try {
    for (const statement of migrationStatements) connection.exec(statement)
    markSchemaIdentity(connection)
    connection.run(
      "INSERT INTO project (id, name, worktree, sandboxes, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
      ["project-lease", "lease", "/tmp/lease", "[]", 1, 1],
    )
    connection.run(
      "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      ["session-lease", "project-lease", "lease", "/tmp/lease", "lease", "1", 1, 1],
    )
  } finally {
    connection.close()
  }
  return filename
}

async function worker(filename: string): Promise<WorkerDatabase> {
  const database = await Effect.runPromise(openWorkerDatabase({ filename }))
  databases.push(database)
  return database
}

afterEach(async () => {
  for (const database of databases.splice(0)) await Effect.runPromise(database.close)
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("worker generation owner lease (C8)", () => {
  test("generation 2 reclaims the same aggregate atomically and fences generation 1", async () => {
    const filename = migratedDatabase()
    const first = await worker(filename)
    const second = await worker(filename)

    const gen1 = createWorkerDatabaseWriter({
      connection: first.connection,
      workerID: 1,
      sessionID: "session-lease",
    })
    const firstPublish = await gen1.publish(durable("gen-1"))
    expect(firstPublish.seq).toBe(0)

    const gen2 = createWorkerDatabaseWriter({
      connection: second.connection,
      workerID: 2,
      sessionID: "session-lease",
    })

    const secondPublish = await gen2.publish(durable("gen-2"))
    expect(secondPublish.seq).toBe(1)

    await expect(gen1.publish(durable("stale"))).rejects.toThrow(Bus.StaleOwnerError)

    const rows = first.connection.all<{ aggregate_id: string; seq: number; owner_id: string }>(
      "SELECT aggregate_id, seq, owner_id FROM event_sequence",
    )
    expect(rows).toEqual([{ aggregate_id: "session-lease", seq: 1, owner_id: expect.any(String) }])
  })

  test("two generations cannot both write after generation 2 reclaims", async () => {
    const filename = migratedDatabase()
    const first = await worker(filename)
    const second = await worker(filename)

    const gen1 = createWorkerDatabaseWriter({ connection: first.connection, workerID: 1, sessionID: "session-lease" })
    await gen1.publish(durable("gen-1"))

    const gen2 = createWorkerDatabaseWriter({ connection: second.connection, workerID: 2, sessionID: "session-lease" })
    await gen2.publish(durable("gen-2"))

    expect(first.connection.all<{ seq: number }>("SELECT seq FROM event_sequence")).toEqual([{ seq: 1 }])
    gen1.dispose()
    gen2.dispose()
  })
})
