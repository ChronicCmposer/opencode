// A worker generation claims only its own worker lease at
// generation start. A lease held by a live generation is refused; a dead one is
// reclaimed only at the observed sequence, atomically, so a raced commit fences
// the reclaim instead of being dropped. The v1 sync owner is never rewritten.
//
// This drives the production `createWorkerDatabaseWriter` over two real
// per-worker WAL connections to the same file, not a DurableLog double.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { DateTime, Effect } from "effect"
import { Event } from "@opencode-ai/schema/event"
import { SessionEvent } from "@opencode-ai/schema/session-event"
import { DurableLog } from "@opencode-ai/core/durable-log"
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

describe("worker generation owner lease", () => {
  test("a live generation is not reclaimed; a disposed generation is", async () => {
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

    // Gen-1 is alive: the lease has not expired, so gen-2 must not steal it.
    expect(() =>
      createWorkerDatabaseWriter({ connection: second.connection, workerID: 2, sessionID: "session-lease" }),
    ).toThrow(DurableLog.LeaseConflictError)

    gen1.dispose()

    const gen2 = createWorkerDatabaseWriter({
      connection: second.connection,
      workerID: 2,
      sessionID: "session-lease",
    })

    const secondPublish = await gen2.publish(durable("gen-2"))
    expect(secondPublish.seq).toBe(1)

    const rows = second.connection.all<{
      aggregate_id: string
      seq: number
      owner_id: string | null
      worker_owner_id: string
    }>("SELECT aggregate_id, seq, owner_id, worker_owner_id FROM event_sequence")
    expect(rows).toEqual([
      {
        aggregate_id: "session-lease",
        seq: 1,
        owner_id: null,
        worker_owner_id: expect.any(String),
      },
    ])
  })

  test("an expired lease can be reclaimed without a graceful dispose", async () => {
    const filename = migratedDatabase()
    const first = await worker(filename)
    const second = await worker(filename)

    const gen1 = createWorkerDatabaseWriter({ connection: first.connection, workerID: 1, sessionID: "session-lease" })
    await gen1.publish(durable("gen-1"))

    // A crashed generation never releases: its lease simply expires.
    first.connection.run("UPDATE event_sequence SET worker_expires_at = 0 WHERE aggregate_id = ?", ["session-lease"])

    const gen2 = createWorkerDatabaseWriter({ connection: second.connection, workerID: 2, sessionID: "session-lease" })
    const secondPublish = await gen2.publish(durable("gen-2"))
    expect(secondPublish.seq).toBe(1)

    await expect(gen1.publish(durable("late"))).rejects.toThrow(DurableLog.StaleOwnerError)
    expect(first.connection.all<{ seq: number }>("SELECT seq FROM event_sequence")).toEqual([{ seq: 1 }])
    gen1.dispose()
    gen2.dispose()
  })

  test("a same-owner lease renews on write after its TTL; a successor is still fenced", async () => {
    const filename = migratedDatabase()
    const first = await worker(filename)
    const second = await worker(filename)

    const gen1 = createWorkerDatabaseWriter({ connection: first.connection, workerID: 1, sessionID: "session-lease" })
    expect((await gen1.publish(durable("gen-1"))).seq).toBe(0)

    // The generation is still alive but its TTL elapsed (e.g. a long model turn
    // between writes). The same writer must renew on its next write rather than
    // self-fence. F8 reserves `worker_expires_at = 0` for a released lease, so
    // an elapsed TTL is a past positive timestamp.
    first.connection.run("UPDATE event_sequence SET worker_expires_at = ? WHERE aggregate_id = ?", [
      Date.now() - 1,
      "session-lease",
    ])
    expect((await gen1.publish(durable("gen-1-after-ttl"))).seq).toBe(1)

    const renewed = first.connection.all<{ worker_expires_at: number }>(
      "SELECT worker_expires_at FROM event_sequence WHERE aggregate_id = ?",
      ["session-lease"],
    )[0]
    expect(renewed?.worker_expires_at).toBeGreaterThan(0)

    // The renewed lease is live, so the successor cannot steal it.
    expect(() =>
      createWorkerDatabaseWriter({ connection: second.connection, workerID: 2, sessionID: "session-lease" }),
    ).toThrow(DurableLog.LeaseConflictError)

    gen1.dispose()
  })

  test("a worker heartbeat renews an idle lease and stops on dispose", async () => {
    const filename = migratedDatabase()
    const first = await worker(filename)
    // Observe the lease from an independent handle: the writer owns (and closes)
    // the connection it was given.
    const observer = openConnection({ filename })
    const lease = () =>
      observer.all<{ worker_expires_at: number }>(
        "SELECT worker_expires_at FROM event_sequence WHERE aggregate_id = ?",
        ["session-lease"],
      )[0]
    try {
      const gen = createWorkerDatabaseWriter({
        connection: first.connection,
        workerID: 1,
        sessionID: "session-lease",
        heartbeatIntervalMs: 20,
      })
      await gen.publish(durable("gen-1"))

      // Simulate the TTL elapsing between writes: the heartbeat must renew it.
      // `0` is the released sentinel, so an elapsed TTL is a past timestamp.
      observer.run("UPDATE event_sequence SET worker_expires_at = ? WHERE aggregate_id = ?", [
        Date.now() - 1_000,
        "session-lease",
      ])
      await new Promise((resolve) => setTimeout(resolve, 80))
      expect(lease()?.worker_expires_at).toBeGreaterThan(0)

      gen.dispose()
      observer.run("UPDATE event_sequence SET worker_expires_at = 0 WHERE aggregate_id = ?", ["session-lease"])
      await new Promise((resolve) => setTimeout(resolve, 60))
      expect(lease()?.worker_expires_at).toBe(0)
    } finally {
      observer.close()
    }
  })

  test("the sync owner is never rewritten by a worker claim", async () => {
    const filename = migratedDatabase()
    const first = await worker(filename)
    first.connection.run("INSERT INTO event_sequence (aggregate_id, seq, owner_id) VALUES (?, -1, ?)", [
      "session-lease",
      "workspace-a",
    ])

    const gen = createWorkerDatabaseWriter({ connection: first.connection, workerID: 1, sessionID: "session-lease" })
    await gen.publish(durable("gen-1"))

    expect(first.connection.all("SELECT owner_id, worker_owner_id FROM event_sequence")).toEqual([
      { owner_id: "workspace-a", worker_owner_id: expect.any(String) },
    ])
    gen.dispose()
  })
})
