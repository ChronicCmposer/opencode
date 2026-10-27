// P0 lease/sync separation.
//
// A workspace-synced session stores its sync owner in
// `event_sequence.owner_id`; v1 replication is fenced on that value. A worker
// generation must claim only its own worker lease, so claiming the aggregate
// never rewrites the sync owner and a later sync replay from that workspace
// still commits.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { DateTime, Effect, Layer } from "effect"
import { Event } from "@opencode-ai/schema/event"
import { SessionEvent } from "@opencode-ai/schema/session-event"
import { openConnection } from "@opencode-ai/core/database/connection"
import { EventV2, setWorkerLeaseOwner } from "@opencode-ai/core/event"
import { migrationStatements } from "@/session/writer/migration.statements.gen"
import { markSchemaIdentity, openWorkerDatabase, databaseLayer, type WorkerDatabase } from "@/session/worker/db"
import { createWorkerDatabaseWriter } from "@/session/worker/session-worker"

const SESSION = "session-lease-sync"
const WORKSPACE = "workspace-a"

const directories: string[] = []
const databases: WorkerDatabase[] = []

function migratedDatabase(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-lease-sync-"))
  directories.push(directory)
  const filename = path.join(directory, "opencode.db")
  const connection = openConnection({ filename })
  try {
    for (const statement of migrationStatements) connection.exec(statement)
    markSchemaIdentity(connection)
    connection.run(
      "INSERT INTO project (id, name, worktree, sandboxes, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
      ["project-lease-sync", "lease", "/tmp/lease-sync", "[]", 1, 1],
    )
    connection.run(
      "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      [SESSION, "project-lease-sync", "lease", "/tmp/lease-sync", "lease", "1", 1, 1],
    )
    // v1 sync claim: the workspace owns the aggregate before any worker boots.
    connection.run("INSERT INTO event_sequence (aggregate_id, seq, owner_id) VALUES (?, -1, ?)", [
      SESSION,
      WORKSPACE,
    ])
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

const switched = (agent: string) => ({
  sessionID: SESSION,
  timestamp: DateTime.makeUnsafe(Date.now()),
  messageID: "msg_lease_sync",
  agent,
})

/** The serialized payload shape `replay` consumes (millis, not a DateTime). */
const switchedWire = (agent: string) => ({
  ...switched(agent),
  timestamp: Date.now(),
})

afterEach(async () => {
  for (const database of databases.splice(0)) {
    await Effect.runPromise(database.close).catch(() => undefined)
  }
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("worker lease vs sync owner (P0)", () => {
  test("a worker claim leaves the sync owner intact and v1 replay still commits", async () => {
    const filename = migratedDatabase()
    const sync = await worker(filename)
    const workerDatabase = await worker(filename)

    const generation = createWorkerDatabaseWriter({
      connection: workerDatabase.connection,
      workerID: 1,
      sessionID: SESSION,
    })
    const first = await generation.publish({
      id: Event.ID.create(),
      type: SessionEvent.AgentSwitched.type,
      aggregateID: SESSION,
      data: switched("gen-1"),
    })
    expect(first.seq).toBe(0)

    const sequence = sync.connection.all<{ owner_id: string | null; worker_owner_id: string | null }>(
      "SELECT owner_id, worker_owner_id FROM event_sequence WHERE aggregate_id = ?",
      [SESSION],
    )
    expect(sequence).toHaveLength(1)
    expect(sequence[0]?.owner_id).toBe(WORKSPACE)
    expect(sequence[0]?.worker_owner_id).toEqual(expect.any(String))

    // The hub replays v1 in its own isolate: no worker identity is active.
    setWorkerLeaseOwner(undefined)
    const syncLayer = EventV2.layerWith().pipe(Layer.provide(databaseLayer(sync)))
    await Effect.runPromise(
      Effect.gen(function* () {
        const events = yield* EventV2.Service
        yield* events.replay(
          {
            id: Event.ID.create(),
            type: EventV2.versionedType(SessionEvent.AgentSwitched.type, 1),
            seq: 1,
            aggregateID: SESSION,
            data: switchedWire("sync"),
          },
          { ownerID: WORKSPACE, strictOwner: true },
        )
      }).pipe(Effect.provide(syncLayer)),
    )

    const admin = openConnection({ filename })
    try {
      const rows = admin.all<{ seq: number }>("SELECT seq FROM event WHERE aggregate_id = ? ORDER BY seq", [SESSION])
      expect(rows.map((row) => row.seq)).toEqual([0, 1])
      expect(admin.all("SELECT owner_id FROM event_sequence WHERE aggregate_id = ?", [SESSION])).toEqual([
        { owner_id: WORKSPACE },
      ])
    } finally {
      admin.close()
      generation.dispose()
    }
  })

  test("a stale generation's durable write fails with the typed owner error", async () => {
    const filename = migratedDatabase()
    const first = await worker(filename)
    const second = await worker(filename)
    const third = await worker(filename)

    const gen1 = createWorkerDatabaseWriter({ connection: first.connection, workerID: 1, sessionID: SESSION })
    const staleOwner = first.connection.all<{ worker_owner_id: string }>(
      "SELECT worker_owner_id FROM event_sequence WHERE aggregate_id = ?",
      [SESSION],
    )[0]?.worker_owner_id
    expect(staleOwner).toEqual(expect.any(String))

    // Crash gen-1 (expire the lease) and let gen-2 take it.
    first.connection.run("UPDATE event_sequence SET worker_expires_at = 0 WHERE aggregate_id = ?", [SESSION])
    const gen2 = createWorkerDatabaseWriter({ connection: second.connection, workerID: 2, sessionID: SESSION })
    expect(
      second.connection.all<{ worker_owner_id: string }>(
        "SELECT worker_owner_id FROM event_sequence WHERE aggregate_id = ?",
        [SESSION],
      )[0]?.worker_owner_id,
    ).not.toBe(staleOwner)

    // A stale generation attempts an EventV2 durable commit.
    setWorkerLeaseOwner(staleOwner)
    const eventLayer = EventV2.layerWith().pipe(Layer.provide(databaseLayer(third)))
    const exit = await Effect.runPromise(
      Effect.gen(function* () {
        const events = yield* EventV2.Service
        return yield* events
          .publish(SessionEvent.AgentSwitched, switched("stale") as never)
          .pipe(Effect.exit)
      }).pipe(Effect.provide(eventLayer)),
    )
    setWorkerLeaseOwner(undefined)

    expect(exit._tag).toBe("Failure")
    expect(String(exit)).toContain("EventV2.StaleOwner")

    gen1.dispose()
    gen2.dispose()
  })
})
