// Final typed-repository acceptance matrix — durable/event/blob/lease half.
//
// Real migrated SQLite file + real bound worker. The durable log has exactly one
// write boundary (`Bus.DurableRepository`) reached through the narrow
// `WorkerDatabase.bus` capability:
//
//   * the Bus handles expose typed publish/read/claim/reclaim/release only — no
//     `connection`, `transaction`, `transactionAsync`, `run`, `exec`, or `all`;
//   * events, sequences, and oversized blob rows are written and read back;
//   * a Bus handle is scoped to the worker's one aggregate;
//   * the worker lease fences a stale generation: it cannot claim, publish,
//     heartbeat, or reclaim a live successor, and only the released generation
//     can be reclaimed;
//   * `event`/`event_sequence`/`event_blob` are refused through the typed
//     repository and through the public raw connection.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Effect, Schema } from "effect"
import { Event } from "@opencode-ai/schema/event"
import { Bus } from "@opencode-ai/core/bus"
import { openConnection } from "@opencode-ai/core/database/connection"
import { markSchemaIdentity } from "@/session/db/schema"
import { openWorkerDatabase, type WorkerDatabase } from "@/session/worker/db"
import { migrationStatements } from "@/session/writer/migration.statements.gen"

const Note = Event.define({
  type: "test.typed.final.note",
  durable: { version: 1, aggregate: "sessionID" },
  schema: {
    sessionID: Schema.String,
    text: Schema.String,
  },
})

const SESSION = "session-final-durable"
const directories: string[] = []
const cleanups: Array<() => void | Promise<void>> = []

function workspace(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-typed-final-durable-"))
  directories.push(directory)
  return directory
}

function migratedWorkerFile(): string {
  const filename = path.join(workspace(), "worker.db")
  const connection = openConnection({ filename })
  try {
    for (const statement of migrationStatements) connection.exec(statement)
    markSchemaIdentity(connection)
    connection.run(
      "INSERT INTO project (id, name, worktree, sandboxes, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
      ["project-final-durable", "final", "/tmp/final-durable", "[]", 1, 1],
    )
    connection.run(
      "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      [SESSION, "project-final-durable", "final", "/tmp/final-durable", "final", "1", 1, 1],
    )
  } finally {
    connection.close()
  }
  return filename
}

async function openBound(filename: string): Promise<WorkerDatabase> {
  const database = await Effect.runPromise(openWorkerDatabase({ filename, sessionID: SESSION }))
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

const note = (text: string) => ({ sessionID: SESSION, text })
const textOf = (event: Event.Payload) => (event.data as { text: string }).text
const oversized = (seed: string) => `${seed}:${"x".repeat(4_096)}`

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("durable log has one typed boundary and no raw handle", () => {
  test("Bus handles expose only typed operations, and event sequence/blob writes work end to end", async () => {
    const filename = migratedWorkerFile()
    const database = await openBound(filename)
    const blobDir = path.join(path.dirname(filename), "event-blobs")

    const bus = database.bus
    expect(bus).not.toHaveProperty("connection")
    expect(bus).not.toHaveProperty("transaction")
    expect(bus).not.toHaveProperty("transactionAsync")

    const handle = bus.create({
      ownerID: "final-gen-1",
      definitions: [Note],
      aggregateID: SESSION,
      blobLimitBytes: 64,
    })
    expect(handle).not.toHaveProperty("connection")
    expect(handle).not.toHaveProperty("transaction")
    expect(handle).not.toHaveProperty("transactionAsync")
    for (const forbidden of ["run", "exec", "all", "connection", "transaction", "transactionAsync"]) {
      expect((handle as unknown as Record<string, unknown>)[forbidden], `bus.${forbidden}`).toBeUndefined()
    }
    for (const method of ["publish", "read", "claim", "reclaim", "release", "heartbeat", "deleteAggregate"]) {
      expect(typeof (handle as unknown as Record<string, unknown>)[method], `bus.${method}`).toBe("function")
    }

    const texts = [oversized("final-small-target"), oversized("final-blob-target")]
    const published = handle.publish([
      { definition: Note, data: note(texts[0]!) },
      { definition: Note, data: note(texts[1]!) },
    ])
    expect(published.map((event) => event.durable?.seq)).toEqual([0, 1])
    expect(handle.read({ aggregateID: SESSION }).map(textOf)).toEqual(texts)
    expect(fs.readdirSync(blobDir)).toHaveLength(2)

    // The rows are the production durable schema, written through the typing.
    expect(
      admin<{ count: number }>(filename, "SELECT COUNT(*) AS count FROM event WHERE aggregate_id = ?", [SESSION]),
    ).toEqual([{ count: 2 }])
    expect(
      admin<{ count: number }>(filename, "SELECT COUNT(*) AS count FROM event_blob WHERE aggregate_id = ?", [SESSION]),
    ).toEqual([{ count: 2 }])
    expect(
      admin<{ seq: number }>(filename, "SELECT seq FROM event_sequence WHERE aggregate_id = ?", [SESSION]),
    ).toEqual([{ seq: 1 }])

    // The same worker cannot write the durable log through the repository or the
    // public raw connection.
    const repository = await settle(() =>
      database.repository.insert(
        "event" as never,
        {
          id: "final-repository-event",
          aggregate_id: SESSION,
          seq: 2,
          type: Note.type,
          data: "{}",
        } as never,
      ),
    )
    expect(repository.ok).toBe(false)
    expect(errorTag(repository.error)).toContain("UnsupportedWriteError")

    const raw = await settle(() =>
      database.connection.run("INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?, ?, ?, ?, ?)", [
        "final-raw-event",
        SESSION,
        2,
        Note.type,
        "{}",
      ]),
    )
    expect(raw.ok).toBe(false)
    expect(
      admin<{ count: number }>(filename, "SELECT COUNT(*) AS count FROM event WHERE id = 'final-raw-event'"),
    ).toEqual([{ count: 0 }])
  }, 60_000)

  test("the durable capability is scoped to the worker's one aggregate", async () => {
    const filename = migratedWorkerFile()
    const database = await openBound(filename)

    expect(() => database.bus.create({ ownerID: "final-foreign", aggregateID: "session-other" })).toThrow()
    expect(() => database.bus.deleteAggregate("session-other")).toThrow()

    const handle = database.bus.create({ ownerID: "final-scope", definitions: [Note], aggregateID: SESSION })
    expect(() => handle.deleteAggregate("session-other")).toThrow()

    const foreign = await settle(() => handle.publish([{ definition: Note, data: note("foreign aggregate") }]).length)
    expect(foreign.ok).toBe(true)
    expect(
      admin<{ count: number }>(filename, "SELECT COUNT(*) AS count FROM event WHERE aggregate_id = 'session-other'"),
    ).toEqual([{ count: 0 }])
  }, 60_000)
})

describe("the worker lease fences a stale generation", () => {
  test("claim/publish/heartbeat/reclaim are owner-exclusive; release unlocks the successor", async () => {
    const filename = migratedWorkerFile()
    const database = await openBound(filename)
    const options = {
      definitions: [Note],
      aggregateID: SESSION,
      leaseTimeoutMs: 60_000,
    } as const

    const first = database.bus.create({ ...options, ownerID: "final-gen-1" })
    const [event] = first.publish([{ definition: Note, data: note("generation one") }])
    expect(event?.durable?.seq).toBe(0)
    const seq = first.latest(SESSION)
    expect(seq).toBe(0)

    const second = database.bus.create({ ...options, ownerID: "final-gen-2" })
    expect(() => second.claim(SESSION)).toThrow(Bus.StaleOwnerError)
    // The predecessor's lease is still live, so a reclaim is refused.
    expect(() =>
      second.reclaim({
        aggregateID: SESSION,
        expectedOwnerID: "final-gen-1",
        expectedSeq: seq,
        nextOwnerID: "final-gen-2",
      }),
    ).toThrow(Bus.LeaseConflictError)
    expect(second.ownerOf(SESSION)).toBe("final-gen-1")

    first.release(SESSION)
    const reclaimed = second.reclaim({
      aggregateID: SESSION,
      expectedOwnerID: "final-gen-1",
      expectedSeq: seq,
      nextOwnerID: "final-gen-2",
    })
    expect(reclaimed.ownerID).toBe("final-gen-2")
    expect(second.ownerOf(SESSION)).toBe("final-gen-2")

    // The successor writes; the stale generation is fenced on every write path.
    second.heartbeat(SESSION)
    const next = second.publish([{ definition: Note, data: note("generation two") }])
    expect(next[0]?.durable?.seq).toBe(1)
    expect(() => first.publish([{ definition: Note, data: note("stale write") }])).toThrow(Bus.StaleOwnerError)
    expect(() => first.heartbeat(SESSION)).toThrow(Bus.StaleOwnerError)

    expect(first.read({ aggregateID: SESSION }).map(textOf)).toEqual(["generation one", "generation two"])
  }, 60_000)
})
