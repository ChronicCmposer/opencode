// Runtime/data contracts: orderly DB close, worker cap, typed build mismatch,
// D16 blobs, session scope, migrations-once, non-blocking busy backoff, the
// derived zero-live-worker gate, the DurableLog write contract, and the per-worker
// page-cache budget.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test"
import { Database as BunDatabase } from "bun:sqlite"
import { Effect, Schema } from "effect"
import { Event } from "@opencode-ai/schema/event"
import { DurableLog } from "@opencode-ai/core/durable-log"
import { openConnection } from "@opencode-ai/core/database/connection"
import {
  DEFAULT_WORKER_CACHE_SIZE_KIB,
  resolveWorkerCacheSizeKib,
  walPragmas,
} from "@opencode-ai/core/database/pragmas"
import { inImmediateTransactionAsync, retryBusyAsync } from "@opencode-ai/core/database/transaction"
import { createDbExecutor } from "@/session/worker/db-executor"
import { buildHashStatus, verifyReadyBuild } from "@/session/worker/build-hash"
import { createProductionSessionHub } from "@/session/worker/hub"
import { createTestBarrier } from "../lib/run-barrier-test"
import { createLazyExecutorWorkerPool } from "../lib/lazy-worker-pool"
import { migrationStatements, ledgerStatements } from "@/session/writer/migration.statements.gen"
import { currentSchemaIdentity, ensureProductionSchema, markSchemaIdentity, schemaIdentity } from "@/session/db/schema"
import {
  ScopeViolationError,
  UnsupportedWriteError,
  WorkerStatementRejectedError,
  openWorkerDatabase,
  type WorkerDatabase,
} from "@/session/worker/db"
import type { ExecutorWorker } from "@/session/worker/execute"
import { sessionWorkerInit } from "../lib/session-worker-init"
import { REAL_WORKER_TEST_TIMEOUT_MS } from "../lib/worker-timeouts"

// Bun resets the per-file default to 5s; this file boots production hubs.
setDefaultTimeout(REAL_WORKER_TEST_TIMEOUT_MS)

const directories: string[] = []
const cleanups: Array<() => void | Promise<void>> = []

function workspace(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-runtime-p1-"))
  directories.push(directory)
  return directory
}

function migratedDatabase(): string {
  const directory = workspace()
  const filename = path.join(directory, "opencode.db")
  const connection = openConnection({ filename })
  try {
    for (const statement of migrationStatements) connection.exec(statement)
    markSchemaIdentity(connection)
  } finally {
    connection.close()
  }
  return filename
}

function seedSessions(filename: string): void {
  const connection = openConnection({ filename })
  try {
    connection.run(
      "INSERT INTO project (id, name, worktree, sandboxes, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
      ["project-p1", "p1", "/tmp/p1", "[]", 1, 1],
    )
    for (const id of ["session-a", "session-b"]) {
      connection.run(
        "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [id, "project-p1", id, "/tmp/p1", id, "1", 1, 1],
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

async function openWorker(filename: string, sessionID?: string): Promise<WorkerDatabase> {
  const database = await Effect.runPromise(
    openWorkerDatabase({ filename, ...(sessionID === undefined ? {} : { sessionID }) }),
  )
  cleanups.push(() => Effect.runPromise(database.close))
  return database
}

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

const Note = Event.define({
  type: "test.runtime.p1.note",
  durable: { version: 1, aggregate: "sessionID" },
  schema: { sessionID: Schema.String, text: Schema.String },
})

describe("orderly SQLite close", () => {
  test("a closed db executor leaves the WAL writable by the main process", async () => {
    const directory = workspace()
    const filename = path.join(directory, "executor.db")
    const executor = createDbExecutor({ filename })
    await executor.run("CREATE TABLE item (id INTEGER PRIMARY KEY, name TEXT)")
    await executor.run("INSERT INTO item (name) VALUES (?)", ["before"])
    await executor.close()

    const main = new BunDatabase(filename)
    try {
      main.exec("BEGIN IMMEDIATE")
      main.query("INSERT INTO item (name) VALUES (?)").run("after")
      main.exec("COMMIT")
      expect(main.query("SELECT name FROM item ORDER BY id").all()).toEqual([{ name: "before" }, { name: "after" }])
      // A stuck WAL lock would make this truncating checkpoint fail/busy.
      main.exec("PRAGMA wal_checkpoint(TRUNCATE)")
    } finally {
      main.close()
    }
  })
})

describe("worker cap + queue (D5)", () => {
  test("eight sessions run at most the cap concurrently and all complete", async () => {
    let running = 0
    let peak = 0
    const releases: Array<() => void> = []
    const create = (id: number): ExecutorWorker => ({
      id,
      alive: true,
      execute: async () => {
        running += 1
        peak = Math.max(peak, running)
        await new Promise<void>((resolve) => releases.push(resolve))
        running -= 1
      },
      cancel: async () => {},
      assertNotBusy: async () => false,
      resultFor: () => undefined,
      cancelPort: { notify: () => {}, onMessage: () => () => {} },
      terminate: async () => {},
    })
    const pool = createLazyExecutorWorkerPool({ create, maxRunning: 2 })
    const workers = Array.from({ length: 8 }, (_, index) => pool.spawn(index + 1))
    const runs = workers.map((worker) => worker.execute({ sessionID: `ses_${worker.id}` as never, depth: 0 }))

    // Let the first two start and queue the other six.
    await Bun.sleep(10)
    expect(peak).toBe(2)
    while (releases.length > 0) {
      releases.shift()?.()
      await Bun.sleep(0)
    }
    await Promise.all(runs)
    expect(peak).toBe(2)
    expect(running).toBe(0)
    await pool.terminateAll()
  })
})

describe("compiled build mismatch (D15)", () => {
  test("a different bundle constant is a typed BuildMismatch, not a generic error", () => {
    const hub = buildHashStatus({ env: {}, dev: true })
    expect(() =>
      verifyReadyBuild({ type: "ready", threadId: 1, buildHash: "different-bundle" }, hub.buildHash),
    ).toThrow(/build mismatch/)
    try {
      verifyReadyBuild({ type: "ready", threadId: 1, buildHash: "different-bundle" }, hub.buildHash)
    } catch (error) {
      expect(error).toBeInstanceOf(Error)
      expect((error as { _tag?: string })._tag).toBe("BuildMismatch")
    }
  })
})

describe("event_blob D16", () => {
  test("an oversized durable payload is stored by reference and fetched through the DurableLog", () => {
    const directory = workspace()
    const filename = path.join(directory, "blob.db")
    const blobDir = path.join(directory, "blobs")
    const native = new BunDatabase(filename)
    try {
      for (const statement of migrationStatements) native.exec(statement)
    } finally {
      native.close()
    }
    const connection = openConnection({ filename })
    try {
      markSchemaIdentity(connection)
      const bus = DurableLog.create({
        connection,
        ownerID: "gen-blob",
        definitions: [Note],
        blobDir,
        blobLimitBytes: 64,
      })
      const text = "x".repeat(4_096)
      bus.publish([{ definition: Note, data: { sessionID: "session-a", text } }])

      expect(fs.readdirSync(blobDir)).toHaveLength(1)
      expect(connection.all("SELECT COUNT(*) AS count FROM event_blob")).toEqual([{ count: 1 }])
      expect(connection.all<{ data: string }>("SELECT data FROM event")[0]?.data ?? "").toContain("__eventBlob")

      const read = bus.read({ aggregateID: "session-a" })
      expect((read[0]?.data as { text: string }).text).toBe(text)

      expect(bus.collectBlobs({ aggregateID: "session-a", beforeSeq: 1 })).toBe(1)
      expect(fs.readdirSync(blobDir)).toEqual([])
    } finally {
      connection.close()
    }
  })
})

describe("session-scoped worker tables (D6)", () => {
  test("worker A cannot read or delete worker B's rows", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    const workerA = await openWorker(filename, "session-a")
    const workerB = await openWorker(filename, "session-b")

    // The bound view filters the scan to worker A's session.
    expect(await Effect.runPromise(workerA.all("SELECT id FROM message ORDER BY id"))).toEqual([
      { id: "message-session-a" },
    ])

    const own = await Effect.runPromise(
      workerA.all<{ id: string }>("SELECT id FROM message WHERE session_id = ?", ["session-a"]),
    )
    expect(own).toEqual([{ id: "message-session-a" }])

    // The typed repository refuses an explicit foreign-session target, so worker
    // B's row is untouched; there is no raw bound `run` to fall back to.
    expect((workerA as { readonly run?: unknown }).run).toBeUndefined()
    expect(() => workerA.repository.delete("message", { session_id: "session-b" })).toThrow(ScopeViolationError)

    const otherStillThere = await Effect.runPromise(
      workerB.all<{ id: string }>("SELECT id FROM message WHERE session_id = ?", ["session-b"]),
    )
    expect(otherStillThere).toEqual([{ id: "message-session-b" }])
  })
})

describe("migrations once (D7)", () => {
  test("core owns the schema; the hub applies only the ledger once", () => {
    // The generated ledger subset must never replay core DDL.
    expect(ledgerStatements.some((statement) => /CREATE TABLE IF NOT EXISTS `session`/i.test(statement))).toBe(false)
    expect(ledgerStatements.some((statement) => /tool_call/i.test(statement))).toBe(true)

    const directory = workspace()
    const filename = path.join(directory, "fresh.db")
    expect(ensureProductionSchema({ filename, liveWorkers: 0 })).toEqual(currentSchemaIdentity())
    // Idempotent: a second boot neither duplicates journal rows nor replays DDL.
    expect(ensureProductionSchema({ filename, liveWorkers: 0 })).toEqual(currentSchemaIdentity())
    const connection = openConnection({ filename })
    try {
      expect(schemaIdentity(connection)).toEqual(currentSchemaIdentity())
      const ids = connection.all<{ id: string }>("SELECT id FROM migration ORDER BY id")
      expect(new Set(ids.map((row) => row.id)).size).toBe(ids.length)
      expect(ids.some((row) => row.id.includes("tool_call_ledger"))).toBe(true)
    } finally {
      connection.close()
    }
  })

  test("an existing app-migrated database is upgraded without replaying the final schema", () => {
    const directory = workspace()
    const filename = path.join(directory, "upgrade.db")
    // A legacy app DB: core migrations plus the ledger applied by the old path.
    const legacy = openConnection({ filename })
    try {
      for (const statement of migrationStatements) legacy.exec(statement)
    } finally {
      legacy.close()
    }
    expect(ensureProductionSchema({ filename, liveWorkers: 0 })).toEqual(currentSchemaIdentity())
  })
})

describe("non-blocking busy backoff (D5)", () => {
  test("the async retry path yields to the event loop between attempts", async () => {
    let ticks = 0
    const timer = setInterval(() => {
      ticks += 1
    }, 5)
    try {
      let attempts = 0
      const result = await retryBusyAsync(
        () => {
          attempts += 1
          if (attempts < 3) {
            throw Object.assign(new Error("SQLITE_BUSY: database is locked"), { code: "SQLITE_BUSY" })
          }
          return "ok"
        },
        { retries: 5, baseDelayMs: 25, maxDelayMs: 25 },
      )
      expect(result).toBe("ok")
      expect(attempts).toBe(3)
      expect(ticks).toBeGreaterThan(0)
    } finally {
      clearInterval(timer)
    }
  })

  test("an async transaction retries the whole unit without blocking", async () => {
    const connection = openConnection({ filename: ":memory:" })
    try {
      let attempts = 0
      const result = await inImmediateTransactionAsync(
        {
          all: (sql: string) => connection.all(sql),
          run: (sql: string) => connection.run(sql),
          exec: (sql: string) => {
            if (sql === "BEGIN IMMEDIATE" && attempts === 0) {
              attempts += 1
              throw Object.assign(new Error("SQLITE_BUSY"), { code: "SQLITE_BUSY" })
            }
            connection.exec(sql)
          },
          pragma: (name: string) => connection.pragma(name),
          journalMode: () => connection.journalMode(),
          close: () => {},
          owner: "test",
          filename: ":memory:",
          get closed() {
            return false
          },
          values: (sql: string) => connection.values(sql),
        },
        () => "committed",
        { retries: 2, baseDelayMs: 5, maxDelayMs: 5 },
      )
      expect(result).toBe("committed")
      expect(attempts).toBe(1)
    } finally {
      connection.close()
    }
  })
})

describe("zero-live-worker gate (D7)", () => {
  test("migration refuses when the derived live count is not zero", () => {
    const directory = workspace()
    const filename = path.join(directory, "gate.db")
    expect(() => ensureProductionSchema({ filename, liveWorkers: 1 })).toThrow(/zero live workers/)
    expect(fs.existsSync(filename)).toBe(false)
  })

  test("a hub's first dispatch refuses to migrate while its derived live count is nonzero", async () => {
    const directory = workspace()
    const filename = path.join(directory, "spawn-during-migration.db")
    // The schema gate is off-loop and lazy: construction stays sync, the first
    // dispatch awaits it and observes the typed live-worker refusal before any
    // isolate is spawned.
    const hub = createProductionSessionHub({ ...{ barrier: createTestBarrier() },
      init: sessionWorkerInit(),
      databasePath: filename,
      liveWorkers: () => 1,
    })
    try {
      await expect(hub.ensureRunning("ses_gate" as never)).rejects.toThrow(/zero live workers/)
    } finally {
      await hub.terminateAll()
    }
  })
})

describe("DurableLog write contract (D13)", () => {
  test("ad-hoc durable writes are rejected; the trusted DurableLog path still works", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    const database = await openWorker(filename, "session-a")

    // A bound worker exposes no raw `run`; every write must go through the
    // repository or the trusted DurableLog.
    expect((database as { readonly run?: unknown }).run).toBeUndefined()

    // The repository refuses durable tables outright: they belong to the DurableLog.
    // (The cast models an untyped caller; the typed surface makes this a
    // compile error.)
    const untyped = database.repository as unknown as {
      readonly insert: (table: string, values: Record<string, unknown>) => unknown
      readonly delete: (table: string, where: Record<string, unknown>) => unknown
    }
    expect(() =>
      untyped.insert("event", { id: "evt-1", aggregate_id: "session-a", seq: 0, type: "test", data: "{}" }),
    ).toThrow(UnsupportedWriteError)
    expect(() => untyped.delete("event_sequence", { aggregate_id: "session-a" })).toThrow(UnsupportedWriteError)

    // The scoped guard also refuses the ad-hoc durable statement outright.
    expect(() =>
      database.connection.run("INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?, ?, ?, ?, ?)", [
        "evt-1",
        "session-a",
        0,
        "test",
        "{}",
      ]),
    ).toThrow(WorkerStatementRejectedError)
    expect(() => database.connection.run("DELETE FROM event_sequence WHERE aggregate_id = ?", ["session-a"])).toThrow(
      WorkerStatementRejectedError,
    )

    const bus = database.bus.create({
      ownerID: "gen-a",
      definitions: [Note],
      aggregateID: "session-a",
    })
    const published = await bus.publishAsync([{ definition: Note, data: { sessionID: "session-a", text: "ok" } }])
    expect(published[0]?.durable?.seq).toBe(0)
    await expect(
      bus.publishAsync([{ definition: Note, data: { sessionID: "session-b", text: "stolen" } }]),
    ).rejects.toBeInstanceOf(DurableLog.CrossAggregateError)
  })
})

describe("worker page-cache budget (D5)", () => {
  test("a worker connection does not inherit the hub's 64 MiB cache", async () => {
    expect(DEFAULT_WORKER_CACHE_SIZE_KIB).toBeLessThan(64_000)
    expect(resolveWorkerCacheSizeKib({})).toBe(DEFAULT_WORKER_CACHE_SIZE_KIB)
    expect(resolveWorkerCacheSizeKib({ OPENCODE_DB_WORKER_CACHE_SIZE_KIB: "4096" })).toBe(4_096)
    expect(walPragmas({ cacheSizeKib: 4_096 })).toContain("PRAGMA cache_size = -4096")

    const directory = workspace()
    const connection = openConnection({ filename: path.join(directory, "cache.db") })
    try {
      // SQLite reports the page cache in pages; a negative KiB budget is
      // converted with the page size, so assert the configured magnitude.
      expect(Math.abs(Number(connection.pragma("cache_size"))) * 1024).toBeLessThanOrEqual(
        DEFAULT_WORKER_CACHE_SIZE_KIB * 1024,
      )
    } finally {
      connection.close()
    }
  })
})
