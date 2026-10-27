// P1 scope/data fixes: every worker surface is bound to one session, EventV2
// enforces the same aggregate/blob bounds as the Bus, the zero-live gate counts
// real isolates, migrations run once per process on a genuinely legacy file,
// the retry timer never pins a worker process, and the page-cache budget stays
// bounded for eight workers.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { fileURLToPath } from "node:url"
import { afterEach, describe, expect, test } from "bun:test"
import { layer as sqliteLayer } from "@opencode-ai/core/database/sqlite.bun"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { Effect, Exit, Schema } from "effect"
import { sql } from "drizzle-orm"
import { Database as DatabaseService } from "@opencode-ai/core/database/database"
import { openConnection } from "@opencode-ai/core/database/connection"
import { DatabaseMigration } from "@opencode-ai/core/database/migration"
import { migrations } from "@opencode-ai/core/database/migration.gen"
import {
  DEFAULT_CACHE_SIZE_KIB,
  DEFAULT_WORKER_CACHE_SIZE_KIB,
  resolveWorkerCacheSizeKib,
  walPragmas,
} from "@opencode-ai/core/database/pragmas"
import { EventV2 } from "@opencode-ai/core/event"
import { Bus } from "@opencode-ai/core/bus"
import { Event } from "@opencode-ai/schema/event"
import { SCHEMA_VERSION } from "@/session/db/allowlist.gen"
import {
  currentSchemaIdentity,
  ensureProductionSchema,
  markSchemaIdentity,
  resetSchemaEnsureCache,
  schemaIdentity,
} from "@/session/db/schema"
import {
  CrossSessionAccessError,
  HubOnlyTableError,
  ImmutableColumnError,
  SESSION_IMMUTABLE_COLUMNS,
  WORKER_READABLE_HUB_TABLES,
  WorkerStatementRejectedError,
  databaseLayer,
  makeBusAuthorizer,
  makeWorkerAuthorizer,
  openWorkerDatabase,
  type WorkerDatabase,
} from "@/session/worker/db"
import { createBunExecutorTransport, createProductionSessionHub, liveSessionWorkerCount } from "@/session/worker/hub"
import { migrationStatements } from "@/session/writer/migration.statements.gen"
import { sessionWorkerInit } from "../lib/session-worker-init"

const directories: string[] = []
const cleanups: Array<() => void | Promise<void>> = []
const originalWorker = (globalThis as { Worker?: unknown }).Worker

function workspace(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-scope-data-"))
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
    expect(Number(connection.pragma("user_version"))).toBe(SCHEMA_VERSION)
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
      ["project-scope", "scope", "/tmp/scope", "[]", 1, 1],
    )
    for (const id of ["session-a", "session-b"]) {
      connection.run(
        "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [id, "project-scope", id, "/tmp/scope", id, "1", 1, 1],
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

function seedRowAddressRows(filename: string): void {
  const connection = openConnection({ filename })
  try {
    for (const id of ["session-a", "session-b"]) {
      connection.run("INSERT INTO event_sequence (aggregate_id, seq) VALUES (?, 0)", [id])
      connection.run("INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?, ?, 0, 'test.event', '{}')", [
        `event-${id}`,
        id,
      ])
      connection.run(
        "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, 1, 1, '{}')",
        [`part-${id}`, `message-${id}`, id],
      )
    }
  } finally {
    connection.close()
  }
}

async function openBound(filename: string, sessionID: string): Promise<WorkerDatabase> {
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

/** A bound worker has no raw write surface; wrap the synchronous denial as an Effect. */
function rawWrite(database: WorkerDatabase, sql: string, params: ReadonlyArray<unknown> = []) {
  return Effect.try({ try: () => database.connection.run(sql, params), catch: (error) => error })
}

const Note = Event.define({
  type: "test.scope.data.p1fix.note",
  durable: { version: 1, aggregate: "sessionID" },
  schema: { sessionID: Schema.String, text: Schema.String },
})

const NoteManifest = { definitions: Event.durable([Note]), schema: Schema.Any }

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
  ;(globalThis as { Worker?: unknown }).Worker = originalWorker
  resetSchemaEnsureCache()
})

describe("bound-session scope on every surface", () => {
  test("live TEMP views filter every read surface and no history is copied", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    const database = await openBound(filename, "session-a")

    // Unqualified reads resolve the live bound view on every surface.
    expect(await Effect.runPromise(database.all<{ id: string }>("SELECT id FROM message"))).toEqual([
      { id: "message-session-a" },
    ])
    expect(database.strictConnection.all<{ id: string }>("SELECT id FROM message")).toEqual([
      { id: "message-session-a" },
    ])
    expect(database.connection.query<{ id: string }>("SELECT id FROM message").all()).toEqual([
      { id: "message-session-a" },
    ])

    // Direct `main` access is denied by the authorizer even for a SELECT.
    const mainRead = thrownBy(() => database.strictConnection.all("SELECT id FROM main.message"))
    expect(mainRead).toBeInstanceOf(CrossSessionAccessError)

    // The query connection is read-only, not a second raw handle.
    const rawWrite = thrownBy(() =>
      database.strictConnection.run(
        "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES ('x', 'session-a', 1, 1, '{}')",
      ),
    )
    expect(rawWrite).toBeInstanceOf(WorkerStatementRejectedError)

    // Only views are installed in temp; there is no mirrored row copy.
    const temp = database.strictConnection.all<{ type: string; name: string }>(
      "SELECT type, name FROM sqlite_temp_master ORDER BY name",
    )
    expect(temp.every((row) => row.type === "view")).toBe(true)
    expect(temp.some((row) => row.name === "message")).toBe(true)
    expect(database.strictConnection.all("SELECT COUNT(*) AS count FROM temp.message")).toEqual([{ count: 1 }])
  })

  test("a hub write is visible immediately through the live view", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    const database = await openBound(filename, "session-a")

    // Another writer (hub/other process) appends to the bound session after the
    // worker opened. A mirror would be stale; a live view sees it.
    const hub = openConnection({ filename })
    cleanups.push(() => hub.close())
    hub.run("INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, 1, 1, '{}')", [
      "message-later",
      "session-a",
    ])
    expect(database.strictConnection.all<{ id: string }>("SELECT id FROM message ORDER BY id")).toEqual([
      { id: "message-later" },
      { id: "message-session-a" },
    ])
  })

  test("a long session's foreign history is not copied at open", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    const hub = openConnection({ filename })
    cleanups.push(() => hub.close())
    // 5k foreign rows would be a full-copy/open cost under the old mirror.
    hub.exec("BEGIN")
    for (let index = 0; index < 5_000; index += 1) {
      hub.run("INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, 1, 1, '{}')", [
        `bulk-${index}`,
        "session-b",
      ])
    }
    hub.exec("COMMIT")

    const database = await openBound(filename, "session-a")
    expect(database.strictConnection.all("SELECT COUNT(*) AS count FROM temp.message")).toEqual([{ count: 1 }])
    expect(
      database.strictConnection.all<{ type: string; name: string }>("SELECT type, name FROM sqlite_temp_master"),
    ).toEqual(expect.arrayContaining([expect.objectContaining({ type: "view", name: "message" })]))
  })

  test("OR, UNION, joins, CTEs, and subqueries cannot widen the live view", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    seedRowAddressRows(filename)
    const database = await openBound(filename, "session-a")

    const hostile = [
      "SELECT id FROM message WHERE session_id = 'session-a' OR 1 = 1",
      "SELECT id FROM message WHERE session_id = 'session-a' UNION SELECT id FROM message",
      "SELECT id FROM message WHERE id IN (SELECT id FROM message)",
      "SELECT id FROM message WHERE EXISTS (SELECT 1 FROM message WHERE session_id = 'session-b')",
      "SELECT m.id FROM message m JOIN part p ON p.message_id = m.id",
      "SELECT m.id FROM message m, part p",
      "SELECT id FROM (message)",
      "SELECT id FROM ((message))",
      "WITH stolen AS (SELECT id FROM message) SELECT id FROM stolen",
    ]
    for (const sql of hostile) {
      const rows = await Effect.runPromise(database.all<{ id: string }>(sql))
      expect(rows.every((row) => !String(row.id).includes("session-b"))).toBe(true)
    }

    // A foreign write still fails closed and leaves the row untouched.
    const multiRow = await Effect.runPromise(
      rawWrite(
        database,
        "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, 1, 1, '{}'), (?, ?, 1, 1, '{}')",
        ["message-own", "session-a", "message-stolen", "session-b"],
      ).pipe(Effect.flip),
    )
    expect(multiRow).toBeInstanceOf(WorkerStatementRejectedError)
    const count = await Effect.runPromise(
      database.all<{ count: number }>("SELECT COUNT(*) AS count FROM message WHERE session_id = ?", ["session-a"]),
    )
    expect(count).toEqual([{ count: 1 }])
  })

  test("CASE/IS predicates cannot widen the live view or the scoped delete", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    const database = await openBound(filename, "session-a")

    const caseRead = await Effect.runPromise(
      database.all<{ id: string; flag: number }>(
        "SELECT id, CASE WHEN session_id = 'session-b' THEN 1 ELSE 0 END AS flag FROM message",
      ),
    )
    expect(caseRead).toEqual([{ id: "message-session-a", flag: 0 }])

    const isRead = await Effect.runPromise(
      database.all<{ id: string }>("SELECT id FROM message WHERE session_id IS 'session-b'"),
    )
    expect(isRead).toEqual([])

    // The typed repository appends the bound predicate even with no WHERE.
    database.repository.delete("message", {})

    const raw = openConnection({ filename })
    cleanups.push(() => raw.close())
    expect(raw.all("SELECT id, session_id FROM message ORDER BY id")).toEqual([
      { id: "message-session-b", session_id: "session-b" },
    ])
  })

  test("the Bus capability exposes operations, never a raw connection", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    const database = await openBound(filename, "session-a")

    expect("connection" in database.bus).toBe(false)
    const bus = database.bus.create({ ownerID: "surface-check" })
    expect("connection" in bus).toBe(false)
    expect("transaction" in bus).toBe(false)
    expect(typeof bus.publish).toBe("function")
    expect(typeof bus.deleteAggregate).toBe("function")
  })

  test("the Drizzle graph filters scans and row-addressed reads through the live view", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    const database = await openBound(filename, "session-a")

    const outcome = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const { db } = yield* DatabaseService.Service
          const own = yield* db.all(sql`SELECT id FROM message WHERE session_id = ${"session-a"}`)
          const scan = yield* db.all(sql`SELECT id FROM message`)
          const rowAddress = yield* db.all(sql`SELECT id FROM message WHERE id = ${"message-session-a"}`)
          return { own, scan, rowAddress }
        }).pipe(Effect.provide(databaseLayer(database))),
      ),
    )
    expect(outcome.own).toEqual([{ id: "message-session-a" }])
    expect(outcome.scan).toEqual([{ id: "message-session-a" }])
    expect(outcome.rowAddress).toEqual([{ id: "message-session-a" }])
  })
})

describe("EventV2 durable path", () => {
  test("a bound worker keeps aggregate scope and offloads an oversized payload", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    const database = await openBound(filename, "session-a")
    const blobDir = path.join(path.dirname(filename), "event-blobs")
    const text = `worker-large:${"x".repeat(16 * 1024 * 1024 + 1)}`

    const outcome = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const events = yield* EventV2.Service
          const { db } = yield* DatabaseService.Service
          const own = yield* events.publish(Note, { sessionID: "session-a", text: "ok" })
          const cross = yield* Effect.exit(events.publish(Note, { sessionID: "session-b", text: "stolen" }))
          const large = yield* events.publish(Note, { sessionID: "session-a", text })
          const read = yield* EventV2.readAggregate(db, {
            aggregateID: "session-a",
            limit: 10,
            manifest: NoteManifest,
          })
          return { own, cross, large, read }
        }).pipe(Effect.provide(EventV2.layerWith()), Effect.provide(databaseLayer(database))),
      ),
    )

    expect(outcome.own.durable?.seq).toBe(0)
    expect(Exit.isFailure(outcome.cross)).toBe(true)
    expect(String(outcome.cross)).toContain("outside the worker's bound session")
    expect(outcome.large.durable?.seq).toBe(1)
    expect(outcome.read.events.map((event) => event.durable?.seq)).toEqual([0, 1])
    expect(outcome.read.events[1]?.data).toEqual({ sessionID: "session-a", text })

    const raw = openConnection({ filename })
    cleanups.push(() => raw.close())
    expect(raw.all("SELECT COUNT(*) AS count FROM event_blob")).toEqual([{ count: 1 }])
    expect(fs.readdirSync(blobDir)).toHaveLength(1)
  })

  test("a hub EventV2 publish offloads a >16 MiB payload and reads it back", async () => {
    const directory = workspace()
    const filename = path.join(directory, "hub-large.db")
    const blobDir = path.join(directory, "event-blobs")
    const text = `hub-large:${"x".repeat(16 * 1024 * 1024 + 1)}`

    const outcome = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const events = yield* EventV2.Service
          const { db } = yield* DatabaseService.Service
          const published = yield* events.publish(Note, { sessionID: "session-a", text })
          const read = yield* EventV2.readAggregate(db, {
            aggregateID: "session-a",
            limit: 10,
            manifest: NoteManifest,
          })
          return { published, read }
        }).pipe(Effect.provide(EventV2.layerWith()), Effect.provide(DatabaseService.layerFromPath(filename))),
      ),
    )

    expect(outcome.published.durable?.seq).toBe(0)
    expect(outcome.read.events.map((event) => event.data)).toEqual([{ sessionID: "session-a", text }])

    const raw = openConnection({ filename })
    cleanups.push(() => raw.close())
    expect(raw.all("SELECT COUNT(*) AS count FROM event_blob")).toEqual([{ count: 1 }])
    expect(fs.readdirSync(blobDir)).toHaveLength(1)
    expect(
      raw.all<{ data: string }>("SELECT data FROM event WHERE aggregate_id = ?", ["session-a"])[0]?.data,
    ).toContain("__eventBlob")
  })
})

describe("real zero-live-worker gate", () => {
  class ShutdownAckWorker {
    onmessage: ((event: MessageEvent) => void) | null = null
    onerror: ((event: ErrorEvent) => void) | null = null
    terminated = false
    private readonly listeners = new Set<(event: MessageEvent) => void>()

    constructor(
      readonly url: string | URL,
      readonly options?: unknown,
    ) {}

    postMessage(message: unknown): void {
      if ((message as { type?: unknown }).type !== "shutdown") return
      queueMicrotask(() => {
        for (const listener of this.listeners) listener({ data: { type: "shutdown-ack" } } as MessageEvent)
      })
    }

    addEventListener(type: string, listener: (event: MessageEvent) => void): void {
      if (type === "message") this.listeners.add(listener)
    }

    removeEventListener(type: string, listener: (event: MessageEvent) => void): void {
      if (type === "message") this.listeners.delete(listener)
    }

    async terminate(): Promise<number> {
      this.terminated = true
      return 0
    }
  }

  test("a worker live on the same database refuses a second hub's migration", async () => {
    const directory = workspace()
    const filename = path.join(directory, "live.db")
    ;(globalThis as { Worker?: unknown }).Worker = ShutdownAckWorker as never

    const transport = createBunExecutorTransport({
      entry: "fake://live",
      env: { OPENCODE_DB: filename },
    })
    cleanups.push(async () => {
      await transport.terminate()
    })

    expect(liveSessionWorkerCount(filename)).toBe(1)
    expect(() =>
      createProductionSessionHub({
        init: sessionWorkerInit(),
        databasePath: filename,
      }),
    ).toThrow(/zero live workers/)
  })

  test("the derived count includes the held spawn permit before construction", () => {
    const directory = workspace()
    const filename = path.join(directory, "permit.db")
    let held = false
    const gate = {
      acquire: async () => ({ release: () => {} }),
      get held() {
        held = true
        return true
      },
    }
    expect(() =>
      createProductionSessionHub({
        init: sessionWorkerInit(),
        databasePath: filename,
        spawnGate: gate,
      }),
    ).toThrow(/zero live workers/)
    expect(held).toBe(true)
  })
})

describe("migrations once per process (legacy upgrade)", () => {
  async function legacyDatabase(filename: string): Promise<void> {
    const makeDb = EffectDrizzleSqlite.makeWithDefaults()
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const db = yield* makeDb
          // The pre-change fixture: every core migration except the two this
          // change adds, and no opencode ledger at all.
          yield* DatabaseMigration.applyOnly(db, migrations.slice(0, -2))
        }),
      ).pipe(Effect.provide(sqliteLayer({ filename, disableWAL: true })), Effect.orDie),
    )
  }

  test("a genuinely legacy file is upgraded through core's journal plus the ledger", async () => {
    const directory = workspace()
    const filename = path.join(directory, "legacy.db")
    await legacyDatabase(filename)

    const before = openConnection({ filename })
    try {
      expect(Number(before.pragma("user_version"))).toBe(0)
      expect(before.all("SELECT name FROM sqlite_master WHERE name = 'event_blob'")).toEqual([])
      expect(before.all("SELECT name FROM sqlite_master WHERE name = 'tool_call'")).toEqual([])
      expect(before.all("SELECT name FROM pragma_table_info('event_sequence')").map((row) => row.name)).not.toContain(
        "worker_owner_id",
      )
    } finally {
      before.close()
    }

    expect(ensureProductionSchema({ filename, liveWorkers: 0 })).toEqual(currentSchemaIdentity())

    const after = openConnection({ filename })
    try {
      expect(schemaIdentity(after)).toEqual(currentSchemaIdentity())
      expect(
        after.all("SELECT name FROM sqlite_master WHERE name IN ('event_blob', 'tool_call') ORDER BY name"),
      ).toEqual([{ name: "event_blob" }, { name: "tool_call" }])
      expect(after.all("SELECT name FROM pragma_table_info('event_sequence')").map((row) => row.name)).toContain(
        "worker_owner_id",
      )
      const journal = after.all<{ id: string }>("SELECT id FROM migration ORDER BY id")
      expect(journal.some((row) => row.id === "20261026000000_tool_call_ledger")).toBe(true)
      expect(new Set(journal.map((row) => row.id)).size).toBe(journal.length)
    } finally {
      after.close()
    }
  })

  test("the second ensure for a filename is a cached no-op", async () => {
    const directory = workspace()
    const filename = path.join(directory, "cached.db")
    await legacyDatabase(filename)

    expect(ensureProductionSchema({ filename, liveWorkers: 0 })).toEqual(currentSchemaIdentity())
    const admin = openConnection({ filename })
    try {
      admin.exec("DROP TABLE tool_call")
    } finally {
      admin.close()
    }
    expect(ensureProductionSchema({ filename, liveWorkers: 0 })).toEqual(currentSchemaIdentity())
    const after = openConnection({ filename })
    try {
      expect(after.all("SELECT name FROM sqlite_master WHERE name = 'tool_call'")).toEqual([])
    } finally {
      after.close()
    }
  })
})

describe("worker retry timer and cache budget", () => {
  test("a worker's backoff timer is unref'd so the isolate can exit", async () => {
    const transactionPath = fileURLToPath(new URL("../../../core/src/database/transaction.ts", import.meta.url))
    const script = `
      import { retryBusyAsync } from ${JSON.stringify(transactionPath)}
      const busy = () => { throw Object.assign(new Error("SQLITE_BUSY"), { name: "SQLiteError", code: "SQLITE_BUSY" }) }
      retryBusyAsync(busy, { retries: 5, baseDelayMs: 30_000, maxDelayMs: 30_000 }).catch(() => {})
    `
    const child = Bun.spawn(["bun", "-e", script], { stdout: "pipe", stderr: "pipe" })
    const outcome = await Promise.race([child.exited.then(() => "exited"), Bun.sleep(3_000).then(() => "timeout")])
    if (outcome === "timeout") child.kill()
    expect(outcome).toBe("exited")
  })

  test("eight workers stay within their 8 MiB budget and the hub keeps 64 MiB", () => {
    expect(DEFAULT_WORKER_CACHE_SIZE_KIB).toBe(8_192)
    expect(DEFAULT_CACHE_SIZE_KIB).toBe(64_000)
    expect(resolveWorkerCacheSizeKib({})).toBe(DEFAULT_WORKER_CACHE_SIZE_KIB)
    expect(walPragmas()).toContain(`PRAGMA cache_size = -${DEFAULT_CACHE_SIZE_KIB}`)

    const directory = workspace()
    const filename = path.join(directory, "cache.db")
    const connections = Array.from({ length: 8 }, () => openConnection({ filename }))
    try {
      let total = 0
      for (const connection of connections) {
        const cacheKib = Math.abs(Number(connection.pragma("cache_size")))
        expect(cacheKib).toBeLessThanOrEqual(DEFAULT_WORKER_CACHE_SIZE_KIB)
        total += cacheKib
      }
      expect(total).toBeLessThanOrEqual(8 * DEFAULT_WORKER_CACHE_SIZE_KIB)
    } finally {
      for (const connection of connections) connection.close()
    }
  })
})

describe("aggregate deletion through Bus.deleteAggregate", () => {
  test("an oversized blob is collected atomically and a shared file survives until its last aggregate", async () => {
    const directory = workspace()
    const filename = path.join(directory, "hub.db")
    const blobDir = path.join(directory, "event-blobs")

    // Build the hub schema once, then publish through the same unguarded Bus
    // the worker/hub writer uses (EventV2 shares the same D16 blob path).
    await Effect.runPromise(Effect.scoped(Effect.void.pipe(Effect.provide(DatabaseService.layerFromPath(filename)))))
    const raw = openConnection({ filename })
    cleanups.push(() => raw.close())
    const bus = Bus.create({
      connection: raw,
      ownerID: "scope-data-delete",
      definitions: [Note],
      blobDir,
      blobLimitBytes: 64,
    })
    bus.publish([
      { definition: Note, data: { sessionID: "session-a", text: `unique:${"u".repeat(4_096)}` } },
      { definition: Note, data: { sessionID: "session-a", text: `shared:${"s".repeat(4_096)}` } },
    ])
    const rows = raw.all<{ seq: number; hash: string; size: number; type: string }>(
      "SELECT seq, hash, size, type FROM event_blob WHERE aggregate_id = ? ORDER BY seq",
      ["session-a"],
    )
    const unique = rows[0]
    const shared = rows[1]
    const uniqueFile = path.join(blobDir, unique.hash)
    const sharedFile = path.join(blobDir, shared.hash)

    // A replayed/imported aggregate can reference the same content hash.
    raw.run("INSERT INTO event_sequence (aggregate_id, seq) VALUES (?, 0)", ["session-b"])
    raw.run(
      "INSERT INTO event_blob (aggregate_id, seq, type, hash, size, data, file_path) VALUES (?, 0, ?, ?, ?, NULL, ?)",
      ["session-b", shared.type, shared.hash, shared.size, shared.hash],
    )
    raw.run("INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?, ?, 0, ?, ?)", [
      "evt-session-b",
      "session-b",
      shared.type,
      JSON.stringify({ __eventBlob: shared.hash, size: shared.size }),
    ])
    expect(fs.existsSync(uniqueFile)).toBe(true)
    expect(fs.existsSync(sharedFile)).toBe(true)

    const remove = (aggregateID: string) =>
      Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const events = yield* EventV2.Service
            yield* events.remove(aggregateID)
          }).pipe(
            Effect.provide(EventV2.layerWith({ blobDir })),
            Effect.provide(DatabaseService.layerFromPath(filename)),
          ),
        ),
      )

    await remove("session-a")

    // The deleted aggregate's events, blob rows, and sequence row are gone.
    expect(raw.all("SELECT COUNT(*) AS count FROM event WHERE aggregate_id = ?", ["session-a"])).toEqual([{ count: 0 }])
    expect(raw.all("SELECT COUNT(*) AS count FROM event_blob WHERE aggregate_id = ?", ["session-a"])).toEqual([
      { count: 0 },
    ])
    expect(raw.all("SELECT COUNT(*) AS count FROM event_sequence WHERE aggregate_id = ?", ["session-a"])).toEqual([
      { count: 0 },
    ])
    // Its unshared file is unlinked; the file another aggregate still names stays.
    expect(fs.existsSync(uniqueFile)).toBe(false)
    expect(fs.existsSync(sharedFile)).toBe(true)
    expect(raw.all("SELECT COUNT(*) AS count FROM event_blob WHERE aggregate_id = ?", ["session-b"])).toEqual([
      { count: 1 },
    ])

    // Every surviving {__eventBlob} reference names a live row and a live file.
    const live = raw.all<{ hash: string; file_path: string }>(
      "SELECT hash, file_path FROM event_blob WHERE aggregate_id = ?",
      ["session-b"],
    )
    expect(live).toEqual([{ hash: shared.hash, file_path: shared.hash }])
    for (const row of live) expect(fs.existsSync(path.join(blobDir, row.file_path))).toBe(true)

    await remove("session-b")

    // The last reference is gone: no rows, no files, no dangling blob reference.
    expect(fs.readdirSync(blobDir)).toEqual([])
    expect(raw.all("SELECT COUNT(*) AS count FROM event")).toEqual([{ count: 0 }])
    expect(raw.all("SELECT COUNT(*) AS count FROM event_blob")).toEqual([{ count: 0 }])
    expect(raw.all("SELECT COUNT(*) AS count FROM event_sequence")).toEqual([{ count: 0 }])
    expect(raw.all("SELECT data FROM event WHERE data LIKE '%__eventBlob%'")).toEqual([])
  })
})

describe("worker aggregate deletion", () => {
  test("a bound worker's EventV2.remove collects its own blob without widening scope", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    const database = await openBound(filename, "session-a")
    const blobDir = path.join(path.dirname(filename), "event-blobs")
    const bus = database.bus.create({
      ownerID: "worker-delete",
      definitions: [Note],
      blobDir,
      blobLimitBytes: 64,
      aggregateID: "session-a",
    })
    bus.publish([{ definition: Note, data: { sessionID: "session-a", text: `worker:${"w".repeat(4_096)}` } }])
    expect(fs.readdirSync(blobDir)).toHaveLength(1)

    const counts = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const events = yield* EventV2.Service
          yield* events.remove("session-a")
          const eventsRows = yield* database.all<{ count: number }>(
            "SELECT COUNT(*) AS count FROM event WHERE aggregate_id = ?",
            ["session-a"],
          )
          const sequenceRows = yield* database.all<{ count: number }>(
            "SELECT COUNT(*) AS count FROM event_sequence WHERE aggregate_id = ?",
            ["session-a"],
          )
          return { eventsRows, sequenceRows }
        }).pipe(Effect.provide(EventV2.layerWith()), Effect.provide(databaseLayer(database))),
      ),
    )

    expect(fs.readdirSync(blobDir)).toEqual([])
    expect(counts.eventsRows).toEqual([{ count: 0 }])
    expect(counts.sequenceRows).toEqual([{ count: 0 }])
  })
})

describe("live view and repository hardening (join, write, ?NNN, row address)", () => {
  test("each joined scoped table is filtered independently", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    seedRowAddressRows(filename)
    const database = await openBound(filename, "session-a")

    const reads = [
      "SELECT e.id FROM event e JOIN event e2 ON 1 = 1",
      "SELECT e.id FROM event e JOIN event e2 ON e.aggregate_id = e2.aggregate_id WHERE e.aggregate_id = 'session-a'",
      "SELECT m.id FROM message m JOIN part p ON p.message_id = m.id",
      "SELECT m.id FROM message m, part p",
    ]
    for (const sql of reads) {
      const rows = await Effect.runPromise(database.all<{ id: string }>(sql))
      expect(rows.every((row) => !String(row.id).includes("session-b"))).toBe(true)
    }

    const safe = await Effect.runPromise(
      database.all<{ id: string }>(
        "SELECT m.id FROM message m JOIN part p ON p.message_id = m.id WHERE m.session_id = ? AND p.session_id = ?",
        ["session-a", "session-a"],
      ),
    )
    expect(safe).toEqual([{ id: "message-session-a" }])
  })

  test("a scoped write cannot move a row to another session", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    const database = await openBound(filename, "session-a")

    const foreign = await Effect.runPromise(
      rawWrite(database, "UPDATE message SET session_id = ? WHERE session_id = ?", ["session-b", "session-a"]).pipe(
        Effect.flip,
      ),
    )
    expect(foreign).toBeInstanceOf(WorkerStatementRejectedError)

    const expression = await Effect.runPromise(
      rawWrite(database, "UPDATE message SET session_id = session_id WHERE session_id = ?", ["session-a"]).pipe(
        Effect.flip,
      ),
    )
    expect(expression).toBeInstanceOf(WorkerStatementRejectedError)

    database.repository.update("message", { data: "{}" }, { session_id: "session-a" })

    const raw = openConnection({ filename })
    cleanups.push(() => raw.close())
    expect(raw.all("SELECT session_id FROM message ORDER BY session_id")).toEqual([
      { session_id: "session-a" },
      { session_id: "session-b" },
    ])
  })

  test("INSERT ... SELECT must project the bound session and cannot launder a foreign row", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    const database = await openBound(filename, "session-a")

    const foreign = await Effect.runPromise(
      rawWrite(
        database,
        "INSERT INTO message (id, session_id, time_created, time_updated, data) SELECT id, 'session-b', time_created, time_updated, data FROM message WHERE session_id = ?",
        ["session-a"],
      ).pipe(Effect.flip),
    )
    expect(foreign).toBeInstanceOf(WorkerStatementRejectedError)

    // The typed repository copies own data under a fresh id without raw SQL.
    database.repository.insert("message", { id: "message-copy", time_created: 1, time_updated: 1, data: "{}" })

    const raw = openConnection({ filename })
    cleanups.push(() => raw.close())
    expect(raw.all("SELECT id FROM message ORDER BY id")).toEqual([
      { id: "message-copy" },
      { id: "message-session-a" },
      { id: "message-session-b" },
    ])
  })

  test("UPDATE conflict clauses cannot touch another session", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    const database = await openBound(filename, "session-a")

    for (const conflict of ["REPLACE", "IGNORE", "ABORT", "FAIL", "ROLLBACK"]) {
      await Effect.runPromise(
        rawWrite(database, `UPDATE OR ${conflict} message SET data = 'hacked' WHERE id = ?`, [
          "message-session-b",
        ]).pipe(Effect.exit),
      )
    }

    const raw = openConnection({ filename })
    cleanups.push(() => raw.close())
    expect(raw.all("SELECT id, data FROM message ORDER BY id")).toEqual([
      { id: "message-session-a", data: "{}" },
      { id: "message-session-b", data: "{}" },
    ])
  })

  test("INSERT ... SELECT set operations are refused before execution", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    const database = await openBound(filename, "session-a")

    for (const operator of ["UNION", "INTERSECT", "EXCEPT"]) {
      const error = await Effect.runPromise(
        rawWrite(
          database,
          `INSERT INTO message (id, session_id, time_created, time_updated, data)
             SELECT 'setop-a', 'session-a', 1, 1, '{}'
             ${operator}
             SELECT 'stolen-' || id, session_id, time_created, time_updated, data FROM message WHERE session_id = 'session-b'`,
        ).pipe(Effect.flip),
      )
      expect(error).toBeInstanceOf(WorkerStatementRejectedError)
    }

    const raw = openConnection({ filename })
    cleanups.push(() => raw.close())
    expect(raw.all("SELECT id FROM message WHERE id LIKE 'stolen-%' OR id = 'setop-a'")).toEqual([])
  })

  test("INSERT ... SELECT from a parenthesized VALUES is not a VALUES insert", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    const database = await openBound(filename, "session-a")

    const error = await Effect.runPromise(
      rawWrite(
        database,
        "INSERT INTO message (session_id, id, time_created, time_updated, data) SELECT 'session-b', 'message-values', 1, 1, '{}' FROM (VALUES ('session-a'))",
      ).pipe(Effect.flip),
    )
    expect(error).toBeInstanceOf(WorkerStatementRejectedError)

    const raw = openConnection({ filename })
    cleanups.push(() => raw.close())
    expect(raw.all("SELECT id FROM message WHERE id = 'message-values'")).toEqual([])
  })

  test("parenthesized FROM and a one-sided join stay inside the view", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    seedRowAddressRows(filename)
    const database = await openBound(filename, "session-a")

    for (const sql of ["SELECT id FROM (message)", "SELECT id FROM (message) AS m", "SELECT id FROM ((message))"]) {
      const rows = await Effect.runPromise(database.all<{ id: string }>(sql))
      expect(rows).toEqual([{ id: "message-session-a" }])
    }

    const safe = await Effect.runPromise(
      database.all<{ id: string }>("SELECT id FROM (SELECT id FROM message WHERE session_id = ?)", ["session-a"]),
    )
    expect(safe).toEqual([{ id: "message-session-a" }])
  })

  test("?NNN and named parameters cannot reach another session", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    const database = await openBound(filename, "session-a")

    const foreignRead = await Effect.runPromise(
      database.all<{ id: string }>("SELECT id FROM message WHERE id = ?2 AND session_id = ?1", [
        "session-a",
        "message-session-b",
      ]),
    )
    expect(foreignRead).toEqual([])

    const ownRead = await Effect.runPromise(
      database.all<{ id: string }>("SELECT id FROM message WHERE session_id = ?2", ["session-b", "session-a"]),
    )
    expect(ownRead).toEqual([{ id: "message-session-a" }])

    database.repository.delete("message", { id: "message-session-b" })
    const namedForeign = await Effect.runPromise(
      database.all<{ id: string }>("SELECT id FROM message WHERE id = $id", { id: "message-session-b" }),
    )
    expect(namedForeign).toEqual([])

    const raw = openConnection({ filename })
    cleanups.push(() => raw.close())
    expect(raw.all("SELECT id FROM message ORDER BY id")).toEqual([
      { id: "message-session-a" },
      { id: "message-session-b" },
    ])
  })

  test("row-addressed reads resolve only own rows through the view", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    seedRowAddressRows(filename)
    const database = await openBound(filename, "session-a")

    expect(database.drizzleConnection.all("SELECT id FROM event WHERE event.id = ?", ["event-session-a"])).toEqual([
      { id: "event-session-a" },
    ])
    expect(
      database.drizzleConnection.all("SELECT id FROM part WHERE part.message_id = ?", ["message-session-a"]),
    ).toEqual([{ id: "part-session-a" }])
    expect(database.drizzleConnection.all("SELECT id FROM event WHERE event.id = ?", ["event-session-b"])).toEqual([])
    expect(
      database.drizzleConnection.all("SELECT id FROM part WHERE part.message_id = ?", ["message-session-b"]),
    ).toEqual([])
    expect(database.drizzleConnection.all("SELECT id FROM event")).toEqual([{ id: "event-session-a" }])
  })
})

describe("EventV2 blob directory follows the connection file", () => {
  test("two custom-file databases with the same content hash never share a blob directory", async () => {
    const fileA = migratedDatabase()
    const fileB = migratedDatabase()
    const dirA = path.join(path.dirname(fileA), "event-blobs")
    const dirB = path.join(path.dirname(fileB), "event-blobs")
    const rawA = openConnection({ filename: fileA })
    const rawB = openConnection({ filename: fileB })
    cleanups.push(
      () => rawA.close(),
      () => rawB.close(),
    )

    // The same payload content hashes identically in both databases, so an
    // EventV2 default derived from the global database would unlink the wrong
    // file (or leave this database's file behind).
    const text = `shared:${"x".repeat(4_096)}`
    for (const [raw, blobDir] of [
      [rawA, dirA],
      [rawB, dirB],
    ] as const) {
      const bus = Bus.create({
        connection: raw,
        ownerID: "blob-dir-source",
        definitions: [Note],
        blobDir,
        blobLimitBytes: 64,
        aggregateID: "session-a",
      })
      bus.publish([{ definition: Note, data: { sessionID: "session-a", text } }])
    }

    const filesA = fs.readdirSync(dirA)
    expect(filesA).toHaveLength(1)
    expect(fs.readdirSync(dirB)).toEqual(filesA)

    const remove = (filename: string) =>
      Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const events = yield* EventV2.Service
            yield* events.remove("session-a")
          }).pipe(Effect.provide(EventV2.layerWith()), Effect.provide(DatabaseService.layerFromPath(filename))),
        ),
      )

    await remove(fileA)

    expect(fs.readdirSync(dirA)).toEqual([])
    expect(fs.readdirSync(dirB)).toEqual(filesA)
    expect(rawA.all("SELECT COUNT(*) AS count FROM event_blob WHERE aggregate_id = ?", ["session-a"])).toEqual([
      { count: 0 },
    ])
    expect(rawB.all("SELECT COUNT(*) AS count FROM event_blob WHERE aggregate_id = ?", ["session-a"])).toEqual([
      { count: 1 },
    ])

    await remove(fileB)
    expect(fs.readdirSync(dirB)).toEqual([])
  })
})

describe("authoritative bound scope (adversarial fail-closed)", () => {
  async function rows(filename: string, sql: string, params: ReadonlyArray<unknown> = []) {
    const connection = openConnection({ filename })
    try {
      return connection.all<Record<string, unknown>>(sql, params)
    } finally {
      connection.close()
    }
  }

  /**
   * A hostile statement is fail-closed when it is rejected OR executed against
   * the bound session only. The invariant is checked on the durable tables, not
   * on which layer produced the refusal.
   */
  async function expectNoForeignEffect(statement: Effect.Effect<unknown, unknown>, verify: () => Promise<void> | void) {
    await Effect.runPromise(Effect.exit(statement))
    await verify()
  }

  test("UPDATE and DELETE RETURNING cannot target or reveal another session", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    const database = await openBound(filename, "session-a")

    await expectNoForeignEffect(
      rawWrite(database, "UPDATE message SET data = 'stolen' WHERE id = ? RETURNING (session_id = ?)", [
        "message-session-b",
        "session-a",
      ]),
      async () => {
        expect(await rows(filename, "SELECT id, session_id, data FROM message ORDER BY id")).toEqual([
          { id: "message-session-a", session_id: "session-a", data: "{}" },
          { id: "message-session-b", session_id: "session-b", data: "{}" },
        ])
      },
    )

    await expectNoForeignEffect(
      rawWrite(database, "DELETE FROM message WHERE id = ? RETURNING id, session_id", ["message-session-b"]),
      async () => {
        expect(await rows(filename, "SELECT id FROM message ORDER BY id")).toEqual([
          { id: "message-session-a" },
          { id: "message-session-b" },
        ])
      },
    )
  })

  test("INSERT ... SELECT cannot launder a foreign row through a subquery or CTE", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    const database = await openBound(filename, "session-a")

    const subquery =
      "INSERT INTO message (id, session_id, time_created, time_updated, data) " +
      "SELECT 'laundered-subquery', 'session-a', 1, 1, data FROM message " +
      "WHERE id IN (SELECT id FROM message WHERE session_id = 'session-b')"
    await expectNoForeignEffect(rawWrite(database, subquery), async () => {
      expect(await rows(filename, "SELECT id FROM message WHERE id LIKE 'laundered-%'")).toEqual([])
    })

    const cte =
      "WITH stolen AS (SELECT id, data FROM message WHERE session_id = 'session-b') " +
      "INSERT INTO message (id, session_id, time_created, time_updated, data) " +
      "SELECT 'laundered-cte', 'session-a', 1, 1, data FROM stolen"
    await expectNoForeignEffect(rawWrite(database, cte), async () => {
      expect(await rows(filename, "SELECT id FROM message WHERE id LIKE 'laundered-%'")).toEqual([])
    })
  })

  test("UPDATE ... SET data=(SELECT ...) cannot read another session", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    const database = await openBound(filename, "session-a")

    await expectNoForeignEffect(
      rawWrite(
        database,
        "UPDATE message SET data = (SELECT data FROM message WHERE session_id = 'session-b') WHERE session_id = ?",
        ["session-a"],
      ),
      async () => {
        expect(await rows(filename, "SELECT id, session_id, data FROM message ORDER BY id")).toEqual([
          { id: "message-session-a", session_id: "session-a", data: "{}" },
          { id: "message-session-b", session_id: "session-b", data: "{}" },
        ])
      },
    )
  })

  test("conflict clauses cannot move or replace another session's row", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    const database = await openBound(filename, "session-a")

    await expectNoForeignEffect(
      rawWrite(
        database,
        "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, 'session-a', 1, 1, '{}') " +
          "ON CONFLICT(id) DO UPDATE SET session_id = 'session-b', data = 'hacked'",
        ["message-session-a"],
      ),
      async () => {},
    )

    await expectNoForeignEffect(
      rawWrite(
        database,
        "INSERT OR REPLACE INTO message (id, session_id, time_created, time_updated, data) VALUES (?, 'session-b', 1, 1, '{}')",
        ["message-session-a"],
      ),
      async () => {},
    )

    for (const conflict of ["REPLACE", "IGNORE"]) {
      await expectNoForeignEffect(
        rawWrite(database, `UPDATE OR ${conflict} message SET data = 'hacked' WHERE id = ?`, ["message-session-b"]),
        async () => {},
      )
    }

    expect(await rows(filename, "SELECT id, session_id, data FROM message ORDER BY id")).toEqual([
      { id: "message-session-a", session_id: "session-a", data: "{}" },
      { id: "message-session-b", session_id: "session-b", data: "{}" },
    ])
  })

  test("parenthesized FROM and a one-sided join cannot widen the bound view", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    seedRowAddressRows(filename)
    const database = await openBound(filename, "session-a")

    const reads = [
      database.all("SELECT id FROM (message)"),
      database.all("SELECT id FROM (message) AS m"),
      database.all("SELECT id FROM ((message))"),
      database.all("SELECT m.id FROM message m JOIN part p ON p.message_id = m.id WHERE m.session_id = ?", [
        "session-a",
      ]),
    ]
    for (const read of reads) {
      const exit = await Effect.runPromise(Effect.exit(read))
      if (Exit.isSuccess(exit)) {
        const ids = exit.value as ReadonlyArray<{ id: string }>
        expect(ids.every((row) => row.id === "message-session-a")).toBe(true)
      }
    }

    expect(await rows(filename, "SELECT id FROM message ORDER BY id")).toEqual([
      { id: "message-session-a" },
      { id: "message-session-b" },
    ])
  })

  test("?NNN and named scope parameters bind the authoritative view, not a predicate", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    const database = await openBound(filename, "session-a")

    const positional = await Effect.runPromise(
      Effect.exit(
        database.all<{ id: string }>("SELECT id FROM message WHERE id = ?2 AND session_id = ?1", [
          "session-a",
          "message-session-b",
        ]),
      ),
    )
    if (Exit.isSuccess(positional)) expect(positional.value).toEqual([])

    const named = await Effect.runPromise(
      Effect.exit(
        database.all<{ id: string }>("SELECT id FROM message WHERE id = $id AND session_id = $sid", {
          id: "message-session-b",
          sid: "session-a",
        }),
      ),
    )
    if (Exit.isSuccess(named)) expect(named.value).toEqual([])

    expect(await rows(filename, "SELECT id FROM message ORDER BY id")).toEqual([
      { id: "message-session-a" },
      { id: "message-session-b" },
    ])
  })

  test("a write-looking string literal is data, not a statement", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    const database = await openBound(filename, "session-a")

    const literal = await Effect.runPromise(
      database.all<{ name: string }>("SELECT name FROM project WHERE name <> 'insert into message'"),
    )
    expect(literal).toEqual([{ name: "scope" }])

    // Every raw write is refused; only the typed repository may write.
    const write = await Effect.runPromise(
      rawWrite(database, "DELETE FROM project WHERE name <> 'insert into message'").pipe(Effect.flip),
    )
    expect(write).toBeInstanceOf(WorkerStatementRejectedError)
  })

  test("row-addressed event and part reads resolve through the bound view", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    seedRowAddressRows(filename)
    const database = await openBound(filename, "session-a")

    expect(database.drizzleConnection.all("SELECT id FROM event WHERE event.id = ?", ["event-session-a"])).toEqual([
      { id: "event-session-a" },
    ])
    expect(
      database.drizzleConnection.all("SELECT id FROM part WHERE part.message_id = ?", ["message-session-a"]),
    ).toEqual([{ id: "part-session-a" }])

    for (const [sql, params] of [
      ["SELECT id FROM event WHERE event.id = ?", ["event-session-b"]],
      ["SELECT id FROM part WHERE part.message_id = ?", ["message-session-b"]],
    ] as const) {
      const exit = await Effect.runPromise(Effect.exit(database.all(sql, [...params])))
      if (Exit.isSuccess(exit)) expect(exit.value).toEqual([])
    }
  })

  test("structural authorizer denies DDL, ATTACH, dangerous pragmas, and main access", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    const database = await openBound(filename, "session-a")

    const hostile = [
      "CREATE TABLE scope_escape (id text)",
      "DROP TABLE message",
      "ATTACH DATABASE ':memory:' AS escape",
      "DETACH DATABASE main",
      "PRAGMA writable_schema = 1",
      "PRAGMA user_version = 99",
      // Parenthesized pragma arguments bypass the SQL analyzer's `=` check.
      "PRAGMA user_version(99)",
      "PRAGMA journal_mode(delete)",
      "SELECT id FROM main.message",
      "UPDATE main.message SET data = 'stolen' WHERE id = 'message-session-b'",
      "DELETE FROM main.message WHERE id = 'message-session-b'",
      "INSERT INTO main.event (id, aggregate_id, seq, type, data) VALUES ('escape', 'session-b', 0, 'x', '{}')",
    ]
    for (const sql of hostile) {
      const exit = await Effect.runPromise(Effect.exit(database.all(sql)))
      expect(Exit.isFailure(exit)).toBe(true)
    }

    const directDurable = thrownBy(() =>
      database.connection.run(
        "INSERT INTO event (id, aggregate_id, seq, type, data) VALUES ('direct', 'session-a', 0, 'x', '{}')",
      ),
    )
    expect(directDurable).toBeInstanceOf(WorkerStatementRejectedError)

    expect(await rows(filename, "SELECT id, session_id, data FROM message ORDER BY id")).toEqual([
      { id: "message-session-a", session_id: "session-a", data: "{}" },
      { id: "message-session-b", session_id: "session-b", data: "{}" },
    ])
    expect(await rows(filename, "SELECT name FROM sqlite_master WHERE name = 'scope_escape'")).toEqual([])
  })

  test("an upsert cannot claim a foreign primary key with the bound scope", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    const database = await openBound(filename, "session-a")

    const hostile = [
      "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, 'session-a', 1, 1, '{}')",
      "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, 'session-a', 1, 1, '{}') " +
        "ON CONFLICT(id) DO UPDATE SET data = 'hacked'",
      "INSERT OR REPLACE INTO message (id, session_id, time_created, time_updated, data) VALUES (?, 'session-a', 1, 1, '{}')",
      "INSERT OR IGNORE INTO message (id, session_id, time_created, time_updated, data) VALUES (?, 'session-a', 1, 1, '{}')",
      "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, 'session-a', 1, 1, '{}') " +
        "ON CONFLICT(id) DO NOTHING",
    ]
    for (const sql of hostile) {
      await expectNoForeignEffect(rawWrite(database, sql, ["message-session-b"]), async () => {
        expect(await rows(filename, "SELECT id, session_id, data FROM message ORDER BY id")).toEqual([
          { id: "message-session-a", session_id: "session-a", data: "{}" },
          { id: "message-session-b", session_id: "session-b", data: "{}" },
        ])
      })
    }
  })

  test("conflict resolution on a view cannot replace a foreign primary key", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    seedRowAddressRows(filename)
    const database = await openBound(filename, "session-a")

    const hostile = [
      "INSERT OR REPLACE INTO event (id, aggregate_id, seq, type, data) VALUES ('event-session-b', 'session-a', 0, 'x', '{}')",
      "UPDATE OR REPLACE event SET id = 'event-session-b' WHERE id = 'event-session-a' AND aggregate_id = 'session-a'",
      "UPDATE OR REPLACE message SET id = 'message-session-b' WHERE id = 'message-session-a' AND session_id = 'session-a'",
    ]
    for (const sql of hostile) {
      const error = thrownBy(() => database.drizzleConnection.run(sql))
      expect(error).toBeInstanceOf(WorkerStatementRejectedError)
      expect(await rows(filename, "SELECT id, aggregate_id FROM event ORDER BY id")).toEqual([
        { id: "event-session-a", aggregate_id: "session-a" },
        { id: "event-session-b", aggregate_id: "session-b" },
      ])
      expect(await rows(filename, "SELECT id, session_id FROM message ORDER BY id")).toEqual([
        { id: "message-session-a", session_id: "session-a" },
        { id: "message-session-b", session_id: "session-b" },
      ])
    }
  })

  test("top-level OR in UPDATE and DELETE cannot distribute around the scope", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    const database = await openBound(filename, "session-a")

    database.repository.update("message", { data: "own-updated" }, {})
    expect(await rows(filename, "SELECT id, data FROM message ORDER BY id")).toEqual([
      { id: "message-session-a", data: "own-updated" },
      { id: "message-session-b", data: "{}" },
    ])

    // A no-WHERE write gets an explicit scope predicate.
    database.repository.update("message", { data: "no-where" }, {})
    expect(await rows(filename, "SELECT id, data FROM message ORDER BY id")).toEqual([
      { id: "message-session-a", data: "no-where" },
      { id: "message-session-b", data: "{}" },
    ])

    database.repository.delete("message", {})
    expect(await rows(filename, "SELECT id, session_id FROM message ORDER BY id")).toEqual([
      { id: "message-session-b", session_id: "session-b" },
    ])

    expect(thrownBy(() => database.connection.run("DELETE FROM message"))).toBeInstanceOf(WorkerStatementRejectedError)
    expect(await rows(filename, "SELECT id, session_id FROM message ORDER BY id")).toEqual([
      { id: "message-session-b", session_id: "session-b" },
    ])
  })

  test("worker updates to session identity columns are refused", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    const database = await openBound(filename, "session-a")

    const hostile = [
      "UPDATE session SET id = 'session-stolen' WHERE id = 'session-a'",
      "UPDATE session SET directory = '/tmp/evil' WHERE id = 'session-a'",
      "UPDATE session SET project_id = 'project-stolen' WHERE id = 'session-a'",
      "UPDATE session SET parent_id = 'session-b' WHERE id = 'session-a'",
      "UPDATE session SET workspace_id = 'workspace-stolen' WHERE id = 'session-a'",
      "UPDATE session SET permission = '[]' WHERE id = 'session-a'",
      "UPDATE session SET slug = 'stolen' WHERE id = 'session-a'",
      "UPDATE session SET title = 'ok', slug = 'stolen' WHERE id = 'session-a'",
      "UPDATE session SET (id, slug) = ('session-stolen', 'stolen') WHERE id = 'session-a'",
    ]
    for (const sql of hostile) {
      const error = thrownBy(() => database.drizzleConnection.run(sql))
      expect(error).toBeInstanceOf(WorkerStatementRejectedError)
    }

    const repository = thrownBy(() => database.repository.update("session", { slug: "stolen" }, { id: "session-a" }))
    expect(repository).toBeInstanceOf(ImmutableColumnError)

    expect(await rows(filename, "SELECT id, directory, project_id, slug FROM session WHERE id = 'session-a'")).toEqual([
      { id: "session-a", directory: "/tmp/scope", project_id: "project-scope", slug: "session-a" },
    ])

    // A full-row projector update may repeat an identity value it does not change.
    database.repository.update(
      "session",
      {
        id: "session-a",
        directory: "/tmp/scope",
        project_id: "project-scope",
        slug: "session-a",
        title: "allowed",
      },
      { id: "session-a" },
    )
    expect(await rows(filename, "SELECT title FROM session WHERE id = 'session-a'")).toEqual([{ title: "allowed" }])
    expect(SESSION_IMMUTABLE_COLUMNS).toEqual([
      "directory",
      "id",
      "parent_id",
      "permission",
      "project_id",
      "slug",
      "workspace_id",
    ])
  })

  test("bound worker and Bus authorizers refuse secret-bearing hub reads", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    const database = await openBound(filename, "session-a")
    const secrets = ["account", "account_state", "control_account", "credential"]

    for (const table of secrets) {
      for (const sql of [`SELECT * FROM ${table}`, `SELECT * FROM main.${table}`]) {
        const error = thrownBy(() => database.strictConnection.all(sql))
        expect(error).toBeInstanceOf(HubOnlyTableError)
      }
      expect(WORKER_READABLE_HUB_TABLES).not.toContain(table)
    }

    // Non-secret hub state the worker projects stays readable.
    expect(await Effect.runPromise(database.all<{ name: string }>("SELECT name FROM project"))).toEqual([
      { name: "scope" },
    ])

    const read = (table: string) => ({ action: 20, table, column: "id", database: "main", trigger: null })
    for (const authorizer of [
      makeWorkerAuthorizer({ sessionID: "session-a", canAccessMain: () => false }),
      makeBusAuthorizer("session-a"),
    ]) {
      for (const table of secrets) expect(() => authorizer(read(table))).toThrow(HubOnlyTableError)
      expect(authorizer(read("project"))).toBe(0)
    }
  })

  test("UPDATE OR REPLACE cannot delete a foreign row through an expression key", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    seedRowAddressRows(filename)
    const database = await openBound(filename, "session-a")

    const hostile = [
      "UPDATE OR REPLACE message SET id = char(120, 120, 120) WHERE id = 'message-session-a' AND session_id = 'session-a'",
      "UPDATE OR REPLACE message SET id = CAST('message-session-b' AS TEXT) WHERE id = 'message-session-a' AND session_id = 'session-a'",
      "UPDATE OR REPLACE event SET id = char(120) WHERE id = 'event-session-a' AND aggregate_id = 'session-a'",
      "UPDATE OR REPLACE event SET id = CAST('event-session-b' AS TEXT) WHERE id = 'event-session-a' AND aggregate_id = 'session-a'",
      "WITH x AS (SELECT 1) UPDATE OR REPLACE message SET id = char(120) " +
        "WHERE id = 'message-session-a' AND session_id = 'session-a'",
    ]
    for (const sql of hostile) {
      const error = thrownBy(() => database.drizzleConnection.run(sql))
      expect(error).toBeInstanceOf(WorkerStatementRejectedError)
    }

    expect(await rows(filename, "SELECT id, session_id FROM message ORDER BY id")).toEqual([
      { id: "message-session-a", session_id: "session-a" },
      { id: "message-session-b", session_id: "session-b" },
    ])
    expect(await rows(filename, "SELECT id, aggregate_id FROM event ORDER BY id")).toEqual([
      { id: "event-session-a", aggregate_id: "session-a" },
      { id: "event-session-b", aggregate_id: "session-b" },
    ])
  })

  test("own upserts, conflict updates, and returning keep working inside the bound view", async () => {
    const filename = migratedDatabase()
    seedSessions(filename)
    const database = await openBound(filename, "session-a")

    database.repository.upsert(
      "message",
      { id: "message-session-a", time_created: 1, time_updated: 1, data: "{}" },
      { columns: ["id"], update: ["data"] },
    )

    expect(database.repository.update("message", { data: "{}" }, { id: "message-session-a" }).changes).toBe(1)
    expect(database.repository.select("message", { id: "message-session-a" })).toEqual([
      { id: "message-session-a", session_id: "session-a", time_created: 1, time_updated: 1, data: "{}" },
    ])

    database.repository.insert("message", { id: "message-upsert-own", time_created: 1, time_updated: 1, data: "{}" })
    expect(await rows(filename, "SELECT id, session_id FROM message WHERE id = 'message-upsert-own'")).toEqual([
      { id: "message-upsert-own", session_id: "session-a" },
    ])
  })
})
