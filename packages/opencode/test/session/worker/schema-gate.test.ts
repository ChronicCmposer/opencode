// Schema gate.
//
// Version is `PRAGMA user_version` plus a migration-content hash. The hub
// migrates only with zero live workers and marks the schema; a worker probes
// read-only and refuses a mismatch without running DDL or marking state.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { fileURLToPath } from "node:url"
import { Database as BunDatabase } from "bun:sqlite"
import { afterEach, describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import { Database as CoreDatabase } from "@opencode-ai/core/database/database"
import { openConnection, type Connection } from "@opencode-ai/core/database/connection"
import { migrationStatements } from "@/session/writer/migration.statements.gen"
import { hashStatements } from "@/session/db/identity"
import {
  LiveWorkersError,
  SchemaMismatch,
  assertSchemaIdentity,
  currentSchemaIdentity,
  ensureProductionSchema,
  ensureProductionSchemaAsync,
  markSchemaIdentity,
  migrate,
  migrationContentHash,
  openGatedConnection,
  resetSchemaEnsureCache,
  schemaGateFilename,
  schemaIdentity,
} from "@/session/db/schema"

const directories: string[] = []
const connections: Connection[] = []

function databasePath(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-schema-"))
  directories.push(directory)
  return path.join(directory, "opencode.db")
}

function open(filename: string): Connection {
  const connection = openConnection({ filename })
  connections.push(connection)
  return connection
}

function migrated(): { filename: string; connection: Connection } {
  const filename = databasePath()
  const connection = open(filename)
  for (const statement of migrationStatements) connection.exec(statement)
  markSchemaIdentity(connection)
  return { filename, connection }
}

interface Snapshot {
  readonly schema: readonly Record<string, unknown>[]
  readonly version: number
  readonly journal: readonly Record<string, unknown>[]
}

function snapshot(filename: string): Snapshot {
  const native = new BunDatabase(filename, { readonly: true })
  try {
    return {
      schema: native.query("SELECT type, name, sql FROM sqlite_master ORDER BY type, name").all() as Record<string, unknown>[],
      version: Number((native.query("PRAGMA user_version").get() as { user_version: number }).user_version),
      journal: native.query("SELECT id, time_completed FROM migration ORDER BY id").all() as Record<string, unknown>[],
    }
  } finally {
    native.close()
  }
}

function refusal(run: () => unknown): unknown {
  try {
    run()
  } catch (cause) {
    return cause
  }
}

afterEach(() => {
  for (const connection of connections.splice(0)) connection.close()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("worker schema gate (D7)", () => {
  test("opens a database whose version and migration content match the build", () => {
    const { filename } = migrated()
    const gated = openGatedConnection({ filename })
    connections.push(gated.connection)

    expect(gated.identity).toEqual(currentSchemaIdentity())
    expect(gated.connection.filename).toBe(filename)
  })

  test("refuses a different user_version with a typed SchemaMismatch", () => {
    const { filename } = migrated()
    const admin = open(filename)
    admin.exec("PRAGMA user_version = 999")
    admin.close()

    const error = refusal(() => openGatedConnection({ filename }))

    expect(error).toBeInstanceOf(SchemaMismatch)
    expect((error as SchemaMismatch).tag).toBe("WorkerDb.SchemaMismatch")
    expect((error as SchemaMismatch).expected.version).not.toBe((error as SchemaMismatch).observed.version)
  })

  test("refuses a migration-content mismatch even when the version matches", () => {
    const { filename } = migrated()
    const admin = open(filename)
    admin.exec("DELETE FROM migration WHERE id = (SELECT id FROM migration ORDER BY id LIMIT 1)")
    admin.close()

    const error = refusal(() => openGatedConnection({ filename }))

    expect(error).toBeInstanceOf(SchemaMismatch)
    expect((error as SchemaMismatch).expected.contentHash).not.toBe((error as SchemaMismatch).observed.contentHash)
  })

  test("never runs DDL or marks schema state when it refuses", () => {
    const { filename } = migrated()
    const admin = open(filename)
    admin.exec("PRAGMA user_version = 123")
    const before = snapshot(filename)
    admin.close()

    const error = refusal(() => openGatedConnection({ filename }))
    expect(error).toBeInstanceOf(SchemaMismatch)
    expect(snapshot(filename)).toEqual(before)
    expect(snapshot(filename).version).toBe(123)
  })

  test("the identity probe is read-only and reports the observed journal hash", () => {
    const { filename, connection } = migrated()
    const before = snapshot(filename)

    expect(schemaIdentity(connection)).toEqual(currentSchemaIdentity())
    expect(assertSchemaIdentity(connection)).toEqual(currentSchemaIdentity())
    expect(snapshot(filename)).toEqual(before)
  })

  test("marking an incomplete journal is refused", () => {
    const { filename, connection } = migrated()
    connection.exec("DELETE FROM migration WHERE id = (SELECT id FROM migration ORDER BY id LIMIT 1)")

    expect(() => markSchemaIdentity(connection)).toThrow(SchemaMismatch)
  })
})

describe("hub migration (D7)", () => {
  test("migrates and marks only with zero live workers", () => {
    const filename = databasePath()
    const connection = open(filename)

    const identity = migrate({ connection, liveWorkers: 0 })

    expect(identity).toEqual(currentSchemaIdentity())
    expect(schemaIdentity(connection)).toEqual(currentSchemaIdentity())
    expect(openGatedConnection({ filename }).connection.close()).toBeUndefined()
  })

  test("refuses to migrate while a worker is live and changes nothing", () => {
    const filename = databasePath()
    const connection = open(filename)
    const before = {
      schema: connection.all("SELECT type, name, sql FROM sqlite_master ORDER BY type, name"),
      version: Number(connection.pragma("user_version")),
    }

    const error = refusal(() => migrate({ connection, liveWorkers: 1 }))

    expect(error).toBeInstanceOf(LiveWorkersError)
    expect((error as LiveWorkersError).tag).toBe("WorkerDb.LiveWorkers")
    expect({
      schema: connection.all("SELECT type, name, sql FROM sqlite_master ORDER BY type, name"),
      version: Number(connection.pragma("user_version")),
    }).toEqual(before)
  })

  test("a statement failure after the DDL rolls the whole migration back", () => {
    const filename = databasePath()
    const connection = open(filename)
    const failing = [...migrationStatements, "THIS IS NOT VALID SQL"]

    expect(() => migrate({ connection, liveWorkers: 0, statements: failing })).toThrow()

    // DDL and the marker are in the same BEGIN IMMEDIATE: neither survives.
    expect(connection.all("SELECT name FROM sqlite_master WHERE type = 'table'")).toEqual([])
    expect(schemaIdentity(connection).version).toBe(0)
    expect(schemaIdentity(connection).contentHash).not.toBe(currentSchemaIdentity().contentHash)
  })

  test("a crashed migration is retryable into a consistent marked schema", () => {
    const filename = databasePath()
    const connection = open(filename)

    expect(() =>
      migrate({ connection, liveWorkers: 0, statements: [...migrationStatements, "THIS IS NOT VALID SQL"] }),
    ).toThrow()
    expect(schemaIdentity(connection).version).toBe(0)

    expect(migrate({ connection, liveWorkers: 0 })).toEqual(currentSchemaIdentity())
    expect(schemaIdentity(connection)).toEqual(currentSchemaIdentity())
    expect(openGatedConnection({ filename }).connection.close()).toBeUndefined()
  })

  test("changing a SQL body without changing the journal ids changes the hash", () => {
    const ids = ["20260101000000_first", "20260102000000_second"]
    const before = hashStatements(["CREATE TABLE a (id TEXT PRIMARY KEY)"])
    const after = hashStatements(["CREATE TABLE a (id TEXT PRIMARY KEY, extra TEXT)"])
    expect(after).not.toBe(before)

    expect(migrationContentHash({ version: 1, ids, bodyHash: before })).not.toBe(
      migrationContentHash({ version: 1, ids, bodyHash: after }),
    )
  })

  test("a gated open of a missing database is refused without creating the file", () => {
    const filename = databasePath()
    expect(fs.existsSync(filename)).toBe(false)

    expect(() => openGatedConnection({ filename })).toThrow()

    expect(fs.existsSync(filename)).toBe(false)
  })

  test("a fresh worker gate refuses an unmigrated database", () => {
    const filename = databasePath()
    open(filename).close()

    const error = refusal(() => openGatedConnection({ filename }))

    expect(error).toBeInstanceOf(SchemaMismatch)
    expect((error as SchemaMismatch).observed.version).toBe(0)
    expect((error as SchemaMismatch).observed.contentHash).not.toBe(currentSchemaIdentity().contentHash)
  })
})


describe("cross-process schema gate", () => {
  test("two executors on the same fresh file serialize into one complete journal", async () => {
    resetSchemaEnsureCache()
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-schema-gate-"))
    directories.push(directory)
    const filename = path.join(directory, "concurrent.db")

    const [first, second] = await Promise.all([
      ensureProductionSchemaAsync({ filename, liveWorkers: 0 }),
      ensureProductionSchemaAsync({ filename, liveWorkers: 0 }),
    ])

    expect(first).toEqual(currentSchemaIdentity())
    expect(second).toEqual(currentSchemaIdentity())
    expect(fs.existsSync(schemaGateFilename(filename))).toBe(true)

    const raw = openConnection({ filename })
    connections.push(raw)
    expect(schemaIdentity(raw)).toEqual(currentSchemaIdentity())
    const journal = raw.all<{ id: string }>("SELECT id FROM migration ORDER BY id").map((row) => row.id)
    expect(new Set(journal).size).toBe(journal.length)
  }, 60_000)

  test("two processes starting together still produce one complete schema", async () => {
    const modulePath = fileURLToPath(new URL("../../../src/session/db/schema.ts", import.meta.url))
    // Both children block on a barrier file the parent creates after spawning
    // them, so the race starts together without a startup sleep.
    const script = `
      import { existsSync } from "node:fs"
      import { ensureProductionSchemaAsync } from ${JSON.stringify(modulePath)}
      while (!existsSync(process.env.BARRIER)) await Bun.sleep(5)
      try {
        await ensureProductionSchemaAsync({ filename: process.env.DB_FILE, liveWorkers: 0 })
        console.log("ok")
      } catch (error) {
        console.error(error)
        process.exit(1)
      }
    `
    const cwd = path.resolve(import.meta.dir, "../../..")

    for (let iteration = 0; iteration < 3; iteration += 1) {
      const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-schema-gate-"))
      directories.push(directory)
      const filename = path.join(directory, `processes-${iteration}.db`)
      const barrier = path.join(directory, "barrier")
      const children = [0, 1].map(() =>
        Bun.spawn([process.execPath, "-e", script], {
          cwd,
          env: { ...process.env, DB_FILE: filename, BARRIER: barrier },
          stdout: "pipe",
          stderr: "pipe",
        }),
      )
      await Bun.write(barrier, "go")
      const results = await Promise.all(
        children.map(async (child) => {
          const [out, err] = await Promise.all([
            new Response(child.stdout).text(),
            new Response(child.stderr).text(),
          ])
          return { exitCode: await child.exited, out: out.trim(), err: err.trim() }
        }),
      )
      expect(results.map((result) => ({ exitCode: result.exitCode, out: result.out, err: result.err }))).toEqual([
        { exitCode: 0, out: "ok", err: "" },
        { exitCode: 0, out: "ok", err: "" },
      ])

      const raw = openConnection({ filename })
      connections.push(raw)
      expect(schemaIdentity(raw)).toEqual(currentSchemaIdentity())
      const journal = raw.all<{ id: string }>("SELECT id FROM migration ORDER BY id").map((row) => row.id)
      expect(new Set(journal).size).toBe(journal.length)
    }
  }, 180_000)

  test("sync ensure retries a contended legacy file with a blocking budget", async () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-schema-sync-"))
    directories.push(directory)
    const filename = path.join(directory, "legacy.db")
    const ready = path.join(directory, "lock-ready")
    const busy = path.join(directory, "begin-busy")

    // Seed core's migration journal without the hub-owned ledger/marker, then
    // contend the write lock in a separate process (a blocking retry cannot let
    // a same-thread timer release it).
    await Effect.runPromise(Effect.scoped(Layer.build(CoreDatabase.layerFromPath(filename))))

    // Deterministic handshake: the holder keeps the write lock until the ensure
    // process publishes that it actually observed a busy BEGIN, so the retry is
    // exercised regardless of host speed (no fixed hold duration).
    const holder = Bun.spawn(
      [
        process.execPath,
        "-e",
        `
          import { Database } from "bun:sqlite"
          import { existsSync, writeFileSync } from "node:fs"
          const db = new Database(process.env.DB_FILE)
          db.exec("PRAGMA journal_mode = WAL")
          db.exec("PRAGMA busy_timeout = 0")
          db.exec("BEGIN IMMEDIATE")
          writeFileSync(process.env.READY, "ready")
          const deadline = Date.now() + 30000
          while (!existsSync(process.env.BUSY) && Date.now() < deadline) await Bun.sleep(5)
          db.exec("COMMIT")
          db.close()
        `,
      ],
      { env: { ...process.env, DB_FILE: filename, READY: ready, BUSY: busy }, stdout: "pipe", stderr: "pipe" },
    )

    const modulePath = fileURLToPath(new URL("../../../src/session/db/schema.ts", import.meta.url))
    const ensure = Bun.spawn(
      [
        process.execPath,
        "-e",
        `
          import { writeFileSync } from "node:fs"
          import { ensureProductionSchema } from ${JSON.stringify(modulePath)}
          try {
            ensureProductionSchema({
              filename: process.env.DB_FILE,
              liveWorkers: 0,
              onMigrationBusy: () => writeFileSync(process.env.BUSY, "busy"),
            })
            console.log("ok")
          } catch (error) {
            console.error(error)
            process.exit(1)
          }
        `,
      ],
      {
        cwd: path.resolve(import.meta.dir, "../../.."),
        env: { ...process.env, DB_FILE: filename, BUSY: busy },
        stdout: "pipe",
        stderr: "pipe",
      },
    )

    try {
      while (!fs.existsSync(ready)) await Bun.sleep(5)
      const [out, err] = await Promise.all([new Response(ensure.stdout).text(), new Response(ensure.stderr).text()])
      const exitCode = await ensure.exited
      expect({ exitCode, out: out.trim(), err: err.trim() }).toEqual({ exitCode: 0, out: "ok", err: "" })
    } finally {
      await holder.exited
    }

    const raw = openConnection({ filename })
    connections.push(raw)
    expect(schemaIdentity(raw)).toEqual(currentSchemaIdentity())
    const journal = raw.all<{ id: string }>("SELECT id FROM migration ORDER BY id").map((row) => row.id)
    expect(new Set(journal).size).toBe(journal.length)
  }, 120_000)
})
