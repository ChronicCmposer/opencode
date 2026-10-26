// W1.6 / D7 — schema gate.
//
// Version is `PRAGMA user_version` plus a migration-content hash. The hub
// migrates only with zero live workers and marks the schema; a worker probes
// read-only and refuses a mismatch without running DDL or marking state.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { Database as BunDatabase } from "bun:sqlite"
import { afterEach, describe, expect, test } from "bun:test"
import { openConnection, type Connection } from "@opencode-ai/core/database/connection"
import { migrationStatements } from "@/session/writer/migration.statements.gen"
import {
  LiveWorkersError,
  SchemaMismatch,
  assertSchemaIdentity,
  currentSchemaIdentity,
  markSchemaIdentity,
  migrate,
  openGatedConnection,
  schemaIdentity,
} from "@/session/db"

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

describe("worker schema gate (W1.6 / D7)", () => {
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

describe("hub migration (W1.6 / D7)", () => {
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

  test("a fresh worker gate refuses an unmigrated database", () => {
    const filename = databasePath()
    open(filename).close()

    const error = refusal(() => openGatedConnection({ filename }))

    expect(error).toBeInstanceOf(SchemaMismatch)
    expect((error as SchemaMismatch).observed.version).toBe(0)
    expect((error as SchemaMismatch).observed.contentHash).not.toBe(currentSchemaIdentity().contentHash)
  })
})
