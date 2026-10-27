// W1.5 / D6 — the worker-writable table allowlist is derived from `session_id`.
//
// The generated allowlist is compared against the live schema. A table that
// gains or loses `session_id` without a regenerated allowlist fails the gate.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { openConnection, type Connection } from "@opencode-ai/core/database/connection"
import { migrationStatements } from "@/session/writer/migration.statements.gen"
import { SCHEMA_TABLES, WORKER_WRITABLE_TABLES } from "@/session/db/allowlist.gen"
import { deriveSessionScopedTables } from "@/session/db/derive"
import {
  DURABLE_LOG_TABLES,
  HUB_ONLY_TABLES,
  TableAllowlistDriftError,
  assertTableAllowlist,
  classifyTables,
  isWorkerWritable,
} from "@/session/db/ownership"
import { markSchemaIdentity } from "@/session/db/schema"

const directories: string[] = []
const connections: Connection[] = []

function migrated(): Connection {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-own-"))
  directories.push(directory)
  const connection = openConnection({ filename: path.join(directory, "opencode.db") })
  connections.push(connection)
  for (const statement of migrationStatements) connection.exec(statement)
  markSchemaIdentity(connection)
  return connection
}

afterEach(() => {
  for (const connection of connections.splice(0)) connection.close()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("table ownership (W1.5 / D6)", () => {
  test("the generated allowlist is exactly the session_id tables in the schema", () => {
    const connection = migrated()
    expect(deriveSessionScopedTables(connection)).toEqual([...WORKER_WRITABLE_TABLES])
    expect([...SCHEMA_TABLES]).toEqual(
      connection
        .all<{ name: string }>("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        .map((r) => r.name),
    )
  })

  test("the ownership report is clean for the shipped schema", () => {
    expect(() => assertTableAllowlist(migrated())).not.toThrow()
  })

  test("a bare session_id column is not ownership evidence", () => {
    const connection = migrated()
    connection.exec("ALTER TABLE project ADD COLUMN session_id text")

    expect(() => assertTableAllowlist(connection)).not.toThrow()
  })

  test("adding a session_id foreign key to a hub-only table fails the gate", () => {
    const connection = migrated()
    connection.exec("ALTER TABLE project ADD COLUMN session_id text REFERENCES session(id)")

    const error = (() => {
      try {
        assertTableAllowlist(connection)
      } catch (cause) {
        return cause
      }
    })()

    expect(error).toBeInstanceOf(TableAllowlistDriftError)
    expect((error as TableAllowlistDriftError).sessionScoped.unexpected).toContain("project")
    expect((error as Error).message).toMatch(/session_id tables changed/)
  })

  test("removing a session_id table fails the gate", () => {
    const connection = migrated()
    connection.exec("DROP TABLE session_share")

    const error = (() => {
      try {
        assertTableAllowlist(connection)
      } catch (cause) {
        return cause
      }
    })()

    expect(error).toBeInstanceOf(TableAllowlistDriftError)
    expect((error as TableAllowlistDriftError).sessionScoped.missing).toContain("session_share")
  })

  test("hub-only includes leases, auth, project state, and schema state", () => {
    for (const table of ["permission", "account", "project", "session", "migration"]) {
      expect(HUB_ONLY_TABLES).toContain(table)
    }
    for (const table of WORKER_WRITABLE_TABLES) {
      expect(HUB_ONLY_TABLES).not.toContain(table)
      expect(isWorkerWritable(table)).toBe(true)
    }
  })

  test("the durable log is not part of session-scoped ownership", () => {
    expect(DURABLE_LOG_TABLES).toEqual(["event", "event_sequence", "event_blob"])
    expect(WORKER_WRITABLE_TABLES).not.toContain("event")
    expect(WORKER_WRITABLE_TABLES).not.toContain("event_sequence")
    expect(WORKER_WRITABLE_TABLES).not.toContain("event_blob")
  })

  test("the ledger tables cascade with the session aggregate", () => {
    const connection = migrated()
    connection.run(
      "INSERT INTO project (id, name, worktree, sandboxes, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
      ["project-ledger", "ledger", "/tmp/ledger", "[]", 1, 1],
    )
    connection.run(
      "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      ["session-ledger", "project-ledger", "ledger", "/tmp/ledger", "ledger", "1", 1, 1],
    )
    connection.run(
      "INSERT INTO tool_call (call_id, session_id, tool, side_effecting, state, time_started) VALUES (?, ?, ?, 1, 'started', 1)",
      ["call-1", "session-ledger", "bash"],
    )
    connection.run(
      "INSERT INTO worker_crash (crash_id, session_id, worker_id, phase, in_flight_call_ids, reason, time_created) VALUES (?, ?, 1, 'boot', '[]', 'boom', 1)",
      ["crash-1", "session-ledger"],
    )

    connection.run("DELETE FROM session WHERE id = ?", ["session-ledger"])

    expect(connection.all("SELECT call_id FROM tool_call")).toEqual([])
    expect(connection.all("SELECT crash_id FROM worker_crash")).toEqual([])
  })

  test("classification derives the same split as the constants", () => {
    const observed = classifyTables(migrated())
    expect(observed.workerWritable).toEqual([...WORKER_WRITABLE_TABLES])
    expect(observed.hubOnly).toEqual([...HUB_ONLY_TABLES])
  })
})
