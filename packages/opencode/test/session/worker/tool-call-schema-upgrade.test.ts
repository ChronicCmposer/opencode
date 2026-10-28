// `tool_call.worker_owner_id` is an additive column.
//
// P0 databases already contain `tool_call` from `CREATE TABLE IF NOT EXISTS`,
// so the create-by-SQL cannot add the generation-fence column to them. The hub
// must upgrade an existing table in place before it marks the schema current,
// otherwise the boot registry gate (`assertTableRegistry`) sees a column drift
// and every fenced start/finish fails with "no column named worker_owner_id".

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { openConnection } from "@opencode-ai/core/database/connection"
import { migrationStatements } from "@/session/writer/migration.statements.gen"
import { createBunSqliteConnection } from "@/session/writer/connection"
import {
  LEDGER_SQL,
  createToolCallLedger,
  toolCallUpgradeStatements,
  toolCallUpgradeStatementsAsync,
  upgradeToolCallSchema,
} from "@/session/ledger/tool-call"
import {
  currentSchemaIdentity,
  ensureProductionSchema,
  ensureProductionSchemaAsync,
  resetSchemaEnsureCache,
  schemaIdentity,
} from "@/session/db/schema"

const SESSION = "ses_ledger_upgrade"
const OWNER = "worker-2-100-generation"

/** The `tool_call` shape shipped by the P0 build (no fence column). */
const LEGACY_TOOL_CALL_DDL = `CREATE TABLE tool_call (
  call_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  tool TEXT NOT NULL,
  side_effecting INTEGER NOT NULL DEFAULT 1 CHECK (side_effecting IN (0, 1)),
  state TEXT NOT NULL CHECK (state IN ('started', 'finished')),
  outcome TEXT,
  time_started INTEGER NOT NULL,
  time_finished INTEGER,
  PRIMARY KEY (call_id, session_id),
  CONSTRAINT tool_call_session_id_session_id_fk FOREIGN KEY (session_id) REFERENCES session(id) ON DELETE CASCADE
)`

const EVENT_SEQUENCE_DDL =
  "CREATE TABLE event_sequence (aggregate_id TEXT PRIMARY KEY, seq INTEGER NOT NULL, owner_id TEXT, worker_owner_id TEXT, worker_heartbeat_at INTEGER, worker_expires_at INTEGER)"

const directories: string[] = []

function columns(connection: { all(sql: string): ReadonlyArray<Record<string, unknown>> }): string[] {
  return connection.all("PRAGMA table_info(tool_call)").map((column) => String(column.name))
}

function fenced(connection: ReturnType<typeof createBunSqliteConnection>) {
  connection.run("INSERT INTO event_sequence (aggregate_id, seq, worker_owner_id) VALUES (?, -1, ?)", [SESSION, OWNER])
  return createToolCallLedger(connection, { fence: { ownerID: OWNER } })
}

/** A file database shaped like a P0 build: stale identity and no fence column. */
function legacyP0Database(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-ledger-upgrade-"))
  directories.push(directory)
  const filename = path.join(directory, "opencode.db")
  const connection = openConnection({ filename, busyTimeoutMs: 5_000 })
  try {
    for (const statement of migrationStatements) connection.exec(statement)
    connection.exec("ALTER TABLE tool_call RENAME TO tool_call_current")
    connection.exec(LEGACY_TOOL_CALL_DDL)
    connection.exec(
      `INSERT INTO tool_call (call_id, session_id, tool, side_effecting, state, outcome, time_started, time_finished)
       SELECT call_id, session_id, tool, side_effecting, state, outcome, time_started, time_finished FROM tool_call_current`,
    )
    connection.exec("DROP TABLE tool_call_current")
    connection.exec("CREATE UNIQUE INDEX IF NOT EXISTS tool_call_unique_call_session ON tool_call (call_id, session_id)")
    connection.run("INSERT OR REPLACE INTO schema_meta (id, content_hash) VALUES ('schema', ?)", ["p0-stale-hash"])
    connection.exec("PRAGMA user_version = 1")
  } finally {
    connection.close()
  }
  return filename
}

afterEach(() => {
  resetSchemaEnsureCache()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("tool_call schema upgrade", () => {
  test("upgrades an old table in place and the fence records the lease owner", () => {
    const connection = createBunSqliteConnection({ filename: ":memory:" })
    try {
      connection.exec(LEGACY_TOOL_CALL_DDL)
      connection.exec(EVENT_SEQUENCE_DDL)
      expect(columns(connection)).not.toContain("worker_owner_id")
      expect(toolCallUpgradeStatements(connection)).toEqual(["ALTER TABLE tool_call ADD COLUMN worker_owner_id TEXT"])

      upgradeToolCallSchema(connection)
      expect(columns(connection)).toContain("worker_owner_id")

      const ledger = fenced(connection)
      const started = ledger.start({ callID: "call_upgraded", sessionID: SESSION, tool: "bash" })
      expect(started.workerOwnerID).toBe(OWNER)

      const finished = ledger.finish({ callID: "call_upgraded", sessionID: SESSION, outcome: "ok" })
      expect(finished).toMatchObject({ state: "finished", outcome: "ok", workerOwnerID: OWNER })
    } finally {
      connection.close()
    }
  })

  test("the upgrade is idempotent", () => {
    const connection = createBunSqliteConnection({ filename: ":memory:" })
    try {
      connection.exec(LEGACY_TOOL_CALL_DDL)
      upgradeToolCallSchema(connection)
      expect(toolCallUpgradeStatements(connection)).toEqual([])
      expect(() => upgradeToolCallSchema(connection)).not.toThrow()
      expect(columns(connection).filter((name) => name === "worker_owner_id")).toHaveLength(1)
    } finally {
      connection.close()
    }
  })

  test("a fresh ledger schema needs no upgrade", () => {
    const connection = createBunSqliteConnection({ filename: ":memory:" })
    try {
      connection.exec(LEDGER_SQL)
      expect(columns(connection)).toContain("worker_owner_id")
      expect(toolCallUpgradeStatements(connection)).toEqual([])
    } finally {
      connection.close()
    }
  })

  test("a table that does not exist yet needs no upgrade", () => {
    const connection = createBunSqliteConnection({ filename: ":memory:" })
    try {
      expect(toolCallUpgradeStatements(connection)).toEqual([])
    } finally {
      connection.close()
    }
  })

  test("the async executor derives the same upgrade and skips a current table", async () => {
    const calls: string[] = []
    const old = {
      query: async (sql: string) => {
        calls.push(sql)
        return [{ name: "call_id" }, { name: "session_id" }]
      },
    }
    expect(await toolCallUpgradeStatementsAsync(old)).toEqual(["ALTER TABLE tool_call ADD COLUMN worker_owner_id TEXT"])

    const current = {
      query: async () => [{ name: "call_id" }, { name: "worker_owner_id" }],
    }
    expect(await toolCallUpgradeStatementsAsync(current)).toEqual([])
    expect(await toolCallUpgradeStatementsAsync({ query: async () => [] })).toEqual([])
    expect(calls).toEqual(["PRAGMA table_info(tool_call)"])
  })

  test("the sync hub ensure upgrades a P0 database before marking it current", () => {
    const filename = legacyP0Database()
    resetSchemaEnsureCache()

    expect(ensureProductionSchema({ filename, liveWorkers: 0 })).toEqual(currentSchemaIdentity())

    const connection = createBunSqliteConnection({ filename })
    try {
      expect(schemaIdentity(connection)).toEqual(currentSchemaIdentity())
      expect(columns(connection)).toContain("worker_owner_id")
      const ledger = fenced(connection)
      ledger.start({ callID: "call_sync_ensure", sessionID: SESSION, tool: "bash" })
      expect(ledger.finish({ callID: "call_sync_ensure", sessionID: SESSION, outcome: "ok" })).toMatchObject({
        state: "finished",
        workerOwnerID: OWNER,
      })
    } finally {
      connection.close()
    }
  })

  test("the async hub ensure upgrades a P0 database before marking it current", async () => {
    const filename = legacyP0Database()
    resetSchemaEnsureCache()

    expect(await ensureProductionSchemaAsync({ filename, liveWorkers: 0 })).toEqual(currentSchemaIdentity())

    const connection = createBunSqliteConnection({ filename })
    try {
      expect(schemaIdentity(connection)).toEqual(currentSchemaIdentity())
      expect(columns(connection)).toContain("worker_owner_id")
      const ledger = fenced(connection)
      ledger.start({ callID: "call_async_ensure", sessionID: SESSION, tool: "bash" })
      expect(ledger.finish({ callID: "call_async_ensure", sessionID: SESSION, outcome: "ok" })).toMatchObject({
        state: "finished",
        workerOwnerID: OWNER,
      })
    } finally {
      connection.close()
    }
  })
})
