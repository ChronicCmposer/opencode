// The async hub boot path (`ensureProductionSchemaAsync`) must serialize on the
// cross-process gate, keep the lease alive for a slow migration, and never
// replay a statement inside an open transaction. These tests exercise the
// executor path under contention, not just the single-threaded happy path.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { SqliteDriverMismatchError, openConnection } from "@opencode-ai/core/database/connection"
import {
  SchemaGateLostError,
  acquireSchemaGate,
  newGateToken,
  openSchemaGate,
  releaseSchemaGate,
} from "@opencode-ai/core/database/schema-gate"
import {
  MissingQueryInTransactionError,
  currentSchemaIdentity,
  ensureProductionSchemaAsync,
  resetSchemaEnsureCache,
  schemaGateFilename,
  schemaIdentity,
} from "@/session/db/schema"
import { createDbExecutor, type DbExecutor } from "@/session/worker/db-executor"

const directories: string[] = []

function databasePath(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-schema-async-"))
  directories.push(directory)
  return path.join(directory, "opencode.db")
}

function journalIDs(filename: string): readonly string[] {
  const connection = openConnection({ filename })
  try {
    return connection.all<{ id: string }>("SELECT id FROM migration ORDER BY id").map((row) => row.id)
  } finally {
    connection.close()
  }
}

afterEach(() => {
  resetSchemaEnsureCache()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("async executor schema gate", () => {
  test("a driver mismatch refuses boot before the database is touched", async () => {
    const filename = databasePath()

    const boot = ensureProductionSchemaAsync({
      filename,
      liveWorkers: 0,
      assertDriverParity: () => {
        throw new SqliteDriverMismatchError({ detail: "version 3.51.0 (bun) vs 3.45.0 (node)" })
      },
    })

    await expect(boot).rejects.toBeInstanceOf(SqliteDriverMismatchError)
    expect(fs.existsSync(filename)).toBe(false)
  })

  test("two callers on a fresh file produce one complete journal", async () => {
    const filename = databasePath()

    const [first, second] = await Promise.all([
      ensureProductionSchemaAsync({ filename, liveWorkers: 0 }),
      ensureProductionSchemaAsync({ filename, liveWorkers: 0 }),
    ])

    expect(first).toEqual(currentSchemaIdentity())
    expect(second).toEqual(currentSchemaIdentity())
    expect(fs.existsSync(schemaGateFilename(filename))).toBe(true)

    const ids = journalIDs(filename)
    expect(new Set(ids).size).toBe(ids.length)
    expect(ids.length).toBeGreaterThan(0)

    const raw = openConnection({ filename })
    try {
      expect(schemaIdentity(raw)).toEqual(currentSchemaIdentity())
    } finally {
      raw.close()
    }
  }, 60_000)

  test("waits while another process holds the gate, then completes once", async () => {
    const filename = databasePath()
    const gate = openSchemaGate(filename)
    const token = newGateToken()
    expect(acquireSchemaGate(gate, token)).toBe(true)

    let resolveGateWait!: () => void
    const gateWait = new Promise<void>((resolve) => {
      resolveGateWait = resolve
    })
    let settled = false
    const pending = ensureProductionSchemaAsync({
      filename,
      liveWorkers: 0,
      onGateWait: resolveGateWait,
    }).then(
      (identity) => {
        settled = true
        return identity
      },
      (error: unknown) => {
        settled = true
        throw error
      },
    )

    try {
      await gateWait
      expect(settled).toBe(false)
    } finally {
      releaseSchemaGate(gate, token)
      gate.close()
    }

    await expect(pending).resolves.toEqual(currentSchemaIdentity())
    expect(settled).toBe(true)
    const ids = journalIDs(filename)
    expect(new Set(ids).size).toBe(ids.length)
  }, 60_000)

  test("a held write lock rolls back and retries the whole migration batch", async () => {
    const filename = databasePath()
    const lock = openConnection({ filename, busyTimeoutMs: 0 })
    lock.exec("BEGIN IMMEDIATE")

    let resolveBusy!: () => void
    const busy = new Promise<void>((resolve) => {
      resolveBusy = resolve
    })
    let settled = false
    const pending = ensureProductionSchemaAsync({
      filename,
      liveWorkers: 0,
      onMigrationBusy: resolveBusy,
    }).then(
      (identity) => {
        settled = true
        return identity
      },
      (error: unknown) => {
        settled = true
        throw error
      },
    )

    try {
      await busy
      expect(settled).toBe(false)
      // No partial schema may be visible while the batch is blocked: the
      // transaction was either not started or rolled back, never half applied.
      expect(journalIDsSafe(filename)).toEqual([])
    } finally {
      lock.exec("COMMIT")
      lock.close()
    }

    expect(await pending).toEqual(currentSchemaIdentity())
    const ids = journalIDs(filename)
    expect(new Set(ids).size).toBe(ids.length)
    expect(ids.length).toBeGreaterThan(0)
  }, 60_000)

  test("a stolen lease aborts the migration before the ledger/marker commit", async () => {
    const filename = databasePath()
    const real = createDbExecutor({ filename })
    let resolveStarted!: () => void
    const started = new Promise<void>((resolve) => {
      resolveStarted = resolve
    })
    let releaseBatch!: () => void
    const blocked = new Promise<void>((resolve) => {
      releaseBatch = resolve
    })
    let sawBatch = false
    const executor: DbExecutor = {
      run: (sql, params) => real.run(sql, params),
      query: (sql, params) => real.query(sql, params),
      transaction: (statements) => real.transaction(statements),
      queryInTransaction: async (sql, params) => {
        if (!sawBatch) {
          sawBatch = true
          resolveStarted()
          await blocked
        }
        const inTransaction = real.queryInTransaction
        return inTransaction === undefined ? real.query(sql, params) : inTransaction.call(real, sql, params)
      },
      close: () => real.close(),
      get threadId() {
        return real.threadId
      },
    }
    const thief = openSchemaGate(filename)

    try {
      const outcome = ensureProductionSchemaAsync({
        filename,
        liveWorkers: 0,
        executor,
        renewIntervalMs: 5,
      }).then(
        (identity) => ({ tag: "resolved" as const, identity }),
        (error: unknown) => ({ tag: "failed" as const, error }),
      )

      await started
      // Take the row after this process proved ownership and started its batch.
      thief.run("UPDATE schema_gate SET owner = 'thief', expires_at = ? WHERE id = 1", [Date.now() + 60_000])
      releaseBatch()

      const result = await outcome
      expect(result.tag).toBe("failed")
      if (result.tag === "failed") expect(result.error).toBeInstanceOf(SchemaGateLostError)

      // Documented guarantee (not "the batch is aborted mid-flight"): the
      // already-started core batch is allowed to finish before the executor is
      // closed, serialized by `BEGIN IMMEDIATE` plus the in-transaction journal
      // recheck. Ownership is re-proven after the batch, so the ledger/marker
      // commit is refused on the stolen lease.
      expect(journalIDs(filename).length).toBeGreaterThan(0)
      const raw = openConnection({ filename })
      try {
        expect(schemaIdentity(raw).version).toBe(0)
      } finally {
        raw.close()
      }
    } finally {
      releaseBatch()
      thief.close()
      await real.close()
    }
  }, 60_000)

  test("a setup throw after the lease is acquired releases it immediately", async () => {
    const filename = databasePath()
    // Simulate `openExecutor()` throwing after `waitForSchemaGateAsync` won the
    // lease: without the post-acquisition finally the row would stay live for
    // the full TTL.
    const options = {
      filename,
      liveWorkers: 0,
      get executor(): DbExecutor {
        throw new Error("executor setup failed")
      },
    }

    await expect(ensureProductionSchemaAsync(options)).rejects.toThrow("executor setup failed")

    const gate = openSchemaGate(filename)
    const token = newGateToken()
    try {
      expect(acquireSchemaGate(gate, token)).toBe(true)
    } finally {
      releaseSchemaGate(gate, token)
      gate.close()
    }
  })

  test("the migration path refuses an executor without queryInTransaction", async () => {
    const filename = databasePath()
    const real = createDbExecutor({ filename })
    // A custom executor that lacks the in-transaction query would replay busy
    // DML inside the migration batch (F1). It must fail fast, not fall back.
    const legacy: DbExecutor = {
      run: (sql, params) => real.run(sql, params),
      query: (sql, params) => real.query(sql, params),
      transaction: (statements) => real.transaction(statements),
      close: () => real.close(),
      get threadId() {
        return real.threadId
      },
    }

    try {
      await expect(
        ensureProductionSchemaAsync({ filename, liveWorkers: 0, executor: legacy }),
      ).rejects.toBeInstanceOf(MissingQueryInTransactionError)
    } finally {
      await real.close()
    }
  })
})

function journalIDsSafe(filename: string): readonly string[] {
  try {
    return journalIDs(filename)
  } catch {
    return []
  }
}
