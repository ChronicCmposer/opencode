import { describe, expect, setDefaultTimeout, test } from "bun:test"
import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { threadId as mainThreadId } from "node:worker_threads"
import { Database as BunDatabase } from "bun:sqlite"
import {
  createDbExecutor,
  DbExecutorClosedError,
  DbExecutorCrashedError,
  DbExecutorRemoteError,
  DbExecutorRollbackError,
  DbExecutorTimeoutError,
  type DbExecutor,
} from "@/session/worker/db-executor"
import { DEFAULT_TEST_TIMEOUT_MS } from "../../lib/worker-timeouts"

// Bun resets the per-file default to 5s. The deadline test's executor budget
// alone is 5s, so the runner must not clip the surrounding test.
setDefaultTimeout(DEFAULT_TEST_TIMEOUT_MS)

const BURN_SQL =
  "WITH RECURSIVE cnt(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM cnt WHERE x < 2000000) SELECT sum(x) AS total FROM cnt"

async function withExecutor(run: (executor: DbExecutor) => Promise<void>): Promise<void> {
  const executor = createDbExecutor({ filename: ":memory:" })
  try {
    await run(executor)
  } finally {
    await executor.close()
  }
}

interface LoopGap {
  readonly maxGapMs: number
  readonly ticks: number
}

function observeLoopGap(intervalMs: number): { sample: () => LoopGap; stop: () => LoopGap } {
  let last = performance.now()
  let maxGapMs = 0
  let ticks = 0
  const timer = setInterval(() => {
    const now = performance.now()
    maxGapMs = Math.max(maxGapMs, now - last)
    last = now
    ticks += 1
  }, intervalMs)
  const sample = () => ({ maxGapMs, ticks })
  return {
    sample,
    stop: () => {
      clearInterval(timer)
      return sample()
    },
  }
}

describe("db executor", () => {
  test("forwards run/query/transaction and serializes concurrent requests", async () => {
    await withExecutor(async (executor) => {
      await executor.run("CREATE TABLE item (id INTEGER PRIMARY KEY, name TEXT)")
      const inserted = await executor.transaction([
        { sql: "INSERT INTO item (name) VALUES (?)", params: ["a"] },
        { sql: "INSERT INTO item (name) VALUES (?)", params: ["b"] },
      ])
      expect(inserted).toEqual([
        { changes: 1, lastInsertRowid: 1 },
        { changes: 1, lastInsertRowid: 2 },
      ])

      const queries = await Promise.all(
        Array.from({ length: 8 }, () => executor.query("SELECT name FROM item ORDER BY id")),
      )
      for (const rows of queries) expect(rows).toEqual([{ name: "a" }, { name: "b" }])
    })
  })

  test("a failing statement inside a transaction rolls the whole forward back", async () => {
    await withExecutor(async (executor) => {
      await executor.run("CREATE TABLE item (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
      const outcome = await executor
        .transaction([
          { sql: "INSERT INTO item (name) VALUES (?)", params: ["kept?"] },
          { sql: "INSERT INTO item (name) VALUES (?)", params: [null] },
        ])
        .then(
          () => "resolved",
          (error: unknown) => (error instanceof DbExecutorRemoteError ? "remote" : `other:${String(error)}`),
        )
      expect(outcome).toBe("remote")
      expect(await executor.query("SELECT * FROM item")).toEqual([])
    })
  })

  test("keeps the synchronous driver off the hub event loop", async () => {
    await withExecutor(async (executor) => {
      const probe = observeLoopGap(5)
      const started = performance.now()
      const rows = await executor.query(BURN_SQL)
      const durationMs = performance.now() - started
      const gap = probe.stop()

      expect(rows).toEqual([{ total: expect.any(Number) }])
      // The recursive query is the blocking unit; on-loop it would stall the
      // timer for the same duration. Off-loop the hub keeps ticking.
      expect(durationMs).toBeGreaterThan(80)
      expect(gap.maxGapMs).toBeLessThan(durationMs / 2)
      expect(gap.ticks).toBeGreaterThan(2)
    })
  })

  test("rejects after close and does not respawn", async () => {
    const executor = createDbExecutor({ filename: ":memory:" })
    await executor.query("SELECT 1 AS n")
    await executor.close()
    await executor.close()
    const outcome = await executor.query("SELECT 1 AS n").then(
      () => "resolved",
      (error: unknown) => (error instanceof DbExecutorClosedError ? "closed" : String(error)),
    )
    expect(outcome).toBe("closed")
  })
})

function tempDatabase(): { filename: string; directory: string } {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-db-executor-"))
  return { filename: path.join(directory, "opencode.db"), directory }
}

describe("db executor hardening", () => {
  test("opens with WAL and the resolved busy timeout before the first request", async () => {
    const { filename, directory } = tempDatabase()
    const executor = createDbExecutor({ filename, env: { OPENCODE_DB_BUSY_TIMEOUT_MS: "1234" } })
    try {
      await executor.query("SELECT 1 AS n")
      const mode = await executor.query("PRAGMA journal_mode")
      const busy = await executor.query("PRAGMA busy_timeout")
      expect(mode).toEqual([{ journal_mode: "wal" }])
      expect(busy).toEqual([{ timeout: 1234 }])
    } finally {
      await executor.close()
      fs.rmSync(directory, { recursive: true, force: true })
    }
  })

  test("waits for a held write lock instead of failing immediately", async () => {
    const { filename, directory } = tempDatabase()
    const holder = new BunDatabase(filename)
    holder.exec("PRAGMA journal_mode = WAL")
    holder.exec("CREATE TABLE item (id INTEGER PRIMARY KEY, name TEXT)")
    holder.exec("BEGIN IMMEDIATE")

    const executor = createDbExecutor({ filename })
    try {
      const release = setTimeout(() => {
        try {
          holder.exec("ROLLBACK")
        } catch {
          // The holder was already released.
        }
      }, 120)
      const inserted = await executor.run("INSERT INTO item (name) VALUES (?)", ["kept"])
      clearTimeout(release)
      expect(inserted.changes).toBe(1)
      expect(await executor.query("SELECT name FROM item")).toEqual([{ name: "kept" }])
    } finally {
      try {
        holder.exec("ROLLBACK")
      } catch {
        // Already released.
      }
      holder.close()
      await executor.close()
      fs.rmSync(directory, { recursive: true, force: true })
    }
  })

  test("a request deadline is indeterminate and retires the thread", async () => {
    // Deterministic block: a second connection holds the write lock while the
    // executor's INSERT waits on it with a long driver busy timeout, so only the
    // executor deadline can settle the request. No CPU burn is involved, and the
    // generous budget covers a loaded host's scheduling delay for the
    // post-retirement read (a 50ms budget made that follow-up query flaky).
    const { filename, directory } = tempDatabase()
    const holder = new BunDatabase(filename)
    holder.exec("PRAGMA journal_mode = WAL")
    holder.exec("CREATE TABLE item (id INTEGER PRIMARY KEY)")
    holder.exec("BEGIN IMMEDIATE")
    const executor = createDbExecutor({
      filename,
      timeoutMs: 5_000,
      env: { OPENCODE_DB_BUSY_TIMEOUT_MS: "60000" },
    })
    try {
      expect(await executor.query("SELECT 1 AS n")).toEqual([{ n: 1 }])
      const firstThread = executor.threadId
      const outcome = await executor.run("INSERT INTO item (id) VALUES (1)").then(
        () => "resolved",
        (error: unknown) =>
          error instanceof DbExecutorTimeoutError
            ? { kind: "timeout" as const, indeterminate: error.indeterminate }
            : { kind: "other" as const, error },
      )
      expect(outcome).toEqual({ kind: "timeout", indeterminate: true })
      // The timed-out thread is gone; the next request opens a fresh one. A read
      // succeeds under WAL even while the holder still owns the write lock.
      expect(await executor.query("SELECT 2 AS n")).toEqual([{ n: 2 }])
      expect(executor.threadId).toBeGreaterThan(0)
      expect(executor.threadId).not.toBe(firstThread)
    } finally {
      await executor.close()
      try {
        holder.exec("ROLLBACK")
      } catch {
        // The holder may already be released.
      }
      holder.close()
      fs.rmSync(directory, { recursive: true, force: true })
    }
  })

  test("a genuine worker crash is a crash, not a timeout", async () => {
    const executor = createDbExecutor({
      filename: ":memory:",
      timeoutMs: 1_000,
      entry: new URL("../../fixture/db-executor-crash.ts", import.meta.url),
    })
    try {
      const outcome = await executor.query("SELECT 1 AS n").then(
        () => "resolved",
        (error: unknown) => error,
      )
      expect(outcome).toBeInstanceOf(DbExecutorCrashedError)
      expect(outcome).not.toBeInstanceOf(DbExecutorTimeoutError)
    } finally {
      await executor.close()
    }
  })

  test("a failed rollback preserves the original cause and poisons the executor", async () => {
    const executor = createDbExecutor({ filename: ":memory:" })
    try {
      await executor.run("CREATE TABLE item (id INTEGER PRIMARY KEY, name TEXT UNIQUE)")
      const outcome = await executor
        .transaction([
          { sql: "INSERT INTO item (name) VALUES (?)", params: ["dup"] },
          { sql: "INSERT OR ROLLBACK INTO item (name) VALUES (?)", params: ["dup"] },
        ])
        .then(
          () => "resolved",
          (error: unknown) => error,
        )
      expect(outcome).toBeInstanceOf(DbExecutorRollbackError)
      const error = outcome as DbExecutorRollbackError
      expect(String((error.cause as Error).message)).toContain("UNIQUE constraint failed")
      expect(String(error.rollbackCause.message)).toContain("no transaction is active")
      // The transaction state is unknown: the executor refuses further work.
      const next = await executor.query("SELECT 1 AS n").then(
        () => "resolved",
        (failure: unknown) => (failure as Error).name,
      )
      expect(next).toBe("DbExecutorPoisonedError")
    } finally {
      await executor.close()
    }
  })

  test("reports the real isolate thread id, not 0", async () => {
    const executor = createDbExecutor({ filename: ":memory:" })
    try {
      await executor.query("SELECT 1 AS n")
      expect(executor.threadId).toBeGreaterThan(0)
      expect(executor.threadId).not.toBe(mainThreadId)
    } finally {
      await executor.close()
    }
  })

  test("a create:false probe refuses a missing file without creating it", async () => {
    const { filename, directory } = tempDatabase()
    const executor = createDbExecutor({ filename, create: false })
    try {
      await executor.query("SELECT 1 AS n")
      throw new Error("expected the probe to refuse a missing database")
    } catch (error) {
      expect(error).toBeInstanceOf(DbExecutorCrashedError)
    } finally {
      await executor.close()
      expect(fs.existsSync(filename)).toBe(false)
      fs.rmSync(directory, { recursive: true, force: true })
    }
  })
})
