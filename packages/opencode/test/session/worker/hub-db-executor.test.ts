import { describe, expect, test } from "bun:test"
import {
  createDbExecutor,
  DbExecutorClosedError,
  DbExecutorRemoteError,
  type DbExecutor,
} from "@/session/worker/db-executor"

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
