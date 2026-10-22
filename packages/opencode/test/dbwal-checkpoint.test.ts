// dbwal C1/C2 — writer connection pragmas and the periodic truncating checkpoint.
//
// C1: the writer isolate must open with the shared `journal_size_limit` so the
// WAL high-water is capped after a checkpoint resets it.
// C2: the writer must invoke its existing TRUNCATE checkpoint on a configurable
// interval, but only while idle (no queued/running work, no pinned transaction).

import { describe, expect, spyOn, test } from "bun:test"
import { statSync } from "node:fs"
import path from "node:path"
import { createWriterClient } from "@/session/writer/client"
import { createBunSqliteConnection, openWriterConnection } from "@/session/writer/connection"
import { createWriterCore, type WriterCore } from "@/session/writer/core"
import { flush } from "./lib/async"
import { pollUntil } from "./lib/poll"
import { tmpdir } from "./fixture/fixture"
import { SCHEMA_SQL, healthyEnv } from "./session/writer/fixtures"

const WAL_LIMIT = 64 * 1024

interface FakeScheduler {
  readonly schedule: (task: () => void, intervalMs: number) => () => void
  tick(): void
  intervalMs(): number | undefined
  stopped(): boolean
}

/** A deterministic scheduler seam: the test decides when the interval fires. */
function fakeScheduler(): FakeScheduler {
  let task: (() => void) | undefined
  let interval: number | undefined
  let stopped = false
  return {
    schedule: (nextTask, intervalMs) => {
      task = nextTask
      interval = intervalMs
      stopped = false
      return () => {
        task = undefined
        stopped = true
      }
    },
    tick: () => task?.(),
    intervalMs: () => interval,
    stopped: () => stopped,
  }
}

function walSize(file: string): number {
  try {
    return statSync(`${file}-wal`).size
  } catch {
    return 0
  }
}

function memoryCore(options: { checkpointIntervalMs: number; schedule?: FakeScheduler["schedule"] }): WriterCore {
  const connection = createBunSqliteConnection({ filename: ":memory:" })
  connection.exec(SCHEMA_SQL)
  return createWriterCore({
    connection,
    checkpointIntervalMs: options.checkpointIntervalMs,
    ...(options.schedule === undefined ? {} : { schedule: options.schedule }),
  })
}

describe("dbwal C1: writer connection applies journal_size_limit at open", () => {
  test("the production writer connection reports the configured limit", async () => {
    await using tmp = await tmpdir()
    const connection = openWriterConnection(path.join(tmp.path, "open.db"), {
      journalSizeLimitBytes: 131072,
      checkpointIntervalMs: 0,
    })
    try {
      expect(connection.all("PRAGMA journal_size_limit")).toEqual([{ journal_size_limit: 131072 }])
      expect(connection.all("PRAGMA journal_mode")).toEqual([{ journal_mode: "wal" }])
    } finally {
      connection.close()
    }
  })

  test("the real writer isolate opens with the configured limit", async () => {
    await using tmp = await tmpdir()
    await using client = createWriterClient({
      env: {
        ...healthyEnv,
        OPENCODE_DB_JOURNAL_SIZE_LIMIT: "777777",
        OPENCODE_DB_CHECKPOINT_INTERVAL_MS: "0",
      },
    })

    await client.open(path.join(tmp.path, "isolate.db"))

    expect(await client.query("all", "PRAGMA journal_size_limit")).toEqual([{ journal_size_limit: 777777 }])
  })
})

describe("dbwal C2: truncating checkpoint bounds the WAL", () => {
  test("a checkpoint leaves the WAL at or below the configured limit", async () => {
    await using tmp = await tmpdir()
    const file = path.join(tmp.path, "truncate.db")
    const connection = openWriterConnection(file, {
      journalSizeLimitBytes: WAL_LIMIT,
      checkpointIntervalMs: 0,
    })
    const core = createWriterCore({ connection, checkpointIntervalMs: 0 })
    try {
      connection.exec("CREATE TABLE payload (id INTEGER PRIMARY KEY, body TEXT)")
      const body = "x".repeat(1024)
      for (let index = 0; index < 200; index++) {
        connection.run("INSERT INTO payload (body) VALUES (?)", [body])
      }

      expect(walSize(file)).toBeGreaterThan(WAL_LIMIT)
      await core.checkpoint()
      expect(walSize(file)).toBeLessThanOrEqual(WAL_LIMIT)
    } finally {
      await core.close()
    }
  })
})

describe("dbwal C2: periodic checkpoint scheduling", () => {
  test("the scheduler is armed with the configured interval and invokes checkpoint", async () => {
    const scheduler = fakeScheduler()
    const core = memoryCore({ checkpointIntervalMs: 250, schedule: scheduler.schedule })
    try {
      expect(scheduler.intervalMs()).toBe(250)
      const checkpoint = spyOn(core, "checkpoint")

      scheduler.tick()
      await flush()

      expect(checkpoint).toHaveBeenCalledTimes(1)
    } finally {
      await core.close()
    }
    expect(scheduler.stopped()).toBe(true)
  })

  test("the idle guard skips the checkpoint while a transaction is pinned", async () => {
    const scheduler = fakeScheduler()
    const core = memoryCore({ checkpointIntervalMs: 100, schedule: scheduler.schedule })
    try {
      const checkpoint = spyOn(core, "checkpoint")
      const handle = await core.beginTransaction()

      scheduler.tick()
      await flush()
      expect(checkpoint).not.toHaveBeenCalled()

      await core.rollbackTransaction(handle)
      scheduler.tick()
      await flush()
      expect(checkpoint).toHaveBeenCalledTimes(1)
    } finally {
      await core.close()
    }
  })

  test("the idle guard skips the checkpoint while work is queued", async () => {
    const scheduler = fakeScheduler()
    const core = memoryCore({ checkpointIntervalMs: 100, schedule: scheduler.schedule })
    try {
      const checkpoint = spyOn(core, "checkpoint")
      let release: () => void = () => {}
      const gate = new Promise<void>((resolve) => {
        release = resolve
      })

      const running = core.publish({
        event: { id: "busy", type: "t", aggregateID: "a", data: {} },
        durable: true,
        commit: () => gate,
      })
      const queued = core.query("all", "SELECT 1")

      scheduler.tick()
      await flush()
      expect(checkpoint).not.toHaveBeenCalled()

      release()
      await running
      await queued
      scheduler.tick()
      await flush()
      expect(checkpoint).toHaveBeenCalledTimes(1)
    } finally {
      await core.close()
    }
  })
})

describe("dbwal C2: WAL stays bounded under a write burst", () => {
  test("an idle checkpoint truncates the WAL after a synthetic burst", async () => {
    await using tmp = await tmpdir()
    const file = path.join(tmp.path, "burst.db")
    const scheduler = fakeScheduler()
    const connection = openWriterConnection(file, {
      journalSizeLimitBytes: WAL_LIMIT,
      checkpointIntervalMs: 1000,
    })
    connection.exec(SCHEMA_SQL)
    const core = createWriterCore({
      connection,
      checkpointIntervalMs: 1000,
      schedule: scheduler.schedule,
    })
    try {
      const body = "x".repeat(2048)
      await Promise.all(
        Array.from({ length: 300 }, (_, index) =>
          core.publish({
            event: { id: `burst-${index}`, type: "t", aggregateID: `a-${index % 7}`, data: { body } },
            durable: true,
          }),
        ),
      )

      expect(walSize(file)).toBeGreaterThan(WAL_LIMIT)
      scheduler.tick()
      await pollUntil(() => walSize(file) <= WAL_LIMIT, {
        message: "WAL was not truncated after the idle checkpoint",
      })
      expect(walSize(file)).toBeLessThanOrEqual(WAL_LIMIT)
    } finally {
      await core.close()
    }
  })
})
