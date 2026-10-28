// dbwal C1 — the shared WAL pragma source of truth.
//
// Both the Effect read connection (`database.ts`) and the writer isolate
// (`session/writer/worker.ts`) must apply the same `journal_size_limit`, or a
// checkpoint on one connection can leave the other's WAL high-water uncapped.
// These tests pin the defaults, the environment overrides, and the fact that a
// real connection reports the configured limit back.

import { describe, expect, test } from "bun:test"
import { Database } from "bun:sqlite"
import path from "node:path"
import {
  DEFAULT_BUSY_TIMEOUT_MS,
  DEFAULT_CHECKPOINT_INTERVAL_MS,
  DEFAULT_EXECUTOR_BUSY_TIMEOUT_MS,
  DEFAULT_EXECUTOR_CACHE_SIZE_KIB,
  DEFAULT_JOURNAL_SIZE_LIMIT_BYTES,
  executorPragmas,
  resolveExecutorCacheSizeKib,
  resolveExecutorBusyTimeoutMs,
  resolveWalConfig,
  walPragmas,
} from "@opencode-ai/core/database/pragmas"
import { tmpdir } from "./fixture/tmpdir"

describe("dbwal: WAL pragma source of truth", () => {
  test("defaults cap the WAL at 64 MiB and keep the main connection responsive", () => {
    expect(resolveWalConfig({})).toEqual({
      journalSizeLimitBytes: DEFAULT_JOURNAL_SIZE_LIMIT_BYTES,
      checkpointIntervalMs: DEFAULT_CHECKPOINT_INTERVAL_MS,
      busyTimeoutMs: DEFAULT_BUSY_TIMEOUT_MS,
    })
    expect(DEFAULT_JOURNAL_SIZE_LIMIT_BYTES).toBe(64 * 1024 * 1024)
    expect(DEFAULT_CHECKPOINT_INTERVAL_MS).toBe(5 * 60 * 1000)
    // The main connection drives the hub's event loop. A 5s native busy wait
    // stalls it for the whole timeout, so the resolved default is the short
    // main-thread budget and the adapter retries asynchronously instead.
    expect(DEFAULT_BUSY_TIMEOUT_MS).toBe(250)
  })

  test("environment overrides are parsed and invalid values fall back", () => {
    expect(
      resolveWalConfig({
        OPENCODE_DB_JOURNAL_SIZE_LIMIT: "1048576",
        OPENCODE_DB_CHECKPOINT_INTERVAL_MS: "1500",
        OPENCODE_DB_BUSY_TIMEOUT_MS: "250",
      }),
    ).toEqual({ journalSizeLimitBytes: 1048576, checkpointIntervalMs: 1500, busyTimeoutMs: 250 })

    expect(
      resolveWalConfig({
        OPENCODE_DB_JOURNAL_SIZE_LIMIT: "not-a-number",
        OPENCODE_DB_CHECKPOINT_INTERVAL_MS: "-5",
        OPENCODE_DB_BUSY_TIMEOUT_MS: "-1",
      }),
    ).toEqual({
      journalSizeLimitBytes: DEFAULT_JOURNAL_SIZE_LIMIT_BYTES,
      checkpointIntervalMs: DEFAULT_CHECKPOINT_INTERVAL_MS,
      busyTimeoutMs: DEFAULT_BUSY_TIMEOUT_MS,
    })
  })

  test("-1 disables the journal size cap and 0 disables the periodic checkpoint", () => {
    expect(
      resolveWalConfig({
        OPENCODE_DB_JOURNAL_SIZE_LIMIT: "-1",
        OPENCODE_DB_CHECKPOINT_INTERVAL_MS: "0",
        OPENCODE_DB_BUSY_TIMEOUT_MS: "0",
      }),
    ).toEqual({ journalSizeLimitBytes: -1, checkpointIntervalMs: 0, busyTimeoutMs: 0 })
  })

  test("the resolved busy timeout is applied unless a connection overrides it", () => {
    // `database.ts` passes the whole resolved config; the busy timeout must flow
    // through rather than stay hard-coded.
    expect(walPragmas(resolveWalConfig({ OPENCODE_DB_BUSY_TIMEOUT_MS: "1234" }))).toContain(
      "PRAGMA busy_timeout = 1234",
    )
    // The writer's shorter timeout stays an explicit per-connection override.
    expect(walPragmas({ busyTimeoutMs: 250 })).toContain("PRAGMA busy_timeout = 250")
  })

  test("walPragmas applies the configured journal size limit and busy timeout", () => {
    const pragmas = walPragmas({ journalSizeLimitBytes: 123456, busyTimeoutMs: 250 })

    expect(pragmas).toContain("PRAGMA journal_size_limit = 123456")
    expect(pragmas).toContain("PRAGMA journal_mode = WAL")
    expect(pragmas).toContain("PRAGMA busy_timeout = 250")
  })

  test("a connection opened with walPragmas reports the configured limit", async () => {
    await using tmp = await tmpdir()
    const db = new Database(path.join(tmp.path, "pragma.db"), { create: true })
    try {
      for (const pragma of walPragmas({ journalSizeLimitBytes: 987654 })) db.exec(pragma)

      expect(db.query("PRAGMA journal_size_limit").get()).toEqual({ journal_size_limit: 987654 })
      expect(db.query("PRAGMA journal_mode").get()).toEqual({ journal_mode: "wal" })
    } finally {
      db.close()
    }
  })

  test("the executor keeps a bounded busy budget and its own 16 MiB page cache", () => {
    expect(DEFAULT_EXECUTOR_BUSY_TIMEOUT_MS).toBe(1_000)
    expect(DEFAULT_EXECUTOR_CACHE_SIZE_KIB).toBe(16 * 1_024)
    expect(resolveExecutorBusyTimeoutMs({})).toBe(DEFAULT_EXECUTOR_BUSY_TIMEOUT_MS)
    expect(resolveExecutorCacheSizeKib({})).toBe(DEFAULT_EXECUTOR_CACHE_SIZE_KIB)

    const pragmas = executorPragmas({})
    expect(pragmas).toContain("PRAGMA busy_timeout = 1000")
    expect(pragmas).toContain("PRAGMA cache_size = -16384")
    // The hub keeps its 64 MiB budget; only the executor is reduced.
    expect(walPragmas()).toContain("PRAGMA cache_size = -64000")
  })

  test("executor env overrides still win over the bounded defaults", () => {
    expect(resolveExecutorBusyTimeoutMs({ OPENCODE_DB_BUSY_TIMEOUT_MS: "1234" })).toBe(1_234)
    expect(resolveExecutorCacheSizeKib({ OPENCODE_DB_EXECUTOR_CACHE_SIZE_KIB: "4096" })).toBe(4_096)
    expect(executorPragmas({ OPENCODE_DB_BUSY_TIMEOUT_MS: "1234" })).toContain("PRAGMA busy_timeout = 1234")
  })
})
