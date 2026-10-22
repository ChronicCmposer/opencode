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
  DEFAULT_CHECKPOINT_INTERVAL_MS,
  DEFAULT_JOURNAL_SIZE_LIMIT_BYTES,
  resolveWalConfig,
  walPragmas,
} from "@opencode-ai/core/database/pragmas"
import { tmpdir } from "./fixture/tmpdir"

describe("dbwal: WAL pragma source of truth", () => {
  test("defaults cap the WAL at 64 MiB and checkpoint every 5 minutes", () => {
    expect(resolveWalConfig({})).toEqual({
      journalSizeLimitBytes: DEFAULT_JOURNAL_SIZE_LIMIT_BYTES,
      checkpointIntervalMs: DEFAULT_CHECKPOINT_INTERVAL_MS,
    })
    expect(DEFAULT_JOURNAL_SIZE_LIMIT_BYTES).toBe(64 * 1024 * 1024)
    expect(DEFAULT_CHECKPOINT_INTERVAL_MS).toBe(5 * 60 * 1000)
  })

  test("environment overrides are parsed and invalid values fall back", () => {
    expect(
      resolveWalConfig({
        OPENCODE_DB_JOURNAL_SIZE_LIMIT: "1048576",
        OPENCODE_DB_CHECKPOINT_INTERVAL_MS: "1500",
      }),
    ).toEqual({ journalSizeLimitBytes: 1048576, checkpointIntervalMs: 1500 })

    expect(
      resolveWalConfig({
        OPENCODE_DB_JOURNAL_SIZE_LIMIT: "not-a-number",
        OPENCODE_DB_CHECKPOINT_INTERVAL_MS: "-5",
      }),
    ).toEqual({
      journalSizeLimitBytes: DEFAULT_JOURNAL_SIZE_LIMIT_BYTES,
      checkpointIntervalMs: DEFAULT_CHECKPOINT_INTERVAL_MS,
    })
  })

  test("-1 disables the journal size cap and 0 disables the periodic checkpoint", () => {
    expect(
      resolveWalConfig({
        OPENCODE_DB_JOURNAL_SIZE_LIMIT: "-1",
        OPENCODE_DB_CHECKPOINT_INTERVAL_MS: "0",
      }),
    ).toEqual({ journalSizeLimitBytes: -1, checkpointIntervalMs: 0 })
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
})
