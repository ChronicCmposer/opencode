// Per-worker WAL connection pragmas.
//
// The writer isolate is gone. The connection helper that replaces it must
// still open with the shared WAL tuning: `journal_size_limit` is applied before
// the first statement, and the journal mode is WAL. The interval checkpoint and
// its idle guard now belong to db-core and are covered
// there, not through the deleted writer core.

import { describe, expect, test } from "bun:test"
import path from "node:path"
import { openWriterConnection } from "@/session/writer/connection"
import { tmpdir } from "./fixture/fixture"

describe("writer connection applies journal_size_limit at open", () => {
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

  test("an omitted limit uses the resolved default", async () => {
    await using tmp = await tmpdir()
    const connection = openWriterConnection(path.join(tmp.path, "default.db"))
    try {
      const [row] = connection.all("PRAGMA journal_size_limit")
      expect(typeof row?.journal_size_limit).toBe("number")
      expect(connection.all("PRAGMA journal_mode")).toEqual([{ journal_mode: "wal" }])
    } finally {
      connection.close()
    }
  })
})
