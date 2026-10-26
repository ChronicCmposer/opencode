import { describe, expect, test } from "bun:test"
import path from "node:path"
import { openConnection } from "@opencode-ai/core/database/connection"
import { tmpdir } from "../../fixture/fixture"

describe("per-connection WAL open", () => {
  test("every connection opens WAL with the worker pragmas and owns its handle", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "wal.db")

    const first = openConnection({ filename, busyTimeoutMs: 40 })
    const second = openConnection({ filename, busyTimeoutMs: 40 })
    try {
      expect(first).not.toBe(second)
      expect(first.owner).not.toBe(second.owner)
      expect(first.journalMode()).toBe("wal")
      expect(second.journalMode()).toBe("wal")
      expect(first.pragma("synchronous")).toBe(1)
      expect(first.pragma("foreign_keys")).toBe(1)
      expect(first.pragma("busy_timeout")).toBe(40)
    } finally {
      first.close()
      second.close()
    }
  })

  test("two file connections see each other's committed writes", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "shared.db")

    const first = openConnection({ filename, busyTimeoutMs: 40 })
    const second = openConnection({ filename, busyTimeoutMs: 40 })
    try {
      first.exec("CREATE TABLE item (id TEXT PRIMARY KEY)")
      first.run("INSERT INTO item (id) VALUES (?)", ["a"])
      second.run("INSERT INTO item (id) VALUES (?)", ["b"])

      expect(first.all<{ id: string }>("SELECT id FROM item ORDER BY id")).toEqual([{ id: "a" }, { id: "b" }])
      expect(second.all<{ id: string }>("SELECT id FROM item ORDER BY id")).toEqual([{ id: "a" }, { id: "b" }])
    } finally {
      first.close()
      second.close()
    }
  })

  test("closing one connection does not close another; use after close fails fast", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "closed.db")

    const first = openConnection({ filename })
    const second = openConnection({ filename })
    try {
      first.exec("CREATE TABLE item (id TEXT PRIMARY KEY)")
      first.close()
      expect(() => first.all("SELECT * FROM item")).toThrow(/closed/i)
      expect(second.all("SELECT * FROM item")).toEqual([])
    } finally {
      first.close()
      second.close()
    }
  })
})
