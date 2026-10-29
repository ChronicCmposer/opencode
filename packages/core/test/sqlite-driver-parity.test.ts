import { describe, expect, test } from "bun:test"
import { sqliteRuntimeMismatch } from "../src/database/connection"

describe("sqlite driver parity", () => {
  const runtime = { version: "3.46.0", compileOptions: ["ENABLE_FTS5", "THREADSAFE=1"] }

  test("identical builds match regardless of option order", () => {
    expect(sqliteRuntimeMismatch(runtime, { ...runtime, compileOptions: ["THREADSAFE=1", "ENABLE_FTS5"] })).toBeUndefined()
  })

  test("a version skew is reported", () => {
    expect(sqliteRuntimeMismatch(runtime, { ...runtime, version: "3.45.1" })).toContain("3.46.0")
  })

  test("a compile option skew names the differing options", () => {
    const detail = sqliteRuntimeMismatch(runtime, { ...runtime, compileOptions: ["THREADSAFE=1", "ENABLE_RTREE"] })
    expect(detail).toContain("ENABLE_FTS5")
    expect(detail).toContain("ENABLE_RTREE")
  })
})
