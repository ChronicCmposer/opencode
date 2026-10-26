import { describe, expect, test } from "bun:test"
import { SessionDepth } from "../../src/session/depth"

describe("SessionDepth.spawnDepth", () => {
  test("parses a finite non-negative integer at metadata.depth", () => {
    expect(SessionDepth.spawnDepth({ depth: 0 })).toBe(0)
    expect(SessionDepth.spawnDepth({ depth: 1 })).toBe(1)
    expect(SessionDepth.spawnDepth({ depth: 7 })).toBe(7)
  })

  test("returns 0 when depth is absent", () => {
    expect(SessionDepth.spawnDepth(undefined)).toBe(0)
    expect(SessionDepth.spawnDepth(null)).toBe(0)
    expect(SessionDepth.spawnDepth({})).toBe(0)
    expect(SessionDepth.spawnDepth({ title: "root" })).toBe(0)
  })

  test("returns 0 for non-object metadata", () => {
    expect(SessionDepth.spawnDepth(42)).toBe(0)
    expect(SessionDepth.spawnDepth("depth")).toBe(0)
    expect(SessionDepth.spawnDepth(true)).toBe(0)
    expect(SessionDepth.spawnDepth([])).toBe(0)
  })

  test("returns 0 for invalid depth values", () => {
    expect(SessionDepth.spawnDepth({ depth: "3" })).toBe(0)
    expect(SessionDepth.spawnDepth({ depth: Number.NaN })).toBe(0)
    expect(SessionDepth.spawnDepth({ depth: Number.POSITIVE_INFINITY })).toBe(0)
    expect(SessionDepth.spawnDepth({ depth: Number.NEGATIVE_INFINITY })).toBe(0)
    expect(SessionDepth.spawnDepth({ depth: -1 })).toBe(0)
    expect(SessionDepth.spawnDepth({ depth: 1.5 })).toBe(0)
    expect(SessionDepth.spawnDepth({ depth: null })).toBe(0)
    expect(SessionDepth.spawnDepth({ depth: undefined })).toBe(0)
  })
})

describe("SessionDepth.nextDepth", () => {
  test("increments a valid spawn depth", () => {
    expect(SessionDepth.nextDepth({ depth: 0 })).toBe(1)
    expect(SessionDepth.nextDepth({ depth: 2 })).toBe(3)
  })

  test("treats missing or invalid metadata as a root spawn", () => {
    expect(SessionDepth.nextDepth(undefined)).toBe(1)
    expect(SessionDepth.nextDepth({})).toBe(1)
    expect(SessionDepth.nextDepth({ depth: -2 })).toBe(1)
  })
})

describe("SessionDepth.exceedsSubagentDepth", () => {
  test("allows a child at or below the limit", () => {
    expect(SessionDepth.exceedsSubagentDepth(0, 0)).toBe(false)
    expect(SessionDepth.exceedsSubagentDepth(1, 1)).toBe(false)
    expect(SessionDepth.exceedsSubagentDepth(2, 3)).toBe(false)
  })

  test("rejects a child above the limit", () => {
    expect(SessionDepth.exceedsSubagentDepth(1, 0)).toBe(true)
    expect(SessionDepth.exceedsSubagentDepth(2, 1)).toBe(true)
  })
})

describe("SessionDepth.subagentDepthMessage", () => {
  test("matches the existing task tool text", () => {
    expect(SessionDepth.subagentDepthMessage(1)).toBe(
      `Subagent depth limit reached (1). Increase "subagent_depth" to allow nested subagents.`,
    )
    expect(SessionDepth.subagentDepthMessage(3)).toBe(
      `Subagent depth limit reached (3). Increase "subagent_depth" to allow nested subagents.`,
    )
  })
})
