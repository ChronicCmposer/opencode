import { describe, expect, test } from "bun:test"
import { ANCHORS, resolveAnchorResult } from "./lib/anchor-manifest"

describe("anchor manifest (plan 3.2)", () => {
  test("anchors are unique and non-trivial", () => {
    const ids = ANCHORS.map((anchor) => anchor.id)
    expect(new Set(ids).size).toBe(ids.length)
    expect(ANCHORS.length).toBeGreaterThanOrEqual(30)
  })

  for (const anchor of ANCHORS) {
    test(`resolves ${anchor.id}: ${anchor.module ?? "globalThis"}#${anchor.symbol}`, async () => {
      const resolution = await resolveAnchorResult(anchor)
      expect(resolution.found, `${anchor.plan}: missing ${anchor.module ?? "globalThis"}#${anchor.symbol}`).toBe(true)
      if (resolution.found) expect(resolution.value).toBeDefined()
    })
  }
})
