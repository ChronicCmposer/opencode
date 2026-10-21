import { describe, expect, test } from "bun:test"
import { ANCHORS, resolveAnchor } from "./lib/anchor-manifest"

describe("anchor manifest (plan 3.2)", () => {
  test("anchors are unique and non-trivial", () => {
    const ids = ANCHORS.map((anchor) => anchor.id)
    expect(new Set(ids).size).toBe(ids.length)
    expect(ANCHORS.length).toBeGreaterThanOrEqual(30)
  })

  for (const anchor of ANCHORS) {
    test(`resolves ${anchor.id}: ${anchor.module ?? "globalThis"}#${anchor.symbol}`, async () => {
      const value = await resolveAnchor(anchor)
      expect(value, `${anchor.plan}: missing ${anchor.module ?? "globalThis"}#${anchor.symbol}`).toBeDefined()
    })
  }
})
