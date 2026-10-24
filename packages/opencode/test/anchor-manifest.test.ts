import { describe, expect, test } from "bun:test"
import { ANCHORS, resolveAnchor, resolveAnchorResult } from "./lib/anchor-manifest"

describe("anchor manifest (plan 3.2)", () => {
  test("anchors are unique, well-formed and non-trivial", () => {
    const ids = ANCHORS.map((anchor) => anchor.id)
    expect(new Set(ids).size).toBe(ids.length)
    expect(ANCHORS.length).toBeGreaterThanOrEqual(30)
    for (const anchor of ANCHORS) {
      expect(anchor.id.length, "anchor id must be non-empty").toBeGreaterThan(0)
      expect(anchor.plan.length, `${anchor.id}: plan must be non-empty`).toBeGreaterThan(0)
      expect(anchor.symbol.length, `${anchor.id}: symbol must be non-empty`).toBeGreaterThan(0)
    }
  })

  for (const anchor of ANCHORS) {
    test(`resolves ${anchor.id}: ${anchor.module ?? "globalThis"}#${anchor.symbol}`, async () => {
      const label = `${anchor.plan}: ${anchor.module ?? "globalThis"}#${anchor.symbol}`
      const resolution = await resolveAnchorResult(anchor)
      expect(resolution.found, `missing ${label}`).toBe(true)
      if (!resolution.found) return

      const value = resolution.value
      expect(value, `${label} must not be null`).not.toBeNull()
      expect(
        ["function", "object"],
        `${label} must resolve to a function or object export`,
      ).toContain(typeof value)
      // The discriminated result must agree with the raw resolver: `found`
      // exists so an `undefined` export can never be mistaken for a present one.
      expect(await resolveAnchor(anchor), `${label} raw resolver must agree`).toBe(value)
    })
  }
})
