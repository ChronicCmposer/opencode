import { describe, expect, test } from "bun:test"
import { resolveAnchorResult, type Anchor } from "./anchor-manifest"

describe("anchor manifest discriminated resolution", () => {
  test("reports a present globalThis anchor as found", async () => {
    const anchor: Anchor = { id: "worker", plan: "test", module: null, symbol: "Worker" }
    const resolution = await resolveAnchorResult(anchor)
    expect(resolution.found).toBe(true)
    if (resolution.found) expect(resolution.value).toBeDefined()
  })

  test("reports a missing symbol as absent", async () => {
    const anchor: Anchor = { id: "ghost", plan: "test", module: null, symbol: "__definitely_missing__" }
    expect(await resolveAnchorResult(anchor)).toEqual({ found: false })
  })
})
