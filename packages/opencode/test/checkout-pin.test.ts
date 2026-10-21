import { describe, expect, test } from "bun:test"
import { assertCheckoutPin, DECLARED_PIN, readCheckoutRevision, readTagRevision } from "./lib/checkout-pin"

describe("checkout pin (plan 3.1)", () => {
  test("accepts the declared revision", () => {
    expect(() => assertCheckoutPin(DECLARED_PIN.revision)).not.toThrow()
  })

  test("rejects a divergent revision loudly", () => {
    const divergent = "0".repeat(40)
    expect(() => assertCheckoutPin(divergent)).toThrow(/does not match declared pin v1\.19\.3/)
    expect(() => assertCheckoutPin(divergent)).toThrow(divergent)
  })

  test("declared pin revision matches what tag v1.19.3 resolves to", () => {
    expect(readTagRevision(DECLARED_PIN.tag)).toBe(DECLARED_PIN.revision)
  })

  // INTENTIONALLY RED until the checkout is re-pinned onto v1.19.3. The live
  // HEAD is the `v1` branch tip, which the plan deliberately gates on.
  test("checkout HEAD is pinned to tag v1.19.3", () => {
    assertCheckoutPin(readCheckoutRevision())
  })
})
