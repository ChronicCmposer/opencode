import { describe, expect, test } from "bun:test"
import { assertCheckoutPin, DECLARED_PIN, readCheckoutRevision, readTagRevision } from "./lib/checkout-pin"

// The pure `assertCheckoutPin` tests always run. The live git reads below inspect
// the ambient checkout, which is expected to drift across branches and worktrees,
// so they only run when explicitly opted in with OPENCODE_CHECKOUT_PIN_CHECK=1.
const checkLiveCheckout = process.env.OPENCODE_CHECKOUT_PIN_CHECK === "1"

describe("checkout pin", () => {
  test("accepts the declared revision", () => {
    expect(() => assertCheckoutPin(DECLARED_PIN.revision)).not.toThrow()
  })

  test("rejects a divergent revision loudly", () => {
    const divergent = "0".repeat(40)
    expect(() => assertCheckoutPin(divergent)).toThrow(/does not match declared pin v1\.19\.3/)
    expect(() => assertCheckoutPin(divergent)).toThrow(divergent)
  })

  test.skipIf(!checkLiveCheckout)("declared pin revision matches what tag v1.19.3 resolves to", () => {
    expect(readTagRevision(DECLARED_PIN.tag)).toBe(DECLARED_PIN.revision)
  })

  test.skipIf(!checkLiveCheckout)("checkout HEAD is pinned to tag v1.19.3", () => {
    assertCheckoutPin(readCheckoutRevision())
  })
})
