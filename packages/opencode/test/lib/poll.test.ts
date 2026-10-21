import { describe, expect, test } from "bun:test"
import { pollUntil } from "./poll"

describe("pollUntil", () => {
  test("resolves once the predicate is true", async () => {
    let ready = false
    setTimeout(() => {
      ready = true
    }, 5)

    await pollUntil(() => ready, { timeoutMs: 500 })
    expect(ready).toBe(true)
  })

  test("awaits an async predicate", async () => {
    let calls = 0
    await pollUntil(async () => ++calls >= 3, { timeoutMs: 500, intervalMs: 1 })
    expect(calls).toBe(3)
  })

  test("fails loudly with a custom message on timeout", async () => {
    const started = Date.now()
    await expect(pollUntil(() => false, { timeoutMs: 20, intervalMs: 5, message: "never ready" })).rejects.toThrow(
      "never ready",
    )
    expect(Date.now() - started).toBeGreaterThanOrEqual(20)
  })

  test("uses a descriptive default message", async () => {
    await expect(pollUntil(() => false, { timeoutMs: 5, intervalMs: 1 })).rejects.toThrow(
      /pollUntil timed out after 5ms/,
    )
  })
})
