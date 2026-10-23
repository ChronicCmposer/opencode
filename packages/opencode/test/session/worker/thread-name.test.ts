// WS2 — session-worker thread-name grammar.
//
// Prefix `oc`, no dash, no zero-padding. Idle sessions show only the slot;
// running sessions append the last 6 chars of the session id. Every result is
// clipped to the kernel's 15-byte `comm` limit by WS1's `threadNameWithinLimit`.

import { describe, expect, test } from "bun:test"
import { sessionThreadName } from "@/session/worker/thread-name"

describe("session.worker.thread-name", () => {
  test("idle session names the slot only", () => {
    expect(sessionThreadName(7)).toBe("ocs7")
  })

  test("does not zero-pad a multi-digit slot", () => {
    expect(sessionThreadName(100)).toBe("ocs100")
  })

  test("running session appends the last 6 chars of the session id", () => {
    expect(sessionThreadName(7, "ses_0123456789ABCDEFabcdef3f9a2c")).toBe("ocs7:3f9a2c")
  })

  test("uses the whole id when it is shorter than 6 chars", () => {
    expect(sessionThreadName(7, "abc")).toBe("ocs7:abc")
  })

  test("clips an over-long composite to 15 chars", () => {
    // "ocs1234567:3f9a2c" is 17 chars; the kernel only keeps the first 15.
    const name = sessionThreadName(1234567, "ses_0123456789ABCDEFabcdef3f9a2c")
    expect(name).toBe("ocs1234567:3f9a")
    expect(name.length).toBe(15)
  })
})
