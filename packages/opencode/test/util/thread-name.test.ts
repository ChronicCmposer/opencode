// WS1 — `threadNameWithinLimit` clips names to the kernel's 15-byte `comm` limit.
//
// `setThreadName` is intentionally NOT exercised here: calling it would rename the
// running bun test thread. The write path is covered by WS4's integration test
// (`test/session/worker/thread-naming-static.test.ts`).

import { describe, expect, test } from "bun:test"
import { threadNameWithinLimit } from "@/util/thread-name"

describe("util.threadNameWithinLimit", () => {
  test("returns short names unchanged", () => {
    expect(threadNameWithinLimit("short")).toBe("short")
  })

  test("clips a 20-char name to exactly 15", () => {
    const clipped = threadNameWithinLimit("abcdefghijklmnopqrst")
    expect(clipped).toBe("abcdefghijklmno")
    expect(clipped.length).toBe(15)
  })

  test("leaves an empty name empty", () => {
    expect(threadNameWithinLimit("")).toBe("")
  })

  test("keeps a name at exactly the limit", () => {
    const name = "abcdefghijklmno"
    expect(name.length).toBe(15)
    expect(threadNameWithinLimit(name)).toBe(name)
  })
})
