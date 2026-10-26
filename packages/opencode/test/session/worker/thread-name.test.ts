// Session-worker thread-name grammar (slice N).
//
// A session worker advertises its bound session through its OS thread name:
//
//   idle    `ocs:<last 6 of sessionID>`
//   running `ocs:<last 6 of sessionID>!`
//
// The trailing bang marks a live run, so `ps -T` shows at a glance which worker
// is busy. Short ids use the whole id; every result is clipped to the kernel's
// 15-byte `comm` limit by `threadNameWithinLimit`.

import { describe, expect, test } from "bun:test"
import { sessionThreadName } from "@/session/worker/thread-name"
import { threadNameWithinLimit } from "@/util/thread-name"

const SESSION_ID = "ses_0123456789ABCDEFabcdef3f9a2c"

describe("session.worker.thread-name", () => {
  test("idle session names the last 6 chars of the session id", () => {
    expect(sessionThreadName(SESSION_ID)).toBe("ocs:3f9a2c")
  })

  test("running session appends the live-run marker", () => {
    expect(sessionThreadName(SESSION_ID, { running: true })).toBe("ocs:3f9a2c!")
  })

  test("running: false is the idle form", () => {
    expect(sessionThreadName(SESSION_ID, { running: false })).toBe("ocs:3f9a2c")
  })

  test("uses exactly the last 6 chars of a long id", () => {
    const name = sessionThreadName("abcdefghijklmnop")
    expect(name).toBe("ocs:klmnop")
    expect(name).not.toContain("abcdef")
  })

  test("uses the whole id when it is shorter than 6 chars", () => {
    expect(sessionThreadName("abc")).toBe("ocs:abc")
    expect(sessionThreadName("abc", { running: true })).toBe("ocs:abc!")
  })

  test("stays inside the 15-byte comm limit", () => {
    const name = sessionThreadName("x".repeat(64), { running: true })
    expect(name).toBe("ocs:xxxxxx!")
    expect(name).toBe(threadNameWithinLimit(name))
    expect(Buffer.byteLength(name, "utf8")).toBeLessThanOrEqual(15)
  })

  test("an empty session id never throws (empty fingerprint)", () => {
    expect(() => sessionThreadName("")).not.toThrow()
    expect(sessionThreadName("")).toBe("ocs:")
  })

  test("clips on UTF-8 bytes without splitting a surrogate pair", () => {
    const name = sessionThreadName("x".repeat(40) + "😀😀😀😀😀😀", { running: true })
    // Prefix + fingerprint + bang cannot push past 15 bytes, and the clip must
    // never leave a lone surrogate (which would be written as U+FFFD).
    expect(Buffer.byteLength(name, "utf8")).toBeLessThanOrEqual(15)
    expect(name).not.toContain("\uFFFD")
    expect(name.isWellFormed()).toBe(true)
  })

  test("threadNameWithinLimit keeps whole code points under 15 bytes", () => {
    const clipped = threadNameWithinLimit("ocs:" + "😀".repeat(10))
    expect(clipped).toBe("ocs:😀😀")
    expect(Buffer.byteLength(clipped, "utf8")).toBeLessThanOrEqual(15)
    expect(clipped.isWellFormed()).toBe(true)
  })
})
