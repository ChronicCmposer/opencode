import { describe, expect, test } from "bun:test"
import { DEFAULT_GIT_TIMEOUT_MS, readRevision, revisionFailureDetail } from "./checkout-pin"

describe("checkout-pin read resilience", () => {
  test("prefers real stderr in the failure detail", () => {
    expect(
      revisionFailureDetail({ exitCode: 128, signalCode: null, stderr: "  fatal: bad ref \n", timeoutMs: 1000 }),
    ).toBe("fatal: bad ref")
  })

  test("describes a timeout (signal) when stderr is empty", () => {
    expect(revisionFailureDetail({ exitCode: null, signalCode: "SIGTERM", stderr: "", timeoutMs: 250 })).toBe(
      "terminated by SIGTERM after 250ms",
    )
  })

  test("falls back to the exit code when stderr is blank", () => {
    expect(revisionFailureDetail({ exitCode: 3, signalCode: null, stderr: "   ", timeoutMs: 1000 })).toBe("exit code 3")
  })

  test("readRevision resolves HEAD when given an explicit timeout", () => {
    const head = readRevision("HEAD", undefined, { timeoutMs: DEFAULT_GIT_TIMEOUT_MS })
    expect(head).toMatch(/^[0-9a-f]{7,40}$/)
  })

  test("readRevision fails loudly on a bogus ref", () => {
    expect(() => readRevision("definitely-not-a-real-ref")).toThrow(
      /git rev-parse --verify definitely-not-a-real-ref failed/,
    )
  })

  test("rejects a non-positive timeout loudly", () => {
    expect(() => readRevision("HEAD", undefined, { timeoutMs: 0 })).toThrow(/positive number/)
  })
})
