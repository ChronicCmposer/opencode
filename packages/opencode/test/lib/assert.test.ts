import { describe, expect, test } from "bun:test"
import { assertNonEmptyString, assertNonNegativeInteger, assertPositiveInteger } from "./assert"

describe("assertion predicates", () => {
  test("assertNonEmptyString accepts non-empty strings and rejects the rest", () => {
    expect(() => assertNonEmptyString("ok", "name")).not.toThrow()
    expect(() => assertNonEmptyString("", "name")).toThrow(/name must be a non-empty string/)
    expect(() => assertNonEmptyString(1, "name")).toThrow(/name must be a non-empty string/)
  })

  test("assertPositiveInteger accepts integers > 0 and rejects the rest", () => {
    expect(() => assertPositiveInteger(1, "k")).not.toThrow()
    for (const bad of [0, -1, 1.5, Number.NaN, "1"]) {
      expect(() => assertPositiveInteger(bad, "k")).toThrow(/k must be a positive integer/)
    }
  })

  test("assertNonNegativeInteger accepts integers >= 0 and rejects the rest", () => {
    expect(() => assertNonNegativeInteger(0, "seq")).not.toThrow()
    for (const bad of [-1, 2.5, Number.POSITIVE_INFINITY, undefined]) {
      expect(() => assertNonNegativeInteger(bad, "seq")).toThrow(/seq must be a non-negative integer/)
    }
  })

  test("names the offending value in the failure message", () => {
    expect(() => assertPositiveInteger(-3, "count")).toThrow(/-3/)
  })
})
