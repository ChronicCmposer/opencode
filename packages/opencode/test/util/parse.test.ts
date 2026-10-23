import { describe, expect, test } from "bun:test"
import { isPositiveInt, parsePositiveInt } from "@/util/parse"

describe("util.parse isPositiveInt", () => {
  test("accepts integers >= 1", () => {
    for (const value of [1, 2, 7, 42, 1_000_000]) {
      expect(isPositiveInt(value)).toBe(true)
    }
  })

  test("rejects zero, negatives and non-integers", () => {
    for (const value of [0, -1, -7, 1.5, 0.5, -0.5, NaN, Infinity, -Infinity]) {
      expect(isPositiveInt(value)).toBe(false)
    }
  })
})

describe("util.parse parsePositiveInt", () => {
  test("accepts numeric strings and raw numbers", () => {
    expect(parsePositiveInt("1", "count")).toBe(1)
    expect(parsePositiveInt("42", "count")).toBe(42)
    expect(parsePositiveInt(" 7 ", "count")).toBe(7)
    expect(parsePositiveInt(9, "count")).toBe(9)
  })

  test("rejects zero, negatives and non-integers", () => {
    for (const value of ["0", "-1", "1.5", "0.5", "-0.5"]) {
      expect(() => parsePositiveInt(value, "count")).toThrow()
    }
  })

  test("rejects non-numeric and non-finite values", () => {
    for (const value of ["", "abc", "NaN", "Infinity", "-Infinity"]) {
      expect(() => parsePositiveInt(value, "count")).toThrow()
    }
    expect(() => parsePositiveInt(NaN, "count")).toThrow()
    expect(() => parsePositiveInt(Infinity, "count")).toThrow()
  })

  test("fails loud with the label and the offending value", () => {
    expect(() => parsePositiveInt("0", "OPENCODE_LIMIT")).toThrow(
      "invalid OPENCODE_LIMIT: 0 (expected a positive integer)",
    )
    expect(() => parsePositiveInt("nope", "OPENCODE_LIMIT")).toThrow(
      "invalid OPENCODE_LIMIT: nope (expected a positive integer)",
    )
  })
})
