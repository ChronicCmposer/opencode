import { describe, expect, test } from "bun:test"
import { readInt } from "../../fixture/stdio-server"

describe("stdio-server readInt (unified empty-string policy)", () => {
  test("uses the fallback for an unset or empty value", () => {
    expect(readInt("X", 7, {})).toBe(7)
    expect(readInt("X", 7, { X: "" })).toBe(7)
  })

  test("parses a non-negative integer, including zero", () => {
    expect(readInt("X", 7, { X: "5" })).toBe(5)
    expect(readInt("X", 7, { X: "0" })).toBe(0)
  })

  test("fails loud on anything else", () => {
    expect(() => readInt("X", 7, { X: "abc" })).toThrow(/non-negative integer/)
    expect(() => readInt("X", 7, { X: "-1" })).toThrow(/non-negative integer/)
    expect(() => readInt("X", 7, { X: "  " })).toThrow(/non-negative integer/)
  })
})
