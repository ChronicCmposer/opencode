import { describe, expect, test } from "bun:test"
import { defined } from "@/util/object"

describe("util.object defined", () => {
  test("strips undefined-valued keys", () => {
    expect(defined({ a: 1, b: undefined, c: "x", d: undefined })).toEqual({ a: 1, c: "x" })
  })

  test("keeps falsy values that are not undefined", () => {
    const input = { zero: 0, empty: "", no: false, nil: null, nan: NaN }
    const result = defined(input)
    expect(Object.keys(result)).toEqual(["zero", "empty", "no", "nil", "nan"])
    expect(result.zero).toBe(0)
    expect(result.empty).toBe("")
    expect(result.no).toBe(false)
    expect(result.nil).toBe(null)
    expect(Number.isNaN(result.nan)).toBe(true)
  })

  test("returns an empty object when every key is undefined", () => {
    expect(defined({ a: undefined, b: undefined })).toEqual({})
  })

  test("is pure: never mutates the input and returns a new object", () => {
    const input = { a: 1, b: undefined }
    const result = defined(input)
    expect(result).not.toBe(input)
    expect(input).toEqual({ a: 1, b: undefined })
    expect("b" in input).toBe(true)
  })
})
