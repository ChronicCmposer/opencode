import { describe, expect, test } from "bun:test"
import { deepFreeze } from "./deep-freeze"

describe("deepFreeze", () => {
  test("freezes nested objects and arrays", () => {
    const value = { a: { b: [{ c: 1 }] } }
    deepFreeze(value)

    expect(Object.isFrozen(value)).toBe(true)
    expect(Object.isFrozen(value.a)).toBe(true)
    expect(Object.isFrozen(value.a.b)).toBe(true)
    expect(Object.isFrozen(value.a.b[0])).toBe(true)
    expect(() => {
      value.a.b[0]!.c = 2
    }).toThrow()
  })

  test("returns the same reference and leaves primitives untouched", () => {
    const value = { n: 1 }
    expect(deepFreeze(value)).toBe(value)
    expect(deepFreeze(1)).toBe(1)
    expect(deepFreeze("x")).toBe("x")
    expect(deepFreeze(null)).toBeNull()
    expect(deepFreeze(undefined)).toBeUndefined()
  })
})
