import { describe, expect, test } from "bun:test"
import { compareMultisets, keyAt, multisetDifference, normalizeKey, stableStringify } from "./compare"

describe("stableStringify", () => {
  test("sorts object keys so insertion order never matters", () => {
    expect(stableStringify({ b: 1, a: 2 })).toBe(stableStringify({ a: 2, b: 1 }))
  })

  test("recurses into arrays and nested objects", () => {
    expect(stableStringify({ list: [{ z: 1, a: 2 }] })).toBe('{"list":[{"a":2,"z":1}]}')
  })

  test("keeps primitives and undefined distinguishable", () => {
    expect(stableStringify(undefined)).toBe("undefined")
    expect(stableStringify(null)).toBe("null")
    expect(stableStringify("x")).toBe('"x"')
  })
})

describe("normalizeKey", () => {
  test("defaults to the whole item", () => {
    expect(normalizeKey()({ a: 1, b: 2 })).toBe('{"a":1,"b":2}')
  })

  test("keys by the normalized projection", () => {
    const keyOf = normalizeKey((row: { id: string; time: number; text: string }) => ({ text: row.text }))
    expect(keyOf({ id: "m1", time: 1, text: "hi" })).toBe(keyOf({ id: "m2", time: 99, text: "hi" }))
  })
})

describe("keyAt", () => {
  test("returns the keyed item at an index", () => {
    expect(keyAt(["a", "b"], 1, (item) => item)).toBe("b")
  })

  test("returns undefined past the end", () => {
    expect(keyAt(["a"], 3, (item) => item)).toBeUndefined()
  })
})

describe("multisetDifference", () => {
  test("respects duplicate counts", () => {
    expect(multisetDifference(["a", "a", "b"], ["a"], (item) => item)).toEqual(["a", "b"])
  })

  test("is empty when every key is consumed", () => {
    expect(multisetDifference(["a", "b"], ["b", "a"], (item) => item)).toEqual([])
  })
})

describe("compareMultisets", () => {
  test("equal true for the same rows in a different order", () => {
    const comparison = compareMultisets([{ id: 1 }, { id: 2 }], [{ id: 2 }, { id: 1 }], {
      normalize: (row) => row.id,
    })
    expect(comparison.equal).toBe(true)
    expect(comparison.missingInLeft).toEqual([])
    expect(comparison.missingInRight).toEqual([])
  })

  test("reports exactly-once violations on the correct side", () => {
    const comparison = compareMultisets([{ id: 1 }], [{ id: 1 }, { id: 1 }], { normalize: (row) => row.id })
    expect(comparison.equal).toBe(false)
    expect(comparison.leftCount).toBe(1)
    expect(comparison.rightCount).toBe(2)
    expect(comparison.missingInLeft).toEqual([{ id: 1 }])
    expect(comparison.missingInRight).toEqual([])
  })

  test("an explicit key overrides normalize", () => {
    const comparison = compareMultisets([{ id: 1, time: 1 }], [{ id: 2, time: 2 }], {
      key: () => "same",
    })
    expect(comparison.equal).toBe(true)
  })
})
