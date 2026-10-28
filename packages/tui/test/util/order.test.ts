import { describe, expect, test } from "bun:test"
import { compareCodepoint, compareCodepointNumeric, compareIds } from "../../src/util/order"

describe("deterministic ordering", () => {
  test("orders by Unicode code point, not host locale", () => {
    const values = ["b", "A", "a", "B"]
    expect(values.toSorted(compareCodepoint)).toEqual(["A", "B", "a", "b"])
  })

  test("is a total order", () => {
    const values = ["x", "x", "y", "x2", "x10"]
    const sorted = values.toSorted(compareCodepoint)
    for (let index = 1; index < sorted.length; index++) {
      expect(compareCodepoint(sorted[index - 1]!, sorted[index]!)).toBeLessThanOrEqual(0)
    }
    expect(compareCodepoint("same", "same")).toBe(0)
  })
})

describe("compareIds", () => {
  // SQLite `ORDER BY id` uses binary collation; ids are persisted in that order.
  // Hex ids are the common case: digit runs must NOT be compared as numbers.
  test("matches SQLite byte order for real-looking hex ids", () => {
    const values = [
      "prt_019bcf2a1a1fXy7QmNp3Rt",
      "prt_019bcf2a19f1Xy7QmNp3Rt",
      "prt_019bcf2a1000Xy7QmNp3Rt",
      "prt_019bcf2a1a00Xy7QmNp3Rt",
      "prt_019bcf2a19f0Xy7QmNp3Rt",
    ]
    expect(values.toSorted(compareIds)).toEqual([
      "prt_019bcf2a1000Xy7QmNp3Rt",
      "prt_019bcf2a19f0Xy7QmNp3Rt",
      "prt_019bcf2a19f1Xy7QmNp3Rt",
      "prt_019bcf2a1a00Xy7QmNp3Rt",
      "prt_019bcf2a1a1fXy7QmNp3Rt",
    ])
  })

  test("distinguishes ids that differ only by a leading zero", () => {
    expect(compareIds("x2", "x02")).not.toBe(0)
    expect(compareIds("x02", "x2")).toBeLessThan(0)
    expect(["x2", "x02", "x1"].toSorted(compareIds)).toEqual(["x02", "x1", "x2"])
  })

  test("is a total order over a hex id corpus", () => {
    const values = [
      "prt_019bcf2a1a00Xy7QmNp3Rt",
      "prt_019bcf2a1000Xy7QmNp3Rt",
      "prt_019bcf2a19f0Xy7QmNp3Rt",
      "prt_019bcf2a1a00Xy7QmNp3Rt",
      "prt_019bcf2a1a1fXy7QmNp3Rt",
      "prt_019bcf2a19f1Xy7QmNp3Rt",
    ]
    const sorted = values.toSorted(compareIds)
    for (let index = 1; index < sorted.length; index++) {
      expect(compareIds(sorted[index - 1]!, sorted[index]!)).toBeLessThanOrEqual(0)
    }
    expect(compareIds("same", "same")).toBe(0)
  })
})

describe("display label ordering", () => {
  test("compares digit runs numerically", () => {
    const values = ["part10", "part2", "part1", "part02"]
    expect(values.toSorted(compareCodepointNumeric)).toEqual(["part1", "part2", "part02", "part10"])
  })

  test("groups labels that differ only by a leading zero", () => {
    // Label-only semantics: `x2` and `x02` share rank. Never used for ids.
    expect(compareCodepointNumeric("x2", "x02")).toBe(0)
  })
})
