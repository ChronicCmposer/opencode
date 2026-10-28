import { describe, expect, test } from "bun:test"
import { compareCodepoint, compareCodepointNumeric } from "../../src/util/order"

describe("deterministic ordering", () => {
  test("orders by Unicode code point, not host locale", () => {
    const values = ["b", "A", "a", "B"]
    expect(values.toSorted(compareCodepoint)).toEqual(["A", "B", "a", "b"])
  })

  test("orders session ids identically on every host", () => {
    const values = ["ses_z", "ses_a", "ses_m"]
    expect(values.toSorted(compareCodepointNumeric)).toEqual(["ses_a", "ses_m", "ses_z"])
  })

  test("compares digit runs numerically", () => {
    const values = ["part10", "part2", "part1", "part02"]
    expect(values.toSorted(compareCodepointNumeric)).toEqual(["part1", "part2", "part02", "part10"])
  })

  test("is a total order", () => {
    const values = ["x", "x", "y", "x2", "x10"]
    const sorted = values.toSorted(compareCodepointNumeric)
    for (let index = 1; index < sorted.length; index++) {
      expect(compareCodepointNumeric(sorted[index - 1]!, sorted[index]!)).toBeLessThanOrEqual(0)
    }
    expect(compareCodepointNumeric("same", "same")).toBe(0)
  })
})
