import { describe, expect, test } from "bun:test"
import { reconcileInPlace } from "@/util/reconcile"

describe("util.reconcile reconcileInPlace", () => {
  test("copies other's elements into target in place and returns target", () => {
    const target = ["a", "b", "c"]
    const other = ["x", "y"]

    const result = reconcileInPlace(target, other)

    expect(result).toBe(target)
    expect(target).toEqual(["x", "y"])
    expect(other).toEqual(["x", "y"])
  })

  test("is a no-op when other is target", () => {
    const target = ["a", "b", "c"]

    const result = reconcileInPlace(target, target)

    expect(result).toBe(target)
    expect(target).toEqual(["a", "b", "c"])
  })

  test("clears target when other is empty", () => {
    const target = ["a", "b"]

    const result = reconcileInPlace(target, [])

    expect(result).toBe(target)
    expect(target).toEqual([])
  })

  test("works for object arrays", () => {
    const target = [{ id: 1 }, { id: 2 }]
    const other = [{ id: 3 }]

    const result = reconcileInPlace(target, other)

    expect(result).toBe(target)
    expect(target).toEqual([{ id: 3 }])
  })
})
