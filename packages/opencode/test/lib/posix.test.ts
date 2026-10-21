import { describe, expect, test } from "bun:test"
import { isPosix } from "./loop-gap"
import { isPosix as fromProcessTree, posixSupport } from "./process-tree"

describe("single-source posix detection", () => {
  test("loop-gap re-exports process-tree's isPosix", () => {
    expect(isPosix).toBe(fromProcessTree)
  })

  test("isPosix mirrors posixSupport", () => {
    expect(isPosix("win32")).toBe(false)
    expect(isPosix("linux")).toBe(true)
    expect(isPosix("darwin")).toBe(true)
    expect(isPosix("linux")).toBe(posixSupport("linux").supported)
  })
})
