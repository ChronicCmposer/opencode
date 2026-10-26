import { describe, expect, test } from "bun:test"
import { probeNameOf } from "./probe-worker"

describe("probeNameOf", () => {
  test("returns every supported probe name", () => {
    for (const name of ["process-role", "no-name", "boot", "clock", "effect", "fault"] as const) {
      expect(probeNameOf(name)).toBe(name)
    }
  })

  test("treats an absent selector as no probe (module import mode)", () => {
    expect(probeNameOf(undefined)).toBeUndefined()
    expect(probeNameOf(null)).toBeUndefined()
    expect(probeNameOf("")).toBeUndefined()
  })

  test("fails loud on an unknown probe instead of silently idling", () => {
    expect(() => probeNameOf("nope")).toThrow(/unknown probe nope/)
    expect(() => probeNameOf(7)).toThrow(/unknown probe 7/)
  })
})
