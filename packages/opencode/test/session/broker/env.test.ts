import { describe, expect, test } from "bun:test"
import { mergeEnv } from "@/session/broker/env"

describe("mergeEnv (shared broker env overlay)", () => {
  test("copies the base, then overrides and deletes via the overlay", () => {
    const base = { KEEP: "1", CHANGE: "old", DROP: "x" }
    expect(mergeEnv(undefined, base)).toEqual(base)
    expect(mergeEnv({ CHANGE: "new", EXTRA: "2" }, base)).toEqual({ KEEP: "1", CHANGE: "new", DROP: "x", EXTRA: "2" })
    expect(mergeEnv({ DROP: undefined }, base)).toEqual({ KEEP: "1", CHANGE: "old" })
  })

  test("drops undefined base values and does not mutate its inputs", () => {
    const base: Record<string, string | undefined> = { A: "1", B: undefined }
    const overlay = { B: "2" }
    expect(mergeEnv(overlay, base)).toEqual({ A: "1", B: "2" })
    expect(base).toEqual({ A: "1", B: undefined })
    expect(overlay).toEqual({ B: "2" })
  })
})
