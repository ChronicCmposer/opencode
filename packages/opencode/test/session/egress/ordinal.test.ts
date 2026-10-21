// Phase 8.5 — writer-stamped, generation-scoped global ordinal (plan task 8.5).
//
// PHASE 7 DEPENDENCY: the single-writer Worker is not landed yet. The wire frame
// carries `{generation, ordinal}` as plain numbers. This module fixes the
// interface assumption — the writer stamps a monotonic `ordinal` within a
// `generation`, and ordering is `(generation, ordinal)` lexicographic so a
// writer respawn (generation bump) never compares a new ordinal against an old
// one. A monotonic fallback source is provided for tests until Phase 7 lands.

import { describe, expect, test } from "bun:test"
import { compareOrdinals, createMonotonicOrdinalSource } from "@/session/egress/ordinal"

describe("writer ordinal", () => {
  test("orders by generation, then ordinal", () => {
    expect(compareOrdinals({ generation: 1, ordinal: 2 }, { generation: 1, ordinal: 3 })).toBeLessThan(0)
    expect(compareOrdinals({ generation: 1, ordinal: 3 }, { generation: 1, ordinal: 2 })).toBeGreaterThan(0)
    expect(compareOrdinals({ generation: 1, ordinal: 2 }, { generation: 1, ordinal: 2 })).toBe(0)
    expect(compareOrdinals({ generation: 2, ordinal: 0 }, { generation: 1, ordinal: 99 })).toBeGreaterThan(0)
  })

  test("the fallback source increments monotonically within one generation", () => {
    const source = createMonotonicOrdinalSource({ generation: 5 })
    expect(source.next()).toEqual({ generation: 5, ordinal: 1 })
    expect(source.next()).toEqual({ generation: 5, ordinal: 2 })
    expect(source.next()).toEqual({ generation: 5, ordinal: 3 })
  })

  test("a fresh generation restarts the ordinal and still orders after the old one", () => {
    const first = createMonotonicOrdinalSource({ generation: 1 })
    first.next()
    first.next()
    const second = createMonotonicOrdinalSource({ generation: 2 })
    const last = { generation: 1, ordinal: 2 }
    expect(compareOrdinals(second.next(), last)).toBeGreaterThan(0)
  })
})
