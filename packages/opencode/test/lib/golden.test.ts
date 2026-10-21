import { describe, expect, test } from "bun:test"
import { createCapture } from "./capture"
import { assertGoldenEqual, createGoldenCapture, diffGolden } from "./golden"

describe("golden capture", () => {
  test("records the durable (aggregate_id, seq, type) sequence and the full row", () => {
    const golden = createGoldenCapture()

    const row = golden.durable({
      aggregate_id: "s1",
      seq: 0,
      type: "message.updated",
      data: { role: "assistant" },
      owner_id: "run-1",
    })

    expect(row).toEqual({
      aggregate_id: "s1",
      seq: 0,
      type: "message.updated",
      data: { role: "assistant" },
      owner_id: "run-1",
    })
    expect(golden.rows).toEqual([row])
    expect(golden.sequence()).toEqual([{ aggregate_id: "s1", seq: 0, type: "message.updated" }])
  })

  test("defaults a missing owner_id to null", () => {
    const golden = createGoldenCapture()
    expect(golden.durable({ aggregate_id: "s1", seq: 0, type: "t", data: {} }).owner_id).toBeNull()
  })

  test("sequence() preserves observed record order across interleaved aggregates", () => {
    const golden = createGoldenCapture()
    golden.durable({ aggregate_id: "s1", seq: 0, type: "a", data: {} })
    golden.durable({ aggregate_id: "s2", seq: 0, type: "x", data: {} })
    golden.durable({ aggregate_id: "s1", seq: 1, type: "b", data: {} })

    expect(golden.sequence().map((entry) => `${entry.aggregate_id}:${entry.seq}:${entry.type}`)).toEqual([
      "s1:0:a",
      "s2:0:x",
      "s1:1:b",
    ])
  })

  test("snapshot() is a canonical, frozen projection grouped by aggregate then seq", () => {
    const golden = createGoldenCapture()
    golden.durable({ aggregate_id: "s2", seq: 0, type: "x", data: {} })
    golden.durable({ aggregate_id: "s1", seq: 1, type: "b", data: {} })
    golden.durable({ aggregate_id: "s1", seq: 0, type: "a", data: {} })

    const snapshot = golden.snapshot()

    expect(snapshot.aggregates.map((aggregate) => aggregate.aggregate_id)).toEqual(["s1", "s2"])
    expect(snapshot.aggregates[0]?.rows.map((r) => r.seq)).toEqual([0, 1])
    expect(snapshot.sequence.map((entry) => `${entry.aggregate_id}:${entry.seq}:${entry.type}`)).toEqual([
      "s1:0:a",
      "s1:1:b",
      "s2:0:x",
    ])
    expect(Object.isFrozen(snapshot)).toBe(true)
    expect(Object.isFrozen(snapshot.aggregates[0])).toBe(true)
  })

  test("records onto a shared capture timeline when one is supplied", () => {
    const capture = createCapture({ clock: () => 5 })
    const golden = createGoldenCapture({ capture })

    golden.durable({ aggregate_id: "s1", seq: 0, type: "t", data: {}, owner_id: null })

    expect(capture.dbWrites()).toHaveLength(1)
    expect(capture.records[0]?.channel).toBe("db")
    expect(capture.records[0]?.at).toBe(5)
  })

  test("stores a private frozen copy of the durable data", () => {
    const golden = createGoldenCapture()
    const data = { nested: { n: 1 } }
    golden.durable({ aggregate_id: "s1", seq: 0, type: "t", data })

    data.nested.n = 2

    expect(golden.rows[0]?.data).toEqual({ nested: { n: 1 } })
    expect(Object.isFrozen(golden.rows[0]?.data)).toBe(true)
  })

  test("fails loudly on an invalid durable row", () => {
    const golden = createGoldenCapture()

    expect(() => golden.durable({ aggregate_id: "", seq: 0, type: "t", data: {} })).toThrow(/aggregate_id/)
    expect(() => golden.durable({ aggregate_id: "s1", seq: -1, type: "t", data: {} })).toThrow(/seq/)
    expect(() => golden.durable({ aggregate_id: "s1", seq: 1.5, type: "t", data: {} })).toThrow(/seq/)
    expect(() => golden.durable({ aggregate_id: "s1", seq: 0, type: "", data: {} })).toThrow(/type/)
    expect(() => golden.durable({ aggregate_id: "s1", seq: 0, type: "t", data: undefined })).toThrow(/data/)
  })

  test("validates rows written directly onto a shared capture timeline", () => {
    const capture = createCapture()
    const golden = createGoldenCapture({ capture })
    capture.dbWrite("durable", { aggregate_id: "s1", seq: 0, type: "t", data: {} })

    expect(golden.rows).toHaveLength(1)
    expect(golden.sequence()).toEqual([{ aggregate_id: "s1", seq: 0, type: "t" }])

    capture.dbWrite("durable", { aggregate_id: "s1", seq: -1, type: "t", data: {} })
    expect(() => golden.rows).toThrow(/seq/)
    expect(() => golden.snapshot()).toThrow(/seq/)
  })
})

describe("golden equivalence", () => {
  test("flag-off and flag-on captures are equivalent despite a different record order", () => {
    const off = createGoldenCapture()
    off.durable({ aggregate_id: "s1", seq: 0, type: "message.updated", data: { a: 1 }, owner_id: "run-1" })
    off.durable({ aggregate_id: "s1", seq: 1, type: "part.updated", data: { b: 2 }, owner_id: "run-1" })
    off.durable({ aggregate_id: "s2", seq: 0, type: "message.updated", data: { a: 1 }, owner_id: "run-2" })

    const on = createGoldenCapture()
    on.durable({ aggregate_id: "s2", seq: 0, type: "message.updated", data: { a: 1 }, owner_id: "run-2" })
    on.durable({ aggregate_id: "s1", seq: 1, type: "part.updated", data: { b: 2 }, owner_id: "run-1" })
    on.durable({ aggregate_id: "s1", seq: 0, type: "message.updated", data: { a: 1 }, owner_id: "run-1" })

    expect(diffGolden(off.snapshot(), on.snapshot())).toEqual([])
    expect(() => assertGoldenEqual(off.snapshot(), on.snapshot())).not.toThrow()
  })

  test("reports a data mismatch with the field, expected and actual values", () => {
    const left = createGoldenCapture()
    left.durable({ aggregate_id: "s1", seq: 0, type: "t", data: { a: 1 }, owner_id: "run-1" })
    const right = createGoldenCapture()
    right.durable({ aggregate_id: "s1", seq: 0, type: "t", data: { a: 2 }, owner_id: "run-1" })

    const diffs = diffGolden(left.snapshot(), right.snapshot())

    expect(diffs).toHaveLength(1)
    expect(diffs[0]).toEqual({
      kind: "field-mismatch",
      aggregate_id: "s1",
      seq: 0,
      field: "data",
      expected: { a: 1 },
      actual: { a: 2 },
    })
    expect(() => assertGoldenEqual(left.snapshot(), right.snapshot(), "flag-off vs flag-on")).toThrow(
      /flag-off vs flag-on/,
    )
  })

  test("reports an owner_id mismatch", () => {
    const left = createGoldenCapture()
    left.durable({ aggregate_id: "s1", seq: 0, type: "t", data: {}, owner_id: "run-1" })
    const right = createGoldenCapture()
    right.durable({ aggregate_id: "s1", seq: 0, type: "t", data: {}, owner_id: "run-2" })

    expect(diffGolden(left.snapshot(), right.snapshot())).toEqual([
      { kind: "field-mismatch", aggregate_id: "s1", seq: 0, field: "owner_id", expected: "run-1", actual: "run-2" },
    ])
  })

  test("reports missing rows, extra rows and extra aggregates", () => {
    const expected = createGoldenCapture()
    expected.durable({ aggregate_id: "s1", seq: 0, type: "t", data: {}, owner_id: null })

    const actual = createGoldenCapture()
    actual.durable({ aggregate_id: "s1", seq: 0, type: "t", data: {}, owner_id: null })
    actual.durable({ aggregate_id: "s1", seq: 1, type: "t2", data: {}, owner_id: null })
    actual.durable({ aggregate_id: "s9", seq: 0, type: "t", data: {}, owner_id: null })

    const diffs = diffGolden(expected.snapshot(), actual.snapshot())

    expect(diffs).toContainEqual({ kind: "extra-row", aggregate_id: "s1", seq: 1 })
    expect(diffs).toContainEqual({ kind: "extra-aggregate", aggregate_id: "s9" })
  })

  test("reports a missing aggregate", () => {
    const expected = createGoldenCapture()
    expected.durable({ aggregate_id: "s1", seq: 0, type: "t", data: {}, owner_id: null })

    const actual = createGoldenCapture()

    expect(diffGolden(expected.snapshot(), actual.snapshot())).toEqual([
      { kind: "missing-aggregate", aggregate_id: "s1" },
    ])
  })
})
