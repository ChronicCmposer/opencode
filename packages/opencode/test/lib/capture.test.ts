import { describe, expect, test } from "bun:test"
import { createCapture } from "./capture"

describe("capture harness", () => {
  test("records events, DB writes and worker messages on one ordered timeline", () => {
    const capture = createCapture({ clock: () => 1000 })

    capture.event("session.updated", { id: "s1" })
    capture.dbWrite("insert", { table: "event", id: 1 })
    capture.workerMessage("delta", { text: "hi" }, 3)

    expect(capture.records.map((record) => record.channel)).toEqual(["event", "db", "worker"])
    expect(capture.records.map((record) => record.seq)).toEqual([1, 2, 3])
    expect(capture.records.every((record) => record.at === 1000)).toBe(true)

    expect(capture.events().map((record) => record.kind)).toEqual(["session.updated"])
    expect(capture.dbWrites().map((record) => record.kind)).toEqual(["insert"])
    expect(capture.workerMessages().map((record) => record.workerId)).toEqual([3])
  })

  test("snapshot is an isolated frozen clone that ignores later writes", () => {
    const capture = createCapture()
    capture.event("a", { nested: { n: 1 } })

    const snapshot = capture.snapshot()
    capture.event("b")

    expect(snapshot.records).toHaveLength(1)
    expect(Object.isFrozen(snapshot.records)).toBe(true)
    expect(Object.isFrozen(snapshot.records[0])).toBe(true)
    expect(() => {
      ;(snapshot.records[0] as { kind: string }).kind = "mutated"
    }).toThrow()
  })

  test("stores a private copy of captured data", () => {
    const capture = createCapture()
    const payload = { value: 1 }

    capture.event("a", payload)
    payload.value = 2

    expect(capture.records[0]?.data).toEqual({ value: 1 })
  })

  test("rejects an unknown channel loudly", () => {
    const capture = createCapture()

    expect(() => capture.record({ channel: "nope" as never, kind: "x" })).toThrow(/unknown capture channel/)
  })

  test("reset clears the timeline and restarts the sequence", () => {
    const capture = createCapture()
    capture.event("a")

    capture.reset()

    expect(capture.records).toEqual([])
    expect(capture.event("b").seq).toBe(1)
  })
})
