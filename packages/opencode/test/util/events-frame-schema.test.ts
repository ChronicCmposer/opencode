// One Schema decode owns the events-port payload union.
//
// The envelope (version/port/correlation) is decoded by the frame codec; this
// schema owns the family-specific `{ kind, payload }` body, so the event,
// resync and ack variants are validated in one place instead of by hand. Schema
// rebuilds plain objects, so an own `__proto__` key never reaches the result.

import { describe, expect, test } from "bun:test"
import { decodeEventsFrameBody } from "@/util/events-frame-schema"

const event = { id: "evt_1", type: "message.updated", properties: { sessionID: "ses_1" } }

describe("util.events-frame-schema", () => {
  test("decodes the event variant with optional durable metadata", () => {
    expect(decodeEventsFrameBody({ kind: "event", payload: { event, generation: 1, ordinal: 2 } })).toEqual({
      kind: "event",
      payload: { event, generation: 1, ordinal: 2 },
    })
    expect(decodeEventsFrameBody({ kind: "event", payload: { event } })).toEqual({
      kind: "event",
      payload: { event },
    })
  })

  test("decodes the resync and ack variants", () => {
    expect(decodeEventsFrameBody({ kind: "resync", payload: { pattern: "session.*" } })).toEqual({
      kind: "resync",
      payload: { pattern: "session.*" },
    })
    expect(decodeEventsFrameBody({ kind: "resync" })).toEqual({ kind: "resync" })
    expect(decodeEventsFrameBody({ kind: "ack", payload: { count: 3 } })).toEqual({
      kind: "ack",
      payload: { count: 3 },
    })
    expect(decodeEventsFrameBody({ kind: "ack" })).toEqual({ kind: "ack" })
  })

  test("decodes end, which carries no payload", () => {
    expect(decodeEventsFrameBody({ kind: "end" })).toEqual({ kind: "end" })
    expect(() => decodeEventsFrameBody({ kind: "end", payload: {} })).toThrow()
  })

  test("rejects malformed event payloads", () => {
    expect(() => decodeEventsFrameBody({ kind: "event" })).toThrow()
    expect(() => decodeEventsFrameBody({ kind: "event", payload: { event: { id: 1, type: "x", properties: {} } } })).toThrow()
    expect(() =>
      decodeEventsFrameBody({ kind: "event", payload: { event, generation: 1.5, ordinal: 2 } }),
    ).toThrow()
  })

  test("rejects malformed resync and ack payloads", () => {
    expect(() => decodeEventsFrameBody({ kind: "resync", payload: { pattern: 1 } })).toThrow()
    expect(() => decodeEventsFrameBody({ kind: "ack", payload: { count: -1 } })).toThrow()
    expect(() => decodeEventsFrameBody({ kind: "ack", payload: { count: 1.5 } })).toThrow()
  })

  test("rejects an unknown discriminant", () => {
    expect(() => decodeEventsFrameBody({ kind: "nope" })).toThrow()
    expect(() => decodeEventsFrameBody(null)).toThrow()
  })

  test("rebuilds the body so a raw __proto__ key cannot leak", () => {
    const input = JSON.parse('{"kind":"ack","__proto__":{"polluted":true}}')
    const decoded = decodeEventsFrameBody(input)
    expect(Object.hasOwn(decoded, "__proto__")).toBe(false)
    expect(({} as Record<string, unknown>).polluted).toBeUndefined()
  })
})
