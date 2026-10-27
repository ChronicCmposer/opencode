// The events-port body is decoded once by `util/events-frame-schema`.
//
// `parseEventsFrame` keeps the envelope contract (version/port/correlation) but
// delegates the `{ kind, payload }` family to the shared discriminated Schema, so
// event/resync/ack validation lives in one place and the decoded body is a plain
// object rebuilt by the Schema rather than the hand-written parser's shape.

import { describe, expect, test } from "bun:test"
import { ProtocolError, WORKER_FRAME_VERSION, parseEventsFrame } from "@/session/worker/frame"

const event = { id: "evt_1", type: "message.updated", properties: { sessionID: "ses_1" } }

describe("events frame body schema", () => {
  test("rebuilds the event body as a plain object", () => {
    const parsed = parseEventsFrame({
      v: WORKER_FRAME_VERSION,
      port: "events",
      kind: "event",
      payload: { event: { id: "e", type: "t", properties: { a: 1 } }, generation: 1 },
    })
    if (parsed.kind !== "event") throw new Error("expected an event frame")
    expect(Object.getPrototypeOf(parsed.payload)).toBe(Object.prototype)
    expect(Object.getPrototypeOf(parsed.payload.event)).toBe(Object.prototype)
    expect(Object.keys(parsed.payload).sort()).toEqual(["event", "generation"])
    expect(Object.keys(parsed.payload.event).sort()).toEqual(["id", "properties", "type"])
  })

  test("maps a malformed event body to a ProtocolError raised by the schema", () => {
    expect(() =>
      parseEventsFrame({ v: WORKER_FRAME_VERSION, port: "events", kind: "event", payload: {} }),
    ).toThrow(ProtocolError)
    expect(() =>
      parseEventsFrame({ v: WORKER_FRAME_VERSION, port: "events", kind: "event", payload: {} }),
    ).toThrow(/does not match the schema/)
    expect(() =>
      parseEventsFrame({
        v: WORKER_FRAME_VERSION,
        port: "events",
        kind: "event",
        payload: { event: { id: 1, type: "t", properties: {} } },
      }),
    ).toThrow(ProtocolError)
  })

  test("maps malformed resync/ack/end bodies to a ProtocolError", () => {
    expect(() =>
      parseEventsFrame({ v: WORKER_FRAME_VERSION, port: "events", kind: "resync", payload: { pattern: 1 } }),
    ).toThrow(ProtocolError)
    expect(() =>
      parseEventsFrame({ v: WORKER_FRAME_VERSION, port: "events", kind: "ack", payload: { count: -1 } }),
    ).toThrow(ProtocolError)
    expect(() =>
      parseEventsFrame({ v: WORKER_FRAME_VERSION, port: "events", kind: "ack", payload: { count: 1.5 } }),
    ).toThrow(ProtocolError)
    expect(() =>
      parseEventsFrame({ v: WORKER_FRAME_VERSION, port: "events", kind: "end", payload: {} }),
    ).toThrow(ProtocolError)
    expect(() => parseEventsFrame({ v: WORKER_FRAME_VERSION, port: "events", kind: "nope" })).toThrow(ProtocolError)
  })

  test("decodes the valid variants and keeps correlation", () => {
    expect(parseEventsFrame({ v: WORKER_FRAME_VERSION, port: "events", kind: "end", id: 4 })).toEqual({
      v: 1,
      port: "events",
      kind: "end",
      id: 4,
    })
    expect(
      parseEventsFrame({
        v: WORKER_FRAME_VERSION,
        port: "events",
        kind: "event",
        payload: { event, generation: 1, ordinal: 2 },
      }),
    ).toEqual({
      v: 1,
      port: "events",
      kind: "event",
      payload: { event, generation: 1, ordinal: 2 },
    })
    expect(
      parseEventsFrame({ v: WORKER_FRAME_VERSION, port: "events", kind: "resync", payload: { pattern: "session.*" } }),
    ).toEqual({ v: 1, port: "events", kind: "resync", payload: { pattern: "session.*" } })
    expect(parseEventsFrame({ v: WORKER_FRAME_VERSION, port: "events", kind: "ack", payload: { count: 3 } })).toEqual({
      v: 1,
      port: "events",
      kind: "ack",
      payload: { count: 3 },
    })
  })
})
