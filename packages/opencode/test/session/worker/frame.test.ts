// Shared versioned worker-frame envelope (spec decisions Q8/Q21/Q22/Q24).
//
// One envelope shape (`{ v, port, kind, id?, payload? }`) is shared across the
// worker ports; the `events` port owns four variants. The parser is the trust
// boundary: anything malformed or carrying an unknown version fails loud, and an
// events `event` frame carries the EventV2 `{ id, type, properties }` verbatim
// (never translated to a writer-style `data` field).

import { describe, expect, test } from "bun:test"
import {
  ProtocolError,
  WORKER_FRAME_VERSION,
  UnsupportedFrameVersionError,
  ackFrame,
  createFramePrimitives,
  decodeWorkerFrame,
  durableMetaOf,
  encodeWorkerFrame,
  endFrame,
  eventFrame,
  isRecord,
  optionalBoolean,
  optionalString,
  parseEventsFrame,
  parseWorkerFrame,
  requireInteger,
  requireString,
  resyncFrame,
  type EventsFrame,
  type WorkerFrameEnvelope,
} from "@/session/worker/frame"

describe("parseWorkerFrame", () => {
  test("normalises a minimal envelope", () => {
    expect(parseWorkerFrame({ v: 1, port: "writer", kind: "query" })).toEqual({
      v: 1,
      port: "writer",
      kind: "query",
    })
  })

  test("keeps an optional correlation id and opaque payload", () => {
    const payload = { anything: [1, 2, 3] }
    expect(parseWorkerFrame({ v: 1, port: "job", kind: "cancel", id: 7, payload })).toEqual({
      v: 1,
      port: "job",
      kind: "cancel",
      id: 7,
      payload,
    })
  })

  test("accepts every documented port", () => {
    for (const port of ["events", "writer", "broker", "job", "hub-proxy"] as const) {
      expect(parseWorkerFrame({ v: 1, port, kind: "ping" }).port).toBe(port)
    }
  })

  test("rejects an unknown version", () => {
    expect(() => parseWorkerFrame({ v: 2, port: "events", kind: "end" })).toThrow(UnsupportedFrameVersionError)
    expect(() => parseWorkerFrame({ v: 2, port: "events", kind: "end" })).toThrow(ProtocolError)
  })

  test("rejects a missing or unknown port", () => {
    expect(() => parseWorkerFrame({ v: 1, kind: "end" })).toThrow(ProtocolError)
    expect(() => parseWorkerFrame({ v: 1, port: "nope", kind: "end" })).toThrow(ProtocolError)
  })

  test("rejects a missing, empty or non-string kind", () => {
    expect(() => parseWorkerFrame({ v: 1, port: "events" })).toThrow(ProtocolError)
    expect(() => parseWorkerFrame({ v: 1, port: "events", kind: "" })).toThrow(ProtocolError)
    expect(() => parseWorkerFrame({ v: 1, port: "events", kind: 5 })).toThrow(ProtocolError)
  })

  test("rejects a non-object frame and a non-integer id", () => {
    expect(() => parseWorkerFrame(null)).toThrow(ProtocolError)
    expect(() => parseWorkerFrame("frame")).toThrow(ProtocolError)
    expect(() => parseWorkerFrame({ v: 1, port: "events", kind: "end", id: 1.5 })).toThrow(ProtocolError)
    expect(() => parseWorkerFrame({ v: 1, port: "events", kind: "end", id: "1" })).toThrow(ProtocolError)
  })
})

describe("worker frame codec", () => {
  test("round-trips every events variant through encode/decode", () => {
    const frames: EventsFrame[] = [
      eventFrame(
        { event: { id: "evt_1", type: "session.text", properties: { text: "héllo 🌍" } }, generation: 3, ordinal: 42 },
        9,
      ),
      endFrame(),
      resyncFrame({ pattern: "session.*" }, 1),
      ackFrame({ count: 2 }),
    ]
    for (const frame of frames) {
      expect(parseEventsFrame(decodeWorkerFrame(encodeWorkerFrame(frame)))).toEqual(frame)
    }
  })

  test("round-trips a non-events port envelope (job) through encode/decode", () => {
    const frame: WorkerFrameEnvelope = {
      v: WORKER_FRAME_VERSION,
      port: "job",
      kind: "cancel",
      id: 3,
      payload: { jobID: "ses_1" },
    }
    expect(decodeWorkerFrame(encodeWorkerFrame(frame))).toEqual(frame)
  })

  test("decodeWorkerFrame rejects invalid JSON as ProtocolError", () => {
    expect(() => decodeWorkerFrame("{not json")).toThrow(ProtocolError)
  })
})

describe("parseEventsFrame", () => {
  test("parses an event variant and keeps {id,type,properties} verbatim", () => {
    const properties = { partID: "prt_1", delta: "hi" }
    const parsed = parseEventsFrame({
      v: WORKER_FRAME_VERSION,
      port: "events",
      kind: "event",
      payload: { event: { id: "evt_1", type: "message.part.updated", properties } },
    })
    expect(parsed).toEqual({
      v: 1,
      port: "events",
      kind: "event",
      payload: { event: { id: "evt_1", type: "message.part.updated", properties } },
    })
    if (parsed.kind !== "event") throw new Error("expected an event frame")
    expect(parsed.payload.event.properties).toBe(properties)
    expect("data" in parsed.payload.event).toBe(false)
  })

  test("carries writer generation/ordinal as frame metadata, not event fields", () => {
    const frame = eventFrame({ event: { id: "e", type: "t", properties: { v: 1 } }, generation: 2, ordinal: 7 })
    expect(parseEventsFrame(frame)).toEqual(frame)
    if (frame.kind !== "event") throw new Error("expected an event frame")
    expect("generation" in frame.payload.event).toBe(false)
    expect("ordinal" in frame.payload.event).toBe(false)
  })

  test("parses end, resync and ack variants", () => {
    expect(parseEventsFrame(endFrame())).toEqual({ v: 1, port: "events", kind: "end" })
    expect(parseEventsFrame(resyncFrame({ pattern: "p" }))).toEqual({
      v: 1,
      port: "events",
      kind: "resync",
      payload: { pattern: "p" },
    })
    expect(parseEventsFrame(ackFrame({ count: 3 }))).toEqual({
      v: 1,
      port: "events",
      kind: "ack",
      payload: { count: 3 },
    })
  })

  test("correlation id is optional on every variant", () => {
    expect(parseEventsFrame({ v: 1, port: "events", kind: "end", id: 4 })).toEqual({
      v: 1,
      port: "events",
      kind: "end",
      id: 4,
    })
    expect("id" in parseEventsFrame(endFrame())).toBe(false)
  })

  test("rejects a frame from another port", () => {
    expect(() => parseEventsFrame({ v: 1, port: "writer", kind: "event" })).toThrow(ProtocolError)
  })

  test("rejects an unknown events kind", () => {
    expect(() => parseEventsFrame({ v: 1, port: "events", kind: "nope" })).toThrow(ProtocolError)
  })

  test("rejects a malformed event shape at the boundary", () => {
    expect(() => parseEventsFrame({ v: 1, port: "events", kind: "event" })).toThrow(ProtocolError)
    expect(() =>
      parseEventsFrame({ v: 1, port: "events", kind: "event", payload: { event: { type: "t", properties: {} } } }),
    ).toThrow(ProtocolError)
    expect(() =>
      parseEventsFrame({ v: 1, port: "events", kind: "event", payload: { event: { id: "e", type: "t" } } }),
    ).toThrow(ProtocolError)
    expect(() =>
      parseEventsFrame({
        v: 1,
        port: "events",
        kind: "event",
        payload: { event: { id: "e", type: "t", properties: {} }, ordinal: "x" },
      }),
    ).toThrow(ProtocolError)
  })

  test("rejects a payload on an end frame", () => {
    expect(() => parseEventsFrame({ v: 1, port: "events", kind: "end", payload: {} })).toThrow(ProtocolError)
  })
})

describe("durableMetaOf", () => {
  test("returns { generation, ordinal } only when both are present", () => {
    expect(
      durableMetaOf(eventFrame({ event: { id: "e", type: "t", properties: {} }, generation: 2, ordinal: 7 })),
    ).toEqual({ generation: 2, ordinal: 7 })
  })

  test("a non-durable delta (neither field) has no durable meta", () => {
    expect(durableMetaOf(eventFrame({ event: { id: "e", type: "t", properties: {} } }))).toBeUndefined()
  })

  test("a half-stamped frame is not durable", () => {
    expect(
      durableMetaOf(eventFrame({ event: { id: "e", type: "t", properties: {} }, ordinal: 7 })),
    ).toBeUndefined()
    expect(
      durableMetaOf(eventFrame({ event: { id: "e", type: "t", properties: {} }, generation: 2 })),
    ).toBeUndefined()
  })

  test("control frames are never durable", () => {
    expect(durableMetaOf(endFrame())).toBeUndefined()
    expect(durableMetaOf(ackFrame({ count: 1 }))).toBeUndefined()
    expect(durableMetaOf(resyncFrame({ pattern: "p" }))).toBeUndefined()
  })
})

describe("shared primitive parsers", () => {
  test("isRecord excludes null and arrays", () => {
    expect(isRecord({ a: 1 })).toBe(true)
    expect(isRecord(null)).toBe(false)
    expect(isRecord([1, 2])).toBe(false)
    expect(isRecord("x")).toBe(false)
  })

  test("requireString/requireInteger fail loud on the wrong shape", () => {
    expect(requireString({ s: "ok" }, "s")).toBe("ok")
    expect(() => requireString({ s: 1 }, "s")).toThrow(ProtocolError)
    expect(requireInteger({ n: 3 }, "n")).toBe(3)
    expect(() => requireInteger({ n: 3.5 }, "n")).toThrow(ProtocolError)
    expect(() => requireInteger({ n: "3" }, "n")).toThrow(ProtocolError)
  })

  test("optionalString/optionalBoolean keep absence as undefined", () => {
    expect(optionalString({}, "s")).toBeUndefined()
    expect(optionalString({ s: "v" }, "s")).toBe("v")
    expect(() => optionalString({ s: 1 }, "s")).toThrow(ProtocolError)
    expect(optionalBoolean({}, "b")).toBeUndefined()
    expect(optionalBoolean({ b: false }, "b")).toBe(false)
    expect(() => optionalBoolean({ b: "true" }, "b")).toThrow(ProtocolError)
  })

  test("createFramePrimitives binds failures to the caller's own error taxonomy", () => {
    class CustomError extends Error {}
    const fail = (message: string): never => {
      throw new CustomError(message)
    }
    const primitives = createFramePrimitives(fail)
    expect(() => primitives.requireString({ s: 1 }, "s")).toThrow(CustomError)
    expect(() => primitives.requireInteger({ n: 1.5 }, "n")).toThrow(CustomError)
  })
})
