// W2 — framed worker event channel codec (spec Q6/Q7/Q21).
//
// The egress channel keeps its `[u32 big-endian length][UTF-8 JSON body]` framing,
// but the body is now a shared `WorkerFrameEnvelope` (events port): the EventV2
// event crosses VERBATIM as `{ id, type, properties }` — there is no `data`
// translation. An unknown envelope version or a malformed body fails loud.

import { describe, expect, test } from "bun:test"
import {
  MAX_FRAME_BYTES,
  MalformedFrameError,
  decodeFrame,
  decodeFrames,
  encodeFrame,
  type EventsFrame,
} from "@/session/egress/frame"
import {
  ProtocolError,
  UnsupportedFrameVersionError,
  ackFrame,
  endFrame,
  eventFrame,
  resyncFrame,
} from "@/session/worker/frame"

const event = { id: "evt_1", type: "message.part.delta", properties: { sessionID: "s1", delta: "héllo 🌍" } }

function asEventFrame(frame: EventsFrame): Extract<EventsFrame, { readonly kind: "event" }> {
  if (frame.kind !== "event") throw new Error(`expected an event frame, got ${frame.kind}`)
  return frame
}

describe("egress frame codec (events envelope)", () => {
  test("round-trips an event frame with the verbatim {id,type,properties}", () => {
    const frame = eventFrame({ event })
    const decoded = decodeFrame(encodeFrame(frame))
    expect(decoded).toEqual(frame)
    expect(asEventFrame(decoded).payload.event).toEqual(event)
  })

  test("preserves properties verbatim and never introduces a `data` field", () => {
    const decoded = asEventFrame(decodeFrame(encodeFrame(eventFrame({ event })))).payload.event
    expect(decoded.properties).toEqual({ sessionID: "s1", delta: "héllo 🌍" })
    expect("properties" in decoded).toBe(true)
    expect("data" in decoded).toBe(false)
  })

  test("round-trips the end, resync, and ack control frames", () => {
    for (const frame of [endFrame(), resyncFrame({ pattern: "message.*" }), ackFrame({ count: 3 })]) {
      expect(decodeFrame(encodeFrame(frame))).toEqual(frame)
    }
  })

  test("is byte-exact: decode then re-encode yields identical bytes", () => {
    const frame = eventFrame({ event, generation: 2, ordinal: 7 })
    const bytes = encodeFrame(frame)
    expect(encodeFrame(decodeFrame(bytes))).toEqual(bytes)
  })

  test("prefixes the body with a big-endian u32 byte length", () => {
    const frame = endFrame()
    const bytes = encodeFrame(frame)
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
    expect(view.getUint32(0, false)).toBe(bytes.byteLength - 4)
    expect(JSON.parse(new TextDecoder().decode(bytes.subarray(4)))).toEqual(frame)
  })

  test("decodes concatenated frames in order", () => {
    const first = eventFrame({ event })
    const second = endFrame()
    const joined = new Uint8Array([...encodeFrame(first), ...encodeFrame(second)])
    const { frames, rest } = decodeFrames(joined)
    expect(frames).toEqual([first, second])
    expect(rest.byteLength).toBe(0)
  })

  test("retains a partial trailing frame as rest and resumes when completed", () => {
    const frame = eventFrame({ event })
    const bytes = encodeFrame(frame)
    const partial = bytes.subarray(0, bytes.byteLength - 3)
    const first = decodeFrames(partial)
    expect(first.frames).toEqual([])
    expect(first.rest).toEqual(partial)

    const completed = new Uint8Array([...first.rest, ...bytes.subarray(bytes.byteLength - 3)])
    expect(decodeFrames(completed).frames).toEqual([frame])
  })

  test("a header-only buffer is rest, not a frame", () => {
    const bytes = encodeFrame(endFrame())
    expect(decodeFrames(bytes.subarray(0, 4))).toEqual({ frames: [], rest: bytes.subarray(0, 4) })
  })

  test("the returned rest is a copy, not a view of the input buffer", () => {
    const bytes = encodeFrame(eventFrame({ event }))
    const partial = bytes.slice(0, bytes.byteLength - 3)
    const { rest } = decodeFrames(partial)
    partial.fill(0)
    expect(rest).toEqual(bytes.subarray(0, bytes.byteLength - 3))
  })

  test("fails loud on an unsupported envelope version", () => {
    expect(() => decodeFrame(rawFrame({ v: 2, port: "events", kind: "end" }))).toThrow(UnsupportedFrameVersionError)
  })

  test("fails loud on a frame from another port", () => {
    expect(() => decodeFrame(rawFrame({ v: 1, port: "writer", kind: "end" }))).toThrow(ProtocolError)
  })

  test("fails loud on an event missing properties (no `data` fallback)", () => {
    expect(() =>
      decodeFrame(rawFrame({ v: 1, port: "events", kind: "event", payload: { event: { id: "e", type: "t" } } })),
    ).toThrow(ProtocolError)
  })

  test("fails loud on a body that is not valid JSON", () => {
    expect(() => decodeFrame(rawBody("{not json"))).toThrow(MalformedFrameError)
  })

  test("decodeFrame rejects a buffer carrying more than one frame", () => {
    const joined = new Uint8Array([...encodeFrame(endFrame()), ...encodeFrame(endFrame())])
    expect(() => decodeFrame(joined)).toThrow(MalformedFrameError)
  })

  test("decodeFrame rejects a truncated frame", () => {
    const bytes = encodeFrame(endFrame())
    expect(() => decodeFrame(bytes.subarray(0, bytes.byteLength - 1))).toThrow(MalformedFrameError)
  })

  test("fails loud on a body length past MAX_FRAME_BYTES instead of pinning rest", () => {
    // A corrupt `u32` length must not be treated as an incomplete frame; that
    // would retain the buffer as `rest` forever.
    const bytes = new Uint8Array(4)
    new DataView(bytes.buffer).setUint32(0, MAX_FRAME_BYTES + 1, false)
    expect(() => decodeFrames(bytes)).toThrow(MalformedFrameError)
    expect(() => decodeFrame(bytes)).toThrow(MalformedFrameError)
  })
})

function rawBody(text: string): Uint8Array {
  const body = new TextEncoder().encode(text)
  const bytes = new Uint8Array(4 + body.byteLength)
  new DataView(bytes.buffer).setUint32(0, body.byteLength, false)
  bytes.set(body, 4)
  return bytes
}

function rawFrame(body: unknown): Uint8Array {
  return rawBody(JSON.stringify(body))
}
