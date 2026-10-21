// Phase 8.2 — length-prefixed versioned egress frames (plan task 8.2).
//
// Frame wire format: `[u32 big-endian length][UTF-8 JSON body]`. The body is a
// versioned envelope (`v: 1`); an unknown version fails loud. The codec must be
// byte-exact: decoding then re-encoding yields identical bytes, and a frame may
// be split across / joined with other frames in one buffer.

import { describe, expect, test } from "bun:test"
import {
  UnsupportedFrameVersionError,
  decodeFrame,
  decodeFrames,
  encodeFrame,
  type EgressEvent,
  type EgressFrame,
} from "@/session/egress/frame"

const event: EgressEvent = {
  id: "evt_1",
  type: "message.part.updated",
  data: { text: "héllo 🌍" },
}

const durableEvent: EgressEvent = {
  id: "evt_2",
  type: "message.updated",
  data: { role: "assistant" },
  durable: { aggregateID: "ses_1", seq: 7 },
}

describe("egress frame codec", () => {
  test("round-trips a deltas frame with UTF-8 content", () => {
    const frame: EgressFrame = { kind: "deltas", v: 1, events: [event] }
    expect(decodeFrame(encodeFrame(frame))).toEqual(frame)
  })

  test("round-trips a durable frame carrying writer-stamped meta", () => {
    const frame: EgressFrame = {
      kind: "durable",
      v: 1,
      meta: { generation: 3, ordinal: 42 },
      event: durableEvent,
    }
    expect(decodeFrame(encodeFrame(frame))).toEqual(frame)
  })

  test("round-trips an end frame", () => {
    const frame: EgressFrame = { kind: "end", v: 1 }
    expect(decodeFrame(encodeFrame(frame))).toEqual(frame)
  })

  test("is byte-exact: decode then re-encode yields identical bytes", () => {
    const frame: EgressFrame = { kind: "deltas", v: 1, events: [event, durableEvent] }
    const bytes = encodeFrame(frame)
    expect(encodeFrame(decodeFrame(bytes))).toEqual(bytes)
  })

  test("prefixes the body with a big-endian u32 byte length", () => {
    const frame: EgressFrame = { kind: "end", v: 1 }
    const bytes = encodeFrame(frame)
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
    expect(view.getUint32(0, false)).toBe(bytes.byteLength - 4)
    const body = new TextDecoder().decode(bytes.subarray(4))
    expect(JSON.parse(body)).toEqual(frame)
  })

  test("decodes concatenated frames in order", () => {
    const first: EgressFrame = { kind: "deltas", v: 1, events: [event] }
    const second: EgressFrame = { kind: "durable", v: 1, meta: { generation: 1, ordinal: 1 }, event: durableEvent }
    const joined = new Uint8Array([...encodeFrame(first), ...encodeFrame(second)])
    const { frames, rest } = decodeFrames(joined)
    expect(frames).toEqual([first, second])
    expect(rest.byteLength).toBe(0)
  })

  test("retains a partial trailing frame as rest and resumes when completed", () => {
    const frame: EgressFrame = { kind: "deltas", v: 1, events: [event] }
    const bytes = encodeFrame(frame)
    const partial = bytes.subarray(0, bytes.byteLength - 3)
    const first = decodeFrames(partial)
    expect(first.frames).toEqual([])
    expect(first.rest).toEqual(partial)

    const completed = new Uint8Array([...first.rest, ...bytes.subarray(bytes.byteLength - 3)])
    expect(decodeFrames(completed).frames).toEqual([frame])
  })

  test("a header-only buffer is rest, not a frame", () => {
    const frame: EgressFrame = { kind: "end", v: 1 }
    const bytes = encodeFrame(frame)
    expect(decodeFrames(bytes.subarray(0, 4))).toEqual({ frames: [], rest: bytes.subarray(0, 4) })
  })

  test("fails loud on an unsupported version", () => {
    const body = new TextEncoder().encode(JSON.stringify({ kind: "end", v: 2 }))
    const bytes = new Uint8Array(4 + body.byteLength)
    new DataView(bytes.buffer).setUint32(0, body.byteLength, false)
    bytes.set(body, 4)
    expect(() => decodeFrame(bytes)).toThrow(UnsupportedFrameVersionError)
  })
})
