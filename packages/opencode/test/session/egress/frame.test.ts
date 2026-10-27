// W2 — worker event channel codec over the SHARED JSON transport (D15).
//
// The egress channel no longer carries a binary `[u32 length][body]` frame. Each
// port message IS one shared `WorkerFrameEnvelope` JSON string, encoded and
// decoded by `session/worker/frame.ts`, so the worker events port, the writer
// port, and the hub-proxy port share one codec and one 4 MiB bound.
//
// The EventV2 event crosses VERBATIM as `{ id, type, properties }` — there is no
// `data` translation. An unknown envelope version, a malformed body, or a
// frame/batch above 4 MiB fails loud with a typed error; there is no numeric
// length header to overflow.

import { describe, expect, test } from "bun:test"
import {
  FrameTooLargeError,
  MAX_BATCH_BYTES,
  MAX_FRAME_BYTES,
  decodeFrame,
  decodeFrames,
  encodeFrame,
  encodeFrames,
  type EventsFrame,
} from "@/session/egress/frame"
import {
  MAX_WORKER_FRAME_BYTES,
  ProtocolError,
  UnsupportedFrameVersionError,
  ackFrame,
  endFrame,
  encodeWorkerFrame,
  eventFrame,
  resyncFrame,
  workerFrameByteLength,
} from "@/session/worker/frame"

const event = { id: "evt_1", type: "message.part.delta", properties: { sessionID: "s1", delta: "héllo 🌍" } }

function asEventFrame(frame: EventsFrame): Extract<EventsFrame, { readonly kind: "event" }> {
  if (frame.kind !== "event") throw new Error(`expected an event frame, got ${frame.kind}`)
  return frame
}

/** Valid JSON frame text padded to exactly `bytes` UTF-8 bytes (ASCII padding). */
function frameTextOfByteLength(bytes: number): string {
  const prefix = '{"v":1,"port":"events","kind":"event","payload":{"event":{"id":"e","type":"t","properties":{"pad":"'
  const suffix = '"}}}}'
  return prefix + "x".repeat(bytes - prefix.length - suffix.length) + suffix
}

describe("egress frame codec (shared JSON transport)", () => {
  test("the shared 4 MiB bound is the single egress bound", () => {
    expect(MAX_FRAME_BYTES).toBe(MAX_WORKER_FRAME_BYTES)
    expect(MAX_BATCH_BYTES).toBe(MAX_WORKER_FRAME_BYTES)
    expect(MAX_FRAME_BYTES).toBe(4 * 1024 * 1024)
  })

  test("encodes one frame as a JSON string, not a binary length-prefixed buffer", () => {
    const frame = eventFrame({ event })
    const text = encodeFrame(frame)
    expect(typeof text).toBe("string")
    expect(text.startsWith("{")).toBe(true)
    expect(JSON.parse(text)).toEqual(frame)
  })

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

  test("decode then re-encode yields identical text", () => {
    const frame = eventFrame({ event, aggregateID: "agg", seq: 7 } as Parameters<typeof eventFrame>[0])
    const text = encodeFrame(frame)
    expect(encodeFrame(decodeFrame(text))).toBe(text)
  })

  test("encodes a batch in order as one JSON string per frame", () => {
    const frames = [eventFrame({ event }), endFrame()]
    const encoded = encodeFrames(frames)
    expect(encoded).toHaveLength(2)
    expect(encoded.every((text) => typeof text === "string")).toBe(true)
    expect(decodeFrames(encoded)).toEqual(frames)
  })

  test("accepts a frame exactly at the 4 MiB bound", () => {
    const text = frameTextOfByteLength(MAX_FRAME_BYTES)
    expect(workerFrameByteLength(text)).toBe(MAX_FRAME_BYTES)
    expect(decodeFrame(text).kind).toBe("event")
  })

  test("rejects a per-frame payload over 4 MiB with a typed rejection", () => {
    const text = frameTextOfByteLength(MAX_FRAME_BYTES + 1)
    expect(() => decodeFrame(text)).toThrow(FrameTooLargeError)
  })

  test("rejects an over-bound frame at encode time, before it is posted", () => {
    const huge = eventFrame({ event: { id: "big", type: "t", properties: { pad: "x".repeat(MAX_FRAME_BYTES) } } })
    let caught: unknown
    try {
      encodeFrame(huge)
    } catch (error) {
      caught = error
    }
    expect(caught).toBeInstanceOf(FrameTooLargeError)
  })

  test("rejects a batch whose total wire size exceeds 4 MiB with a typed rejection", () => {
    // Two individually valid frames whose combined UTF-8 size is over the bound.
    const first = frameTextOfByteLength(MAX_FRAME_BYTES - 8)
    let caught: unknown
    try {
      encodeFrames([decodeFrame(first), endFrame()])
    } catch (error) {
      caught = error
    }
    expect(caught).toBeInstanceOf(FrameTooLargeError)
  })

  test("fails loud on an unsupported envelope version", () => {
    expect(() => decodeFrame(JSON.stringify({ v: 2, port: "events", kind: "end" }))).toThrow(
      UnsupportedFrameVersionError,
    )
  })

  test("fails loud on a frame from another port", () => {
    expect(() => decodeFrame(JSON.stringify({ v: 1, port: "writer", kind: "end" }))).toThrow(ProtocolError)
  })

  test("fails loud on an event missing properties (no `data` fallback)", () => {
    expect(() =>
      decodeFrame(JSON.stringify({ v: 1, port: "events", kind: "event", payload: { event: { id: "e", type: "t" } } })),
    ).toThrow(ProtocolError)
  })

  test("fails loud on a body that is not valid JSON", () => {
    expect(() => decodeFrame("{not json")).toThrow(ProtocolError)
  })

  test("fails loud on a non-string message", () => {
    expect(() => decodeFrame(new Uint8Array([1, 2, 3]) as unknown as string)).toThrow(ProtocolError)
  })

  test("rejects a producer-side duplicate frame text is not silently merged", () => {
    // Each message is exactly one frame: decoding a concatenation must fail
    // instead of splitting it on a numeric header.
    const joined = encodeFrame(endFrame()) + encodeFrame(endFrame())
    expect(() => decodeFrame(joined)).toThrow(ProtocolError)
  })

  test("encodeWorkerFrame is the single serialiser (byte-exact parity)", () => {
    const frame = eventFrame({ event })
    expect(encodeFrame(frame)).toBe(encodeWorkerFrame(frame))
  })
})
