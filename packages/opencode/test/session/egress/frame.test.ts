// Worker event channel codec over the SHARED JSON transport (D15).
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
  encodeFrame,
  encodeFrameChunks,
  encodeFrames,
  type EventsFrame,
} from "@/session/egress/frame"
import {
  MAX_WORKER_FRAME_BYTES,
  ProtocolError,
  UnsupportedFrameVersionError,
  ackFrame,
  endFrame,
  eventFrame,
  resyncFrame,
} from "@/session/worker/frame"
import { readByteFrame } from "./byte-frames"

const event = { id: "evt_1", type: "message.part.delta", properties: { sessionID: "s1", delta: "héllo 🌍" } }

function asEventFrame(frame: EventsFrame): Extract<EventsFrame, { readonly kind: "event" }> {
  if (frame.kind !== "event") throw new Error(`expected an event frame, got ${frame.kind}`)
  return frame
}

describe("egress frame codec (shared JSON transport)", () => {
  test("the shared 4 MiB bound is the single egress bound", () => {
    expect(MAX_FRAME_BYTES).toBe(MAX_WORKER_FRAME_BYTES)
    expect(MAX_BATCH_BYTES).toBe(MAX_WORKER_FRAME_BYTES)
    expect(MAX_FRAME_BYTES).toBe(4 * 1024 * 1024)
  })

  test("encodes one frame as a byte frame: header, envelope, properties segment", () => {
    const frame = eventFrame({ event })
    const buffer = encodeFrame(frame)
    expect(buffer).toBeInstanceOf(ArrayBuffer)
    const envelopeLength = new DataView(buffer).getUint32(0, true)
    const envelope = JSON.parse(new TextDecoder().decode(new Uint8Array(buffer, 4, envelopeLength)))
    expect(envelope.payload.event).toEqual({ id: event.id, type: event.type })
    expect(readByteFrame(buffer)).toEqual(frame)
  })

  test("round-trips an event frame with the verbatim {id,type,properties}", () => {
    const frame = eventFrame({ event })
    const decoded = readByteFrame(encodeFrame(frame))
    expect(decoded).toEqual(frame)
    expect(asEventFrame(decoded).payload.event).toEqual(event)
  })

  test("preserves properties verbatim and never introduces a `data` field", () => {
    const decoded = asEventFrame(readByteFrame(encodeFrame(eventFrame({ event })))).payload.event
    expect(decoded.properties).toEqual({ sessionID: "s1", delta: "héllo 🌍" })
    expect("properties" in decoded).toBe(true)
    expect("data" in decoded).toBe(false)
  })

  test("round-trips the end, resync, and ack control frames", () => {
    for (const frame of [endFrame(), resyncFrame({ pattern: "message.*" }), ackFrame({ count: 3 })]) {
      expect(readByteFrame(encodeFrame(frame))).toEqual(frame)
    }
  })

  test("encodes a batch in order as one byte frame per frame", () => {
    const frames = [eventFrame({ event }), endFrame()]
    const encoded = encodeFrames(frames)
    expect(encoded).toHaveLength(2)
    expect(encoded.every((buffer) => buffer instanceof ArrayBuffer)).toBe(true)
    expect(encoded.map(readByteFrame)).toEqual(frames)
  })

  test("accepts a frame exactly at the 4 MiB bound", () => {
    const base = encodeFrame(eventFrame({ event: { id: "e", type: "t", properties: { pad: "" } } }))
    const pad = "x".repeat(MAX_FRAME_BYTES - base.byteLength)
    const buffer = encodeFrame(eventFrame({ event: { id: "e", type: "t", properties: { pad } } }))
    expect(buffer.byteLength).toBe(MAX_FRAME_BYTES)
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

  test("encodeFrameChunks splits a batch that would exceed the 4 MiB batch cap", () => {
    const big = (id: string) =>
      eventFrame({ event: { id, type: "t", properties: { pad: "x".repeat(2.5 * 1024 * 1024) } } })
    const chunks = encodeFrameChunks([big("a"), big("b"), endFrame()])

    // Nothing is dropped: the batch is split, and every chunk stays within the
    // one bound the transport enforces before posting.
    expect(chunks.flat()).toHaveLength(3)
    expect(chunks.length).toBeGreaterThan(1)
    for (const chunk of chunks) {
      const bytes = chunk.reduce((sum, buffer) => sum + buffer.byteLength, 0)
      expect(bytes).toBeLessThanOrEqual(MAX_BATCH_BYTES)
    }
    expect(chunks.flat().map((buffer) => readByteFrame(buffer).kind)).toEqual(["event", "event", "end"])
  })

  test("encodeFrameChunks still rejects one frame that alone exceeds the cap", () => {
    const huge = eventFrame({ event: { id: "big", type: "t", properties: { pad: "x".repeat(MAX_FRAME_BYTES) } } })
    expect(() => encodeFrameChunks([huge])).toThrow(FrameTooLargeError)
  })

  test("encodeFrameChunks drops an oversized advisory frame through onDrop and keeps the rest", () => {
    const huge = eventFrame({ event: { id: "big", type: "t", properties: { pad: "x".repeat(MAX_FRAME_BYTES) } } })
    const dropped: EventsFrame[] = []
    const chunks = encodeFrameChunks([huge, endFrame()], { onDrop: (frame) => void dropped.push(frame) })
    expect(dropped).toEqual([huge])
    expect(chunks.flat().map((buffer) => readByteFrame(buffer).kind)).toEqual(["end"])
  })

  test("encodeFrameChunks substitutes a position reference for an oversized durable event", () => {
    const frame = eventFrame({
      event: { id: "big", type: "t", properties: { pad: "x".repeat(MAX_FRAME_BYTES) } },
      aggregateID: "agg",
      seq: 4,
    })
    const [buffer] = encodeFrameChunks([frame]).flat()
    const decoded = asEventFrame(readByteFrame(buffer))
    expect(decoded.payload.event.properties).toMatchObject({ __eventBlobPosition: { aggregateID: "agg", seq: 4 } })
    expect(buffer!.byteLength).toBeLessThan(MAX_FRAME_BYTES)
  })

  test("rejects a batch whose total wire size exceeds 4 MiB with a typed rejection", () => {
    const pad = "x".repeat(MAX_FRAME_BYTES - 1024)
    const first = eventFrame({ event: { id: "e", type: "t", properties: { pad } } })
    expect(() => encodeFrames([first, first])).toThrow(FrameTooLargeError)
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
})
