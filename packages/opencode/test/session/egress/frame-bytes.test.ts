import { describe, expect, test } from "bun:test"
import { EVENTS_FRAME_HEADER_BYTES, encodeEventsFrameBytes } from "@/session/egress/frame-bytes"
import { jsDecoder } from "@/session/egress/scan"
import { ackFrame, endFrame, eventFrame, FrameTooLargeError, MAX_WORKER_FRAME_BYTES } from "@/session/worker/frame"

const decoder = new TextDecoder()

function segments(buffer: ArrayBuffer) {
  const bytes = new Uint8Array(buffer)
  const envelopeLength = new DataView(buffer).getUint32(0, true)
  const envelopeEnd = EVENTS_FRAME_HEADER_BYTES + envelopeLength
  return {
    envelope: decoder.decode(bytes.subarray(EVENTS_FRAME_HEADER_BYTES, envelopeEnd)),
    properties: decoder.decode(bytes.subarray(envelopeEnd)),
  }
}

describe("events byte frames", () => {
  test("lays out header, envelope without properties, then the properties segment", () => {
    const properties = { text: "héllo", nested: { n: [1, 2] } }
    const buffer = encodeEventsFrameBytes(
      eventFrame({ event: { id: "e1", type: "message.part.delta", properties }, aggregateID: "agg", seq: 3 }),
    )
    const parts = segments(buffer)
    const envelope = JSON.parse(parts.envelope)
    expect(envelope.kind).toBe("event")
    expect(envelope.payload.event).toEqual({ id: "e1", type: "message.part.delta" })
    expect(envelope.payload.aggregateID).toBe("agg")
    expect(parts.envelope).not.toContain("héllo")
    expect(parts.properties).toBe(JSON.stringify(properties))
    expect(buffer.byteLength).toBe(
      EVENTS_FRAME_HEADER_BYTES + new TextEncoder().encode(parts.envelope).byteLength + new TextEncoder().encode(parts.properties).byteLength,
    )
  })

  test("undefined properties occupy the segment as null", () => {
    const buffer = encodeEventsFrameBytes(eventFrame({ event: { id: "e", type: "t", properties: undefined } }))
    expect(segments(buffer).properties).toBe("null")
  })

  test("non-event frames carry no properties segment", () => {
    for (const frame of [endFrame(), ackFrame({ count: 2 })]) {
      const buffer = encodeEventsFrameBytes(frame)
      const parts = segments(buffer)
      expect(parts.properties).toBe("")
      expect(JSON.parse(parts.envelope)).toEqual(JSON.parse(JSON.stringify(frame)))
    }
  })

  test("a buffer over 4 MiB throws a labelled FrameTooLargeError", () => {
    const frame = eventFrame({
      event: { id: "big", type: "session.big", properties: { pad: "x".repeat(MAX_WORKER_FRAME_BYTES) } },
    })
    let caught: unknown
    try {
      encodeEventsFrameBytes(frame)
    } catch (error) {
      caught = error
    }
    expect(caught).toBeInstanceOf(FrameTooLargeError)
    const error = caught as FrameTooLargeError
    expect(error.limit).toBe(MAX_WORKER_FRAME_BYTES)
    expect(error.byteLength).toBeGreaterThan(MAX_WORKER_FRAME_BYTES)
    expect(error.message).toContain('events frame "session.big"')
    expect(() => encodeEventsFrameBytes(endFrame(), 4)).toThrow('events frame "end"')
  })

  test("a buffer exactly at the bound encodes", () => {
    const base = encodeEventsFrameBytes(eventFrame({ event: { id: "e", type: "t", properties: { p: "" } } }))
    const pad = MAX_WORKER_FRAME_BYTES - base.byteLength
    const buffer = encodeEventsFrameBytes(eventFrame({ event: { id: "e", type: "t", properties: { p: "x".repeat(pad) } } }))
    expect(buffer.byteLength).toBe(MAX_WORKER_FRAME_BYTES)
  })

  test("roundtrips through the decoder", () => {
    const properties = { sessionID: "ses_1", text: "x" }
    const decoded = jsDecoder.decode(
      new Uint8Array(
        encodeEventsFrameBytes(
          eventFrame({ event: { id: "e1", type: "t", properties }, aggregateID: "ses_1", seq: 7 }, 5),
        ),
      ),
    )
    if (decoded.kind !== "event") throw new Error("expected event")
    expect(decoded.id).toBe(5)
    expect(decoded.event.id).toBe("e1")
    expect(decoded.aggregateID).toBe("ses_1")
    expect(decoded.seq).toBe(7)
    expect(decoded.event.properties.value).toEqual(properties)
    expect(jsDecoder.decode(new Uint8Array(encodeEventsFrameBytes(ackFrame({ count: 4 }))))).toEqual({
      kind: "ack",
      payload: { count: 4 },
    })
  })
})
