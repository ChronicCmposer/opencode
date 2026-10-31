// Events-port byte frames (D15): `[u32 LE envelopeLength][envelope JSON][properties JSON]` in one buffer.
// The envelope is the events frame without `payload.event.properties`; only `event` frames carry a properties
// segment. The worker encodes once and transfers the buffer; the hub decodes with `scan.ts`.

import { FrameTooLargeError, MAX_WORKER_FRAME_BYTES, type EventsFrame } from "@/session/worker/frame"

export const EVENTS_FRAME_HEADER_BYTES = 4

const encoder = new TextEncoder()

/** Encode one events frame as a transferable byte frame, enforcing the 4 MiB bound on the whole buffer. */
export function encodeEventsFrameBytes(frame: EventsFrame, limit: number = MAX_WORKER_FRAME_BYTES): ArrayBuffer {
  const { envelope, properties } = splitFrame(frame)
  const envelopeBytes = encoder.encode(JSON.stringify(envelope))
  const propertiesBytes = properties === undefined ? undefined : encoder.encode(JSON.stringify(properties))
  const total = EVENTS_FRAME_HEADER_BYTES + envelopeBytes.byteLength + (propertiesBytes?.byteLength ?? 0)
  if (total > limit) {
    const label = frame.kind === "event" ? `events frame "${frame.payload.event.type}"` : `events frame "${frame.kind}"`
    throw new FrameTooLargeError(total, limit, label)
  }
  const buffer = new ArrayBuffer(total)
  const view = new DataView(buffer)
  view.setUint32(0, envelopeBytes.byteLength, true)
  const bytes = new Uint8Array(buffer)
  bytes.set(envelopeBytes, EVENTS_FRAME_HEADER_BYTES)
  if (propertiesBytes !== undefined) bytes.set(propertiesBytes, EVENTS_FRAME_HEADER_BYTES + envelopeBytes.byteLength)
  return buffer
}

function splitFrame(frame: EventsFrame): { readonly envelope: unknown; readonly properties?: unknown } {
  if (frame.kind !== "event") return { envelope: frame }
  const { properties, ...event } = frame.payload.event
  return {
    envelope: { ...frame, payload: { ...frame.payload, event } },
    // A body JSON cannot represent (undefined) still occupies the segment as `null`, so the decoder always finds one.
    properties: properties === undefined ? null : properties,
  }
}
