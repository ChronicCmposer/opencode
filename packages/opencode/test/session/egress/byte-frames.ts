import { EVENTS_FRAME_HEADER_BYTES } from "@/session/egress/frame-bytes"
import type { EventsFrame } from "@/session/worker/frame"

const decoder = new TextDecoder()

/** Re-join an events byte frame into the plain frame it was encoded from (envelope plus properties segment). */
export function readByteFrame(buffer: unknown): EventsFrame {
  if (!(buffer instanceof ArrayBuffer)) throw new Error(`expected an ArrayBuffer, got ${Object.prototype.toString.call(buffer)}`)
  const bytes = new Uint8Array(buffer)
  const envelopeEnd = EVENTS_FRAME_HEADER_BYTES + new DataView(buffer).getUint32(0, true)
  const envelope = JSON.parse(decoder.decode(bytes.subarray(EVENTS_FRAME_HEADER_BYTES, envelopeEnd)))
  if (envelope.kind !== "event") return envelope
  const properties = JSON.parse(decoder.decode(bytes.subarray(envelopeEnd)))
  return {
    ...envelope,
    payload: { ...envelope.payload, event: { ...envelope.payload.event, properties } },
  }
}
