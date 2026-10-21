// W2 — length-prefixed framing for the generic worker→hub event channel.
//
// The channel keeps its byte framing, but the body is now a shared
// `WorkerFrameEnvelope` (events port) instead of the old `EgressEvent`:
//
//     [u32 big-endian body length][UTF-8 JSON body]
//
// Envelope validation is delegated to `session/worker/frame.ts` (the single
// shared envelope, spec Q8/Q21/Q22/Q24), so the events `event` variant carries
// the EventV2 event VERBATIM as `{ id, type, properties }` — there is no `data`
// translation. The codec is total on the *framing* layer: `decodeFrames` returns
// the complete frames plus any partial trailing bytes as `rest`, so a caller can
// reassemble frames split across `MessagePort` deliveries. Anything well-framed
// but structurally invalid (bad JSON, unknown version/port/kind) fails loud, so
// illegal states never enter internal logic.

import {
  ProtocolError,
  UnsupportedFrameVersionError,
  WORKER_FRAME_VERSION,
  encodeWorkerFrame,
  parseEventsFrame,
  type EventsAckPayload,
  type EventsEventPayload,
  type EventsFrame,
  type EventsResyncPayload,
  type EventV2Event,
  type WorkerFrameEnvelope,
} from "@/session/worker/frame"

import type { WriterOrdinal } from "./ordinal"

export { ProtocolError, UnsupportedFrameVersionError }
export type {
  EventsAckPayload,
  EventsEventPayload,
  EventsFrame,
  EventsResyncPayload,
  EventV2Event,
  WorkerFrameEnvelope,
}

/** The only frame version understood by this build (shared envelope version). */
export const FRAME_VERSION = WORKER_FRAME_VERSION

/** Writer-stamped durable metadata; identical in shape to `WriterOrdinal`. */
export type DurableMeta = WriterOrdinal

/**
 * @deprecated The channel now carries EventV2 events. Use {@link EventV2Event}.
 * Retained only so opaque carriers (`control.ts`, `mesh.ts`) keep compiling.
 */
export type EgressEvent = EventV2Event

/**
 * @deprecated The channel now carries events frames. Use {@link EventsFrame}.
 * Retained only as a stable name for existing imports.
 */
export type EgressFrame = EventsFrame

const HEADER_BYTES = 4
const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()

/** Raised when the byte framing is wrong (truncated, multi-frame, or bad JSON). */
export class MalformedFrameError extends Error {
  constructor(message: string) {
    super(`malformed egress frame: ${message}`)
    this.name = "MalformedFrameError"
  }
}

/** Encode one frame as `[u32 length][UTF-8 JSON]`. */
export function encodeFrame(frame: WorkerFrameEnvelope): Uint8Array {
  const body = textEncoder.encode(encodeWorkerFrame(frame))
  const bytes = new Uint8Array(HEADER_BYTES + body.byteLength)
  new DataView(bytes.buffer).setUint32(0, body.byteLength, false)
  bytes.set(body, HEADER_BYTES)
  return bytes
}

/**
 * Encode a batch of frames into ONE buffer, each length-prefixed in order, so a
 * coalesced delta batch is posted as a single zero-copy message.
 */
export function encodeFrames(frames: readonly WorkerFrameEnvelope[]): Uint8Array {
  const encoded = frames.map(encodeFrame)
  const bytes = new Uint8Array(encoded.reduce((total, frame) => total + frame.byteLength, 0))
  let offset = 0
  for (const frame of encoded) {
    bytes.set(frame, offset)
    offset += frame.byteLength
  }
  return bytes
}

/** Decode a complete single frame; fails loud on a partial, multi-frame, or unsupported frame. */
export function decodeFrame(bytes: Uint8Array): EventsFrame {
  if (bytes.byteLength < HEADER_BYTES) {
    throw new MalformedFrameError(`truncated frame header: ${bytes.byteLength} of ${HEADER_BYTES} byte(s)`)
  }
  const length = new DataView(bytes.buffer, bytes.byteOffset, HEADER_BYTES).getUint32(0, false)
  const total = HEADER_BYTES + length
  if (bytes.byteLength < total) {
    throw new MalformedFrameError(`truncated frame body: ${bytes.byteLength - HEADER_BYTES} of ${length} byte(s)`)
  }
  if (bytes.byteLength > total) {
    throw new MalformedFrameError(`expected exactly 1 frame, got ${bytes.byteLength - total} trailing byte(s)`)
  }
  return parseBody(bytes.subarray(HEADER_BYTES, total))
}

export interface DecodedFrames {
  readonly frames: readonly EventsFrame[]
  /** Bytes of an incomplete trailing frame, to prepend to the next chunk. */
  readonly rest: Uint8Array
}

/** A decoded frame plus the exact wire bytes it came from (for zero-copy fan-out). */
export interface FrameChunk {
  readonly frame: EventsFrame
  readonly bytes: Uint8Array
}

export interface DecodedFrameChunks {
  readonly frames: readonly FrameChunk[]
  readonly rest: Uint8Array
}

/** Decode every complete frame in `bytes`, retaining each frame's raw bytes. */
export function decodeFrameChunks(bytes: Uint8Array): DecodedFrameChunks {
  const frames: FrameChunk[] = []
  let offset = 0
  while (offset + HEADER_BYTES <= bytes.byteLength) {
    const length = new DataView(bytes.buffer, bytes.byteOffset + offset, HEADER_BYTES).getUint32(0, false)
    if (offset + HEADER_BYTES + length > bytes.byteLength) break
    const view = bytes.subarray(offset, offset + HEADER_BYTES + length)
    frames.push({
      frame: parseBody(bytes.subarray(offset + HEADER_BYTES, offset + HEADER_BYTES + length)),
      bytes: view,
    })
    offset += HEADER_BYTES + length
  }
  // `rest` must not alias the caller's buffer: a later mutation or reuse of the
  // chunk would silently corrupt the bytes we hand to the next decode.
  return { frames, rest: bytes.slice(offset) }
}

/** Decode every complete frame in `bytes`; a partial trailing frame is returned as `rest`. */
export function decodeFrames(bytes: Uint8Array): DecodedFrames {
  const { frames, rest } = decodeFrameChunks(bytes)
  return { frames: frames.map((chunk) => chunk.frame), rest }
}

/** Parse the framed body as an events-port frame; malformed bodies fail loud. */
function parseBody(body: Uint8Array): EventsFrame {
  return parseEventsFrame(parseJson(body))
}

/** Parse the JSON body, translating the syntax error into the codec's own error type. */
function parseJson(body: Uint8Array): unknown {
  try {
    return JSON.parse(textDecoder.decode(body))
  } catch (error) {
    throw new MalformedFrameError(`invalid JSON body: ${error instanceof Error ? error.message : String(error)}`)
  }
}

export * as EgressFrameCodec from "./frame"
