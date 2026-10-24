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
 * The writer-stamped `(generation, ordinal)` a durable `event` frame carries, or
 * `undefined` for a non-durable delta / a non-`event` frame. This is the single
 * durability test the fan-out (backpressure) and receiver (ordering) share, so
 * both agree on exactly which frames may be dropped and which are ordered.
 *
 * Re-exported from the shared envelope (`session/worker/frame.ts`) so there is
 * exactly one implementation; the events-port codec simply surfaces it.
 */
export { durableMetaOf } from "@/session/worker/frame"

const HEADER_BYTES = 4
const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()

/**
 * Upper bound on a single frame body. The wire length is an untrusted `u32`, so
 * a corrupt or hostile header could otherwise claim up to 4 GiB and pin the
 * reassembly buffer forever. A length past this bound fails loud instead.
 */
export const MAX_FRAME_BYTES = 16 * 1024 * 1024

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
  const length = readFrameLength(bytes, 0)
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
    const length = readFrameLength(bytes, offset)
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

/**
 * Incremental reassembly buffer for the length-prefixed codec.
 *
 * A frame may be split across deliveries and one delivery may carry several
 * frames; a caller appends each chunk and receives the frames completed so far.
 * Bytes accumulate in one preallocated, geometrically-grown buffer rather than a
 * fresh concatenation per chunk, so a large frame delivered in many small pieces
 * reassembles in linear time instead of quadratic. Each completed frame's wire
 * bytes are copied out, because the internal buffer is reused for later chunks.
 */
export interface FrameDecoder {
  /** Append `chunk` and return the frames it completed, in order. */
  push(chunk: Uint8Array): readonly FrameChunk[]
  /** Bytes buffered for an as-yet incomplete frame. */
  readonly pending: number
  /** Discard every buffered byte (after a fatal error). */
  reset(): void
}

export function createFrameDecoder(initialCapacity = 1024): FrameDecoder {
  let buffer = new Uint8Array(initialCapacity < HEADER_BYTES ? HEADER_BYTES : initialCapacity)
  let used = 0

  const grow = (needed: number): void => {
    let capacity = buffer.byteLength
    while (capacity < needed) capacity *= 2
    const grown = new Uint8Array(capacity)
    grown.set(buffer.subarray(0, used))
    buffer = grown
  }

  return {
    push(chunk) {
      if (used + chunk.byteLength > buffer.byteLength) grow(used + chunk.byteLength)
      buffer.set(chunk, used)
      used += chunk.byteLength

      const frames: FrameChunk[] = []
      let offset = 0
      while (used - offset >= HEADER_BYTES) {
        const length = readFrameLength(buffer, offset)
        const total = HEADER_BYTES + length
        if (used - offset < total) break
        frames.push({
          frame: parseBody(buffer.subarray(offset + HEADER_BYTES, offset + total)),
          // Copy out: the buffer region is reclaimed for the next frame.
          bytes: buffer.slice(offset, offset + total),
        })
        offset += total
      }
      if (offset > 0) {
        buffer.copyWithin(0, offset, used)
        used -= offset
      }
      return frames
    },
    get pending() {
      return used
    },
    reset() {
      used = 0
    },
  }
}

/** Read the big-endian body length at `offset`, failing loud past {@link MAX_FRAME_BYTES}. */
function readFrameLength(bytes: Uint8Array, offset: number): number {
  const length = new DataView(bytes.buffer, bytes.byteOffset + offset, HEADER_BYTES).getUint32(0, false)
  if (length > MAX_FRAME_BYTES) {
    throw new MalformedFrameError(`frame length ${length} exceeds the ${MAX_FRAME_BYTES}-byte maximum`)
  }
  return length
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
