// Phase 8.2 — length-prefixed versioned egress frames (plan task 8.2).
//
// The worker→hub egress channel is a byte stream of frames:
//
//     [u32 big-endian body length][UTF-8 JSON body]
//
// `v1` encodes the body once as UTF-8 JSON (room for a binary v2 later). The
// codec is deliberately total on the *framing* layer: `decodeFrames` returns the
// complete frames plus any partial trailing bytes as `rest`, so a caller can
// reassemble frames that arrive split across `MessagePort` deliveries. Anything
// that is well-framed but structurally invalid (unknown version / kind) fails
// loud, so illegal states never enter internal logic.

export const FRAME_VERSION = 1

const HEADER_BYTES = 4
const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()

/** Writer-stamped global ordinal carried on durable frames (plan task 8.5). */
export interface DurableMeta {
  readonly generation: number
  readonly ordinal: number
}

/** The minimal event envelope the egress channel transports. */
export interface EgressEvent {
  readonly id: string
  readonly type: string
  readonly data: unknown
  /** Present only for durable events; the per-aggregate sequence from the writer. */
  readonly durable?: { readonly aggregateID: string; readonly seq: number }
}

/**
 * A frame on the wire.
 *
 * - `deltas` — one or more coalesced non-durable events; carries no ordinal.
 * - `durable` — a single durable boundary event plus its writer-stamped meta.
 * - `end` — end-of-stream marker.
 */
export type EgressFrame =
  | { readonly kind: "deltas"; readonly v: typeof FRAME_VERSION; readonly events: readonly EgressEvent[] }
  | {
      readonly kind: "durable"
      readonly v: typeof FRAME_VERSION
      readonly meta: DurableMeta
      readonly event: EgressEvent
    }
  | { readonly kind: "end"; readonly v: typeof FRAME_VERSION }

export class UnsupportedFrameVersionError extends Error {
  constructor(readonly version: unknown) {
    super(`unsupported egress frame version: ${String(version)} (expected ${FRAME_VERSION})`)
    this.name = "UnsupportedFrameVersionError"
  }
}

export class MalformedFrameError extends Error {
  constructor(message: string) {
    super(`malformed egress frame: ${message}`)
    this.name = "MalformedFrameError"
  }
}

/** Encode one frame as `[u32 length][UTF-8 JSON]`. */
export function encodeFrame(frame: EgressFrame): Uint8Array {
  const body = textEncoder.encode(JSON.stringify(frame))
  const bytes = new Uint8Array(HEADER_BYTES + body.byteLength)
  new DataView(bytes.buffer).setUint32(0, body.byteLength, false)
  bytes.set(body, HEADER_BYTES)
  return bytes
}

/** Decode a complete single frame; fails loud on a partial, multi-frame, or unsupported frame. */
export function decodeFrame(bytes: Uint8Array): EgressFrame {
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
  return parseFrame(bytes.subarray(HEADER_BYTES, total))
}

export interface DecodedFrames {
  readonly frames: readonly EgressFrame[]
  /** Bytes of an incomplete trailing frame, to prepend to the next chunk. */
  readonly rest: Uint8Array
}

/** A decoded frame plus the exact wire bytes it came from (for zero-copy fan-out). */
export interface FrameChunk {
  readonly frame: EgressFrame
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
    frames.push({ frame: parseFrame(bytes.subarray(offset + HEADER_BYTES, offset + HEADER_BYTES + length)), bytes: view })
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

function parseFrame(body: Uint8Array): EgressFrame {
  const parsed = parseJson(body)
  const frame = asRecord(parsed, "body is not an object")
  if (frame.v !== FRAME_VERSION) throw new UnsupportedFrameVersionError(frame.v)
  const kind = frame.kind
  if (kind === "end") return { kind: "end", v: FRAME_VERSION }
  if (kind === "deltas") {
    const events = frame.events
    if (!Array.isArray(events)) throw new MalformedFrameError("deltas.events is not an array")
    return { kind: "deltas", v: FRAME_VERSION, events: events.map(parseEvent) }
  }
  if (kind === "durable") {
    const meta = asRecord(frame.meta, "durable.meta is not an object")
    return {
      kind: "durable",
      v: FRAME_VERSION,
      meta: {
        generation: asInteger(meta.generation, "durable.meta.generation is not an integer"),
        ordinal: asInteger(meta.ordinal, "durable.meta.ordinal is not an integer"),
      },
      event: parseEvent(frame.event),
    }
  }
  throw new MalformedFrameError(`unknown kind: ${String(kind)}`)
}

/** Parse the JSON body, translating the syntax error into the codec's own error type. */
function parseJson(body: Uint8Array): unknown {
  try {
    return JSON.parse(textDecoder.decode(body))
  } catch (error) {
    throw new MalformedFrameError(`invalid JSON body: ${error instanceof Error ? error.message : String(error)}`)
  }
}

/** Parse one event at the frame boundary so internal logic never sees a partial event. */
function parseEvent(value: unknown): EgressEvent {
  const record = asRecord(value, "event is not an object")
  const { id, type, data, durable } = record
  if (typeof id !== "string") throw new MalformedFrameError("event.id is not a string")
  if (typeof type !== "string") throw new MalformedFrameError("event.type is not a string")
  if (data === undefined) throw new MalformedFrameError("event.data is missing")
  if (durable === undefined) return { id, type, data }
  const stamp = asRecord(durable, "event.durable is not an object")
  return {
    id,
    type,
    data,
    durable: {
      aggregateID: asString(stamp.aggregateID, "event.durable.aggregateID is not a string"),
      seq: asInteger(stamp.seq, "event.durable.seq is not an integer"),
    },
  }
}

function asRecord(value: unknown, message: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null) throw new MalformedFrameError(message)
  return value as Record<string, unknown>
}

function asInteger(value: unknown, message: string): number {
  if (typeof value !== "number" || !Number.isInteger(value)) throw new MalformedFrameError(message)
  return value
}

function asString(value: unknown, message: string): string {
  if (typeof value !== "string") throw new MalformedFrameError(message)
  return value
}

export * as EgressFrameCodec from "./frame"
