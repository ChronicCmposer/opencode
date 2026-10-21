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

/** Decode a complete single frame; fails loud on a partial or unsupported frame. */
export function decodeFrame(bytes: Uint8Array): EgressFrame {
  const { frames, rest } = decodeFrames(bytes)
  if (rest.byteLength !== 0) throw new MalformedFrameError(`truncated frame: ${rest.byteLength} trailing byte(s)`)
  if (frames.length !== 1) throw new MalformedFrameError(`expected exactly 1 frame, got ${frames.length}`)
  return frames[0]
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
  return { frames, rest: bytes.subarray(offset) }
}

/** Decode every complete frame in `bytes`; a partial trailing frame is returned as `rest`. */
export function decodeFrames(bytes: Uint8Array): DecodedFrames {
  const { frames, rest } = decodeFrameChunks(bytes)
  return { frames: frames.map((chunk) => chunk.frame), rest }
}

function parseFrame(body: Uint8Array): EgressFrame {
  const parsed: unknown = JSON.parse(textDecoder.decode(body))
  const frame = asRecord(parsed, "body is not an object")
  if (frame.v !== FRAME_VERSION) throw new UnsupportedFrameVersionError(frame.v)
  const kind = frame.kind
  if (kind === "end") return { kind: "end", v: FRAME_VERSION }
  if (kind === "deltas") {
    const events = frame.events
    if (!Array.isArray(events)) throw new MalformedFrameError("deltas.events is not an array")
    return { kind: "deltas", v: FRAME_VERSION, events: events as readonly EgressEvent[] }
  }
  if (kind === "durable") {
    const meta = asRecord(frame.meta, "durable.meta is not an object")
    return {
      kind: "durable",
      v: FRAME_VERSION,
      meta: { generation: Number(meta.generation), ordinal: Number(meta.ordinal) },
      event: frame.event as EgressEvent,
    }
  }
  throw new MalformedFrameError(`unknown kind: ${String(kind)}`)
}

function asRecord(value: unknown, message: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null) throw new MalformedFrameError(message)
  return value as Record<string, unknown>
}

export * as EgressFrameCodec from "./frame"
