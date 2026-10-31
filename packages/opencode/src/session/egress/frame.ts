// Worker event channel over the SHARED JSON transport (spec Q6/Q7/Q21, D15).

import {
  FrameTooLargeError,
  MAX_WORKER_FRAME_BYTES,
  ProtocolError,
  UnsupportedFrameVersionError,
  WORKER_FRAME_VERSION,
  blobPositionReferenceFrame,
  decodeWorkerFrame,
  durableAggregateMetaOf,
  isEventBlobReference,
  parseEventsFrame,
  type EventsAckPayload,
  type EventsEventPayload,
  type EventsFrame,
  type EventsResyncPayload,
  type EventV2Event,
  type WorkerFrameEnvelope,
  type AggregatePosition,
} from "@/session/worker/frame"
import { encodeEventsFrameBytes } from "./frame-bytes"

import { activeDecoder, type ScannedEventsFrame } from "./scan"

export { FrameTooLargeError, ProtocolError, UnsupportedFrameVersionError, isEventBlobReference }
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

/** The single 4 MiB bound shared with every other worker port. */
export const MAX_FRAME_BYTES = MAX_WORKER_FRAME_BYTES

/** A coalesced batch may not exceed the same 4 MiB wire bound. */
export const MAX_BATCH_BYTES = MAX_WORKER_FRAME_BYTES

/**
 * Envelope space reserved inside one frame: port/kind/id, committed `(aggregateID, seq)` metadata, and JSON
 * punctuation.
 */
export const ENVELOPE_HEADROOM_BYTES = 64 * 1024

/** Largest event body that always fits one frame with its envelope. */
export const MAX_INLINE_EVENT_BYTES = MAX_FRAME_BYTES - ENVELOPE_HEADROOM_BYTES

/** A committed position in one aggregate's durable log. */
export type { AggregatePosition }

/** Durable aggregate metadata carried by every durable `event` frame. */
export type DurableMeta = AggregatePosition

/**
 * The aggregate identity a durable `event` frame carries, or `undefined` for a non-durable delta / a
 * non-`event` frame.
 */
export { durableAggregateMetaOf as durableMetaOf } from "@/session/worker/frame"

/** A resync's optional durable position. */
export interface DurableResyncPayload extends EventsResyncPayload {
  readonly aggregateID?: string
  readonly seq?: number
}

/** A decoded `resync` frame with the optional aggregate position restored. */
export type ResyncFrame = Omit<Extract<EventsFrame, { readonly kind: "resync" }>, "payload"> & {
  readonly payload?: DurableResyncPayload
}

/** Every decoded events frame; `resync` may carry the aggregate position. */
export type DecodedEventsFrame = Exclude<EventsFrame, { readonly kind: "resync" }> | ResyncFrame

/** Build a `resync` frame asking a peer to heal the exact `(aggregateID, seq)`. */
export function resyncWithPositionFrame(payload: DurableResyncPayload): WorkerFrameEnvelope {
  return { v: WORKER_FRAME_VERSION, port: "events", kind: "resync", payload }
}

/** Parse the trailing aggregate position on a resync payload, failing loud on half a pair. */
function durablePositionOf(value: unknown): AggregatePosition | undefined {
  if (typeof value !== "object" || value === null) return undefined
  const record = value as Record<string, unknown>
  const aggregateID = record["aggregateID"]
  const seq = record["seq"]
  if (aggregateID === undefined && seq === undefined) return undefined
  if (aggregateID === undefined || seq === undefined) {
    throw new ProtocolError("resync must carry both aggregateID and seq, or neither")
  }
  if (typeof aggregateID !== "string" || aggregateID.length === 0) {
    throw new ProtocolError(`resync aggregateID must be a non-empty string (got ${JSON.stringify(aggregateID)})`)
  }
  if (typeof seq !== "number" || !Number.isInteger(seq) || seq < 0) {
    throw new ProtocolError(`resync seq must be a non-negative integer (got ${JSON.stringify(seq)})`)
  }
  return { aggregateID, seq }
}

/** Encode one events frame as a transferable byte frame, enforcing the 4 MiB bound on the whole buffer. */
export function encodeFrame(frame: EventsFrame): ArrayBuffer {
  return encodeEventsFrameBytes(frame, MAX_FRAME_BYTES)
}

/** Encode a coalesced batch, one byte frame per frame, preserving order. */
export function encodeFrames(frames: readonly EventsFrame[]): readonly ArrayBuffer[] {
  const encoded: ArrayBuffer[] = []
  let total = 0
  for (const frame of frames) {
    const buffer = encodeFrame(frame)
    total += buffer.byteLength
    encoded.push(buffer)
  }
  if (total > MAX_BATCH_BYTES) throw new FrameTooLargeError(total, MAX_BATCH_BYTES)
  return encoded
}

/**
 * Split a coalesced batch into chunks whose combined wire size stays within the transport bound, measuring
 * each frame exactly once.
 */
export interface EncodeFrameChunksOptions {
  /** Total wire bound per chunk; defaults to {@link MAX_BATCH_BYTES}. */
  readonly limit?: number
  /** Called for an advisory (non-durable) frame that alone exceeds the bound. */
  readonly onDrop?: (frame: EventsFrame, error: FrameTooLargeError) => void
}

export function encodeFrameChunks(
  frames: readonly EventsFrame[],
  limitOrOptions: number | EncodeFrameChunksOptions = MAX_BATCH_BYTES,
): readonly (readonly ArrayBuffer[])[] {
  const options = typeof limitOrOptions === "number" ? { limit: limitOrOptions } : limitOrOptions
  const limit = options.limit ?? MAX_BATCH_BYTES
  const onDrop = options.onDrop
  const chunks: ArrayBuffer[][] = []
  let current: ArrayBuffer[] = []
  let total = 0
  for (const frame of frames) {
    let buffer: ArrayBuffer
    try {
      buffer = encodeFrameOrReference(frame)
    } catch (error) {
      // Only a non-durable advisory frame is droppable: a durable commit has a
      // position reference or fails loud, so its bytes are never silently lost.
      if (onDrop !== undefined && error instanceof FrameTooLargeError && durableAggregateMetaOf(frame) === undefined) {
        onDrop(frame, error)
        continue
      }
      throw error
    }
    const bytes = buffer.byteLength
    if (bytes > limit) throw new FrameTooLargeError(bytes, limit)
    if (current.length > 0 && total + bytes > limit) {
      chunks.push(current)
      current = []
      total = 0
    }
    current.push(buffer)
    total += bytes
  }
  if (current.length > 0) chunks.push(current)
  return chunks
}

/** Core's D16 offload marker: the body lives in the content-addressed blob store. */
interface CoreBlobReference {
  readonly __eventBlob: string
  readonly size: number
}

function isCoreBlobReference(value: unknown): value is CoreBlobReference {
  if (typeof value !== "object" || value === null) return false
  const record = value as Record<string, unknown>
  return typeof record["__eventBlob"] === "string" && typeof record["size"] === "number"
}

/**
 * The frame to encode when the event ALREADY carries a resolvable reference, checked before any
 * serialisation.
 */
function referenceFrameOf(frame: EventsFrame): EventsFrame | undefined {
  if (frame.kind !== "event") return undefined
  const properties = frame.payload.event.properties
  if (isEventBlobReference(properties)) return frame
  if (!isCoreBlobReference(properties)) return undefined
  return blobPositionReferenceFrame(frame, properties.size)
}

/** Encode one frame within the per-frame bound. */
export function encodeFrameOrReference(frame: EventsFrame): ArrayBuffer {
  const existingReference = referenceFrameOf(frame)
  if (existingReference !== undefined) return encodeFrame(existingReference)
  try {
    return encodeFrame(frame)
  } catch (error) {
    if (!(error instanceof FrameTooLargeError)) throw error
    const reference = blobPositionReferenceFrame(frame, error.byteLength)
    if (reference === undefined) throw error
    // The substituted frame must be the small, resolvable reference — never the original body re-encoded
    // (which would overflow again) and never a half-formed marker the hub cannot resolve by position.
    if (reference.kind !== "event" || !isEventBlobReference(reference.payload.event.properties)) throw error
    return encodeFrame(reference)
  }
}

/** Decode one wire message (a JSON string) into a trusted events frame. */
export function decodeFrame(text: string): DecodedEventsFrame {
  const envelope = decodeWorkerFrame(text)
  const frame = parseEventsFrame(envelope)
  if (frame.kind !== "resync") return frame
  const position = durablePositionOf(envelope.payload)
  if (position === undefined) return frame
  return { ...frame, payload: { ...frame.payload, ...position } }
}

/** Decode one events-port byte frame; `properties` stays raw as `LazyJson`. */
export function decodeFrameBytes(buffer: ArrayBuffer): ScannedEventsFrame {
  return activeDecoder().decode(new Uint8Array(buffer))
}

/** Decode a batch of wire messages, in order. */
export function decodeFrames(texts: readonly string[]): readonly DecodedEventsFrame[] {
  return texts.map(decodeFrame)
}

export * as EgressFrameCodec from "./frame"
