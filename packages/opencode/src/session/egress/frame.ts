// Worker event channel over the SHARED JSON transport (spec Q6/Q7/Q21, D15).
//
// The events port no longer uses a binary `[u32 length][body]` framing. Every
// port message is exactly one shared `WorkerFrameEnvelope` JSON string, encoded
// and parsed by `session/worker/frame.ts`; the events family (`event`/`end`/
// `resync`/`ack`) is validated there too. This gives the worker one codec and
// one bound across the events, writer, job, and hub-proxy ports.
//
// The `event` variant carries the EventV2 event VERBATIM as
// `{ id, type, properties }` — there is no `data` translation and no per-frame
// numeric header (so no `u32` length can overflow). Encoding rejects a frame or
// a coalesced batch above 4 MiB with a typed `FrameTooLargeError` before it is
// posted; decoding applies the same bound to untrusted input. The one exception
// is a DURABLE event that does not fit its envelope: it crosses as a small
// `{ __eventBlobPosition, frameBytes }` reference the hub resolves by position
// from either the shared blob store OR the committed inline `event.data` row, so
// a committed event is never lost to the frame cap.

import {
  FrameTooLargeError,
  MAX_WORKER_FRAME_BYTES,
  ProtocolError,
  UnsupportedFrameVersionError,
  WORKER_FRAME_VERSION,
  blobPositionReferenceFrame,
  decodeWorkerFrame,
  durableAggregateMetaOf,
  encodeWorkerFrameSized,
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
 * Envelope space reserved inside one frame: port/kind/id, committed
 * `(aggregateID, seq)` metadata, and JSON punctuation. The core D16 inline bound
 * reserves the same headroom, so a body stored inline is guaranteed to fit.
 */
export const ENVELOPE_HEADROOM_BYTES = 64 * 1024

/** Largest event body that always fits one frame with its envelope. */
export const MAX_INLINE_EVENT_BYTES = MAX_FRAME_BYTES - ENVELOPE_HEADROOM_BYTES

/**
 * A committed position in one aggregate's durable log. This is the ONLY durable
 * identity on the wire: there is no sender-global ordinal (D12/D17). The shape
 * is the shared envelope's, so the codec and the sender/receiver cannot drift.
 */
export type { AggregatePosition }

/** Durable aggregate metadata carried by every durable `event` frame. */
export type DurableMeta = AggregatePosition

/**
 * The aggregate identity a durable `event` frame carries, or `undefined` for a
 * non-durable delta / a non-`event` frame. Present only when BOTH `aggregateID`
 * and `seq` are set; the shared parser rejects a half-specified identity, so it
 * can never be mistaken for a delta.
 */
export { durableAggregateMetaOf as durableMetaOf } from "@/session/worker/frame"

/**
 * A resync's optional durable position. The shared events body schema owns the
 * `pattern`; the aggregate identity rides beside it as forward-compatible
 * payload metadata (exactly like `event` frames), so an older peer that ignores
 * it still resyncs by pattern.
 */
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

/**
 * Encode one frame and measure it ONCE, enforcing the 4 MiB per-frame bound.
 * Returning the measured size lets a batch total reuse it instead of encoding
 * and measuring the same text twice.
 */
function encodeFrameSized(frame: WorkerFrameEnvelope): { readonly text: string; readonly bytes: number } {
  const sized = encodeWorkerFrameSized(frame)
  if (sized.bytes > MAX_FRAME_BYTES) throw new FrameTooLargeError(sized.bytes, MAX_FRAME_BYTES)
  return sized
}

/** Encode one frame as the shared envelope JSON string, enforcing the 4 MiB bound. */
export function encodeFrame(frame: WorkerFrameEnvelope): string {
  return encodeFrameSized(frame).text
}

/**
 * Encode a coalesced batch, one JSON string per frame, preserving order. The
 * combined wire size is bounded by {@link MAX_BATCH_BYTES}: a batch too large to
 * transport is a typed rejection. The sender uses {@link encodeFrameChunks}
 * instead so a durable frame is never lost to a batch total.
 */
export function encodeFrames(frames: readonly WorkerFrameEnvelope[]): readonly string[] {
  const encoded: string[] = []
  let total = 0
  for (const frame of frames) {
    const sized = encodeFrameSized(frame)
    total += sized.bytes
    encoded.push(sized.text)
  }
  if (total > MAX_BATCH_BYTES) throw new FrameTooLargeError(total, MAX_BATCH_BYTES)
  return encoded
}

/**
 * Split a coalesced batch into chunks whose combined wire size stays within the
 * transport bound, measuring each frame exactly once. A durable event frame that
 * alone exceeds the bound is replaced by its committed blob-position reference
 * (core already offloaded the body before fan-out), so the batch survives and no
 * durable event is dropped by the cap.
 *
 * A non-durable frame is advisory stream data. When `onDrop` is supplied, a
 * non-durable frame that alone exceeds the bound is dropped and reported rather
 * than failing the whole batch; without it the codec keeps its typed rejection.
 * A batch that merely totals too much is always split, never dropped.
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
): readonly (readonly string[])[] {
  const options = typeof limitOrOptions === "number" ? { limit: limitOrOptions } : limitOrOptions
  const limit = options.limit ?? MAX_BATCH_BYTES
  const onDrop = options.onDrop
  const chunks: string[][] = []
  let current: string[] = []
  let total = 0
  for (const frame of frames) {
    let sized: { readonly text: string; readonly bytes: number }
    try {
      sized = encodeFrameOrReference(frame)
    } catch (error) {
      // Only a non-durable advisory frame is droppable: a durable commit has a
      // position reference or fails loud, so its bytes are never silently lost.
      if (onDrop !== undefined && error instanceof FrameTooLargeError && durableAggregateMetaOf(frame) === undefined) {
        onDrop(frame, error)
        continue
      }
      throw error
    }
    if (sized.bytes > limit) throw new FrameTooLargeError(sized.bytes, limit)
    if (current.length > 0 && total + sized.bytes > limit) {
      chunks.push(current)
      current = []
      total = 0
    }
    current.push(sized.text)
    total += sized.bytes
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
 * The frame to encode when the event ALREADY carries a resolvable reference,
 * checked before any serialisation. An `{ __eventBlobPosition }` frame is sent
 * as-is (never re-wrapped), and EVERY durable core `{ __eventBlob, size }` marker
 * is turned into the committed position reference the hub can resolve. The
 * marker's own frame is tiny, so its declared `size` must not gate the rewrite:
 * a body in the window between the inline bound and the frame cap otherwise
 * crossed unresolved. A non-durable marker has no position and keeps its legacy
 * pass-through (`blobPositionReferenceFrame` returns `undefined`).
 */
function referenceFrameOf(frame: EventsFrame): EventsFrame | undefined {
  if (frame.kind !== "event") return undefined
  const properties = frame.payload.event.properties
  if (isEventBlobReference(properties)) return frame
  if (!isCoreBlobReference(properties)) return undefined
  return blobPositionReferenceFrame(frame, properties.size)
}

/**
 * Encode one frame within the per-frame bound. A durable event body that does
 * not fit is re-expressed as its blob-position reference and re-encoded; the
 * substituted frame is tiny by construction. Everything else keeps the typed
 * rejection, so a caller bug (or an over-cap non-durable delta) still fails loud.
 */
function encodeFrameOrReference(frame: EventsFrame): { readonly text: string; readonly bytes: number } {
  const existingReference = referenceFrameOf(frame)
  if (existingReference !== undefined) return encodeFrameSized(existingReference)
  try {
    return encodeFrameSized(frame)
  } catch (error) {
    if (!(error instanceof FrameTooLargeError)) throw error
    const reference = blobPositionReferenceFrame(frame, error.byteLength)
    if (reference === undefined) throw error
    // The substituted frame must be the small, resolvable reference — never the
    // original body re-encoded (which would overflow again) and never a
    // half-formed marker the hub cannot resolve by position.
    if (reference.kind !== "event" || !isEventBlobReference(reference.payload.event.properties)) throw error
    return encodeFrameSized(reference)
  }
}

/**
 * Decode one wire message (a JSON string) into a trusted events frame. The
 * shared parser validates the body; the raw envelope is still available for
 * forward-compatible metadata the body schema does not own, so a resync's
 * `(aggregateID, seq)` position is restored here rather than dropped.
 */
export function decodeFrame(text: string): DecodedEventsFrame {
  const envelope = decodeWorkerFrame(text)
  const frame = parseEventsFrame(envelope)
  if (frame.kind !== "resync") return frame
  const position = durablePositionOf(envelope.payload)
  if (position === undefined) return frame
  return { ...frame, payload: { ...frame.payload, ...position } }
}

/** Decode a batch of wire messages, in order. */
export function decodeFrames(texts: readonly string[]): readonly DecodedEventsFrame[] {
  return texts.map(decodeFrame)
}

export * as EgressFrameCodec from "./frame"
