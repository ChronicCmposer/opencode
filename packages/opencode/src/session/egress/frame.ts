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
// posted; decoding applies the same bound to untrusted input.

import {
  FrameTooLargeError,
  MAX_WORKER_FRAME_BYTES,
  ProtocolError,
  UnsupportedFrameVersionError,
  WORKER_FRAME_VERSION,
  assertWorkerFrameSize,
  decodeWorkerFrame,
  encodeWorkerFrame,
  parseEventsFrame,
  workerFrameByteLength,
  type EventsAckPayload,
  type EventsEventPayload,
  type EventsFrame,
  type EventsResyncPayload,
  type EventV2Event,
  type WorkerFrameEnvelope,
  type AggregatePosition,
} from "@/session/worker/frame"

export { FrameTooLargeError, ProtocolError, UnsupportedFrameVersionError }
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

/** Encode one frame as the shared envelope JSON string, enforcing the 4 MiB bound. */
export function encodeFrame(frame: WorkerFrameEnvelope): string {
  return assertWorkerFrameSize(encodeWorkerFrame(frame), MAX_FRAME_BYTES)
}

/**
 * Encode a coalesced batch, one JSON string per frame, preserving order. The
 * combined wire size is bounded by {@link MAX_BATCH_BYTES}: a batch too large to
 * transport is a typed rejection rather than a silent split or a stalled flush.
 */
export function encodeFrames(frames: readonly WorkerFrameEnvelope[]): readonly string[] {
  const encoded = frames.map(encodeFrame)
  const total = encoded.reduce((sum, text) => sum + workerFrameByteLength(text), 0)
  if (total > MAX_BATCH_BYTES) throw new FrameTooLargeError(total, MAX_BATCH_BYTES)
  return encoded
}

/** Decode one wire message (a JSON string) into a trusted events frame. */
export function decodeFrame(text: string): EventsFrame {
  return parseEventsFrame(decodeWorkerFrame(text))
}

/** Decode a batch of wire messages, in order. */
export function decodeFrames(texts: readonly string[]): readonly EventsFrame[] {
  return texts.map(decodeFrame)
}

export * as EgressFrameCodec from "./frame"
