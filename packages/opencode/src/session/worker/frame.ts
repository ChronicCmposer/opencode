// Shared versioned frame envelope used across the worker ports (spec decisions
// Q8/Q21/Q22/Q24).
//
// Every worker `MessagePort` speaks one envelope shape, so transport, correlation
// and versioning live in exactly one place instead of drifting per protocol:
//
//   * `events`    — EventV2 delivery plus flow control. THIS module owns the
//                   `event` / `end` / `resync` / `ack` variants.
//   * `writer`    — reserved legacy single-writer SQLite + file-KV RPC family.
//                   The writer protocol was removed with per-worker DBs; the port
//                   name stays in the versioned set for frame compatibility.
//                   No producer exists in v2.
//   * `job`       — reserved legacy background-job envelope family. The job
//                   registry/control port was removed with per-session workers;
//                   the port name stays in the versioned set so an older frame
//                   is still recognised (and rejected by its port consumer)
//                   rather than silently misrouted. No producer exists in v2.
//   * `hub-proxy` — worker → hub authority forwarding (`worker/layer.ts`).
//
// The envelope is `{ v, port, kind, id?, payload? }`:
//   * `v`       versions the envelope itself (`1` = this JSON shape);
//   * `port`    selects the family, so a frame can never be misrouted silently;
//   * `kind`    discriminates within the family;
//   * `id`      correlates a request/response pair when the family needs it;
//   * `payload` is family-specific and left opaque to the envelope parser.
//
// Parsing happens once, at the boundary: `decodeWorkerFrame` accepts one JSON
// string and `parseWorkerFrame` decodes the envelope with Effect Schema. After
// that the value is trusted. Anything malformed — a non-string, invalid JSON, an
// unknown shape or version, or a message above 4 MiB — fails loud rather than
// being patched or truncated, so illegal states never enter internal logic.
//
// The events `event` variant carries the EventV2 event VERBATIM as
// `{ id, type, properties }`; there is deliberately no writer-style `data`
// translation. `generation` / `ordinal` are frame metadata (payload siblings of
// `event`), never fields on the event itself.

import { Option, Schema } from "effect"
import { decodeEventsFrameBody, type EventsFrameBody } from "@/util/events-frame-schema"
import { PrototypePollutionError, parseJson as parseSafeJson } from "@/util/safe-json"
import type { WriterOrdinal } from "@/session/egress/ordinal"

export const WorkerID = Schema.Int.pipe(Schema.brand("WorkerID"))
export type WorkerID = typeof WorkerID.Type
export const Generation = Schema.Int.pipe(Schema.brand("Generation"))
export type Generation = typeof Generation.Type
export const AggregateID = Schema.NonEmptyString.pipe(Schema.brand("AggregateID"))
export type AggregateID = typeof AggregateID.Type
export const CallID = Schema.NonEmptyString.pipe(Schema.brand("CallID"))
export type CallID = typeof CallID.Type

export const WORKER_FRAME_VERSION = 1
export type WorkerFrameVersion = typeof WORKER_FRAME_VERSION

export const WORKER_PROTOCOL_SCHEMA_VERSION = WORKER_FRAME_VERSION

export const MAX_WORKER_FRAME_BYTES = 4 * 1024 * 1024

export const WORKER_FRAME_PORTS = ["events", "writer", "job", "hub-proxy"] as const
export type WorkerFramePort = (typeof WORKER_FRAME_PORTS)[number]

export interface WorkerFrameEnvelope {
  readonly v: WorkerFrameVersion
  readonly port: WorkerFramePort
  readonly kind: string
  readonly id?: number
  readonly payload?: unknown
}


/**
 * An EventV2 event as it crosses the events port. The `{ id, type, properties }`
 * shape is preserved exactly — the frame never translates it into a writer event.
 */
export interface EventV2Event {
  readonly id: string
  readonly type: string
  readonly properties: unknown
}

export interface AggregatePosition {
  readonly aggregateID: string
  readonly seq: number
}

export interface EventsEventPayload {
  readonly event: EventV2Event
  readonly aggregateID?: string
  readonly seq?: number
  readonly generation?: number
  readonly ordinal?: number
}

export interface EventsResyncPayload {
  readonly pattern?: string
}

export interface EventsAckPayload {
  readonly count?: number
}

export const EVENT_BLOB_POSITION_KEY = "__eventBlobPosition"

export interface EventBlobPosition {
  readonly aggregateID: string
  readonly seq: number
}

export interface EventBlobReference {
  readonly __eventBlobPosition: EventBlobPosition
  readonly frameBytes: number
}

const EventBlobReferenceSchema = Schema.Struct({
  [EVENT_BLOB_POSITION_KEY]: Schema.Struct({
    aggregateID: Schema.String,
    seq: Schema.Number,
  }),
  frameBytes: Schema.Number,
})

const decodeEventBlobReference = Schema.decodeUnknownOption(EventBlobReferenceSchema)

export function isEventBlobReference(value: unknown): value is EventBlobReference {
  return Option.isSome(decodeEventBlobReference(value))
}

export type EventsFrame =
  | {
      readonly v: WorkerFrameVersion
      readonly port: "events"
      readonly kind: "event"
      readonly payload: EventsEventPayload
      readonly id?: number
    }
  | { readonly v: WorkerFrameVersion; readonly port: "events"; readonly kind: "end"; readonly id?: number }
  | {
      readonly v: WorkerFrameVersion
      readonly port: "events"
      readonly kind: "resync"
      readonly payload?: EventsResyncPayload
      readonly id?: number
    }
  | {
      readonly v: WorkerFrameVersion
      readonly port: "events"
      readonly kind: "ack"
      readonly payload?: EventsAckPayload
      readonly id?: number
    }

export function eventFrame(payload: EventsEventPayload, id?: number): EventsFrame {
  return { v: WORKER_FRAME_VERSION, port: "events", kind: "event", payload, ...correlation(id) }
}

export function endFrame(id?: number): EventsFrame {
  return { v: WORKER_FRAME_VERSION, port: "events", kind: "end", ...correlation(id) }
}

export function resyncFrame(payload?: EventsResyncPayload, id?: number): EventsFrame {
  return {
    v: WORKER_FRAME_VERSION,
    port: "events",
    kind: "resync",
    ...(payload === undefined ? {} : { payload }),
    ...correlation(id),
  }
}

export function ackFrame(payload?: EventsAckPayload, id?: number): EventsFrame {
  return {
    v: WORKER_FRAME_VERSION,
    port: "events",
    kind: "ack",
    ...(payload === undefined ? {} : { payload }),
    ...correlation(id),
  }
}


export class ProtocolError extends Schema.TaggedErrorClass<ProtocolError>("ProtocolError")(
  "WorkerFrame.Protocol",
  { message: Schema.String },
) {
  readonly tag = "PROTOCOL"
  constructor(message: string) {
    super({ message })
    this.name = "ProtocolError"
  }
}

export class UnsupportedFrameVersionError extends ProtocolError {
  constructor(readonly version: unknown) {
    super(`unsupported worker frame version: ${JSON.stringify(version)} (expected ${WORKER_FRAME_VERSION})`)
    Object.defineProperty(this, "_tag", { value: "WorkerFrame.UnsupportedVersion", enumerable: true, configurable: true })
    this.name = "UnsupportedFrameVersionError"
  }
}

export class FrameTooLargeError extends ProtocolError {
  constructor(
    readonly byteLength: number,
    readonly limit: number = MAX_WORKER_FRAME_BYTES,
  ) {
    super(`worker frame is ${byteLength} bytes (limit ${limit}); the frame was rejected, not truncated`)
    Object.defineProperty(this, "_tag", { value: "WorkerFrame.TooLarge", enumerable: true, configurable: true })
    this.name = "FrameTooLargeError"
  }
}


export function jsonErrorOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}


// A boundary that carries untrusted reply values and must reject silently-dropped functions or symbols calls {@link assertJsonRepresentable} explicitly before encoding.
export function encodeWorkerFrame(frame: WorkerFrameEnvelope): string {
  try {
    return JSON.stringify(frame)
  } catch (error) {
    try {
      assertJsonRepresentable(frame)
    } catch (rejection) {
      if (rejection instanceof ProtocolError) throw rejection
    }
    throw new ProtocolError(`frame is not JSON-serialisable (not structured-cloneable): ${jsonErrorOf(error)}`)
  }
}

/** Encode a frame and measure its UTF-8 size exactly once. */
export function encodeWorkerFrameSized(frame: WorkerFrameEnvelope): { readonly text: string; readonly bytes: number } {
  const text = encodeWorkerFrame(frame)
  return { text, bytes: workerFrameByteLength(text) }
}

export function assertJsonRepresentable(value: unknown): void {
  try {
    JSON.stringify(value, rejectNonJson)
  } catch (error) {
    if (error instanceof ProtocolError) throw error
    throw new ProtocolError(`frame is not JSON-serialisable (not structured-cloneable): ${jsonErrorOf(error)}`)
  }
}

function rejectNonJson(_key: string, value: unknown): unknown {
  if (typeof value === "function" || typeof value === "symbol" || typeof value === "bigint") {
    throw new ProtocolError(`frame is not JSON-serialisable (not structured-cloneable): encountered a ${typeof value}`)
  }
  return value
}

export function workerFrameByteLength(text: string): number {
  return Buffer.byteLength(text, "utf8")
}

/** Reject an over-bound message before it is posted or parsed. */
export function assertWorkerFrameSize(text: string, limit: number = MAX_WORKER_FRAME_BYTES): string {
  const byteLength = workerFrameByteLength(text)
  if (byteLength > limit) throw new FrameTooLargeError(byteLength, limit)
  return text
}

export function decodeWorkerFrame(text: unknown): WorkerFrameEnvelope {
  if (typeof text !== "string") {
    throw new ProtocolError(`worker frame must be a JSON string (got ${typeof text})`)
  }
  return parseWorkerFrame(parseJson(assertWorkerFrameSize(text)))
}

const WorkerFrameSchema = Schema.Struct({
  v: Schema.Literal(WORKER_FRAME_VERSION),
  port: Schema.Literals(WORKER_FRAME_PORTS),
  kind: Schema.NonEmptyString,
  id: Schema.optional(Schema.Int),
  payload: Schema.optional(Schema.Unknown),
})

const decodeWorkerFrameEnvelope = Schema.decodeUnknownSync(WorkerFrameSchema)

export function parseWorkerFrame(input: unknown): WorkerFrameEnvelope {
  if (typeof input !== "object" || input === null || Array.isArray(input)) {
    throw new ProtocolError(`frame must be an object (got ${JSON.stringify(input)})`)
  }
  const record = input as Record<string, unknown>
  if (record["v"] !== WORKER_FRAME_VERSION) throw new UnsupportedFrameVersionError(record["v"])

  let parsed: typeof WorkerFrameSchema.Type
  try {
    parsed = decodeWorkerFrameEnvelope(input)
  } catch (error) {
    throw new ProtocolError(`frame does not match the worker envelope: ${jsonErrorOf(error)}`)
  }

  return {
    v: WORKER_FRAME_VERSION,
    port: parsed.port,
    kind: parsed.kind,
    ...(parsed.id === undefined ? {} : { id: parsed.id }),
    ...(parsed.payload === undefined ? {} : { payload: parsed.payload }),
  }
}

export function parseEventsFrame(input: unknown): EventsFrame {
  const frame = parseWorkerFrame(input)
  if (frame.port !== "events") {
    throw new ProtocolError(`expected port "events" (got ${JSON.stringify(frame.port)})`)
  }

  const id = frame.id
  const body = parseEventsBody(frame)
  switch (body.kind) {
    case "event":
      return {
        v: WORKER_FRAME_VERSION,
        port: "events",
        kind: "event",
        payload: { ...body.payload, ...durableAggregateFieldsOf(frame.payload) },
        ...correlation(id),
      }
    case "end":
      return { v: WORKER_FRAME_VERSION, port: "events", kind: "end", ...correlation(id) }
    case "resync":
      return {
        v: WORKER_FRAME_VERSION,
        port: "events",
        kind: "resync",
        ...(body.payload === undefined ? {} : { payload: body.payload }),
        ...correlation(id),
      }
    case "ack":
      return {
        v: WORKER_FRAME_VERSION,
        port: "events",
        kind: "ack",
        ...(body.payload === undefined ? {} : { payload: body.payload }),
        ...correlation(id),
      }
  }
}

// The envelope is already trusted; a body that fails the Schema is still a typed protocol rejection, never a raw Schema error escaping the boundary.
function parseEventsBody(frame: WorkerFrameEnvelope): EventsFrameBody {
  try {
    return decodeEventsFrameBody({
      kind: frame.kind,
      ...(frame.payload === undefined ? {} : { payload: frame.payload }),
    })
  } catch (error) {
    throw new ProtocolError(`events frame body does not match the schema: ${jsonErrorOf(error)}`)
  }
}

// A non-durable delta carries neither, so this is the single durability test shared by the sender, the fan-out backpressure policy, and the receiver's commit-ordering check.
export function durableAggregateMetaOf(frame: EventsFrame): AggregatePosition | undefined {
  if (frame.kind !== "event") return undefined
  const { aggregateID, seq } = frame.payload
  if (aggregateID === undefined || seq === undefined) return undefined
  return { aggregateID, seq }
}

export function durableMetaOf(frame: EventsFrame): WriterOrdinal | undefined {
  if (frame.kind !== "event") return undefined
  const { generation, ordinal } = frame.payload
  if (generation === undefined || ordinal === undefined) return undefined
  return { generation, ordinal }
}

// Returns `undefined` for anything that cannot be resolved by position — a non-event frame, or a non-durable delta — so the caller keeps its typed `FrameTooLargeError` for those.
export function blobPositionReferenceFrame(
  frame: EventsFrame,
  frameBytes: number,
): EventsFrame | undefined {
  if (frame.kind !== "event") return undefined
  const position = durableAggregateMetaOf(frame)
  if (position === undefined) return undefined
  return eventFrame({
    ...frame.payload,
    event: {
      ...frame.payload.event,
      properties: {
        [EVENT_BLOB_POSITION_KEY]: { aggregateID: position.aggregateID, seq: position.seq },
        frameBytes,
      } satisfies EventBlobReference,
    },
  })
}

function correlation(id: number | undefined): { id?: number } {
  return id === undefined ? {} : { id }
}

function durableAggregateFieldsOf(value: unknown): { aggregateID?: string; seq?: number } {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return {}
  const record = value as Record<string, unknown>
  const aggregateID = parseOptionalAggregateID(record["aggregateID"])
  const seq = parseOptionalSeq(record["seq"])
  if ((aggregateID === undefined) !== (seq === undefined)) {
    throw new ProtocolError("durable event must carry both aggregateID and seq, or neither")
  }
  return {
    ...(aggregateID === undefined ? {} : { aggregateID }),
    ...(seq === undefined ? {} : { seq }),
  }
}

function parseOptionalAggregateID(value: unknown): string | undefined {
  if (value === undefined) return undefined
  const decoded = Schema.decodeUnknownOption(AggregateID)(value)
  if (Option.isNone(decoded)) {
    throw new ProtocolError(`field "aggregateID" must be a non-empty string (got ${JSON.stringify(value)})`)
  }
  return decoded.value
}

function parseOptionalSeq(value: unknown): number | undefined {
  if (value === undefined) return undefined
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    throw new ProtocolError(`field "seq" must be a non-negative integer (got ${JSON.stringify(value)})`)
  }
  return value
}

function parseJson(text: string): unknown {
  try {
    return parseSafeJson(text)
  } catch (error) {
    if (error instanceof PrototypePollutionError) {
      throw new ProtocolError(`frame JSON rejected at the boundary: ${jsonErrorOf(error)}`)
    }
    throw new ProtocolError(`frame is not valid JSON: ${jsonErrorOf(error)}`)
  }
}

export * as WorkerFrame from "./frame"
