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

import { Schema } from "effect"
import { decodeEventsFrameBody, type EventsFrameBody } from "@/util/events-frame-schema"
import { PrototypePollutionError, parseJson as parseSafeJson } from "@/util/safe-json"
import type { WriterOrdinal } from "@/session/egress/ordinal"

/** The only envelope version understood by this build. */
export const WORKER_FRAME_VERSION = 1
export type WorkerFrameVersion = typeof WORKER_FRAME_VERSION

/** The protocol schema version carried by the ready-handshake build hash. */
export const WORKER_PROTOCOL_SCHEMA_VERSION = WORKER_FRAME_VERSION

/** Upper bound on one JSON message crossing a worker port (4 MiB). */
export const MAX_WORKER_FRAME_BYTES = 4 * 1024 * 1024

/** The worker ports that share the envelope. */
export const WORKER_FRAME_PORTS = ["events", "writer", "job", "hub-proxy"] as const
export type WorkerFramePort = (typeof WORKER_FRAME_PORTS)[number]

/** The shared envelope every worker port transports. */
export interface WorkerFrameEnvelope {
  readonly v: WorkerFrameVersion
  readonly port: WorkerFramePort
  readonly kind: string
  readonly id?: number
  readonly payload?: unknown
}

// ---------------------------------------------------------------------------
// Events-port variants
// ---------------------------------------------------------------------------

/**
 * An EventV2 event as it crosses the events port. The `{ id, type, properties }`
 * shape is preserved exactly — the frame never translates it into a writer event.
 */
export interface EventV2Event {
  readonly id: string
  readonly type: string
  readonly properties: unknown
}

/** The durable position of an event within one aggregate. */
export interface AggregatePosition {
  readonly aggregateID: string
  /** Monotonic sequence within `aggregateID`. */
  readonly seq: number
}

/** `kind: "event"` payload: the event plus optional writer-stamped delivery metadata. */
export interface EventsEventPayload {
  readonly event: EventV2Event
  /** Aggregate the durable event belongs to; frame metadata, not an event field. */
  readonly aggregateID?: string
  /** Per-aggregate durable sequence; frame metadata, not an event field. */
  readonly seq?: number
  /** Writer generation for durable ordinals; frame metadata, not an event field. */
  readonly generation?: number
  /** Writer ordinal for durable events; frame metadata, not an event field. */
  readonly ordinal?: number
}

/** `kind: "resync"` payload: the subscription pattern that overflowed, if scoped. */
export interface EventsResyncPayload {
  readonly pattern?: string
}

/** `kind: "ack"` payload: how many deliveries/resyncs were acknowledged. */
export interface EventsAckPayload {
  readonly count?: number
}

/**
 * The four events-port frames: an EventV2 `event`, the end-of-stream `end`, a
 * backpressure `resync`, and an `ack`.
 */
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

/** Build an `event` frame; `id` is optional correlation. */
export function eventFrame(payload: EventsEventPayload, id?: number): EventsFrame {
  return { v: WORKER_FRAME_VERSION, port: "events", kind: "event", payload, ...correlation(id) }
}

/** Build an `end` frame; `id` is optional correlation. */
export function endFrame(id?: number): EventsFrame {
  return { v: WORKER_FRAME_VERSION, port: "events", kind: "end", ...correlation(id) }
}

/** Build a `resync` frame; both the pattern and `id` are optional. */
export function resyncFrame(payload?: EventsResyncPayload, id?: number): EventsFrame {
  return {
    v: WORKER_FRAME_VERSION,
    port: "events",
    kind: "resync",
    ...(payload === undefined ? {} : { payload }),
    ...correlation(id),
  }
}

/** Build an `ack` frame; both the count and `id` are optional. */
export function ackFrame(payload?: EventsAckPayload, id?: number): EventsFrame {
  return {
    v: WORKER_FRAME_VERSION,
    port: "events",
    kind: "ack",
    ...(payload === undefined ? {} : { payload }),
    ...correlation(id),
  }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/** Raised when a frame does not match the protocol; a caller bug, so fail loud. */
export class ProtocolError extends Error {
  readonly tag = "PROTOCOL"
  constructor(message: string) {
    super(message)
    this.name = "ProtocolError"
  }
}

/** Raised specifically when `v` is not the version this build understands. */
export class UnsupportedFrameVersionError extends ProtocolError {
  constructor(readonly version: unknown) {
    super(`unsupported worker frame version: ${JSON.stringify(version)} (expected ${WORKER_FRAME_VERSION})`)
    this.name = "UnsupportedFrameVersionError"
  }
}

/** Raised when one JSON message exceeds {@link MAX_WORKER_FRAME_BYTES}. */
export class FrameTooLargeError extends ProtocolError {
  readonly _tag = "FrameTooLargeError"

  constructor(
    readonly byteLength: number,
    readonly limit: number = MAX_WORKER_FRAME_BYTES,
  ) {
    super(`worker frame is ${byteLength} bytes (limit ${limit}); the frame was rejected, not truncated`)
    this.name = "FrameTooLargeError"
  }
}

// ---------------------------------------------------------------------------
// Shared primitive parsers
// ---------------------------------------------------------------------------

/**
 * A protocol's failure factory: build and throw that module's own protocol error.
 * Binding the shared parsers to it lets every framed protocol (writer,
 * events) keep its own error taxonomy while sharing one implementation.
 */
export type ParseFail = (message: string) => never

/** The failure factory used by the default parser exports: this module's `ProtocolError`. */
const frameFail: ParseFail = (message) => {
  throw new ProtocolError(message)
}

/** True for a plain object; `null` and arrays are excluded. */
export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

/** Parse a required string field; fails loud through `fail` (frame `ProtocolError` by default). */
export function requireString(record: Record<string, unknown>, field: string, fail: ParseFail = frameFail): string {
  const value = record[field]
  if (typeof value !== "string") fail(`field "${field}" must be a string (got ${JSON.stringify(value)})`)
  return value
}

/** Parse a required finite-number field. */
export function requireNumber(record: Record<string, unknown>, field: string, fail: ParseFail = frameFail): number {
  const value = record[field]
  if (typeof value !== "number" || !Number.isFinite(value)) {
    fail(`field "${field}" must be a finite number (got ${JSON.stringify(value)})`)
  }
  return value
}

/** Parse a required integer field. */
export function requireInteger(record: Record<string, unknown>, field: string, fail: ParseFail = frameFail): number {
  const value = record[field]
  if (typeof value !== "number" || !Number.isInteger(value)) {
    fail(`field "${field}" must be an integer (got ${JSON.stringify(value)})`)
  }
  return value
}

/** Parse an optional string field; `undefined` stays absent. */
export function optionalString(
  record: Record<string, unknown>,
  field: string,
  fail: ParseFail = frameFail,
): string | undefined {
  const value = record[field]
  if (value === undefined) return undefined
  if (typeof value !== "string") fail(`field "${field}" must be a string (got ${JSON.stringify(value)})`)
  return value
}

/** Parse an optional boolean field; `undefined` stays absent. */
export function optionalBoolean(
  record: Record<string, unknown>,
  field: string,
  fail: ParseFail = frameFail,
): boolean | undefined {
  const value = record[field]
  if (value === undefined) return undefined
  if (typeof value !== "boolean") fail(`field "${field}" must be a boolean (got ${JSON.stringify(value)})`)
  return value
}

/** The total primitive parsers shared by every framed protocol. */
export interface FramePrimitives {
  /** True for a plain object; `null` and arrays are excluded. */
  isRecord(value: unknown): value is Record<string, unknown>
  requireString(record: Record<string, unknown>, field: string): string
  requireNumber(record: Record<string, unknown>, field: string): number
  requireInteger(record: Record<string, unknown>, field: string): number
  optionalString(record: Record<string, unknown>, field: string): string | undefined
  optionalBoolean(record: Record<string, unknown>, field: string): boolean | undefined
}

/**
 * Bind the shared primitive parsers to a protocol's own failure type, so a
 * module can parse with its own error while reusing the
 * implementation. Once bound, each parser takes only `(record, field)`.
 */
export function createFramePrimitives(fail: ParseFail): FramePrimitives {
  return {
    isRecord,
    requireString: (record, field) => requireString(record, field, fail),
    requireNumber: (record, field) => requireNumber(record, field, fail),
    requireInteger: (record, field) => requireInteger(record, field, fail),
    optionalString: (record, field) => optionalString(record, field, fail),
    optionalBoolean: (record, field) => optionalBoolean(record, field, fail),
  }
}

/** Coerce a thrown JSON-parse error to a message for a protocol error. */
export function jsonErrorOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

// ---------------------------------------------------------------------------
// Codec + parsing
// ---------------------------------------------------------------------------

/**
 * Serialise a frame to its JSON wire text. Inverse of `decodeWorkerFrame`.
 * `strict` (the default) walks the payload with a replacer so a function or
 * symbol is rejected instead of silently dropped. The events hot path opts out
 * and uses plain `JSON.stringify`; boundaries that carry untrusted reply values
 * call {@link assertJsonRepresentable} explicitly.
 */
export function encodeWorkerFrame(frame: WorkerFrameEnvelope, options: { readonly strict?: boolean } = {}): string {
  try {
    return options.strict === false ? JSON.stringify(frame) : JSON.stringify(frame, rejectNonJson)
  } catch (error) {
    if (error instanceof ProtocolError) throw error
    throw new ProtocolError(`frame is not JSON-serialisable (not structured-cloneable): ${jsonErrorOf(error)}`)
  }
}

/**
 * Assert a value survives JSON encoding without silently dropping anything
 * (functions, symbols) — used on untrusted reply payloads, not the frame hot
 * path. Bigints throw from `JSON.stringify` itself.
 */
export function assertJsonRepresentable(value: unknown): void {
  try {
    JSON.stringify(value, rejectNonJson)
  } catch (error) {
    if (error instanceof ProtocolError) throw error
    throw new ProtocolError(`frame is not JSON-serialisable (not structured-cloneable): ${jsonErrorOf(error)}`)
  }
}

/** JSON has no representation for these; fail instead of silently dropping them. */
function rejectNonJson(_key: string, value: unknown): unknown {
  if (typeof value === "function" || typeof value === "symbol" || typeof value === "bigint") {
    throw new ProtocolError(`frame is not JSON-serialisable (not structured-cloneable): encountered a ${typeof value}`)
  }
  return value
}

/** The exact UTF-8 size of one encoded worker frame (no encoder allocation). */
export function workerFrameByteLength(text: string): number {
  return Buffer.byteLength(text, "utf8")
}

/** Reject an over-bound message before it is posted or parsed. */
export function assertWorkerFrameSize(text: string, limit: number = MAX_WORKER_FRAME_BYTES): string {
  const byteLength = workerFrameByteLength(text)
  if (byteLength > limit) throw new FrameTooLargeError(byteLength, limit)
  return text
}

/**
 * Parse one wire message into a trusted envelope, failing loud on invalid JSON,
 * a non-string message, a malformed shape, or an over-bound payload.
 */
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

/** Decoder built once at module load, not per frame. */
const decodeWorkerFrameEnvelope = Schema.decodeUnknownSync(WorkerFrameSchema)

/**
 * Decode an untrusted message once with Effect Schema. The envelope is the shared
 * boundary: after this decode its `v`, `port`, `kind` and optional `id` are
 * trusted, and `payload` is left for the family parser.
 */
export function parseWorkerFrame(input: unknown): WorkerFrameEnvelope {
  if (!isRecord(input)) throw new ProtocolError(`frame must be an object (got ${JSON.stringify(input)})`)
  if (input["v"] !== WORKER_FRAME_VERSION) throw new UnsupportedFrameVersionError(input["v"])

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

/**
 * Parse an `events`-port frame, validating the `kind` discriminant and the
 * verbatim EventV2 shape. A frame from another port is a routing bug, so it fails
 * loud rather than being reinterpreted.
 */
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

/**
 * Decode the `{ kind, payload }` family once with the shared events Schema. The
 * envelope is already trusted; a body that fails the Schema is still a typed
 * protocol rejection, never a raw Schema error escaping the boundary.
 */
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

/**
 * The writer-stamped durable metadata on an events `event` frame: present only
 * when BOTH `generation` and `ordinal` are set. A non-durable delta carries
 * neither, so this is the single durability test shared by the sender, the
 * fan-out backpressure policy, and the receiver's commit-ordering check.
 */
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

/** Spread `{ id }` only when present, so absent correlation stays absent. */
function correlation(id: number | undefined): { id?: number } {
  return id === undefined ? {} : { id }
}

/**
 * The transport durability metadata carried beside the event. The body Schema
 * owns `event` / `generation` / `ordinal`; `(aggregateID, seq)` is the wire
 * position and is validated here as one pair. A half-specified pair would make a
 * frame "almost durable", so it is rejected rather than dropped.
 */
function durableAggregateFieldsOf(value: unknown): { aggregateID?: string; seq?: number } {
  if (!isRecord(value)) return {}
  const aggregateID = parseOptionalAggregateID(value["aggregateID"])
  const seq = parseOptionalSeq(value["seq"])
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
  if (typeof value !== "string" || value.length === 0) {
    throw new ProtocolError(`field "aggregateID" must be a non-empty string (got ${JSON.stringify(value)})`)
  }
  return value
}

function parseOptionalSeq(value: unknown): number | undefined {
  if (value === undefined) return undefined
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    throw new ProtocolError(`field "seq" must be a non-negative integer (got ${JSON.stringify(value)})`)
  }
  return value
}

/**
 * Decode untrusted wire text through the shared boundary guard. It rejects
 * prototype-reaching keys at any depth and translates invalid JSON to one typed
 * error; both become this module's `ProtocolError` so callers see one taxonomy.
 */
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
