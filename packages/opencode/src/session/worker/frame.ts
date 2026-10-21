// Shared versioned frame envelope used across the worker ports (spec decisions
// Q8/Q21/Q22/Q24).
//
// Every worker `MessagePort` speaks one envelope shape, so transport, correlation
// and versioning live in exactly one place instead of drifting per protocol:
//
//   * `events`    — EventV2 delivery plus flow control. THIS module owns the
//                   `event` / `end` / `resync` / `ack` variants.
//   * `writer`    — the single-writer SQLite + file-KV RPC (`session/writer/protocol.ts`).
//   * `broker`    — the MCP/LSP child-process broker RPC (`session/broker/protocol.ts`).
//   * `job`       — the background-job control plane (`worker/job-client.ts`).
//   * `hub-proxy` — worker → hub authority forwarding (`worker/layer.ts`).
//
// The envelope is `{ v, port, kind, id?, payload? }`:
//   * `v`       versions the envelope itself (`1` = this JSON shape);
//   * `port`    selects the family, so a frame can never be misrouted silently;
//   * `kind`    discriminates within the family;
//   * `id`      correlates a request/response pair when the family needs it;
//   * `payload` is family-specific and left opaque to the envelope parser.
//
// Parsing happens once, at the boundary: `parseWorkerFrame` validates the
// envelope and `parseEventsFrame` validates the `events` variants. After that the
// value is trusted. Anything malformed — or carrying an unknown version — fails
// loud rather than being patched, so illegal states never enter internal logic.
//
// The events `event` variant carries the EventV2 event VERBATIM as
// `{ id, type, properties }`; there is deliberately no writer-style `data`
// translation. `generation` / `ordinal` are frame metadata (payload siblings of
// `event`), never fields on the event itself.

/** The only envelope version understood by this build. */
export const WORKER_FRAME_VERSION = 1
export type WorkerFrameVersion = typeof WORKER_FRAME_VERSION

/** The worker ports that share the envelope. */
export const WORKER_FRAME_PORTS = ["events", "writer", "broker", "job", "hub-proxy"] as const
export type WorkerFramePort = (typeof WORKER_FRAME_PORTS)[number]
const PORT_SET: ReadonlySet<string> = new Set(WORKER_FRAME_PORTS)

/** Narrow an untrusted value to a known worker port without an unsafe cast. */
function isWorkerFramePort(value: unknown): value is WorkerFramePort {
  return typeof value === "string" && PORT_SET.has(value)
}

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

/** `kind: "event"` payload: the event plus optional writer-stamped delivery metadata. */
export interface EventsEventPayload {
  readonly event: EventV2Event
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

// ---------------------------------------------------------------------------
// Codec + parsing
// ---------------------------------------------------------------------------

/** Serialise a frame to its JSON wire text. Inverse of `decodeWorkerFrame`. */
export function encodeWorkerFrame(frame: WorkerFrameEnvelope): string {
  return JSON.stringify(frame)
}

/** Parse wire text into a trusted envelope, failing loud on invalid JSON or shape. */
export function decodeWorkerFrame(text: string): WorkerFrameEnvelope {
  return parseWorkerFrame(parseJson(text))
}

/**
 * Parse an untrusted message into a trusted envelope, failing loud. The envelope
 * is the shared boundary: once a frame passes here, its `v`, `port`, `kind` and
 * optional `id` are trusted, and `payload` is left for the family parser.
 */
export function parseWorkerFrame(input: unknown): WorkerFrameEnvelope {
  if (!isRecord(input)) throw new ProtocolError(`frame must be an object (got ${JSON.stringify(input)})`)

  const v = input["v"]
  if (v !== WORKER_FRAME_VERSION) throw new UnsupportedFrameVersionError(v)

  const port = input["port"]
  if (!isWorkerFramePort(port)) {
    throw new ProtocolError(
      `field "port" must be one of ${WORKER_FRAME_PORTS.join(", ")} (got ${JSON.stringify(port)})`,
    )
  }

  const kind = input["kind"]
  if (typeof kind !== "string" || kind.length === 0) {
    throw new ProtocolError(`field "kind" must be a non-empty string (got ${JSON.stringify(kind)})`)
  }

  const id = parseOptionalId(input["id"])
  const payload = input["payload"]
  return {
    v: WORKER_FRAME_VERSION,
    port,
    kind,
    ...(id === undefined ? {} : { id }),
    ...(payload === undefined ? {} : { payload }),
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
  switch (frame.kind) {
    case "event":
      return {
        v: WORKER_FRAME_VERSION,
        port: "events",
        kind: "event",
        payload: parseEventsEventPayload(frame.payload),
        ...correlation(id),
      }
    case "end":
      if (frame.payload !== undefined) throw new ProtocolError(`frame "end" must not carry a payload`)
      return { v: WORKER_FRAME_VERSION, port: "events", kind: "end", ...correlation(id) }
    case "resync":
      return {
        v: WORKER_FRAME_VERSION,
        port: "events",
        kind: "resync",
        ...(frame.payload === undefined ? {} : { payload: parseResyncPayload(frame.payload) }),
        ...correlation(id),
      }
    case "ack":
      return {
        v: WORKER_FRAME_VERSION,
        port: "events",
        kind: "ack",
        ...(frame.payload === undefined ? {} : { payload: parseAckPayload(frame.payload) }),
        ...correlation(id),
      }
    default:
      throw new ProtocolError(`unknown events frame kind ${JSON.stringify(frame.kind)}`)
  }
}

/** Spread `{ id }` only when present, so absent correlation stays absent. */
function correlation(id: number | undefined): { id?: number } {
  return id === undefined ? {} : { id }
}

function parseEventsEventPayload(value: unknown): EventsEventPayload {
  if (!isRecord(value)) {
    throw new ProtocolError(`events "event" payload must be an object (got ${JSON.stringify(value)})`)
  }
  const event = parseEventV2(value["event"])
  const generation = parseOptionalInteger(value["generation"], "generation")
  const ordinal = parseOptionalInteger(value["ordinal"], "ordinal")
  return {
    event,
    ...(generation === undefined ? {} : { generation }),
    ...(ordinal === undefined ? {} : { ordinal }),
  }
}

/**
 * Validate the verbatim EventV2 event `{ id, type, properties }`. The
 * `properties` value is passed through by reference — no `data` translation.
 */
function parseEventV2(value: unknown): EventV2Event {
  if (!isRecord(value)) throw new ProtocolError(`event must be an object (got ${JSON.stringify(value)})`)

  const id = value["id"]
  if (typeof id !== "string") throw new ProtocolError(`field "event.id" must be a string (got ${JSON.stringify(id)})`)

  const type = value["type"]
  if (typeof type !== "string") {
    throw new ProtocolError(`field "event.type" must be a string (got ${JSON.stringify(type)})`)
  }

  if (!("properties" in value)) {
    throw new ProtocolError(`field "event.properties" is missing (EventV2 events carry { id, type, properties })`)
  }

  return { id, type, properties: value["properties"] }
}

function parseResyncPayload(value: unknown): EventsResyncPayload {
  if (!isRecord(value)) {
    throw new ProtocolError(`events "resync" payload must be an object (got ${JSON.stringify(value)})`)
  }
  const pattern = value["pattern"]
  if (pattern !== undefined && typeof pattern !== "string") {
    throw new ProtocolError(`field "pattern" must be a string (got ${JSON.stringify(pattern)})`)
  }
  return pattern === undefined ? {} : { pattern }
}

function parseAckPayload(value: unknown): EventsAckPayload {
  if (!isRecord(value)) {
    throw new ProtocolError(`events "ack" payload must be an object (got ${JSON.stringify(value)})`)
  }
  const count = value["count"]
  if (count !== undefined && (typeof count !== "number" || !Number.isInteger(count) || count < 0)) {
    throw new ProtocolError(`field "count" must be a non-negative integer (got ${JSON.stringify(count)})`)
  }
  return count === undefined ? {} : { count }
}

function parseOptionalId(value: unknown): number | undefined {
  if (value === undefined) return undefined
  if (typeof value !== "number" || !Number.isInteger(value)) {
    throw new ProtocolError(`field "id" must be an integer (got ${JSON.stringify(value)})`)
  }
  return value
}

function parseOptionalInteger(value: unknown, field: string): number | undefined {
  if (value === undefined) return undefined
  if (typeof value !== "number" || !Number.isInteger(value)) {
    throw new ProtocolError(`field "${field}" must be an integer (got ${JSON.stringify(value)})`)
  }
  return value
}

function parseJson(text: string): unknown {
  try {
    return JSON.parse(text)
  } catch (error) {
    throw new ProtocolError(`frame is not valid JSON: ${error instanceof Error ? error.message : String(error)}`)
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  // Arrays are objects but never valid records here, so exclude them explicitly.
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

export * as WorkerFrame from "./frame"
