// The events-frame decoder contract (D15). One interface, two implementations: the native scanner on linux
// arm64 (`native-scan.ts`) and the JS fallback below. Both accept and reject the same inputs with the same
// error classes; `properties` is never parsed here, only validated and handed on as `LazyJson`.

import { Effect } from "effect"
import { LazyJson } from "@/util/lazy-json"
import { FrameTooLargeError, MAX_WORKER_FRAME_BYTES, ProtocolError, WORKER_FRAME_VERSION } from "@/session/worker/frame"
import { workerLogMessage } from "@/session/worker/worker-logging"
import { EVENTS_FRAME_HEADER_BYTES } from "./frame-bytes"
import { loadNativeEgressScan, type NativeEgressScan, type NativeScanResult } from "./native-scan"

export type ScannedEventsFrame =
  | {
      readonly kind: "event"
      readonly id?: number
      readonly event: { readonly id: string; readonly type: string; readonly properties: LazyJson }
      readonly generation?: number
      readonly ordinal?: number
      readonly aggregateID?: string
      readonly seq?: number
      /** `properties` is a `{ __eventBlobPosition, frameBytes }` reference, resolved by position on the hub. */
      readonly isBlobReference: boolean
      /** Top-level `properties.sessionID` / `properties.aggregateID`, when they are strings. */
      readonly propertiesSessionID?: string
      readonly propertiesAggregateID?: string
    }
  | { readonly kind: "end"; readonly id?: number }
  | {
      readonly kind: "resync"
      readonly id?: number
      readonly payload?: {
        readonly pattern?: string
        readonly aggregateID?: string
        readonly seq?: number
        readonly inclusive?: boolean
      }
    }
  | { readonly kind: "ack"; readonly id?: number; readonly payload?: { readonly count?: number } }

/** A rejected frame names the byte offset where the decoder stopped. */
export class EventsFrameRejectedError extends ProtocolError {
  constructor(
    message: string,
    readonly offset: number,
    readonly code: string,
  ) {
    super(`${message} (at byte ${offset})`)
    this.name = "EventsFrameRejectedError"
  }
}

export interface EventsFrameDecoder {
  readonly name: "native" | "js"
  /** Decode one events byte frame; throws `FrameTooLargeError`, `EventsFrameRejectedError`, or `ProtocolError`. */
  readonly decode: (bytes: Uint8Array) => ScannedEventsFrame
}

const MAX_DEPTH = 512
const MAX_KEY_BYTES = 65536
const DANGEROUS_KEYS = new Set(["__proto__", "constructor", "prototype"])
const KINDS = new Set(["event", "end", "resync", "ack"])

// Fatal so invalid UTF-8 throws; ignoreBOM so a leading BOM reaches JSON.parse and is rejected there.
const utf8 = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true })

function reject(message: string, offset: number, code: string): never {
  throw new EventsFrameRejectedError(message, offset, code)
}

function decodeText(bytes: Uint8Array, offset: number): string {
  try {
    return utf8.decode(bytes)
  } catch {
    return reject("invalid UTF-8", offset, "invalid-utf8")
  }
}

function parseJson(text: string, offset: number): unknown {
  try {
    return JSON.parse(text)
  } catch (error) {
    if (error instanceof RangeError) return reject("nesting too deep", offset, "too-deep")
    return reject(`invalid JSON: ${error instanceof Error ? error.message : String(error)}`, offset, "invalid-json")
  }
}

/** Iterative walk: depth cap, key length cap, and no `__proto__` / `constructor` / `prototype` key anywhere. */
function checkValue(root: unknown, offset: number): void {
  if (typeof root !== "object" || root === null) return
  const stack: unknown[] = [root]
  const depths: number[] = [1]
  while (stack.length > 0) {
    const value = stack.pop()
    const depth = depths.pop()!
    if (depth > MAX_DEPTH) reject("nesting too deep", offset, "too-deep")
    if (Array.isArray(value)) {
      for (const item of value) {
        if (typeof item === "object" && item !== null) {
          stack.push(item)
          depths.push(depth + 1)
        }
      }
      continue
    }
    const record = value as Record<string, unknown>
    for (const key in record) {
      if (!Object.hasOwn(record, key)) continue
      if (DANGEROUS_KEYS.has(key)) throw dangerousKey(key, offset)
      if (key.length * 3 > MAX_KEY_BYTES && Buffer.byteLength(key) > MAX_KEY_BYTES) {
        reject("object key too long", offset, "key-too-long")
      }
      const item = record[key]
      if (typeof item === "object" && item !== null) {
        stack.push(item)
        depths.push(depth + 1)
      }
    }
  }
}

function dangerousKey(key: string, offset: number) {
  return new EventsFrameRejectedError(`dangerous key "${key}"`, offset, "dangerous-key")
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value)
const isInt = (value: unknown): value is number => typeof value === "number" && Number.isInteger(value)
const isCount = (value: unknown): value is number => isInt(value) && value >= 0

function optional<T>(record: Record<string, unknown>, key: string, ok: (value: unknown) => value is T): T | undefined {
  const value = record[key]
  if (value === undefined) return undefined
  if (!ok(value)) return reject(`envelope field "${key}" is malformed`, EVENTS_FRAME_HEADER_BYTES, "bad-envelope")
  return value
}

const isString = (value: unknown): value is string => typeof value === "string"
const isBoolean = (value: unknown): value is boolean => typeof value === "boolean"

function defined<T extends Record<string, unknown>>(fields: T): Partial<T> {
  return Object.fromEntries(Object.entries(fields).filter(([, value]) => value !== undefined)) as Partial<T>
}

function isBlobReference(value: unknown): boolean {
  if (!isRecord(value)) return false
  const position = value["__eventBlobPosition"]
  return (
    isRecord(position) &&
    typeof position["aggregateID"] === "string" &&
    typeof position["seq"] === "number" &&
    typeof value["frameBytes"] === "number"
  )
}

export const jsDecoder: EventsFrameDecoder = {
  name: "js",
  decode: (bytes) => {
    if (bytes.byteLength > MAX_WORKER_FRAME_BYTES) throw new FrameTooLargeError(bytes.byteLength, MAX_WORKER_FRAME_BYTES)
    if (bytes.byteLength < EVENTS_FRAME_HEADER_BYTES) reject("truncated header", 0, "bad-header")
    const envelopeLength = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(0, true)
    const envelopeEnd = EVENTS_FRAME_HEADER_BYTES + envelopeLength
    if (envelopeEnd > bytes.byteLength) reject("envelope length exceeds frame", 0, "bad-header")

    const envelope = parseJson(
      decodeText(bytes.subarray(EVENTS_FRAME_HEADER_BYTES, envelopeEnd), EVENTS_FRAME_HEADER_BYTES),
      EVENTS_FRAME_HEADER_BYTES,
    )
    checkValue(envelope, EVENTS_FRAME_HEADER_BYTES)
    if (!isRecord(envelope)) reject("envelope is not an object", EVENTS_FRAME_HEADER_BYTES, "bad-envelope")
    if (envelope["v"] !== WORKER_FRAME_VERSION) reject("unsupported frame version", EVENTS_FRAME_HEADER_BYTES, "unsupported-version")
    if (envelope["port"] !== "events") reject("not an events frame", EVENTS_FRAME_HEADER_BYTES, "bad-envelope")
    const kind = envelope["kind"]
    if (typeof kind !== "string" || !KINDS.has(kind)) reject("unknown frame kind", EVENTS_FRAME_HEADER_BYTES, "bad-envelope")
    const id = optional(envelope, "id", isCount)
    const idField = id === undefined ? {} : { id }
    const payload = envelope["payload"]
    if (payload !== undefined && !isRecord(payload)) reject("payload is not an object", EVENTS_FRAME_HEADER_BYTES, "bad-envelope")

    if (kind !== "event") {
      if (envelopeEnd !== bytes.byteLength) reject("properties segment on a non-event frame", envelopeEnd, "bad-envelope")
      if (kind === "end") return { kind: "end", ...idField }
      if (payload === undefined) return { kind, ...idField } as ScannedEventsFrame
      if (kind === "ack") {
        const count = optional(payload, "count", isCount)
        return { kind: "ack", ...idField, ...(count === undefined ? {} : { payload: { count } }) }
      }
      const fields = defined({
        pattern: optional(payload, "pattern", isString),
        aggregateID: optional(payload, "aggregateID", isString),
        seq: optional(payload, "seq", isInt),
        inclusive: optional(payload, "inclusive", isBoolean),
      })
      return { kind: "resync", ...idField, ...(Object.keys(fields).length === 0 ? {} : { payload: fields }) }
    }

    if (payload === undefined) reject("event frame has no payload", EVENTS_FRAME_HEADER_BYTES, "bad-envelope")
    const event = payload["event"]
    if (!isRecord(event) || typeof event["id"] !== "string" || typeof event["type"] !== "string") {
      reject("event is malformed", EVENTS_FRAME_HEADER_BYTES, "bad-envelope")
    }
    const generation = optional(payload, "generation", isInt)
    const ordinal = optional(payload, "ordinal", isInt)
    const aggregateID = optional(payload, "aggregateID", isString)
    const seq = optional(payload, "seq", isInt)
    if ((aggregateID === undefined) !== (seq === undefined)) {
      reject("aggregateID and seq must appear together", EVENTS_FRAME_HEADER_BYTES, "bad-envelope")
    }

    const segment = bytes.subarray(envelopeEnd)
    const properties = parseJson(decodeText(segment, envelopeEnd), envelopeEnd)
    checkValue(properties, envelopeEnd)
    const record = isRecord(properties) ? properties : undefined
    const sessionID = record?.["sessionID"]
    const propertiesAggregateID = record?.["aggregateID"]
    return {
      kind: "event",
      ...idField,
      event: { id: event["id"], type: event["type"], properties: new LazyJson(segment) },
      ...defined({ generation, ordinal, aggregateID, seq }),
      isBlobReference: isBlobReference(properties),
      ...(typeof sessionID === "string" ? { propertiesSessionID: sessionID } : {}),
      ...(typeof propertiesAggregateID === "string" ? { propertiesAggregateID } : {}),
    }
  },
}

function nativeDecoder(native: NativeEgressScan): EventsFrameDecoder {
  return {
    name: "native",
    decode: (bytes) => {
      const result = native.scan(bytes)
      if (!result.ok) {
        if (result.code === "too-large") throw new FrameTooLargeError(bytes.byteLength, MAX_WORKER_FRAME_BYTES)
        throw new EventsFrameRejectedError(result.message, result.offset, result.code)
      }
      return scanned(result, bytes)
    },
  }
}

function scanned(result: Extract<NativeScanResult, { ok: true }>, bytes: Uint8Array): ScannedEventsFrame {
  const idField = result.id === undefined ? {} : { id: result.id }
  if (result.kind === "end") return { kind: "end", ...idField }
  if (result.kind === "ack") {
    return { kind: "ack", ...idField, ...(result.count === undefined ? {} : { payload: { count: result.count } }) }
  }
  if (result.kind === "resync") {
    const fields = defined({
      pattern: result.pattern,
      aggregateID: result.aggregateID,
      seq: result.seq,
      inclusive: result.inclusive,
    })
    return { kind: "resync", ...idField, ...(Object.keys(fields).length === 0 ? {} : { payload: fields }) }
  }
  return {
    kind: "event",
    ...idField,
    event: {
      id: result.eventID ?? "",
      type: result.eventType ?? "",
      properties: new LazyJson(bytes.subarray(result.propertiesStart ?? bytes.byteLength, result.propertiesEnd ?? bytes.byteLength)),
    },
    ...defined({ generation: result.generation, ordinal: result.ordinal, aggregateID: result.aggregateID, seq: result.seq }),
    isBlobReference: result.isBlobReference === true,
    ...defined({
      propertiesSessionID: result.propertiesSessionID,
      propertiesAggregateID: result.propertiesAggregateID,
    }),
  }
}

let active: EventsFrameDecoder | undefined

function select(native: NativeEgressScan | undefined): EventsFrameDecoder {
  const chosen = native === undefined ? jsDecoder : nativeDecoder(native)
  Effect.runSync(Effect.logInfo(workerLogMessage("egress decoder"), { decoder: chosen.name }))
  return chosen
}

/** The process-wide decoder: native when the addon loads (unless `OPENCODE_EGRESS_SCAN=js`), else the JS fallback. */
export function activeDecoder(): EventsFrameDecoder {
  if (active !== undefined) return active
  active = select(process.env["OPENCODE_EGRESS_SCAN"] === "js" ? undefined : loadNative())
  return active
}

function loadNative(): NativeEgressScan | undefined {
  try {
    return loadNativeEgressScan()
  } catch {
    return undefined
  }
}

/** Test seam: drop the cached decoder; with `native`, select it instead of calling the loader on the next use. */
export function resetActiveDecoderForTest(native?: NativeEgressScan): void {
  active = native === undefined ? undefined : select(native)
}
