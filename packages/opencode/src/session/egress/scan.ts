// The events-frame decoder contract (D15). One interface, two implementations: the native scanner on linux
// arm64 (`native-scan.ts`) and the JS fallback below. Both accept and reject the same inputs with the same
// error classes; `properties` is never parsed here, only validated and handed on as `LazyJson`.

import { LazyJson } from "@/util/lazy-json"
import { FrameTooLargeError, MAX_WORKER_FRAME_BYTES, ProtocolError } from "@/session/worker/frame"
import { EVENTS_FRAME_HEADER_BYTES } from "./frame-bytes"
import { loadNativeEgressScan } from "./native-scan"

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

const decoder = new TextDecoder("utf-8", { fatal: true })

/** The JS fallback. Minimal on the contract commit; the validating implementation replaces it. */
export const jsDecoder: EventsFrameDecoder = {
  name: "js",
  decode: (bytes) => {
    if (bytes.byteLength > MAX_WORKER_FRAME_BYTES) throw new FrameTooLargeError(bytes.byteLength, MAX_WORKER_FRAME_BYTES)
    if (bytes.byteLength < EVENTS_FRAME_HEADER_BYTES) throw new EventsFrameRejectedError("truncated header", 0, "bad-header")
    const envelopeLength = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(0, true)
    const envelopeEnd = EVENTS_FRAME_HEADER_BYTES + envelopeLength
    if (envelopeEnd > bytes.byteLength) throw new EventsFrameRejectedError("envelope length exceeds frame", 0, "bad-header")
    const envelope = JSON.parse(decoder.decode(bytes.subarray(EVENTS_FRAME_HEADER_BYTES, envelopeEnd))) as Record<
      string,
      unknown
    >
    const kind = envelope["kind"]
    const id = typeof envelope["id"] === "number" ? envelope["id"] : undefined
    const payload = envelope["payload"] as Record<string, unknown> | undefined
    if (kind !== "event") {
      return { kind, ...(id === undefined ? {} : { id }), ...(payload === undefined ? {} : { payload }) } as ScannedEventsFrame
    }
    const event = payload!["event"] as { id: string; type: string }
    const properties = new LazyJson(bytes.subarray(envelopeEnd))
    const value = properties.value as Record<string, unknown> | null
    return {
      kind: "event",
      ...(id === undefined ? {} : { id }),
      event: { id: event.id, type: event.type, properties },
      ...pick(payload!, ["generation", "ordinal", "aggregateID", "seq"]),
      isBlobReference:
        typeof value === "object" && value !== null && "__eventBlobPosition" in value && "frameBytes" in value,
      ...(typeof value?.["sessionID"] === "string" ? { propertiesSessionID: value["sessionID"] } : {}),
      ...(typeof value?.["aggregateID"] === "string" ? { propertiesAggregateID: value["aggregateID"] } : {}),
    }
  },
}

function pick(record: Record<string, unknown>, keys: readonly string[]): Record<string, unknown> {
  return Object.fromEntries(keys.filter((key) => record[key] !== undefined).map((key) => [key, record[key]]))
}

let active: EventsFrameDecoder | undefined

/** The process-wide decoder: native when the addon loads (unless `OPENCODE_EGRESS_SCAN=js`), else the JS fallback. */
export function activeDecoder(): EventsFrameDecoder {
  if (active !== undefined) return active
  active = jsDecoder
  if (process.env["OPENCODE_EGRESS_SCAN"] !== "js") {
    const native = loadNativeEgressScan()
    void native
  }
  return active
}
