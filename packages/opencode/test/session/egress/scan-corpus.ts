// Shared corpus for the events-frame decoders: the JS decoder runs all of it, and the native addon's tests
// reuse it so both reject and accept identical inputs with identical error classes.

import { encodeEventsFrameBytes } from "@/session/egress/frame-bytes"
import type { ScannedEventsFrame } from "@/session/egress/scan"
import type { NativeScanErrorCode } from "@/session/egress/native-scan"
import { ackFrame, endFrame, eventFrame, resyncFrame, MAX_WORKER_FRAME_BYTES, type EventsFrame } from "@/session/worker/frame"

/** The expected decode, with `properties` given as the exact text of the segment. */
export type CorpusOk = Omit<Extract<ScannedEventsFrame, { kind: "event" }>, "event"> & {
  readonly event: { readonly id: string; readonly type: string }
  readonly propertiesText: string
}
export type CorpusExpect =
  | { readonly ok: CorpusOk | Exclude<ScannedEventsFrame, { kind: "event" }> }
  | { readonly error: "FrameTooLargeError" }
  | { readonly error: "EventsFrameRejectedError"; readonly code: NativeScanErrorCode }

export interface CorpusCase {
  readonly name: string
  readonly bytes: Uint8Array
  readonly expect: CorpusExpect
}

const encoder = new TextEncoder()

export function frameBytes(frame: EventsFrame): Uint8Array {
  return new Uint8Array(encodeEventsFrameBytes(frame))
}

/** Build a frame by hand, so malformed envelopes and properties segments can be expressed. */
export function rawFrame(
  envelope: unknown,
  properties?: string | Uint8Array,
  envelopeLength?: number,
): Uint8Array {
  const envelopeBytes =
    envelope instanceof Uint8Array
      ? envelope
      : typeof envelope === "string"
        ? encoder.encode(envelope)
        : encoder.encode(JSON.stringify(envelope))
  const body = properties === undefined ? new Uint8Array(0) : typeof properties === "string" ? encoder.encode(properties) : properties
  const out = new Uint8Array(4 + envelopeBytes.byteLength + body.byteLength)
  new DataView(out.buffer).setUint32(0, envelopeLength ?? envelopeBytes.byteLength, true)
  out.set(envelopeBytes, 4)
  out.set(body, 4 + envelopeBytes.byteLength)
  return out
}

const eventEnvelope = (extra: Record<string, unknown> = {}, event: Record<string, unknown> = { id: "evt_1", type: "t" }) => ({
  v: 1,
  port: "events",
  kind: "event",
  payload: { event, ...extra },
})

const rejected = (code: NativeScanErrorCode): CorpusExpect => ({ error: "EventsFrameRejectedError", code })

function okEvent(
  frame: EventsFrame,
  properties: unknown,
  extra: Partial<CorpusOk> = {},
): { bytes: Uint8Array; expect: CorpusExpect } {
  if (frame.kind !== "event") throw new Error("okEvent needs an event frame")
  const { aggregateID, seq, generation, ordinal } = frame.payload
  return {
    bytes: frameBytes(frame),
    expect: {
      ok: {
        kind: "event",
        ...(frame.id === undefined ? {} : { id: frame.id }),
        event: { id: frame.payload.event.id, type: frame.payload.event.type },
        ...(generation === undefined ? {} : { generation }),
        ...(ordinal === undefined ? {} : { ordinal }),
        ...(aggregateID === undefined ? {} : { aggregateID }),
        ...(seq === undefined ? {} : { seq }),
        isBlobReference: false,
        propertiesText: JSON.stringify(properties),
        ...extra,
      },
    },
  }
}

const delta = { sessionID: "ses_abc", messageID: "msg_1", partID: "prt_1", field: "text", delta: "x".repeat(300) }

const nested = (depth: number, leaf: unknown): unknown => (depth === 0 ? leaf : { child: nested(depth - 1, leaf), tag: "n" })
const partUpdated = {
  sessionID: "ses_abc",
  part: {
    id: "prt_1",
    type: "tool",
    state: {
      status: "completed",
      input: { files: Array.from({ length: 40 }, (_, i) => ({ path: `/src/file${i}.ts`, lines: [i, i + 1, i + 2] })) },
      output: "o".repeat(1500),
      metadata: nested(20, { ok: true, none: null, n: -1.5e3 }),
    },
  },
}

const large = {
  sessionID: "ses_big",
  items: Array.from({ length: 6000 }, (_, i) => ({ i, k: `key${i}`, f: i % 2 === 0 })),
  text: "long ".repeat(40000),
}

const unicode = {
  sessionID: "ses_u",
  text: "héllo 日本語 🎉   \u0000 \"quoted\" \\ back\n\t",
  "ключ": ["😀", "é", "😀"],
}

const durable = eventFrame(
  { event: { id: "evt_d", type: "message.updated", properties: { sessionID: "ses_d", aggregateID: "agg_p" } }, aggregateID: "ses_d", seq: 42, generation: 3, ordinal: 7 },
  9,
)

const blob = { __eventBlobPosition: { aggregateID: "ses_d", seq: 12 }, frameBytes: 900000 }

const bigEnvelope = JSON.stringify(eventEnvelope())

const encodeCase = (name: string, build: { bytes: Uint8Array; expect: CorpusExpect }): CorpusCase => ({ name, ...build })

const deep = (n: number) => "[".repeat(n) + "]".repeat(n)

const malformed = (name: string, bytes: Uint8Array, code: NativeScanErrorCode): CorpusCase => ({
  name,
  bytes,
  expect: rejected(code),
})

export const corpus: readonly CorpusCase[] = [
  encodeCase("delta (~400 B)", okEvent(eventFrame({ event: { id: "evt_1", type: "message.part.delta", properties: delta } }), delta, { propertiesSessionID: "ses_abc" })),
  encodeCase("part.updated (~4 KB nested)", okEvent(eventFrame({ event: { id: "evt_2", type: "message.part.updated", properties: partUpdated } }, 3), partUpdated, { propertiesSessionID: "ses_abc" })),
  encodeCase("large (~480 KB)", okEvent(eventFrame({ event: { id: "evt_3", type: "big", properties: large } }), large, { propertiesSessionID: "ses_big" })),
  encodeCase("blob reference", okEvent(eventFrame({ event: { id: "evt_4", type: "message.updated", properties: blob } }), blob, { isBlobReference: true })),
  encodeCase("not a blob reference (wrong field types)", okEvent(eventFrame({ event: { id: "evt_4", type: "t", properties: { __eventBlobPosition: { aggregateID: 1, seq: 2 }, frameBytes: 3 } } }), { __eventBlobPosition: { aggregateID: 1, seq: 2 }, frameBytes: 3 })),
  encodeCase("durable event", okEvent(durable, { sessionID: "ses_d", aggregateID: "agg_p" }, { propertiesSessionID: "ses_d", propertiesAggregateID: "agg_p" })),
  encodeCase("unicode, emoji, escapes", okEvent(eventFrame({ event: { id: "evt_u", type: "text.é", properties: unicode } }), unicode, { propertiesSessionID: "ses_u" })),
  encodeCase("non-object properties (array)", okEvent(eventFrame({ event: { id: "e", type: "t", properties: [1, "a", null] } }), [1, "a", null])),
  encodeCase("null properties", okEvent(eventFrame({ event: { id: "e", type: "t", properties: undefined } }), null)),
  { name: "end", bytes: frameBytes(endFrame()), expect: { ok: { kind: "end" } } },
  { name: "end with id", bytes: frameBytes(endFrame(5)), expect: { ok: { kind: "end", id: 5 } } },
  { name: "resync bare", bytes: frameBytes(resyncFrame()), expect: { ok: { kind: "resync" } } },
  { name: "resync with pattern", bytes: frameBytes(resyncFrame({ pattern: "session.*" }, 2)), expect: { ok: { kind: "resync", id: 2, payload: { pattern: "session.*" } } } },
  {
    name: "resync with aggregateID, seq, inclusive",
    bytes: rawFrame({ v: 1, port: "events", kind: "resync", payload: { aggregateID: "ses_1", seq: 4, inclusive: true } }),
    expect: { ok: { kind: "resync", payload: { aggregateID: "ses_1", seq: 4, inclusive: true } } },
  },
  { name: "ack count", bytes: frameBytes(ackFrame({ count: 12 }, 1)), expect: { ok: { kind: "ack", id: 1, payload: { count: 12 } } } },
  { name: "ack bare", bytes: frameBytes(ackFrame()), expect: { ok: { kind: "ack" } } },
  { name: "properties at depth 512", bytes: rawFrame(eventEnvelope(), deep(512)), expect: { ok: { kind: "event", event: { id: "evt_1", type: "t" }, isBlobReference: false, propertiesText: deep(512) } } },
  { name: "key of 64 KiB", bytes: rawFrame(eventEnvelope(), JSON.stringify({ ["k".repeat(65536)]: 1 })), expect: { ok: { kind: "event", event: { id: "evt_1", type: "t" }, isBlobReference: false, propertiesText: JSON.stringify({ ["k".repeat(65536)]: 1 }) } } },
  { name: "lone surrogate escape (JSON.parse accepts)", bytes: rawFrame(eventEnvelope(), '{"s":"\\ud800"}'), expect: { ok: { kind: "event", event: { id: "evt_1", type: "t" }, isBlobReference: false, propertiesText: '{"s":"\\ud800"}' } } },
  { name: "\"constructor\" as a value, not a key", bytes: rawFrame(eventEnvelope(), '{"a":"__proto__"}'), expect: { ok: { kind: "event", event: { id: "evt_1", type: "t" }, isBlobReference: false, propertiesText: '{"a":"__proto__"}' } } },

  { name: "over 4 MiB", bytes: new Uint8Array(MAX_WORKER_FRAME_BYTES + 1), expect: { error: "FrameTooLargeError" } },
  { name: "over 4 MiB with valid prefix", bytes: rawFrame(eventEnvelope(), `"${"a".repeat(MAX_WORKER_FRAME_BYTES)}"`), expect: { error: "FrameTooLargeError" } },
  malformed("truncated header", new Uint8Array([1, 0]), "bad-header"),
  malformed("empty buffer", new Uint8Array(0), "bad-header"),
  malformed("envelopeLength > buffer", rawFrame(bigEnvelope, "null", 9999), "bad-header"),
  malformed("envelope not JSON", rawFrame("{nope", "null"), "invalid-json"),
  malformed("envelope not an object", rawFrame("[1]"), "bad-envelope"),
  malformed("envelope null", rawFrame("null"), "bad-envelope"),
  malformed("wrong v", rawFrame({ ...eventEnvelope(), v: 2 }, "null"), "unsupported-version"),
  malformed("missing v", rawFrame({ port: "events", kind: "end" }), "unsupported-version"),
  malformed("wrong port", rawFrame({ ...eventEnvelope(), port: "writer" }, "null"), "bad-envelope"),
  malformed("unknown kind", rawFrame({ v: 1, port: "events", kind: "bogus" }), "bad-envelope"),
  malformed("missing event.id", rawFrame(eventEnvelope({}, { type: "t" }), "null"), "bad-envelope"),
  malformed("missing event.type", rawFrame(eventEnvelope({}, { id: "e" }), "null"), "bad-envelope"),
  malformed("event payload missing", rawFrame({ v: 1, port: "events", kind: "event" }, "null"), "bad-envelope"),
  malformed("non-integer id", rawFrame({ v: 1, port: "events", kind: "end", id: 1.5 }), "bad-envelope"),
  malformed("negative id", rawFrame({ v: 1, port: "events", kind: "end", id: -1 }), "bad-envelope"),
  malformed("non-integer seq", rawFrame(eventEnvelope({ aggregateID: "a", seq: 1.5 }), "null"), "bad-envelope"),
  malformed("non-integer generation", rawFrame(eventEnvelope({ generation: "1" }), "null"), "bad-envelope"),
  malformed("aggregateID without seq", rawFrame(eventEnvelope({ aggregateID: "a" }), "null"), "bad-envelope"),
  malformed("seq without aggregateID", rawFrame(eventEnvelope({ seq: 1 }), "null"), "bad-envelope"),
  malformed("negative ack count", rawFrame({ v: 1, port: "events", kind: "ack", payload: { count: -1 } }), "bad-envelope"),
  malformed("resync pattern not a string", rawFrame({ v: 1, port: "events", kind: "resync", payload: { pattern: 1 } }), "bad-envelope"),
  malformed("invalid UTF-8 in properties", rawFrame(eventEnvelope(), new Uint8Array([0x22, 0xc3, 0x28, 0x22])), "invalid-utf8"),
  malformed("invalid UTF-8 in envelope", rawFrame(new Uint8Array([0x7b, 0xff, 0x7d])), "invalid-utf8"),
  malformed("trailing garbage after properties", rawFrame(eventEnvelope(), '{"a":1}x'), "invalid-json"),
  malformed("two properties values", rawFrame(eventEnvelope(), "1 2"), "invalid-json"),
  malformed("leading BOM in properties", rawFrame(eventEnvelope(), new Uint8Array([0xef, 0xbb, 0xbf, 0x7b, 0x7d])), "invalid-json"),
  malformed("empty properties segment on an event", rawFrame(eventEnvelope()), "invalid-json"),
  malformed("properties segment on a non-event frame", rawFrame({ v: 1, port: "events", kind: "end" }, "null"), "bad-envelope"),
  malformed("nesting depth 513", rawFrame(eventEnvelope(), deep(513)), "too-deep"),
  malformed("nesting depth 100000", rawFrame(eventEnvelope(), deep(100000)), "too-deep"),
  malformed("key longer than 64 KiB", rawFrame(eventEnvelope(), JSON.stringify({ ["k".repeat(65537)]: 1 })), "key-too-long"),
  malformed("__proto__ at depth 0", rawFrame(eventEnvelope(), '{"__proto__":{"x":1}}'), "dangerous-key"),
  malformed("constructor at depth 0", rawFrame(eventEnvelope(), '{"constructor":1}'), "dangerous-key"),
  malformed("prototype at depth 0", rawFrame(eventEnvelope(), '{"prototype":1}'), "dangerous-key"),
  malformed("__proto__ deep inside properties", rawFrame(eventEnvelope(), '{"a":[{"b":{"c":[1,{"__proto__":null}]}}]}'), "dangerous-key"),
  malformed("__proto__ via escape", rawFrame(eventEnvelope(), '{"__pro\\u0074o__":1}'), "dangerous-key"),
  malformed("__proto__ in the envelope", rawFrame('{"v":1,"port":"events","kind":"event","payload":{"event":{"id":"e","type":"t"}},"__proto__":{"x":1}}', "null"), "dangerous-key"),
  malformed("constructor inside the envelope payload", rawFrame('{"v":1,"port":"events","kind":"ack","payload":{"constructor":1}}'), "dangerous-key"),
]
