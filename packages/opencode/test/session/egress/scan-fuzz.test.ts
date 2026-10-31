// Differential fuzzing of the events-frame decoders against a reference built from JSON.parse and the contract rules.

import { describe, expect, test } from "bun:test"
import { EventsFrameRejectedError, jsDecoder, type EventsFrameDecoder, type ScannedEventsFrame } from "@/session/egress/scan"
import { loadNativeEgressScan, type NativeEgressScan, type NativeScanResult } from "@/session/egress/native-scan"
import { FrameTooLargeError, MAX_WORKER_FRAME_BYTES, ProtocolError } from "@/session/worker/frame"
import { LazyJson } from "@/util/lazy-json"
import { assembleFrame, DANGEROUS, makeRng, mutate, nested, randomFrameBytes, MAX_DEPTH, type Rng } from "../../lib/json-fuzz"

const MAX_KEY_BYTES = 65536
const ITERATIONS = Number(process.env["SCAN_FUZZ_ITERATIONS"] ?? 3000)
const SEED = Number(process.env["SCAN_FUZZ_SEED"] ?? 0x5ca17)
const strict = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true })
const encoder = new TextEncoder()

type Verdict = { readonly ok: true; readonly fields: Fields; readonly properties?: unknown } | { readonly ok: false; readonly tooLarge: boolean; readonly reason: string }
type Fields = Record<string, unknown>

class Reject extends Error {}
const reject = (reason: string): never => {
  throw new Reject(reason)
}

// Containers nested more than MAX_DEPTH deep, dangerous keys at any depth, and keys over the byte limit are rejected.
function checkValue(value: unknown, depth: number): void {
  if (typeof value !== "object" || value === null) return
  if (depth > MAX_DEPTH) reject("too-deep")
  if (Array.isArray(value)) return value.forEach((item) => checkValue(item, depth + 1))
  for (const key of Object.keys(value)) {
    if ((DANGEROUS as readonly string[]).includes(key)) reject("dangerous-key")
    if (encoder.encode(key).byteLength > MAX_KEY_BYTES) reject("key-too-long")
    checkValue((value as Record<string, unknown>)[key], depth + 1)
  }
}

function parseSegment(bytes: Uint8Array): unknown {
  let text: string
  try {
    text = strict.decode(bytes)
  } catch {
    return reject("invalid-utf8")
  }
  let value: unknown
  try {
    value = JSON.parse(text)
  } catch {
    return reject("invalid-json")
  }
  checkValue(value, 1)
  return value
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value)
const optional = (record: Record<string, unknown>, key: string, test: (v: unknown) => boolean) => {
  if (record[key] !== undefined && !test(record[key])) reject(`bad ${key}`)
}
const isInt = (v: unknown) => typeof v === "number" && Number.isInteger(v)
const isString = (v: unknown) => typeof v === "string"

function oracle(bytes: Uint8Array): Verdict {
  if (bytes.byteLength > MAX_WORKER_FRAME_BYTES) return { ok: false, tooLarge: true, reason: "too-large" }
  try {
    if (bytes.byteLength < 4) reject("bad-header")
    const length = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(0, true)
    const envelopeEnd = 4 + length
    if (envelopeEnd > bytes.byteLength) reject("bad-header")
    const envelope = parseSegment(bytes.subarray(4, envelopeEnd))
    if (!isRecord(envelope)) reject("bad-envelope")
    const e = envelope as Record<string, unknown>
    if (e["v"] !== 1 || e["port"] !== "events") reject("bad-envelope")
    optional(e, "id", isInt)
    const payload = e["payload"]
    const fields: Fields = { kind: e["kind"], id: e["id"] }
    switch (e["kind"]) {
      case "event": {
        if (!isRecord(payload) || !isRecord(payload["event"])) reject("bad-envelope")
        const p = payload as Record<string, unknown>
        const event = p["event"] as Record<string, unknown>
        if (!isString(event["id"]) || !isString(event["type"])) reject("bad-envelope")
        optional(p, "generation", isInt)
        optional(p, "ordinal", isInt)
        optional(p, "aggregateID", isString)
        optional(p, "seq", isInt)
        const properties = parseSegment(bytes.subarray(envelopeEnd))
        const rec = isRecord(properties) ? properties : undefined
        Object.assign(fields, {
          eventID: event["id"],
          eventType: event["type"],
          generation: p["generation"],
          ordinal: p["ordinal"],
          aggregateID: p["aggregateID"],
          seq: p["seq"],
          isBlobReference: rec !== undefined && "__eventBlobPosition" in rec && "frameBytes" in rec,
          propertiesSessionID: typeof rec?.["sessionID"] === "string" ? rec["sessionID"] : undefined,
          propertiesAggregateID: typeof rec?.["aggregateID"] === "string" ? rec["aggregateID"] : undefined,
        })
        return { ok: true, fields, properties }
      }
      case "end":
        if (payload !== undefined) reject("bad-envelope")
        break
      case "resync":
        if (payload !== undefined) {
          if (!isRecord(payload)) reject("bad-envelope")
          const p = payload as Record<string, unknown>
          optional(p, "pattern", isString)
          optional(p, "aggregateID", isString)
          optional(p, "seq", isInt)
          optional(p, "inclusive", (v) => typeof v === "boolean")
          Object.assign(fields, { pattern: p["pattern"], aggregateID: p["aggregateID"], seq: p["seq"], inclusive: p["inclusive"] })
        }
        break
      case "ack":
        if (payload !== undefined) {
          if (!isRecord(payload)) reject("bad-envelope")
          const p = payload as Record<string, unknown>
          optional(p, "count", (v) => isInt(v) && (v as number) >= 0)
          Object.assign(fields, { count: p["count"] })
        }
        break
      default:
        reject("bad-envelope")
    }
    if (bytes.byteLength !== envelopeEnd) reject("trailing-bytes")
    return { ok: true, fields }
  } catch (error) {
    if (error instanceof Reject) return { ok: false, tooLarge: false, reason: error.message }
    throw error
  }
}

// What a scanned frame exposes, flattened to the oracle's field names.
function fieldsOf(frame: ScannedEventsFrame): Fields {
  const base: Fields = { kind: frame.kind, id: frame.id }
  if (frame.kind === "event") {
    return {
      ...base,
      eventID: frame.event.id,
      eventType: frame.event.type,
      generation: frame.generation,
      ordinal: frame.ordinal,
      aggregateID: frame.aggregateID,
      seq: frame.seq,
      isBlobReference: frame.isBlobReference,
      propertiesSessionID: frame.propertiesSessionID,
      propertiesAggregateID: frame.propertiesAggregateID,
    }
  }
  if (frame.kind === "resync" && frame.payload !== undefined) return { ...base, ...frame.payload }
  if (frame.kind === "ack" && frame.payload !== undefined) return { ...base, ...frame.payload }
  return base
}

const defined = (fields: Fields) => Object.fromEntries(Object.entries(fields).filter(([, v]) => v !== undefined))

// The native path, built from the raw scan result the same way the production adapter must.
function nativeDecoder(scanner: NativeEgressScan): EventsFrameDecoder {
  return {
    name: "native",
    decode: (bytes) => {
      const r: NativeScanResult = scanner.scan(bytes)
      if (!r.ok) {
        if (r.code === "too-large") throw new FrameTooLargeError(bytes.byteLength, MAX_WORKER_FRAME_BYTES)
        throw new EventsFrameRejectedError(r.message, r.offset, r.code)
      }
      const id = r.id === undefined ? {} : { id: r.id }
      if (r.kind === "end") return { kind: "end", ...id }
      if (r.kind === "ack") return { kind: "ack", ...id, ...(r.count === undefined ? {} : { payload: { count: r.count } }) }
      if (r.kind === "resync") {
        const payload = defined({ pattern: r.pattern, aggregateID: r.aggregateID, seq: r.seq, inclusive: r.inclusive })
        return { kind: "resync", ...id, ...(Object.keys(payload).length === 0 ? {} : { payload }) }
      }
      return {
        kind: "event",
        ...id,
        event: { id: r.eventID!, type: r.eventType!, properties: new LazyJson(bytes.subarray(r.propertiesStart!, r.propertiesEnd!)) },
        ...defined({ generation: r.generation, ordinal: r.ordinal, aggregateID: r.aggregateID, seq: r.seq }),
        isBlobReference: r.isBlobReference === true,
        ...defined({ propertiesSessionID: r.propertiesSessionID, propertiesAggregateID: r.propertiesAggregateID }),
      }
    },
  }
}

type Outcome =
  | { readonly ok: true; readonly frame: ScannedEventsFrame }
  | { readonly ok: false; readonly cls: string; readonly code?: string }

function run(decoder: EventsFrameDecoder, bytes: Uint8Array): Outcome {
  try {
    return { ok: true, frame: decoder.decode(bytes) }
  } catch (error) {
    if (error instanceof FrameTooLargeError) return { ok: false, cls: "FrameTooLargeError" }
    if (error instanceof EventsFrameRejectedError) return { ok: false, cls: "EventsFrameRejectedError", code: error.code }
    if (error instanceof ProtocolError) return { ok: false, cls: "ProtocolError" }
    return { ok: false, cls: `unexpected:${error instanceof Error ? error.constructor.name : typeof error}` }
  }
}

function check(decoder: EventsFrameDecoder, bytes: Uint8Array, label: string): string | undefined {
  const verdict = oracle(bytes)
  const got = run(decoder, bytes)
  if (!verdict.ok) {
    if (got.ok) return `${label}: accepted a frame the oracle rejects (${verdict.reason})`
    const want = verdict.tooLarge ? "FrameTooLargeError" : "EventsFrameRejectedError"
    return got.cls === want ? undefined : `${label}: ${got.cls} for oracle ${verdict.reason}, want ${want}`
  }
  if (!got.ok) return `${label}: rejected a valid frame (${got.cls} ${got.code ?? ""})`
  const a = defined(fieldsOf(got.frame))
  const b = defined(verdict.fields)
  if (JSON.stringify(a, Object.keys(a).sort()) !== JSON.stringify(b, Object.keys(b).sort())) {
    return `${label}: fields differ ${JSON.stringify(a)} vs ${JSON.stringify(b)}`
  }
  if (got.frame.kind === "event") {
    const reparsed = JSON.parse(new TextDecoder().decode(got.frame.event.properties.raw))
    if (JSON.stringify(reparsed) !== JSON.stringify(verdict.properties)) return `${label}: properties differ`
  }
  return undefined
}

const native = loadNativeEgressScan()

function cases(rng: Rng, count: number): { readonly bytes: Uint8Array; readonly label: string; readonly mutated: boolean }[] {
  return Array.from({ length: count }, (_, i) => {
    const valid = randomFrameBytes(rng)
    if (i % 3 === 0) return { bytes: valid, label: `#${i} generated`, mutated: !oracle(valid).ok }
    const { bytes, applied } = mutate(rng, valid)
    return { bytes, label: `#${i} ${applied.join("+")}`, mutated: true }
  })
}

describe("events-frame decoder fuzz", () => {
  const corpus = cases(makeRng(SEED), ITERATIONS)

  test("generated frames the oracle accepts match the oracle in jsDecoder", () => {
    const failures = corpus.filter((c) => !c.mutated).map((c) => check(jsDecoder, c.bytes, c.label)).filter((m) => m !== undefined)
    expect(failures.slice(0, 5)).toEqual([])
  })

  test("mutated and over-limit frames match the oracle in jsDecoder", () => {
    const failures = corpus.filter((c) => c.mutated).map((c) => check(jsDecoder, c.bytes, c.label)).filter((m) => m !== undefined)
    if (failures.length > 0) console.log(`jsDecoder: ${failures.length}/${corpus.filter((c) => c.mutated).length} mutated cases differ`)
    expect(failures.slice(0, 5)).toEqual([])
  })

  test("boundary cases", () => {
    const envelope = '{"v":1,"port":"events","kind":"event","payload":{"event":{"id":"e","type":"t"}}}'
    const failures = [
      ["depth at cap", JSON.stringify(nested(MAX_DEPTH))],
      ["depth over cap", JSON.stringify(nested(MAX_DEPTH + 1))],
      ["key at cap", `{"${"k".repeat(MAX_KEY_BYTES)}":1}`],
      ["key over cap", `{"${"k".repeat(MAX_KEY_BYTES + 1)}":1}`],
      ["escaped proto", '{"a":{"\\u005f_proto__":1}}'],
      ["constructor", '{"constructor":{"prototype":{}}}'],
      ["duplicate keys", '{"a":1,"a":2}'],
      ["empty", ""],
      ["astral", '{"s":"😀𝄞\\ud83d\\ude00"}'],
      ["numbers", "[-0,1e5,1E-5,-1.5e+10,0]"],
      ["leading zero", "[01]"],
      ["bare control", '{"s":"a\tb"}'],
    ]
      .map(([label, props]) => check(jsDecoder, assembleFrame(envelope, props), label!))
      .filter((m) => m !== undefined)
    expect(failures).toEqual([])
  })

  test("oversized buffers raise FrameTooLargeError", () => {
    expect(run(jsDecoder, new Uint8Array(MAX_WORKER_FRAME_BYTES + 1))).toEqual({ ok: false, cls: "FrameTooLargeError" })
  })

  test.skipIf(native === undefined)("native adapter equals jsDecoder on every case", () => {
    const adapter = nativeDecoder(native!)
    const failures = corpus
      .map((c) => {
        const js = run(jsDecoder, c.bytes)
        const nat = run(adapter, c.bytes)
        if (js.ok !== nat.ok) return `${c.label}: js ${js.ok} native ${nat.ok}`
        if (!js.ok && !nat.ok) return js.cls === nat.cls && js.code === nat.code ? undefined : `${c.label}: ${js.cls}/${js.code} vs ${nat.cls}/${nat.code}`
        return check(adapter, c.bytes, c.label)
      })
      .filter((m) => m !== undefined)
    expect(failures.slice(0, 5)).toEqual([])
  })
})
