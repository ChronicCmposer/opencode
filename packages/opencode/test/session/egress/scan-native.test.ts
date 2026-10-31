// The native scanner addon on its own contract: valid frames, every error code, and loading inside a Worker.
// Skipped (with a printed reason) when the addon is not built; `bun run script/build-egress-scan.ts` builds it.

import { afterAll, describe, expect, test } from "bun:test"
import { encodeEventsFrameBytes } from "@/session/egress/frame-bytes"
import { loadNativeEgressScan, type NativeScanResult } from "@/session/egress/native-scan"
import { activeDecoder, EventsFrameRejectedError, resetActiveDecoderForTest } from "@/session/egress/scan"
import { ackFrame, endFrame, eventFrame, FrameTooLargeError, resyncFrame } from "@/session/worker/frame"
import { LazyJson } from "@/util/lazy-json"
import { corpus } from "./scan-corpus"

const native = loadNativeEgressScan()
if (native === undefined) {
  console.log("scan-native: skipped, the addon is not built (run `bun run script/build-egress-scan.ts`)")
}

const encoder = new TextEncoder()

function frameOf(envelope: string, properties = ""): Uint8Array {
  const head = encoder.encode(envelope)
  const body = encoder.encode(properties)
  const bytes = new Uint8Array(4 + head.length + body.length)
  new DataView(bytes.buffer).setUint32(0, head.length, true)
  bytes.set(head, 4)
  bytes.set(body, 4 + head.length)
  return bytes
}

function rawFrame(envelope: Uint8Array, properties: Uint8Array): Uint8Array {
  const bytes = new Uint8Array(4 + envelope.length + properties.length)
  new DataView(bytes.buffer).setUint32(0, envelope.length, true)
  bytes.set(envelope, 4)
  bytes.set(properties, 4 + envelope.length)
  return bytes
}

const eventEnvelope = (extra = "") =>
  `{"v":1,"port":"events","kind":"event","payload":{"event":{"id":"evt_1","type":"t.x"}${extra}}}`

function scanOf(bytes: Uint8Array): NativeScanResult {
  return native!.scan(bytes)
}

function ok(result: NativeScanResult): Extract<NativeScanResult, { ok: true }> {
  if (!result.ok) throw new Error(`expected ok, got ${result.code}: ${result.message}`)
  return result
}

function rejected(bytes: Uint8Array): Extract<NativeScanResult, { ok: false }> {
  const result = scanOf(bytes)
  if (result.ok) throw new Error("expected a rejection")
  return result
}

describe.skipIf(native === undefined)("native egress scan", () => {
  test("scans an event frame built by the encoder", () => {
    const frame = eventFrame(
      {
        event: { id: "evt_1", type: "message.part.delta", properties: { sessionID: "s1", delta: "héllo 🌍" } },
        generation: 2,
        ordinal: 9,
        aggregateID: "agg_1",
        seq: 41,
      },
      7,
    )
    const bytes = new Uint8Array(encodeEventsFrameBytes(frame))
    const result = ok(scanOf(bytes))
    expect(result).toMatchObject({
      kind: "event",
      id: 7,
      eventID: "evt_1",
      eventType: "message.part.delta",
      generation: 2,
      ordinal: 9,
      aggregateID: "agg_1",
      seq: 41,
      isBlobReference: false,
      propertiesSessionID: "s1",
    })
    const text = new TextDecoder().decode(bytes.subarray(result.propertiesStart, result.propertiesEnd))
    expect(JSON.parse(text)).toEqual({ sessionID: "s1", delta: "héllo 🌍" })
    expect(result.propertiesEnd).toBe(bytes.length)
  })

  test("a non-durable delta has no aggregate fields", () => {
    const bytes = new Uint8Array(encodeEventsFrameBytes(eventFrame({ event: { id: "e", type: "t", properties: null } })))
    const result = ok(scanOf(bytes))
    expect(result.aggregateID).toBeUndefined()
    expect(result.seq).toBeUndefined()
    expect(result.id).toBeUndefined()
  })

  test("scans end, resync and ack frames", () => {
    expect(ok(scanOf(new Uint8Array(encodeEventsFrameBytes(endFrame(3)))))).toEqual({ ok: true, kind: "end", id: 3 })
    expect(ok(scanOf(new Uint8Array(encodeEventsFrameBytes(endFrame()))))).toEqual({ ok: true, kind: "end" })
    expect(ok(scanOf(new Uint8Array(encodeEventsFrameBytes(resyncFrame({ pattern: "session.*" })))))).toEqual({
      ok: true,
      kind: "resync",
      pattern: "session.*",
    })
    expect(ok(scanOf(frameOf('{"v":1,"port":"events","kind":"resync","payload":{"aggregateID":"a","seq":4,"inclusive":true}}')))).toEqual({
      ok: true,
      kind: "resync",
      aggregateID: "a",
      seq: 4,
      inclusive: true,
    })
    expect(ok(scanOf(new Uint8Array(encodeEventsFrameBytes(ackFrame({ count: 12 }, 5)))))).toEqual({
      ok: true,
      kind: "ack",
      id: 5,
      count: 12,
    })
  })

  test("decodes escapes and keeps lone surrogate escapes", () => {
    const result = ok(
      scanOf(
        frameOf(
          '{"v":1,"port":"events","kind":"event","payload":{"event":{"id":"a\\u0041\\n\\ud83c\\udf0d","type":"x\\ud800y\\"\\\\\\/"}}}',
          "null",
        ),
      ),
    )
    expect(result.eventID).toBe("aA\n🌍")
    expect(result.eventType).toBe('x\ud800y"\\/')
  })

  test("accepts multi-byte UTF-8 in envelope strings and escaped kind", () => {
    const result = ok(
      scanOf(frameOf('{"v":1,"port":"ev\\u0065nts","kind":"ev\\u0065nt","payload":{"event":{"id":"é€𝄞","type":"t"}}}', "1")),
    )
    expect(result.eventID).toBe("é€𝄞")
  })

  test("accepts last-wins duplicate envelope keys and ignores unknown keys", () => {
    const result = ok(
      scanOf(
        frameOf(
          '{"v":1,"extra":[1,{"a":null}],"port":"events","kind":"event","id":"x","id":4,"payload":{"event":{"id":"a","type":"b","properties":{"a":1}},"seq":1,"seq":2,"aggregateID":"g"}}',
          "{}",
        ),
      ),
    )
    expect(result.id).toBe(4)
    expect(result.seq).toBe(2)
  })

  test("number grammar: exponent and fraction integers are integers", () => {
    const result = ok(
      scanOf(frameOf(eventEnvelope().replace("}}}", '},"generation":1e2,"ordinal":3.0}}'), "{}")),
    )
    expect(result.generation).toBe(100)
    expect(result.ordinal).toBe(3)
  })

  test("captures top-level sessionID and aggregateID only", () => {
    const result = ok(
      scanOf(frameOf(eventEnvelope(), '{"nested":{"sessionID":"no"},"sessionID":"s_1","aggregateID":"a_1","x":[1,2,{"y":"z"}]}')),
    )
    expect(result.propertiesSessionID).toBe("s_1")
    expect(result.propertiesAggregateID).toBe("a_1")
    const nonString = ok(scanOf(frameOf(eventEnvelope(), '{"sessionID":"s","sessionID":5}')))
    expect(nonString.propertiesSessionID).toBeUndefined()
    const escaped = ok(scanOf(frameOf(eventEnvelope(), '{"\\u0073essionID":"s\\u0031"}')))
    expect(escaped.propertiesSessionID).toBe("s1")
  })

  test("detects blob references", () => {
    const reference = '{"__eventBlobPosition":{"aggregateID":"a","seq":3},"frameBytes":9000}'
    expect(ok(scanOf(frameOf(eventEnvelope(), reference))).isBlobReference).toBe(true)
    expect(ok(scanOf(frameOf(eventEnvelope(), '{"frameBytes":9000,"__eventBlobPosition":{"seq":3,"aggregateID":"a"}}'))).isBlobReference).toBe(true)
    for (const body of [
      '{"__eventBlobPosition":{"aggregateID":"a","seq":3}}',
      '{"__eventBlobPosition":{"aggregateID":1,"seq":3},"frameBytes":1}',
      '{"__eventBlobPosition":{"aggregateID":"a","seq":"3"},"frameBytes":1}',
      '{"__eventBlobPosition":{"aggregateID":"a","seq":3},"frameBytes":"1"}',
      '{"__eventBlobPosition":[],"frameBytes":1}',
      '[{"__eventBlobPosition":{"aggregateID":"a","seq":3},"frameBytes":1}]',
      '"x"',
    ]) {
      expect(ok(scanOf(frameOf(eventEnvelope(), body))).isBlobReference).toBe(false)
    }
  })

  test("accepts any valid JSON value as properties, with surrounding whitespace", () => {
    for (const body of ["null", "1", "-0.5e+3", '"s"', "[]", "[1,[2,[3]]]", ' \n{"a": [true, false, null] }\t ']) {
      expect(ok(scanOf(frameOf(eventEnvelope(), body))).kind).toBe("event")
    }
  })

  test("the properties range excludes the envelope", () => {
    const bytes = frameOf(eventEnvelope(), '{"a":1}')
    const result = ok(scanOf(bytes))
    expect(new TextDecoder().decode(bytes.subarray(result.propertiesStart, result.propertiesEnd))).toBe('{"a":1}')
  })

  test("a Uint8Array view with a byte offset is scanned in place", () => {
    const frame = frameOf(eventEnvelope(), '{"a":1}')
    const padded = new Uint8Array(frame.length + 10)
    padded.set(frame, 7)
    const result = ok(scanOf(padded.subarray(7, 7 + frame.length)))
    expect(result.eventID).toBe("evt_1")
    expect(result.propertiesEnd).toBe(frame.length)
  })

  test("accepts nesting at the depth cap and rejects past it", () => {
    const deep = (n: number) => "[".repeat(n) + "]".repeat(n)
    expect(ok(scanOf(frameOf(eventEnvelope(), deep(512)))).kind).toBe("event")
    expect(rejected(frameOf(eventEnvelope(), deep(513))).code).toBe("too-deep")
    const objects = (n: number) => '{"a":'.repeat(n) + "1" + "}".repeat(n)
    expect(ok(scanOf(frameOf(eventEnvelope(), objects(512)))).kind).toBe("event")
    expect(rejected(frameOf(eventEnvelope(), objects(513))).code).toBe("too-deep")
  })

  test("accepts a key at the cap and rejects one past it", () => {
    expect(ok(scanOf(frameOf(eventEnvelope(), `{"${"k".repeat(65536)}":1}`))).kind).toBe("event")
    expect(rejected(frameOf(eventEnvelope(), `{"${"k".repeat(65537)}":1}`)).code).toBe("key-too-long")
    expect(rejected(frameOf(eventEnvelope(), `{"a":{"${"k".repeat(65537)}":1}}`)).code).toBe("key-too-long")
  })

  test("too-large: more than 4 MiB in total", () => {
    const limit = 4 * 1024 * 1024
    const prefix = frameOf(eventEnvelope(), '"')
    const make = (total: number) => {
      const bytes = new Uint8Array(total)
      bytes.set(prefix)
      bytes.fill(0x61, prefix.length, total - 1)
      bytes[total - 1] = 0x22
      return bytes
    }
    expect(ok(scanOf(make(limit))).kind).toBe("event")
    const result = rejected(make(limit + 1))
    expect(result.code).toBe("too-large")
  })

  test("bad-header", () => {
    expect(rejected(new Uint8Array([1, 0, 0])).code).toBe("bad-header")
    expect(rejected(new Uint8Array(0)).code).toBe("bad-header")
    const lying = frameOf(eventEnvelope(), "{}")
    new DataView(lying.buffer).setUint32(0, lying.length, true)
    expect(rejected(lying).code).toBe("bad-header")
    new DataView(lying.buffer).setUint32(0, 0xffffffff, true)
    expect(rejected(lying).code).toBe("bad-header")
    expect(rejected(frameOf("", "")).code).toBe("invalid-json")
  })

  test("invalid-json: the strict grammar", () => {
    const bad = [
      "",
      " ",
      "{",
      "[1,]",
      "[,1]",
      '{"a":1,}',
      '{"a"}',
      "{a:1}",
      "'x'",
      "01",
      "-",
      "1.",
      ".5",
      "1e",
      "+1",
      "0x10",
      "NaN",
      "Infinity",
      "tru",
      "nulll",
      '"abc',
      '"a\nb"',
      '"a\tb"',
      '"\\x"',
      '"\\u12"',
      '"\\u12G4"',
      '"\\',
      "{} {}",
      "{} x",
      "1 2",
      "﻿{}",
      "// c\n{}",
      "{\"a\":1}\u0000",
    ]
    for (const body of bad) {
      const result = rejected(frameOf(eventEnvelope(), body))
      expect([body, result.code]).toEqual([body, "invalid-json"])
    }
  })

  test("invalid-json reports a byte offset into the frame", () => {
    const frame = frameOf(eventEnvelope(), '{"a":1,}')
    const result = rejected(frame)
    expect(frame[result.offset]).toBe(0x7d)
  })

  test("invalid-utf8 in properties and the envelope", () => {
    const head = encoder.encode(eventEnvelope())
    const body = (...bytes: number[]) => new Uint8Array([0x22, ...bytes, 0x22])
    const cases: Record<string, number[]> = {
      "lone continuation": [0x80],
      "overlong 2-byte": [0xc0, 0x80],
      "overlong c1": [0xc1, 0xbf],
      "overlong 3-byte": [0xe0, 0x80, 0x80],
      "overlong 4-byte": [0xf0, 0x80, 0x80, 0x80],
      "utf-8 encoded surrogate": [0xed, 0xa0, 0x80],
      "above U+10FFFF": [0xf4, 0x90, 0x80, 0x80],
      "f5 lead": [0xf5, 0x80, 0x80, 0x80],
      "ff byte": [0xff],
      "truncated 3-byte": [0xe2, 0x82],
      "bad continuation": [0xe2, 0x28, 0xa1],
    }
    for (const [name, bytes] of Object.entries(cases)) {
      expect([name, rejected(rawFrame(head, body(...bytes))).code]).toEqual([name, "invalid-utf8"])
      expect([name, rejected(rawFrame(head, new Uint8Array([0x7b, 0x22, ...bytes, 0x22, 0x3a, 0x31, 0x7d]))).code]).toEqual([
        name,
        "invalid-utf8",
      ])
    }
    const envelope = encoder.encode('{"v":1,"port":"events","kind":"end","x":"')
    const bad = new Uint8Array([...envelope, 0xed, 0xa0, 0x80, 0x22, 0x7d])
    expect(rejected(rawFrame(bad, new Uint8Array(0))).code).toBe("invalid-utf8")
  })

  test("valid UTF-8 boundaries are accepted", () => {
    const head = encoder.encode(eventEnvelope())
    const body = (...bytes: number[]) => rawFrame(head, new Uint8Array([0x22, ...bytes, 0x22]))
    for (const bytes of [
      [0xc2, 0x80],
      [0xdf, 0xbf],
      [0xe0, 0xa0, 0x80],
      [0xed, 0x9f, 0xbf],
      [0xee, 0x80, 0x80],
      [0xef, 0xbf, 0xbf],
      [0xf0, 0x90, 0x80, 0x80],
      [0xf4, 0x8f, 0xbf, 0xbf],
    ]) {
      expect(ok(scanOf(body(...bytes))).kind).toBe("event")
    }
  })

  test("lone surrogate escapes are valid JSON", () => {
    for (const body of ['"\\ud800"', '"\\udc00x"', '"\\ud800\\ud800"', '"\\uDBFF\\uDFFF"']) {
      expect(ok(scanOf(frameOf(eventEnvelope(), body))).kind).toBe("event")
    }
  })

  test("dangerous-key at any depth, including escaped spellings", () => {
    const bodies: Array<[string, string]> = [
      ['{"__proto__":{}}', "__proto__"],
      ['{"a":{"__proto__":1}}', "__proto__"],
      ['{"a":[{"b":{"constructor":{}}}]}', "constructor"],
      ['[{"prototype":1}]', "prototype"],
      ['{"__\\u0070roto__":1}', "__proto__"],
      ['{"\\u005f\\u005fproto\\u005f\\u005f":1}', "__proto__"],
      ['{"con\\u0073tructor":1}', "constructor"],
      ['{"__eventBlobPosition":{"prototype":1},"frameBytes":1}', "prototype"],
    ]
    for (const [body, key] of bodies) {
      const result = rejected(frameOf(eventEnvelope(), body))
      expect([body, result.code, result.key]).toEqual([body, "dangerous-key", key])
    }
    expect(ok(scanOf(frameOf(eventEnvelope(), '{"proto":1,"__proto":1,"constructors":1,"Prototype":1,"x":"__proto__"}'))).kind).toBe("event")
  })

  test("dangerous-key offset points at the key", () => {
    const frame = frameOf(eventEnvelope(), '{"a":1,"__proto__":2}')
    const result = rejected(frame)
    expect(new TextDecoder().decode(frame.subarray(result.offset, result.offset + 11))).toBe('"__proto__"')
  })

  test("bad-envelope: structural rules", () => {
    const ev = (inner: string) => `{"v":1,"port":"events","kind":"event","payload":${inner}}`
    const cases: Array<[string, string]> = [
      ["[]", ""],
      ['"x"', ""],
      ["1", ""],
      ['{"v":1,"kind":"end"}', ""],
      ['{"v":1,"port":"writer","kind":"end"}', ""],
      ['{"v":1,"port":"events"}', ""],
      ['{"v":1,"port":"events","kind":"nope"}', ""],
      ['{"v":1,"port":"events","kind":1}', ""],
      ['{"v":1,"port":"events","kind":"end","id":-1}', ""],
      ['{"v":1,"port":"events","kind":"end","id":1.5}', ""],
      ['{"v":1,"port":"events","kind":"end","id":"1"}', ""],
      ['{"v":1,"port":"events","kind":"end","payload":{}}', ""],
      ['{"v":1,"port":"events","kind":"end","payload":null}', ""],
      ['{"v":1,"port":"events","kind":"end"}', "{}"],
      ['{"v":1,"port":"events","kind":"ack","payload":{"count":-1}}', ""],
      ['{"v":1,"port":"events","kind":"ack","payload":{"count":1.5}}', ""],
      ['{"v":1,"port":"events","kind":"ack","payload":[]}', ""],
      ['{"v":1,"port":"events","kind":"ack","payload":null}', ""],
      ['{"v":1,"port":"events","kind":"resync","payload":{"pattern":1}}', ""],
      ['{"v":1,"port":"events","kind":"resync","payload":{"inclusive":"yes"}}', ""],
      ['{"v":1,"port":"events","kind":"resync","payload":{"seq":-2}}', ""],
      ['{"v":1,"port":"events","kind":"resync","payload":{"aggregateID":3}}', ""],
      [`{"v":1,"port":"events","kind":"event"}`, "{}"],
      [ev("null"), "{}"],
      [ev("[]"), "{}"],
      [ev("{}"), "{}"],
      [ev('{"event":null}'), "{}"],
      [ev('{"event":{"id":"a"}}'), "{}"],
      [ev('{"event":{"id":1,"type":"t"}}'), "{}"],
      [ev('{"event":{"id":"a","type":null}}'), "{}"],
      [ev('{"event":{"id":"a","type":"t"},"generation":1.5}'), "{}"],
      [ev('{"event":{"id":"a","type":"t"},"generation":"1"}'), "{}"],
      [ev('{"event":{"id":"a","type":"t"},"ordinal":null}'), "{}"],
      [ev('{"event":{"id":"a","type":"t"},"seq":-1,"aggregateID":"g"}'), "{}"],
      [ev('{"event":{"id":"a","type":"t"},"seq":1}'), "{}"],
      [ev('{"event":{"id":"a","type":"t"},"aggregateID":"g"}'), "{}"],
      [ev('{"event":{"id":"a","type":"t"},"seq":1,"aggregateID":""}'), "{}"],
      [ev('{"event":{"id":"a","type":"t"},"seq":1.5,"aggregateID":"g"}'), "{}"],
      [ev('{"event":{"id":"a","type":"t"},"seq":1e400,"aggregateID":"g"}'), "{}"],
      [ev('{"event":{"id":"a","type":"t"},"event":5}'), "{}"],
    ]
    for (const [envelope, properties] of cases) {
      const result = rejected(frameOf(envelope, properties))
      expect([envelope, properties, result.code]).toEqual([envelope, properties, "bad-envelope"])
    }
  })

  test("unsupported-version", () => {
    for (const envelope of [
      '{"v":2,"port":"events","kind":"end"}',
      '{"v":"1","port":"events","kind":"end"}',
      '{"port":"events","kind":"end"}',
      '{"v":null,"port":"events","kind":"end"}',
      '{"v":1.5,"port":"events","kind":"end"}',
    ]) {
      expect([envelope, rejected(frameOf(envelope)).code]).toEqual([envelope, "unsupported-version"])
    }
    expect(ok(scanOf(frameOf('{"v":1.0,"port":"events","kind":"end"}'))).kind).toBe("end")
  })

  test("an event frame with an empty properties segment is invalid-json", () => {
    expect(rejected(frameOf(eventEnvelope(), "")).code).toBe("invalid-json")
    expect(rejected(frameOf(eventEnvelope(), "  ")).code).toBe("invalid-json")
  })

  test("non-Uint8Array input throws a TypeError", () => {
    expect(() => native!.scan(new Uint16Array(4) as unknown as Uint8Array)).toThrow(TypeError)
    expect(() => native!.scan("x" as unknown as Uint8Array)).toThrow(TypeError)
  })

  test("loading the addon inside a Worker scans identically", async () => {
    const bytes = new Uint8Array(
      encodeEventsFrameBytes(
        eventFrame({ event: { id: "e1", type: "t.y", properties: { sessionID: "s9" } }, aggregateID: "g", seq: 1 }, 2),
      ),
    )
    const worker = new Worker(new URL("./scan-native.worker.ts", import.meta.url).href)
    try {
      const reply = await new Promise<unknown>((resolve, reject) => {
        worker.onmessage = (message) => resolve(message.data)
        worker.onerror = (error) => reject(error)
        worker.postMessage(bytes.buffer, [bytes.buffer.slice(0)])
      })
      expect(reply).toMatchObject({ ok: true, kind: "event", id: 2, eventID: "e1", propertiesSessionID: "s9", seq: 1 })
    } finally {
      worker.terminate()
    }
  })

  test("repeated scans of one frame stay valid", () => {
    const bytes = frameOf(eventEnvelope(), '{"sessionID":"s"}')
    for (let n = 0; n < 5000; n++) expect(scanOf(bytes).ok).toBe(true)
  })
})

describe.skipIf(native === undefined)("native decoder on the shared corpus", () => {
  afterAll(() => resetActiveDecoderForTest())

  for (const item of corpus) {
    test(item.name, () => {
      resetActiveDecoderForTest(native)
      const decoder = activeDecoder()
      expect(decoder.name).toBe("native")
      if ("ok" in item.expect) {
        const decoded = decoder.decode(item.bytes)
        const { propertiesText, ...expected } = item.expect.ok as { propertiesText?: string } & Record<string, unknown>
        if (decoded.kind !== "event") return expect(decoded).toEqual(expected as typeof decoded)
        const { event, ...rest } = decoded
        const { event: expectedEvent, ...expectedRest } = expected as { event: object } & Record<string, unknown>
        expect(rest).toEqual(expectedRest as typeof rest)
        expect({ id: event.id, type: event.type }).toEqual(expectedEvent as { id: string; type: string })
        expect(LazyJson.is(event.properties)).toBe(true)
        expect(event.properties.text).toBe(propertiesText!)
        return
      }
      const error = (() => {
        try {
          decoder.decode(item.bytes)
        } catch (error) {
          return error
        }
      })()
      if (item.expect.error === "FrameTooLargeError") return expect(error).toBeInstanceOf(FrameTooLargeError)
      expect(error).toBeInstanceOf(EventsFrameRejectedError)
      expect((error as EventsFrameRejectedError).code).toBe(item.expect.code!)
    })
  }
})
