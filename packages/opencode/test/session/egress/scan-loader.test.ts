import { afterEach, beforeEach, describe, expect, spyOn, test } from "bun:test"
import * as nativeScan from "@/session/egress/native-scan"
import type { NativeEgressScan, NativeScanResult } from "@/session/egress/native-scan"
import { activeDecoder, EventsFrameRejectedError, jsDecoder, resetActiveDecoderForTest } from "@/session/egress/scan"
import { FrameTooLargeError } from "@/session/worker/frame"
import { frameBytes, rawFrame } from "./scan-corpus"
import { eventFrame } from "@/session/worker/frame"

const fake = (result: NativeScanResult): NativeEgressScan => ({ scan: () => result })
const saved = process.env["OPENCODE_EGRESS_SCAN"]

beforeEach(() => {
  delete process.env["OPENCODE_EGRESS_SCAN"]
  resetActiveDecoderForTest()
})
afterEach(() => {
  if (saved === undefined) delete process.env["OPENCODE_EGRESS_SCAN"]
  else process.env["OPENCODE_EGRESS_SCAN"] = saved
  resetActiveDecoderForTest()
})

describe("activeDecoder", () => {
  test("no addon selects js", () => {
    const spy = spyOn(nativeScan, "loadNativeEgressScan").mockReturnValue(undefined)
    try {
      expect(activeDecoder()).toBe(jsDecoder)
      expect(spy).toHaveBeenCalledTimes(1)
    } finally {
      spy.mockRestore()
    }
  })

  test("the selection is cached", () => {
    expect(activeDecoder()).toBe(activeDecoder())
  })

  test("an injected native scanner is wrapped and maps an event result", () => {
    const bytes = frameBytes(eventFrame({ event: { id: "e", type: "t", properties: { a: 1 } } }))
    const text = '{"a":1}'
    const start = bytes.byteLength - text.length
    resetActiveDecoderForTest(
      fake({
        ok: true,
        kind: "event",
        id: 4,
        eventID: "e",
        eventType: "t",
        aggregateID: "agg",
        seq: 2,
        generation: 1,
        ordinal: 3,
        propertiesStart: start,
        propertiesEnd: bytes.byteLength,
        isBlobReference: false,
        propertiesSessionID: "ses",
      }),
    )
    const decoder = activeDecoder()
    expect(decoder.name).toBe("native")
    const frame = decoder.decode(bytes)
    if (frame.kind !== "event") throw new Error("expected event")
    expect(frame).toMatchObject({ id: 4, aggregateID: "agg", seq: 2, generation: 1, ordinal: 3, isBlobReference: false, propertiesSessionID: "ses" })
    expect(frame.event.id).toBe("e")
    expect(frame.event.properties.text).toBe(text)
    expect(frame.propertiesAggregateID).toBeUndefined()
  })

  test("maps end, resync, and ack results", () => {
    const bytes = new Uint8Array(8)
    resetActiveDecoderForTest(fake({ ok: true, kind: "end" }))
    expect(activeDecoder().decode(bytes)).toEqual({ kind: "end" })
    resetActiveDecoderForTest(fake({ ok: true, kind: "resync", id: 1, pattern: "p" }))
    expect(activeDecoder().decode(bytes)).toEqual({ kind: "resync", id: 1, payload: { pattern: "p" } })
    resetActiveDecoderForTest(fake({ ok: true, kind: "ack", count: 3 }))
    expect(activeDecoder().decode(bytes)).toEqual({ kind: "ack", payload: { count: 3 } })
  })

  test("maps error results to the shared error classes", () => {
    const bytes = rawFrame({ v: 1, port: "events", kind: "end" })
    resetActiveDecoderForTest(fake({ ok: false, code: "too-large", offset: 0, message: "big" }))
    expect(() => activeDecoder().decode(bytes)).toThrow(FrameTooLargeError)
    resetActiveDecoderForTest(fake({ ok: false, code: "dangerous-key", offset: 17, message: "bad key", key: "__proto__" }))
    const error = (() => {
      try {
        activeDecoder().decode(bytes)
      } catch (error) {
        return error
      }
    })()
    expect(error).toBeInstanceOf(EventsFrameRejectedError)
    expect(error).toMatchObject({ code: "dangerous-key", offset: 17 })
  })

  test("OPENCODE_EGRESS_SCAN=js forces js even when the addon loads", () => {
    process.env["OPENCODE_EGRESS_SCAN"] = "js"
    const spy = spyOn(nativeScan, "loadNativeEgressScan").mockReturnValue(fake({ ok: true, kind: "end" }))
    try {
      expect(activeDecoder()).toBe(jsDecoder)
      expect(spy).not.toHaveBeenCalled()
    } finally {
      spy.mockRestore()
    }
  })

  test("a loader that finds an addon selects native", () => {
    const spy = spyOn(nativeScan, "loadNativeEgressScan").mockReturnValue(fake({ ok: true, kind: "end" }))
    try {
      expect(activeDecoder().name).toBe("native")
    } finally {
      spy.mockRestore()
    }
  })

  test("a throwing loader falls back to js", () => {
    const spy = spyOn(nativeScan, "loadNativeEgressScan").mockImplementation(() => {
      throw new Error("dlopen failed")
    })
    try {
      expect(activeDecoder()).toBe(jsDecoder)
    } finally {
      spy.mockRestore()
    }
  })
})
