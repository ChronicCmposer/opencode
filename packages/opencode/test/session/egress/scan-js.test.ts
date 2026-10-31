import { describe, expect, test } from "bun:test"
import { EventsFrameRejectedError, jsDecoder } from "@/session/egress/scan"
import { FrameTooLargeError } from "@/session/worker/frame"
import { LazyJson } from "@/util/lazy-json"
import { corpus } from "./scan-corpus"

describe("jsDecoder", () => {
  for (const item of corpus) {
    test(item.name, () => {
      if ("ok" in item.expect) {
        const decoded = jsDecoder.decode(item.bytes)
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
          jsDecoder.decode(item.bytes)
        } catch (error) {
          return error
        }
      })()
      if (item.expect.error === "FrameTooLargeError") return expect(error).toBeInstanceOf(FrameTooLargeError)
      expect(error).toBeInstanceOf(EventsFrameRejectedError)
      expect((error as EventsFrameRejectedError).code).toBe(item.expect.code!)
    })
  }

  test("dangerous-key error names the key", () => {
    const item = corpus.find((c) => c.name === "constructor at depth 0")!
    expect(() => jsDecoder.decode(item.bytes)).toThrow(/constructor/)
  })
})
