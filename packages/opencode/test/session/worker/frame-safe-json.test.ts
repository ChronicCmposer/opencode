// The JSON boundary on the worker frame codec refuses prototype-reaching keys.
//
// `JSON.parse` keeps `__proto__` as an own property, so a later spread turns it
// into the prototype setter. The frame boundary is the one place untrusted wire
// text is decoded, so it rejects the dangerous names there before any family
// parser sees the payload.

import { describe, expect, test } from "bun:test"
import { ProtocolError, WORKER_FRAME_VERSION, decodeWorkerFrame } from "@/session/worker/frame"

const envelope = (payload: unknown, kind = "ack") =>
  JSON.stringify({ v: WORKER_FRAME_VERSION, port: "events", kind, payload })

describe("worker frame JSON boundary", () => {
  test("rejects a top-level __proto__ key inside a payload", () => {
    expect(() => decodeWorkerFrame(envelope(JSON.parse('{"__proto__":{}}')))).toThrow(ProtocolError)
    expect(() => decodeWorkerFrame('{"v":1,"port":"events","kind":"ack","payload":{"__proto__":{}}}')).toThrow(
      ProtocolError,
    )
  })

  test("rejects constructor/prototype keys at any depth", () => {
    expect(() =>
      decodeWorkerFrame(
        '{"v":1,"port":"events","kind":"event","payload":{"event":{"id":"e","type":"t","properties":{"constructor":{"prototype":{"polluted":true}}}}}}',
      ),
    ).toThrow(ProtocolError)
    expect(() => decodeWorkerFrame(envelope({ nested: [{ prototype: 1 }] }))).toThrow(ProtocolError)
  })

  test("never touches Object.prototype", () => {
    expect(() => decodeWorkerFrame(envelope(JSON.parse('{"__proto__":{"polluted":true}}')))).toThrow(ProtocolError)
    expect(({} as Record<string, unknown>).polluted).toBeUndefined()
  })

  test("still decodes an ordinary frame and reports invalid JSON as ProtocolError", () => {
    expect(decodeWorkerFrame(envelope({ count: 2 }))).toEqual({
      v: 1,
      port: "events",
      kind: "ack",
      payload: { count: 2 },
    })
    expect(() => decodeWorkerFrame("{not json")).toThrow(ProtocolError)
  })
})
