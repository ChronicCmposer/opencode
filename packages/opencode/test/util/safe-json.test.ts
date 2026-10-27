// Every untrusted JSON payload crosses one guard.
//
// `JSON.parse` keeps `__proto__` as an own property, so any later spread or
// `Object.assign` turns it into the prototype setter. The boundary rejects the
// dangerous key names instead of trying to sanitise every downstream merge.

import { describe, expect, test } from "bun:test"
import { DANGEROUS_KEYS, PrototypePollutionError, parseJson } from "@/util/safe-json"

describe("util.safe-json", () => {
  test("parses ordinary JSON unchanged", () => {
    const value = parseJson('{"a":1,"nested":{"b":[true,null,"x"]}}')
    expect(value).toEqual({ a: 1, nested: { b: [true, null, "x"] } })
  })

  test.each(DANGEROUS_KEYS)("rejects a top-level %s key", (key) => {
    expect(() => parseJson(`{"${key}":{"polluted":true}}`)).toThrow(PrototypePollutionError)
  })

  test("rejects a nested dangerous key", () => {
    expect(() => parseJson('{"outer":{"__proto__":{"polluted":true}}}')).toThrow(PrototypePollutionError)
  })

  test("rejects a dangerous key inside an array element", () => {
    expect(() => parseJson('[{"constructor":{"prototype":{"polluted":true}}}]')).toThrow(PrototypePollutionError)
  })

  test("never touches Object.prototype", () => {
    expect(() => parseJson('{"__proto__":{"polluted":true}}')).toThrow(PrototypePollutionError)
    expect(({} as Record<string, unknown>).polluted).toBeUndefined()
  })

  test("reports the offending key and path", () => {
    try {
      parseJson('{"outer":[{"prototype":1}]}')
      throw new Error("expected a rejection")
    } catch (error) {
      expect(error).toBeInstanceOf(PrototypePollutionError)
      expect((error as Error).message).toContain("prototype")
      expect((error as Error).message).toContain("$.outer[0]")
    }
  })

  test("rejects invalid JSON with a typed error", () => {
    expect(() => parseJson("{")).toThrow(PrototypePollutionError)
  })
})
