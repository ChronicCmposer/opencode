// Phase 7.2 / 7.3 — writer RPC protocol tests (plan tasks 7.2, 7.3).
//
// The frame parser is the trust boundary: a malformed frame is a caller bug and
// must fail loud, while a valid frame must be parsed into a fully trusted shape
// with optional fields defaulted.

import { describe, expect, test } from "bun:test"
import { ProtocolError, errorCode, isEnoent, parseWriterRequest, parseWriterResponse } from "@/session/writer/protocol"

describe("parseWriterRequest: remote-query proxy", () => {
  test("parses a positional query carrying {method, sql, params}", () => {
    const request = parseWriterRequest({ type: "query", id: 1, method: "all", sql: "SELECT 1", params: ["a", 2] })
    expect(request).toEqual({ type: "query", id: 1, method: "all", sql: "SELECT 1", params: ["a", 2] })
  })

  test("defaults params and leaves cache unset when absent", () => {
    const request = parseWriterRequest({ type: "query", id: 2, method: "run", sql: "DELETE FROM t" })
    expect(request).toEqual({ type: "query", id: 2, method: "run", sql: "DELETE FROM t" })
  })

  test("rejects an unknown method and a non-array params", () => {
    expect(() => parseWriterRequest({ type: "query", id: 1, method: "select", sql: "x" })).toThrow(ProtocolError)
    expect(() => parseWriterRequest({ type: "query", id: 1, method: "all", sql: "x", params: {} })).toThrow(
      ProtocolError,
    )
  })
})

describe("parseWriterRequest: admin", () => {
  test("parses open, pragma, migrate and checkpoint", () => {
    expect(parseWriterRequest({ type: "admin.open", id: 1, filename: "/tmp/x.db" })).toEqual({
      type: "admin.open",
      id: 1,
      filename: "/tmp/x.db",
    })
    expect(parseWriterRequest({ type: "admin.migrate", id: 2, statements: ["CREATE TABLE t (a)"] })).toEqual({
      type: "admin.migrate",
      id: 2,
      statements: ["CREATE TABLE t (a)"],
    })
    expect(parseWriterRequest({ type: "admin.checkpoint", id: 3 })).toEqual({ type: "admin.checkpoint", id: 3 })
  })

  test("rejects a non-string statement array", () => {
    expect(() => parseWriterRequest({ type: "admin.migrate", id: 1, statements: [1] })).toThrow(ProtocolError)
  })
})

describe("parseWriterRequest: transactions", () => {
  test("parses begin, tx.query, commit and rollback", () => {
    expect(parseWriterRequest({ type: "tx.begin", id: 1 })).toEqual({ type: "tx.begin", id: 1 })
    expect(
      parseWriterRequest({ type: "tx.query", id: 2, handle: "tx-1", method: "all", sql: "SELECT 1" }),
    ).toEqual({ type: "tx.query", id: 2, handle: "tx-1", method: "all", sql: "SELECT 1" })
    expect(parseWriterRequest({ type: "tx.commit", id: 3, handle: "tx-1" })).toEqual({
      type: "tx.commit",
      id: 3,
      handle: "tx-1",
    })
    expect(parseWriterRequest({ type: "tx.rollback", id: 4, handle: "tx-1" })).toEqual({
      type: "tx.rollback",
      id: 4,
      handle: "tx-1",
    })
  })

  test("rejects a missing handle", () => {
    expect(() => parseWriterRequest({ type: "tx.commit", id: 1 })).toThrow(ProtocolError)
  })
})

describe("parseWriterRequest: publish", () => {
  const event = { id: "e1", type: "t", aggregateID: "a", data: { v: 1 } }

  test("parses a durable publish with owner fencing", () => {
    expect(
      parseWriterRequest({ type: "publish", id: 1, event, durable: true, ownerID: "o1", strictOwner: true }),
    ).toEqual({ type: "publish", id: 1, event, durable: true, ownerID: "o1", strictOwner: true })
  })

  test("defaults durable to false and omits absent optionals", () => {
    expect(parseWriterRequest({ type: "publish", id: 1, event })).toEqual({
      type: "publish",
      id: 1,
      event,
      durable: false,
    })
  })

  test("rejects an event without an object data payload", () => {
    expect(() =>
      parseWriterRequest({ type: "publish", id: 1, event: { ...event, data: "nope" }, durable: true }),
    ).toThrow(ProtocolError)
  })
})

describe("parseWriterRequest: ownership and replay", () => {
  test("parses claim, steal, remove and replay", () => {
    expect(parseWriterRequest({ type: "claim", id: 1, aggregateID: "a", ownerID: "o" })).toEqual({
      type: "claim",
      id: 1,
      aggregateID: "a",
      ownerID: "o",
    })
    expect(parseWriterRequest({ type: "steal", id: 2, aggregateID: "a", ownerID: "o" })).toEqual({
      type: "steal",
      id: 2,
      aggregateID: "a",
      ownerID: "o",
    })
    expect(parseWriterRequest({ type: "remove", id: 3, aggregateID: "a" })).toEqual({
      type: "remove",
      id: 3,
      aggregateID: "a",
    })
    expect(
      parseWriterRequest({
        type: "replay",
        id: 4,
        event: { id: "e", type: "t", aggregateID: "a", data: {} },
        seq: 3,
      }),
    ).toEqual({ type: "replay", id: 4, event: { id: "e", type: "t", aggregateID: "a", data: {} }, seq: 3 })
  })
})

describe("parseWriterRequest: malformed frames", () => {
  test("rejects non-objects, missing id and unknown types", () => {
    expect(() => parseWriterRequest(null)).toThrow(ProtocolError)
    expect(() => parseWriterRequest("query")).toThrow(ProtocolError)
    expect(() => parseWriterRequest({ type: "query", method: "all", sql: "x" })).toThrow(ProtocolError)
    expect(() => parseWriterRequest({ type: "explode", id: 1 })).toThrow(/unknown request type/)
  })

  test("rejects a non-boolean durable rather than coercing it (step 20)", () => {
    const event = { id: "e", type: "t", aggregateID: "a", data: {} }
    expect(() => parseWriterRequest({ type: "publish", id: 1, event, durable: "yes" })).toThrow(ProtocolError)
    expect(() => parseWriterRequest({ type: "publish", id: 1, event, durable: 1 })).toThrow(ProtocolError)
  })

  test("rejects an array where a record is required (step 21)", () => {
    expect(() => parseWriterRequest({ type: "publish", id: 1, event: [], durable: true })).toThrow(ProtocolError)
    expect(() => parseWriterRequest({ type: "publish", id: 1, event: { id: "e", type: "t", aggregateID: "a", data: [] } })).toThrow(
      ProtocolError,
    )
  })
})

describe("parseWriterResponse (step 15)", () => {
  test("parses ready, result and failure frames", () => {
    expect(parseWriterResponse({ type: "ready", generation: 3 })).toEqual({ type: "ready", generation: 3 })
    expect(parseWriterResponse({ type: "result", id: 1, value: { a: 1 } })).toEqual({
      type: "result",
      id: 1,
      value: { a: 1 },
    })
    expect(parseWriterResponse({ type: "failure", id: 2, error: { tag: "X", message: "m" } })).toEqual({
      type: "failure",
      id: 2,
      error: { tag: "X", message: "m" },
    })
  })

  test("carries the optional native SQLite code on a failure frame (R1#2)", () => {
    expect(
      parseWriterResponse({ type: "failure", id: 2, error: { tag: "X", message: "m", code: "SQLITE_BUSY" } }),
    ).toEqual({ type: "failure", id: 2, error: { tag: "X", message: "m", code: "SQLITE_BUSY" } })
    // An absent code is omitted rather than defaulted.
    expect(parseWriterResponse({ type: "failure", id: 3, error: { tag: "X", message: "m" } })).toEqual({
      type: "failure",
      id: 3,
      error: { tag: "X", message: "m" },
    })
  })

  test("fails loud on malformed responses", () => {
    expect(() => parseWriterResponse(null)).toThrow(ProtocolError)
    expect(() => parseWriterResponse({ type: "result", id: "x" })).toThrow(ProtocolError)
    expect(() => parseWriterResponse({ type: "failure", id: 1 })).toThrow(ProtocolError)
    expect(() => parseWriterResponse({ type: "failure", id: 1, error: { tag: "X", message: "m", code: {} } })).toThrow(
      ProtocolError,
    )
    expect(() => parseWriterResponse({ type: "nope" })).toThrow(/unknown response type/)
  })
})

describe("errorCode (R1#2)", () => {
  test("reads the native code and the injected sqliteCode, ignoring everything else", () => {
    expect(errorCode(Object.assign(new Error("locked"), { code: "SQLITE_BUSY" }))).toBe("SQLITE_BUSY")
    expect(errorCode({ code: 5 })).toBe(5)
    expect(errorCode({ sqliteCode: "SQLITE_LOCKED" })).toBe("SQLITE_LOCKED")
    expect(errorCode(new Error("no code"))).toBeUndefined()
    expect(errorCode(null)).toBeUndefined()
    expect(errorCode("SQLITE_BUSY")).toBeUndefined()
  })
})

describe("isEnoent (step 24)", () => {
  test("recognizes only the ENOENT code", () => {
    expect(isEnoent({ code: "ENOENT" })).toBe(true)
    expect(isEnoent({ code: "EACCES" })).toBe(false)
    expect(isEnoent(null)).toBe(false)
    expect(isEnoent("ENOENT")).toBe(false)
  })
})
