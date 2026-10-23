// Phase 10 — broker wire-protocol contract (plan tasks 10.2/10.3).
//
// The broker is a dedicated Worker; the hub and every session worker speak to
// it over a transferred `MessagePort` using request-id-routed frames. This
// suite pins the pure boundary: the parser that turns an untrusted `unknown`
// into a trusted `BrokerRequest` (parse, don't validate), the TLS-style timeout
// resolution, and the error taxonomy. No processes, no ports.

import { describe, expect, test } from "bun:test"
import {
  BrokerFailure,
  BrokerProtocolError,
  DEFAULT_MCP_TIMEOUT_MS,
  parseAcquireResult,
  parseBrokerRequest,
  parseOAuthResult,
  parsePingResult,
  rememberBounded,
  resolveRequestTimeout,
} from "@/session/broker/protocol"

describe("parseBrokerRequest", () => {
  test("accepts every request kind", () => {
    const acquire = parseBrokerRequest({
      kind: "acquire",
      requestId: "r1",
      directory: "/work/project",
      servers: [{ name: "fake", kind: "mcp", command: ["bun", "server.ts"] }],
    })
    expect(acquire.kind).toBe("acquire")

    const call = parseBrokerRequest({
      kind: "call",
      requestId: "r2",
      directory: "/work/project",
      server: "fake",
      method: "tools/list",
    })
    expect(call.kind).toBe("call")

    expect(parseBrokerRequest({ kind: "cancel", requestId: "r3", targetId: "r2" }).kind).toBe("cancel")
    expect(parseBrokerRequest({ kind: "release", requestId: "r4", directory: "/d" }).kind).toBe("release")
    expect(parseBrokerRequest({ kind: "oauth", requestId: "r5", directory: "/d" }).kind).toBe("oauth")
    expect(parseBrokerRequest({ kind: "ping", requestId: "r6" }).kind).toBe("ping")
    expect(parseBrokerRequest({ kind: "teardown", requestId: "r7" }).kind).toBe("teardown")
  })

  test("preserves server specs, including kind, env and timeout", () => {
    const parsed = parseBrokerRequest({
      kind: "acquire",
      requestId: "r1",
      directory: "/d",
      servers: [
        { name: "m", kind: "mcp", command: ["node", "m.js"], env: { A: "1" }, timeoutMs: 1234 },
        { name: "l", kind: "lsp", command: ["node", "l.js"] },
      ],
    })
    if (parsed.kind !== "acquire") throw new Error("expected acquire")
    expect(parsed.servers).toHaveLength(2)
    expect(parsed.servers[0]).toMatchObject({ name: "m", kind: "mcp", timeoutMs: 1234, env: { A: "1" } })
    expect(parsed.servers[1]?.kind).toBe("lsp")
  })

  test("builds a genuinely string[]-typed command (not a re-cast unknown[])", () => {
    const parsed = parseBrokerRequest({
      kind: "acquire",
      requestId: "r1",
      directory: "/d",
      servers: [{ name: "m", kind: "mcp", command: ["bun", "run", ""] }],
    })
    if (parsed.kind !== "acquire") throw new Error("expected acquire")
    const command: ReadonlyArray<string> = parsed.servers[0]!.command
    expect(command).toEqual(["bun", "run", ""])
    // A non-string element is rejected at the boundary, not silently tolerated.
    expect(() =>
      parseBrokerRequest({
        kind: "acquire",
        requestId: "r1",
        directory: "/d",
        servers: [{ name: "m", kind: "mcp", command: ["bun", 7] }],
      }),
    ).toThrow(BrokerProtocolError)
  })

  test("rejects a non-object, a missing kind and an unknown kind", () => {
    expect(() => parseBrokerRequest(null)).toThrow(BrokerProtocolError)
    expect(() => parseBrokerRequest("acquire")).toThrow(BrokerProtocolError)
    expect(() => parseBrokerRequest({})).toThrow(/kind/i)
    expect(() => parseBrokerRequest({ kind: "explode", requestId: "r" })).toThrow(/kind/i)
  })

  test("rejects malformed fields with a descriptive message", () => {
    expect(() => parseBrokerRequest({ kind: "call", requestId: "", directory: "/d", server: "s", method: "m" })).toThrow(
      /requestId/,
    )
    expect(() => parseBrokerRequest({ kind: "acquire", requestId: "r", directory: "/d", servers: [] })).toThrow(/servers/)
    expect(() =>
      parseBrokerRequest({ kind: "acquire", requestId: "r", directory: "/d", servers: [{ name: "", kind: "mcp", command: ["x"] }] }),
    ).toThrow(/name/)
    expect(() =>
      parseBrokerRequest({ kind: "acquire", requestId: "r", directory: "/d", servers: [{ name: "s", kind: "bogus", command: ["x"] }] }),
    ).toThrow(/kind/)
    expect(() =>
      parseBrokerRequest({ kind: "acquire", requestId: "r", directory: "/d", servers: [{ name: "s", kind: "mcp", command: [] }] }),
    ).toThrow(/command/)
  })

  test("rejects an array where a record is required (A12)", () => {
    // `env` must be an object; an array is an object at runtime but not a record.
    expect(() =>
      parseBrokerRequest({
        kind: "acquire",
        requestId: "r",
        directory: "/d",
        servers: [{ name: "s", kind: "mcp", command: ["x"], env: [] }],
      }),
    ).toThrow(/env/)
  })

})

describe("response-value parsers (parse, don't validate)", () => {
  test("parseAcquireResult trusts a well-formed value and rejects junk", () => {
    const parsed = parseAcquireResult({
      instanceId: "inst",
      servers: [{ name: "m", pid: 10, pgid: 10, instanceId: "inst" }],
    })
    expect(parsed.instanceId).toBe("inst")
    expect(parsed.servers[0]).toEqual({ name: "m", pid: 10, pgid: 10, instanceId: "inst" })
    expect(() => parseAcquireResult(null)).toThrow(BrokerProtocolError)
    expect(() => parseAcquireResult({ instanceId: "i" })).toThrow(/servers/)
    expect(() => parseAcquireResult({ instanceId: "i", servers: [{ name: "m", pid: 0, pgid: 1, instanceId: "i" }] })).toThrow(
      /pid/,
    )
  })

  test("parseOAuthResult enforces the broker-owned shape", () => {
    expect(parseOAuthResult({ callbackUrl: "u", instanceId: "i", owner: "broker" })).toEqual({
      callbackUrl: "u",
      instanceId: "i",
      owner: "broker",
    })
    expect(() => parseOAuthResult({ callbackUrl: "u", instanceId: "i", owner: "someone" })).toThrow(/owner/)
  })

  test("parsePingResult requires integer fields", () => {
    expect(parsePingResult({ threadId: 3, directories: 1 })).toEqual({ threadId: 3, directories: 1 })
    expect(() => parsePingResult({ threadId: "3", directories: 1 })).toThrow(/threadId/)
  })
})

describe("resolveRequestTimeout", () => {
  test("prefers the per-request timeout, then the spec, then the default", () => {
    expect(resolveRequestTimeout(50, 500)).toBe(50)
    expect(resolveRequestTimeout(undefined, 500)).toBe(500)
    expect(resolveRequestTimeout(undefined, undefined)).toBe(DEFAULT_MCP_TIMEOUT_MS)
  })

  test("fails loud with a BrokerFailure (not a parse error) on a non-positive timeout", () => {
    expect(() => resolveRequestTimeout(0, undefined)).toThrow(/timeout/i)
    expect(() => resolveRequestTimeout(undefined, -1)).toThrow(/timeout/i)
    const error = (() => {
      try {
        resolveRequestTimeout(0, undefined)
      } catch (thrown) {
        return thrown
      }
    })()
    expect(error).toBeInstanceOf(BrokerFailure)
    expect((error as BrokerFailure).code).toBe("invalid-request")
  })
})

describe("rememberBounded (A14)", () => {
  test("evicts the oldest entry once the cap is reached", () => {
    const set = new Set<string>()
    for (let i = 0; i < 5; i++) rememberBounded(set, `r${i}`, 3)
    expect([...set]).toEqual(["r2", "r3", "r4"])
  })

  test("is a no-op for an id already present", () => {
    const set = new Set<string>(["a"])
    rememberBounded(set, "a", 3)
    expect([...set]).toEqual(["a"])
  })
})
