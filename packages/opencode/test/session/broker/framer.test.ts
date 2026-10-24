// Child stdout framing rules for the MCP/LSP broker (plan A16 / item 12).
//
// The framers are the trust boundary for a child's stdout: a valid frame decodes
// across arbitrary chunk boundaries, while malformed JSON, a missing header and an
// oversized length are surfaced as errors instead of silently swallowed.

import { describe, expect, test } from "bun:test"
import { LspFramer, MAX_CHILD_FRAME_BYTES, McpFramer, framerFor } from "@/session/broker/framer"

const encoder = new TextEncoder()

describe("McpFramer", () => {
  test("decodes newline-delimited messages across chunk boundaries", () => {
    const framer = new McpFramer()
    expect(framer.push(encoder.encode('{"id":1}\n{"id"'))).toEqual({ messages: [{ id: 1 }], errors: [] })
    expect(framer.push(encoder.encode(':2}\n'))).toEqual({ messages: [{ id: 2 }], errors: [] })
  })

  test("surfaces malformed JSON instead of swallowing it", () => {
    const framer = new McpFramer()
    const result = framer.push(encoder.encode("not json\n"))
    expect(result.messages).toEqual([])
    expect(result.errors).toHaveLength(1)
    expect(result.errors[0]).toMatch(/malformed MCP frame/)
  })

  test("rejects an oversized line and an oversized unterminated buffer", () => {
    const framer = new McpFramer()
    const oversized = "x".repeat(MAX_CHILD_FRAME_BYTES + 1)
    expect(framer.push(encoder.encode(`${oversized}\n`)).errors[0]).toMatch(/exceeds/)
    expect(framer.push(encoder.encode(oversized)).errors[0]).toMatch(/unterminated/)
  })
})

describe("LspFramer", () => {
  test("decodes a Content-Length framed message across chunks", () => {
    const framer = new LspFramer()
    const bytes = encoder.encode(framer.encode({ id: 1 }))
    expect(framer.push(bytes.slice(0, 5))).toEqual({ messages: [], errors: [] })
    expect(framer.push(bytes.slice(5))).toEqual({ messages: [{ id: 1 }], errors: [] })
  })

  test("rejects a missing or oversized Content-Length", () => {
    const framer = new LspFramer()
    expect(framer.push(encoder.encode("X-Test: 1\r\n\r\n{}")).errors[0]).toMatch(/missing a Content-Length/)
    expect(framer.push(encoder.encode(`Content-Length: ${MAX_CHILD_FRAME_BYTES + 1}\r\n\r\n`)).errors[0]).toMatch(
      /invalid LSP Content-Length/,
    )
  })

  test("surfaces a malformed JSON body", () => {
    const framer = new LspFramer()
    const result = framer.push(encoder.encode("Content-Length: 3\r\n\r\nnot"))
    expect(result.messages).toEqual([])
    expect(result.errors[0]).toMatch(/malformed LSP frame/)
  })
})

describe("framerFor", () => {
  test("picks the framing by server kind", () => {
    expect(framerFor("lsp")).toBeInstanceOf(LspFramer)
    expect(framerFor("mcp")).toBeInstanceOf(McpFramer)
  })
})
