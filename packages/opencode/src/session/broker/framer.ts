// Child stdout framing for the MCP/LSP broker.
//
// MCP speaks newline-delimited JSON-RPC; LSP prefixes each message with a
// `Content-Length` header. Both buffers are attacker-influenced (a child process
// can emit anything), so a frame is capped at {@link MAX_CHILD_FRAME_BYTES}: an
// oversized or corrupt length must never grow the buffer without bound. Framing
// and JSON errors are returned to the caller rather than swallowed, so the broker
// can surface a misbehaving child instead of silently dropping its output.
//
// This module is side-effect free (the worker entry that uses it is not), so the
// framing rules can be unit-tested directly.

import type { ServerSpec } from "./protocol"

/** Hard cap on one child frame; a corrupt/hostile length cannot exceed this. */
export const MAX_CHILD_FRAME_BYTES = 8 * 1024 * 1024

export interface FramerPushResult {
  /** Complete, successfully decoded messages. */
  readonly messages: unknown[]
  /** Human-readable framing/JSON errors for the caller to surface (never silent). */
  readonly errors: string[]
}

export interface Framer {
  /** Encode one JSON-RPC message into the child's stdin wire format. */
  encode(message: unknown): string
  /** Feed a chunk of the child's stdout, returning any complete messages. */
  push(chunk: Uint8Array): FramerPushResult
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

/** Newline-delimited JSON-RPC (MCP stdio). */
export class McpFramer implements Framer {
  private buffer = ""
  private readonly decoder = new TextDecoder()

  encode(message: unknown): string {
    return `${JSON.stringify(message)}\n`
  }

  push(chunk: Uint8Array): FramerPushResult {
    this.buffer += this.decoder.decode(chunk, { stream: true })
    const messages: unknown[] = []
    const errors: string[] = []
    let newline: number
    while ((newline = this.buffer.indexOf("\n")) >= 0) {
      const line = this.buffer.slice(0, newline).trim()
      this.buffer = this.buffer.slice(newline + 1)
      if (!line) continue
      if (line.length > MAX_CHILD_FRAME_BYTES) {
        errors.push(`MCP frame exceeds ${MAX_CHILD_FRAME_BYTES} bytes`)
        continue
      }
      try {
        messages.push(JSON.parse(line))
      } catch (error) {
        errors.push(`malformed MCP frame: ${errorMessage(error)}`)
      }
    }
    // No newline in sight and the buffer is already over the cap: drop it rather
    // than wait for a terminator that may never arrive.
    if (this.buffer.length > MAX_CHILD_FRAME_BYTES) {
      this.buffer = ""
      errors.push(`unterminated MCP frame exceeds ${MAX_CHILD_FRAME_BYTES} bytes`)
    }
    return { messages, errors }
  }
}

/** LSP-style `Content-Length` framing. */
export class LspFramer implements Framer {
  private buffer = Buffer.alloc(0)

  encode(message: unknown): string {
    const json = JSON.stringify(message)
    return `Content-Length: ${Buffer.byteLength(json, "utf8")}\r\n\r\n${json}`
  }

  push(chunk: Uint8Array): FramerPushResult {
    this.buffer = Buffer.concat([this.buffer, Buffer.from(chunk)])
    const messages: unknown[] = []
    const errors: string[] = []
    for (;;) {
      const headerEnd = this.buffer.indexOf("\r\n\r\n")
      if (headerEnd < 0) break
      const header = this.buffer.slice(0, headerEnd).toString("utf8")
      const match = /Content-Length:\s*(\d+)/i.exec(header)
      if (!match) {
        this.buffer = this.buffer.slice(headerEnd + 4)
        errors.push("LSP frame is missing a Content-Length header")
        continue
      }
      const length = Number.parseInt(match[1] ?? "", 10)
      const bodyStart = headerEnd + 4
      if (!Number.isSafeInteger(length) || length < 0 || length > MAX_CHILD_FRAME_BYTES) {
        // Drop the header without waiting for a body we will never buffer.
        this.buffer = this.buffer.slice(bodyStart)
        errors.push(`invalid LSP Content-Length ${String(match[1])} (max ${MAX_CHILD_FRAME_BYTES})`)
        continue
      }
      if (this.buffer.length < bodyStart + length) break
      const body = this.buffer.slice(bodyStart, bodyStart + length).toString("utf8")
      this.buffer = this.buffer.slice(bodyStart + length)
      try {
        messages.push(JSON.parse(body))
      } catch (error) {
        errors.push(`malformed LSP frame: ${errorMessage(error)}`)
      }
    }
    return { messages, errors }
  }
}

export function framerFor(kind: ServerSpec["kind"]): Framer {
  return kind === "lsp" ? new LspFramer() : new McpFramer()
}
