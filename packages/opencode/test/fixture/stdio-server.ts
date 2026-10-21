// Shared fake MCP/LSP stdio-server plumbing.
//
// Two test fixtures run as real child processes and must not diverge on the
// mechanics of being an MCP/LSP server:
//
//   - `test/session/broker/broker-child.ts` — the broker's fake child.
//   - `test/fixture/mcp-lsp-descendants.ts` — the process-tree fake child.
//
// They differ only in their per-method behaviour, so this module owns the parts
// they share: integer environment parsing, process-group discovery, and the MCP
// (newline-delimited JSON) / LSP (`Content-Length`) framing. Both fixtures must
// import from here rather than re-implementing these.

import { readFileSync } from "node:fs"

export interface JsonRpcMessage {
  readonly jsonrpc?: string
  readonly id?: unknown
  readonly method?: string
  readonly params?: unknown
}

/**
 * Read a non-negative integer from the environment.
 *
 * Single documented policy for every fixture: an **unset or empty-string** value
 * uses `fallback`; any other value must parse to a non-negative integer or this
 * throws. (Before unification the two fixtures disagreed about `""`.)
 */
export function readInt(name: string, fallback: number, env: NodeJS.ProcessEnv = process.env): number {
  const raw = env[name]
  if (raw === undefined || raw === "") return fallback
  const value = Number.parseInt(raw, 10)
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`${name} must be a non-negative integer, got ${raw}`)
  }
  return value
}

/** Process-group id of this process, from `/proc` (Linux) or `ps` (POSIX). */
export function processGroupId(): number {
  try {
    const stat = readFileSync("/proc/self/stat", "utf8")
    const close = stat.lastIndexOf(")")
    if (close === -1) throw new Error("malformed /proc/self/stat")
    const fields = stat.slice(close + 2).split(" ")
    const pgrp = Number.parseInt(fields[2] ?? "", 10)
    if (Number.isInteger(pgrp) && pgrp > 0) return pgrp
  } catch {
    /* fall through to ps / self */
  }
  try {
    const result = Bun.spawnSync(["ps", "-o", "pgid=", "-p", String(process.pid)])
    const pgrp = Number.parseInt(result.stdout.toString().trim(), 10)
    if (Number.isInteger(pgrp) && pgrp > 0) return pgrp
  } catch {
    /* fall through to self */
  }
  // A detached process is its own group leader; this is the expected case.
  return process.pid
}

/** Write one newline-delimited JSON-RPC message (MCP stdio framing). */
export function sendMcp(message: unknown): void {
  process.stdout.write(`${JSON.stringify(message)}\n`)
}

/** Write one `Content-Length`-framed JSON-RPC message (LSP stdio framing). */
export function sendLsp(message: unknown): void {
  const json = JSON.stringify(message)
  process.stdout.write(`Content-Length: ${Buffer.byteLength(json, "utf8")}\r\n\r\n${json}`)
}

export type MessageHandler = (message: JsonRpcMessage) => void | Promise<void>

/** Serve newline-delimited JSON-RPC from stdin until EOF. */
export async function serveMcp(handle: MessageHandler): Promise<void> {
  const decoder = new TextDecoder()
  const reader = Bun.stdin.stream().getReader()
  let buffer = ""
  for (;;) {
    const { done, value } = await reader.read()
    if (done) return
    buffer += decoder.decode(value, { stream: true })
    let newline: number
    while ((newline = buffer.indexOf("\n")) >= 0) {
      const line = buffer.slice(0, newline).trim()
      buffer = buffer.slice(newline + 1)
      if (!line) continue
      let parsed: JsonRpcMessage
      try {
        parsed = JSON.parse(line) as JsonRpcMessage
      } catch {
        continue
      }
      // Deliberately do not await: the broker must serialize, not this child.
      void handle(parsed)
    }
  }
}

/** Serve `Content-Length`-framed JSON-RPC from stdin until EOF. */
export async function serveLsp(handle: MessageHandler): Promise<void> {
  const reader = Bun.stdin.stream().getReader()
  let buffer = Buffer.alloc(0)
  for (;;) {
    const { done, value } = await reader.read()
    if (done) return
    buffer = Buffer.concat([buffer, Buffer.from(value)])
    for (;;) {
      const headerEnd = buffer.indexOf("\r\n\r\n")
      if (headerEnd < 0) break
      const header = buffer.slice(0, headerEnd).toString("utf8")
      const match = /Content-Length:\s*(\d+)/i.exec(header)
      if (!match) {
        buffer = buffer.slice(headerEnd + 4)
        continue
      }
      const length = Number.parseInt(match[1], 10)
      const bodyStart = headerEnd + 4
      if (buffer.length < bodyStart + length) break
      const body = buffer.slice(bodyStart, bodyStart + length).toString("utf8")
      buffer = buffer.slice(bodyStart + length)
      try {
        void handle(JSON.parse(body) as JsonRpcMessage)
      } catch {
        /* ignore malformed body */
      }
    }
  }
}
