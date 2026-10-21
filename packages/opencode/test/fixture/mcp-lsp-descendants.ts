// Fake MCP/LSP stdio server that ALSO builds a descendant process tree.
//
// Plan task 3.8 needs a child process that (a) behaves like an MCP or LSP stdio
// server and (b) spawns descendants, so teardown can be proven to reap the
// whole process group. The existing fakes (`mcp-lifecycle-stdio.ts`,
// `lsp/fake-lsp-server.js`) cover full protocol fidelity but spawn no children;
// this entry point exists only for the process-tree property. Protocol support
// is therefore intentionally minimal: enough to complete a handshake, nothing
// more.
//
// Environment:
//   CHILD_TREE_KIND       "mcp" (default) | "lsp" — which stdio protocol to serve
//   CHILD_TREE_FANOUT     children per process (default 2)
//   CHILD_TREE_DEPTH      levels below the root (default 1); 0 = root only
//   CHILD_TREE_ROLE       "root" (default) | "descendant"
//   CHILD_TREE_REMAINING  remaining descendant levels (internal, set per child)
//
// When `CHILD_TREE_MANIFEST` is set, the root writes
// `{ pid, pgid, kind, fanout, depth }` so a test can cross-check the process
// group without guessing. Descendants
// inherit the root's PGID because children are spawned non-detached; the root
// itself is spawned detached by the test, making it the group leader.

import { readFileSync } from "node:fs"

function readInt(name: string, fallback: number): number {
  const raw = process.env[name]
  if (raw === undefined) return fallback
  const value = Number.parseInt(raw, 10)
  if (!Number.isInteger(value) || value < 0) throw new Error(`${name} must be a non-negative integer, got ${raw}`)
  return value
}

const KIND = process.env.CHILD_TREE_KIND ?? "mcp"
const FANOUT = readInt("CHILD_TREE_FANOUT", 2)
const DEPTH = readInt("CHILD_TREE_DEPTH", 1)
const REMAINING = readInt("CHILD_TREE_REMAINING", DEPTH)
const ROLE = process.env.CHILD_TREE_ROLE ?? "root"

if (KIND !== "mcp" && KIND !== "lsp") throw new Error(`CHILD_TREE_KIND must be "mcp" or "lsp", got ${KIND}`)

/** Process-group id of this process, read from `/proc` (Linux) or `ps` (POSIX). */
function processGroupId(): number {
  try {
    const stat = readFileSync("/proc/self/stat", "utf8")
    const close = stat.lastIndexOf(")")
    if (close === -1) throw new Error("malformed /proc/self/stat")
    const fields = stat.slice(close + 2).split(" ")
    const pgrp = Number.parseInt(fields[2] ?? "", 10)
    if (Number.isInteger(pgrp) && pgrp > 0) return pgrp
  } catch {
    // fall through to ps / self
  }
  try {
    const result = Bun.spawnSync(["ps", "-o", "pgid=", "-p", String(process.pid)])
    const pgrp = Number.parseInt(result.stdout.toString().trim(), 10)
    if (Number.isInteger(pgrp) && pgrp > 0) return pgrp
  } catch {
    // fall through to self
  }
  // A detached process is its own group leader; this is the expected case.
  return process.pid
}

function spawnDescendants(remaining: number): void {
  if (remaining <= 0) return
  for (let index = 0; index < FANOUT; index++) {
    Bun.spawn([process.execPath, import.meta.path], {
      env: { ...process.env, CHILD_TREE_ROLE: "descendant", CHILD_TREE_REMAINING: String(remaining - 1) },
      stdin: "ignore",
      stdout: "ignore",
      stderr: "ignore",
    })
  }
}

function sendMcp(message: unknown): void {
  process.stdout.write(`${JSON.stringify(message)}\n`)
}

function handleMcp(line: string): void {
  let message: { id?: unknown; method?: string }
  try {
    message = JSON.parse(line) as typeof message
  } catch {
    return
  }
  if (message.id === undefined) return // notification
  if (message.method === "initialize") {
    sendMcp({
      jsonrpc: "2.0",
      id: message.id,
      result: {
        protocolVersion: "2024-11-05",
        capabilities: { tools: {} },
        serverInfo: { name: "mcp-lsp-descendants", version: "1.0.0" },
      },
    })
    return
  }
  if (message.method === "tools/list") {
    sendMcp({ jsonrpc: "2.0", id: message.id, result: { tools: [] } })
    return
  }
  sendMcp({ jsonrpc: "2.0", id: message.id, result: {} })
}

async function serveMcp(): Promise<void> {
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
      if (line) handleMcp(line)
    }
  }
}

function sendLsp(message: unknown): void {
  const json = JSON.stringify(message)
  process.stdout.write(`Content-Length: ${Buffer.byteLength(json, "utf8")}\r\n\r\n${json}`)
}

function handleLsp(body: string): void {
  let message: { id?: unknown; method?: string }
  try {
    message = JSON.parse(body) as typeof message
  } catch {
    return
  }
  if (message.id === undefined) return // notification
  if (message.method === "initialize") {
    sendLsp({
      jsonrpc: "2.0",
      id: message.id,
      result: {
        capabilities: { textDocumentSync: 1 },
        serverInfo: { name: "mcp-lsp-descendants", version: "1.0.0" },
      },
    })
    return
  }
  sendLsp({ jsonrpc: "2.0", id: message.id, result: null })
}

async function serveLsp(): Promise<void> {
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
      const length = Number.parseInt(match[1]!, 10)
      const bodyStart = headerEnd + 4
      if (buffer.length < bodyStart + length) break
      const body = buffer.slice(bodyStart, bodyStart + length).toString("utf8")
      buffer = buffer.slice(bodyStart + length)
      handleLsp(body)
    }
  }
}

const manifestPath = process.env.CHILD_TREE_MANIFEST
if (ROLE === "root" && manifestPath) {
  await Bun.write(manifestPath, JSON.stringify({ pid: process.pid, pgid: processGroupId(), kind: KIND, fanout: FANOUT, depth: DEPTH }))
}

spawnDescendants(REMAINING)

// Keep the process alive even if the parent closes stdin, so teardown is the
// only thing that stops it.
setInterval(() => {}, 1 << 30)

if (ROLE === "root") {
  if (KIND === "mcp") await serveMcp()
  else await serveLsp()
}
