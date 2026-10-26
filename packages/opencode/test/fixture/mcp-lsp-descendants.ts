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
//
// Integer env parsing, process-group discovery, and the MCP/LSP framing come
// from `./stdio-server`.

import {
  type JsonRpcMessage,
  processGroupId,
  readInt,
  sendLsp,
  sendMcp,
  serveLsp,
  serveMcp,
} from "./stdio-server"

const KIND = process.env.CHILD_TREE_KIND ?? "mcp"
const FANOUT = readInt("CHILD_TREE_FANOUT", 2)
const DEPTH = readInt("CHILD_TREE_DEPTH", 1)
const REMAINING = readInt("CHILD_TREE_REMAINING", DEPTH)
const ROLE = process.env.CHILD_TREE_ROLE ?? "root"

if (KIND !== "mcp" && KIND !== "lsp") throw new Error(`CHILD_TREE_KIND must be "mcp" or "lsp", got ${KIND}`)

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

function handleMcp(message: JsonRpcMessage): void {
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

function handleLsp(message: JsonRpcMessage): void {
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

const manifestPath = process.env.CHILD_TREE_MANIFEST
if (ROLE === "root" && manifestPath) {
  await Bun.write(manifestPath, JSON.stringify({ pid: process.pid, pgid: processGroupId(), kind: KIND, fanout: FANOUT, depth: DEPTH }))
}

spawnDescendants(REMAINING)

// Keep the process alive even if the parent closes stdin, so teardown is the
// only thing that stops it.
setInterval(() => {}, 1 << 30)

if (ROLE === "root") {
  if (KIND === "mcp") await serveMcp(handleMcp)
  else await serveLsp(handleLsp)
}
