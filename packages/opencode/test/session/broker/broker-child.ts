// Phase 10 test fixture — a fake MCP (or LSP) stdio server for the broker.
//
// The broker's job is supervision, not protocol fidelity, so this child serves
// just enough JSON-RPC to complete a handshake and answer `work`. It exists to
// prove four broker properties that a real MCP server would make flaky:
//
//   1. per-directory serialization: each `work` request increments a shared
//      `active` counter, sleeps `BROKER_CHILD_DELAY_MS`, then decrements. Every
//      transition is appended to `BROKER_CHILD_JOURNAL` as JSON lines, so a test
//      can assert the maximum observed concurrency. A correctly serializing
//      broker keeps it at 1.
//   2. PGID ownership: `BROKER_CHILD_DESCENDANTS` children are spawned
//      non-detached, so they inherit this process's group id. Group-killing the
//      root's PGID must reap them all.
//   3. mcp_timeout: a `hang` method is deliberately never answered, so a caller
//      observes the broker's timeout.
//   4. notification fan-out: a `notify` method emits a JSON-RPC notification
//      before its response, which the broker must forward to subscribers.
//
// Environment:
//   BROKER_CHILD_MODE        "mcp" (default) | "lsp" — stdio framing
//   BROKER_CHILD_DELAY_MS    delay before a `work` response (default 0)
//   BROKER_CHILD_DESCENDANTS number of descendant processes (default 0)
//   BROKER_CHILD_JOURNAL     path to append JSONL concurrency events
//   BROKER_CHILD_MANIFEST    path to write { pid, pgid, mode } at startup
//   BROKER_CHILD_ROLE        "root" (default) | "descendant"
//   BROKER_CHILD_FAIL_INIT   "1" to reject the initialize handshake (JSON-RPC error)
//
// The broker always spawns the root detached, making it a group leader; this
// child never sets a process group itself.

import { appendFileSync } from "node:fs"
import {
  type JsonRpcMessage,
  processGroupId,
  readInt,
  sendLsp,
  sendMcp,
  serveLsp,
  serveMcp,
} from "../../fixture/stdio-server"

type Mode = "mcp" | "lsp"

const MODE: Mode = (process.env.BROKER_CHILD_MODE as Mode | undefined) ?? "mcp"
if (MODE !== "mcp" && MODE !== "lsp") throw new Error(`BROKER_CHILD_MODE must be "mcp" or "lsp", got ${MODE}`)
const DELAY_MS = readInt("BROKER_CHILD_DELAY_MS", 0)
const DESCENDANTS = readInt("BROKER_CHILD_DESCENDANTS", 0)
const JOURNAL = process.env.BROKER_CHILD_JOURNAL
const MANIFEST = process.env.BROKER_CHILD_MANIFEST
const ROLE = process.env.BROKER_CHILD_ROLE ?? "root"
const FAIL_INIT = process.env.BROKER_CHILD_FAIL_INIT === "1"

let active = 0

/** Append one JSONL concurrency event; small writes are atomic enough here. */
function journal(event: Record<string, unknown>): void {
  if (!JOURNAL) return
  try {
    appendFileSync(JOURNAL, `${JSON.stringify(event)}\n`)
  } catch {
    /* best-effort: the child may be mid-teardown */
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function spawnDescendants(): void {
  for (let index = 0; index < DESCENDANTS; index++) {
    Bun.spawn([process.execPath, import.meta.path], {
      env: { ...process.env, BROKER_CHILD_ROLE: "descendant", BROKER_CHILD_DESCENDANTS: "0" },
      stdin: "ignore",
      stdout: "ignore",
      stderr: "ignore",
    })
  }
}

/** Handle one JSON-RPC request. Responses/notifications may be asynchronous. */
async function handleMcp(message: JsonRpcMessage): Promise<void> {
  if (message.id === undefined) return // a notification; ignore
  const id = message.id

  if (message.method === "initialize") {
    if (FAIL_INIT) {
      sendMcp({ jsonrpc: "2.0", id, error: { code: -32000, message: "initialize refused" } })
      return
    }
    sendMcp({
      jsonrpc: "2.0",
      id,
      result: {
        protocolVersion: "2024-11-05",
        capabilities: { tools: {} },
        serverInfo: { name: "broker-child", version: "1.0.0" },
      },
    })
    return
  }

  if (message.method === "hang") return // never answer: exercises the broker timeout

  if (message.method === "notify") {
    sendMcp({ jsonrpc: "2.0", method: "notifications/message", params: { level: "info", data: "hi" } })
    sendMcp({ jsonrpc: "2.0", id, result: { ok: true } })
    return
  }

  if (message.method === "work") {
    active += 1
    journal({ event: "start", id, active, pid: process.pid, t: Date.now() })
    await sleep(DELAY_MS)
    journal({ event: "end", id, active, pid: process.pid, t: Date.now() })
    active -= 1
    sendMcp({ jsonrpc: "2.0", id, result: { echo: message.params ?? null } })
    return
  }

  sendMcp({ jsonrpc: "2.0", id, result: {} })
}

async function handleLsp(message: JsonRpcMessage): Promise<void> {
  if (message.id === undefined) return
  const id = message.id
  if (message.method === "initialize") {
    sendLsp({
      jsonrpc: "2.0",
      id,
      result: { capabilities: { textDocumentSync: 1 }, serverInfo: { name: "broker-child", version: "1.0.0" } },
    })
    return
  }
  if (message.method === "hang") return
  if (message.method === "work") {
    active += 1
    journal({ event: "start", id, active, pid: process.pid, t: Date.now() })
    await sleep(DELAY_MS)
    journal({ event: "end", id, active, pid: process.pid, t: Date.now() })
    active -= 1
    sendLsp({ jsonrpc: "2.0", id, result: { echo: message.params ?? null } })
    return
  }
  sendLsp({ jsonrpc: "2.0", id, result: null })
}

if (ROLE === "root" && MANIFEST) {
  await Bun.write(MANIFEST, JSON.stringify({ pid: process.pid, pgid: processGroupId(), mode: MODE }))
}

if (ROLE === "root") spawnDescendants()

// Keep the process alive even after the parent closes stdin, so only an explicit
// teardown (group kill) stops it.
setInterval(() => {}, 1 << 30)

if (ROLE === "root") {
  if (MODE === "mcp") await serveMcp(handleMcp)
  else await serveLsp(handleLsp)
}
