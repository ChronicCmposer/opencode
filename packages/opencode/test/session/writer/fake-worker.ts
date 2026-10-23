// Test double for the writer isolate. Unlike `worker.ts` it needs no database; it
// lets `client.test.ts` exercise the client's ready/timeout/generation and
// protocol-hardening paths deterministically. Behaviour is chosen via `workerData`:
//
//   mode: "healthy" (default) | "silent" | "sentinel" | "garbage" | "coded-failure"
//         | "crash-first-open" | "stall"
//   readyGeneration?: number  — override the echoed generation (mismatch tests)
//   stall?: string            — with mode "stall", the request type to never answer
//
// `coded-failure` succeeds `admin.open` (so the client becomes alive) and replies
// to every other request with a failure carrying a native SQLite `code`.
// `crash-first-open` exits without replying when the first generation receives
// `admin.open`/`admin.openStorage`, and thereafter requires an open before any
// other request succeeds — so a client that fails to record the open cannot
// recover after the crash.

import { workerData } from "node:worker_threads"

interface FakeScope {
  postMessage(message: unknown): void
  onmessage: ((event: { data: unknown }) => void) | null
}

const scope = globalThis as unknown as FakeScope
const data = (workerData ?? {}) as Record<string, unknown>
const mode = typeof data.mode === "string" ? data.mode : "healthy"
const echoedGeneration = typeof data.readyGeneration === "number" ? data.readyGeneration : (data.generation as number)

// Exit before ready on the first spawn: the client's ready promise must settle
// (not hang) and the next call must respawn with a bumped generation.
if (mode === "exit-once" && data.generation === 1) process.exit(1)

let opened = false

scope.onmessage = (event) => {
  const request = event.data as { readonly id: number; readonly type: string; readonly filename?: string }
  if (mode === "garbage") {
    scope.postMessage({ not: "a response" })
    return
  }
  if (mode === "sentinel") {
    scope.postMessage({ type: "failure", id: -1, error: { tag: "PROTOCOL", message: "unparseable frame" } })
    return
  }
  if (mode === "coded-failure" && request.type !== "admin.open") {
    scope.postMessage({
      type: "failure",
      id: request.id,
      error: { tag: "SQLITE_BUSY_EXHAUSTED", message: "SQLite remained busy", code: "SQLITE_BUSY" },
    })
    return
  }
  if (mode === "stall") {
    if (request.type === data.stall) return
    scope.postMessage({
      type: "result",
      id: request.id,
      value: request.type === "admin.open" ? { opened: true, filename: request.filename } : null,
    })
    return
  }
  if (mode === "crash-first-open") {
    if (request.type === "admin.open" || request.type === "admin.openStorage") {
      if (data.generation === 1) process.exit(1)
      opened = true
      scope.postMessage({
        type: "result",
        id: request.id,
        value: request.type === "admin.open" ? { opened: true, filename: request.filename } : null,
      })
      return
    }
    if (!opened) {
      scope.postMessage({ type: "failure", id: request.id, error: { tag: "CLOSED", message: "connection is not open" } })
      return
    }
    scope.postMessage({ type: "result", id: request.id, value: null })
    return
  }
  switch (request.type) {
    case "admin.open":
      scope.postMessage({ type: "result", id: request.id, value: { opened: true, filename: request.filename } })
      return
    default:
      scope.postMessage({ type: "result", id: request.id, value: null })
  }
}

if (mode !== "silent") scope.postMessage({ type: "ready", generation: echoedGeneration })
