// Test double for the writer isolate. Unlike `worker.ts` it needs no database; it
// lets `client.test.ts` exercise the client's ready/timeout/generation and
// protocol-hardening paths deterministically. Behaviour is chosen via `workerData`:
//
//   mode: "healthy" (default) | "silent" | "sentinel" | "garbage"
//   readyGeneration?: number  — override the echoed generation (mismatch tests)

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
  switch (request.type) {
    case "admin.open":
      scope.postMessage({ type: "result", id: request.id, value: { opened: true, filename: request.filename } })
      return
    default:
      scope.postMessage({ type: "result", id: request.id, value: null })
  }
}

if (mode !== "silent") scope.postMessage({ type: "ready", generation: echoedGeneration })
