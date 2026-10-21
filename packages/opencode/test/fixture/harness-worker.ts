// Dependency-free Bun worker used by test/lib/worker-harness.test.ts.
// Protocol (JSON messages over the worker channel):
//   in : { type: "echo", data }        -> out: { type: "echo", data }
//   in : { type: "error", message }    -> out: { type: "error", message }
//   in : { type: "spin", ms }          -> out: { type: "spun" }   (busy-waits, no await)
//   in : { type: "close" }             -> worker closes itself
//   anything else                      -> out: { type: "unknown" }
// On startup: { type: "ready", pid, threadId }

import { threadId } from "node:worker_threads"

interface Incoming {
  type?: string
  data?: unknown
  message?: string
  ms?: number
}

interface WorkerScope {
  postMessage(message: unknown): void
  onmessage: ((event: { data: unknown }) => void) | null
  close(): void
}

const scope = globalThis as unknown as WorkerScope

scope.onmessage = (event) => {
  const input = (event.data ?? {}) as Incoming

  switch (input.type) {
    case "echo":
      scope.postMessage({ type: "echo", data: input.data })
      return
    case "error":
      scope.postMessage({ type: "error", message: input.message ?? "worker error" })
      return
    case "spin": {
      const deadline = Date.now() + (input.ms ?? 0)
      while (Date.now() < deadline) {
        // deliberately synchronous: proves terminate() can interrupt a tight loop
      }
      scope.postMessage({ type: "spun" })
      return
    }
    case "close":
      scope.close()
      return
    default:
      scope.postMessage({ type: "unknown" })
  }
}

scope.postMessage({ type: "ready", pid: process.pid, threadId })
