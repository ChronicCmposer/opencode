// Dependency-free Bun worker used by test/lib/worker-harness.test.ts.
// Protocol (JSON messages over the worker channel):
//   in : { type: "echo", data }        -> out: { type: "echo", data }
//   in : { type: "error", message }    -> out: { type: "error", message }
//   in : { type: "spin", ms }          -> out: { type: "spun" }   (busy-waits, no await)
//   in : { type: "throw", message }    -> worker throws uncaught (fires Worker.onerror)
//   in : { type: "close" }             -> worker closes itself
//   anything else                      -> out: { type: "unknown" }
// On startup: { type: "ready", pid, threadId }

import { threadId } from "node:worker_threads"
import { onMessage, postToParent, scope } from "./worker-scope"

interface Incoming {
  type?: string
  data?: unknown
  message?: string
  ms?: number
}

onMessage<Incoming>((raw) => {
  const input: Incoming = raw ?? {}

  switch (input.type) {
    case "echo":
      postToParent({ type: "echo", data: input.data })
      return
    case "error":
      postToParent({ type: "error", message: input.message ?? "worker error" })
      return
    case "spin": {
      const deadline = Date.now() + (input.ms ?? 0)
      while (Date.now() < deadline) {
        // deliberately synchronous: proves terminate() can interrupt a tight loop
      }
      postToParent({ type: "spun" })
      return
    }
    case "throw":
      throw new Error(input.message ?? "worker threw")
    case "close":
      scope.close()
      return
    default:
      postToParent({ type: "unknown" })
  }
})

postToParent({ type: "ready", pid: process.pid, threadId })
