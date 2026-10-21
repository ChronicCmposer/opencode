// Dependency-free-ish worker fixture for the fault-injection seams (plan 3.7).
//
// Proves the `OPENCODE_TEST_FAULTS` gate crosses the isolate boundary and that
// `crashWorker` is point-scoped inside a real Bun worker thread.
//
// Protocol (JSON messages over the worker channel):
//   in : { type: "crash", point } -> out: { type: "crashing", point } then exit(70)
//                                    or   { type: "survived", point } when not armed
// On startup: { type: "ready" }

import { Faults } from "../../src/testing/faults"

interface Incoming {
  type?: string
  point?: string
}

interface WorkerScope {
  postMessage(message: unknown): void
  onmessage: ((event: { data: unknown }) => void) | null
}

const scope = globalThis as unknown as WorkerScope

scope.onmessage = (event) => {
  const input = (event.data ?? {}) as Incoming
  if (input.type !== "crash") return

  const point = input.point ?? ""
  Faults.crashWorker(point, {
    onCrash: (directive) => {
      scope.postMessage({ type: "crashing", point: directive.point })
      process.exit(Faults.WORKER_CRASH_EXIT_CODE)
    },
  })
  scope.postMessage({ type: "survived", point })
}

scope.postMessage({ type: "ready" })
