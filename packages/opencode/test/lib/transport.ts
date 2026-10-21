// Shared executor-transport adapter for dispatch test suites (finding #188).
//
// The `worker/session-execute-worker.ts` fixture speaks the executor protocol
// over the shared worker harness. Both `execute-worker.test.ts` and
// `subagent.test.ts` drove it through a verbatim `spawnTransport` copy; this
// module is the single source.

import type { ExecutorTransport, SessionExecutorInbound, SessionExecutorOutbound } from "@/session/worker/execute"
import type { WorkerHarness } from "./worker-harness"

/** The executor-protocol worker fixture the dispatch suites drive. */
export const EXECUTOR_WORKER_ENTRY = new URL("../fixture/session-execute-worker.ts", import.meta.url)

/** Adapt the shared worker harness into an `ExecutorTransport` for one executor worker. */
export function spawnTransport(harness: WorkerHarness): ExecutorTransport {
  const handle = harness.spawn<SessionExecutorOutbound, SessionExecutorInbound>(EXECUTOR_WORKER_ENTRY)
  return {
    post: (message) => handle.post(message),
    waitFor: (match, options) => handle.waitFor(match, options),
    get alive() {
      return handle.alive
    },
    terminate: (code) => handle.terminate(code),
  }
}
