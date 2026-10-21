// Shared executor-transport adapter for dispatch test suites (finding #188).
//
// The `worker/session-execute-worker.ts` fixture speaks the executor protocol
// over the shared worker harness. Both `execute-worker.test.ts` and
// `subagent.test.ts` drove it through a verbatim `spawnTransport` copy; this
// module is the single source.

import type { ExecutorTransport, SessionExecutorInbound, SessionExecutorOutbound } from "@/session/worker/execute"
import type { HarnessWorker, WorkerHarness } from "./worker-harness"

/** The executor-protocol worker fixture the dispatch suites drive. */
export const EXECUTOR_WORKER_ENTRY = new URL("../fixture/session-execute-worker.ts", import.meta.url)

/** Per-spawn overrides; defaults to the executor fixture with no workerData. */
export interface SpawnTransportOptions {
  readonly entry?: string | URL
  readonly workerData?: unknown
  readonly env?: Record<string, string | undefined>
}

/**
 * Adapt an existing harness worker into an `ExecutorTransport`. Split out from
 * `spawnTransport` so a caller that already owns a handle (e.g. to observe a
 * transferred `job-port` frame) reuses the exact same transport shape.
 */
export function adaptTransport<TOut, TIn>(handle: HarnessWorker<TOut, TIn>): ExecutorTransport {
  return {
    post: (message) => handle.post(message as unknown as TIn),
    waitFor: (match, options) =>
      handle.waitFor(match as (message: TOut) => boolean, options) as unknown as Promise<SessionExecutorOutbound>,
    get alive() {
      return handle.alive
    },
    terminate: (code) => handle.terminate(code),
  }
}

/** Spawn an executor worker on `harness` and adapt it into an `ExecutorTransport`. */
export function spawnTransport(harness: WorkerHarness, options: SpawnTransportOptions = {}): ExecutorTransport {
  const handle = harness.spawn<SessionExecutorOutbound, SessionExecutorInbound>(options.entry ?? EXECUTOR_WORKER_ENTRY, {
    ...(options.workerData === undefined ? {} : { workerData: options.workerData }),
    ...(options.env === undefined ? {} : { env: options.env }),
  })
  return adaptTransport(handle)
}
