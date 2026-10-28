// Shared executor-transport adapter for dispatch test suites.
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
 *
 * The handle is pinned to the executor protocol, so the inbound/outbound frames
 * and the `waitFor` predicate line up without any casting.
 */
export function adaptTransport(
  handle: HarnessWorker<SessionExecutorOutbound, SessionExecutorInbound>,
): ExecutorTransport {
  return {
    post: (message) => handle.post(message),
    waitFor: (match, options) => handle.waitFor(match, options),
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

/** A transport that answers a bare pong to every wait; enough to build a worker. */
export function noopTransport(): ExecutorTransport {
  let alive = true
  return {
    post: () => {},
    waitFor: async () => ({ type: "pong", id: 0 }),
    get alive() {
      return alive
    },
    async terminate() {
      alive = false
      return 0
    },
  }
}

/** A recording scripted transport: the worker-side frames plus their fake replies. */
export interface RecordingTransport {
  readonly transport: ExecutorTransport
  /** Every inbound frame the worker posted, in order. */
  readonly posted: ReadonlyArray<SessionExecutorInbound>
}

/**
 * A fake `ExecutorTransport` that records each posted frame and answers the
 * pending wait from the most recent one. `reply` maps a request to its outbound
 * frame; a wait with no posted request, or a reply the waiter does not match,
 * fails loudly instead of hanging.
 */
export function recordingTransport(
  reply: (request: SessionExecutorInbound) => SessionExecutorOutbound,
): RecordingTransport {
  const posted: SessionExecutorInbound[] = []
  let alive = true
  return {
    posted,
    transport: {
      post: (message) => posted.push(message),
      waitFor: async (match) => {
        const request = posted[posted.length - 1]
        if (request === undefined) throw new Error("scripted transport waitFor called before any post")
        const outbound = reply(request)
        if (!match(outbound)) throw new Error("scripted transport produced a frame the waiter did not match")
        return outbound
      },
      get alive() {
        return alive
      },
      async terminate() {
        alive = false
        return 0
      },
    },
  }
}
