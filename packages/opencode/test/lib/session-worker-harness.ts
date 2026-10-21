// Step 2 — harness for the real session-worker entry.
//
// `bootSessionWorker` spawns `src/session/worker/session-worker.ts` with a
// structured-cloned `SessionWorkerInit` as `workerData`, then observes its two
// control frames:
//
//   * `{type:"job-port", port}` — the worker's dedicated job control link
//     (transferred before `ready`), surfaced as `jobPort`;
//   * `{type:"ready", threadId}` — the executor's readiness announcement.
//
// It exposes an `ExecutorTransport` (`port`) plus `terminate`, reusing the shared
// worker harness and `test/lib/transport.ts`'s adapter rather than re-implementing
// message buffering.

import type { ExecutorTransport, SessionExecutorInbound, SessionExecutorOutbound } from "@/session/worker/execute"
import type { SessionWorkerInit } from "@/session/worker/session-worker"
import { adaptTransport } from "./transport"
import { createWorkerHarness, type HarnessWorker, type WorkerHarness } from "./worker-harness"
import { sessionWorkerInit } from "./session-worker-init"

/** The production session-worker entry the hub spawns. */
export const SESSION_WORKER_ENTRY = new URL("../../src/session/worker/session-worker.ts", import.meta.url)

/** The worker's readiness frame (`execute.ts`'s `SessionExecutorReady`). */
export interface SessionWorkerReady {
  readonly type: "ready"
  readonly threadId: number
}

/** The worker's transferred job control link frame. */
export interface SessionJobPortFrame {
  readonly type: "job-port"
  readonly port: MessagePort
}

export interface BootSessionWorkerOptions {
  /** Structured-cloned init; defaults to `sessionWorkerInit()`. */
  readonly init?: SessionWorkerInit
  /** Default `waitFor` timeout in ms. Defaults to 15000 (layer build + boot). */
  readonly timeoutMs?: number
  /** Reuse an existing harness (e.g. to boot several workers consistently). */
  readonly harness?: WorkerHarness
  /** Per-worker environment overlay. */
  readonly env?: Record<string, string | undefined>
}

export interface SessionWorkerHandle extends AsyncDisposable {
  /** Executor-protocol transport over the worker's default channel. */
  readonly port: ExecutorTransport
  /** The underlying harness worker (for lower-level frame inspection). */
  readonly worker: HarnessWorker<SessionExecutorOutbound, SessionExecutorInbound>
  /** The worker's transferred job control link, once observed. */
  readonly jobPort: MessagePort | undefined
  /** The observed readiness frame. */
  readonly ready: SessionWorkerReady
  readonly harness: WorkerHarness
  terminate(code?: number): Promise<number>
}

const DEFAULT_BOOT_TIMEOUT_MS = 15_000

/** Find the most recent transferred port frame in a worker's buffered messages. */
function findJobPort(messages: ReadonlyArray<unknown>): MessagePort | undefined {
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const frame = messages[index]
    if (frame === null || typeof frame !== "object") continue
    if ((frame as { readonly type?: unknown }).type !== "job-port") continue
    const port = (frame as { readonly port?: unknown }).port
    if (typeof MessagePort !== "undefined" && port instanceof MessagePort) return port
  }
  return undefined
}

/**
 * Spawn the production entry and resolve once its `ready` (and transferred
 * `job-port`) frames are observed. The returned handle owns the isolate:
 * `terminate()` closes the job port and tears the worker down.
 */
export async function bootSessionWorker(options: BootSessionWorkerOptions = {}): Promise<SessionWorkerHandle> {
  const ownsHarness = options.harness === undefined
  const harness = options.harness ?? createWorkerHarness({ timeoutMs: options.timeoutMs ?? DEFAULT_BOOT_TIMEOUT_MS })
  const worker = harness.spawn<SessionExecutorOutbound, SessionExecutorInbound>(SESSION_WORKER_ENTRY, {
    workerData: options.init ?? sessionWorkerInit(),
    ...(options.env === undefined ? {} : { env: options.env }),
  })
  const port = adaptTransport(worker)

  // The entry posts `job-port` at module top level, before `installSessionExecutor`
  // announces `ready`; awaiting both is safe because buffered frames resolve.
  const ready = (await worker.waitFor("ready")) as SessionWorkerReady
  await worker.waitFor("job-port")
  const jobPort = findJobPort(worker.messages)

  const terminate = async (code = 0): Promise<number> => {
    try {
      jobPort?.close()
    } catch {
      // The port may already be closed with the isolate; termination still proceeds.
    }
    if (ownsHarness) {
      await harness.dispose()
      return code
    }
    return worker.terminate(code)
  }

  return {
    port,
    worker,
    jobPort,
    ready,
    harness,
    terminate,
    [Symbol.asyncDispose]: async () => {
      await terminate()
    },
  }
}
