// Step 2 — harness for the real session-worker entry.
//
// `bootSessionWorker` spawns `src/session/worker/session-worker.ts` with a
// structured-cloned `SessionWorkerInit` as `workerData`, then waits for the
// executor's `{type:"ready", threadId}` handshake. On a full boot the entry also
// transfers its worker-port channels before that handshake:
//
//   * `{type:"ports", egress, egressControl}` — the egress plus dedicated control
//     relays. D5 removed the writer port/RPC (the worker opens its own WAL
//     connection) and W4.3 removed the broker;
//   * `{type:"hub-proxy", port}` — the multiplexed hub control link.
//
// Those frames are captured from the buffered messages once `ready` arrives; a
// degraded boot (`sessionWorkerPorts` unset) sends only `ready`. It exposes an
// `ExecutorTransport` (`port`) plus `terminate`, reusing the shared worker
// harness and `test/lib/transport.ts`'s adapter rather than re-implementing
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

/** The worker's transferred relay ports (full hub-port boot only). */
export interface SessionWorkerPorts {
  readonly egress: MessagePort
  readonly egressControl?: MessagePort
}

/** The worker's transferred hub control link (full hub-port boot only). */
export interface SessionHubProxyFrame {
  readonly type: "hub-proxy"
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
  /** The worker's relay ports, when it booted with hub-served ports. */
  readonly ports: SessionWorkerPorts | undefined
  /** The worker's hub control link, when it booted with hub-served ports. */
  readonly hubProxyPort: MessagePort | undefined
  /** The observed readiness frame. */
  readonly ready: SessionWorkerReady
  readonly harness: WorkerHarness
  terminate(code?: number): Promise<number>
}

const DEFAULT_BOOT_TIMEOUT_MS = 15_000

const isMessagePort = (value: unknown): value is MessagePort =>
  typeof MessagePort !== "undefined" && value instanceof MessagePort

/** A frame that claims to carry a port must carry a real one; a malformed boot frame fails loud. */
function requireMessagePort(value: unknown, frameType: string, field: string): MessagePort {
  if (!isMessagePort(value)) {
    throw new Error(`"${frameType}" boot frame is missing a MessagePort "${field}" (received ${typeof value})`)
  }
  return value
}

/** Find the most recent transferred relay-ports frame in the buffered messages. */
function findPorts(messages: ReadonlyArray<unknown>): SessionWorkerPorts | undefined {
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const frame = messages[index]
    if (frame === null || typeof frame !== "object") continue
    if ((frame as { readonly type?: unknown }).type !== "ports") continue
    const candidate = frame as {
      readonly egress?: unknown
      readonly egressControl?: unknown
    }
    return {
      egress: requireMessagePort(candidate.egress, "ports", "egress"),
      ...(candidate.egressControl === undefined
        ? {}
        : { egressControl: requireMessagePort(candidate.egressControl, "ports", "egressControl") }),
    }
  }
  return undefined
}

/** Find the most recent transferred hub-proxy port frame in the buffered messages. */
function findHubProxyPort(messages: ReadonlyArray<unknown>): MessagePort | undefined {
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const frame = messages[index]
    if (frame === null || typeof frame !== "object") continue
    if ((frame as { readonly type?: unknown }).type !== "hub-proxy") continue
    return requireMessagePort((frame as { readonly port?: unknown }).port, "hub-proxy", "port")
  }
  return undefined
}

/**
 * Spawn the production entry and resolve once its `ready` handshake is observed;
 * any relay ports posted before it are captured from the same buffered stream.
 * The returned handle owns the isolate: `terminate()` closes the transferred
 * ports and tears the worker down.
 */
export async function bootSessionWorker(options: BootSessionWorkerOptions = {}): Promise<SessionWorkerHandle> {
  const ownsHarness = options.harness === undefined
  const harness = options.harness ?? createWorkerHarness({ timeoutMs: options.timeoutMs ?? DEFAULT_BOOT_TIMEOUT_MS })
  const worker = harness.spawn<SessionExecutorOutbound, SessionExecutorInbound>(SESSION_WORKER_ENTRY, {
    workerData: options.init ?? sessionWorkerInit(),
    ...(options.env === undefined ? {} : { env: options.env }),
  })
  const port = adaptTransport(worker)

  // `ready` is the terminal boot frame: the entry posts its relay ports before
  // installing the executor, so once it arrives the frames are already buffered.
  const ready = (await worker.waitFor("ready")) as SessionWorkerReady
  const ports = findPorts(worker.messages)
  const hubProxyPort = findHubProxyPort(worker.messages)

  const closePorts = () => {
    for (const transferred of [ports?.egress, ports?.egressControl, hubProxyPort]) {
      try {
        transferred?.close()
      } catch {
        // The port may already be closed with the isolate; termination still proceeds.
      }
    }
  }

  const terminate = async (code = 0): Promise<number> => {
    closePorts()
    if (ownsHarness) {
      await harness.dispose()
      return code
    }
    return worker.terminate(code)
  }

  return {
    port,
    worker,
    ports,
    hubProxyPort,
    ready,
    harness,
    terminate,
    [Symbol.asyncDispose]: async () => {
      await terminate()
    },
  }
}
