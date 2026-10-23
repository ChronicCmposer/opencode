// Production wiring (plan step 18) — worker-side broker client.
//
// A session worker cannot own MCP/LSP children: the hub's `createBroker`
// (`broker/broker.ts`) is the single authority and spawns the dedicated broker
// worker. This module is the worker's thin client across the transferred
// `MessagePort`, exposing the `BrokerClientHandle` seam (`call`) from `layer.ts`
// plus the rest of the broker surface (`acquire`/`oauth`/`release`/`ping`/
// `cancel`/`teardown`).
//
// It reuses the pure plumbing and boundary parsers so the client never
// re-implements the wire format:
//   * `portFromMessagePort` (`./port`) adapts the `MessagePort`;
//   * `parseAcquireResult` / `parseOAuthResult` / `parsePingResult`
//     (`../broker/protocol`) turn an untrusted response value into a trusted,
//     typed result — parse, don't validate;
//   * `BrokerFailure` carries the wire error taxonomy back to the caller.
//
// Frames are the broker protocol's own discriminated objects (`BrokerRequest`
// outbound, `ResponseFrame` inbound), so both ends switch exhaustively.

import {
  BrokerFailure,
  parseAcquireResult,
  parseOAuthResult,
  parsePingResult,
  type AcquireResult,
  type BrokerFrame,
  type BrokerRequest,
  type OAuthResult,
  type PingResult,
  type ResponseFrame,
  type ServerSpec,
} from "../broker/protocol"
import type { BrokerClientHandle } from "./layer"
import { connectPeer, createRequestChannel, messageOf, peerLifecycle, portFromMessagePort } from "./port"
import { noopWorkerLogger, workerLogMessage, type WorkerLogger } from "./log"

/** Every frame either end may post: requests outbound, response/push frames inbound. */
export type BrokerWireFrame = BrokerRequest | BrokerFrame

/**
 * The worker's broker client. It structurally satisfies `BrokerClientHandle`
 * (the production seam the reduced session layer installs), and adds the wider
 * broker surface a worker needs.
 */
export interface WorkerBrokerClient extends BrokerClientHandle {
  acquire(input: {
    readonly directory: string
    readonly servers: ReadonlyArray<ServerSpec>
  }): Promise<AcquireResult>
  cancel(targetId: string): Promise<void>
  oauth(directory: string): Promise<OAuthResult>
  release(directory: string): Promise<void>
  ping(): Promise<PingResult>
  teardown(): Promise<void>
  dispose(): void
}

export interface WorkerBrokerClientOptions {
  /**
   * Default per-request timeout. The hub broker enforces `mcp_timeout` itself,
   * so this is a safety net for a wedged/lost port; when set, an unanswered
   * request rejects with a `timeout` `BrokerFailure` instead of hanging.
   */
  readonly timeoutMs?: number
  /** Optional structured logger seam; absent defaults to a no-op (flag-off behavior). */
  readonly log?: WorkerLogger
  /**
   * The worker's owner id, when the caller knows it. Included on the lifecycle
   * log lines so a hub-side reader can correlate the broker peer with its pool
   * slot. Omitted (never fabricated) when not supplied.
   */
  readonly workerID?: number
}

const DISPOSED_MESSAGE = "worker broker client disposed"

/**
 * Adapt a transferred `MessagePort` into a worker broker client. The client
 * never serves requests and never receives hub→worker pushes, so every inbound
 * frame other than a correlated `response` is ignored.
 *
 * Emits the E2 peer lifecycle lines: an INFO `broker connected` at construction,
 * a WARN `broker connect failed` (with `reason`) if `port.start()` throws, and an
 * INFO `broker closed` at dispose.
 */
export function createWorkerBrokerClient(
  port: MessagePort,
  options: WorkerBrokerClientOptions = {},
): WorkerBrokerClient {
  return connectPeer(options.log ?? noopWorkerLogger, "broker", options.workerID, () =>
    buildBrokerClient(port, options),
  )
}

/** The connect seam: build the client over a started port; a start failure throws. */
function buildBrokerClient(port: MessagePort, options: WorkerBrokerClientOptions): WorkerBrokerClient {
  const channel = portFromMessagePort<BrokerWireFrame>(port)
  const log = options.log ?? noopWorkerLogger
  const workerID = options.workerID

  let nextRequestId = 0
  let disposed = false

  // Hub→worker pushes and (never-sent) requests are not replies, so the shared
  // channel ignores them; only a correlated `response` settles a request. Timers
  // are unref'd by the shared scheduler.
  const requests = createRequestChannel<BrokerWireFrame, ResponseFrame, string>(channel, {
    timeoutMs: options.timeoutMs,
    isReply: (frame): frame is ResponseFrame => frame.kind === "response",
    idOf: (reply) => reply.requestId,
    nextId: () => `w${++nextRequestId}`,
    disposedError: () => new BrokerFailure("crashed", DISPOSED_MESSAGE),
    timeoutError: (id, timeoutMs) =>
      new BrokerFailure("timeout", `broker request ${id} timed out after ${timeoutMs}ms`),
    mapPostError: (error) => new BrokerFailure("crashed", messageOf(error)),
  })

  const request = (build: (requestId: string) => BrokerRequest, timeoutMs?: number): Promise<unknown> =>
    requests.request(build, timeoutMs).then((frame) => {
      if (!frame.ok) throw new BrokerFailure(frame.error.code, frame.error.message)
      return frame.value
    })

  const requestAs = async <T>(build: (requestId: string) => BrokerRequest, parse: (value: unknown) => T): Promise<T> =>
    parse(await request(build))

  return {
    acquire: (input) =>
      requestAs(
        (requestId) => ({ kind: "acquire", requestId, directory: input.directory, servers: input.servers }),
        parseAcquireResult,
      ),
    call: (directory, server, method, params) =>
      request((requestId) => ({
        kind: "call",
        requestId,
        directory,
        server,
        method,
        ...(params === undefined ? {} : { params }),
      })),
    cancel: (targetId) => request((requestId) => ({ kind: "cancel", requestId, targetId })).then(() => undefined),
    oauth: (directory) => requestAs((requestId) => ({ kind: "oauth", requestId, directory }), parseOAuthResult),
    release: (directory) => request((requestId) => ({ kind: "release", requestId, directory })).then(() => undefined),
    ping: () => requestAs((requestId) => ({ kind: "ping", requestId }), parsePingResult),
    teardown: () => request((requestId) => ({ kind: "teardown", requestId })).then(() => undefined),
    dispose() {
      if (disposed) return
      disposed = true
      requests.dispose()
      log(workerLogMessage("broker closed"), peerLifecycle("broker", workerID)("info"))
    },
  }
}

export * as WorkerBrokerClient from "./broker-client"
