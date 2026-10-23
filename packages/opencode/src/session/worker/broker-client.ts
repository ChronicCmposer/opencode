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
  type ServerSpec,
} from "../broker/protocol"
import type { BrokerClientHandle } from "./layer"
import { messageOf, portFromMessagePort } from "./port"
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

interface PendingRequest {
  readonly resolve: (value: unknown) => void
  readonly reject: (error: unknown) => void
  readonly timer: ReturnType<typeof setTimeout> | undefined
}

const DISPOSED_MESSAGE = "worker broker client disposed"

/** The lifecycle log fields shared by the connect/close lines for this peer. */
const brokerLifecycleFields = (workerID: number | undefined, level: "info" | "warn"): Record<string, unknown> => ({
  peer: "broker",
  level,
  ...(workerID === undefined ? {} : { workerID }),
})

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
  const log = options.log ?? noopWorkerLogger
  const workerID = options.workerID
  try {
    const client = buildBrokerClient(port, options)
    log(workerLogMessage("broker connected"), brokerLifecycleFields(workerID, "info"))
    return client
  } catch (error) {
    log(workerLogMessage("broker connect failed"), {
      ...brokerLifecycleFields(workerID, "warn"),
      reason: messageOf(error),
    })
    throw error
  }
}

/** The connect seam: build the client over a started port; a start failure throws. */
function buildBrokerClient(port: MessagePort, options: WorkerBrokerClientOptions): WorkerBrokerClient {
  const channel = portFromMessagePort<BrokerWireFrame>(port)
  const defaultTimeoutMs = options.timeoutMs
  const log = options.log ?? noopWorkerLogger
  const workerID = options.workerID

  let nextRequestId = 0
  let disposed = false
  const pending = new Map<string, PendingRequest>()

  const unsubscribe = channel.onMessage((frame) => {
    switch (frame.kind) {
      case "response": {
        const waiter = pending.get(frame.requestId)
        if (waiter === undefined) return
        pending.delete(frame.requestId)
        if (waiter.timer !== undefined) clearTimeout(waiter.timer)
        if (frame.ok) waiter.resolve(frame.value)
        else waiter.reject(new BrokerFailure(frame.error.code, frame.error.message))
        return
      }
      // Hub→worker pushes and (never-sent) requests are explicitly ignored.
      case "notification":
      case "child":
      case "child-exit":
      case "acquire":
      case "call":
      case "cancel":
      case "release":
      case "oauth":
      case "ping":
      case "teardown":
        return
      default: {
        // Exhaustiveness guard: a new frame kind must be handled here.
        const unknown: never = frame
        void unknown
        return
      }
    }
  })

  const request = (
    build: (requestId: string) => BrokerRequest,
    timeoutMs = defaultTimeoutMs,
  ): Promise<unknown> => {
    if (disposed) return Promise.reject(new BrokerFailure("crashed", DISPOSED_MESSAGE))
    const requestId = `w${++nextRequestId}`
    return new Promise<unknown>((resolve, reject) => {
      const timer =
        timeoutMs === undefined
          ? undefined
          : setTimeout(() => {
              pending.delete(requestId)
              reject(new BrokerFailure("timeout", `broker request ${requestId} timed out after ${timeoutMs}ms`))
            }, timeoutMs)
      pending.set(requestId, { resolve, reject, timer })
      try {
        channel.post(build(requestId))
      } catch (error) {
        pending.delete(requestId)
        if (timer !== undefined) clearTimeout(timer)
        reject(new BrokerFailure("crashed", messageOf(error)))
      }
    })
  }

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
      unsubscribe()
      for (const waiter of pending.values()) {
        if (waiter.timer !== undefined) clearTimeout(waiter.timer)
        waiter.reject(new BrokerFailure("crashed", DISPOSED_MESSAGE))
      }
      pending.clear()
      log(workerLogMessage("broker closed"), brokerLifecycleFields(workerID, "info"))
    },
  }
}

export * as WorkerBrokerClient from "./broker-client"
