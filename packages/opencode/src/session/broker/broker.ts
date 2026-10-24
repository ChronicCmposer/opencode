// Phase 10 — hub-side MCP/LSP broker client (plan tasks 10.2–10.9).
//
// Spawns the dedicated broker Worker, completes the port-pairing handshake,
// and routes request-id-tagged frames over a transferred `MessagePort`, so no
// MCP/LSP I/O ever runs on the hub loop. It also supervises the broker:
//
//   - a worker exit/crash rejects every in-flight call (`crashed`),
//   - the hub mirrors child PGIDs and reaps orphans on crash (POSIX group kill),
//   - the broker respawns with a bumped generation and a rebroadcast port,
//   - a supervision signal path notifies observers (`crashed` / `respawned`).
//
// The contract intentionally depends on nothing from the pool/writer/egress
// modules: it is a leaf service the hub can own directly.

import {
  BrokerFailure,
  DEFAULT_MCP_TIMEOUT_MS,
  parseAcquireResult,
  parseBrokerFrame,
  parseBrokerRequest,
  parseOAuthResult,
  parsePingResult,
  parseWorkerHandshake,
  toBrokerError,
} from "./protocol"
import type {
  AcquireResult,
  BrokerErrorCode,
  BrokerFrame,
  BrokerRequest,
  BrokerWorkerOutbound,
  OAuthResult,
  PingResult,
  ResponseFrame,
  ServerSpec,
} from "./protocol"
import { mergeEnv } from "./env"
import {
  createRequestChannel,
  portFromMessagePort,
  rawRequestId,
  resolveWorkerEntry,
  servePort,
  type FrameChannel,
} from "../worker/port"

export interface WorkerLike {
  readonly generation: number
  readonly alive: boolean
}

export interface PortLike {
  close(): void
}

export interface BrokerChild {
  readonly directory: string
  readonly server: string
  readonly pid: number
  readonly pgid: number
}

export type SupervisionEvent =
  | { readonly type: "crashed"; readonly generation: number; readonly orphans: number }
  | { readonly type: "respawned"; readonly generation: number }

/** Kills a leaked child process group; injectable so tests can observe reaping. */
export interface ProcessReaper {
  reap(pgid: number): void
}

/** Raised for a broker-level failure; `code` mirrors the wire error taxonomy. */
export class BrokerRequestError extends Error {
  readonly code: BrokerErrorCode
  constructor(code: BrokerErrorCode, message: string) {
    super(message)
    this.name = "BrokerRequestError"
    this.code = code
  }
}

export interface BrokerOptions {
  /** Broker worker entry; defaults to the production `broker-worker.ts`. */
  readonly entry?: string | URL
  /** Environment overlay applied to the broker worker. */
  readonly env?: Record<string, string | undefined>
  /** Hub-side request timeout in ms; defaults to the broker `mcp_timeout`. */
  readonly timeoutMs?: number
  /**
   * Deadline for the initial `ready` handshake in ms; defaults to the broker
   * `mcp_timeout`. Kept independent of `timeoutMs` so a short per-request timeout
   * cannot make startup look like a crash.
   */
  readonly readyTimeoutMs?: number
  /** Override orphan reaping (defaults to a POSIX process-group kill). */
  readonly reaper?: ProcessReaper
}

export interface AcquireInput {
  readonly directory: string
  readonly servers: ReadonlyArray<ServerSpec>
}

export interface CallInput {
  readonly directory: string
  readonly server: string
  readonly method: string
  readonly params?: unknown
  readonly timeoutMs?: number
  /** Explicit request id, so a caller can cancel it by id. */
  readonly requestId?: string
}

export interface NotificationLike {
  readonly directory: string
  readonly server: string
  readonly method: string
  readonly params?: unknown
}

export interface BrokerClient extends AsyncDisposable {
  readonly ready: Promise<void>
  readonly generation: number
  readonly threadId: number
  readonly alive: boolean
  readonly transport: "message-port"
  readonly handshake: ReadonlyArray<string>
  readonly workerHandle: WorkerLike
  readonly portHandle: PortLike
  /** Mirrored live children, keyed by (directory, server). */
  readonly children: ReadonlyArray<BrokerChild>
  acquire(input: AcquireInput): Promise<AcquireResult>
  call(input: CallInput): Promise<unknown>
  cancel(targetId: string): Promise<void>
  oauth(directory: string): Promise<OAuthResult>
  release(directory: string): Promise<void>
  ping(): Promise<PingResult>
  teardown(): Promise<void>
  onNotification(handler: (notification: NotificationLike) => void): () => void
  onSupervision(handler: (event: SupervisionEvent) => void): () => void
}

declare global {
  /**
   * Compile-time path to the bundled broker isolate, injected by `script/build.ts`
   * (mirrors `OPENCODE_SESSION_WORKER_PATH`). Absent in dev/test.
   */
  const OPENCODE_BROKER_WORKER_PATH: string | undefined
}

/**
 * Resolve the broker isolate entry: the runtime env override, else the build-time
 * define, else the source-relative isolate. The packaged binary cannot resolve
 * the source path (`new URL("./broker-worker.ts", import.meta.url)` becomes
 * `/$bunfs/root/broker-worker.ts`), so this mirrors `resolveSessionWorkerEntry`
 * to load the bundled worker. Pure so the resolution is testable without
 * re-importing.
 */
export function resolveBrokerWorkerEntry(
  options: { readonly env?: Record<string, string | undefined>; readonly definedPath?: string } = {},
): string | URL {
  return resolveWorkerEntry({
    env: options.env,
    definedPath:
      options.definedPath ??
      (typeof OPENCODE_BROKER_WORKER_PATH !== "undefined" ? OPENCODE_BROKER_WORKER_PATH : undefined),
    envVar: "OPENCODE_BROKER_WORKER_PATH",
    fallback: new URL("./broker-worker.ts", import.meta.url),
  })
}

const defaultReaper: ProcessReaper = {
  reap(pgid: number): void {
    try {
      process.kill(-pgid, "SIGTERM")
    } catch {
      /* best-effort: the group is already gone (ESRCH) or not ours to kill */
    }
  },
}

/** Minimal structural view of a Bun Worker, avoiding DOM lib coupling. */
interface BrokerWorkerHandle {
  onmessage: ((event: { data: unknown }) => void) | null
  addEventListener(type: string, listener: () => void): void
  removeEventListener(type: string, listener: () => void): void
  postMessage(message: unknown, transfer?: ReadonlyArray<Transferable>): void
  terminate(): Promise<number> | number
}

function keyOf(directory: string, server: string): string {
  return `${directory}\u0000${server}`
}

export function createBroker(options: BrokerOptions = {}): BrokerClient {
  // Resolve against the caller's env overlay: a spawn must honour the same
  // override the caller passed, not whatever `process.env` held at import time.
  const entry = options.entry ?? resolveBrokerWorkerEntry({ env: options.env })
  const env = mergeEnv(options.env)
  const reaper = options.reaper ?? defaultReaper
  const requestTimeoutMs = options.timeoutMs ?? DEFAULT_MCP_TIMEOUT_MS
  const readyTimeoutMs = options.readyTimeoutMs ?? DEFAULT_MCP_TIMEOUT_MS
  /** Upper bound on any per-call timeout override (10 minutes). */
  const MAX_REQUEST_TIMEOUT_MS = 10 * 60_000

  /** Respawn backoff bounds: a crash loop must not spawn workers without limit. */
  const RESPAWN_BASE_MS = 50
  const RESPAWN_MAX_MS = 2_000
  const MAX_CONSECUTIVE_CRASHES = 6

  let generation = 1
  let threadId = 0
  let alive = false
  let disposed = false
  let currentWorkerId = 0
  let nextRequestId = 0
  let consecutiveCrashes = 0
  /** True once this generation has been torn down, so duplicate events are ignored. */
  let workerGone = false

  const handshake: string[] = []
  const mirror = new Map<string, BrokerChild>()
  /** Inbound-frame subscribers, stable across generations (the port is per-generation). */
  const inbound = new Set<(frame: BrokerRequest | BrokerFrame) => void>()
  const notificationHandlers = new Set<(notification: NotificationLike) => void>()
  const supervisionHandlers = new Set<(event: SupervisionEvent) => void>()

  let worker: BrokerWorkerHandle | undefined
  let port: MessagePort | undefined
  let readySettled = false
  let readyTimer: ReturnType<typeof setTimeout> | undefined
  let respawnTimer: ReturnType<typeof setTimeout> | undefined

  let resolveTransport: (() => void) | undefined
  let rejectTransport: ((error: Error) => void) | undefined

  /**
   * A fresh per-generation transport deferred. A crash before any request is
   * waiting must not surface as an unhandled rejection, so the eager catch is
   * attached here; `request` installs its own handlers on top.
   */
  function newTransportReady(): Promise<void> {
    const promise = new Promise<void>((resolve, reject) => {
      resolveTransport = resolve
      rejectTransport = reject
    })
    promise.catch(() => {})
    return promise
  }

  let transportReady = newTransportReady()

  /**
   * The RPC channel facade. Outbound requests post to the current generation's
   * `port`; inbound frames are fanned out to subscribers (the request channel's
   * reply resolver plus the mirror/notification push handler). The port arrives
   * after the handshake, so requests are gated on `transportReady` — and the
   * shared request channel arms their timeout around that wait.
   */
  const rpcChannel: FrameChannel<BrokerRequest | BrokerFrame> = {
    post: (frame) => {
      if (port === undefined) throw new BrokerRequestError("crashed", "broker is not available")
      port.postMessage(frame)
    },
    onMessage: (handler) => {
      inbound.add(handler)
      return () => inbound.delete(handler)
    },
  }

  const requests = createRequestChannel<BrokerRequest | BrokerFrame, ResponseFrame, string>(rpcChannel, {
    isReply: (frame): frame is ResponseFrame => frame.kind === "response",
    idOf: (reply) => reply.requestId,
    nextId,
    duplicateError: (id) => new BrokerRequestError("invalid-request", `duplicate request id ${id}`),
    disposedError: () => new BrokerRequestError("crashed", "broker torn down"),
    timeoutError: (id, timeoutMs) => new BrokerRequestError("timeout", `request ${id} timed out after ${timeoutMs}ms`),
    awaitPost: () => transportReady,
  })
  // Non-response frames are pushes (notifications / child mirror), never replies;
  // narrow by kind so a request-kind frame can never reach the push handler.
  requests.onFrame((frame) => {
    if (frame.kind === "notification" || frame.kind === "child" || frame.kind === "child-exit") handlePush(frame)
  })

  /** Fan one parsed inbound frame out to the channel's subscribers. */
  function dispatchInbound(frame: BrokerFrame): void {
    for (const handler of Array.from(inbound)) handler(frame)
  }

  let settleReady!: () => void
  let rejectReady!: (error: Error) => void
  const ready = new Promise<void>((resolve, reject) => {
    settleReady = resolve
    rejectReady = reject
  })
  // An observer may never await `ready`; a rejection must not go unhandled.
  void ready.catch(() => {})

  /** Settle `ready` as failed at most once (a later generation cannot revive it). */
  function failReady(error: Error): void {
    if (readySettled) return
    readySettled = true
    rejectReady(error)
  }

  function clearReadyTimer(): void {
    if (readyTimer === undefined) return
    clearTimeout(readyTimer)
    readyTimer = undefined
  }

  const workerHandle: WorkerLike = {
    get generation() {
      return generation
    },
    get alive() {
      return alive
    },
  }
  const portHandle: PortLike = {
    close: () => port?.close(),
  }

  function emitSupervision(event: SupervisionEvent): void {
    for (const handler of [...supervisionHandlers]) handler(event)
  }

  /** Handle a non-reply frame: fan out notifications, mirror child lifecycle. */
  function handlePush(frame: BrokerFrame): void {
    switch (frame.kind) {
      case "response":
        return
      case "notification": {
        for (const handler of [...notificationHandlers]) handler(frame)
        return
      }
      case "child": {
        mirror.set(keyOf(frame.directory, frame.server), {
          directory: frame.directory,
          server: frame.server,
          pid: frame.pid,
          pgid: frame.pgid,
        })
        return
      }
      case "child-exit": {
        mirror.delete(keyOf(frame.directory, frame.server))
        return
      }
    }
  }

  function nextId(): string {
    // Internal ids are namespaced so a caller-supplied `requestId` cannot collide
    // with (and silently steal) an internal request's correlation id.
    return `internal:${++nextRequestId}`
  }

  /**
   * Issue one broker request. The timeout is clamped to the max and armed by the
   * shared channel around both the wait for transport and the reply; the reply's
   * `ok`/`error` is mapped onto the caller-facing `BrokerRequestError`.
   */
  function request(frame: BrokerRequest, timeoutMs: number = requestTimeoutMs): Promise<unknown> {
    const effectiveTimeout = Math.min(timeoutMs, MAX_REQUEST_TIMEOUT_MS)
    return requests.request(() => frame, effectiveTimeout, frame.requestId).then((reply) => {
      if (!reply.ok) throw new BrokerRequestError(reply.error.code, reply.error.message)
      return reply.value
    })
  }

  function scheduleRespawn(): void {
    if (respawnTimer !== undefined) clearTimeout(respawnTimer)
    // Exponential backoff, capped: a crash loop must not spawn workers without bound.
    const delay = Math.min(RESPAWN_BASE_MS * 2 ** (consecutiveCrashes - 1), RESPAWN_MAX_MS)
    respawnTimer = setTimeout(() => {
      respawnTimer = undefined
      if (!disposed) startWorker()
    }, delay)
    ;(respawnTimer as { unref?: () => void }).unref?.()
  }

  function handleCrash(): void {
    // `error` and `close` (or a parse failure plus `close`) can fire for the same
    // generation; handle it exactly once.
    if (workerGone) return
    workerGone = true
    clearReadyTimer()
    const orphans = [...mirror.values()]
    mirror.clear()
    requests.fail(new BrokerRequestError("crashed", `broker worker exited (generation ${generation})`))
    alive = false
    threadId = 0
    port?.close()
    port = undefined
    // Settle the outgoing transport before reassigning it, so any request caught
    // waiting for the port fails fast instead of dangling on the old promise.
    rejectTransport?.(new BrokerRequestError("crashed", `broker worker exited before ready (generation ${generation})`))
    rejectTransport = undefined
    resolveTransport = undefined
    emitSupervision({ type: "crashed", generation, orphans: orphans.length })
    for (const child of orphans) reaper.reap(child.pgid)
    generation += 1
    consecutiveCrashes += 1
    if (consecutiveCrashes > MAX_CONSECUTIVE_CRASHES) {
      // Give up: leave the rejected transport in place so requests fail fast
      // rather than wait on a worker we will never spawn again.
      return
    }
    transportReady = newTransportReady()
    scheduleRespawn()
  }

  function startWorker(): void {
    const myId = ++currentWorkerId
    workerGone = false
    // Each generation gets its own handshake trace; reset before it re-runs.
    handshake.length = 0
    // Born named `ocb` so the isolate is observable from creation, not only once
    // its own entry self-names.
    const handle = new Worker(entry, { env, name: "ocb" }) as unknown as BrokerWorkerHandle
    worker = handle

    // Readiness deadline: a worker that never completes the handshake must not
    // leave `ready` (and anyone awaiting it) hanging forever.
    clearReadyTimer()
    readyTimer = setTimeout(() => {
      if (myId !== currentWorkerId || readySettled) return
      failReady(
        new BrokerRequestError("timeout", `broker worker did not become ready within ${readyTimeoutMs}ms`),
      )
      void handle.terminate()
      if (!disposed) handleCrash()
    }, readyTimeoutMs)
    ;(readyTimer as { unref?: () => void }).unref?.()

    handle.onmessage = (event) => {
      // A frame from a superseded generation is stale: terminate it, don't route it.
      if (myId !== currentWorkerId) {
        void handle.terminate()
        return
      }
      let handshakeFrame: BrokerWorkerOutbound
      try {
        handshakeFrame = parseWorkerHandshake(event.data)
      } catch {
        void handle.terminate()
        if (!disposed) handleCrash()
        return
      }
      if (handshakeFrame.type === "ready") {
        threadId = handshakeFrame.threadId
        handshake.push("ready")
        const channel = new MessageChannel()
        port = channel.port1
        port.onmessage = (message) => {
          let frame: BrokerFrame
          try {
            frame = parseBrokerFrame(message.data)
          } catch {
            // A broker that emits a malformed frame is unusable: drop the
            // generation rather than letting a half-formed frame into the mirror.
            if (!disposed) handleCrash()
            return
          }
          dispatchInbound(frame)
        }
        port.start?.()
        handle.postMessage({ type: "attach", port: channel.port2 }, [channel.port2 as unknown as Transferable])
        handshake.push("attach")
        return
      }
      if (handshakeFrame.type === "attached") {
        clearReadyTimer()
        // A generation reached `attached`, so the crash loop (if any) is over.
        consecutiveCrashes = 0
        handshake.push("attached")
        alive = true
        resolveTransport?.()
        resolveTransport = undefined
        rejectTransport = undefined
        if (generation > 1) emitSupervision({ type: "respawned", generation })
        if (!readySettled) {
          readySettled = true
          settleReady()
        }
      }
    }

    const onGone = () => {
      if (disposed || myId !== currentWorkerId) return
      // Terminate the handle even on an `error` event: a worker that errored but
      // did not close must not linger alongside its replacement.
      void handle.terminate()
      handleCrash()
    }
    handle.addEventListener("close", onGone)
    handle.addEventListener("error", onGone)
  }

  startWorker()

  async function requestAs<T>(frame: BrokerRequest, parse: (value: unknown) => T): Promise<T> {
    return parse(await request(frame))
  }

  const acquire = (input: AcquireInput): Promise<AcquireResult> =>
    requestAs({ kind: "acquire", requestId: nextId(), directory: input.directory, servers: input.servers }, parseAcquireResult)

  const call = (input: CallInput): Promise<unknown> =>
    request(
      {
        kind: "call",
        requestId: input.requestId ?? nextId(),
        directory: input.directory,
        server: input.server,
        method: input.method,
        ...(input.params === undefined ? {} : { params: input.params }),
        ...(input.timeoutMs === undefined ? {} : { timeoutMs: input.timeoutMs }),
      },
      // The per-call override bounds the hub-side wait too, clamped to the max.
      input.timeoutMs,
    )

  const cancel = (targetId: string): Promise<void> =>
    request({ kind: "cancel", requestId: nextId(), targetId }).then(() => undefined)

  const oauth = (directory: string): Promise<OAuthResult> =>
    requestAs({ kind: "oauth", requestId: nextId(), directory }, parseOAuthResult)

  const release = (directory: string): Promise<void> =>
    request({ kind: "release", requestId: nextId(), directory }).then(() => undefined)

  const ping = (): Promise<PingResult> => requestAs({ kind: "ping", requestId: nextId() }, parsePingResult)

  async function teardown(): Promise<void> {
    if (disposed) return
    disposed = true
    if (respawnTimer !== undefined) {
      clearTimeout(respawnTimer)
      respawnTimer = undefined
    }
    clearReadyTimer()
    // A caller awaiting `ready` must not hang when teardown wins the race.
    failReady(new BrokerRequestError("crashed", "broker torn down"))
    if (alive && port) {
      // Best-effort graceful teardown frame, bounded at 1.5s; a missing reply is
      // not fatal because the worker is terminated below either way.
      await requests.request(() => ({ kind: "teardown", requestId: nextId() }), 1500).catch(() => undefined)
    }
    alive = false
    // Reject every remaining in-flight request and refuse later ones.
    requests.dispose()
    for (const child of mirror.values()) reaper.reap(child.pgid)
    mirror.clear()
    port?.close()
    port = undefined
    await Promise.resolve(worker?.terminate()).catch(() => undefined)
  }

  const onNotification = (handler: (notification: NotificationLike) => void): (() => void) => {
    notificationHandlers.add(handler)
    return () => notificationHandlers.delete(handler)
  }

  const onSupervision = (handler: (event: SupervisionEvent) => void): (() => void) => {
    supervisionHandlers.add(handler)
    return () => supervisionHandlers.delete(handler)
  }

  return {
    ready,
    get generation() {
      return generation
    },
    get threadId() {
      return threadId
    },
    get alive() {
      return alive
    },
    transport: "message-port",
    get handshake() {
      return handshake.slice()
    },
    workerHandle,
    portHandle,
    get children() {
      return [...mirror.values()]
    },
    acquire,
    call,
    cancel,
    oauth,
    release,
    ping,
    teardown,
    onNotification,
    onSupervision,
    [Symbol.asyncDispose]: teardown,
  }
}

// ---------------------------------------------------------------------------
// Worker-facing proxy (plan step 19).
//
// A session worker must not spawn MCP/LSP children, so the hub exposes its
// broker over a transferred `MessagePort` with `serveBrokerPort`. The worker
// pairs it with `createWorkerBrokerClient` (`../worker/broker-client`). Requests
// are parsed at this boundary with `parseBrokerRequest`; replies are the
// protocol's `ResponseFrame`s, carrying either a trusted value or a coded error.
// ---------------------------------------------------------------------------

/** The broker surface `serveBrokerPort` drives; `BrokerClient` satisfies it. */
export interface BrokerBackend {
  acquire(input: AcquireInput): Promise<AcquireResult>
  call(input: CallInput): Promise<unknown>
  cancel(targetId: string): Promise<void>
  oauth(directory: string): Promise<OAuthResult>
  release(directory: string): Promise<void>
  ping(): Promise<PingResult>
  teardown(): Promise<void>
}

/** Dispatch one parsed request to the hub broker; results are re-parsed so the
 * wire value is always the trusted protocol shape. */
function executeBrokerRequest(broker: BrokerBackend, request: BrokerRequest): unknown {
  switch (request.kind) {
    case "acquire":
      return broker.acquire({ directory: request.directory, servers: request.servers }).then(parseAcquireResult)
    case "call":
      return broker.call({
        directory: request.directory,
        server: request.server,
        method: request.method,
        ...(request.params === undefined ? {} : { params: request.params }),
        ...(request.timeoutMs === undefined ? {} : { timeoutMs: request.timeoutMs }),
      })
    case "cancel":
      return broker.cancel(request.targetId)
    case "release":
      return broker.release(request.directory)
    case "oauth":
      return broker.oauth(request.directory).then(parseOAuthResult)
    case "ping":
      return broker.ping().then(parsePingResult)
    case "teardown":
      return broker.teardown()
    default: {
      const exhausted: never = request
      void exhausted
      return Promise.reject(new BrokerFailure("invalid-request", "unknown broker request kind"))
    }
  }
}

/**
 * Serve worker broker calls on `port`. Returns a disposer that detaches the
 * listener. A malformed frame is answered with an `invalid-request` error
 * correlated by its `requestId` (or `"unknown"`); a rejected handler becomes a
 * coded error frame. Replies are guarded: a port that closed between request
 * and reply drops the frame instead of crashing the hub.
 */
export function serveBrokerPort(broker: BrokerBackend, port: MessagePort): () => void {
  const channel = portFromMessagePort<unknown>(port)

  const reply = (frame: ResponseFrame): void => {
    try {
      channel.post(frame)
    } catch {
      // The port is gone (worker terminated); drop the reply rather than crash.
    }
  }

  return servePort(
    channel,
    parseBrokerRequest,
    (request) => executeBrokerRequest(broker, request),
    (raw, outcome) => {
      // An unparseable frame still carries its own `requestId` when it has one.
      const requestId = outcome.request?.requestId ?? rawRequestId(raw)
      if (outcome.ok) reply({ kind: "response", requestId, ok: true, value: outcome.value ?? null })
      else reply({ kind: "response", requestId, ok: false, error: toBrokerError(outcome.error) })
    },
  )
}

export * as Broker from "./broker"
