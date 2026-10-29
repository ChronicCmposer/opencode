// Shared worker port plumbing.
//
// The cancel control channel (`cancel.ts`), the writer relay
// all adapt a real
// `MessagePort` to a framed request/response channel, and all need the same
// unref'd timer, error-message coercion and peer lifecycle lines. This module is
// the one implementation they share, so the port glue is not duplicated per
// protocol. (The background-job channel was removed with the per-session worker
// refactor; its `job` envelope port name is retained in `frame.ts` for wire
// compatibility only.)
//
// `createRequestChannel` is deliberately dependency-free: it depends only on the
// `FrameChannel` shape and the `PortScheduler` seam declared here, so importing
// it never drags in a protocol or creates an import cycle. The lifecycle helpers
// additionally use the structured logger, which is a leaf module.

import { Deferred, Effect, Schema } from "effect"
import type { EffectBridge } from "@/effect/bridge"
import { workerLogMessage } from "./worker-logging"
import {
  ProtocolError,
  WORKER_FRAME_VERSION,
  assertWorkerFrameSize,
  decodeWorkerFrame,
  encodeWorkerFrame,
  type WorkerFramePort,
} from "./frame"

/** Timer seam shared by dispatch and cancel; unref'd. */
export type PortScheduler = (delayMs: number, run: () => void) => () => void

export const defaultSchedule: PortScheduler = (delayMs, run) => {
  const timer = setTimeout(run, delayMs)
  ;(timer as { unref?: () => void }).unref?.()
  return () => clearTimeout(timer)
}

/** Coerce an unknown thrown value to a message, for error frames. */
export const messageOf = (error: unknown): string => (error instanceof Error ? error.message : String(error))

/**
 * The `_tag` (or a non-generic `name`) of a thrown tagged error, when it carries
 * one. The executor frame and the hub-proxy wire both stamp a tagged error with
 * its tag, so this is the single reader they share rather than re-deriving it.
 * A plain `Error` has `name === "Error"`, which is not a domain tag, so it is
 * reported as untagged.
 */
export function errorTagOf(error: unknown): string | undefined {
  if (typeof error !== "object" || error === null) return undefined
  const tag = (error as { _tag?: unknown })._tag
  if (typeof tag === "string") return tag
  const name = (error as { name?: unknown }).name
  return typeof name === "string" && name !== "Error" ? name : undefined
}

/**
 * Resolve a worker isolate entry: a runtime env override first, then the
 * build-time define, then the caller's source-relative fallback. The packaged
 * binary cannot resolve a source path (`new URL("./x.ts", import.meta.url)`
 * becomes `/$bunfs/root/x.ts`), so the build injects a define; this is the one
 * resolution order every worker shares.
 *
 * `fallback` is passed in rather than constructed here so it resolves against the
 * caller's `import.meta.url`.
 */
export function resolveWorkerEntry(input: {
  readonly env?: Record<string, string | undefined>
  readonly definedPath?: string
  readonly envVar: string
  readonly fallback: string | URL
}): string | URL {
  const fromEnv = (input.env ?? process.env)[input.envVar]
  const resolved = fromEnv ?? input.definedPath
  return resolved === undefined || resolved === "" ? input.fallback : resolved
}

/** The frame-agnostic channel shape a `MessagePort` is adapted to. */
export interface FrameChannel<Frame> {
  post(frame: Frame): void
  onMessage(handler: (frame: Frame) => void): () => void
}

/**
 * Adapt a real `MessagePort` to `FrameChannel`. Incoming messages are cast to
 * `Frame` at this boundary; `port.start()` is called so a fresh port receives.
 */
export function portFromMessagePort<Frame>(port: MessagePort): FrameChannel<Frame> {
  return {
    post: (frame) => port.postMessage(frame),
    onMessage: (handler) => {
      const listener = (event: MessageEvent) => handler(event.data as Frame)
      port.addEventListener("message", listener)
      port.start()
      return () => port.removeEventListener("message", listener)
    },
  }
}

/** The minimum shape every enveloped frame carries for correlation. */
export interface WorkerFrameCarrier {
  readonly kind: string
  readonly id?: number
}

/**
 * Adapt a `MessagePort` whose frames share the versioned worker envelope. Outbound
 * frames are encoded to ONE JSON string and size-checked; inbound messages are
 * decoded exactly once with the frame Schema at this boundary, so a frame from
 * another port or with a mismatched payload discriminant fails loud instead of
 * being blindly cast. There is no structured-clone fallback.
 */
export interface WorkerFrameChannelOptions<Frame = unknown> {
  /**
   * Host-boundary containment: receive a decode failure as a typed error instead
   * of an exception escaping the message listener. When supplied, the channel
   * isolates itself after the first failure (no further frames are delivered),
   * so one malformed worker frame can never take the host down.
   */
  readonly onProtocolError?: (error: unknown) => void
  /**
   * Decode the family payload once at the port boundary. A throw is contained by
   * the same host boundary as a malformed envelope.
   */
  readonly decodePayload?: (payload: unknown) => Frame
  /**
   * Validate an outbound frame before it is encoded. Left unset on the egress
   * hot path, where the encoder's plain fast path is deliberate. An untrusted
   * control boundary (hub-proxy) supplies `assertJsonRepresentable` so a
   * function or symbol payload fails loud instead of being silently dropped by
   * `JSON.stringify`.
   */
  readonly validateOutbound?: (frame: Frame) => void
}

export function workerFrameChannel<Frame extends WorkerFrameCarrier>(
  port: MessagePort,
  portName: WorkerFramePort,
  options: WorkerFrameChannelOptions<Frame> = {},
): FrameChannel<Frame> {
  const channel = portFromMessagePort<string>(port)
  return {
    post: (frame) => {
      options.validateOutbound?.(frame)
      const text = encodeWorkerFrame({
        v: WORKER_FRAME_VERSION,
        port: portName,
        kind: frame.kind,
        ...(frame.id === undefined ? {} : { id: frame.id }),
        payload: frame,
      })
      channel.post(assertWorkerFrameSize(text))
    },
    onMessage: (handler) => {
      let unsubscribe = (): void => {}
      unsubscribe = channel.onMessage((data) => {
        try {
          const envelope = decodeWorkerFrame(data)
          if (envelope.port !== portName) {
            throw new ProtocolError(`expected worker port "${portName}" (got ${JSON.stringify(envelope.port)})`)
          }
          const payload = envelope.payload
          if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
            throw new ProtocolError(`frame "${envelope.kind}" is missing a matching payload`)
          }
          if ((payload as { readonly kind?: unknown }).kind !== envelope.kind) {
            throw new ProtocolError(`frame "${envelope.kind}" is missing a matching payload`)
          }
          handler(options.decodePayload === undefined ? (payload as Frame) : options.decodePayload(payload))
        } catch (error) {
          if (options.onProtocolError === undefined) throw error
          unsubscribe()
          options.onProtocolError(error)
        }
      })
      return unsubscribe
    },
  }
}

// ---------------------------------------------------------------------------
// Request/response correlation
// ---------------------------------------------------------------------------

/**
 * A reply deadline fired before the peer answered. The request id identifies the
 * frame still outstanding, so a caller can correlate the timeout with the wire.
 */
export class WorkerReplyTimeout extends Schema.TaggedErrorClass<WorkerReplyTimeout>()(
  "WorkerPort.ReplyTimeout",
  {
    id: Schema.Number,
    timeoutMs: Schema.Number,
  },
) {
  override get message() {
    return `worker request ${this.id} timed out after ${this.timeoutMs}ms`
  }
}

export interface RequestChannelOptions<Frame, Reply extends Frame, Id> {
  /** Narrow an inbound frame to a correlated reply; non-replies are ignored. */
  readonly isReply: (frame: Frame) => frame is Reply
  /** Read the correlation id from a reply. */
  readonly idOf: (reply: Reply) => Id
  /** Mint the next correlation id for an outbound request. */
  readonly nextId: () => Id
  /** Default per-request timeout; `undefined` disables timeouts. */
  readonly timeoutMs?: number
  /** Build the failure used once the channel is disposed. */
  readonly disposedError: () => unknown
  /** Build the failure used when a request times out. */
  readonly timeoutError: (id: Id, timeoutMs: number) => unknown
  /** Map a `post` failure before failing the request; defaults to the raw thrown error. */
  readonly mapPostError?: (error: unknown) => unknown
  /**
   * Reject a request whose id is already in flight, instead of silently
   * overwriting its waiter. Omitted callers keep the historical overwrite
   * behaviour; a caller with caller-supplied ids supplies it.
   */
  readonly duplicateError?: (id: Id) => unknown
  /**
   * Gate the actual `post` behind a transport becoming ready, so the deadline
   * also covers the wait for the transport. The frame is built and registered
   * immediately; `post` runs once this resolves, or the request fails with the
   * transport's failure. Omitted callers post inline.
   */
  readonly awaitPost?: () => Promise<void>
}

/**
 * A framed request/response channel: `request` correlates a reply by id,
 * `onFrame` observes every inbound frame (for unsolicited pushes), `fail`
 * completes every in-flight `Deferred` with one failure without tearing the
 * channel down, and `dispose` fails every in-flight request permanently.
 */
export interface RequestChannel<Frame, Reply extends Frame, Id> {
  request(build: (id: Id) => Frame, timeoutMs?: number, id?: Id): Effect.Effect<Reply, unknown>
  onFrame(handler: (frame: Frame) => void): () => void
  /** Fail every in-flight request with `error`, leaving the channel usable. */
  fail(error: unknown): void
  dispose(): void
}

/**
 * Build a request/response channel over `channel`. Each in-flight request is a
 * `Deferred`: the message handler completes the matching one, a deadline is
 * `Effect.timeoutOrElse`, and a disconnect fails every pending `Deferred` in one
 * place. The frame codec stays with the caller.
 */
export function createRequestChannel<Frame, Reply extends Frame, Id>(
  channel: FrameChannel<Frame>,
  options: RequestChannelOptions<Frame, Reply, Id>,
): RequestChannel<Frame, Reply, Id> {
  const pending = new Map<Id, Deferred.Deferred<Reply, unknown>>()
  const observers = new Set<(frame: Frame) => void>()
  let disposed = false

  const unsubscribe = channel.onMessage((frame) => {
    // Snapshot before delivering: an observer may unsubscribe (or subscribe)
    // while handling a frame, and that must not perturb this delivery.
    for (const observer of Array.from(observers)) observer(frame)
    if (!options.isReply(frame)) return
    const id = options.idOf(frame)
    const waiter = pending.get(id)
    if (waiter === undefined) return
    pending.delete(id)
    Deferred.doneUnsafe(waiter, Effect.succeed(frame))
  })

  const mapPostError = options.mapPostError ?? ((error: unknown) => error)

  /** Complete every pending waiter with `error`; the map is the one place. */
  const failPending = (error: unknown): void => {
    const waiters = Array.from(pending.values())
    pending.clear()
    for (const waiter of waiters) Deferred.doneUnsafe(waiter, Effect.fail(error))
  }

  const request = (
    build: (id: Id) => Frame,
    timeoutMs = options.timeoutMs,
    id = options.nextId(),
  ): Effect.Effect<Reply, unknown> =>
    Effect.suspend(() => {
      if (disposed) return Effect.fail(options.disposedError())
      if (options.duplicateError !== undefined && pending.has(id)) {
        return Effect.fail(options.duplicateError(id))
      }
      const waiter = Deferred.makeUnsafe<Reply, unknown>()
      pending.set(id, waiter)
      const fail = (error: unknown): void => {
        pending.delete(id)
        Deferred.doneUnsafe(waiter, Effect.fail(error))
      }
      const post = Effect.try({
        try: () => {
          if (!pending.has(id)) return
          channel.post(build(id))
        },
        catch: mapPostError,
      }).pipe(Effect.tapError((error) => Effect.sync(() => fail(error))), Effect.ignore)
      const send = options.awaitPost === undefined
        ? post
        : Effect.tryPromise({ try: () => options.awaitPost!(), catch: mapPostError }).pipe(
            Effect.tapError((error) => Effect.sync(() => fail(error))),
            Effect.ignore,
            Effect.andThen(post),
          )
      const awaited = send.pipe(Effect.andThen(Deferred.await(waiter)))
      const timed =
        timeoutMs === undefined
          ? awaited
          : awaited.pipe(
              Effect.timeoutOrElse({
                duration: timeoutMs,
                orElse: () => Effect.fail(options.timeoutError(id, timeoutMs)),
              }),
            )
      return timed.pipe(Effect.ensuring(Effect.sync(() => pending.delete(id))))
    })

  return {
    request,
    onFrame(handler) {
      if (disposed) return () => {}
      observers.add(handler)
      return () => observers.delete(handler)
    },
    fail: failPending,
    dispose() {
      if (disposed) return
      disposed = true
      unsubscribe()
      failPending(options.disposedError())
      observers.clear()
    },
  }
}

// ---------------------------------------------------------------------------
// Request/reply server loop
// ---------------------------------------------------------------------------

/** The outcome of serving one inbound frame: its value, or the error it failed with. */
export type ServeOutcome<Request, Value> =
  | { readonly ok: true; readonly request: Request; readonly value: Value }
  | { readonly ok: false; readonly request: Request | undefined; readonly error: unknown }

/**
 * Serve a request/reply loop over `channel`: parse each raw frame, execute the
 * trusted request, and hand the outcome to `respond`. A parse failure is
 * delivered with `request: undefined`, so the caller correlates the error reply
 * from the raw frame instead. `respond` runs only after the execute promise
 * settles, so a rejected execute is answered — never an unhandled rejection.
 */
export function servePort<Request, Value>(
  channel: FrameChannel<unknown>,
  parse: (raw: unknown) => Request,
  execute: (request: Request) => Value | Promise<Value>,
  respond: (raw: unknown, outcome: ServeOutcome<Request, Value>) => void,
): () => void {
  return channel.onMessage((raw) => {
    let request: Request
    try {
      request = parse(raw)
    } catch (error) {
      respond(raw, { ok: false, request: undefined, error })
      return
    }
    void Promise.resolve()
      .then(() => execute(request))
      .then(
        (value) => respond(raw, { ok: true, request, value }),
        (error: unknown) => respond(raw, { ok: false, request, error }),
      )
  })
}

/** Read a string `requestId` from a raw frame, or `"unknown"` when it is absent. */
export function rawRequestId(raw: unknown): string {
  const requestId = (raw as { requestId?: unknown } | null)?.requestId
  return typeof requestId === "string" ? requestId : "unknown"
}

// ---------------------------------------------------------------------------
// Peer lifecycle logging
// ---------------------------------------------------------------------------

/**
 * The lifecycle log fields for one peer at a given severity. Returned as a
 * function of `level` so a single call site can emit both the INFO `connected`
 * / `closed` lines and the WARN `connect failed` line from one source.
 */
export function peerLifecycle(
  peer: string,
  workerID: number | undefined,
): (level: "info" | "warn") => Record<string, unknown> {
  return (level) => ({ peer, level, ...(workerID === undefined ? {} : { workerID }) })
}

/**
 * Build a port-backed client, emitting the peer lifecycle lines: an INFO
 * `${peer} connected` once `build` succeeds, or a WARN `${peer} connect failed`
 * (with the error `reason`) before rethrowing. `build` is where the port is
 * started, so a start failure surfaces here.
 */
export function connectPeer<T>(
  bridge: EffectBridge.Shape | undefined,
  peer: string,
  workerID: number | undefined,
  build: () => T,
): T {
  const lifecycle = peerLifecycle(peer, workerID)
  try {
    const client = build()
    void bridge?.fork(Effect.logInfo(workerLogMessage(`${peer} connected`), lifecycle("info")))
    return client
  } catch (error) {
    void bridge?.fork(
      Effect.logWarning(workerLogMessage(`${peer} connect failed`), { ...lifecycle("warn"), reason: messageOf(error) }),
    )
    throw error
  }
}

export * as WorkerPort from "./port"
