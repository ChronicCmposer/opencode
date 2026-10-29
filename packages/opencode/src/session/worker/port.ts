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

export type PortScheduler = (delayMs: number, run: () => void) => () => void

export const defaultSchedule: PortScheduler = (delayMs, run) => {
  const timer = setTimeout(run, delayMs)
  ;(timer as { unref?: () => void }).unref?.()
  return () => clearTimeout(timer)
}

export const messageOf = (error: unknown): string => (error instanceof Error ? error.message : String(error))

export function errorTagOf(error: unknown): string | undefined {
  if (typeof error !== "object" || error === null) return undefined
  const tag = (error as { _tag?: unknown })._tag
  if (typeof tag === "string") return tag
  const name = (error as { name?: unknown }).name
  return typeof name === "string" && name !== "Error" ? name : undefined
}

// The packaged binary cannot resolve a source path (`new URL("./x.ts", import.meta.url)` becomes `/$bunfs/root/x.ts`), so the build injects a define; this is the one resolution order every worker shares.
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

export interface FrameChannel<Frame> {
  post(frame: Frame): void
  onMessage(handler: (frame: Frame) => void): () => void
}

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

export interface WorkerFrameCarrier {
  readonly kind: string
  readonly id?: number
}

// Outbound frames are encoded to ONE JSON string and size-checked; inbound messages are decoded exactly once with the frame Schema at this boundary, so a frame from another port or with a mismatched payload discriminant fails loud instead of being blindly cast.
export interface WorkerFrameChannelOptions<Frame = unknown> {
  // When supplied, the channel isolates itself after the first failure (no further frames are delivered), so one malformed worker frame can never take the host down.
  readonly onProtocolError?: (error: unknown) => void
  readonly decodePayload?: (payload: unknown) => Frame
  // Validate an outbound frame before it is encoded.
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
  readonly isReply: (frame: Frame) => frame is Reply
  readonly idOf: (reply: Reply) => Id
  readonly nextId: () => Id
  readonly timeoutMs?: number
  readonly disposedError: () => unknown
  readonly timeoutError: (id: Id, timeoutMs: number) => unknown
  /** Map a `post` failure before failing the request; defaults to the raw thrown error. */
  readonly mapPostError?: (error: unknown) => unknown
  readonly duplicateError?: (id: Id) => unknown
  readonly awaitPost?: () => Promise<void>
}

export interface RequestChannel<Frame, Reply extends Frame, Id> {
  request(build: (id: Id) => Frame, timeoutMs?: number, id?: Id): Effect.Effect<Reply, unknown>
  onFrame(handler: (frame: Frame) => void): () => void
  fail(error: unknown): void
  dispose(): void
}

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


export type ServeOutcome<Request, Value> =
  | { readonly ok: true; readonly request: Request; readonly value: Value }
  | { readonly ok: false; readonly request: Request | undefined; readonly error: unknown }

// `respond` runs only after the execute promise settles, so a rejected execute is answered — never an unhandled rejection.
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

export function rawRequestId(raw: unknown): string {
  const requestId = (raw as { requestId?: unknown } | null)?.requestId
  return typeof requestId === "string" ? requestId : "unknown"
}


export function peerLifecycle(
  peer: string,
  workerID: number | undefined,
): (level: "info" | "warn") => Record<string, unknown> {
  return (level) => ({ peer, level, ...(workerID === undefined ? {} : { workerID }) })
}

// Build a port-backed client, emitting the peer lifecycle lines: an INFO `${peer} connected` once `build` succeeds, or a WARN `${peer} connect failed` (with the error `reason`) before rethrowing.
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
