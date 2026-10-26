// The hub-proxy control wire: the request/reply protocol a session worker uses
// to reach the hub-resident `SessionStatus` / `Permission` / `Question`
// authorities over one multiplexed `MessagePort`.
//
// This module is deliberately dependency-free apart from the shared port
// helpers: both the worker (`layer.ts`) and the hub (`hub.ts`) import it, so it
// must not reach back into either. Extracting it removes the old
// `hub → layer → run-state → hub` import cycle and the duplicated hub-side
// `serveHubProxy` workaround.

import { createRequestChannel, errorTagOf, messageOf, workerFrameChannel } from "./port"
import type { SessionSpawnRequest, SessionSpawnResult } from "./subagent"

export type HubProxyRequest = {
  readonly kind: "request"
  readonly id: number
  readonly method: string
  readonly args: readonly unknown[]
}

export type HubProxyReply =
  | { readonly kind: "reply"; readonly id: number; readonly ok: true; readonly value: unknown }
  | {
      readonly kind: "reply"
      readonly id: number
      readonly ok: false
      readonly message: string
      /**
       * The thrown error's `_tag` (or `name`) when it is a tagged domain error.
       * Carried so the worker can rebuild the original error class — a bare
       * message would erase the `instanceof` identity callers branch on.
       */
      readonly tag?: string
      /** The thrown error's own serialisable fields (e.g. `feedback`, `ruleset`). */
      readonly data?: unknown
    }

/**
 * An unsolicited hub → worker push correlated to an in-flight request's `id`.
 * A streaming handler emits these through its {@link HubProxyEventSink}; the
 * client routes them to that request's `onEvent`, and drops any whose id is not
 * (or is no longer) in flight.
 */
export type HubProxyEventFrame = {
  readonly kind: "event"
  readonly id: number
  readonly method: string
  readonly payload: unknown
}

export type HubProxyFrame = HubProxyRequest | HubProxyReply | HubProxyEventFrame

/**
 * The namespaced hub-proxy methods. One worker control port multiplexes the
 * `SessionStatus` / `Permission` / `Question` authorities, so each method names
 * its authority explicitly (`question.ask` vs `permission.ask`) — a misrouted
 * call cannot type-check, and no request-shape guessing is needed on the hub.
 */
export type HubProxyMethod =
  | "sessionStatus.get"
  | "sessionStatus.list"
  | "sessionStatus.set"
  | "permission.ask"
  | "permission.reply"
  | "permission.list"
  | "question.ask"
  | "question.reply"
  | "question.reject"
  | "question.list"
  | "systemTransform"
  | "messagesTransform"
  | "pluginTool.execute"
  | "pluginTool.list"
  | "pluginTool.abort"
  | "session.spawn"

/** A typed failure returned by a hub authority across the port. */
export class HubProxyRemoteError extends Error {
  /**
   * The remote error's `_tag` when it was a tagged domain error, else
   * `undefined`. Lets a caller rebuild the original error class.
   */
  readonly tag: string | undefined
  /** The remote error's own serialisable fields (e.g. `feedback`, `ruleset`). */
  readonly data: unknown

  constructor(message: string, options: { tag?: string; data?: unknown } = {}) {
    super(message)
    this.name = "HubProxyRemoteError"
    this.tag = options.tag
    this.data = options.data
  }
}

export interface HubProxyClient {
  /**
   * Call a hub authority. An optional `onEvent` receives unsolicited
   * {@link HubProxyEventFrame} pushes correlated to this request's id while it
   * is in flight; events for a settled or unknown id are dropped.
   */
  readonly call: (
    method: HubProxyMethod,
    args: readonly unknown[],
    onEvent?: (method: string, payload: unknown) => void,
  ) => Promise<unknown>
  /**
   * The typed session-spawn RPC. Optional so lightweight test doubles
   * (`{ call, dispose }`) remain assignable; the multiplexed client always
   * installs it.
   */
  readonly spawn?: (request: SessionSpawnRequest) => Promise<SessionSpawnResult>
  dispose(): void
}

export interface HubProxyClientOptions {
  /**
   * Default per-request timeout in ms. `undefined` (the default) waits for a
   * reply indefinitely, matching the historical behaviour; when set, an
   * unanswered request rejects with a `HubProxyRemoteError`.
   */
  readonly timeoutMs?: number
}

const DISPOSED_MESSAGE = "hub proxy client disposed"

/**
 * Worker side: a thin request/reply client over a hub-proxy `MessagePort`.
 * Requests carry a numeric `id` correlated by the client; replies echo it. Only
 * replies are addressed to the client, so request frames are ignored.
 *
 * Correlation, timeout and dispose plumbing live in the shared
 * `createRequestChannel`; this module only supplies the frame codec and error
 * mapping.
 */
export function createHubProxyClient(port: MessagePort, options: HubProxyClientOptions = {}): HubProxyClient {
  const channel = workerFrameChannel<HubProxyFrame>(port, "hub-proxy")
  let nextId = 1

  const requests = createRequestChannel<HubProxyFrame, HubProxyReply, number>(channel, {
    timeoutMs: options.timeoutMs,
    isReply: (frame): frame is HubProxyReply => frame.kind === "reply",
    idOf: (reply) => reply.id,
    nextId: () => nextId++,
    disposedError: () => new HubProxyRemoteError(DISPOSED_MESSAGE),
    timeoutError: (id, timeoutMs) =>
      new HubProxyRemoteError(`hub proxy request ${id} timed out after ${timeoutMs}ms`),
  })

  // The `onEvent` of each in-flight request, keyed by its correlation id. An
  // event frame is only ever routed here, so one whose id is absent (settled or
  // never requested) is dropped rather than surfaced.
  const eventHandlers = new Map<number, (method: string, payload: unknown) => void>()
  requests.onFrame((frame) => {
    if (frame.kind !== "event") return
    eventHandlers.get(frame.id)?.(frame.method, frame.payload)
  })

  const call = async (
    method: HubProxyMethod,
    args: readonly unknown[],
    onEvent?: (method: string, payload: unknown) => void,
  ): Promise<unknown> => {
    // Capture the minted id so the handler is unregistered once this request
    // settles — before the request is even posted, so no event can slip past it.
    let id: number | undefined
    try {
      const reply = await requests.request((requestId) => {
        id = requestId
        if (onEvent !== undefined) eventHandlers.set(requestId, onEvent)
        return { kind: "request", id: requestId, method, args }
      })
      if (reply.ok) return reply.value
      throw new HubProxyRemoteError(reply.message, { tag: reply.tag, data: reply.data })
    } finally {
      if (id !== undefined) eventHandlers.delete(id)
    }
  }

  return {
    call,
    spawn: (request) => call("session.spawn", [request]) as Promise<SessionSpawnResult>,
    dispose: () => requests.dispose(),
  }
}

export type HubProxyHandlers = Readonly<Partial<Record<HubProxyMethod, (...args: readonly unknown[]) => unknown>>>

/**
 * A streaming handler's push channel: `emit` posts an unsolicited
 * {@link HubProxyEventFrame} correlated to the request currently being served,
 * so the caller's `onEvent` observes it while still awaiting the reply.
 */
export interface HubProxyEventSink {
  emit(method: string, payload: unknown): void
}

/**
 * A streaming hub handler. Unlike a {@link HubProxyHandlers} entry it may push
 * any number of correlated events through `sink` before returning (or resolving
 * to) the value that becomes the request's reply.
 */
export type HubProxyStreamHandler = (sink: HubProxyEventSink, args: readonly unknown[]) => unknown

/** Own fields not worth carrying: the message rides the reply, the rest are noise. */
const UNSERIALISED_ERROR_FIELDS = new Set(["message", "name", "stack", "cause", "_tag"])

/**
 * A tagged error's own structured-clone-safe fields, or `undefined` when there
 * are none. A field that cannot cross the port (a function, an accessor that
 * throws) is dropped rather than failing the whole reply.
 */
function errorDataOf(error: unknown): unknown {
  if (error === null || typeof error !== "object") return undefined
  try {
    const fields = Object.entries(error).filter(([key]) => !UNSERIALISED_ERROR_FIELDS.has(key))
    return fields.length === 0 ? undefined : structuredClone(Object.fromEntries(fields))
  } catch {
    return undefined
  }
}

/** Own-property lookup, so an inherited `Object.prototype` name never matches. */
function ownLookup<T>(record: Readonly<Record<string, T>> | undefined, key: string): T | undefined {
  return record !== undefined && Object.hasOwn(record, key) ? record[key] : undefined
}

/**
 * Hub side: serve hub-proxy requests against `handlers`; returns a disposer.
 *
 * Replying can fail two ways. A genuinely dead peer (the port is gone) drops the
 * reply silently — nothing can be done. But `channel.post` also throws
 * `DataCloneError` synchronously when a SUCCESS reply's `value` is not
 * structured-cloneable (e.g. a plugin hook leaked a function). That case is
 * recoverable: post ONE clone-safe `ok:false` frame with the same `id`, so the
 * worker fails loud with a descriptive error instead of hanging or timing out.
 * Error replies are already clone-safe (`errorDataOf` drops non-cloneable
 * fields), so only a success reply needs the fallback.
 *
 * `streaming` adds an opt-in push channel: a method listed there is served by
 * its {@link HubProxyStreamHandler}, which may `emit` correlated events before
 * replying. Omitting `streaming` leaves the two-argument behaviour unchanged.
 */
export function serveHubProxy(
  handlers: HubProxyHandlers,
  port: MessagePort,
  streaming?: Readonly<Record<string, HubProxyStreamHandler>>,
): () => void {
  const channel = workerFrameChannel<HubProxyFrame>(port, "hub-proxy")
  return channel.onMessage((frame) => {
    if (frame.kind !== "request") return
    const reply = (payload: HubProxyReply): void => {
      try {
        channel.post(payload)
      } catch {
        // Either the peer is gone or `payload.value` is not structured-cloneable.
        // A clone-safe `ok:false` fallback distinguishes the latter for the
        // worker; on a detached port this second post may throw or silently
        // no-op, and either way there is nobody left to hear the fallback.
        if (!payload.ok) return
        try {
          channel.post({
            kind: "reply",
            id: payload.id,
            ok: false,
            message: `hub-proxy reply value for id ${payload.id} is not structured-cloneable`,
          })
        } catch {
          // Peer gone: nothing to do.
        }
      }
    }
    const succeed = (value: unknown): void =>
      reply({ kind: "reply", id: frame.id, ok: true, value: value ?? null })
    const fail = (error: unknown): void => {
      const tag = errorTagOf(error)
      const data = errorDataOf(error)
      reply({
        kind: "reply",
        id: frame.id,
        ok: false,
        message: messageOf(error),
        ...(tag === undefined ? {} : { tag }),
        ...(data === undefined ? {} : { data }),
      })
    }
    // `emit` pushes an unsolicited event correlated to this request. A
    // non-cloneable payload reuses the reply path's fail-loud recovery so the
    // worker learns the real cause instead of silently losing the event.
    const emit = (method: string, payload: unknown): void => {
      try {
        channel.post({ kind: "event", id: frame.id, method, payload })
      } catch {
        reply({
          kind: "reply",
          id: frame.id,
          ok: false,
          message: `hub-proxy event payload for id ${frame.id} is not structured-cloneable`,
        })
      }
    }
    // Own-property guard: a bare lookup would resolve inherited `Object.prototype`
    // members (`toString`, `valueOf`, ...) to functions and bypass the miss below.
    // A streaming method wins over a plain handler of the same name.
    const streamHandler = ownLookup(streaming, frame.method)
    if (streamHandler !== undefined) {
      Promise.resolve()
        .then(() => streamHandler({ emit }, frame.args))
        .then(succeed, fail)
      return
    }
    const handler = Object.hasOwn(handlers, frame.method) ? handlers[frame.method as HubProxyMethod] : undefined
    if (handler === undefined) {
      reply({ kind: "reply", id: frame.id, ok: false, message: `unknown hub-proxy method "${frame.method}"` })
      return
    }
    Promise.resolve()
      .then(() => handler(...frame.args))
      .then(succeed, fail)
  })
}
