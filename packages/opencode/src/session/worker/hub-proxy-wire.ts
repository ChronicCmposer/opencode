// The hub-proxy control wire: the request/reply protocol a session worker uses to reach the hub-resident `SessionStatus` / `Permission` / `Question`.

import { Effect, Option, Schema } from "effect"
import { createRequestChannel, errorTagOf, messageOf, workerFrameChannel } from "./port"
import { ProtocolError, assertJsonRepresentable } from "./frame"
import { parseJson } from "@/util/safe-json"
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
      /** The thrown error's `_tag` (or `name`) when it is a tagged domain error. */
      readonly tag?: string
      /** The thrown error's own serialisable fields (e.g. `feedback`, `ruleset`). */
      readonly data?: unknown
    }

/** An unsolicited hub → worker push correlated to an in-flight request's `id`. */
export type HubProxyEventFrame = {
  readonly kind: "event"
  readonly id: number
  readonly method: string
  readonly payload: unknown
}

export type HubProxyFrame = HubProxyRequest | HubProxyReply | HubProxyEventFrame

/** The wire shape, decoded once at the port. */
const HubProxyFrameSchema = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("request"),
    id: Schema.Int,
    method: Schema.Unknown,
    args: Schema.Unknown,
  }),
  Schema.Struct({
    kind: Schema.Literal("reply"),
    id: Schema.Int,
    ok: Schema.Boolean,
    value: Schema.optional(Schema.Unknown),
    message: Schema.optional(Schema.String),
    tag: Schema.optional(Schema.String),
    data: Schema.optional(Schema.Unknown),
  }),
  Schema.Struct({
    kind: Schema.Literal("event"),
    id: Schema.Int,
    method: Schema.String,
    payload: Schema.Unknown,
  }),
])

const decodeHubProxyFrameOption = Schema.decodeUnknownOption(HubProxyFrameSchema)

/** Decode one hub-proxy payload, failing the port on a structurally invalid frame. */
export function decodeHubProxyFrame(input: unknown): HubProxyFrame {
  const decoded = decodeHubProxyFrameOption(input)
  if (Option.isNone(decoded)) throw new ProtocolError("hub-proxy frame does not match the wire schema")
  return decoded.value as HubProxyFrame
}

/** The namespaced hub-proxy methods. */
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
  | "pluginTrigger"
  | "pluginTool.execute"
  | "pluginTool.list"
  | "pluginTool.abort"
  | "backgroundJob.register"
  | "backgroundJob.settle"
  | "backgroundJob.awaitPromotion"
  | "session.spawn"
  | "session.create"
  | "session.get"
  | "session.children"
  | "session.cancel"
  | "session.discardReverted"
  | "lifecycle.commit"
  | "lifecycle.phase"
  | "lifecycle.toolStarted"
  | "lifecycle.toolFinished"

/** A typed failure returned by a hub authority across the port. */
export class HubProxyRemoteError extends Schema.TaggedErrorClass<HubProxyRemoteError>("HubProxyRemoteError")(
  "HubProxy.Remote",
  {
    message: Schema.String,
    tag: Schema.optional(Schema.String),
    data: Schema.optional(Schema.Unknown),
  },
) {
  constructor(message: string, options: { tag?: string; data?: unknown } = {}) {
    super({ message, tag: options.tag, data: options.data })
    this.name = "HubProxyRemoteError"
  }
}

export interface HubProxyClient {
  /** Call a hub authority. */
  readonly call: (
    method: HubProxyMethod,
    args: readonly unknown[],
    onEvent?: (method: string, payload: unknown) => void,
  ) => Promise<unknown>
  /** The Effect form of {@link call}; optional so `{ call, dispose }` test doubles stay assignable. */
  readonly callEffect?: (
    method: HubProxyMethod,
    args: readonly unknown[],
    onEvent?: (method: string, payload: unknown) => void,
  ) => Effect.Effect<unknown, unknown>
  /** The typed session-spawn RPC. */
  readonly spawn?: (request: SessionSpawnRequest) => Promise<SessionSpawnResult>
  dispose(): void
}

export interface HubProxyClientOptions {
  /** Default per-request timeout in ms. */
  readonly timeoutMs?: number
}

const DISPOSED_MESSAGE = "hub proxy client disposed"

/** Worker side: a thin request/reply client over a hub-proxy `MessagePort`. */
export function createHubProxyClient(port: MessagePort, options: HubProxyClientOptions = {}): HubProxyClient {
  // Requests carry untrusted plugin/model values.
  const channel = workerFrameChannel<HubProxyFrame>(port, "hub-proxy", {
    decodePayload: decodeHubProxyFrame,
    validateOutbound: assertJsonRepresentable,
  })
  let nextId = 1

  const requests = createRequestChannel<HubProxyFrame, HubProxyReply, number>(channel, {
    timeoutMs: options.timeoutMs,
    isReply: (frame): frame is HubProxyReply => frame.kind === "reply",
    idOf: (reply) => reply.id,
    nextId: () => nextId++,
    disposedError: () => new HubProxyRemoteError(DISPOSED_MESSAGE),
    timeoutError: (id, timeoutMs) => new HubProxyRemoteError(`hub proxy request ${id} timed out after ${timeoutMs}ms`),
  })

  // The `onEvent` of each in-flight request, keyed by its correlation id.
  const eventHandlers = new Map<number, (method: string, payload: unknown) => void>()
  requests.onFrame((frame) => {
    if (frame.kind !== "event") return
    eventHandlers.get(frame.id)?.(frame.method, frame.payload)
  })

  const callEffect = (
    method: HubProxyMethod,
    args: readonly unknown[],
    onEvent?: (method: string, payload: unknown) => void,
  ): Effect.Effect<unknown, unknown> =>
    Effect.suspend(() => {
      // The handler is unregistered once this request settles; it is registered before the request is posted, so no event can slip past it.
      let id: number | undefined
      return requests
        .request((requestId) => {
          id = requestId
          if (onEvent !== undefined) eventHandlers.set(requestId, onEvent)
          return { kind: "request", id: requestId, method, args }
        })
        .pipe(
          Effect.flatMap((reply) =>
            reply.ok
              ? Effect.succeed(reply.value)
              : Effect.fail(new HubProxyRemoteError(reply.message, { tag: reply.tag, data: reply.data })),
          ),
          Effect.ensuring(
            Effect.sync(() => {
              if (id !== undefined) eventHandlers.delete(id)
            }),
          ),
        )
    })

  const call = (
    method: HubProxyMethod,
    args: readonly unknown[],
    onEvent?: (method: string, payload: unknown) => void,
  ): Promise<unknown> => Effect.runPromise(callEffect(method, args, onEvent))

  return {
    call,
    callEffect,
    spawn: (request) => call("session.spawn", [request]) as Promise<SessionSpawnResult>,
    dispose: () => requests.dispose(),
  }
}

export type HubProxyHandlers = Readonly<Partial<Record<HubProxyMethod, (...args: readonly unknown[]) => unknown>>>

/** A streaming handler's push channel: `emit` posts an unsolicited {@link HubProxyEventFrame} correlated to the request currently being served. */
export interface HubProxyEventSink {
  emit(method: string, payload: unknown): void
}

/** A streaming hub handler. */
export type HubProxyStreamHandler = (sink: HubProxyEventSink, args: readonly unknown[]) => unknown

/** Own fields not worth carrying: the message rides the reply, the rest are noise. */
const UNSERIALISED_ERROR_FIELDS = new Set(["message", "name", "stack", "cause", "_tag"])

/** A tagged error's own JSON-safe fields, or `undefined` when there are none. */
function errorDataOf(error: unknown): unknown {
  if (error === null || typeof error !== "object") return undefined
  try {
    const fields = Object.entries(error).filter(([key]) => !UNSERIALISED_ERROR_FIELDS.has(key))
    if (fields.length === 0) return undefined
    const text = JSON.stringify(Object.fromEntries(fields))
    return text === undefined ? undefined : parseJson(text)
  } catch {
    return undefined
  }
}

/** Own-property lookup, so an inherited `Object.prototype` name never matches. */
function ownLookup<T>(record: Readonly<Record<string, T>> | undefined, key: string): T | undefined {
  return record !== undefined && Object.hasOwn(record, key) ? record[key] : undefined
}

/** Hub side: serve hub-proxy requests against `handlers`; returns a disposer. */
export interface ServeHubProxyOptions {
  /** Host-boundary containment: a malformed inbound frame is reported here as a typed error and the poisoned control channel is isolated instead of. */
  readonly onProtocolError?: (error: unknown) => void
}

export function serveHubProxy(
  handlers: HubProxyHandlers,
  port: MessagePort,
  streaming?: Readonly<Record<string, HubProxyStreamHandler>>,
  options: ServeHubProxyOptions = {},
): () => void {
  const channel = workerFrameChannel<HubProxyFrame>(port, "hub-proxy", {
    decodePayload: decodeHubProxyFrame,
    ...(options.onProtocolError === undefined ? {} : { onProtocolError: options.onProtocolError }),
  })
  return channel.onMessage((frame) => {
    if (frame.kind !== "request") return
    // Validate the untrusted request payload before touching any handler.
    const id = frame.id
    if (typeof id !== "number" || !Number.isInteger(id) || id < 0) {
      throw new ProtocolError(`hub-proxy request id must be a non-negative integer (got ${JSON.stringify(id)})`)
    }
    const method = frame.method
    const args = frame.args
    const reply = (payload: HubProxyReply): void => {
      try {
        channel.post(payload)
      } catch {
        // Either the peer is gone or `payload.value` is not JSON-serialisable.
        if (!payload.ok) return
        try {
          channel.post({
            kind: "reply",
            id: payload.id,
            ok: false,
            message: `hub-proxy reply value for id ${payload.id} is not JSON-serialisable (not structured-cloneable)`,
          })
        } catch {
        }
      }
    }
    if (typeof method !== "string" || method.length === 0) {
      reply({ kind: "reply", id, ok: false, message: `hub-proxy request ${id} method must be a non-empty string` })
      return
    }
    if (!Array.isArray(args)) {
      reply({ kind: "reply", id, ok: false, message: `hub-proxy request ${id} args must be an array` })
      return
    }
    const succeed = (value: unknown): void => {
      const safe = value ?? null
      try {
        assertJsonRepresentable(safe)
      } catch {
        reply({
          kind: "reply",
          id,
          ok: false,
          message: `hub-proxy reply value for id ${id} is not JSON-serialisable (not structured-cloneable)`,
        })
        return
      }
      reply({ kind: "reply", id, ok: true, value: safe })
    }
    const fail = (error: unknown): void => {
      const tag = errorTagOf(error)
      const data = errorDataOf(error)
      reply({
        kind: "reply",
        id,
        ok: false,
        message: messageOf(error),
        ...(tag === undefined ? {} : { tag }),
        ...(data === undefined ? {} : { data }),
      })
    }
    // `emit` pushes an unsolicited event correlated to this request.
    const emit = (eventMethod: string, payload: unknown): void => {
      try {
        assertJsonRepresentable(payload)
        channel.post({ kind: "event", id, method: eventMethod, payload })
      } catch {
        reply({
          kind: "reply",
          id,
          ok: false,
          message: `hub-proxy event payload for id ${id} is not JSON-serialisable (not structured-cloneable)`,
        })
      }
    }
    // Own-property guard: a bare lookup would resolve inherited `Object.prototype` members (`toString`, `valueOf`.
    const streamHandler = ownLookup(streaming, method)
    if (streamHandler !== undefined) {
      Promise.resolve()
        .then(() => streamHandler({ emit }, args))
        .then(succeed, fail)
      return
    }
    const handler = Object.hasOwn(handlers, method) ? handlers[method as HubProxyMethod] : undefined
    if (handler === undefined) {
      reply({ kind: "reply", id, ok: false, message: `unknown hub-proxy method "${method}"` })
      return
    }
    Promise.resolve()
      .then(() => handler(...args))
      .then(succeed, fail)
  })
}
