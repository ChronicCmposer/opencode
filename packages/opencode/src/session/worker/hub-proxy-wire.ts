// The hub-proxy control wire: the request/reply protocol a session worker uses
// to reach the hub-resident `SessionStatus` / `Permission` / `Question`
// authorities over one multiplexed `MessagePort`.
//
// This module is deliberately dependency-free apart from the shared port
// helpers: both the worker (`layer.ts`) and the hub (`hub.ts`) import it, so it
// must not reach back into either. Extracting it removes the old
// `hub → layer → run-state → hub` import cycle and the duplicated hub-side
// `serveHubProxy` workaround.

import { messageOf, portFromMessagePort } from "./port"
import type { JobInfo } from "./job-registry"
import type { SubagentDispatchRequest } from "./subagent"

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

export type HubProxyFrame = HubProxyRequest | HubProxyReply

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
  | "dispatchSubagent"

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
  readonly call: (method: HubProxyMethod, args: readonly unknown[]) => Promise<unknown>
  /**
   * The typed subagent-dispatch RPC (Solution 1). Optional so lightweight test
   * doubles (`{ call, dispose }`) remain assignable; the multiplexed client
   * always installs it.
   */
  readonly dispatchSubagent?: (request: SubagentDispatchRequest) => Promise<JobInfo>
  dispose(): void
}

interface HubPending {
  readonly resolve: (frame: HubProxyReply) => void
  readonly reject: (error: unknown) => void
}

/**
 * Worker side: a thin request/reply client over a hub-proxy `MessagePort`.
 * Requests carry a numeric `id` correlated by the client; replies echo it. Only
 * replies are addressed to the client, so request frames are ignored.
 */
export function createHubProxyClient(port: MessagePort): HubProxyClient {
  const channel = portFromMessagePort<HubProxyFrame>(port)
  let nextId = 1
  let disposed = false
  const pending = new Map<number, HubPending>()

  const unsubscribe = channel.onMessage((frame) => {
    if (frame.kind !== "reply") return
    const waiter = pending.get(frame.id)
    if (waiter === undefined) return
    pending.delete(frame.id)
    waiter.resolve(frame)
  })

  const call = (method: HubProxyMethod, args: readonly unknown[]): Promise<unknown> => {
    if (disposed) return Promise.reject(new HubProxyRemoteError("hub proxy client disposed"))
    const id = nextId++
    return new Promise<unknown>((resolve, reject) => {
      pending.set(id, {
        resolve: (frame) =>
          frame.ok
            ? resolve(frame.value)
            : reject(new HubProxyRemoteError(frame.message, { tag: frame.tag, data: frame.data })),
        reject,
      })
      try {
        channel.post({ kind: "request", id, method, args })
      } catch (error) {
        pending.delete(id)
        reject(error)
      }
    })
  }

  return {
    call,
    dispatchSubagent: (request) => call("dispatchSubagent", [request]) as Promise<JobInfo>,
    dispose() {
      if (disposed) return
      disposed = true
      unsubscribe()
      for (const waiter of pending.values()) waiter.reject(new HubProxyRemoteError("hub proxy client disposed"))
      pending.clear()
    },
  }
}

export type HubProxyHandlers = Readonly<Partial<Record<HubProxyMethod, (...args: readonly unknown[]) => unknown>>>

/** Own fields not worth carrying: the message rides the reply, the rest are noise. */
const UNSERIALISED_ERROR_FIELDS = new Set(["message", "name", "stack", "cause", "_tag"])

/** The `_tag` (or `name`) of a thrown tagged error, when it has one. */
function errorTagOf(error: unknown): string | undefined {
  if (error === null || typeof error !== "object") return undefined
  const tag = (error as { _tag?: unknown })._tag ?? (error as { name?: unknown }).name
  return typeof tag === "string" ? tag : undefined
}

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

/** Hub side: serve hub-proxy requests against `handlers`; returns a disposer. */
export function serveHubProxy(handlers: HubProxyHandlers, port: MessagePort): () => void {
  const channel = portFromMessagePort<HubProxyFrame>(port)
  return channel.onMessage((frame) => {
    if (frame.kind !== "request") return
    const reply = (payload: HubProxyReply): void => {
      try {
        channel.post(payload)
      } catch {
        // Drop the reply: the worker is gone.
      }
    }
    const handler = handlers[frame.method as HubProxyMethod]
    if (handler === undefined) {
      reply({ kind: "reply", id: frame.id, ok: false, message: `unknown hub-proxy method "${frame.method}"` })
      return
    }
    Promise.resolve()
      .then(() => handler(...frame.args))
      .then(
        (value) => reply({ kind: "reply", id: frame.id, ok: true, value: value ?? null }),
        (error) => {
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
        },
      )
  })
}
