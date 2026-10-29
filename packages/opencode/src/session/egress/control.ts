// Dedicated control channel.

import type { AggregatePosition, EventV2Event } from "./frame"

/** Permissive control-frame envelope: the transport base every frame satisfies. */
export interface ControlEnvelope {
  readonly type: string
  readonly id?: number
  readonly [key: string]: unknown
}

/** Every typed control frame. */
export type ControlFrame =
  | { readonly type: "ping"; readonly id: number }
  | { readonly type: "pong"; readonly id: number }
  | { readonly type: "listen"; readonly id: number; readonly pattern: string }
  | { readonly type: "unlisten"; readonly id: number; readonly pattern: string }
  | { readonly type: "listen.ok"; readonly id: number }
  | { readonly type: "durable"; readonly meta: AggregatePosition; readonly event: EventV2Event }
  | { readonly type: "resync"; readonly pattern: string; readonly meta?: AggregatePosition }
  | { readonly type: "durable.ack"; readonly count: number }
  | { readonly type: "resync.ack"; readonly pattern?: string }

/** Hub → worker control frames. */
export type ControlOutbound = Extract<ControlFrame, { readonly type: "ping" | "listen.ok" | "durable" | "resync" }>

/** Worker → hub control frames. */
export type ControlInbound = Exclude<ControlFrame, ControlOutbound>

/** A request frame: the link stamps `id`, so callers may omit it. */
type DistributiveOmit<T, K extends PropertyKey> = T extends unknown ? Omit<T, K> : never

/** The frames a caller may send as a request: only request/reply pairs. */
type ControlRequestFrame = Extract<ControlFrame, { readonly type: "ping" | "listen" | "unlisten" }>
export type ControlRequest = DistributiveOmit<ControlRequestFrame, "id"> & { readonly id?: number }

/** A non-negative integer — the only valid delivery/resync acknowledgement count. */
export function isNonNegativeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value) && value >= 0
}

export interface ControlLink {
  /** Send a request and resolve with the reply carrying the same `id`. */
  request(frame: ControlRequest): Promise<ControlFrame>
  /** Send a one-way notification. */
  notify(frame: ControlEnvelope): void
  /** Register the handler for inbound frames that are not replies. */
  onMessage(handler: (frame: ControlFrame) => void): void
  dispose(): void
  readonly lastError: Error | undefined
}

export interface ControlLinkOptions {
  /** Reject a request that receives no reply within this many ms. */
  readonly timeoutMs?: number
}

const DISPOSED_ERROR = "control link disposed"

interface PendingRequest {
  resolve(frame: ControlFrame): void
  reject(error: Error): void
  readonly timer: ReturnType<typeof setTimeout> | undefined
}

/** The `type` discriminants the typed {@link ControlFrame} union understands. */
const KNOWN_CONTROL_TYPES: ReadonlySet<string> = new Set([
  "ping",
  "pong",
  "listen",
  "unlisten",
  "listen.ok",
  "durable",
  "resync",
  "durable.ack",
  "resync.ack",
])

function isControlRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

/** A numeric correlation id — the only id shape a request/reply frame carries. */
function controlId(record: Record<string, unknown>): number | undefined {
  const id = record.id
  return typeof id === "number" && Number.isInteger(id) ? id : undefined
}

/** The EventV2 event `{ id, type, properties }` a `durable` frame must carry. */
function isEventV2(value: unknown): value is EventV2Event {
  return isControlRecord(value) && typeof value.id === "string" && typeof value.type === "string" && "properties" in value
}

/** The `{ aggregateID, seq }` durable identity a `durable` frame must carry. */
function isAggregatePositionShape(value: unknown): value is AggregatePosition {
  return (
    isControlRecord(value) &&
    typeof value.aggregateID === "string" &&
    value.aggregateID.length > 0 &&
    Number.isInteger(value.seq) &&
    (value.seq as number) >= 0
  )
}

/** True when `data` carries one of the typed frame discriminants. */
function isKnownControlType(data: unknown): boolean {
  return isControlRecord(data) && typeof data.type === "string" && KNOWN_CONTROL_TYPES.has(data.type)
}

/** Parse an untrusted port message into a trusted {@link ControlFrame}. */
function parseControlFrame(data: unknown): ControlFrame | undefined {
  if (!isControlRecord(data) || typeof data.type !== "string") return undefined
  switch (data.type) {
    case "ping": {
      const id = controlId(data)
      return id === undefined ? undefined : { type: "ping", id }
    }
    case "pong": {
      const id = controlId(data)
      return id === undefined ? undefined : { type: "pong", id }
    }
    case "listen.ok": {
      const id = controlId(data)
      return id === undefined ? undefined : { type: "listen.ok", id }
    }
    case "listen": {
      const id = controlId(data)
      return id === undefined || typeof data.pattern !== "string"
        ? undefined
        : { type: "listen", id, pattern: data.pattern }
    }
    case "unlisten": {
      const id = controlId(data)
      return id === undefined || typeof data.pattern !== "string"
        ? undefined
        : { type: "unlisten", id, pattern: data.pattern }
    }
    case "durable":
      return isAggregatePositionShape(data.meta) && isEventV2(data.event)
        ? { type: "durable", meta: data.meta, event: data.event }
        : undefined
    case "resync": {
      if (typeof data.pattern !== "string") return undefined
      if (data.meta === undefined) return { type: "resync", pattern: data.pattern }
      return isAggregatePositionShape(data.meta)
        ? { type: "resync", pattern: data.pattern, meta: data.meta }
        : undefined
    }
    case "durable.ack":
      return isNonNegativeInteger(data.count) ? { type: "durable.ack", count: data.count } : undefined
    case "resync.ack":
      if (data.pattern === undefined) return { type: "resync.ack" }
      return typeof data.pattern === "string" ? { type: "resync.ack", pattern: data.pattern } : undefined
    default:
      return data as unknown as ControlFrame
  }
}

export function createControlLink(port: MessagePort, options: ControlLinkOptions = {}): ControlLink {
  const timeoutMs = options.timeoutMs
  const pending = new Map<number, PendingRequest>()
  let handler: ((frame: ControlFrame) => void) | undefined
  let nextId = 1
  let lastError: Error | undefined
  let disposed = false

  const clearPending = (error: Error): void => {
    for (const waiter of pending.values()) {
      if (waiter.timer !== undefined) clearTimeout(waiter.timer)
      waiter.reject(error)
    }
    pending.clear()
  }

  port.onmessage = (event: MessageEvent) => {
    const frame = parseControlFrame(event.data)
    if (frame === undefined) {
      // A malformed KNOWN frame is a protocol violation: surface it loudly
      // instead of dropping it. Unknown / forward-compatible frames are ignored.
      if (isKnownControlType(event.data)) {
        lastError = new Error(`invalid control frame: ${JSON.stringify(event.data)}`)
      }
      return
    }
    if ("id" in frame && typeof frame.id === "number") {
      const waiter = pending.get(frame.id)
      if (waiter) {
        pending.delete(frame.id)
        if (waiter.timer !== undefined) clearTimeout(waiter.timer)
        waiter.resolve(frame)
        return
      }
    }
    if (!handler) return
    try {
      handler(frame)
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error))
    }
  }
  port.start()

  return {
    request(frame) {
      if (disposed) return Promise.reject(new Error(DISPOSED_ERROR))
      const id = nextId++
      return new Promise<ControlFrame>((resolve, reject) => {
        const timer =
          timeoutMs === undefined
            ? undefined
            : setTimeout(() => {
                if (!pending.delete(id)) return
                reject(new Error(`control request ${id} timed out after ${timeoutMs}ms`))
              }, timeoutMs)
        pending.set(id, { resolve, reject, timer })
        try {
          port.postMessage({ ...frame, id })
        } catch (error) {
          pending.delete(id)
          if (timer !== undefined) clearTimeout(timer)
          reject(error instanceof Error ? error : new Error(String(error)))
        }
      })
    },
    notify: (frame) => {
      if (disposed) return
      try {
        port.postMessage(frame)
      } catch (error) {
        // A closed or hostile port must not escape as an opaque throw: record it as the link's last error and
        // let the caller decide (the mesh rolls back the delivery it was accounting for).
        const failure = error instanceof Error ? error : new Error(String(error))
        lastError = failure
        throw failure
      }
    },
    onMessage: (next) => {
      handler = next
    },
    dispose: () => {
      if (disposed) return
      disposed = true
      port.close()
      clearPending(new Error(DISPOSED_ERROR))
    },
    get lastError() {
      return lastError
    },
  }
}

export * as EgressControl from "./control"
