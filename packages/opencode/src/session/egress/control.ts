// Phase 8.13 — dedicated control channel (plan task 8.13).
//
// Cancel, `listen`, durable-wake and resync must never queue behind a burst of
// delta frames, so a SEPARATE `MessagePort` carries control traffic for every
// worker (plan decision line 88). This module is the shared request/reply +
// notification plumbing both ends use; the concrete EventV2 mesh protocol lives
// in `mesh.ts`.

import type { EgressEvent } from "./frame"
import type { WriterOrdinal } from "./ordinal"

/**
 * Permissive control-frame envelope: the transport base every frame satisfies.
 *
 * Kept structurally open (`[key: string]: unknown`) so a protocol can carry its
 * own fields and so unknown / forward-compatible frames survive the boundary.
 * Prefer the typed {@link ControlFrame} union for consumer-side handling.
 */
export interface ControlEnvelope {
  readonly type: string
  readonly id?: number
  readonly [key: string]: unknown
}

/**
 * Every typed control frame (plan task 8.13).
 *
 * `id` is present on requests and their replies; `durable` / `resync` are
 * hub→worker pushes; `durable.ack` / `resync.ack` acknowledge them back.
 */
export type ControlFrame =
  | { readonly type: "ping"; readonly id: number }
  | { readonly type: "pong"; readonly id: number }
  | { readonly type: "listen"; readonly id: number; readonly pattern: string }
  | { readonly type: "unlisten"; readonly id: number; readonly pattern: string }
  | { readonly type: "listen.ok"; readonly id: number }
  | { readonly type: "durable"; readonly meta: WriterOrdinal; readonly event: EgressEvent }
  | { readonly type: "resync"; readonly pattern: string }
  | { readonly type: "durable.ack"; readonly count: number }
  | { readonly type: "resync.ack"; readonly pattern?: string }

/** Hub → worker control frames. */
export type ControlOutbound = Extract<ControlFrame, { readonly type: "ping" | "listen.ok" | "durable" | "resync" }>

/** Worker → hub control frames. */
export type ControlInbound = Exclude<ControlFrame, ControlOutbound>

/** A request frame: the link stamps `id`, so callers may omit it. */
type DistributiveOmit<T, K extends PropertyKey> = T extends unknown ? Omit<T, K> : never

/**
 * The frames a caller may send as a request: only request/reply pairs. Replies
 * (`pong`, `listen.ok`) and push-only frames (`durable`, `resync`, `durable.ack`,
 * `resync.ack`) are unrepresentable here — pushes go through {@link ControlLink.notify}.
 */
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

/**
 * Shape check for the port boundary: accepts any object carrying a string
 * `type`, so unknown / forward-compatible frames are still forwarded to
 * `onMessage` rather than dropped. A KNOWN frame with a malformed body (an ack
 * whose `count` is not a non-negative integer) is rejected here so it never
 * enters internal logic as a trusted {@link ControlFrame}.
 */
function isControlFrame(data: unknown): data is ControlFrame {
  if (typeof data !== "object" || data === null || !("type" in data)) return false
  if (typeof data.type !== "string") return false
  if (data.type === "durable.ack") return isNonNegativeInteger((data as { readonly count?: unknown }).count)
  return true
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
    const data: unknown = event.data
    if (!isControlFrame(data)) {
      // A malformed KNOWN frame is a protocol violation: surface it loudly
      // instead of dropping it. Unknown / forward-compatible frames are ignored.
      if (typeof data === "object" && data !== null && "type" in data && typeof data.type === "string") {
        lastError = new Error(`invalid control frame: ${JSON.stringify(data)}`)
      }
      return
    }
    if ("id" in data && typeof data.id === "number") {
      const waiter = pending.get(data.id)
      if (waiter) {
        pending.delete(data.id)
        if (waiter.timer !== undefined) clearTimeout(waiter.timer)
        waiter.resolve(data)
        return
      }
    }
    if (!handler) return
    try {
      handler(data)
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
      if (!disposed) port.postMessage(frame)
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
