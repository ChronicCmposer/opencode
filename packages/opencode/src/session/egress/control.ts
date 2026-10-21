// Phase 8.13 — dedicated control channel (plan task 8.13).
//
// Cancel, `listen`, durable-wake and resync must never queue behind a burst of
// delta frames, so a SEPARATE `MessagePort` carries control traffic for every
// worker (plan decision line 88). This module is the shared request/reply +
// notification plumbing both ends use; the concrete EventV2 mesh protocol lives
// in `mesh.ts`.

import type { EgressEvent } from "./frame"
import type { WriterOrdinal } from "./ordinal"

/** A control frame; replies echo the request `id`. */
export interface ControlEnvelope {
  readonly type: string
  readonly id?: number
  readonly [key: string]: unknown
}

/** Control frames the worker sends to the hub (requests + notifications). */
export type ControlInbound =
  | { readonly type: "ping"; readonly id: number }
  | { readonly type: "listen"; readonly id: number; readonly pattern: string }
  | { readonly type: "unlisten"; readonly id: number; readonly pattern: string }
  | { readonly type: "resync.ack"; readonly pattern: string }

/** Control frames the hub sends to the worker (replies + notifications). */
export type ControlOutbound =
  | { readonly type: "pong"; readonly id: number }
  | { readonly type: "listen.ok"; readonly id: number }
  | { readonly type: "durable"; readonly meta: WriterOrdinal; readonly event: EgressEvent }
  | { readonly type: "resync"; readonly pattern: string }

export interface ControlLink {
  /** Send a request and resolve with the reply carrying the same `id`. */
  request(frame: { readonly type: string; readonly [key: string]: unknown }): Promise<ControlEnvelope>
  /** Send a one-way notification. */
  notify(frame: ControlEnvelope): void
  /** Register the handler for inbound frames that are not replies. */
  onMessage(handler: (frame: ControlEnvelope) => void): void
  dispose(): void
  readonly lastError: Error | undefined
}

export function createControlLink(port: MessagePort): ControlLink {
  const pending = new Map<number, { resolve: (frame: ControlEnvelope) => void; reject: (error: Error) => void }>()
  let handler: ((frame: ControlEnvelope) => void) | undefined
  let nextId = 1
  let lastError: Error | undefined

  port.onmessage = (event: MessageEvent) => {
    const frame = event.data as ControlEnvelope | undefined
    if (!frame || typeof frame.type !== "string") return
    if (typeof frame.id === "number") {
      const waiter = pending.get(frame.id)
      if (waiter) {
        pending.delete(frame.id)
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
      const id = nextId++
      return new Promise<ControlEnvelope>((resolve, reject) => {
        pending.set(id, { resolve, reject })
        port.postMessage({ ...frame, id })
      })
    },
    notify: (frame) => port.postMessage(frame),
    onMessage: (next) => {
      handler = next
    },
    dispose: () => {
      port.close()
      for (const waiter of pending.values()) waiter.reject(new Error("control link disposed"))
      pending.clear()
    },
    get lastError() {
      return lastError
    },
  }
}

export * as EgressControl from "./control"
