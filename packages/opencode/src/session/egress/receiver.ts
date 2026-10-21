// W2 — hub-side egress receiver over the events envelope (spec Q6/Q16/Q21).
//
// The hub end of a worker's dedicated egress port. Incoming `Uint8Array` chunks
// are reassembled with the length-prefixed codec (a frame may be split across
// deliveries and a delivery may carry several frames). Event frames are handed to
// `onFrame` in arrival order — which is commit order for a single worker — and
// durable `(generation, ordinal)` pairs are checked for generation-scoped
// monotonicity, so an out-of-order or stale-generation durable frame fails loud
// instead of silently corrupting commit order. The `end`/`resync`/`ack` control
// frames are surfaced distinctly rather than swallowed.
//
// Backpressure (spec Q16): when the downstream consumer reports pressure, a
// non-durable delta is DROPPED and a resync is surfaced, so a slow hub never
// blocks the worker. Durable events are never dropped.

import { type EventsAckPayload, type EventsFrame, type EventsResyncPayload, decodeFrameChunks } from "./frame"
import { compareOrdinals, type WriterOrdinal } from "./ordinal"

type EventFrame = Extract<EventsFrame, { readonly kind: "event" }>

export interface EgressReceiverOptions {
  readonly port: MessagePort
  /** Called with each delivered event frame and the exact wire bytes it arrived as. */
  readonly onFrame: (frame: EventsFrame, bytes: Uint8Array) => void
  /** Called once when the worker's `end` frame arrives. */
  readonly onEnd?: () => void
  /** Called for a received `resync` frame, or `undefined` for an overflow resync. */
  readonly onResync?: (payload: EventsResyncPayload | undefined) => void
  /** Called for a received `ack` frame. */
  readonly onAck?: (payload: EventsAckPayload | undefined) => void
  /** Called once on the first framing/ordering error; the receiver then stops. */
  readonly onError?: (error: Error) => void
  /**
   * Backpressure signal: when it returns `true`, non-durable deltas are dropped
   * (and a resync surfaced) instead of delivered. Durable events always deliver.
   */
  readonly pressure?: () => boolean
}

export interface EgressReceiver {
  /** Number of event frames delivered to `onFrame`. */
  readonly received: number
  /** Number of non-durable deltas dropped under backpressure. */
  readonly dropped: number
  /** Number of resyncs surfaced (received `resync` frames + overflow resyncs). */
  readonly resyncs: number
  readonly ended: boolean
  /** Bytes buffered awaiting a complete frame. */
  readonly pendingBytes: number
  readonly lastError: Error | undefined
  lastOrdinal(): WriterOrdinal | undefined
  close(): void
}

const EMPTY = new Uint8Array(0)

function asBytes(data: unknown): Uint8Array {
  if (data instanceof Uint8Array) return data
  if (data instanceof ArrayBuffer) return new Uint8Array(data)
  if (ArrayBuffer.isView(data)) return new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
  throw new Error(
    `egress receiver expected Uint8Array, ArrayBuffer, or ArrayBufferView, got: ${Object.prototype.toString.call(data)}`,
  )
}

function concat(left: Uint8Array, right: Uint8Array): Uint8Array {
  if (left.byteLength === 0) return right
  const joined = new Uint8Array(left.byteLength + right.byteLength)
  joined.set(left, 0)
  joined.set(right, left.byteLength)
  return joined
}

/** A durable event carries BOTH a generation and an ordinal; a delta carries neither. */
function durableOrdinal(frame: EventFrame): WriterOrdinal | undefined {
  const { generation, ordinal } = frame.payload
  if (generation === undefined || ordinal === undefined) return undefined
  return { generation, ordinal }
}

export function createEgressReceiver(options: EgressReceiverOptions): EgressReceiver {
  const { port } = options
  let rest: Uint8Array = EMPTY
  let received = 0
  let dropped = 0
  let resyncs = 0
  let ended = false
  let lastError: Error | undefined
  let last: WriterOrdinal | undefined

  const fail = (error: Error): void => {
    if (lastError) return
    lastError = error
    options.onError?.(error)
    port.close()
  }

  const handleEvent = (frame: EventFrame, bytes: Uint8Array): void => {
    const meta = durableOrdinal(frame)
    if (meta) {
      if (last && compareOrdinals(meta, last) <= 0) {
        throw new Error(
          `out-of-order durable egress: got ${meta.generation}/${meta.ordinal}, last ${last.generation}/${last.ordinal}`,
        )
      }
      last = meta
      received++
      options.onFrame(frame, bytes)
      return
    }
    if (options.pressure?.() === true) {
      dropped++
      resyncs++
      options.onResync?.(undefined)
      return
    }
    received++
    options.onFrame(frame, bytes)
  }

  const handleFrame = (frame: EventsFrame, bytes: Uint8Array): void => {
    switch (frame.kind) {
      case "event":
        handleEvent(frame, bytes)
        return
      case "end":
        ended = true
        options.onEnd?.()
        return
      case "resync":
        resyncs++
        options.onResync?.(frame.payload)
        return
      case "ack":
        options.onAck?.(frame.payload)
        return
    }
  }

  port.onmessage = (event: MessageEvent) => {
    if (lastError || ended) return
    try {
      const chunk = asBytes(event.data)
      const { frames, rest: nextRest } = decodeFrameChunks(concat(rest, chunk))
      // Commit the trailing bytes only once every frame in the batch is handled:
      // a mid-batch failure must not advance `rest` past undelivered frames.
      for (const { frame, bytes } of frames) handleFrame(frame, bytes)
      rest = nextRest
    } catch (error) {
      fail(error instanceof Error ? error : new Error(String(error)))
    }
  }
  port.start()

  return {
    get received() {
      return received
    },
    get dropped() {
      return dropped
    },
    get resyncs() {
      return resyncs
    },
    get ended() {
      return ended
    },
    get pendingBytes() {
      return rest.byteLength
    },
    get lastError() {
      return lastError
    },
    lastOrdinal: () => last,
    close: () => port.close(),
  }
}

export * as EgressReceiverModule from "./receiver"
