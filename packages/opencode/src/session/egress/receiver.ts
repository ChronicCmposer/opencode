// W2 — hub-side egress receiver over the events envelope (spec Q6/Q16/Q21).
//
// The hub end of a worker's dedicated egress port. Incoming `Uint8Array` chunks
// are reassembled with the length-prefixed codec (a frame may be split across
// deliveries and a delivery may carry several frames). Event frames are handed to
// `onFrame` in arrival order — which is commit order for a single worker — and
// durable `(generation, ordinal)` pairs are checked for generation-scoped order,
// so an out-of-order, stale-generation, or gapped durable frame fails loud
// instead of silently corrupting commit order. The `end`/`resync`/`ack` control
// frames are surfaced distinctly rather than swallowed.
//
// Backpressure (spec Q16): when the downstream consumer reports pressure, a
// non-durable delta is DROPPED and a resync is surfaced, so a slow hub never
// blocks the worker. Durable events are never dropped.

import { type EventsAckPayload, type EventsFrame, type EventsResyncPayload, createFrameDecoder } from "./frame"
// The single durability test lives on the shared envelope; the codec re-exports
// it, but the receiver depends on the canonical module so there is one source.
import { durableMetaOf } from "@/session/worker/frame"
import { compareOrdinals, type WriterOrdinal } from "./ordinal"

type EventFrame = Extract<EventsFrame, { readonly kind: "event" }>

/**
 * Raised when a durable event violates generation-scoped order: a regression
 * (stale generation or repeated/lower ordinal) or a gap in the ordinal sequence
 * within one generation. Named so callers can distinguish it from a framing
 * error and decide to fail loud or resync.
 */
export class DurableOrderError extends Error {
  constructor(
    readonly reason: "out-of-order" | "gap",
    got: WriterOrdinal,
    last: WriterOrdinal,
  ) {
    super(`${reason} durable egress: got ${got.generation}/${got.ordinal}, last ${last.generation}/${last.ordinal}`)
    this.name = "DurableOrderError"
  }
}

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

function asBytes(data: unknown): Uint8Array {
  if (data instanceof Uint8Array) return data
  if (data instanceof ArrayBuffer) return new Uint8Array(data)
  if (ArrayBuffer.isView(data)) return new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
  throw new Error(
    `egress receiver expected Uint8Array, ArrayBuffer, or ArrayBufferView, got: ${Object.prototype.toString.call(data)}`,
  )
}

/**
 * Validate a durable's `(generation, ordinal)` against the last accepted one.
 * A lower generation, or a repeated/lower ordinal within the same generation, is
 * out-of-order; an ordinal that skips ahead within the same generation is a gap.
 * A generation bump resets the sequence, so it is accepted with any ordinal.
 */
function assertDurableOrder(meta: WriterOrdinal, last: WriterOrdinal | undefined): void {
  if (last === undefined) return
  if (meta.generation < last.generation) throw new DurableOrderError("out-of-order", meta, last)
  if (meta.generation !== last.generation) return
  if (compareOrdinals(meta, last) <= 0) throw new DurableOrderError("out-of-order", meta, last)
  if (meta.ordinal !== last.ordinal + 1) throw new DurableOrderError("gap", meta, last)
}

export function createEgressReceiver(options: EgressReceiverOptions): EgressReceiver {
  const { port } = options
  // One preallocated reassembly buffer (see `createFrameDecoder`): a frame split
  // across many deliveries reassembles in linear time, never quadratic.
  const decoder = createFrameDecoder()
  let received = 0
  let dropped = 0
  let resyncs = 0
  let ended = false
  let lastError: Error | undefined
  let last: WriterOrdinal | undefined

  const fail = (error: Error): void => {
    if (lastError) return
    lastError = error
    // The receiver stops here, so buffered bytes can never complete a frame.
    decoder.reset()
    options.onError?.(error)
    port.close()
  }

  const handleEvent = (frame: EventFrame, bytes: Uint8Array): void => {
    const meta = durableMetaOf(frame)
    if (meta) {
      assertDurableOrder(meta, last)
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
      for (const { frame, bytes } of decoder.push(chunk)) handleFrame(frame, bytes)
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
      return decoder.pending
    },
    get lastError() {
      return lastError
    },
    lastOrdinal: () => last,
    close: () => port.close(),
  }
}

export * as EgressReceiverModule from "./receiver"
