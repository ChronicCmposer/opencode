// Hub-side egress receiver over the events envelope (spec Q6/Q16/Q21).
//
// The hub end of a worker's dedicated egress port. Every incoming message is one
// shared-envelope JSON string, decoded by the same codec as every other worker
// port. Event frames are handed to `onFrame` in arrival order — which is commit
// order for a single worker — and durable `(aggregateID, seq)` pairs are checked
// for per-aggregate order, so an out-of-order or gapped frame within one
// aggregate fails loud while interleaved aggregates never look gapped. The
// `end`/`resync`/`ack` control
// frames are surfaced distinctly rather than swallowed.
//
// Backpressure (spec Q16): when the downstream consumer reports pressure, a
// non-durable delta is DROPPED and a resync is surfaced, so a slow hub never
// blocks the worker. Durable events are never dropped.

import {
  type AggregatePosition,
  type EventsAckPayload,
  type EventsFrame,
  type EventsResyncPayload,
  ProtocolError,
  decodeFrame,
  durableMetaOf,
} from "./frame"

type EventFrame = Extract<EventsFrame, { readonly kind: "event" }>

/**
 * Raised when a durable event violates PER-AGGREGATE order: a repeated/lower
 * seq, or a gap in that aggregate's sequence. Named so callers can distinguish
 * it from a framing error and decide to fail loud or resync.
 */
export class DurableOrderError extends Error {
  constructor(
    readonly reason: "out-of-order" | "gap",
    got: AggregatePosition,
    last: AggregatePosition,
  ) {
    super(`${reason} durable egress: got ${got.aggregateID}#${got.seq}, last ${last.aggregateID}#${last.seq}`)
    this.name = "DurableOrderError"
  }
}

export interface EgressReceiverOptions {
  readonly port: MessagePort
  /** Called with each delivered event frame and the exact JSON text it arrived as. */
  readonly onFrame: (frame: EventsFrame, text: string) => void
  /** Called once when the worker's `end` frame arrives. */
  readonly onEnd?: () => void
  /** Called for a received `resync` frame, or `undefined` for an overflow resync. */
  readonly onResync?: (payload: EventsResyncPayload | undefined) => void
  /** Called for a received `ack` frame. */
  readonly onAck?: (payload: EventsAckPayload | undefined) => void
  /** Called once on the first framing/ordering error; the receiver then stops. */
  readonly onError?: (error: Error) => void
  /**
   * Durable-gap policy: when supplied, a gap/out-of-order frame is surfaced here
   * (with the position that arrived and the last accepted one) and the port
   * STAYS OPEN so the consumer can heal from a database snapshot. Without it the
   * receiver keeps its fail-loud contract and closes the port.
   */
  readonly onDurableGap?: (got: AggregatePosition, last: AggregatePosition) => void
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
  /** Last accepted durable position for one aggregate, or `undefined`. */
  lastDurable(aggregateID: string): AggregatePosition | undefined
  close(): void
}

/** Every worker port message is a shared-envelope JSON string. */
function asText(data: unknown): string {
  if (typeof data === "string") return data
  throw new ProtocolError(
    `egress receiver expected a JSON string, got: ${Object.prototype.toString.call(data)}`,
  )
}

// Durable order is checked FOR EACH AGGREGATE: a repeated/lower seq is
// out-of-order, a seq that skips ahead is a gap, and interleaving different
// aggregates is never a gap. Duplicates/stale frames are dropped without a
// resync when a gap policy exists (and fail loud otherwise); a gap surfaces the
// policy and still advances the observed cursor.
export function createEgressReceiver(options: EgressReceiverOptions): EgressReceiver {
  const { port } = options
  let received = 0
  let dropped = 0
  let resyncs = 0
  let ended = false
  let lastError: Error | undefined
  const lastByAggregate = new Map<string, AggregatePosition>()

  const fail = (error: Error): void => {
    if (lastError) return
    lastError = error
    options.onError?.(error)
    port.close()
  }

  const handleEvent = (frame: EventFrame, text: string): void => {
    const meta = durableMetaOf(frame)
    if (meta) {
      const last = lastByAggregate.get(meta.aggregateID)
      if (last !== undefined) {
        if (meta.seq <= last.seq) {
          // A duplicate/stale frame is idempotent and must never rewind the
          // per-aggregate cursor or trigger a resync. Without a gap policy the
          // receiver keeps its fail-loud contract.
          if (options.onDurableGap === undefined) {
            throw new DurableOrderError("out-of-order", meta, last)
          }
          return
        }
        if (meta.seq !== last.seq + 1) {
          // A gap is recoverable when the consumer owns a resync policy: surface
          // it and keep the stream alive. The observed position still advances so
          // the frames after the gap continue to deliver in order.
          if (options.onDurableGap === undefined) throw new DurableOrderError("gap", meta, last)
          resyncs++
          options.onDurableGap(meta, last)
        }
      }
      lastByAggregate.set(meta.aggregateID, meta)
      received++
      options.onFrame(frame, text)
      return
    }
    if (options.pressure?.() === true) {
      dropped++
      resyncs++
      options.onResync?.(undefined)
      return
    }
    received++
    options.onFrame(frame, text)
  }

  const handleFrame = (frame: EventsFrame, text: string): void => {
    switch (frame.kind) {
      case "event":
        handleEvent(frame, text)
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
      const text = asText(event.data)
      handleFrame(decodeFrame(text), text)
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
      // JSON messages are atomic on a `MessagePort`; nothing is ever buffered.
      return 0
    },
    get lastError() {
      return lastError
    },
    lastDurable: (aggregateID) => lastByAggregate.get(aggregateID),
    close: () => port.close(),
  }
}

export * as EgressReceiverModule from "./receiver"
