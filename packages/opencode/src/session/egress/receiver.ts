// Hub-side egress receiver over the events envelope (spec Q6/Q16/Q21).
//
// The hub end of a worker's dedicated egress port. Every incoming message is one
// shared-envelope JSON string, decoded ONCE at the port boundary by the same
// codec as every other worker port (`decodeFrame` -> `decodeWorkerFrame`, so the
// 4 MiB cap and safe-json prototype guards apply). The listener is registered by
// a `Stream.callback` + `Effect.acquireRelease`, and the decoded messages are
// driven through `Stream.runForEach` so event delivery, blob resolution, gap
// healing, `end`, `resync`, and `ack` all observe arrival order — which is commit
// order for a single worker.
//
// Durable `(aggregateID, seq)` pairs are checked for per-aggregate order, so an
// out-of-order or gapped frame within one aggregate fails loud while interleaved
// aggregates never look gapped. The `end`/`resync`/`ack` control frames are
// surfaced distinctly rather than swallowed.
//
// Backpressure (spec Q16): when the downstream consumer reports pressure, a
// non-durable delta is DROPPED and a resync is surfaced, so a slow hub never
// blocks the worker. Durable events are never dropped.

import { Cause, Effect, Queue, Stream } from "effect"

import {
  type AggregatePosition,
  type DecodedEventsFrame,
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

export interface EgressPortOptions {
  /** Called with each delivered event frame and the exact JSON text it arrived as. */
  readonly onFrame: (frame: EventsFrame, text: string) => unknown
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
   * STAYS OPEN so the consumer can heal from a database snapshot. The handler may
   * return an Effect so healing completes before the gapped frame is delivered.
   * Without it the receiver keeps its fail-loud contract and closes the port.
   */
  readonly onDurableGap?: (got: AggregatePosition, last: AggregatePosition) => unknown
  /**
   * Backpressure signal: when it returns `true`, non-durable deltas are dropped
   * (and a resync surfaced) instead of delivered. Durable events always deliver.
   */
  readonly pressure?: () => boolean
}

export interface EgressReceiverOptions extends EgressPortOptions {
  readonly port: MessagePort
}

/** Live counters for one decoded events port. */
export interface EgressPortStats {
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
}

export interface EgressPortConnection {
  readonly stats: EgressPortStats
  /**
   * The ordered stream program. It fails only for a fatal port error, which has
   * already been reported through `onError`; run it with the hub's runner.
   */
  readonly program: Effect.Effect<void, Error>
  /** Synchronous, idempotent teardown: detach the listener and close the port once. */
  readonly close: () => void
}

/**
 * Decode one worker port message into either an events frame or a typed failure,
 * never parsing JSON ad hoc. Framing/ordering failures are surfaced to the
 * ordered stream so they are reported exactly once before the port closes.
 */
type PortMessage =
  | { readonly _tag: "frame"; readonly frame: DecodedEventsFrame; readonly text: string }
  | { readonly _tag: "failure"; readonly error: Error }

/** Every worker port message is a shared-envelope JSON string. */
function asText(data: unknown): string {
  if (typeof data === "string") return data
  throw new ProtocolError(
    `egress receiver expected a JSON string, got: ${Object.prototype.toString.call(data)}`,
  )
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error))
}

/**
 * Build the single live events-port pipeline. The returned `program` decodes each
 * message at the port, then sequentially:
 *   * enforces per-aggregate durable order (duplicate/stale drop, gap policy),
 *   * applies non-durable backpressure,
 *   * surfaces `end`/`resync`/`ack`,
 *   * reports the first malformed frame once and closes the port.
 */
export function createEgressPort(port: MessagePort, options: EgressPortOptions): EgressPortConnection {
  let received = 0
  let dropped = 0
  let resyncs = 0
  let ended = false
  let lastError: Error | undefined
  let released = false
  const lastByAggregate = new Map<string, AggregatePosition>()

  /** Detach the listener and close the port exactly once, whether by error or dispose. */
  const releasePort = (): void => {
    if (released) return
    released = true
    port.onmessage = null
    port.close()
  }

  const fail = (error: Error): Effect.Effect<never, Error> => {
    if (lastError === undefined) {
      lastError = error
      options.onError?.(error)
    }
    releasePort()
    return Effect.fail(error)
  }

  /** A handler may return an Effect (async resolution/healing); anything else is sync. */
  const settle = (result: unknown): Effect.Effect<void, Error> => {
    if (!Effect.isEffect(result)) return Effect.void
    return Effect.asVoid(result as Effect.Effect<unknown, Error, never>)
  }

  const handleEvent = (frame: EventFrame, text: string): Effect.Effect<void, Error> =>
    Effect.gen(function* () {
      // The canonical durability test (present only when BOTH `aggregateID` and
      // `seq` are set), shared with the sender and fan-out so the three never
      // drift on exactly which frames are durable.
      const meta = durableMetaOf(frame)
      if (meta !== undefined) {
        const last = lastByAggregate.get(meta.aggregateID)
        if (last !== undefined) {
          if (meta.seq <= last.seq) {
            // A duplicate/stale frame is idempotent and must never rewind the
            // per-aggregate cursor or trigger a resync. Without a gap policy the
            // receiver keeps its fail-loud contract.
            if (options.onDurableGap === undefined) {
              yield* fail(new DurableOrderError("out-of-order", meta, last))
              return
            }
            return
          }
          if (meta.seq !== last.seq + 1) {
            // A gap is recoverable when the consumer owns a resync policy:
            // surface it, await any healing, and keep the stream alive. The
            // observed position still advances so later frames deliver in order.
            if (options.onDurableGap === undefined) {
              yield* fail(new DurableOrderError("gap", meta, last))
              return
            }
            resyncs++
            yield* settle(options.onDurableGap(meta, last))
          }
        }
        lastByAggregate.set(meta.aggregateID, meta)
        received++
        yield* settle(options.onFrame(frame, text))
        return
      }
      if (options.pressure?.() === true) {
        dropped++
        resyncs++
        options.onResync?.(undefined)
        return
      }
      received++
      yield* settle(options.onFrame(frame, text))
    })

  const process = (message: PortMessage): Effect.Effect<void, Error> =>
    Effect.gen(function* () {
      if (released || ended || lastError !== undefined) return
      if (message._tag === "failure") {
        yield* fail(message.error)
        return
      }
      const frame = message.frame
      switch (frame.kind) {
        case "event":
          yield* handleEvent(frame, message.text)
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
    })

  const stream = Stream.callback<PortMessage>((queue) => {
    const listener = (event: MessageEvent): void => {
      if (released || ended || lastError !== undefined) return
      const message: PortMessage = (() => {
        try {
          const text = asText(event.data)
          return { _tag: "frame", frame: decodeFrame(text), text }
        } catch (error) {
          return { _tag: "failure", error: asError(error) }
        }
      })()
      try {
        Queue.offerUnsafe(queue, message)
      } catch {
        // The stream already finished (dispose/fatal error); the port is closed,
        // so dropping this late message is the only correct outcome.
      }
    }
    return Effect.acquireRelease(
      Effect.sync(() => {
        if (released) return
        port.onmessage = listener
        port.start()
      }),
      () => Effect.sync(releasePort),
    )
  })

  const stats: EgressPortStats = {
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
  }

  // Every callback — `onFrame`, `onEnd`, `onResync`, `onAck`, `onDurableGap`,
  // and the backpressure probe — runs inside the ordered stream. A synchronous
  // throw surfaces as a defect and a handler Effect that fails surfaces as a
  // typed failure; both are fatal for the port and must be reported through the
  // one `fail` path so `onError`/`lastError` observe the underlying error. An
  // interrupt (dispose or external cancellation) is not a delivery failure: it
  // is re-raised untouched, leaving the port silent while its scope finalizer
  // detaches the listener.
  const reportFatal = (cause: Cause.Cause<Error>): Effect.Effect<never, Error> =>
    Cause.hasInterruptsOnly(cause) ? Effect.failCause(cause) : fail(asError(Cause.squash(cause)))

  return {
    stats,
    // Process one message per scheduler turn: a port message is its own arrival
    // turn, and downstream consumers must be able to drain a delivered frame
    // before a later terminal frame finalizes the stream.
    program: Stream.runForEach(stream, (message) =>
      Effect.gen(function* () {
        yield* process(message)
        yield* Effect.yieldNow
      }),
    ).pipe(Effect.catchCause(reportFatal)),
    close: releasePort,
  }
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

/**
 * Synchronous facade over the one decoded events pipeline, retained for callers
 * that observe the receiver directly (the no-publisher drain and the receiver
 * tests). Production hub serving runs the SAME {@link createEgressPort} program
 * through the hub's `EffectBridge` instead of a private runtime.
 */
export function createEgressReceiver(options: EgressReceiverOptions): EgressReceiver {
  const connection = createEgressPort(options.port, options)
  const fiber = Effect.runFork(connection.program.pipe(Effect.catch(() => Effect.void)))
  return {
    get received() {
      return connection.stats.received
    },
    get dropped() {
      return connection.stats.dropped
    },
    get resyncs() {
      return connection.stats.resyncs
    },
    get ended() {
      return connection.stats.ended
    },
    get pendingBytes() {
      return connection.stats.pendingBytes
    },
    get lastError() {
      return connection.stats.lastError
    },
    lastDurable: (aggregateID) => connection.stats.lastDurable(aggregateID),
    close: () => {
      connection.close()
      fiber.interruptUnsafe()
    },
  }
}
