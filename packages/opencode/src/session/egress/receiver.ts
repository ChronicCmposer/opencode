// Hub-side egress receiver over the events envelope (spec Q6/Q16/Q21).

import { Cause, Effect, Queue, Schema, Stream } from "effect"

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
 * Raised when a durable event violates PER-AGGREGATE order: a repeated/lower seq, or a gap in that
 * aggregate's sequence.
 */
export class DurableOrderError extends Schema.TaggedErrorClass<DurableOrderError>("DurableOrderError")(
  "Egress.DurableOrder",
  {
    reason: Schema.Literals(["out-of-order", "gap"]),
    got: Schema.Struct({ aggregateID: Schema.String, seq: Schema.Number }),
    last: Schema.Struct({ aggregateID: Schema.String, seq: Schema.Number }),
  },
) {
  constructor(
    override readonly reason: "out-of-order" | "gap",
    override readonly got: AggregatePosition,
    override readonly last: AggregatePosition,
  ) {
    super({ reason, got, last })
    this.name = "DurableOrderError"
  }
  override get message() {
    return `${this.reason} durable egress: got ${this.got.aggregateID}#${this.got.seq}, last ${this.last.aggregateID}#${this.last.seq}`
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
   * Recoverable-frame policy: when supplied, a frame that fails decode/size checks or whose delivery throws
   * is dropped, counted as a resync and reported here, and the port STAYS OPEN. Without it the first such
   * failure is fatal.
   */
  readonly onFrameRejected?: (error: Error) => void
  /**
   * Durable-gap policy: when supplied, a gap/out-of-order frame is surfaced here (with the position that
   * arrived and the last accepted one) and the port STAYS OPEN so the consumer can heal from a database
   * snapshot.
   */
  readonly onDurableGap?: (got: AggregatePosition, last: AggregatePosition) => unknown
  /**
   * Backpressure signal: when it returns `true`, non-durable deltas are dropped (and a resync surfaced)
   * instead of delivered.
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
  /** The ordered stream program. */
  readonly program: Effect.Effect<void, Error>
  /** Synchronous, idempotent teardown: detach the listener and close the port once. */
  readonly close: () => void
}

/**
 * Decode one worker port message into either an events frame or a typed failure, never parsing JSON ad hoc.
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

/** Build the single live events-port pipeline. */
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
      // The canonical durability test (present only when BOTH `aggregateID` and `seq` are set), shared with
      // the sender and fan-out so the three never drift on exactly which frames are durable.
      const meta = durableMetaOf(frame)
      if (meta !== undefined) {
        const last = lastByAggregate.get(meta.aggregateID)
        if (last !== undefined) {
          if (meta.seq <= last.seq) {
            // A duplicate/stale frame is idempotent and must never rewind the per-aggregate cursor or trigger
            // a resync.
            if (options.onDurableGap === undefined) {
              yield* fail(new DurableOrderError("out-of-order", meta, last))
              return
            }
            return
          }
          if (meta.seq !== last.seq + 1) {
            // A gap is recoverable when the consumer owns a resync policy: surface it, await any healing, and
            // keep the stream alive.
            if (options.onDurableGap === undefined) {
              yield* fail(new DurableOrderError("gap", meta, last))
              return
            }
            resyncs++
            yield* settle(options.onDurableGap(meta, last))
          }
        }
        // Advance only once delivery succeeded: a frame that failed to deliver
        // leaves the cursor behind, so the next frame surfaces as a gap and heals it.
        yield* settle(options.onFrame(frame, text))
        lastByAggregate.set(meta.aggregateID, meta)
        received++
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

  const reject = (error: Error): void => {
    dropped++
    resyncs++
    options.onFrameRejected?.(error)
  }

  const process = (message: PortMessage): Effect.Effect<void, Error> =>
    Effect.gen(function* () {
      if (released || ended || lastError !== undefined) return
      if (message._tag === "failure") {
        if (options.onFrameRejected === undefined) {
          yield* fail(message.error)
          return
        }
        reject(message.error)
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

  // Every callback — `onFrame`, `onEnd`, `onResync`, `onAck`, `onDurableGap`, and the backpressure probe —
  // runs inside the ordered stream.
  const reportFatal = (cause: Cause.Cause<Error>): Effect.Effect<never, Error> =>
    Cause.hasInterruptsOnly(cause) ? Effect.failCause(cause) : fail(asError(Cause.squash(cause)))

  return {
    stats,
    // Process one message per scheduler turn: a port message is its own arrival turn, and downstream
    // consumers must be able to drain a delivered frame before a later terminal frame finalizes the stream.
    program: Stream.runForEach(stream, (message) =>
      Effect.gen(function* () {
        yield* process(message).pipe(
          Effect.catchCause((cause) =>
            Cause.hasInterruptsOnly(cause) || lastError !== undefined || options.onFrameRejected === undefined
              ? Effect.failCause(cause)
              : Effect.sync(() => reject(asError(Cause.squash(cause)))),
          ),
        )
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
 * Synchronous facade over the one decoded events pipeline, retained for callers that observe the receiver
 * directly (the no-publisher drain and the receiver tests).
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
