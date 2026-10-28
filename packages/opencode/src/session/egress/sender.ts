// Worker-side egress sender over the events envelope (spec Q7/Q16).
//
// The worker owns a dedicated `MessagePort` to the hub. Every batch the
// coalescer emits is encoded once as shared-envelope JSON strings and posted one
// message per frame; the 4 MiB transport bound applies per frame and per batch.
// Every durable frame carries its aggregate `(aggregateID, seq)`; there is no
// sender-global ordinal fallback, so a durable without identity is a caller bug.
//
// Events-port ack handshake (spec Q16): after a batch that carried durable
// frames, the sender posts an `ack` frame whose `count` is the number of durable
// hand-offs in that batch. The hub receiver surfaces it via `onAck`, so durable
// progress is observable across the port rather than only implied by the events.

import { ackFrame, type EventsFrame, type EventV2Event } from "@/session/worker/frame"
import { noopWorkerLogger, workerLogMessage, type WorkerLogger } from "@/session/worker/log"

import { createCoalescer, type Coalescer, type CoalescerScheduler } from "./coalesce"
import { durableMetaOf, encodeFrame, encodeFrameChunks, type DurableMeta } from "./frame"

export interface EgressSenderOptions {
  readonly port: MessagePort
  readonly windowMs?: number
  readonly maxBatch?: number
  readonly scheduler?: CoalescerScheduler
  /** Structured logger; an oversized advisory frame is dropped with a warning. */
  readonly log?: WorkerLogger
}

export interface EgressSender {
  pushDelta(event: EventV2Event): void
  /** Push a durable boundary event; its aggregate identity is mandatory. */
  pushDurable(event: EventV2Event, meta: DurableMeta): void
  /**
   * Flush buffered deltas, post a zero-count RUN-END flush ack after the last
   * durable frame, then emit the end frame.
   */
  end(): void
  readonly buffered: number
  readonly ended: boolean
}

export function createEgressSender(options: EgressSenderOptions): EgressSender {
  const log = options.log ?? noopWorkerLogger
  const coalescer: Coalescer = createCoalescer({
    windowMs: options.windowMs,
    maxBatch: options.maxBatch,
    scheduler: options.scheduler,
    onBatch: (frames: readonly EventsFrame[]) => {
      // Encode and size the whole batch BEFORE posting anything, then split it
      // into chunks that each fit the 4 MiB transport cap. A batch that merely
      // totals too much is split, never dropped; a DURABLE frame whose body alone
      // exceeds the cap crosses as its blob-position reference (core already
      // offloaded the body), and an advisory non-durable one is dropped with a
      // warning so it cannot take the rest of the batch and its ack with it.
      const chunks = encodeFrameChunks(frames, {
        onDrop: (frame, error) => {
          log(
            workerLogMessage("dropped oversized non-durable egress frame"),
            {
              kind: frame.kind,
              byteLength: error.byteLength,
              limit: error.limit,
            },
            "warn",
          )
        },
      })
      for (const chunk of chunks) {
        for (const text of chunk) options.port.postMessage(text)
      }
      const durables = frames.filter((frame) => durableMetaOf(frame) !== undefined).length
      if (durables === 0) return
      options.port.postMessage(encodeFrame(ackFrame({ count: durables })))
    },
  })

  return {
    pushDelta: (event) => coalescer.pushDelta(event),
    pushDurable: (event, meta) => coalescer.pushDurable(event, requireDurableMeta(meta)),
    end: () => {
      if (coalescer.ended) return
      // Flush pending deltas first, then post the RUN-END flush boundary: a
      // zero-count ack after every durable frame. The hub's run barrier can wait
      // for it to know the events port drained before the run reply (which
      // crosses a different channel) is considered final.
      coalescer.flush()
      options.port.postMessage(encodeFrame(ackFrame({ count: 0 })))
      coalescer.end()
    },
    get buffered() {
      return coalescer.buffered
    },
    get ended() {
      return coalescer.ended
    },
  }
}

/** Reject a durable with no/invalid aggregate identity at the public boundary. */
function requireDurableMeta(meta: DurableMeta | undefined): DurableMeta {
  if (meta === undefined || typeof meta.aggregateID !== "string" || meta.aggregateID.length === 0) {
    throw new Error(`durable egress requires a non-empty aggregateID (got ${JSON.stringify(meta?.aggregateID)})`)
  }
  if (!Number.isInteger(meta.seq) || meta.seq < 0) {
    throw new Error(`durable egress requires a non-negative integer seq (got ${JSON.stringify(meta.seq)})`)
  }
  return meta
}

export * as EgressSenderModule from "./sender"
