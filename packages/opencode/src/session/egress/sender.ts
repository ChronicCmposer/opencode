// Worker-side egress sender over the events envelope (spec Q7/Q16).
//
// Events-port ack handshake (spec Q16): after a batch that carried durable
// frames, the sender posts an `ack` frame whose `count` is the number of durable
// hand-offs in that batch. The hub receiver surfaces it via `onAck`, so durable
// progress is observable across the port rather than only implied by the events.

import { Effect } from "effect"
import type { EffectBridge } from "@/effect/bridge"
import { ackFrame, type EventsFrame, type EventV2Event } from "@/session/worker/frame"
import { workerLogMessage } from "@/session/worker/worker-logging"

import { createCoalescer, type Coalescer, type CoalescerScheduler } from "./coalesce"
import { durableMetaOf, encodeFrame, encodeFrameChunks, type DurableMeta } from "./frame"

export interface EgressSenderOptions {
  readonly port: MessagePort
  readonly windowMs?: number
  readonly maxBatch?: number
  readonly scheduler?: CoalescerScheduler
  /** Re-entry bridge; an oversized advisory frame is dropped with a warning. */
  readonly bridge?: EffectBridge.Shape
}

const emittedBySession = new Map<string, number>()

/** Durable events pushed for one aggregate since {@link resetDurableEmitted}. */
export function durableEmitted(aggregateID: string): number {
  return emittedBySession.get(aggregateID) ?? 0
}

/** Start a fresh durable count for one run. */
export function resetDurableEmitted(aggregateID: string): void {
  emittedBySession.delete(aggregateID)
}

export interface EgressSender {
  pushDelta(event: EventV2Event): void
  /** Push a durable boundary event; its aggregate identity is mandatory. */
  pushDurable(event: EventV2Event, meta: DurableMeta): void
  /**
   * Flush buffered deltas, post a zero-count RUN-END flush ack after the last durable frame, then emit the
   * end frame.
   */
  end(): void
  readonly buffered: number
  readonly ended: boolean
}

export function createEgressSender(options: EgressSenderOptions): EgressSender {
  const coalescer: Coalescer = createCoalescer({
    windowMs: options.windowMs,
    maxBatch: options.maxBatch,
    scheduler: options.scheduler,
    onBatch: (frames: readonly EventsFrame[]) => {
      // Encode and size the whole batch BEFORE posting anything, then split it into chunks that each fit the
      // 4 MiB transport cap.
      const chunks = encodeFrameChunks(frames, {
        onDrop: (frame, error) => {
          void options.bridge?.fork(
            Effect.logWarning(workerLogMessage("dropped oversized non-durable egress frame"), {
              kind: frame.kind,
              byteLength: error.byteLength,
              limit: error.limit,
            }),
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
    pushDurable: (event, meta) => {
      const checked = requireDurableMeta(meta)
      coalescer.pushDurable(event, checked)
      emittedBySession.set(checked.aggregateID, durableEmitted(checked.aggregateID) + 1)
    },
    end: () => {
      if (coalescer.ended) return
      // Flush pending deltas first, then post the RUN-END flush boundary: a zero-count ack after every
      // durable frame.
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

export * as EgressSender from "./sender"
