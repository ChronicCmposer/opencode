// W2 — worker-side egress sender over the events envelope (spec Q7/Q16).
//
// The worker owns a dedicated `MessagePort` to the hub. Every batch the
// coalescer emits is encoded once (v1 UTF-8 JSON, length-prefixed) and posted as
// a single byte buffer; the buffer is transferred so the send is zero-copy.
// Durable frames are stamped with the writer ordinal here via the `meta`
// argument (or an internal monotonic fallback until Phase 7 lands).
//
// Events-port ack handshake (spec Q16): after a batch that carried durable
// frames, the sender posts an `ack` frame whose `count` is the number of durable
// hand-offs in that batch. The hub receiver surfaces it via `onAck`, so durable
// progress is observable across the port rather than only implied by the events.

import { ackFrame, type EventsFrame, type EventV2Event } from "@/session/worker/frame"

import { createCoalescer, type Coalescer, type CoalescerScheduler } from "./coalesce"
import { durableMetaOf, encodeFrame, encodeFrames, type DurableMeta } from "./frame"
import { createMonotonicOrdinalSource } from "./ordinal"

export interface EgressSenderOptions {
  readonly port: MessagePort
  /** Writer generation used by the fallback ordinal; defaults to 1. */
  readonly generation?: number
  readonly windowMs?: number
  readonly maxBatch?: number
  readonly scheduler?: CoalescerScheduler
}

export interface EgressSender {
  pushDelta(event: EventV2Event): void
  /** Push a durable boundary event; `meta` defaults to the fallback ordinal. */
  pushDurable(event: EventV2Event, meta?: DurableMeta): void
  /** Flush buffered deltas and emit the end frame. */
  end(): void
  readonly buffered: number
  readonly ended: boolean
}

export function createEgressSender(options: EgressSenderOptions): EgressSender {
  const ordinals = createMonotonicOrdinalSource({ generation: options.generation ?? 1 })

  const coalescer: Coalescer = createCoalescer({
    windowMs: options.windowMs,
    maxBatch: options.maxBatch,
    scheduler: options.scheduler,
    onBatch: (frames: readonly EventsFrame[]) => {
      const bytes = encodeFrames(frames)
      options.port.postMessage(bytes, [bytes.buffer])
      const durables = frames.filter((frame) => durableMetaOf(frame) !== undefined).length
      if (durables === 0) return
      const ack = encodeFrame(ackFrame({ count: durables }))
      options.port.postMessage(ack, [ack.buffer])
    },
  })

  return {
    pushDelta: (event) => coalescer.pushDelta(event),
    pushDurable: (event, meta) => coalescer.pushDurable(event, meta ?? ordinals.next()),
    end: () => coalescer.end(),
    get buffered() {
      return coalescer.buffered
    },
    get ended() {
      return coalescer.ended
    },
  }
}

export * as EgressSenderModule from "./sender"
