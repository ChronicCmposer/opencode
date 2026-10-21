// Phase 8.1 — worker-side egress sender (plan task 8.1).
//
// The worker owns a NEW dedicated `MessagePort` to the hub. Every frame the
// coalescer emits is encoded once (v1 UTF-8 JSON, length-prefixed) and posted as
// bytes; the buffer is transferred so the send is zero-copy. Durable frames are
// stamped with the writer ordinal here via the `meta` argument (or an internal
// monotonic fallback until Phase 7 lands).

import { createCoalescer, type Coalescer, type CoalescerScheduler } from "./coalesce"
import { encodeFrame, type DurableMeta, type EgressEvent } from "./frame"

export interface EgressSenderOptions {
  readonly port: MessagePort
  /** Writer generation used by the fallback ordinal; defaults to 1. */
  readonly generation?: number
  readonly windowMs?: number
  readonly maxBatch?: number
  readonly scheduler?: CoalescerScheduler
}

export interface EgressSender {
  pushDelta(event: EgressEvent): void
  /** Push a durable boundary event; `meta` defaults to the fallback ordinal. */
  pushDurable(event: EgressEvent, meta?: DurableMeta): void
  /** Flush buffered deltas and emit the end frame. */
  end(): void
  readonly buffered: number
  readonly ended: boolean
}

export function createEgressSender(options: EgressSenderOptions): EgressSender {
  const generation = options.generation ?? 1
  let ordinal = 0

  const coalescer: Coalescer = createCoalescer({
    windowMs: options.windowMs,
    maxBatch: options.maxBatch,
    scheduler: options.scheduler,
    onFrame: (frame) => {
      const bytes = encodeFrame(frame)
      options.port.postMessage(bytes, [bytes.buffer])
    },
  })

  return {
    pushDelta: (event) => coalescer.pushDelta(event),
    pushDurable: (event, meta) => coalescer.pushDurable(event, meta ?? { generation, ordinal: ++ordinal }),
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
