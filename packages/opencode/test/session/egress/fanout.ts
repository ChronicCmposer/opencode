// Test-only egress fan-out (relocated from `src/session/egress/fanout.ts`, plan
// finding B1). Production serves the worker events port through
// `session/worker/hub.ts`; this bounded fan-out was never wired to a production
// caller, so it lives under `test/` and backs the hub/SSE stream tests only.
//
// The hub is the only place frames are encoded: `publish` encodes a frame ONCE
// and queues the same byte buffer for every subscriber, so an SSE client receives
// JSON text rather than a re-serialized object.
//
// Backpressure is per subscriber and never blocks the publisher: each subscriber
// holds at most `bound` (default 256) frames. On overflow the OLDEST non-durable
// frame is dropped, so a non-durable delta is never dropped silently; only when
// every queued frame is durable and an incoming DURABLE cannot be enqueued does
// the subscriber get reported in `PublishResult.overflowed` and flagged until it
// drains. A durable boundary is therefore never dropped silently; an overflow
// surfaces `EgressStreamOverflowError` to the subscriber's stream.

import { decodeFrame, durableMetaOf, encodeFrame } from "@/session/egress/frame"
import type { EventsFrame } from "@/session/worker/frame"

/** Per-client egress bound. */
export const SUBSCRIBER_BOUND = 256

export class SubscriberOverflowError extends Error {
  constructor(readonly subscriberId: string) {
    super(`egress subscriber "${subscriberId}" overflowed: durable queue is full`)
    this.name = "SubscriberOverflowError"
  }
}

interface QueuedFrame {
  readonly text: string
  readonly durable: boolean
}

/**
 * A durable event is an `event` frame carrying BOTH `aggregateID` and `seq`
 * (frame metadata, not event fields); a non-durable delta carries neither.
 * Shares the receiver's {@link durableMetaOf} test so backpressure and commit
 * ordering agree on exactly which frames may be dropped.
 */
function isDurable(frame: EventsFrame): boolean {
  return durableMetaOf(frame) !== undefined
}

export interface FanoutSubscriber {
  readonly id: string
  readonly depth: number
  /** Whether this subscriber has overflowed its durable-saturated bound since last drained. */
  readonly overflowed: boolean
  /** Remove and decode up to `limit` frames, oldest first. Default: all. */
  drain(limit?: number): readonly EventsFrame[]
  /** Remove and return up to `limit` raw frame buffers, oldest first. Default: all. */
  drainBytes(limit?: number): readonly string[]
  /** Drop every queued non-durable frame, keeping durable boundaries (terminate). */
  dropNonDurable(): void
  /** Drop everything queued for this subscriber. */
  clear(): void
}

export interface PublishResult {
  readonly delivered: number
  /** Subscribers whose durable-saturated queue dropped an incoming durable (a lost commit). */
  readonly overflowed: readonly string[]
  /** Subscribers whose durable-saturated queue dropped an incoming non-durable delta (lossy, expected). */
  readonly droppedDeltas: readonly string[]
}

export interface Fanout {
  subscribe(id: string): FanoutSubscriber
  unsubscribe(id: string): void
  publish(frame: EventsFrame): PublishResult
  /**
   * Fan out ALREADY-ENCODED frame text: the JSON received from a worker is
   * handed to every subscriber verbatim, with no hub re-encode. `frame` is only
   * consulted to classify durables for backpressure.
   */
  publishBytes(text: string, frame: EventsFrame): PublishResult
  /**
   * Terminal broadcast: drop every subscriber's non-durable frames, then deliver
   * `frame` to all of them unconditionally (bypassing the bound, so an `end`
   * marker is never lost). Durable frames are kept.
   */
  finalize(frame: EventsFrame): void
  readonly subscribers: readonly FanoutSubscriber[]
  /** Count of `encodeFrame` calls — proves a single encode fans out to N clients. */
  readonly encodeCount: number
}

export interface FanoutOptions {
  readonly bound?: number
  readonly encode?: (frame: EventsFrame) => string
}

interface SubscriberState {
  readonly id: string
  readonly queue: QueuedFrame[]
  overflowed: boolean
  handle?: FanoutSubscriber
}

type EnqueueOutcome =
  | { readonly outcome: "enqueued" }
  | { readonly outcome: "dropped-delta" }
  | { readonly outcome: "dropped-durable" }

export function createFanout(options: FanoutOptions = {}): Fanout {
  const bound = options.bound ?? SUBSCRIBER_BOUND
  if (!Number.isInteger(bound) || bound < 1) {
    throw new Error(`invalid subscriber bound: ${bound} (expected a positive integer)`)
  }
  const encode = options.encode ?? encodeFrame

  const states = new Map<string, SubscriberState>()
  let encodeCount = 0

  const enqueue = (state: SubscriberState, text: string, durable: boolean): EnqueueOutcome => {
    if (state.queue.length < bound) {
      state.queue.push({ text, durable })
      return { outcome: "enqueued" }
    }
    const oldestNonDurable = state.queue.findIndex((entry) => !entry.durable)
    if (oldestNonDurable === -1) return { outcome: durable ? "dropped-durable" : "dropped-delta" }
    state.queue.splice(oldestNonDurable, 1)
    state.queue.push({ text, durable })
    return { outcome: "enqueued" }
  }

  const drainBytes = (state: SubscriberState, limit?: number): string[] => {
    const count = limit === undefined ? state.queue.length : Math.min(limit, state.queue.length)
    const drained = state.queue.splice(0, count).map((entry) => entry.text)
    if (state.queue.length < bound) state.overflowed = false
    return drained
  }

  const handle = (state: SubscriberState): FanoutSubscriber => {
    if (state.handle) return state.handle
    const created: FanoutSubscriber = {
      get id() {
        return state.id
      },
      get depth() {
        return state.queue.length
      },
      get overflowed() {
        return state.overflowed
      },
      drainBytes: (limit) => drainBytes(state, limit),
      drain: (limit) => drainBytes(state, limit).map((text) => decodeFrame(text)),
      dropNonDurable: () => {
        const durable = state.queue.filter((entry) => entry.durable)
        state.queue.length = 0
        state.queue.push(...durable)
      },
      clear: () => {
        state.queue.length = 0
        state.overflowed = false
      },
    }
    state.handle = created
    return created
  }

  const publishBytes = (text: string, frame: EventsFrame): PublishResult => {
    const durable = isDurable(frame)
    const overflowed: string[] = []
    const droppedDeltas: string[] = []
    let delivered = 0
    for (const state of states.values()) {
      const outcome = enqueue(state, text, durable)
      if (outcome.outcome === "enqueued") {
        delivered++
        continue
      }
      if (outcome.outcome === "dropped-delta") {
        droppedDeltas.push(state.id)
        continue
      }
      // A durable could not be enqueued: a committed boundary was lost.
      state.overflowed = true
      overflowed.push(state.id)
    }
    return { delivered, overflowed, droppedDeltas }
  }

  return {
    subscribe(id) {
      const existing = states.get(id)
      if (existing) return handle(existing)
      const state: SubscriberState = { id, queue: [], overflowed: false }
      states.set(id, state)
      return handle(state)
    },
    unsubscribe(id) {
      states.delete(id)
    },
    publish(frame) {
      const text = encode(frame)
      encodeCount++
      return publishBytes(text, frame)
    },
    publishBytes,
    finalize(frame) {
      const text = encode(frame)
      encodeCount++
      const durable = isDurable(frame)
      for (const state of states.values()) {
        const kept = state.queue.filter((entry) => entry.durable)
        state.queue.length = 0
        state.queue.push(...kept, { text, durable })
      }
    },
    get subscribers() {
      return [...states.values()].map(handle)
    },
    get encodeCount() {
      return encodeCount
    },
  }
}
