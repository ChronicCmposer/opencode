// Phase 8.6 + 8.8 — pre-encode once, bounded per-client fan-out (plan tasks 8.6, 8.8).
//
// The hub is the only place frames are encoded: `publish` encodes a frame ONCE
// and queues the same byte buffer for every subscriber, so an SSE client receives
// bytes rather than a re-serialized object (plan decision line 129 / task 8.6).
//
// Backpressure is per subscriber and never blocks the publisher: each subscriber
// holds at most `bound` (default 256) frames. On overflow the OLDEST non-durable
// frame is dropped; if every queued frame is durable the subscriber is surfaced as
// overflowed via `SubscriberOverflowError` (plan decision line 70 / task 8.8).
// Durable frames are never dropped, so a durable boundary is always observable.

import { decodeFrame, encodeFrame, type EgressFrame } from "./frame"

/** Per-client egress bound (plan decision line 70). */
export const SUBSCRIBER_BOUND = 256

export class SubscriberOverflowError extends Error {
  constructor(readonly subscriberId: string) {
    super(`egress subscriber "${subscriberId}" overflowed: durable queue is full`)
    this.name = "SubscriberOverflowError"
  }
}

interface QueuedFrame {
  readonly bytes: Uint8Array
  readonly durable: boolean
}

export interface FanoutSubscriber {
  readonly id: string
  readonly depth: number
  /** Whether this subscriber has overflowed its durable-saturated bound. */
  readonly overflowed: boolean
  /** Snapshot of queued byte buffers, oldest first (same references as published). */
  queued(): readonly Uint8Array[]
  /** Remove and decode up to `limit` frames, oldest first. Default: all. */
  drain(limit?: number): readonly EgressFrame[]
  /** Remove and return up to `limit` raw frame buffers, oldest first. Default: all. */
  drainBytes(limit?: number): readonly Uint8Array[]
  /** Drop every queued non-durable frame, keeping durable boundaries (terminate). */
  dropNonDurable(): void
  /** Drop everything queued for this subscriber. */
  clear(): void
}

export interface PublishResult {
  readonly delivered: number
  /** Subscribers whose durable-saturated queue overflowed on this publish. */
  readonly overflowed: readonly string[]
}

export interface Fanout {
  subscribe(id: string): FanoutSubscriber
  unsubscribe(id: string): void
  publish(frame: EgressFrame): PublishResult
  /**
   * Fan out ALREADY-ENCODED frame bytes (plan task 8.6): the bytes received from
   * a worker are handed to every subscriber verbatim, with no hub re-encode.
   * `frame` is only consulted to classify durables for backpressure.
   */
  publishBytes(bytes: Uint8Array, frame: EgressFrame): PublishResult
  /**
   * Terminal broadcast (plan task 8.12): drop every subscriber's non-durable
   * frames, then deliver `frame` to all of them unconditionally (bypassing the
   * bound, so an `end` marker is never lost). Durable frames are kept.
   */
  finalize(frame: EgressFrame): void
  readonly subscribers: readonly FanoutSubscriber[]
  /** Count of `encodeFrame` calls — proves a single encode fans out to N clients. */
  readonly encodeCount: number
}

export interface FanoutOptions {
  readonly bound?: number
  readonly encode?: (frame: EgressFrame) => Uint8Array
}

interface SubscriberState {
  readonly id: string
  readonly queue: QueuedFrame[]
  overflowed: boolean
}

export function createFanout(options: FanoutOptions = {}): Fanout {
  const bound = options.bound ?? SUBSCRIBER_BOUND
  if (!Number.isInteger(bound) || bound < 1) {
    throw new Error(`invalid subscriber bound: ${bound} (expected a positive integer)`)
  }
  const encode = options.encode ?? encodeFrame

  const states = new Map<string, SubscriberState>()
  let encodeCount = 0

  const enqueue = (state: SubscriberState, bytes: Uint8Array, durable: boolean): void => {
    if (state.queue.length < bound) {
      state.queue.push({ bytes, durable })
      return
    }
    const oldestNonDurable = state.queue.findIndex((entry) => !entry.durable)
    if (oldestNonDurable === -1) throw new SubscriberOverflowError(state.id)
    state.queue.splice(oldestNonDurable, 1)
    state.queue.push({ bytes, durable })
  }

  const drainBytes = (state: SubscriberState, limit?: number): Uint8Array[] => {
    const count = limit === undefined ? state.queue.length : Math.min(limit, state.queue.length)
    return state.queue.splice(0, count).map((entry) => entry.bytes)
  }

  const handle = (state: SubscriberState): FanoutSubscriber => ({
    get id() {
      return state.id
    },
    get depth() {
      return state.queue.length
    },
    get overflowed() {
      return state.overflowed
    },
    queued: () => state.queue.map((entry) => entry.bytes),
    drainBytes: (limit) => drainBytes(state, limit),
    drain: (limit) => drainBytes(state, limit).map((bytes) => decodeFrame(bytes)),
    dropNonDurable: () => {
      const durable = state.queue.filter((entry) => entry.durable)
      state.queue.length = 0
      state.queue.push(...durable)
    },
    clear: () => {
      state.queue.length = 0
    },
  })

  const publishBytes = (bytes: Uint8Array, frame: EgressFrame): PublishResult => {
    const durable = frame.kind === "durable"
    const overflowed: string[] = []
    for (const state of states.values()) {
      try {
        enqueue(state, bytes, durable)
      } catch (error) {
        if (!(error instanceof SubscriberOverflowError)) throw error
        state.overflowed = true
        overflowed.push(state.id)
      }
    }
    return { delivered: states.size, overflowed }
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
      const bytes = encode(frame)
      encodeCount++
      return publishBytes(bytes, frame)
    },
    publishBytes,
    finalize(frame) {
      const bytes = encode(frame)
      encodeCount++
      const durable = frame.kind === "durable"
      for (const state of states.values()) {
        const kept = state.queue.filter((entry) => entry.durable)
        state.queue.length = 0
        state.queue.push(...kept, { bytes, durable })
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

export * as EgressFanout from "./fanout"
