// Delta coalescing window over the events envelope (spec Q7).

import { Schema } from "effect"

import { type EventsFrame, type EventV2Event, endFrame, eventFrame } from "@/session/worker/frame"

import type { DurableMeta } from "./frame"

/** Default coalescing window, matching the TUI's 16 ms batching. */
export const COALESCE_WINDOW_MS = 16
/** Default maximum deltas per frame before an eager threshold flush. */
export const COALESCE_MAX_BATCH = 256

/** Injectable timer so the window is not tied to wall-clock in tests. */
export interface CoalescerScheduler {
  schedule(callback: () => void, delayMs: number): unknown
  cancel(handle: unknown): void
}

/** Narrow an injectable timer handle back to the platform timer type. */
function isTimerHandle(handle: unknown): handle is ReturnType<typeof setTimeout> {
  return typeof handle === "number" || (typeof handle === "object" && handle !== null)
}

const defaultScheduler: CoalescerScheduler = {
  schedule: (callback, delayMs) => setTimeout(callback, delayMs),
  cancel: (handle) => {
    if (isTimerHandle(handle)) clearTimeout(handle)
  },
}

export interface CoalescerOptions {
  /** Coalescing window in ms; defaults to 16. */
  readonly windowMs?: number
  /** Eager flush threshold; defaults to 256. */
  readonly maxBatch?: number
  readonly scheduler?: CoalescerScheduler
  /** Called with each flushed batch of frames, in order. */
  readonly onBatch: (frames: readonly EventsFrame[]) => void
}

export interface Coalescer {
  pushDelta(event: EventV2Event): void
  pushDurable(event: EventV2Event, meta: DurableMeta): void
  /** Emit any buffered deltas now (no-op when empty or ended). */
  flush(): void
  /** Flush buffered deltas, then emit the `end` frame (idempotent). */
  end(): void
  /** Number of deltas currently buffered. */
  readonly buffered: number
  readonly ended: boolean
}

/** Thrown when a delta or durable is pushed after `end()`. */
export class CoalescerEndedError extends Schema.TaggedErrorClass<CoalescerEndedError>("CoalescerEndedError")(
  "Egress.CoalescerEnded",
  {},
) {
  constructor() {
    super({})
    this.name = "CoalescerEndedError"
  }
  override get message() {
    return "coalescer has ended"
  }
}

export function createCoalescer(options: CoalescerOptions): Coalescer {
  const windowMs = options.windowMs ?? COALESCE_WINDOW_MS
  const maxBatch = options.maxBatch ?? COALESCE_MAX_BATCH
  const scheduler = options.scheduler ?? defaultScheduler
  const onBatch = options.onBatch

  // Parse the tuning options at the boundary: a fractional / negative window or a
  // non-positive batch would make the flush schedule nonsensical, so fail loud.
  if (!Number.isInteger(windowMs) || windowMs < 0) {
    throw new Error(`invalid coalescing window: ${windowMs} (expected a non-negative integer)`)
  }
  if (!Number.isInteger(maxBatch) || maxBatch < 1) {
    throw new Error(`invalid coalescing max batch: ${maxBatch} (expected a positive integer)`)
  }

  let buffer: EventsFrame[] = []
  let timer: unknown
  let ended = false

  const cancelTimer = (): void => {
    if (timer === undefined) return
    scheduler.cancel(timer)
    timer = undefined
  }

  const flush = (): void => {
    cancelTimer()
    if (ended) return
    if (buffer.length === 0) return
    const frames = buffer
    buffer = []
    onBatch(frames)
  }

  const scheduleIdle = (): void => {
    if (timer !== undefined) return
    timer = scheduler.schedule(() => {
      timer = undefined
      flush()
    }, windowMs)
  }

  return {
    pushDelta(event) {
      if (ended) throw new CoalescerEndedError()
      buffer.push(eventFrame({ event }))
      if (buffer.length >= maxBatch) flush()
      else scheduleIdle()
    },
    pushDurable(event, meta) {
      if (ended) throw new CoalescerEndedError()
      flush()
      onBatch([
        eventFrame({ event, aggregateID: meta.aggregateID, seq: meta.seq } as Parameters<typeof eventFrame>[0]),
      ])
    },
    flush,
    end() {
      if (ended) return
      flush()
      ended = true
      onBatch([endFrame()])
    },
    get buffered() {
      return buffer.length
    },
    get ended() {
      return ended
    },
  }
}

export * as DeltaCoalescer from "./coalesce"
