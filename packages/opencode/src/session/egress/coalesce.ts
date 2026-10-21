// Phase 8.3 + 8.4 — delta coalescing window (plan tasks 8.3, 8.4).
//
// Non-durable deltas are the high-frequency channel (streamed text, part
// updates). Buffering them for a short window and flushing as one frame keeps the
// hub fan-out cheap without delaying durable boundaries:
//
//   * `pushDelta`  — buffers a non-durable event; flushes on idle (window
//                    elapsed) or on threshold (batch full).
//   * `pushDurable`— flushes buffered deltas first, then emits the durable frame
//                    immediately. Durable events are never coalesced/reordered.
//   * `end`        — flushes buffered deltas, then emits the `end` frame.
//
// The clock and timer are injected so the window is deterministic in tests, and
// so production can drive it from the cross-worker fake clock.

import type { DurableMeta, EgressEvent, EgressFrame } from "./frame"
import { FRAME_VERSION } from "./frame"

/** Default coalescing window, matching the TUI's 16 ms batching (plan line 69). */
export const COALESCE_WINDOW_MS = 16
/** Default maximum deltas per frame before an eager threshold flush. */
export const COALESCE_MAX_BATCH = 256

/** Injectable time + timer so the window is not tied to wall-clock in tests. */
export interface CoalescerScheduler {
  now(): number
  schedule(callback: () => void, delayMs: number): unknown
  cancel(handle: unknown): void
}

const defaultScheduler: CoalescerScheduler = {
  now: () => Date.now(),
  schedule: (callback, delayMs) => setTimeout(callback, delayMs),
  cancel: (handle) => clearTimeout(handle as ReturnType<typeof setTimeout>),
}

export interface CoalescerOptions {
  /** Coalescing window in ms; defaults to 16. */
  readonly windowMs?: number
  /** Eager flush threshold; defaults to 256. */
  readonly maxBatch?: number
  readonly scheduler?: CoalescerScheduler
  readonly onFrame: (frame: EgressFrame) => void
}

export interface Coalescer {
  pushDelta(event: EgressEvent): void
  pushDurable(event: EgressEvent, meta: DurableMeta): void
  /** Emit any buffered deltas now (no-op when empty or ended). */
  flush(): void
  /** Flush buffered deltas, then emit the `end` frame (idempotent). */
  end(): void
  /** Number of deltas currently buffered. */
  readonly buffered: number
  readonly ended: boolean
}

const ENDED_ERROR = "coalescer has ended"

export function createCoalescer(options: CoalescerOptions): Coalescer {
  const windowMs = options.windowMs ?? COALESCE_WINDOW_MS
  const maxBatch = options.maxBatch ?? COALESCE_MAX_BATCH
  const scheduler = options.scheduler ?? defaultScheduler
  const onFrame = options.onFrame

  let buffer: EgressEvent[] = []
  let timer: unknown
  let ended = false

  const cancelTimer = (): void => {
    if (timer === undefined) return
    scheduler.cancel(timer)
    timer = undefined
  }

  const flush = (): void => {
    if (buffer.length === 0) return
    const events = buffer
    buffer = []
    cancelTimer()
    onFrame({ kind: "deltas", v: FRAME_VERSION, events })
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
      if (ended) throw new Error(ENDED_ERROR)
      buffer.push(event)
      if (buffer.length >= maxBatch) flush()
      else scheduleIdle()
    },
    pushDurable(event, meta) {
      if (ended) throw new Error(ENDED_ERROR)
      flush()
      onFrame({ kind: "durable", v: FRAME_VERSION, meta, event })
    },
    flush,
    end() {
      if (ended) return
      flush()
      ended = true
      cancelTimer()
      onFrame({ kind: "end", v: FRAME_VERSION })
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
