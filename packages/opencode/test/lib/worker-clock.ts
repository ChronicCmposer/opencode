// Cross-worker fake clock.
//
// Effect's `TestClock` is per-runtime and cannot cross an isolate boundary.
// This module therefore splits the clock in two:
//
//   * `attachClock(port)` runs inside the worker. It owns a frame-driven
//     timestamp plus the set of pending `Effect.sleep`s, and exposes a `Clock`
//     layer backed by that state. The hub drives it over a control
//     `MessagePort`; when the hub advances time, due sleeps resume in
//     timestamp order and `Effect.sleep` resolves exactly then.
//
//   * `createClockController({ port })` runs on the hub. It sends the control
//     frames (`clock.read` / `clock.advance` / `clock.set`) and resolves a
//     promise with the worker's reported state.
//
// The control frames are plain JSON so they survive structured clone; only the
// `MessagePort` itself is transferred.

import { Clock, Duration, Effect, Layer } from "effect"

/** Observable clock state, reported by the worker after every control frame. */
export interface ClockState {
  readonly timestamp: number
  readonly pending: number
}

/** A sleep request sent by the hub to the worker over the ordinary channel. */
export interface ClockSleepRequest {
  readonly type: "sleep"
  readonly id: string
  readonly ms: number
}

/** The single message the worker emits when a sleep's target time is reached. */
export interface ClockSleptEvent {
  readonly type: "slept"
  readonly id: string
  readonly at: number
}

/** Messages the hub sends to the worker (clock requests + sleep requests). */
export type ClockWorkerInbound = ClockSleepRequest

/** Messages the worker sends back to the hub. */
export type ClockWorkerOutbound = ClockSleptEvent

interface ClockReadFrame {
  readonly type: "clock.read"
  readonly id: number
}

interface ClockAdvanceFrame {
  readonly type: "clock.advance"
  readonly id: number
  readonly ms: number
}

interface ClockSetFrame {
  readonly type: "clock.set"
  readonly id: number
  readonly timestamp: number
}

type ClockControlFrame = ClockReadFrame | ClockAdvanceFrame | ClockSetFrame

interface ClockStateFrame extends ClockState {
  readonly type: "clock.state"
  readonly id: number
}

interface SleepEntry {
  readonly target: number
  readonly sequence: number
  readonly resume: (effect: Effect.Effect<void>) => void
}

export interface AttachedClock {
  /** The Effect `Clock` layer whose `sleep` resolves on hub advance. */
  readonly layer: Layer.Layer<Clock.Clock>
  /** Current frame-driven timestamp; used by the worker fixture to stamp events. */
  currentTimeMillis(): number
  /** Number of sleeps still waiting for time to advance. */
  pending(): number
  /** Detach from the control port. */
  dispose(): void
}

const NANOS_PER_MILLI = 1_000_000

/**
 * Build the worker-side clock. `port` is the worker end of the control channel;
 * the hub holds the other end and drives it with `clock.*` frames.
 */
export function attachClock(port: MessagePort): AttachedClock {
  let timestamp = 0
  let sequence = 0
  const sleeps: SleepEntry[] = []

  const state = (): ClockState => ({ timestamp, pending: sleeps.length })

  const remove = (entry: SleepEntry): void => {
    const index = sleeps.indexOf(entry)
    if (index !== -1) sleeps.splice(index, 1)
  }

  // Move to `destination`, resuming every sleep whose target has been reached,
  // earliest first. `resume` runs the fiber continuation synchronously, so a
  // sleep scheduled for t sees `currentTimeMillis() === t` even when the hub
  // jumps far past t. The final timestamp is always `destination`.
  const fireDue = (destination: number): void => {
    timestamp = destination
    while (true) {
      let next: SleepEntry | undefined
      for (const entry of sleeps) {
        if (entry.target > destination) continue
        if (!next || entry.target < next.target || (entry.target === next.target && entry.sequence < next.sequence)) {
          next = entry
        }
      }
      if (!next) break
      remove(next)
      timestamp = next.target
      next.resume(Effect.void)
    }
    timestamp = destination
  }

  const sleep = (duration: Duration.Duration): Effect.Effect<void> =>
    Effect.callback<void>((resume) => {
      const millis = Duration.toMillis(duration)
      if (millis <= 0) {
        resume(Effect.void)
        return
      }
      const entry: SleepEntry = { target: timestamp + millis, sequence: sequence++, resume }
      sleeps.push(entry)
      return Effect.sync(() => remove(entry))
    })

  const service: Clock.Clock = {
    currentTimeMillisUnsafe: () => timestamp,
    currentTimeMillis: Effect.sync(() => timestamp),
    currentTimeNanosUnsafe: () => BigInt(Math.floor(timestamp * NANOS_PER_MILLI)),
    currentTimeNanos: Effect.sync(() => BigInt(Math.floor(timestamp * NANOS_PER_MILLI))),
    sleep,
  }

  const reply = (id: number): void => port.postMessage({ type: "clock.state", id, ...state() } satisfies ClockStateFrame)

  port.onmessage = (event: MessageEvent) => {
    const frame = event.data as ClockControlFrame | undefined
    if (!frame || typeof frame.id !== "number") return
    switch (frame.type) {
      case "clock.read":
        reply(frame.id)
        return
      case "clock.advance":
        fireDue(timestamp + frame.ms)
        reply(frame.id)
        return
      case "clock.set":
        fireDue(frame.timestamp)
        reply(frame.id)
        return
    }
  }
  port.start()

  return {
    layer: Layer.effect(Clock.Clock, Effect.succeed(service)) as Layer.Layer<Clock.Clock>,
    currentTimeMillis: () => timestamp,
    pending: () => sleeps.length,
    dispose: () => port.close(),
  }
}

export interface ClockController {
  /** Read the worker clock's current state. */
  read(): Promise<ClockState>
  /** Advance the worker clock by `ms` milliseconds. */
  advance(ms: number): Promise<ClockState>
  /** Jump the worker clock to an absolute `timestamp`. */
  set(timestamp: number): Promise<ClockState>
  /** Close the control port and reject any in-flight request. */
  dispose(): void
}

export interface ClockControllerOptions {
  readonly port: MessagePort
  /** Per-request timeout in ms; a silent worker then fails loudly. Defaults to 5000. */
  readonly timeoutMs?: number
}

interface PendingClockRequest {
  readonly resolve: (state: ClockState) => void
  readonly reject: (error: Error) => void
  readonly timer: ReturnType<typeof setTimeout>
}

const DEFAULT_REQUEST_TIMEOUT_MS = 5000

/** Build the hub-side controller for an `attachClock` worker. */
export function createClockController(options: ClockControllerOptions): ClockController {
  const port = options.port
  const timeoutMs = options.timeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    throw new Error(`clock request timeoutMs must be a positive number, got: ${String(timeoutMs)}`)
  }

  const pending = new Map<number, PendingClockRequest>()
  let nextId = 1

  const settle = (id: number, state: ClockState): void => {
    const waiter = pending.get(id)
    if (!waiter) return
    pending.delete(id)
    clearTimeout(waiter.timer)
    waiter.resolve(state)
  }

  const failAll = (error: Error): void => {
    for (const waiter of pending.values()) {
      clearTimeout(waiter.timer)
      waiter.reject(error)
    }
    pending.clear()
  }

  port.onmessage = (event: MessageEvent) => {
    const frame = event.data as ClockStateFrame | undefined
    if (!frame || frame.type !== "clock.state") return
    settle(frame.id, { timestamp: frame.timestamp, pending: frame.pending })
  }
  port.onmessageerror = (event: MessageEvent) => {
    failAll(new Error(`clock control port message error: ${String(event.data)}`))
  }
  port.start()

  const request = (frame: ClockControlFrame): Promise<ClockState> =>
    new Promise<ClockState>((resolve, reject) => {
      const timer = setTimeout(() => {
        pending.delete(frame.id)
        reject(new Error(`clock ${frame.type} timed out after ${timeoutMs}ms`))
      }, timeoutMs)
      pending.set(frame.id, { resolve, reject, timer })
      port.postMessage(frame)
    })

  return {
    read: () => request({ type: "clock.read", id: nextId++ }),
    advance: (ms) => request({ type: "clock.advance", id: nextId++, ms }),
    set: (timestamp) => request({ type: "clock.set", id: nextId++, timestamp }),
    dispose: () => {
      port.close()
      failAll(new Error("clock controller disposed"))
    },
  }
}
