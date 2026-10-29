// Shared test recorders extracted from the per-file spies copy-pasted across the
// worker/hub tests:
//
//   - `recordingLogger` — an `EffectBridge` sink that captures every structured
//     `Effect.log*` event the production code forks through it.
//   - `lineFor` — a fail-loud lookup for a specific logged event.
//   - `recordingPublisher` — a `LiveEventPublisher` that captures every event.
//
// They exist so observability assertions read the same way everywhere and a
// missing line fails with a descriptive message instead of `undefined`.

import { Effect, Fiber, Logger } from "effect"
import type { EffectBridge } from "@/effect/bridge"
import type { LiveEvent, LiveEventPublisher } from "@/event-v2-bridge"
import { workerLogMessage } from "@/session/worker/worker-logging"

export type RecordedLogLevel = "info" | "warn" | "error"

/** A structured log line captured by {@link recordingLogger}. */
export interface RecordedLog {
  readonly message: string
  readonly fields: Record<string, unknown>
  readonly level: RecordedLogLevel
}

export interface RecordingLogger {
  /** The re-entry bridge production code logs through. */
  readonly bridge: EffectBridge.Shape
  /** Every line emitted so far, oldest first. This is the live record. */
  readonly lines: RecordedLog[]
}

const severityOf = (level: string): RecordedLogLevel => {
  const normalized = level.toLowerCase()
  if (normalized === "warn" || normalized === "error") return normalized
  return "info"
}

/**
 * An `EffectBridge` whose `fork` runs each log effect synchronously under a
 * capturing sink. The captured event is exactly what the emission site logged;
 * the worker prefix is rendered by the forwarding logger, which has its own
 * unit tests.
 */
export function recordingLogger(): RecordingLogger {
  const lines: RecordedLog[] = []
  return { bridge: recordingBridgeFrom((line) => lines.push(line)), lines }
}

/** Build a recording bridge from a plain callback, for one-off assertions. */
export function recordingBridgeFrom(record: (line: RecordedLog) => void): EffectBridge.Shape {
  const sink = Logger.make<unknown, void>(({ logLevel, message }) => {
    const [head, fields] = Array.isArray(message) ? message : [message, undefined]
    record({
      message: typeof head === "string" ? head : String(head),
      fields: typeof fields === "object" && fields !== null ? (fields as Record<string, unknown>) : {},
      level: severityOf(logLevel),
    })
  })
  return recordingBridge(sink)
}

/** Turn a capturing sink into a bridge whose `fork` logs synchronously. */
export function recordingBridge(sink: Logger.Logger<unknown, void>): EffectBridge.Shape {
  return {
    promise: () => Promise.reject(new Error("recording bridge: promise is unused")),
    fork: <A, E, R>(effect: Effect.Effect<A, E, R>): Fiber.Fiber<A, E> => {
      Effect.runSync((effect as Effect.Effect<A, E, never>).pipe(Effect.provide(Logger.layer([sink]))))
      return undefined as unknown as Fiber.Fiber<A, E>
    },
    run: <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E> =>
      effect as unknown as Effect.Effect<A, E>,
    bind: <Args extends readonly unknown[], Result>(fn: (...args: Args) => Result) => fn,
  }
}

/**
 * Return the first recorded line for `event`, or fail loudly listing the events
 * that were actually seen (Law of "Fail Fast, Fail Loud").
 */
export function lineFor(lines: readonly RecordedLog[], event: string): RecordedLog {
  const expected = workerLogMessage(event)
  const line = lines.find((entry) => entry.message === expected)
  if (line === undefined) {
    const seen = lines.map((entry) => entry.message)
    throw new Error(`no log line for "${event}"; saw ${JSON.stringify(seen)}`)
  }
  return line
}

export interface RecordingPublisher {
  readonly publisher: LiveEventPublisher
  /** Every event published so far, oldest first. This is the live record. */
  readonly published: LiveEvent[]
}

/** A `LiveEventPublisher` spy that records every republished event. */
export function recordingPublisher(): RecordingPublisher {
  const published: LiveEvent[] = []
  const publisher: LiveEventPublisher = {
    publish: (event) => {
      published.push(event)
    },
    publishDefinition: (definition, data) => {
      published.push({ type: definition.type, data })
    },
  }
  return { publisher, published }
}
