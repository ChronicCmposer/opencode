// Shared test recorders extracted from the per-file spies copy-pasted across the
// worker/hub tests:
//
//   - `recordingLogger` — a `WorkerLogger` that captures every structured line.
//   - `lineFor` — a fail-loud lookup for a specific logged event.
//   - `recordingPublisher` — a `LiveEventPublisher` that captures every event.
//
// They exist so observability assertions read the same way everywhere and a
// missing line fails with a descriptive message instead of `undefined`.

import type { LiveEvent, LiveEventPublisher } from "@/event-v2-bridge"
import { workerLogMessage, type WorkerLogLevel, type WorkerLogger } from "@/session/worker/log"

/** A structured log line captured by {@link recordingLogger}. */
export interface RecordedLog {
  readonly message: string
  readonly fields: Record<string, unknown>
  readonly level?: WorkerLogLevel
}

export interface RecordingLogger {
  readonly log: WorkerLogger
  /** Every line emitted so far, oldest first. This is the live record. */
  readonly lines: RecordedLog[]
}

/** A `WorkerLogger` that appends every emitted line to a readable array. */
export function recordingLogger(): RecordingLogger {
  const lines: RecordedLog[] = []
  const log: WorkerLogger = (message, fields, level) => {
    lines.push(level === undefined ? { message, fields } : { message, fields, level })
  }
  return { log, lines }
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
