// Capture harness for golden-equivalence tests (plan task 3.3).
//
// A single ordered timeline records the three observable channels that a
// session pipeline emits: durable/non-durable `event`s, `db` writes, and
// `worker` messages. Later phases (3.12, 13.8, 14.2) snapshot this timeline
// and compare runs for full-row equivalence.
//
// Design: records are immutable and clock-injected so a capture is a pure,
// comparable value. Data is structured-cloned at the boundary, so a capture
// owns a private copy of whatever it records.

import { assertNonEmptyString } from "./assert"
import { deepFreeze } from "./deep-freeze"

export type CaptureChannel = "event" | "db" | "worker"

export interface CaptureRecord {
  /** Global capture order across all channels; starts at 1. */
  readonly seq: number
  /** Timestamp from the injected clock (deterministic in tests). */
  readonly at: number
  readonly channel: CaptureChannel
  /** Event type, SQL method, or worker message type. */
  readonly kind: string
  readonly data: unknown
  /** Only present for `worker` records. */
  readonly workerId?: number
}

export interface CaptureInput {
  readonly channel: CaptureChannel
  readonly kind: string
  readonly data?: unknown
  readonly workerId?: number
}

export interface CaptureSnapshot {
  readonly records: ReadonlyArray<CaptureRecord>
}

export interface CaptureOptions {
  /** Injectable clock for deterministic `at` values. Defaults to `Date.now`. */
  readonly clock?: () => number
}

export interface CaptureHarness {
  record(input: CaptureInput): CaptureRecord
  event(kind: string, data?: unknown): CaptureRecord
  dbWrite(kind: string, data?: unknown): CaptureRecord
  workerMessage(kind: string, data?: unknown, workerId?: number): CaptureRecord
  readonly records: ReadonlyArray<CaptureRecord>
  events(): ReadonlyArray<CaptureRecord>
  dbWrites(): ReadonlyArray<CaptureRecord>
  workerMessages(): ReadonlyArray<CaptureRecord>
  snapshot(): CaptureSnapshot
  reset(): void
}

const CHANNELS: ReadonlySet<CaptureChannel> = new Set(["event", "db", "worker"])

export function createCapture(options: CaptureOptions = {}): CaptureHarness {
  const clock = options.clock ?? (() => Date.now())
  let counter = 0
  let records: CaptureRecord[] = []

  const record = (input: CaptureInput): CaptureRecord => {
    if (!CHANNELS.has(input.channel)) {
      throw new Error(`unknown capture channel: ${String(input.channel)}`)
    }
    assertNonEmptyString(input.kind, "capture kind")
    const entry: CaptureRecord = Object.freeze({
      seq: ++counter,
      at: clock(),
      channel: input.channel,
      kind: input.kind,
      data: deepFreeze(structuredClone(input.data)),
      ...(input.workerId === undefined ? {} : { workerId: input.workerId }),
    })
    records.push(entry)
    return entry
  }

  const byChannel = (channel: CaptureChannel) => records.filter((entry) => entry.channel === channel)

  return {
    record,
    event: (kind, data) => record({ channel: "event", kind, data }),
    dbWrite: (kind, data) => record({ channel: "db", kind, data }),
    workerMessage: (kind, data, workerId) => record({ channel: "worker", kind, data, workerId }),
    get records() {
      return records.slice()
    },
    events: () => byChannel("event"),
    dbWrites: () => byChannel("db"),
    workerMessages: () => byChannel("worker"),
    snapshot: () => deepFreeze({ records: structuredClone(records) }),
    reset: () => {
      records = []
      counter = 0
    },
  }
}
