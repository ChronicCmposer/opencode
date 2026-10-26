// W2.4 — advisory delta subscriptions over durable aggregate identity (D17).
//
// Deltas are the high-frequency streaming channel. They are coalescible and
// droppable under a bounded per-session queue, and they NEVER advance a durable
// cursor. Durable events carry their real identity, `(aggregateID, seq)`, which is
// the only cursor a snapshot may resume from.
//
// The two halves are deliberately separate:
//
//   * `createDeltaQueue` is the producer side. It bounds pending deltas, folds a
//     later update for the same `coalesceKey` over the intermediate one, drops the
//     oldest pending delta on overflow, and assigns one monotonic sequence per
//     subscription AT FLUSH TIME. Durable events bypass the queue entirely: they
//     flush the pending deltas first (preserving order) and are never dropped.
//   * `createDeltaSubscription` is the consumer side. A gap in the subscription
//     sequence produces a typed `Resync { aggregateID, seq }` and a snapshot read
//     at the last durable sequence; the gapped delta itself is superseded by the
//     snapshot. Duplicate deliveries are idempotent.
//
// There is no sender/global ordinal here: durable ordering is the aggregate log's
// `(aggregate_id, seq)`, and the delta sequence is local to one subscription.

import type { EventV2Event } from "./frame"

/** Bounded pending-delta default for one session subscription. */
export const DELTA_QUEUE_BOUND = 256

/** One committed position in an aggregate's durable log. */
export interface AggregatePosition {
  readonly aggregateID: string
  readonly seq: number
}

/** A delta as it enters the bounded queue; `coalesceKey` folds repeated updates. */
export interface DeltaInput {
  readonly aggregateID: string
  readonly event: EventV2Event
  /** Stable identity (e.g. a part id); a later delta replaces the pending one. */
  readonly coalesceKey?: string
}

/** A sequenced delta as it leaves one subscription. */
export interface DeltaDelivery extends AggregatePosition {
  readonly event: EventV2Event
}

/** A durable event delivered with its aggregate identity. */
export interface DurableDelivery extends AggregatePosition {
  readonly event: EventV2Event
}

/** Typed request to refetch `aggregateID` at durable sequence `seq`. */
export interface DeltaResync extends AggregatePosition {
  readonly type: "resync"
}

/** Durable aggregate state at `seq`, used to heal a delta gap. */
export interface DeltaSnapshot extends AggregatePosition {
  readonly events: readonly EventV2Event[]
}

type SnapshotReader = (aggregateID: string, seq: number) => Promise<readonly EventV2Event[]>

export interface DeltaQueueOptions {
  /** Maximum pending deltas; defaults to {@link DELTA_QUEUE_BOUND}. */
  readonly bound?: number
  /** Called for each sequenced delta batch, in order. */
  readonly onDelta: (delta: DeltaDelivery) => void
  /** Called for each durable event, in order, after pending deltas are flushed. */
  readonly onDurable: (durable: DurableDelivery) => void
}

export interface DeltaQueue {
  pushDelta(input: DeltaInput): void
  pushDurable(input: DurableDelivery): void
  /** Emit every pending delta now, assigning their subscription sequences. */
  flush(): void
  /** Number of deltas waiting to be sequenced. */
  readonly pending: number
  /** Last assigned subscription sequence (0 before the first flush). */
  readonly lastSeq: number
  /** Intermediate deltas replaced by a later delta with the same key. */
  readonly coalesced: number
  /** Intermediate deltas dropped because the bound was reached. */
  readonly dropped: number
}

interface PendingDelta {
  readonly aggregateID: string
  readonly event: EventV2Event
  readonly coalesceKey?: string
}

/** Parse a trusted aggregate id at the queue boundary; an empty id is a caller bug. */
function requireAggregateID(value: unknown): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`invalid aggregateID: ${JSON.stringify(value)} (expected a non-empty string)`)
  }
  return value
}

/** Parse a positive integer sequence at the boundary; 0 is not a valid position. */
function requireSeq(value: unknown, field = "seq"): number {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 1) {
    throw new Error(`invalid ${field}: ${JSON.stringify(value)} (expected a positive integer)`)
  }
  return value
}

export function createDeltaQueue(options: DeltaQueueOptions): DeltaQueue {
  const bound = options.bound ?? DELTA_QUEUE_BOUND
  if (!Number.isInteger(bound) || bound < 1) {
    throw new Error(`invalid delta queue bound: ${bound} (expected a positive integer)`)
  }

  const pending: PendingDelta[] = []
  let lastSeq = 0
  let coalesced = 0
  let dropped = 0

  const flush = (): void => {
    if (pending.length === 0) return
    const batch = pending.splice(0, pending.length)
    for (const delta of batch) {
      lastSeq += 1
      options.onDelta({ aggregateID: delta.aggregateID, seq: lastSeq, event: delta.event })
    }
  }

  return {
    pushDelta(input) {
      requireAggregateID(input.aggregateID)
      const candidate: PendingDelta = {
        aggregateID: input.aggregateID,
        event: input.event,
        ...(input.coalesceKey === undefined ? {} : { coalesceKey: input.coalesceKey }),
      }

      if (candidate.coalesceKey !== undefined) {
        const index = pending.findIndex(
          (delta) => delta.aggregateID === candidate.aggregateID && delta.coalesceKey === candidate.coalesceKey,
        )
        if (index !== -1) {
          // Fold the intermediate update in place: the newest value wins and the
          // subscription sequence stays contiguous across the replacement.
          pending[index] = candidate
          coalesced += 1
          return
        }
      }

      if (pending.length >= bound) {
        // Deltas are advisory: drop the oldest intermediate frame rather than
        // growing without bound or blocking the writer.
        pending.shift()
        dropped += 1
      }
      pending.push(candidate)
    },
    pushDurable(input) {
      requireAggregateID(input.aggregateID)
      const seq = requireSeq(input.seq)
      // A durable boundary must never be reordered behind or dropped with deltas:
      // flush the pending deltas first, then emit the durable immediately.
      flush()
      options.onDurable({ aggregateID: input.aggregateID, seq, event: input.event })
    },
    flush,
    get pending() {
      return pending.length
    },
    get lastSeq() {
      return lastSeq
    },
    get coalesced() {
      return coalesced
    },
    get dropped() {
      return dropped
    },
  }
}

export interface DeltaSubscriptionOptions {
  readonly aggregateID: string
  /** Fetch durable events up to `seq`; production reads the `event` table. */
  readonly snapshot: SnapshotReader
  readonly onDelta: (delta: DeltaDelivery) => void
  readonly onDurable: (durable: DurableDelivery) => void
  readonly onResync?: (resync: DeltaResync) => void
  readonly onSnapshot?: (snapshot: DeltaSnapshot) => void
  readonly onError?: (error: Error) => void
}

export interface DeltaSubscription {
  receiveDelta(delta: DeltaDelivery): void
  receiveDurable(durable: DurableDelivery): void
  readonly lastDeltaSeq: number
  readonly lastDurableSeq: number
  /** Gaps healed by a resync. */
  readonly resyncs: number
  /** Duplicate/stale deliveries ignored. */
  readonly stale: number
}

export function createDeltaSubscription(options: DeltaSubscriptionOptions): DeltaSubscription {
  const aggregateID = requireAggregateID(options.aggregateID)
  let lastDeltaSeq = 0
  let lastDurableSeq = 0
  let resyncs = 0
  let stale = 0
  // Serialize resync snapshots so a burst of gaps cannot interleave a stale read
  // ahead of a newer one, and hold durables behind the snapshot that healed them.
  let healing = 0
  let healed: Promise<void> = Promise.resolve()

  const report = (error: unknown): void => {
    options.onError?.(error instanceof Error ? error : new Error(String(error)))
  }

  const heal = (at: number): void => {
    resyncs += 1
    options.onResync?.({ type: "resync", aggregateID, seq: at })
    healing += 1
    healed = healed.then(async () => {
      try {
        const events = await options.snapshot(aggregateID, at)
        options.onSnapshot?.({ aggregateID, seq: at, events })
      } catch (error) {
        report(error)
      } finally {
        healing -= 1
      }
    })
  }

  // A durable that arrives while a snapshot is in flight must not overtake the
  // snapshot or the durable that triggered it, so it joins the same chain.
  const deliverDurable = (durable: DurableDelivery): void => {
    if (healing === 0) {
      options.onDurable(durable)
      return
    }
    healed = healed.then(() => options.onDurable(durable))
  }

  const assertAggregate = (got: string): void => {
    if (got !== aggregateID) {
      throw new Error(`delta subscription for ${aggregateID} received aggregateID ${JSON.stringify(got)}`)
    }
  }

  return {
    receiveDelta(delta) {
      assertAggregate(delta.aggregateID)
      const seq = requireSeq(delta.seq)
      if (seq <= lastDeltaSeq) {
        stale += 1
        return
      }
      if (seq !== lastDeltaSeq + 1) {
        // The gapped frame is superseded by the snapshot; advance the observed
        // cursor so later contiguous deltas keep flowing.
        lastDeltaSeq = seq
        heal(lastDurableSeq)
        return
      }
      lastDeltaSeq = seq
      options.onDelta(delta)
    },
    receiveDurable(durable) {
      assertAggregate(durable.aggregateID)
      const seq = requireSeq(durable.seq)
      if (seq <= lastDurableSeq) {
        stale += 1
        return
      }
      const previous = lastDurableSeq
      lastDurableSeq = seq
      if (seq === previous + 1) {
        deliverDurable(durable)
        return
      }
      // A missing durable is a lost cursor, not a droppable delta: heal at the
      // last confirmed position and then deliver this durable.
      heal(previous)
      deliverDurable(durable)
    },
    get lastDeltaSeq() {
      return lastDeltaSeq
    },
    get lastDurableSeq() {
      return lastDurableSeq
    },
    get resyncs() {
      return resyncs
    },
    get stale() {
      return stale
    },
  }
}

// ---------------------------------------------------------------------------
// Durable snapshot from the event log
// ---------------------------------------------------------------------------

/**
 * The read seam a snapshot needs. `WorkerWriterClient.query` satisfies it, as
 * does a test double; the egress layer does not own the database connection.
 */
export type SnapshotQuery = (method: "all", sql: string, params: readonly unknown[]) => Promise<unknown>

const SNAPSHOT_SQL =
  "SELECT id, type, data FROM event WHERE aggregate_id = ? AND seq <= ? ORDER BY seq ASC"

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

/** Decode a stored payload; SQLite stores `data` as JSON text, tests may use objects. */
function decodeData(value: unknown): unknown {
  if (typeof value !== "string") return value
  try {
    return JSON.parse(value)
  } catch (error) {
    throw new Error(`invalid persisted event data: ${error instanceof Error ? error.message : String(error)}`)
  }
}

/**
 * Build a snapshot reader over the durable `event` log. It returns every event
 * at or below the requested `(aggregate_id, seq)` cursor, in commit order, as
 * EventV2 events (`properties` is the persisted data reference).
 */
export function createEventLogSnapshotReader(query: SnapshotQuery): SnapshotReader {
  return async (aggregateID, seq) => {
    const aggregate = requireAggregateID(aggregateID)
    if (typeof seq !== "number" || !Number.isInteger(seq) || seq < 0) {
      throw new Error(`invalid snapshot seq: ${JSON.stringify(seq)} (expected a non-negative integer)`)
    }
    // The empty log is a valid snapshot at cursor 0; do not hit the database.
    if (seq === 0) return []

    const rows = await query("all", SNAPSHOT_SQL, [aggregate, seq])
    if (!Array.isArray(rows)) throw new Error("snapshot query returned a non-array result")
    return rows.map((row, index) => {
      if (!isRecord(row)) throw new Error(`invalid event row ${index}: expected an object`)
      if (typeof row.id !== "string") throw new Error(`invalid event row ${index}: id must be a string`)
      if (typeof row.type !== "string") throw new Error(`invalid event row ${index}: type must be a string`)
      if (!("data" in row)) throw new Error(`invalid event row ${index}: data is missing`)
      return { id: row.id, type: row.type, properties: decodeData(row.data) }
    })
  }
}

export * as EgressDelta from "./delta"
