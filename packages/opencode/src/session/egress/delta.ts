// Advisory delta subscriptions over durable aggregate identity (D17).
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

export type SnapshotReader = (aggregateID: string, seq: number) => Promise<readonly EventV2Event[]>

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
  /** Highest per-aggregate subscription sequence assigned (-1 before the first flush). */
  readonly lastSeq: number
  /** Last subscription sequence assigned for one aggregate (-1 if none). */
  lastSeqOf(aggregateID: string): number
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

/** Parse a non-negative integer sequence at the boundary; sequences are 0-based. */
function requireSeq(value: unknown, field = "seq"): number {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    throw new Error(`invalid ${field}: ${JSON.stringify(value)} (expected a non-negative integer)`)
  }
  return value
}

export function createDeltaQueue(options: DeltaQueueOptions): DeltaQueue {
  const bound = options.bound ?? DELTA_QUEUE_BOUND
  if (!Number.isInteger(bound) || bound < 1) {
    throw new Error(`invalid delta queue bound: ${bound} (expected a positive integer)`)
  }

  // Insertion-ordered index: a coalesce key maps straight to its pending entry,
  // so folding a later update and dropping the oldest are both O(1) (a linear
  // scan per push would be quadratic on the streaming hot path).
  const pending = new Map<string, PendingDelta>()
  // One cursor PER AGGREGATE: two interleaved aggregates must each see a
  // contiguous 1..n sequence rather than sharing a global ordinal (which would
  // make every aggregate look gapped).
  const seqByAggregate = new Map<string, number>()
  let unkeyed = 0
  let coalesced = 0
  let dropped = 0

  const pendingKey = (delta: PendingDelta): string =>
    delta.coalesceKey === undefined
      ? `\u0000unkeyed-${++unkeyed}`
      : `\u0000keyed-${delta.aggregateID}\u0000${delta.coalesceKey}`

  const dropOldest = (): void => {
    const oldest = pending.keys().next()
    if (oldest.done === true) return
    pending.delete(oldest.value)
    dropped += 1
  }

  const flush = (): void => {
    if (pending.size === 0) return
    const batch = [...pending.values()]
    pending.clear()
    for (const delta of batch) {
      const seq = (seqByAggregate.get(delta.aggregateID) ?? -1) + 1
      seqByAggregate.set(delta.aggregateID, seq)
      options.onDelta({ aggregateID: delta.aggregateID, seq, event: delta.event })
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
        const key = pendingKey(candidate)
        if (pending.has(key)) {
          // `Map.set` on an existing key keeps its insertion position, so the
          // newest value wins without reordering or a linear search.
          pending.set(key, candidate)
          coalesced += 1
          return
        }
        if (pending.size >= bound) {
          // Deltas are advisory: drop the oldest intermediate frame rather than
          // growing without bound or blocking the writer.
          dropOldest()
        }
        pending.set(key, candidate)
        return
      }

      if (pending.size >= bound) dropOldest()
      pending.set(pendingKey(candidate), candidate)
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
      return pending.size
    },
    get lastSeq() {
      let max = -1
      for (const seq of seqByAggregate.values()) if (seq > max) max = seq
      return max
    },
    lastSeqOf(aggregateID) {
      return seqByAggregate.get(aggregateID) ?? -1
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
  /** Snapshots currently healing; every increment is paired with a decrement. */
  readonly healing: number
}

export function createDeltaSubscription(options: DeltaSubscriptionOptions): DeltaSubscription {
  const aggregateID = requireAggregateID(options.aggregateID)
  // Sequences are 0-based, so "no position observed yet" is -1, never 0 (which
  // is a real first event).
  let lastDeltaSeq = -1
  let lastDurableSeq = -1
  let resyncs = 0
  let stale = 0
  // Serialize resync snapshots so a burst of gaps cannot interleave a stale read
  // ahead of a newer one, and hold durables behind the snapshot that healed them.
  let healing = 0
  let healed: Promise<void> = Promise.resolve()

  // Reporting is best effort: a throwing `onError` must never reject the heal
  // chain (that would permanently poison every later snapshot/delivery).
  const report = (error: unknown): void => {
    try {
      options.onError?.(error instanceof Error ? error : new Error(String(error)))
    } catch {
      // Swallowed by contract: the chain is never rejected.
    }
  }

  /** Append one link to the non-rejecting heal chain. */
  const enqueue = (task: () => void | Promise<void>): void => {
    healed = healed.then(task).catch(report)
  }

  const heal = (at: number): void => {
    resyncs += 1
    options.onResync?.({ type: "resync", aggregateID, seq: at })
    healing += 1
    enqueue(async () => {
      try {
        const events = await options.snapshot(aggregateID, at)
        options.onSnapshot?.({ aggregateID, seq: at, events })
      } catch (error) {
        report(error)
      } finally {
        // Paired with the increment above: a throwing snapshot or callback can
        // never leak the counter, so durables are not held back forever.
        healing -= 1
      }
    })
  }

  // A durable that arrives while a snapshot is in flight must not overtake the
  // snapshot or the durable that triggered it, so it joins the same chain.
  const deliverDurable = (durable: DurableDelivery): void => {
    const deliver = (): void => {
      try {
        options.onDurable(durable)
      } catch (error) {
        report(error)
      }
    }
    if (healing === 0) {
      deliver()
      return
    }
    enqueue(deliver)
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
      // A missing durable is a lost cursor, not a droppable delta. Heal at
      // `seq - 1` so the snapshot includes the skipped range `previous+1..seq-1`;
      // the durable at `seq` itself is delivered below exactly once.
      heal(seq - 1)
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
    get healing() {
      return healing
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
    if (typeof seq !== "number" || !Number.isInteger(seq) || seq < -1) {
      throw new Error(`invalid snapshot seq: ${JSON.stringify(seq)} (expected an integer >= -1)`)
    }
    // -1 means "before the first event" and is the only empty snapshot. Cursor 0
    // is a real event position and must be read.
    if (seq < 0) return []

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
