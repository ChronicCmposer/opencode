import type { EventV2Event } from "./frame"
import { Option, Schema } from "effect"

/** One committed position in an aggregate's durable log. */
export interface AggregatePosition {
  readonly aggregateID: string
  readonly seq: number
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

export type SnapshotReader = (
  aggregateID: string,
  seq: number,
  /** Exclusive lower bound; omit to read the aggregate from before its first event. */
  after?: number,
) => Promise<readonly EventV2Event[]>

/** Parse a trusted aggregate id at the delivery boundary; an empty id is a caller bug. */
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

/** The reported cursor before any position has been observed. */
export const UNSEEN_SEQUENCE = -1

export function createDeltaSubscription(options: DeltaSubscriptionOptions): DeltaSubscription {
  const aggregateID = requireAggregateID(options.aggregateID)
  // `undefined` is the explicit unseen sentinel; `0` is a real first event.
  let lastDeltaSeq: number | undefined
  let lastDurableSeq: number | undefined
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

  const heal = (at: number, after: number): void => {
    resyncs += 1
    options.onResync?.({ type: "resync", aggregateID, seq: at })
    healing += 1
    enqueue(async () => {
      try {
        const events = await options.snapshot(aggregateID, at, after)
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
      const previous = lastDeltaSeq
      if (previous !== undefined && seq <= previous) {
        stale += 1
        return
      }
      lastDeltaSeq = seq
      // The first observed delta SEEDS this subscription's local sequence.
      if (previous === undefined || seq === previous + 1) {
        options.onDelta(delta)
        return
      }
      // The gapped frame is superseded by the snapshot; the cursor was already advanced so later contiguous
      // deltas keep flowing.
      const cursor = lastDurableSeq ?? UNSEEN_SEQUENCE
      heal(cursor, cursor)
    },
    receiveDurable(durable) {
      assertAggregate(durable.aggregateID)
      const seq = requireSeq(durable.seq)
      const previous = lastDurableSeq
      if (previous !== undefined && seq <= previous) {
        stale += 1
        return
      }
      lastDurableSeq = seq
      // The first observed commit seeds the cursor: an existing aggregate may
      // first be observed at any sequence, and that is not a gap.
      if (previous === undefined || seq === previous + 1) {
        deliverDurable(durable)
        return
      }
      // A missing durable is a lost cursor, not a droppable delta.
      heal(seq - 1, previous)
      deliverDurable(durable)
    },
    get lastDeltaSeq() {
      return lastDeltaSeq ?? UNSEEN_SEQUENCE
    },
    get lastDurableSeq() {
      return lastDurableSeq ?? UNSEEN_SEQUENCE
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

// --------------------------------------------------------------------------- Durable snapshot from the event
// log ---------------------------------------------------------------------------

/** The read seam a snapshot needs. */
export type SnapshotQuery = (method: "all", sql: string, params: readonly unknown[]) => Promise<unknown>

const SNAPSHOT_SQL =
  "SELECT id, type, data FROM event WHERE aggregate_id = ? AND seq <= ? ORDER BY seq ASC"

const SNAPSHOT_RANGE_SQL =
  "SELECT id, type, data FROM event WHERE aggregate_id = ? AND seq > ? AND seq <= ? ORDER BY seq ASC"

const SnapshotRowSchema = Schema.Struct({
  id: Schema.String,
  type: Schema.String,
  data: Schema.Unknown,
})

const decodeSnapshotRow = Schema.decodeUnknownOption(SnapshotRowSchema)

/** Decode a stored payload; SQLite stores `data` as JSON text, tests may use objects. */
function decodeData(value: unknown): unknown {
  if (typeof value !== "string") return value
  try {
    return JSON.parse(value)
  } catch (error) {
    throw new Error(`invalid persisted event data: ${error instanceof Error ? error.message : String(error)}`)
  }
}

/** Build a snapshot reader over the durable `event` log. */
export function createEventLogSnapshotReader(query: SnapshotQuery): SnapshotReader {
  return async (aggregateID, seq, after) => {
    const aggregate = requireAggregateID(aggregateID)
    if (typeof seq !== "number" || !Number.isInteger(seq) || seq < -1) {
      throw new Error(`invalid snapshot seq: ${JSON.stringify(seq)} (expected an integer >= -1)`)
    }
    if (after !== undefined && (!Number.isInteger(after) || after < -1)) {
      throw new Error(`invalid snapshot after: ${JSON.stringify(after)} (expected an integer >= -1)`)
    }
    // -1 means "before the first event" and is the only empty snapshot. Cursor 0
    // is a real event position and must be read.
    if (seq < 0) return []
    // An empty range is already at the cursor: no query, no redundant read.
    if (after !== undefined && after >= seq) return []

    const rows =
      after === undefined
        ? await query("all", SNAPSHOT_SQL, [aggregate, seq])
        : await query("all", SNAPSHOT_RANGE_SQL, [aggregate, after, seq])
    if (!Array.isArray(rows)) throw new Error("snapshot query returned a non-array result")
    return rows.map((row, index) => {
      const decoded = decodeSnapshotRow(row)
      if (Option.isNone(decoded)) throw new Error(`invalid event row ${index}: expected { id, type, data }`)
      return { id: decoded.value.id, type: decoded.value.type, properties: decodeData(decoded.value.data) }
    })
  }
}

export * as EgressDelta from "./delta"
