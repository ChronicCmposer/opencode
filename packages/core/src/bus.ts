export * as Bus from "./bus"

import { Schema } from "effect"
import { Event } from "@opencode-ai/schema/event"
import { decodeDurablePayload, type SerializedEvent } from "./event"
import { BusyExhaustedError, isBusyError } from "./database/transaction"

/**
 * The synchronous connection a Bus writes through. It matches the worker
 * connection surface (`all`/`run`/`exec`) so a per-worker WAL connection can be
 * passed directly; the Bus owns `BEGIN IMMEDIATE` for its own operations.
 */
export interface Connection {
  all(sql: string, params?: ReadonlyArray<unknown>): ReadonlyArray<Record<string, unknown>>
  run(sql: string, params?: ReadonlyArray<unknown>): { readonly changes: number }
  exec(sql: string): void
}

export interface CacheInvalidation {
  invalidate(aggregateID: string): void
}

export interface Options {
  readonly connection: Connection
  /** Worker-generation lease stamped onto every aggregate this Bus writes. */
  readonly ownerID: string
  /** Definitions used to decode stored events; defaults to the global durable manifest. */
  readonly definitions?: ReadonlyArray<Event.Definition>
  readonly cache?: CacheInvalidation
}

export interface PublishEvent<D extends Event.Definition = Event.Definition> {
  readonly definition: D
  readonly data: Event.Data<D>
  readonly id?: Event.ID
}

/** A non-empty batch. Every event must be durable and share one aggregate. */
export type PublishEvents = readonly [PublishEvent, ...PublishEvent[]]

export type Projector<D extends Event.Definition = Event.Definition> = (event: Event.Payload<D>) => void

export interface Lease {
  readonly aggregateID: string
  readonly ownerID: string
  readonly seq: number
}

export interface ReclaimInput {
  readonly aggregateID: string
  readonly expectedOwnerID: string
  /** Last sequence the reclaimer observed; a newer value means a commit raced the reclaim. */
  readonly expectedSeq: number
  readonly nextOwnerID: string
}

export interface Transaction {
  readonly connection: Connection
  readonly all: Connection["all"]
  readonly run: Connection["run"]
  readonly publish: (events: PublishEvents) => readonly Event.Payload[]
}

export interface Interface {
  readonly connection: Connection
  readonly ownerID: string
  readonly publish: (events: PublishEvents) => readonly Event.Payload[]
  /** Publish inside a caller-owned `BEGIN IMMEDIATE`; does not commit or invalidate the cache. */
  readonly publishInTransaction: (events: PublishEvents) => readonly Event.Payload[]
  /** Run `work` inside `BEGIN IMMEDIATE` and invalidate touched cache keys after commit. */
  readonly transaction: <T>(work: (tx: Transaction) => T) => T
  readonly project: <D extends Event.Definition>(definition: D, projector: Projector<D>) => void
  readonly latest: (aggregateID: string) => number
  readonly ownerOf: (aggregateID: string) => string | undefined
  readonly read: (input: { readonly aggregateID: string; readonly after?: number }) => readonly Event.Payload[]
  readonly claim: (aggregateID: string, nextOwnerID?: string) => Lease
  readonly reclaim: (input: ReclaimInput) => Lease
}

export class InvalidDurableEventError extends Schema.TaggedErrorClass<InvalidDurableEventError>()(
  "Bus.InvalidDurableEvent",
  {
    type: Schema.String,
    message: Schema.String,
  },
) {}

export class StaleOwnerError extends Schema.TaggedErrorClass<StaleOwnerError>()("Bus.StaleOwner", {
  message: Schema.String,
}) {}

export class LeaseConflictError extends Schema.TaggedErrorClass<LeaseConflictError>()("Bus.LeaseConflict", {
  message: Schema.String,
}) {}

export class UnknownAggregateError extends Schema.TaggedErrorClass<UnknownAggregateError>()("Bus.UnknownAggregate", {
  message: Schema.String,
}) {}

export class StaleStateVersionError extends Schema.TaggedErrorClass<StaleStateVersionError>()("Bus.StaleStateVersion", {
  expected: Schema.Int,
  actual: Schema.Int,
}) {}

/**
 * Reserve `block` contiguous sequences for one aggregate with a single
 * upsert-returning statement. The inserted `seq` is `block - 1`, so a fresh
 * aggregate ends at `block - 1` (first `0`) while an existing aggregate at `L`
 * ends at `L + block` (first `L + 1`). The reservation is rolled back with the
 * surrounding transaction, so a failed commit never leaves a gap.
 */
const RESERVE_BLOCK = `
  INSERT INTO event_sequence (aggregate_id, seq, owner_id)
  VALUES (?, ?, ?)
  ON CONFLICT(aggregate_id) DO UPDATE SET
    seq = event_sequence.seq + excluded.seq + 1,
    owner_id = COALESCE(event_sequence.owner_id, excluded.owner_id)
  RETURNING seq
`

interface SequenceRow {
  readonly seq: number
  readonly ownerID: string | undefined
}

interface PreparedEvent {
  readonly id: Event.ID
  readonly type: string
  readonly version: number
  readonly aggregateID: string
  readonly data: Event.Data<Event.Definition>
  readonly encoded: Record<string, unknown>
}

const parseSequence = (row: Record<string, unknown>): SequenceRow => {
  const seq = Number(row.seq)
  if (!Number.isInteger(seq)) {
    throw new Error(`event_sequence contained a non-integer sequence: ${String(row.seq)}`)
  }
  return {
    seq,
    ownerID: typeof row.owner_id === "string" ? row.owner_id : undefined,
  }
}

const readSequence = (connection: Connection, aggregateID: string): SequenceRow | undefined => {
  const row = connection.all("SELECT seq, owner_id FROM event_sequence WHERE aggregate_id = ?", [aggregateID])[0]
  return row === undefined ? undefined : parseSequence(row)
}

const requireSequence = (row: Record<string, unknown> | undefined): number => {
  if (row === undefined) throw new Error("sequence reservation returned no row")
  const seq = Number(row.seq)
  if (!Number.isInteger(seq)) throw new Error("sequence reservation returned a non-integer")
  return seq
}

const parseStoredData = (value: unknown): Record<string, unknown> => {
  const parsed = typeof value === "string" ? JSON.parse(value) : value
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error("stored event data is not a JSON object")
  }
  return parsed as Record<string, unknown>
}

const parseStoredEvent = (row: Record<string, unknown>): SerializedEvent => {
  const seq = Number(row.seq)
  if (
    typeof row.id !== "string" ||
    typeof row.type !== "string" ||
    typeof row.aggregate_id !== "string" ||
    !Number.isInteger(seq)
  ) {
    throw new Error("stored event row is malformed")
  }
  return {
    id: row.id as Event.ID,
    type: row.type,
    aggregateID: row.aggregate_id,
    seq,
    data: parseStoredData(row.data),
  }
}

const prepareEvent = (input: PublishEvent): PreparedEvent => {
  const durable = input.definition.durable
  if (durable === undefined) {
    throw new InvalidDurableEventError({
      type: input.definition.type,
      message: `Event type ${input.definition.type} is not durable`,
    })
  }
  const aggregateID = (input.data as Record<string, unknown>)[durable.aggregate]
  if (typeof aggregateID !== "string" || aggregateID.length === 0) {
    throw new InvalidDurableEventError({
      type: input.definition.type,
      message: `Expected a non-empty string aggregate field ${durable.aggregate}`,
    })
  }
  let encoded: Record<string, unknown>
  try {
    encoded = Schema.encodeUnknownSync(input.definition.data)(input.data) as Record<string, unknown>
  } catch (cause) {
    throw new InvalidDurableEventError({
      type: input.definition.type,
      message: `Event payload did not encode: ${cause instanceof Error ? cause.message : String(cause)}`,
    })
  }
  return {
    id: input.id ?? Event.ID.create(),
    type: input.definition.type,
    version: durable.version,
    aggregateID,
    data: input.data,
    encoded,
  }
}

const requireSingleAggregate = (events: ReadonlyArray<PreparedEvent>): string => {
  if (events.length === 0) {
    throw new InvalidDurableEventError({ type: "unknown", message: "publish requires at least one event" })
  }
  const aggregateID = events[0]?.aggregateID
  if (aggregateID === undefined) {
    throw new InvalidDurableEventError({ type: "unknown", message: "publish requires an aggregate" })
  }
  if (events.some((event) => event.aggregateID !== aggregateID)) {
    throw new InvalidDurableEventError({
      type: events[0]?.type ?? "unknown",
      message: "Published events must belong to one aggregate",
    })
  }
  return aggregateID
}

const requireUniqueIds = (events: ReadonlyArray<PreparedEvent>): void => {
  const ids = new Set<string>()
  for (const event of events) {
    if (ids.has(event.id)) {
      throw new InvalidDurableEventError({ type: event.type, message: `Event ${event.id} appears more than once` })
    }
    ids.add(event.id)
  }
}

const requireWritableOwner = (current: SequenceRow | undefined, ownerID: string, aggregateID: string): void => {
  if (current?.ownerID !== undefined && current.ownerID !== ownerID) {
    throw new StaleOwnerError({
      message: `Aggregate ${aggregateID} is leased to ${current.ownerID}, not ${ownerID}`,
    })
  }
}

const writeEvents = (
  connection: Connection,
  ownerID: string,
  projectors: ReadonlyMap<string, ReadonlyArray<Projector>>,
  inputs: PublishEvents,
  touched: Set<string> | undefined,
): readonly Event.Payload[] => {
  const prepared = inputs.map(prepareEvent)
  const aggregateID = requireSingleAggregate(prepared)
  requireUniqueIds(prepared)

  const current = readSequence(connection, aggregateID)
  requireWritableOwner(current, ownerID, aggregateID)

  const block = prepared.length
  const lastSeq = requireSequence(connection.all(RESERVE_BLOCK, [aggregateID, block - 1, ownerID])[0])
  const firstSeq = lastSeq - block + 1
  if (firstSeq < 0) {
    throw new InvalidDurableEventError({
      type: prepared[0]?.type ?? "unknown",
      message: `Reserved block for ${aggregateID} started before sequence zero`,
    })
  }

  const payloads = prepared.map(
    (event, index): Event.Payload => ({
      id: event.id,
      type: event.type,
      durable: { aggregateID, seq: firstSeq + index, version: event.version },
      data: event.data,
    }),
  )

  for (const payload of payloads) {
    const version = payload.durable?.version
    if (version === undefined) continue
    for (const projector of projectors.get(Event.versionedType(payload.type, version)) ?? []) {
      projector(payload)
    }
  }

  for (const [index, event] of prepared.entries()) {
    connection.run("INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?, ?, ?, ?, ?)", [
      event.id,
      aggregateID,
      firstSeq + index,
      Event.versionedType(event.type, event.version),
      JSON.stringify(event.encoded),
    ])
  }

  touched?.add(aggregateID)
  return payloads
}

// Busy retry budget (D5/W1.2): the whole `BEGIN IMMEDIATE … COMMIT` unit is the
// retry boundary, so a busy statement can never leave a half-open transaction.
const BUSY_RETRIES = 5
const BUSY_BASE_DELAY_MS = 10
const BUSY_MAX_DELAY_MS = 1_000

function sleepSync(ms: number): void {
  if (ms <= 0) return
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms)
}

function backoff(failedAttempt: number): number {
  return Math.min(BUSY_BASE_DELAY_MS * 10 ** (failedAttempt - 1), BUSY_MAX_DELAY_MS)
}

/**
 * A busy COMMIT did not commit and the transaction is still open, so re-issue
 * COMMIT in place; replaying the body could duplicate effects.
 */
function commit(connection: Connection): void {
  for (let attempt = 1; ; attempt += 1) {
    try {
      connection.exec("COMMIT")
      return
    } catch (error) {
      if (!isBusyError(error)) throw error
      if (attempt > BUSY_RETRIES) throw new BusyExhaustedError(attempt, error)
      sleepSync(backoff(attempt))
    }
  }
}

const inImmediateTransaction = <T>(connection: Connection, work: () => T): T => {
  for (let attempt = 1; ; attempt += 1) {
    let begun = false
    try {
      connection.exec("BEGIN IMMEDIATE")
      begun = true
      const result = work()
      commit(connection)
      return result
    } catch (error) {
      if (begun) {
        try {
          connection.exec("ROLLBACK")
        } catch {
          // The original failure is authoritative; the connection stays suspect.
        }
      }
      if (!isBusyError(error)) throw error
      if (attempt > BUSY_RETRIES) throw new BusyExhaustedError(attempt, error)
      sleepSync(backoff(attempt))
    }
  }
}

/** Hub-side cache fenced by a monotonically increasing state version. */
export class StateCache<T> {
  private readonly entries = new Map<string, Map<number, T>>()
  private stateVersion = 0

  get version(): number {
    return this.stateVersion
  }

  get(aggregateID: string, seq: number): T | undefined {
    return this.entries.get(aggregateID)?.get(seq)
  }

  set(aggregateID: string, seq: number, value: T, version: number = this.stateVersion): void {
    if (version !== this.stateVersion) {
      throw new StaleStateVersionError({ expected: version, actual: this.stateVersion })
    }
    const aggregate = this.entries.get(aggregateID) ?? new Map<number, T>()
    aggregate.set(seq, value)
    this.entries.set(aggregateID, aggregate)
  }

  delete(aggregateID: string, seq: number): void {
    this.entries.get(aggregateID)?.delete(seq)
  }

  invalidate(aggregateID: string): void {
    this.entries.delete(aggregateID)
    this.stateVersion += 1
  }

  clear(): void {
    this.entries.clear()
    this.stateVersion += 1
  }
}

export function create(options: Options): Interface {
  const { connection, ownerID, cache } = options
  if (ownerID.length === 0) {
    throw new Error("Bus.create requires a non-empty ownerID")
  }
  const projectors = new Map<string, Projector[]>()
  const definitions = options.definitions === undefined ? undefined : Event.durable(options.definitions)

  const publishInTransaction = (events: PublishEvents): readonly Event.Payload[] =>
    writeEvents(connection, ownerID, projectors, events, undefined)

  const transaction = <T>(work: (tx: Transaction) => T): T => {
    const touched = new Set<string>()
    const tx: Transaction = {
      connection,
      all: connection.all.bind(connection),
      run: connection.run.bind(connection),
      publish: (events) => writeEvents(connection, ownerID, projectors, events, touched),
    }
    const result = inImmediateTransaction(connection, () => work(tx))
    for (const aggregateID of touched) cache?.invalidate(aggregateID)
    return result
  }

  const publish = (events: PublishEvents): readonly Event.Payload[] => transaction((tx) => tx.publish(events))

  const project = <D extends Event.Definition>(definition: D, projector: Projector<D>): void => {
    const durable = definition.durable
    if (durable === undefined) {
      throw new InvalidDurableEventError({
        type: definition.type,
        message: `Cannot project non-durable event ${definition.type}`,
      })
    }
    const key = Event.versionedType(definition.type, durable.version)
    const list = projectors.get(key) ?? []
    list.push((event) => projector(event as Event.Payload<D>))
    projectors.set(key, list)
  }

  const latest = (aggregateID: string): number => readSequence(connection, aggregateID)?.seq ?? -1

  const ownerOf = (aggregateID: string): string | undefined => readSequence(connection, aggregateID)?.ownerID

  const read = (input: { readonly aggregateID: string; readonly after?: number }): readonly Event.Payload[] => {
    const rows = connection.all(
      "SELECT id, aggregate_id, seq, type, data FROM event WHERE aggregate_id = ? AND seq > ? ORDER BY seq ASC",
      [input.aggregateID, input.after ?? -1],
    )
    return rows.map((row) => decodeDurablePayload(parseStoredEvent(row), definitions))
  }

  const claim = (aggregateID: string, nextOwnerID: string = ownerID): Lease => {
    const lease = transaction(() => {
      const current = readSequence(connection, aggregateID)
      if (current?.ownerID !== undefined && current.ownerID !== nextOwnerID) {
        throw new StaleOwnerError({
          message: `Aggregate ${aggregateID} is leased to ${current.ownerID}, not ${nextOwnerID}`,
        })
      }
      if (current === undefined) {
        connection.run("INSERT INTO event_sequence (aggregate_id, seq, owner_id) VALUES (?, -1, ?)", [
          aggregateID,
          nextOwnerID,
        ])
      } else if (current.ownerID === undefined) {
        connection.run("UPDATE event_sequence SET owner_id = ? WHERE aggregate_id = ? AND owner_id IS NULL", [
          nextOwnerID,
          aggregateID,
        ])
      }
      return { aggregateID, ownerID: nextOwnerID, seq: latest(aggregateID) }
    })
    cache?.invalidate(aggregateID)
    return lease
  }

  const reclaim = (input: ReclaimInput): Lease => {
    const lease = transaction(() => {
      const current = readSequence(connection, input.aggregateID)
      if (current === undefined) {
        throw new UnknownAggregateError({ message: `Aggregate ${input.aggregateID} has no lease row` })
      }
      if (current.ownerID !== input.expectedOwnerID) {
        throw new StaleOwnerError({
          message: `Aggregate ${input.aggregateID} is leased to ${current.ownerID ?? "none"}, not ${input.expectedOwnerID}`,
        })
      }
      if (current.seq !== input.expectedSeq) {
        throw new LeaseConflictError({
          message: `Aggregate ${input.aggregateID} advanced to sequence ${current.seq}; reclaim expected ${input.expectedSeq}`,
        })
      }
      const outcome = connection.run(
        "UPDATE event_sequence SET owner_id = ? WHERE aggregate_id = ? AND owner_id = ? AND seq = ?",
        [input.nextOwnerID, input.aggregateID, input.expectedOwnerID, input.expectedSeq],
      )
      if (outcome.changes !== 1) {
        throw new LeaseConflictError({
          message: `Reclaim of ${input.aggregateID} did not match the observed lease`,
        })
      }
      return { aggregateID: input.aggregateID, ownerID: input.nextOwnerID, seq: current.seq }
    })
    cache?.invalidate(input.aggregateID)
    return lease
  }

  return {
    connection,
    ownerID,
    publish,
    publishInTransaction,
    transaction,
    project,
    latest,
    ownerOf,
    read,
    claim,
    reclaim,
  }
}
