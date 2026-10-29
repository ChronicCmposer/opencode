export * as EventV2 from "./event"

import { Cause, Context, Effect, Layer, Option, PubSub, Queue, Schema, Stream } from "effect"
import { Event } from "@opencode-ai/schema/event"
import type { Data, Definition, Payload } from "@opencode-ai/schema/event"
import { and, asc, eq, gt, inArray } from "drizzle-orm"
import { Database } from "./database/database"
import { EventSequenceTable, EventTable } from "./event/sql"
import { Location } from "./location"
import { makeGlobalNode, Node, type GlobalNode } from "./effect/app-node"
import { DurableLog } from "./durable-log"
import { isDeepStrictEqual } from "node:util"
import type { SqlClient } from "effect/unstable/sql/SqlClient"
import { Durable } from "@opencode-ai/schema/durable-event-manifest"

export const ID = Event.ID
export type ID = import("@opencode-ai/schema/event").ID
export type { Data, Definition, Payload } from "@opencode-ai/schema/event"

export type Subscriber<D extends Definition = Definition> = (event: Payload<D>) => Effect.Effect<void>
export type Unsubscribe = Effect.Effect<void>

export const latestSequence = Effect.fn("EventV2.latestSequence")(function* (
  db: Database.Interface["db"],
  aggregateID: string,
) {
  assertAggregateScope(
    Option.getOrUndefined(yield* Effect.serviceOption(WorkerScopeService)),
    "event_sequence",
    aggregateID,
  )
  const row = yield* db
    .select({ seq: EventSequenceTable.seq })
    .from(EventSequenceTable)
    .where(eq(EventSequenceTable.aggregate_id, aggregateID))
    .get()
    .pipe(Effect.orDie)
  return row?.seq ?? -1
})

export type SerializedEvent = {
  readonly id: ID
  readonly type: string
  readonly seq: number
  readonly aggregateID: string
  readonly data: Record<string, unknown>
}

export class InvalidDurableEventError extends Schema.TaggedErrorClass<InvalidDurableEventError>()(
  "EventV2.InvalidDurableEvent",
  {
    type: Schema.String,
    message: Schema.String,
  },
) {}

/** A durable write came from a generation that no longer holds the worker lease. */
export class StaleOwnerError extends Schema.TaggedErrorClass<StaleOwnerError>()("EventV2.StaleOwner", {
  message: Schema.String,
}) {}

/** The narrow durable-log operations a bound worker's EventV2 aggregate deletion may use. */
export interface WorkerDurableOperations {
  deleteAggregate(aggregateID: string, options?: { readonly blobDir?: string }): number
}

/** The per-worker scope: lease generation, aggregate fence, and deletion capability. */
export interface WorkerScope {
  readonly sessionID?: string
  durable?: WorkerDurableOperations
  leaseOwner: string | undefined
  getLeaseOwner(): Effect.Effect<string | undefined>
  setLeaseOwner(owner: string): Effect.Effect<void>
  clear(): Effect.Effect<void>
}

/** The Context key a worker graph provides and EventV2 reads optionally. */
export class WorkerScopeService extends Context.Service<WorkerScopeService, WorkerScope>()(
  "@opencode/EventV2/WorkerScope",
) {}

export interface WorkerScopeOptions {
  readonly sessionID?: string
  readonly leaseOwner?: string
  readonly durable?: WorkerDurableOperations
}

/** Build the one scope value a worker database carries for its EventV2 layer. */
export function makeWorkerScope(options: WorkerScopeOptions = {}): WorkerScope {
  let leaseOwner = options.leaseOwner
  return {
    ...(options.sessionID === undefined ? {} : { sessionID: options.sessionID }),
    ...(options.durable === undefined ? {} : { durable: options.durable }),
    get leaseOwner() {
      return leaseOwner
    },
    set leaseOwner(value) {
      leaseOwner = value
    },
    getLeaseOwner: () => Effect.sync(() => leaseOwner),
    setLeaseOwner: (owner) =>
      Effect.sync(() => {
        leaseOwner = owner
      }),
    clear: () =>
      Effect.sync(() => {
        leaseOwner = undefined
      }),
  }
}

/** Durable writes renew the worker lease; a dead generation stops renewing. */
const WORKER_LEASE_TTL_MS = 30_000

/**
 * Preserve EventV2's typed owner error while delegating the actual owner check and conditional renewal to the
 * DurableLog primitives.
 */
function asEventOwnerError<A>(work: () => A): A {
  try {
    return work()
  } catch (error) {
    if (error instanceof DurableLog.StaleOwnerError) throw new StaleOwnerError({ message: error.message })
    throw error
  }
}

/** Fail loud when a bound worker addresses another session's aggregate. */
function assertAggregateScope(scope: WorkerScope | undefined, type: string, aggregateID: string): void {
  if (scope?.sessionID !== undefined && aggregateID !== scope.sessionID) {
    throw new InvalidDurableEventError({
      type,
      message: `Aggregate ${aggregateID} is outside the worker's bound session ${scope.sessionID}`,
    })
  }
}

/** A stored `event` row as the SQL layer presents it (before any decoding). */
type StoredEventRow = {
  readonly id: ID
  readonly type: string
  readonly aggregate_id: string
  readonly seq: number
  readonly data: Record<string, unknown>
}

/** Lift a stored row into the serialized envelope both decode paths share. */
const serializeStoredRow = (row: StoredEventRow): SerializedEvent => ({
  id: row.id,
  type: row.type,
  aggregateID: row.aggregate_id,
  seq: row.seq,
  data: row.data,
})

/** The undecoded payload envelope every stored event decodes into. */
const payloadEnvelope = <Version extends number | undefined>(
  event: SerializedEvent,
  definition: Definition,
  version: Version,
) => ({
  id: event.id,
  type: definition.type,
  durable: {
    aggregateID: event.aggregateID,
    seq: event.seq,
    version,
  },
  data: event.data,
})

/** Decode one durable stored event. */
const decodeWithDefinition = (event: SerializedEvent, definition: Definition): Payload => {
  const durable = definition.durable
  if (durable === undefined) {
    throw new InvalidDurableEventError({ type: event.type, message: `Event type ${event.type} is not durable` })
  }
  return {
    ...payloadEnvelope(event, definition, durable.version),
    data: Schema.decodeUnknownSync(definition.data)(event.data),
  }
}

/**
 * The one durable-event decode policy, shared by the Effect read paths and the synchronous worker DurableLog.
 */
export const decodeDurablePayload = (
  event: SerializedEvent,
  definitions: ReadonlyMap<string, Definition> = Durable,
): Payload => {
  const definition = definitions.get(event.type)
  if (!definition?.durable) {
    throw new InvalidDurableEventError({ type: event.type, message: `Unknown durable event type ${event.type}` })
  }
  return decodeWithDefinition(event, definition)
}

const decodeSerializedEvent = (event: SerializedEvent): Effect.Effect<Payload, InvalidDurableEventError> =>
  Effect.gen(function* () {
    const definition = Durable.get(event.type)
    if (!definition?.durable) {
      return yield* new InvalidDurableEventError({
        type: event.type,
        message: `Unknown durable event type ${event.type}`,
      })
    }
    return yield* Effect.sync(() => decodeWithDefinition(event, definition)).pipe(Effect.orDie)
  })

export const readAggregate = Effect.fn("EventV2.readAggregate")(function* <A>(
  db: Database.Interface["db"],
  input: {
    readonly aggregateID: string
    readonly after?: number
    readonly limit: number
    readonly manifest: {
      readonly definitions: ReadonlyMap<string, Definition>
      readonly schema: Schema.Decoder<A, never>
    }
  },
) {
  const after = input.after ?? -1
  assertAggregateScope(
    Option.getOrUndefined(yield* Effect.serviceOption(WorkerScopeService)),
    "event",
    input.aggregateID,
  )
  const rows = yield* db
    .select()
    .from(EventTable)
    .where(
      and(
        eq(EventTable.aggregate_id, input.aggregateID),
        gt(EventTable.seq, after),
        inArray(EventTable.type, Array.from(input.manifest.definitions.keys())),
      ),
    )
    .orderBy(asc(EventTable.seq))
    .limit(input.limit + 1)
    .all()
    .pipe(Effect.orDie)
  // A stored blob reference proves the schema carried `event_blob`; the read path
  // never consults `supportsTable`.
  const page = yield* resolveStoredRows(db, input.aggregateID, rows.slice(0, input.limit), undefined, true)
  const decode = Schema.decodeUnknownSync(input.manifest.schema)
  const events = page.flatMap((row) => {
    const definition = input.manifest.definitions.get(row.type)
    // The query already restricted rows to the manifest's definition keys, so a missing definition is
    // impossible; skip defensively rather than fabricate a type from the raw row.
    if (definition === undefined) return []
    return [decode(payloadEnvelope(serializeStoredRow(row), definition, definition.durable?.version))]
  })
  return {
    events,
    hasMore: rows.length > input.limit,
  }
})

export class SubscriberOverflowError extends Schema.TaggedErrorClass<SubscriberOverflowError>()(
  "EventV2.SubscriberOverflow",
  { capacity: Schema.Int },
) {}

export const define = Event.define
export const versionedType = Event.versionedType

export interface PublishOptions {
  readonly id?: ID
  readonly metadata?: Record<string, unknown>
  readonly location?: Location.Ref
  /** Local operational projection committed atomically with a new durable event. Not replayed or serialized. */
  readonly commit?: (seq: number) => Effect.Effect<void>
}

export interface Interface {
  readonly publish: <D extends Definition>(
    definition: D,
    data: Data<D>,
    options?: PublishOptions,
  ) => Effect.Effect<Payload<D>>
  /**
   * Publish an already-formed payload live-only: notify subscribers without a `Definition` lookup and without
   * the durable commit path.
   */
  readonly publishLive: (event: Payload) => Effect.Effect<void>
  /**
   * Wake the `durable({ aggregateID })` subscribers for an aggregate WITHOUT committing or persisting: a
   * writer in another isolate owns the durable commit, and the local durable stream re-reads the aggregate to
   * observe it.
   */
  readonly wakeDurable?: (aggregateID: string) => Effect.Effect<void>
  readonly subscribe: <D extends Definition>(definition: D) => Stream.Stream<Payload<D>>
  readonly all: () => Stream.Stream<Payload>
  readonly durable: (input: { readonly aggregateID: string; readonly after?: number }) => Stream.Stream<Payload>
  /** @deprecated Use `all()` and consume the returned stream. */
  readonly listen: (listener: Subscriber) => Effect.Effect<Unsubscribe>
  readonly project: <D extends Definition>(definition: D, projector: Subscriber<D>) => Effect.Effect<void>
  readonly replay: (
    event: SerializedEvent,
    options?: { readonly publish?: boolean; readonly ownerID?: string; readonly strictOwner?: boolean },
  ) => Effect.Effect<void>
  readonly replayAll: (
    events: SerializedEvent[],
    options?: { readonly publish?: boolean; readonly ownerID?: string; readonly strictOwner?: boolean },
  ) => Effect.Effect<string | undefined>
  readonly remove: (aggregateID: string) => Effect.Effect<void>
  readonly claim: (aggregateID: string, ownerID: string) => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/Event") {}

export const allBounded = (events: Interface, capacity: number) =>
  Effect.gen(function* () {
    const queue = yield* Queue.dropping<Payload, SubscriberOverflowError>(capacity)
    const unsubscribe = yield* events.listen((event) =>
      Queue.offer(queue, event).pipe(
        Effect.flatMap((accepted) =>
          accepted ? Effect.void : Queue.fail(queue, new SubscriberOverflowError({ capacity })).pipe(Effect.asVoid),
        ),
      ),
    )
    yield* Effect.addFinalizer(() => unsubscribe.pipe(Effect.andThen(Queue.shutdown(queue)), Effect.asVoid))
    return Stream.fromQueue(queue)
  })

export interface LayerOptions {
  readonly beforeAggregateRead?: (aggregateID: string) => Effect.Effect<void>
  /** D16 content-addressed directory owned by this EventV2 layer. */
  readonly blobDir?: string
}

/** The Effect SQL client behind `Database.Interface["db"]`. */
type DurableClient = SqlClient

/** The driver client carries the file it actually opened (`bun:sqlite`/`node:sqlite`). */
interface ClientWithFilename {
  readonly config?: { readonly filename?: string }
}

function clientFilename(client: DurableClient): string | undefined {
  return (client as DurableClient & ClientWithFilename).config?.filename
}

/**
 * The DurableLog drives a synchronous connection, so this adapter is only used for clients whose statements
 * complete synchronously (the hub database and the unbound delete path).
 */
function busConnection(client: DurableClient): DurableLog.Connection {
  return DurableLog.connectionFromClient(client)
}

/** The D16 directory for the file this client actually opened. */
function defaultBlobDirectory(client: DurableClient): string | undefined {
  return DurableLog.blobDirectoryFor(clientFilename(client))
}

function durableClient(db: Database.Interface["db"]): DurableClient | undefined {
  return (db as Database.Interface["db"] & { readonly $client?: DurableClient }).$client
}

function hasEventBlobTable(client: DurableClient): Effect.Effect<boolean> {
  return client
    .unsafe<{ readonly name: string }>("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'event_blob'")
    .pipe(
      Effect.map((rows) => rows.length > 0),
      Effect.orDie,
    )
}

/**
 * Stage an oversized durable payload through D16's content-addressed file before the write transaction
 * starts.
 */
function prepareOversizedPayload(
  db: Database.Interface["db"],
  encoded: Record<string, unknown>,
  explicitBlobDir: string | undefined,
  supportsTable: boolean,
): DurableLog.BlobReference | undefined {
  const payload = JSON.stringify(encoded)
  const size = Buffer.byteLength(payload)
  if (size <= DurableLog.DEFAULT_INLINE_PAYLOAD_LIMIT_BYTES) return undefined
  const client = durableClient(db)
  if (client === undefined) {
    throw new InvalidDurableEventError({
      type: "event",
      message: `durable payload is ${size} bytes (limit ${DurableLog.DEFAULT_INLINE_PAYLOAD_LIMIT_BYTES}) but this database has no DurableLog blob path`,
    })
  }
  if (!supportsTable) {
    throw new InvalidDurableEventError({
      type: "event",
      message: `durable payload is ${size} bytes (limit ${DurableLog.DEFAULT_INLINE_PAYLOAD_LIMIT_BYTES}) but this schema has no event_blob table`,
    })
  }
  const blobDir = explicitBlobDir ?? defaultBlobDirectory(client)
  if (blobDir === undefined) {
    throw new InvalidDurableEventError({
      type: "event",
      message: `durable payload is ${size} bytes (limit ${DurableLog.DEFAULT_INLINE_PAYLOAD_LIMIT_BYTES}) and no blob directory is configured`,
    })
  }
  const hash = DurableLog.digest(payload)
  DurableLog.writeBlobFile(blobDir, hash, payload)
  return { __eventBlob: hash, size }
}

/** SQLite binds at most 999 parameters by default; chunk well below it. */
const BLOB_QUERY_CHUNK = 500

/**
 * Load blob rows through the Effect client so both synchronous and worker-backed (asynchronous acquisition)
 * clients can resolve references.
 */
const loadBlobRows = (
  client: DurableClient,
  aggregateID: string,
  sequences: ReadonlyArray<number>,
): Effect.Effect<ReadonlyMap<number, Record<string, unknown>>> =>
  Effect.gen(function* () {
    const rows = new Map<number, Record<string, unknown>>()
    for (let offset = 0; offset < sequences.length; offset += BLOB_QUERY_CHUNK) {
      const query = DurableLog.blobRowQuery(aggregateID, sequences.slice(offset, offset + BLOB_QUERY_CHUNK))
      const chunk = yield* client.unsafe<Record<string, unknown>>(query.sql, query.params)
      for (const [seq, row] of DurableLog.blobRowsBySeq(chunk)) rows.set(seq, row)
    }
    return rows
  }).pipe(Effect.orDie)

/** Resolve `event.data` blob references for a page of stored rows. */
function resolveStoredRows(
  db: Database.Interface["db"],
  aggregateID: string,
  rows: ReadonlyArray<StoredEventRow>,
  explicitBlobDir: string | undefined,
  supportsTable: boolean,
): Effect.Effect<ReadonlyArray<StoredEventRow>> {
  const referenced = rows.filter((row) => DurableLog.isBlobReference(row.data))
  if (referenced.length === 0) return Effect.succeed(rows)
  const client = durableClient(db)
  if (client === undefined) {
    return Effect.die(
      new InvalidDurableEventError({
        type: "event",
        message: `aggregate ${aggregateID} has blob-backed events but this database has no DurableLog blob path`,
      }),
    )
  }
  return Effect.gen(function* () {
    const blobRows = yield* loadBlobRows(
      client,
      aggregateID,
      referenced.map((row) => row.seq),
    )
    const blobDir = explicitBlobDir ?? defaultBlobDirectory(client)
    const blob: DurableLog.BlobStore = {
      ...(blobDir === undefined ? {} : { dir: blobDir }),
      limitBytes: DurableLog.DEFAULT_INLINE_PAYLOAD_LIMIT_BYTES,
      supportsTable,
    }
    const files = new Map<string, string>()
    return rows.map((row) => {
      if (!DurableLog.isBlobReference(row.data)) return row
      return {
        ...row,
        data: DurableLog.resolveBlobData(serializeStoredRow(row), blobRows.get(row.seq), blob, files),
      }
    })
  })
}

/**
 * Delete one aggregate's durable log, blob rows, sequence row, and any content-addressed files left with zero
 * references across every aggregate, in one DurableLog `BEGIN IMMEDIATE`.
 */
export const deleteAggregate = (
  db: Database.Interface["db"],
  aggregateID: string,
  options: { readonly blobDir?: string } = {},
): Effect.Effect<number> =>
  Effect.gen(function* () {
    const worker = Option.getOrUndefined(yield* Effect.serviceOption(WorkerScopeService))
    assertAggregateScope(worker, "event", aggregateID)
    const client = (db as Database.Interface["db"] & { readonly $client?: DurableClient }).$client
    if (client === undefined) return yield* removeRows(db, aggregateID)
    const blobDir = options.blobDir ?? defaultBlobDirectory(client)
    if (worker?.sessionID !== undefined && worker.durable === undefined) {
      return yield* Effect.die(
        new InvalidDurableEventError({
          type: "event",
          message: "a bound worker's aggregate deletion requires the worker durable capability",
        }),
      )
    }
    return yield* Effect.sync(() => {
      if (worker?.durable !== undefined) {
        return worker.durable.deleteAggregate(aggregateID, blobDir === undefined ? {} : { blobDir })
      }
      const bus = DurableLog.create({
        connection: busConnection(client),
        ownerID: "event-v2-delete",
        aggregateID,
        ...(blobDir === undefined ? {} : { blobDir }),
      })
      return bus.deleteAggregate(aggregateID)
    }).pipe(Effect.orDie)
  })

/** Fallback for reduced database stubs that carry no SQL client. */
const removeRows = (db: Database.Interface["db"], aggregateID: string): Effect.Effect<number> =>
  db
    .transaction(() =>
      Effect.gen(function* () {
        yield* db.delete(EventSequenceTable).where(eq(EventSequenceTable.aggregate_id, aggregateID)).run()
        yield* db.delete(EventTable).where(eq(EventTable.aggregate_id, aggregateID)).run()
        return 0
      }),
    )
    .pipe(Effect.orDie)

export const layerWith = (options?: LayerOptions) =>
  Layer.effect(
    Service,
    Effect.gen(function* () {
      const pubsub = {
        all: yield* PubSub.unbounded<Payload>(),
        durable: new Map<string, Set<PubSub.PubSub<void>>>(),
        typed: new Map<string, PubSub.PubSub<Payload>>(),
      }
      const projectors = new Map<string, Subscriber[]>()
      // TODO: Bind durable projectors to exact type+version before supporting incompatible historical payloads.
      const listeners = new Array<Subscriber>()
      const { db } = yield* Database.Service
      // The one durable-write implementation.
      const durableLog = yield* DurableLog.Service
      const worker = Option.getOrUndefined(yield* Effect.serviceOption(WorkerScopeService))
      const client = durableClient(db)
      const blobDir = options?.blobDir ?? (client === undefined ? undefined : defaultBlobDirectory(client))
      // Probe the schema once, through the Effect client.
      const supportsTable = client === undefined ? false : yield* hasEventBlobTable(client)
      const supportsWorkerLease = durableLog.workerLease
      // The same store `DurableLog.create` uses: an inline body below the frame-headroom
      // bound, and a commit-scoped re-stage for any file a concurrent GC unlinked.
      const blob: DurableLog.BlobStore = {
        ...(blobDir === undefined ? {} : { dir: blobDir }),
        limitBytes: DurableLog.DEFAULT_INLINE_PAYLOAD_LIMIT_BYTES,
        supportsTable,
      }
      const writes =
        client === undefined ? undefined : DurableLog.createEffectDurableRepository(client, supportsWorkerLease)

      const getOrCreate = (definition: Definition) =>
        Effect.gen(function* () {
          const existing = pubsub.typed.get(definition.type)
          if (existing) return existing
          const created = yield* PubSub.unbounded<Payload>()
          pubsub.typed.set(definition.type, created)
          return created
        })

      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          yield* PubSub.shutdown(pubsub.all)
          yield* Effect.forEach(
            pubsub.durable.values(),
            (pubsubs) => Effect.forEach(pubsubs, PubSub.shutdown, { discard: true }),
            { discard: true },
          )
          yield* Effect.forEach(pubsub.typed.values(), PubSub.shutdown, { discard: true })
        }),
      )

      function commitDurableEvent(
        definition: Definition,
        event: Payload,
        input?: {
          readonly seq: number
          readonly aggregateID: string
          readonly ownerID?: string
          readonly strictOwner?: boolean
        },
        commit?: (seq: number) => Effect.Effect<void>,
      ) {
        return Effect.gen(function* () {
          const durable = definition?.durable
          if (durable) {
            const aggregateID = (event.data as Record<string, unknown>)[durable.aggregate]
            if (typeof aggregateID !== "string") {
              yield* Effect.die(
                new InvalidDurableEventError({
                  type: event.type,
                  message: `Expected string aggregate field ${durable.aggregate}`,
                }),
              )
            } else {
              if (worker?.sessionID !== undefined && aggregateID !== worker.sessionID) {
                yield* Effect.die(
                  new InvalidDurableEventError({
                    type: event.type,
                    message: `Aggregate ${aggregateID} is outside the worker's bound session ${worker.sessionID}`,
                  }),
                )
              }
              if (input && input.aggregateID !== aggregateID) {
                yield* Effect.die(
                  new InvalidDurableEventError({
                    type: event.type,
                    message: `Aggregate mismatch: expected ${input.aggregateID}, got ${aggregateID}`,
                  }),
                )
              }
              const leaseOwner = worker === undefined ? undefined : yield* worker.getLeaseOwner()
              // A worker generation must never be written into the v1 sync `owner_id` column.
              if (leaseOwner !== undefined && !supportsWorkerLease) {
                return yield* Effect.die(
                  new DurableLog.WorkerLeaseUnsupportedError({
                    type: event.type,
                    message: `Aggregate ${aggregateID} requires a worker lease but this schema has no worker_owner_id column`,
                  }),
                )
              }
              const list = projectors.get(event.type) ?? []
              const encoded = Schema.encodeUnknownSync(definition.data)(event.data) as Record<string, unknown>
              // Stage the D16 file before the write lock; the transaction
              // re-validates it and records the reference atomically.
              prepareOversizedPayload(db, encoded, options?.blobDir, supportsTable)
              return yield* Effect.uninterruptible(
                Effect.gen(function* () {
                  const committed = yield* db
                    .transaction(
                      () =>
                        Effect.gen(function* () {
                          if (writes === undefined) {
                            return yield* Effect.die(
                              new InvalidDurableEventError({
                                type: event.type,
                                message: "durable commit requires a SQL connection",
                              }),
                            )
                          }
                          const row = yield* writes.sequence(aggregateID)
                          if (leaseOwner !== undefined) {
                            // Same owner check and conditional renewal as the DurableLog publisher; a
                            // superseded generation can never re-stamp its lease.
                            asEventOwnerError(() =>
                              DurableLog.assertWritableOwner(row, leaseOwner, aggregateID, true),
                            )
                          }
                          const renew = () => {
                            if (leaseOwner === undefined) return Effect.void
                            return DurableLog.renewWorkerLease(writes, {
                              aggregateID,
                              ownerID: leaseOwner,
                              leaseTimeoutMs: WORKER_LEASE_TTL_MS,
                            }).pipe(
                              // Keep EventV2's typed owner error on the public
                              // surface while the check itself stays in DurableLog.
                              Effect.catchDefect((defect) =>
                                defect instanceof DurableLog.StaleOwnerError
                                  ? Effect.die(new StaleOwnerError({ message: defect.message }))
                                  : Effect.die(defect),
                              ),
                            )
                          }
                          const latest = row?.seq ?? -1
                          if (input?.strictOwner && row?.syncOwnerID && row.syncOwnerID !== input.ownerID) {
                            yield* Effect.die(
                              new InvalidDurableEventError({
                                type: event.type,
                                message: `Replay owner mismatch for aggregate ${aggregateID}: expected ${row.syncOwnerID}, got ${input.ownerID ?? "none"}`,
                              }),
                            )
                          }
                          if (input && input.seq <= latest) {
                            const stored = yield* db
                              .select()
                              .from(EventTable)
                              .where(and(eq(EventTable.aggregate_id, aggregateID), eq(EventTable.seq, input.seq)))
                              .get()
                              .pipe(Effect.orDie)
                            const storedData =
                              stored === undefined
                                ? undefined
                                : (yield* resolveStoredRows(
                                    db,
                                    aggregateID,
                                    [stored],
                                    options?.blobDir,
                                    supportsTable,
                                  ))[0]?.data
                            if (
                              stored?.id === event.id &&
                              stored.type === versionedType(definition.type, durable.version) &&
                              isDeepStrictEqual(storedData, encoded)
                            ) {
                              if (input.ownerID && row?.syncOwnerID == null) {
                                yield* writes.fillSyncOwner({ aggregateID, ownerID: input.ownerID })
                              }
                              yield* renew()
                              return
                            }
                            yield* Effect.die(
                              new InvalidDurableEventError({
                                type: event.type,
                                message: `Replay diverged at aggregate ${aggregateID} sequence ${input.seq}`,
                              }),
                            )
                          }
                          if (input && row?.syncOwnerID && row.syncOwnerID !== input.ownerID) {
                            yield* renew()
                            return
                          }
                          const seq = input?.seq ?? latest + 1
                          if (input && seq !== latest + 1) {
                            yield* Effect.die(
                              new InvalidDurableEventError({
                                type: event.type,
                                message: `Sequence mismatch for aggregate ${aggregateID}: expected ${latest + 1}, got ${seq}`,
                              }),
                            )
                          }
                          const stored = yield* db
                            .select({ aggregateID: EventTable.aggregate_id, seq: EventTable.seq })
                            .from(EventTable)
                            .where(eq(EventTable.id, event.id))
                            .get()
                            .pipe(Effect.orDie)
                          if (stored)
                            yield* Effect.die(
                              new InvalidDurableEventError({
                                type: event.type,
                                message: `Event ${event.id} already exists at aggregate ${stored.aggregateID} sequence ${stored.seq}`,
                              }),
                            )
                          // Reserve through the DurableLog primitive: a new append uses the block-reservation
                          // upsert, an exact replay sets the observed position.
                          if (input === undefined) {
                            const reserved = yield* writes.appendSequence({ aggregateID, block: 1 })
                            if (reserved !== seq) {
                              yield* Effect.die(
                                new InvalidDurableEventError({
                                  type: event.type,
                                  message: `Sequence reservation for aggregate ${aggregateID} returned ${reserved}; expected ${seq}`,
                                }),
                              )
                            }
                          } else {
                            yield* writes.advanceSequence({
                              aggregateID,
                              seq,
                              ...(input.ownerID ? { syncOwnerID: input.ownerID } : {}),
                            })
                          }
                          const committed = {
                            ...event,
                            durable: { aggregateID, seq, version: durable.version },
                          } as Payload
                          for (const projector of list) {
                            yield* projector(committed)
                          }
                          if (commit) yield* commit(seq)
                          yield* DurableLog.writePreparedEvent(writes, blob, {
                            aggregateID,
                            seq,
                            id: event.id,
                            type: versionedType(definition.type, durable.version),
                            encoded,
                          })
                          yield* renew()
                          return { aggregateID, seq }
                        }),
                      { behavior: "immediate" },
                    )
                    // The adapter retries a busy `BEGIN IMMEDIATE`/`COMMIT` at the transaction boundary; the
                    // body itself is never replayed inside a failed transaction.
                    .pipe(Effect.orDie)
                  if (committed) {
                    yield* Effect.forEach(
                      pubsub.durable.get(committed.aggregateID) ?? [],
                      (wake) => PubSub.publish(wake, undefined),
                      { discard: true },
                    )
                  }
                  return committed
                }),
              )
            }
          }
        })
      }

      function publishEvent<D extends Definition>(definition: D, event: Payload<D>, commit?: PublishOptions["commit"]) {
        return Effect.gen(function* () {
          if (!definition?.durable && commit)
            return yield* Effect.die(
              new InvalidDurableEventError({
                type: event.type,
                message: "Local commit hooks require a durable event",
              }),
            )
          if (definition?.durable) {
            const committed = yield* commitDurableEvent(definition, event as Payload, undefined, commit)
            if (committed) {
              event = {
                ...event,
                durable: {
                  aggregateID: committed.aggregateID,
                  seq: committed.seq,
                  version: definition.durable.version,
                },
              }
              yield* notify(event as Payload, true)
              return event
            }
          }
          yield* notify(event as Payload, false)
          return event
        })
      }

      const observe = (event: Payload, observer: (event: Payload) => Effect.Effect<void>) =>
        Effect.suspend(() => observer(event)).pipe(
          Effect.catchCauseIf(
            (cause) => !Cause.hasInterrupts(cause),
            (cause) => Effect.logError("Event listener failed", { eventID: event.id, eventType: event.type, cause }),
          ),
        )

      function notify(event: Payload, isolateListeners: boolean) {
        return Effect.gen(function* () {
          // When `isolateListeners` is set, one failing observer is caught and logged by `observe` instead of
          // aborting the publish for the others (or the caller).
          yield* Effect.forEach(
            listeners,
            (listener) => (isolateListeners ? observe(event, listener) : listener(event)),
            { discard: true },
          )
          const typed = pubsub.typed.get(event.type)
          if (typed) yield* PubSub.publish(typed, event)
          yield* PubSub.publish(pubsub.all, event)
        })
      }

      function publish<D extends Definition>(definition: D, data: Data<D>, options?: PublishOptions) {
        return Effect.gen(function* () {
          const serviceLocation = Option.getOrUndefined(yield* Effect.serviceOption(Location.Service))
          const location =
            options?.location ??
            (serviceLocation
              ? { directory: serviceLocation.directory, workspaceID: serviceLocation.workspaceID }
              : undefined)
          return yield* publishEvent(
            definition,
            {
              id: options?.id ?? ID.create(),
              ...(options?.metadata ? { metadata: options.metadata } : {}),
              type: definition.type,
              ...(location ? { location } : {}),
              data,
            } as Payload<D>,
            options?.commit,
          )
        })
      }

      function replay(
        event: SerializedEvent,
        options?: { readonly publish?: boolean; readonly ownerID?: string; readonly strictOwner?: boolean },
      ) {
        return Effect.gen(function* () {
          const definition = Durable.get(event.type)
          if (!definition?.durable) {
            yield* Effect.die(
              new InvalidDurableEventError({ type: event.type, message: `Unknown durable event type ${event.type}` }),
            )
          } else {
            const payload = {
              id: event.id,
              type: definition.type,
              data: Schema.decodeUnknownSync(definition.data)(event.data),
            } as Payload
            const committed = yield* commitDurableEvent(definition, payload, {
              seq: event.seq,
              aggregateID: event.aggregateID,
              ownerID: options?.ownerID,
              strictOwner: options?.strictOwner,
            })
            if (committed && options?.publish) {
              yield* notify(
                {
                  ...payload,
                  durable: {
                    aggregateID: committed.aggregateID,
                    seq: committed.seq,
                    version: definition.durable.version,
                  },
                },
                true,
              )
            }
          }
        })
      }

      function replayAll(
        events: SerializedEvent[],
        options?: { readonly publish?: boolean; readonly ownerID?: string; readonly strictOwner?: boolean },
      ) {
        return Effect.gen(function* () {
          const source = events[0]?.aggregateID
          if (!source) return undefined
          if (events.some((event) => event.aggregateID !== source)) {
            yield* Effect.die(
              new InvalidDurableEventError({
                type: events[0]?.type ?? "unknown",
                message: "Replay events must belong to the same aggregate",
              }),
            )
          }
          const start = events[0]?.seq ?? 0
          for (const [index, event] of events.entries()) {
            const seq = start + index
            if (event.seq !== seq) {
              yield* Effect.die(
                new InvalidDurableEventError({
                  type: event.type,
                  message: `Replay sequence mismatch at index ${index}: expected ${seq}, got ${event.seq}`,
                }),
              )
            }
          }
          for (const event of events) {
            yield* replay(event, options)
          }
          return source
        })
      }

      function remove(aggregateID: string) {
        const deletion = deleteAggregate(
          db,
          aggregateID,
          options?.blobDir === undefined ? {} : { blobDir: options.blobDir },
        )
        // The service's effects run in the caller's context; re-provide the
        // captured scope so a direct layer (without a graph root) still fences.
        return worker === undefined
          ? deletion
          : deletion.pipe(Effect.provideService(WorkerScopeService, worker))
      }

      function claim(aggregateID: string, ownerID: string) {
        return Effect.gen(function* () {
          assertAggregateScope(worker, "event_sequence", aggregateID)
          if (writes === undefined) {
            return yield* Effect.die(
              new InvalidDurableEventError({
                type: "event_sequence",
                message: "claim requires a SQL connection",
              }),
            )
          }
          yield* writes.claimSyncOwner({ aggregateID, ownerID })
        })
      }

      const subscribe = <D extends Definition>(definition: D): Stream.Stream<Payload<D>> =>
        Stream.unwrap(getOrCreate(definition).pipe(Effect.map((pubsub) => Stream.fromPubSub(pubsub)))).pipe(
          Stream.map((event) => event as Payload<D>),
        )

      const streamAll = (): Stream.Stream<Payload> => Stream.fromPubSub(pubsub.all)

      const readAfter = (aggregateID: string, after: number) =>
        Effect.suspend(() => {
          assertAggregateScope(worker, "event", aggregateID)
          return (options?.beforeAggregateRead?.(aggregateID) ?? Effect.void).pipe(
            Effect.andThen(
              db
                .select()
                .from(EventTable)
                .where(and(eq(EventTable.aggregate_id, aggregateID), gt(EventTable.seq, after)))
                .orderBy(asc(EventTable.seq))
                .all(),
            ),
            Effect.orDie,
            Effect.flatMap((rows) => resolveStoredRows(db, aggregateID, rows, options?.blobDir, supportsTable)),
            Effect.flatMap((rows) => Effect.forEach(rows, (row) => decodeSerializedEvent(serializeStoredRow(row)))),
            Effect.orDie,
          )
        })

      const subscribeDurable = (aggregateID: string) =>
        Effect.gen(function* () {
          const wake = yield* PubSub.sliding<void>(1)
          const subscription = yield* PubSub.subscribe(wake)
          yield* Effect.acquireRelease(
            Effect.sync(() => {
              const wakes = pubsub.durable.get(aggregateID) ?? new Set()
              wakes.add(wake)
              pubsub.durable.set(aggregateID, wakes)
            }),
            () =>
              Effect.sync(() => {
                const wakes = pubsub.durable.get(aggregateID)
                wakes?.delete(wake)
                if (wakes?.size === 0) pubsub.durable.delete(aggregateID)
              }).pipe(Effect.andThen(PubSub.shutdown(wake))),
          )
          return subscription
        })

      /**
       * Publish the per-aggregate durable wake exactly as `commitDurableEvent` does after a commit — without
       * re-committing or touching the database.
       */
      function wakeDurable(aggregateID: string): Effect.Effect<void> {
        return Effect.forEach(pubsub.durable.get(aggregateID) ?? [], (wake) => PubSub.publish(wake, undefined), {
          discard: true,
        })
      }

      const durable = (input: { readonly aggregateID: string; readonly after?: number }): Stream.Stream<Payload> =>
        Stream.unwrap(
          Effect.gen(function* () {
            const wakes = yield* subscribeDurable(input.aggregateID)
            let sequence = input.after ?? -1
            const read = Effect.suspend(() => readAfter(input.aggregateID, sequence)).pipe(
              Effect.tap((events) =>
                Effect.sync(() => {
                  sequence = events.at(-1)?.durable?.seq ?? sequence
                }),
              ),
            )
            const historical = yield* read
            const live = Stream.fromSubscription(wakes).pipe(
              Stream.mapEffect(() => read),
              Stream.flattenIterable,
            )
            return Stream.concat(Stream.fromIterable(historical), live)
          }),
        )

      const listen = (listener: Subscriber): Effect.Effect<Unsubscribe> =>
        Effect.sync(() => {
          listeners.push(listener)
          return Effect.sync(() => {
            const index = listeners.indexOf(listener)
            if (index >= 0) listeners.splice(index, 1)
          })
        })

      const project = <D extends Definition>(definition: D, projector: Subscriber<D>): Effect.Effect<void> =>
        Effect.sync(() => {
          const list = projectors.get(definition.type) ?? []
          list.push((event) => projector(event as Payload<D>))
          projectors.set(definition.type, list)
        })

      return Service.of({
        publish,
        publishLive: (event) => notify(event, true),
        wakeDurable,
        subscribe,
        all: streamAll,
        durable,
        listen,
        project,
        replay,
        replayAll,
        remove,
        claim,
      })
    }),
  )

export const layer = layerWith()
const eventNode: GlobalNode<Service> = {
  kind: "layer",
  name: Service.key,
  service: Service,
  implementation: layer,
  dependencies: [],
  tag: Node.tags.values.global,
}
// `durable-log.ts` and `event.ts` form a type-level cycle (the log decodes through EventV2).
Object.defineProperty(eventNode, "dependencies", {
  configurable: true,
  enumerable: true,
  get: () => [Database.node, DurableLog.node],
})
export const node = eventNode
