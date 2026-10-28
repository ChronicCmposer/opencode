export * as EventV2 from "./event"

import { Cause, Context, Effect, Layer, Option, PubSub, Queue, Schema, Stream } from "effect"
import { Event } from "@opencode-ai/schema/event"
import type { Data, Definition, Payload } from "@opencode-ai/schema/event"
import { and, asc, eq, gt, inArray, isNull } from "drizzle-orm"
import { Database } from "./database/database"
import { EventBlobTable, EventSequenceTable, EventTable } from "./event/sql"
import { Location } from "./location"
import { makeGlobalNode } from "./effect/app-node"
import { Bus } from "./bus"
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
  assertAggregateScope(workerScopeOf(db), "event_sequence", aggregateID)
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

/**
 * The narrow durable-log operations a bound worker's EventV2 aggregate deletion
 * may use. The worker DB owns a viewless Bus handle with a session-bound
 * authorizer and exposes only these operations (never a raw connection); the
 * BlobStore refcount probe is inherently cross-aggregate and must not run
 * through the session-filtered views.
 */
export interface WorkerDurableOperations {
  deleteAggregate(aggregateID: string, options?: { readonly blobDir?: string }): number
}

/** The per-database worker binding: lease generation, aggregate fence, deletion capability. */
export interface WorkerScope {
  leaseOwner?: string
  sessionID?: string
  durable?: WorkerDurableOperations
}

const workerScopes = new WeakMap<object, WorkerScope>()

/** Create the binding a worker database carries for its EventV2 layer. */
export function createWorkerScope(sessionID?: string): WorkerScope {
  return sessionID === undefined ? {} : { sessionID }
}

/** Attach a worker binding to the database instance it scopes. */
export function bindWorkerScope(db: object, scope: WorkerScope): void {
  workerScopes.set(db, scope)
}

/** The binding for one database, or `undefined` for an unbound hub database. */
export function workerScopeOf(db: object): WorkerScope | undefined {
  return workerScopes.get(db)
}

/** Durable writes renew the worker lease; a dead generation stops renewing. */
const WORKER_LEASE_TTL_MS = 30_000

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

/**
 * Lift a stored row into the serialized envelope both decode paths share. The
 * stored `type` is the *versioned* key (`message.removed.1`), which is what the
 * durable manifest is indexed by, so it must be preserved verbatim.
 */
const serializeStoredRow = (row: StoredEventRow): SerializedEvent => ({
  id: row.id,
  type: row.type,
  aggregateID: row.aggregate_id,
  seq: row.seq,
  data: row.data,
})

/**
 * The undecoded payload envelope every stored event decodes into. Centralizing it
 * keeps `readAggregate` and `readAfter` from drifting on how `durable.version`
 * and the aggregate identity are laid out.
 */
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

/**
 * Decode one durable stored event. Returns a typed failure (rather than the
 * previous synchronous throw / raw `decodeUnknownSync`) so callers must decide
 * how an unknown or malformed durable row is surfaced.
 */
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
 * The one durable-event decode policy, shared by the Effect read paths and the
 * synchronous worker Bus. An unknown or non-durable stored type is a hard,
 * descriptive failure rather than a fabricated payload.
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
  assertAggregateScope(workerScopeOf(db), "event", input.aggregateID)
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
  const page = resolveStoredRows(db, input.aggregateID, rows.slice(0, input.limit), undefined)
  const decode = Schema.decodeUnknownSync(input.manifest.schema)
  const events = page.flatMap((row) => {
    const definition = input.manifest.definitions.get(row.type)
    // The query already restricted rows to the manifest's definition keys, so a
    // missing definition is impossible; skip defensively rather than fabricate a
    // type from the raw row.
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
   * Publish an already-formed payload live-only: notify subscribers without a
   * `Definition` lookup and without the durable commit path. Used to replay
   * worker-originated events on the hub, where the durable write already happened
   * in the worker isolate and must never be repeated.
   */
  readonly publishLive: (event: Payload) => Effect.Effect<void>
  /**
   * Wake the `durable({ aggregateID })` subscribers for an aggregate WITHOUT
   * committing or persisting: a writer in another isolate owns the durable
   * commit, and the local durable stream re-reads the aggregate to observe it.
   * Optional only so reduced `EventV2` stubs stay structurally assignable; the
   * production layer always provides it.
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
  /**
   * D16 content-addressed directory owned by this EventV2 layer. Defaults to a
   * sibling of the database file, matching the worker Bus and the hub writer.
   */
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
 * The Bus drives a synchronous connection. Every SQLite statement behind the
 * Effect client is synchronous, so aggregate GC can cross that boundary without
 * an async transaction: the Bus owns the `BEGIN IMMEDIATE` and the refcounted
 * file unlink, and this adapter only makes each statement run to completion.
 */
function busConnection(client: DurableClient): Bus.Connection {
  const all = (sql: string, params: ReadonlyArray<unknown> = []) =>
    Effect.runSync(client.unsafe<Record<string, unknown>>(sql, params))
  const filename = clientFilename(client)
  return {
    ...(filename === undefined ? {} : { filename }),
    all,
    run: (sql, params) => {
      all(sql, params)
      const [row] = all("SELECT changes() AS changes")
      return { changes: Number(row?.changes ?? 0) }
    },
    exec: (sql) => {
      all(sql)
    },
  }
}

/**
 * The D16 directory for the file this client actually opened. `undefined` means
 * "unknown or in-memory"; the Bus then derives from its connection filename and
 * fails loud if a file-backed row cannot be collected, rather than guessing the
 * global database's directory.
 */
function defaultBlobDirectory(client: DurableClient): string | undefined {
  return Bus.blobDirectoryFor(clientFilename(client))
}

function durableClient(db: Database.Interface["db"]): DurableClient | undefined {
  return (db as Database.Interface["db"] & { readonly $client?: DurableClient }).$client
}

function hasEventBlobTable(client: DurableClient): boolean {
  return (
    Effect.runSync(
      client.unsafe<{ readonly name: string }>(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'event_blob'",
      ),
    ).length > 0
  )
}

/**
 * Stage an oversized durable payload through D16's content-addressed file before
 * the write transaction starts. Returns the reference the event row stores; an
 * in-bounds payload stays inline.
 */
function prepareOversizedPayload(
  db: Database.Interface["db"],
  encoded: Record<string, unknown>,
  explicitBlobDir: string | undefined,
): Bus.BlobReference | undefined {
  const payload = JSON.stringify(encoded)
  const size = Buffer.byteLength(payload)
  if (size <= Bus.DEFAULT_BLOB_LIMIT_BYTES) return undefined
  const client = durableClient(db)
  if (client === undefined) {
    throw new InvalidDurableEventError({
      type: "event",
      message: `durable payload is ${size} bytes (limit ${Bus.DEFAULT_BLOB_LIMIT_BYTES}) but this database has no Bus blob path`,
    })
  }
  if (!hasEventBlobTable(client)) {
    throw new InvalidDurableEventError({
      type: "event",
      message: `durable payload is ${size} bytes (limit ${Bus.DEFAULT_BLOB_LIMIT_BYTES}) but this schema has no event_blob table`,
    })
  }
  const blobDir = explicitBlobDir ?? defaultBlobDirectory(client)
  if (blobDir === undefined) {
    throw new InvalidDurableEventError({
      type: "event",
      message: `durable payload is ${size} bytes (limit ${Bus.DEFAULT_BLOB_LIMIT_BYTES}) and no blob directory is configured`,
    })
  }
  const hash = Bus.digest(payload)
  Bus.writeBlobFile(blobDir, hash, payload)
  return { __eventBlob: hash, size }
}

/**
 * Resolve `event.data` blob references for a page of stored rows. The Bus owns
 * the hash/path validation, so EventV2 reads and synchronous Bus reads agree on
 * what a valid D16 reference is.
 */
function resolveStoredRows(
  db: Database.Interface["db"],
  aggregateID: string,
  rows: ReadonlyArray<StoredEventRow>,
  explicitBlobDir: string | undefined,
): ReadonlyArray<StoredEventRow> {
  const referenced = rows.filter((row) => Bus.isBlobReference(row.data))
  if (referenced.length === 0) return rows
  const client = durableClient(db)
  if (client === undefined) {
    throw new InvalidDurableEventError({
      type: "event",
      message: `aggregate ${aggregateID} has blob-backed events but this database has no Bus blob path`,
    })
  }
  const connection = busConnection(client)
  const blobRows = Bus.loadBlobRows(
    connection,
    aggregateID,
    referenced.map((row) => row.seq),
  )
  const blobDir = explicitBlobDir ?? defaultBlobDirectory(client)
  const blob: Bus.BlobStore = {
    ...(blobDir === undefined ? {} : { dir: blobDir }),
    limitBytes: Bus.DEFAULT_BLOB_LIMIT_BYTES,
    supportsTable: true,
  }
  const files = new Map<string, string>()
  return rows.map((row) => {
    if (!Bus.isBlobReference(row.data)) return row
    return {
      ...row,
      data: Bus.resolveBlobData(serializeStoredRow(row), blobRows.get(row.seq), blob, files),
    }
  })
}

/**
 * Delete one aggregate's durable log, blob rows, sequence row, and any
 * content-addressed files left with zero references across every aggregate, in
 * one Bus `BEGIN IMMEDIATE`. The Bus is scoped to this aggregate, so a deletion
 * can never widen to another session.
 */
export const deleteAggregate = (
  db: Database.Interface["db"],
  aggregateID: string,
  options: { readonly blobDir?: string } = {},
): Effect.Effect<number> => {
  const worker = workerScopeOf(db)
  assertAggregateScope(worker, "event", aggregateID)
  const client = (db as Database.Interface["db"] & { readonly $client?: DurableClient }).$client
  if (client === undefined) return removeRows(db, aggregateID)
  const blobDir = options.blobDir ?? defaultBlobDirectory(client)
  if (worker?.sessionID !== undefined && worker.durable === undefined) {
    return Effect.die(
      new InvalidDurableEventError({
        type: "event",
        message: "a bound worker's aggregate deletion requires the worker durable capability",
      }),
    )
  }
  return Effect.sync(() => {
    if (worker?.durable !== undefined) {
      return worker.durable.deleteAggregate(aggregateID, blobDir === undefined ? {} : { blobDir })
    }
    const bus = Bus.create({
      connection: busConnection(client),
      ownerID: "event-v2-delete",
      aggregateID,
      ...(blobDir === undefined ? {} : { blobDir }),
    })
    return bus.deleteAggregate(aggregateID)
  }).pipe(Effect.orDie)
}

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
      const worker = workerScopeOf(db)
      // Typed durable-write methods for the Effect-owned EventV2 transaction.
      // They are the only place this layer writes `event`, `event_sequence`, or
      // `event_blob`; the commit body composes them inside `db.transaction`.
      const writes = {
        sequence: (aggregateID: string) =>
          db
            .select({
              seq: EventSequenceTable.seq,
              syncOwnerID: EventSequenceTable.owner_id,
              workerOwnerID: EventSequenceTable.worker_owner_id,
              workerExpiresAt: EventSequenceTable.worker_expires_at,
            })
            .from(EventSequenceTable)
            .where(eq(EventSequenceTable.aggregate_id, aggregateID))
            .get()
            .pipe(
              Effect.orDie,
              Effect.map((row) =>
                row === undefined
                  ? undefined
                  : {
                      seq: row.seq,
                      syncOwnerID: row.syncOwnerID ?? undefined,
                      workerOwnerID: row.workerOwnerID ?? undefined,
                      workerExpiresAt: row.workerExpiresAt ?? undefined,
                    },
              ),
            ),
        claimLease: (input: { readonly aggregateID: string; readonly ownerID: string; readonly leaseTimeoutMs: number }) =>
          Effect.gen(function* () {
            const current = yield* db
              .select({ seq: EventSequenceTable.seq })
              .from(EventSequenceTable)
              .where(eq(EventSequenceTable.aggregate_id, input.aggregateID))
              .get()
              .pipe(Effect.orDie)
            const now = Date.now()
            const expiresAt = input.leaseTimeoutMs > 0 ? now + input.leaseTimeoutMs : null
            if (current === undefined) {
              yield* db
                .insert(EventSequenceTable)
                .values([
                  {
                    aggregate_id: input.aggregateID,
                    seq: -1,
                    worker_owner_id: input.ownerID,
                    worker_heartbeat_at: now,
                    worker_expires_at: expiresAt,
                  },
                ])
                .run()
                .pipe(Effect.orDie)
              return
            }
            yield* db
              .update(EventSequenceTable)
              .set({
                worker_owner_id: input.ownerID,
                worker_heartbeat_at: now,
                worker_expires_at: expiresAt,
              })
              .where(eq(EventSequenceTable.aggregate_id, input.aggregateID))
              .run()
              .pipe(Effect.orDie)
          }),
        fillSyncOwner: (input: { readonly aggregateID: string; readonly ownerID: string }) =>
          db
            .update(EventSequenceTable)
            .set({ owner_id: input.ownerID })
            .where(and(eq(EventSequenceTable.aggregate_id, input.aggregateID), isNull(EventSequenceTable.owner_id)))
            .run()
            .pipe(Effect.orDie, Effect.asVoid),
        claimSyncOwner: (input: { readonly aggregateID: string; readonly ownerID: string }) =>
          db
            .update(EventSequenceTable)
            .set({ owner_id: input.ownerID })
            .where(eq(EventSequenceTable.aggregate_id, input.aggregateID))
            .run()
            .pipe(Effect.orDie, Effect.asVoid),
        advanceSequence: (input: {
          readonly aggregateID: string
          readonly seq: number
          readonly ownerID?: string
        }) =>
          Effect.gen(function* () {
            const current = yield* db
              .select({ seq: EventSequenceTable.seq, ownerID: EventSequenceTable.owner_id })
              .from(EventSequenceTable)
              .where(eq(EventSequenceTable.aggregate_id, input.aggregateID))
              .get()
              .pipe(Effect.orDie)
            if (current === undefined) {
              yield* db
                .insert(EventSequenceTable)
                .values([
                  {
                    aggregate_id: input.aggregateID,
                    seq: input.seq,
                    owner_id: input.ownerID,
                    worker_owner_id: worker?.leaseOwner,
                    worker_heartbeat_at: worker?.leaseOwner === undefined ? undefined : Date.now(),
                  },
                ])
                .run()
                .pipe(Effect.orDie)
              return
            }
            yield* db
              .update(EventSequenceTable)
              .set({
                seq: input.seq,
                ...(input.ownerID && current.ownerID == null ? { owner_id: input.ownerID } : {}),
              })
              .where(eq(EventSequenceTable.aggregate_id, input.aggregateID))
              .run()
              .pipe(Effect.orDie)
          }),
        appendBlob: (input: {
          readonly aggregateID: string
          readonly seq: number
          readonly type: string
          readonly hash: string
          readonly size: number
        }) =>
          db
            .insert(EventBlobTable)
            .values([
              {
                aggregate_id: input.aggregateID,
                seq: input.seq,
                type: input.type,
                hash: input.hash,
                size: input.size,
                data: null,
                file_path: input.hash,
              },
            ])
            .run()
            .pipe(Effect.orDie, Effect.asVoid),
        appendEvent: (input: {
          readonly id: ID
          readonly aggregateID: string
          readonly seq: number
          readonly type: string
          readonly data: Record<string, unknown>
        }) =>
          db
            .insert(EventTable)
            .values([
              {
                id: input.id,
                aggregate_id: input.aggregateID,
                seq: input.seq,
                type: input.type,
                data: input.data,
              },
            ])
            .run()
            .pipe(Effect.orDie, Effect.asVoid),
      }

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
              const list = projectors.get(event.type) ?? []
              const encoded = Schema.encodeUnknownSync(definition.data)(event.data) as Record<string, unknown>
              // Stage the D16 file before the write lock; the transaction then
              // records the reference and the row atomically.
              const oversized = prepareOversizedPayload(db, encoded, options?.blobDir)
              return yield* Effect.uninterruptible(
                Effect.gen(function* () {
                  const committed = yield* db
                    .transaction(
                      () =>
                        Effect.gen(function* () {
                          const row = yield* writes.sequence(aggregateID)
                          if (worker?.leaseOwner !== undefined) {
                            if (row?.workerOwnerID !== undefined && row.workerOwnerID !== worker.leaseOwner) {
                              yield* Effect.die(
                                new StaleOwnerError({
                                  message: `Aggregate ${aggregateID} worker lease is held by ${row.workerOwnerID}, not ${worker.leaseOwner}`,
                                }),
                              )
                            }
                            // A same-owner lease renews on write even after its TTL
                            // elapsed; the write below re-stamps the expiry. A
                            // superseded generation is fenced by the owner mismatch
                            // above, so an expired same-owner lease is never stolen.
                            yield* writes.claimLease({
                              aggregateID,
                              ownerID: worker.leaseOwner,
                              leaseTimeoutMs: WORKER_LEASE_TTL_MS,
                            })
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
                                : resolveStoredRows(db, aggregateID, [stored], options?.blobDir)[0]?.data
                            if (
                              stored?.id === event.id &&
                              stored.type === versionedType(definition.type, durable.version) &&
                              isDeepStrictEqual(storedData, encoded)
                            ) {
                              if (input.ownerID && row?.syncOwnerID == null) {
                                yield* writes.fillSyncOwner({ aggregateID, ownerID: input.ownerID })
                              }
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
                          const committed = {
                            ...event,
                            durable: { aggregateID, seq, version: durable.version },
                          } as Payload
                          for (const projector of list) {
                            yield* projector(committed)
                          }
                          if (commit) yield* commit(seq)
                          // The typed boundary owns the UPSERT; the surrounding
                          // `BEGIN IMMEDIATE` transaction makes the read-then-write
                          // atomic for this aggregate.
                          yield* writes.advanceSequence({
                            aggregateID,
                            seq,
                            ...(input?.ownerID ? { ownerID: input.ownerID } : {}),
                          })
                          const type = versionedType(definition.type, durable.version)
                          const storedData: Record<string, unknown> =
                            oversized === undefined
                              ? encoded
                              : { __eventBlob: oversized.__eventBlob, size: oversized.size }
                          if (oversized !== undefined) {
                            yield* writes.appendBlob({
                              aggregateID,
                              seq,
                              type,
                              hash: oversized.__eventBlob,
                              size: oversized.size,
                            })
                          }
                          yield* writes.appendEvent({
                            id: event.id,
                            aggregateID,
                            seq,
                            type,
                            data: storedData,
                          })
                          return { aggregateID, seq }
                        }),
                      { behavior: "immediate" },
                    )
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
          // When `isolateListeners` is set, one failing observer is caught and
          // logged by `observe` instead of aborting the publish for the others (or
          // the caller). Live-only republishes take this path so a listener defect
          // never crosses a synchronous port boundary.
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
        return deleteAggregate(db, aggregateID, options?.blobDir === undefined ? {} : { blobDir: options.blobDir })
      }

      function claim(aggregateID: string, ownerID: string) {
        assertAggregateScope(workerScopeOf(db), "event_sequence", aggregateID)
        return writes.claimSyncOwner({ aggregateID, ownerID })
      }

      const subscribe = <D extends Definition>(definition: D): Stream.Stream<Payload<D>> =>
        Stream.unwrap(getOrCreate(definition).pipe(Effect.map((pubsub) => Stream.fromPubSub(pubsub)))).pipe(
          Stream.map((event) => event as Payload<D>),
        )

      const streamAll = (): Stream.Stream<Payload> => Stream.fromPubSub(pubsub.all)

      const readAfter = (aggregateID: string, after: number) => {
        assertAggregateScope(workerScopeOf(db), "event", aggregateID)
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
          Effect.map((rows) => resolveStoredRows(db, aggregateID, rows, options?.blobDir)),
          Effect.flatMap((rows) => Effect.forEach(rows, (row) => decodeSerializedEvent(serializeStoredRow(row)))),
          Effect.orDie,
        )
      }

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
       * Publish the per-aggregate durable wake exactly as `commitDurableEvent`
       * does after a commit — without re-committing or touching the database.
       * Named for the caller's intent: wake the aggregate's `durable()` streams
       * so they re-read and observe an event committed in another isolate.
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

const layer = layerWith()
export const node = makeGlobalNode({ service: Service, layer: layer, deps: [Database.node] })
