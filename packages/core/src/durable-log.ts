export * as DurableLog from "./durable-log"

import { createHash, randomUUID } from "node:crypto"
import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { Context, Effect, Layer, Schema } from "effect"
import type { SqlClient } from "effect/unstable/sql/SqlClient"
import { Event } from "@opencode-ai/schema/event"
import { decodeDurablePayload, type SerializedEvent } from "./event"
import { Database } from "./database/database"
import { inImmediateTransaction, inImmediateTransactionAsync } from "./database/transaction"
import { makeGlobalNode, type GlobalNode } from "./effect/app-node"

/** The synchronous connection a DurableLog writes through. */
export interface Connection {
  all(sql: string, params?: ReadonlyArray<unknown>): ReadonlyArray<Record<string, unknown>>
  run(sql: string, params?: ReadonlyArray<unknown>): { readonly changes: number }
  exec(sql: string): void
  /** The file this connection owns, when the driver exposes it. */
  readonly filename?: string
}

export interface CacheInvalidation {
  invalidate(aggregateID: string): void
}

export interface Options {
  /** Raw connection for the legacy path. */
  readonly connection?: Connection
  /** The typed durable-write boundary. */
  readonly repository?: DurableRepository
  /** Worker-generation lease stamped onto every aggregate this DurableLog writes. */
  readonly ownerID: string
  /** Worker lease TTL in milliseconds. */
  readonly leaseTimeoutMs?: number
  /** Definitions used to decode stored events; defaults to the global durable manifest. */
  readonly definitions?: ReadonlyArray<Event.Definition>
  readonly cache?: CacheInvalidation
  /** The one aggregate this DurableLog may touch (a worker's session). */
  readonly aggregateID?: string
  /**
   * D16 content-addressed directory for payloads above {@link blobLimitBytes}. Absent means an oversized
   * payload is refused rather than silently truncated.
   */
  readonly blobDir?: string
  /** Inline durable payload bound; above it the payload is stored by reference. */
  readonly blobLimitBytes?: number
}

/**
 * D16 schema bound: 4 MiB, the same bound as the worker egress frame cap (`MAX_WORKER_FRAME_BYTES`).
 */
export const DEFAULT_BLOB_LIMIT_BYTES = 4 * 1024 * 1024

/**
 * Space reserved inside one worker frame for the envelope: the port/kind/id fields, the committed
 * `(aggregateID, seq)` metadata, and JSON punctuation.
 */
export const ENVELOPE_HEADROOM_BYTES = 64 * 1024

/**
 * Default DurableLog inline bound: one frame minus {@link ENVELOPE_HEADROOM_BYTES}. A payload at or below
 * this is stored inline AND always fits its frame.
 */
export const DEFAULT_INLINE_PAYLOAD_LIMIT_BYTES = DEFAULT_BLOB_LIMIT_BYTES - ENVELOPE_HEADROOM_BYTES

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
  /** Clock for a live-lease decision; defaults to `Date.now()`. */
  readonly now?: number
  /** Override the dead-generation probe. */
  readonly isDeadOwner?: (ownerID: string) => boolean
}

export interface Transaction {
  readonly connection: Connection
  readonly all: Connection["all"]
  readonly run: Connection["run"]
  readonly publish: (events: PublishEvents) => readonly Event.Payload[]
}

/** One aggregate's durable cursor plus both owners (v1 sync and worker generation). */
export interface SequenceState {
  readonly seq: number
  /** v1 sync owner (`event_sequence.owner_id`). */
  readonly syncOwnerID: string | undefined
  /** Worker-generation lease owner; undefined when this schema predates it. */
  readonly workerOwnerID: string | undefined
  readonly workerHeartbeatAt: number | undefined
  readonly workerExpiresAt: number | undefined
}

export interface AppendSequenceInput {
  readonly aggregateID: string
  /** Reserve this many contiguous positions; returns the block's last sequence. */
  readonly block: number
  /** v1 sync owner, written only when the row has none. */
  readonly syncOwnerID?: string
}

export interface AdvanceSequenceInput {
  readonly aggregateID: string
  /** Set the cursor to exactly this sequence (replay/explicit commit). */
  readonly seq: number
  readonly syncOwnerID?: string
}

export interface AppendEventInput {
  readonly id: Event.ID
  readonly aggregateID: string
  readonly seq: number
  readonly type: string
  readonly data: Record<string, unknown>
}

export interface AppendBlobInput {
  readonly aggregateID: string
  readonly seq: number
  readonly type: string
  readonly hash: string
  readonly size: number
  /** Content hash naming the file; validated before it becomes a path. */
  readonly filePath: string
}

export interface LeaseOwnerInput {
  readonly aggregateID: string
  readonly ownerID: string
  /** Clock for the lease stamps; defaults to `Date.now()`. */
  readonly now?: number
}

export interface ReclaimLeaseInput {
  readonly aggregateID: string
  readonly expectedOwnerID: string
  readonly expectedSeq: number
  readonly nextOwnerID: string
  readonly now?: number
  readonly isDeadOwner?: (ownerID: string) => boolean
}

/** Startup sweep: mark every lease whose owner process is gone reclaimable. */
export interface ReleaseDeadLeasesInput {
  /** Restrict the sweep to one aggregate; absent scans every aggregate. */
  readonly aggregateID?: string
  /** Proves an owner generation is dead. */
  readonly isDeadOwner?: (ownerID: string) => boolean
}

/** An `event_blob` row as the typed boundary exposes it. */
export interface BlobRow {
  readonly seq: number
  readonly hash: string
  readonly filePath: string | null
  /** The pre-D16 inline payload. */
  readonly data?: string | null
}

/** The only durable-log write boundary. */
export interface DurableRepository {
  readonly workerLease: boolean
  readonly blobTable: boolean
  /** The file this repository writes, when the driver exposes it. */
  readonly filename?: string
  readonly sequence: (aggregateID: string) => SequenceState | undefined
  readonly appendSequence: (input: AppendSequenceInput) => number
  readonly advanceSequence: (input: AdvanceSequenceInput) => void
  readonly appendEvent: (input: AppendEventInput) => void
  readonly appendBlob: (input: AppendBlobInput) => void
  readonly claimLease: (input: LeaseOwnerInput & { readonly leaseTimeoutMs: number }) => void
  readonly renewLease: (input: LeaseOwnerInput & { readonly leaseTimeoutMs: number }) => number
  readonly releaseLease: (input: LeaseOwnerInput) => number
  readonly reclaimLease: (input: ReclaimLeaseInput & { readonly leaseTimeoutMs: number }) => number
  /** Mark every lease owned by a provably dead generation reclaimable (`worker_expires_at = 0`). */
  readonly releaseDeadLeases: (input: ReleaseDeadLeasesInput) => number
  /** v1 sync claim: set `owner_id` unconditionally. */
  readonly claimSyncOwner: (input: LeaseOwnerInput) => number
  /** Set `owner_id` only when the row has no sync owner. */
  readonly fillSyncOwner: (input: LeaseOwnerInput) => number
  readonly readEvents: (input: { readonly aggregateID: string; readonly after?: number }) => readonly SerializedEvent[]
  readonly readBlobs: (input: {
    readonly aggregateID: string
    readonly sequences?: readonly number[]
    readonly beforeSeq?: number
  }) => readonly BlobRow[]
  readonly blobReferences: (hash: string) => number
  readonly deleteEvents: (input: { readonly aggregateID: string; readonly beforeSeq?: number }) => number
  readonly deleteBlobs: (input: { readonly aggregateID: string; readonly beforeSeq?: number }) => number
  readonly deleteSequence: (aggregateID: string) => number
  readonly transaction: <T>(work: () => T) => T
  readonly transactionAsync: <T>(work: () => T) => Promise<T>
}

export interface Interface {
  /** Absent when the DurableLog was built from a typed repository alone. */
  readonly connection?: Connection
  readonly ownerID: string
  /** Schema capability: true when the journal has the dedicated `worker_owner_id` lease column. */
  readonly workerLease: boolean
  readonly publish: (events: PublishEvents) => readonly Event.Payload[]
  /** Publish inside a caller-owned `BEGIN IMMEDIATE`; does not commit or invalidate the cache. */
  readonly publishInTransaction: (events: PublishEvents) => readonly Event.Payload[]
  /** Run `work` inside `BEGIN IMMEDIATE` and invalidate touched cache keys after commit. */
  readonly transaction: <T>(work: (tx: Transaction) => T) => T
  /**
   * Async twin of {@link transaction}: the body stays synchronous/atomic, but a busy backoff yields to the
   * event loop.
   */
  readonly transactionAsync: <T>(work: (tx: Transaction) => T) => Promise<T>
  /** Async twin of {@link publish} (worker path). */
  readonly publishAsync: (events: PublishEvents) => Promise<readonly Event.Payload[]>
  readonly project: <D extends Event.Definition>(definition: D, projector: Projector<D>) => void
  readonly latest: (aggregateID: string) => number
  readonly ownerOf: (aggregateID: string) => string | undefined
  readonly read: (input: { readonly aggregateID: string; readonly after?: number }) => readonly Event.Payload[]
  readonly claim: (aggregateID: string, nextOwnerID?: string) => Lease
  readonly reclaim: (input: ReclaimInput) => Lease
  /** Mark dead-generation leases reclaimable; preserves live-generation fencing. */
  readonly releaseDeadLeases: (input?: ReleaseDeadLeasesInput) => number
  /** Renew this generation's lease; a stale owner cannot extend it. */
  readonly heartbeat: (aggregateID: string) => void
  /** Mark this generation's lease dead so a successor can reclaim it. */
  readonly release: (aggregateID: string) => void
  /**
   * Retention/cursor GC: delete durable events and their blob rows below `beforeSeq`, plus any orphaned blob
   * rows whose event is already gone, then unlink only content-addressed files with zero remaining
   * references.
   */
  readonly collectBlobs: (input: { readonly aggregateID: string; readonly beforeSeq: number }) => number
  /**
   * Aggregate-deletion GC: delete one aggregate's whole durable log and blob rows, then unlink only files
   * with zero references across every aggregate.
   */
  readonly deleteAggregate: (aggregateID: string) => number
}

/** The `DurableLog` service exposes the synchronous durable bus through the Effect context. */
export class Service extends Context.Service<Service, Interface>()("@opencode/DurableLog") {}

/** Where oversized payloads live and whether this schema can offload them. */
export interface BlobStore {
  readonly dir?: string
  readonly limitBytes: number
  readonly supportsTable: boolean
}

/** A stored `event.data` that points at an `event_blob` row/file. */
export interface BlobReference {
  readonly __eventBlob: string
  readonly size: number
}

export function isBlobReference(value: unknown): value is BlobReference {
  if (typeof value !== "object" || value === null) return false
  const candidate = value as { readonly __eventBlob?: unknown; readonly size?: unknown }
  return typeof candidate.__eventBlob === "string" && typeof candidate.size === "number"
}

/** D16 content hashes are SHA-256 hex digests; only those may become a filesystem name. */
const BLOB_HASH = /^[0-9a-f]{64}$/

function isBlobHash(value: string): boolean {
  return BLOB_HASH.test(value)
}

function requireBlobHash(value: unknown, source: string): string {
  if (typeof value !== "string" || !isBlobHash(value)) {
    throw new InvalidDurableEventError({
      type: "event_blob",
      message: `${source} is not a 64-character lowercase hex content hash`,
    })
  }
  return value
}

export function digest(payload: string): string {
  return createHash("sha256").update(payload).digest("hex")
}

function blobFile(directory: string, hash: string): string {
  // Never join an untrusted `file_path`: a blob row value becomes a filesystem
  // path, so it must already be a content hash before it reaches `path.join`.
  return path.join(directory, requireBlobHash(hash, "blob file name"))
}

/** The content-addressed file already holds exactly this payload. */
function blobFileMatches(file: string, hash: string, size: number): boolean {
  try {
    const stat = fs.statSync(file)
    return stat.isFile() && stat.size === size && digest(fs.readFileSync(file, "utf8")) === hash
  } catch {
    return false
  }
}

/** Flush one path's inode to stable storage (file data or directory entries). */
function fsyncPath(target: string, flags: "r" | "r+"): void {
  const fd = fs.openSync(target, flags)
  try {
    fs.fsyncSync(fd)
  } finally {
    fs.closeSync(fd)
  }
}

/** Flush a blob file's data through a WRITABLE descriptor. */
function fsyncFile(file: string): void {
  fsyncPath(file, "r+")
}

/** Flush a directory entry after a rename. */
function fsyncDirectory(directory: string): void {
  try {
    fsyncPath(directory, "r")
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code
    if (code === "EINVAL" || code === "ENOTSUP" || code === "EISDIR" || code === "EPERM") return
    throw error
  }
}

/**
 * Store `payload` under its content hash with a temp-file + rename, so a reader never observes a partially
 * written blob.
 */
export function writeBlobFile(directory: string, hash: string, payload: string): void {
  const file = blobFile(directory, hash)
  const size = Buffer.byteLength(payload)
  if (blobFileMatches(file, hash, size)) {
    fsyncFile(file)
    fsyncDirectory(directory)
    return
  }
  fs.mkdirSync(directory, { recursive: true })
  const temporary = path.join(directory, `.${hash}.${randomUUID()}.tmp`)
  try {
    fs.writeFileSync(temporary, payload, { flag: "wx" })
    fsyncFile(temporary)
    fs.renameSync(temporary, file)
    fsyncDirectory(directory)
  } catch (error) {
    try {
      fs.unlinkSync(temporary)
    } catch {
      // The temp file was never created, or the rename already consumed it.
    }
    throw error
  }
}

export class InvalidDurableEventError extends Schema.TaggedErrorClass<InvalidDurableEventError>()(
  "DurableLog.InvalidDurableEvent",
  {
    type: Schema.String,
    message: Schema.String,
  },
) {}

export class StaleOwnerError extends Schema.TaggedErrorClass<StaleOwnerError>()("DurableLog.StaleOwner", {
  message: Schema.String,
}) {}

/**
 * A worker lease was requested against a schema that predates the dedicated `worker_owner_id` column.
 */
export class WorkerLeaseUnsupportedError extends Schema.TaggedErrorClass<WorkerLeaseUnsupportedError>()(
  "DurableLog.WorkerLeaseUnsupported",
  {
    type: Schema.String,
    message: Schema.String,
  },
) {}

export class LeaseConflictError extends Schema.TaggedErrorClass<LeaseConflictError>()("DurableLog.LeaseConflict", {
  message: Schema.String,
}) {}

export class CrossAggregateError extends Schema.TaggedErrorClass<CrossAggregateError>()("DurableLog.CrossAggregate", {
  aggregateID: Schema.String,
  scopedAggregateID: Schema.String,
}) {
  override get message() {
    return `DurableLog is scoped to aggregate ${this.scopedAggregateID}; refused ${this.aggregateID}`
  }
}

export class UnknownAggregateError extends Schema.TaggedErrorClass<UnknownAggregateError>()("DurableLog.UnknownAggregate", {
  message: Schema.String,
}) {}

export class StaleStateVersionError extends Schema.TaggedErrorClass<StaleStateVersionError>()("DurableLog.StaleStateVersion", {
  expected: Schema.Int,
  actual: Schema.Int,
}) {}

/**
 * Reserve `block` contiguous sequences for one aggregate with a single upsert-returning statement.
 */
const RESERVE_BLOCK = `
  INSERT INTO event_sequence (aggregate_id, seq, owner_id)
  VALUES (?, ?, ?)
  ON CONFLICT(aggregate_id) DO UPDATE SET
    seq = event_sequence.seq + excluded.seq + 1,
    owner_id = COALESCE(event_sequence.owner_id, excluded.owner_id)
  RETURNING seq
`

/** With a separate worker lease the sync `owner_id` is never part of a write. */
const RESERVE_BLOCK_WITH_WORKER_LEASE = `
  INSERT INTO event_sequence (aggregate_id, seq)
  VALUES (?, ?)
  ON CONFLICT(aggregate_id) DO UPDATE SET
    seq = event_sequence.seq + excluded.seq + 1
  RETURNING seq
`

const SELECT_SEQUENCE_WITH_WORKER_LEASE =
  "SELECT seq, owner_id, worker_owner_id, worker_heartbeat_at, worker_expires_at FROM event_sequence WHERE aggregate_id = ?"

const SELECT_SEQUENCE_LEGACY = "SELECT seq, owner_id FROM event_sequence WHERE aggregate_id = ?"

const ADVANCE_SEQUENCE = `
  INSERT INTO event_sequence (aggregate_id, seq, owner_id)
  VALUES (?, ?, ?)
  ON CONFLICT(aggregate_id) DO UPDATE SET
    seq = excluded.seq,
    owner_id = COALESCE(event_sequence.owner_id, excluded.owner_id)
`

const INSERT_EVENT = "INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?, ?, ?, ?, ?)"

const INSERT_BLOB =
  "INSERT INTO event_blob (aggregate_id, seq, type, hash, size, data, file_path) VALUES (?, ?, ?, ?, ?, NULL, ?)"

// A released lease is `worker_expires_at = 0` (whether `releaseLease` cleared the owner or
// `releaseDeadLeases` kept it).
const RENEW_LEASE = `
  UPDATE event_sequence SET worker_owner_id = ?, worker_heartbeat_at = ?, worker_expires_at = ?
  WHERE aggregate_id = ?
    AND (worker_owner_id = ? OR (worker_owner_id IS NULL AND worker_expires_at IS NULL))
    AND (worker_expires_at IS NULL OR worker_expires_at <> 0)
`

const FILL_SYNC_OWNER = "UPDATE event_sequence SET owner_id = ? WHERE aggregate_id = ? AND owner_id IS NULL"

const CLAIM_SYNC_OWNER = "UPDATE event_sequence SET owner_id = ? WHERE aggregate_id = ?"

type SequenceRow = SequenceState

/** A schema with the dedicated worker-lease columns (post `add_worker_lease`). */
const supportsWorkerLease = (connection: Connection): boolean => {
  const columns = connection.all("PRAGMA table_info(event_sequence)")
  return columns.some((column) => column.name === "worker_owner_id")
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
    syncOwnerID: typeof row.owner_id === "string" ? row.owner_id : undefined,
    workerOwnerID: typeof row.worker_owner_id === "string" ? row.worker_owner_id : undefined,
    workerHeartbeatAt: typeof row.worker_heartbeat_at === "number" ? row.worker_heartbeat_at : undefined,
    workerExpiresAt: typeof row.worker_expires_at === "number" ? row.worker_expires_at : undefined,
  }
}

function readHostScope(): string {
  const attempt = (probe: () => string): string => {
    try {
      return probe().trim()
    } catch {
      return ""
    }
  }
  const identity = [
    os.hostname(),
    attempt(() => fs.readlinkSync("/proc/self/ns/pid")),
    attempt(() => fs.readFileSync("/proc/sys/kernel/random/boot_id", "utf8")),
  ].join("|")
  return createHash("sha256").update(identity).digest("hex").slice(0, 8)
}

/** Identifies the host, boot, and PID namespace this process runs in. */
export const HOST_SCOPE = readHostScope()

/** `worker-<id>-<pid>-<hostScope>-<uuid>` is the worker generation identity. */
export function workerOwnerID(workerID: string | number, pid: number = process.pid): string {
  return `worker-${workerID}-${pid}-${HOST_SCOPE}-${randomUUID()}`
}

/**
 * A lease is "provably dead" only when its owner was minted in this process's host scope and that PID no
 * longer exists.
 */
export function isDeadWorkerOwner(ownerID: string): boolean {
  const parts = ownerID.split("-")
  if (parts[0] !== "worker" || parts.length < 8) return false
  if (parts[3] !== HOST_SCOPE) return false
  const pid = Number(parts[2])
  if (!Number.isSafeInteger(pid) || pid <= 0) return false
  try {
    process.kill(pid, 0)
    return false
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === "ESRCH"
  }
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

const parseBlobRow = (row: Record<string, unknown>): BlobRow => {
  const seq = Number(row.seq)
  if (!Number.isInteger(seq) || typeof row.hash !== "string") {
    throw new Error("stored event_blob row is malformed")
  }
  const data = typeof row.data === "string" ? row.data : undefined
  return {
    seq,
    hash: row.hash,
    filePath: typeof row.file_path === "string" ? row.file_path : null,
    ...(data === undefined ? {} : { data }),
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

export const assertWritableOwner = (
  current: SequenceRow | undefined,
  ownerID: string,
  aggregateID: string,
  workerLease: boolean,
): void => {
  if (!workerLease) {
    if (current?.syncOwnerID !== undefined && current.syncOwnerID !== ownerID) {
      throw new StaleOwnerError({
        message: `Aggregate ${aggregateID} is leased to ${current.syncOwnerID}, not ${ownerID}`,
      })
    }
    return
  }
  // A released lease has `worker_expires_at = 0` (and `releaseLease` clears the owner).
  if (current?.workerExpiresAt === 0) {
    throw new StaleOwnerError({
      message: `Aggregate ${aggregateID} worker lease has been released`,
    })
  }
  if (current?.workerOwnerID === undefined) return
  if (current.workerOwnerID !== ownerID) {
    throw new StaleOwnerError({
      message: `Aggregate ${aggregateID} worker lease is held by ${current.workerOwnerID}, not ${ownerID}`,
    })
  }
  // A same-owner lease is renewable even after its TTL elapsed: the generation is still the lease owner, so
  // the write below re-stamps `worker_expires_at`.
}

interface PreparedBatch {
  readonly aggregateID: string
  readonly events: readonly PreparedEvent[]
}

/** Validate and encode one publish batch before any transaction or file work. */
const prepareBatch = (inputs: PublishEvents, scopeAggregateID: string | undefined): PreparedBatch => {
  const events = inputs.map(prepareEvent)
  const aggregateID = requireSingleAggregate(events)
  requireUniqueIds(events)
  if (scopeAggregateID !== undefined && aggregateID !== scopeAggregateID) {
    throw new CrossAggregateError({ aggregateID, scopedAggregateID: scopeAggregateID })
  }
  return { aggregateID, events }
}

/** Write oversized payload files before `BEGIN IMMEDIATE`. */
const stageBlobPayloads = (batch: PreparedBatch, blob: BlobStore): void => {
  for (const event of batch.events) {
    const encoded = JSON.stringify(event.encoded)
    const size = Buffer.byteLength(encoded)
    if (size <= blob.limitBytes) continue
    if (!blob.supportsTable) {
      throw new InvalidDurableEventError({
        type: event.type,
        message: `durable payload is ${size} bytes (limit ${blob.limitBytes}) but this schema has no event_blob table`,
      })
    }
    if (blob.dir === undefined) {
      throw new InvalidDurableEventError({
        type: event.type,
        message: `durable payload is ${size} bytes (limit ${blob.limitBytes}) and no blob directory is configured`,
      })
    }
    writeBlobFile(blob.dir, digest(encoded), encoded)
  }
}

/** Re-assert a staged blob file inside the publisher's write transaction. */
export const ensureStagedBlob = (blob: BlobStore, encoded: string): void => {
  if (blob.dir === undefined) return
  const hash = digest(encoded)
  const file = blobFile(blob.dir, hash)
  try {
    const stat = fs.statSync(file)
    if (stat.isFile() && stat.size === Buffer.byteLength(encoded)) return
  } catch {
    // Missing after a concurrent GC: rewrite below.
  }
  writeBlobFile(blob.dir, hash, encoded)
}

/** One durable row about to be persisted at an already-reserved sequence. */
export interface PreparedStoredEvent {
  readonly aggregateID: string
  readonly seq: number
  readonly id: Event.ID
  readonly type: string
  readonly encoded: Record<string, unknown>
}

/** The effectful row sink shared by EventV2 and the synchronous DurableLog. */
export interface PreparedEventWriter {
  readonly appendEvent: (input: AppendEventInput) => Effect.Effect<void>
  readonly appendBlob: (input: AppendBlobInput) => Effect.Effect<void>
}

/** The effectful lease sink shared by EventV2 and the synchronous DurableLog. */
export interface LeaseWriter {
  readonly workerLease: boolean
  readonly renewLease: (input: LeaseOwnerInput & { readonly leaseTimeoutMs: number }) => Effect.Effect<number>
}

/** The async twin of {@link DurableRepository} for EventV2's authoritative transaction. */
export interface EffectDurableRepository extends PreparedEventWriter, LeaseWriter {
  readonly sequence: (aggregateID: string) => Effect.Effect<SequenceState | undefined>
  readonly appendSequence: (input: AppendSequenceInput) => Effect.Effect<number>
  readonly advanceSequence: (input: AdvanceSequenceInput) => Effect.Effect<void>
  readonly fillSyncOwner: (input: LeaseOwnerInput) => Effect.Effect<void>
  readonly claimSyncOwner: (input: LeaseOwnerInput) => Effect.Effect<void>
}

/**
 * The one blob/event row writer shared by the DurableLog batch path and EventV2's Effect-owned transaction.
 */
export function writePreparedEvent(
  writer: PreparedEventWriter,
  blob: BlobStore,
  event: PreparedStoredEvent,
): Effect.Effect<void> {
  const encoded = JSON.stringify(event.encoded)
  const size = Buffer.byteLength(encoded)
  if (size <= blob.limitBytes) {
    return writer.appendEvent({
      id: event.id,
      aggregateID: event.aggregateID,
      seq: event.seq,
      type: event.type,
      data: event.encoded,
    })
  }
  return Effect.suspend(() => {
    ensureStagedBlob(blob, encoded)
    const hash = digest(encoded)
    const data = { __eventBlob: hash, size } satisfies BlobReference
    return writer
      .appendBlob({
        aggregateID: event.aggregateID,
        seq: event.seq,
        type: event.type,
        hash,
        size,
        filePath: hash,
      })
      .pipe(
        Effect.andThen(
          writer.appendEvent({
            id: event.id,
            aggregateID: event.aggregateID,
            seq: event.seq,
            type: event.type,
            data,
          }),
        ),
      )
  })
}

/** Renew a worker generation's lease with the one conditional update both publish paths use. */
export function renewWorkerLease(
  writer: LeaseWriter,
  input: { readonly aggregateID: string; readonly ownerID: string; readonly leaseTimeoutMs: number },
): Effect.Effect<void> {
  if (!writer.workerLease) return Effect.void
  return Effect.flatMap(writer.renewLease(input), (renewed) =>
    renewed === 1
      ? Effect.void
      : Effect.die(
          new StaleOwnerError({
            message: `Aggregate ${input.aggregateID} worker lease is not held by ${input.ownerID}`,
          }),
        ),
  )
}

/** Adapt the synchronous repository to the effectful sink the shared writer uses. */
const syncWriter = (repository: DurableRepository): EffectDurableRepository => ({
  workerLease: repository.workerLease,
  appendEvent: (input) => Effect.sync(() => repository.appendEvent(input)),
  appendBlob: (input) => Effect.sync(() => repository.appendBlob(input)),
  renewLease: (input) => Effect.sync(() => repository.renewLease(input)),
  sequence: (aggregateID) => Effect.sync(() => repository.sequence(aggregateID)),
  appendSequence: (input) => Effect.sync(() => repository.appendSequence(input)),
  advanceSequence: (input) => Effect.sync(() => repository.advanceSequence(input)),
  fillSyncOwner: (input) => Effect.sync(() => repository.fillSyncOwner(input)),
  claimSyncOwner: (input) => Effect.sync(() => repository.claimSyncOwner(input)),
})

const writePrepared = (
  repository: DurableRepository,
  ownerID: string,
  projectors: ReadonlyMap<string, ReadonlyArray<Projector>>,
  batch: PreparedBatch,
  touched: Set<string> | undefined,
  workerLease: boolean,
  leaseTimeoutMs: number,
  blob: BlobStore,
): readonly Event.Payload[] => {
  const writer = syncWriter(repository)
  const { aggregateID, events: prepared } = batch
  const current = repository.sequence(aggregateID)
  assertWritableOwner(current, ownerID, aggregateID, workerLease)

  const block = prepared.length
  const lastSeq = repository.appendSequence({
    aggregateID,
    block,
    ...(workerLease ? {} : { syncOwnerID: ownerID }),
  })
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
    const seq = firstSeq + index
    const type = Event.versionedType(event.type, event.version)
    // The sync writer's effects are pure `Effect.sync`; running them keeps the
    // synchronous `publish` API while using the one shared row writer.
    Effect.runSync(
      writePreparedEvent(writer, blob, {
        aggregateID,
        seq,
        id: event.id,
        type,
        encoded: event.encoded,
      }),
    )
  }

  if (workerLease) {
    Effect.runSync(renewWorkerLease(writer, { aggregateID, ownerID, leaseTimeoutMs }))
  }

  touched?.add(aggregateID)
  return payloads
}

/** Publish inside an already-open transaction. */
const writeEvents = (
  repository: DurableRepository,
  ownerID: string,
  projectors: ReadonlyMap<string, ReadonlyArray<Projector>>,
  inputs: PublishEvents,
  touched: Set<string> | undefined,
  workerLease: boolean,
  leaseTimeoutMs: number,
  scopeAggregateID: string | undefined,
  blob: BlobStore,
): readonly Event.Payload[] => {
  const batch = prepareBatch(inputs, scopeAggregateID)
  stageBlobPayloads(batch, blob)
  return writePrepared(repository, ownerID, projectors, batch, touched, workerLease, leaseTimeoutMs, blob)
}

// The retry boundary is the whole `BEGIN IMMEDIATE … COMMIT` unit, so a busy statement can never leave a
// half-open transaction.

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

/** The typed durable-write boundary over one synchronous connection. */
export function createDurableRepository(connection: Connection): DurableRepository {
  const workerLease = supportsWorkerLease(connection)
  const blobTable =
    connection.all("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'event_blob'").length > 0

  const sequence = (aggregateID: string): SequenceState | undefined => {
    const row = connection.all(workerLease ? SELECT_SEQUENCE_WITH_WORKER_LEASE : SELECT_SEQUENCE_LEGACY, [
      aggregateID,
    ])[0]
    return row === undefined ? undefined : parseSequence(row)
  }

  const appendSequence = (input: AppendSequenceInput): number => {
    const reservation = workerLease
      ? connection.all(RESERVE_BLOCK_WITH_WORKER_LEASE, [input.aggregateID, input.block - 1])[0]
      : connection.all(RESERVE_BLOCK, [input.aggregateID, input.block - 1, input.syncOwnerID ?? null])[0]
    return requireSequence(reservation)
  }

  const advanceSequence = (input: AdvanceSequenceInput): void => {
    connection.run(ADVANCE_SEQUENCE, [input.aggregateID, input.seq, input.syncOwnerID ?? null])
  }

  const appendEvent = (input: AppendEventInput): void => {
    connection.run(INSERT_EVENT, [input.id, input.aggregateID, input.seq, input.type, JSON.stringify(input.data)])
  }

  const appendBlob = (input: AppendBlobInput): void => {
    connection.run(INSERT_BLOB, [input.aggregateID, input.seq, input.type, input.hash, input.size, input.filePath])
  }

  const claimLease = (input: LeaseOwnerInput & { readonly leaseTimeoutMs: number }): void => {
    if (!workerLease) {
      throw new WorkerLeaseUnsupportedError({
        type: "event_sequence",
        message: `Aggregate ${input.aggregateID} requires a worker lease but this schema has no worker_owner_id column`,
      })
    }
    const current = sequence(input.aggregateID)
    const now = input.now ?? Date.now()
    const expiresAt = input.leaseTimeoutMs > 0 ? now + input.leaseTimeoutMs : null
    if (current?.workerOwnerID !== undefined && current.workerOwnerID !== input.ownerID) {
      throw new StaleOwnerError({
        message: `Aggregate ${input.aggregateID} worker lease is held by ${current.workerOwnerID}, not ${input.ownerID}`,
      })
    }
    if (current === undefined) {
      connection.run(
        "INSERT INTO event_sequence (aggregate_id, seq, worker_owner_id, worker_heartbeat_at, worker_expires_at) VALUES (?, -1, ?, ?, ?)",
        [input.aggregateID, input.ownerID, now, expiresAt],
      )
      return
    }
    connection.run(
      "UPDATE event_sequence SET worker_owner_id = ?, worker_heartbeat_at = ?, worker_expires_at = ? WHERE aggregate_id = ?",
      [input.ownerID, now, expiresAt, input.aggregateID],
    )
  }

  const renewLease = (input: LeaseOwnerInput & { readonly leaseTimeoutMs: number }): number => {
    if (!workerLease) return 0
    const now = input.now ?? Date.now()
    const expiresAt = input.leaseTimeoutMs > 0 ? now + input.leaseTimeoutMs : null
    return connection.run(RENEW_LEASE, [input.ownerID, now, expiresAt, input.aggregateID, input.ownerID]).changes
  }

  const releaseLease = (input: LeaseOwnerInput): number => {
    if (!workerLease) return 0
    // Clearing the owner is the fence: a retired-but-alive generation can no longer match its own lease on
    // the next write, and the successor's claim sees an unowned row instead of a live PID.
    return connection.run(
      "UPDATE event_sequence SET worker_owner_id = NULL, worker_expires_at = 0 WHERE aggregate_id = ? AND worker_owner_id = ?",
      [input.aggregateID, input.ownerID],
    ).changes
  }

  const reclaimLease = (input: ReclaimLeaseInput & { readonly leaseTimeoutMs: number }): number => {
    // Fail before the row lookup/UPDATE: a schema without the worker lease column can never host a worker
    // generation, and `owner_id` is v1 sync state that a reclaim must not overwrite.
    if (!workerLease) {
      throw new WorkerLeaseUnsupportedError({
        type: "event_sequence",
        message: `Aggregate ${input.aggregateID} requires a worker lease but this schema has no worker_owner_id column`,
      })
    }
    const current = sequence(input.aggregateID)
    if (current === undefined) {
      throw new UnknownAggregateError({ message: `Aggregate ${input.aggregateID} has no lease row` })
    }
    const now = input.now ?? Date.now()
    // A released lease has no owner.
    const released = current.workerOwnerID === undefined
    if (!released && current.workerOwnerID !== input.expectedOwnerID) {
      throw new StaleOwnerError({
        message: `Aggregate ${input.aggregateID} worker lease is held by ${current.workerOwnerID}, not ${input.expectedOwnerID}`,
      })
    }
    if (current.seq !== input.expectedSeq) {
      throw new LeaseConflictError({
        message: `Aggregate ${input.aggregateID} advanced to sequence ${current.seq}; reclaim expected ${input.expectedSeq}`,
      })
    }
    if (current.workerExpiresAt !== undefined && current.workerExpiresAt > now) {
      const ownerIsDead = !released && (input.isDeadOwner ?? isDeadWorkerOwner)(current.workerOwnerID!)
      if (!ownerIsDead) {
        throw new LeaseConflictError({
          message: `Worker lease for aggregate ${input.aggregateID} is still live until ${current.workerExpiresAt}; refusing reclaim from a live generation`,
        })
      }
    }
    const expiresAt = input.leaseTimeoutMs > 0 ? now + input.leaseTimeoutMs : null
    const changes = connection.run(
      `UPDATE event_sequence SET worker_owner_id = ?, worker_heartbeat_at = ?, worker_expires_at = ?
       WHERE aggregate_id = ? AND (worker_owner_id = ? OR worker_owner_id IS NULL) AND seq = ?`,
      [input.nextOwnerID, now, expiresAt, input.aggregateID, input.expectedOwnerID, input.expectedSeq],
    ).changes
    if (changes !== 1) {
      throw new LeaseConflictError({
        message: `Reclaim of ${input.aggregateID} did not match the observed worker lease`,
      })
    }
    return changes
  }

  const releaseDeadLeases = (input: ReleaseDeadLeasesInput): number => {
    if (!workerLease) return 0
    const isDeadOwner = input.isDeadOwner ?? isDeadWorkerOwner
    const scope = input.aggregateID
    const rows =
      scope === undefined
        ? connection.all("SELECT aggregate_id, worker_owner_id FROM event_sequence WHERE worker_owner_id IS NOT NULL")
        : connection.all(
            "SELECT aggregate_id, worker_owner_id FROM event_sequence WHERE aggregate_id = ? AND worker_owner_id IS NOT NULL",
            [scope],
          )
    let changes = 0
    for (const row of rows) {
      const aggregateID = typeof row.aggregate_id === "string" ? row.aggregate_id : undefined
      const ownerID = typeof row.worker_owner_id === "string" ? row.worker_owner_id : undefined
      if (aggregateID === undefined || ownerID === undefined) continue
      if (!isDeadOwner(ownerID)) continue
      changes += connection.run(
        "UPDATE event_sequence SET worker_expires_at = 0 WHERE aggregate_id = ? AND worker_owner_id = ?",
        [aggregateID, ownerID],
      ).changes
    }
    return changes
  }

  const claimSyncOwner = (input: LeaseOwnerInput): number =>
    connection.run(CLAIM_SYNC_OWNER, [input.ownerID, input.aggregateID]).changes

  const fillSyncOwner = (input: LeaseOwnerInput): number =>
    connection.run(FILL_SYNC_OWNER, [input.ownerID, input.aggregateID]).changes

  const readEvents = (input: { readonly aggregateID: string; readonly after?: number }): readonly SerializedEvent[] =>
    connection
      .all("SELECT id, aggregate_id, seq, type, data FROM event WHERE aggregate_id = ? AND seq > ? ORDER BY seq ASC", [
        input.aggregateID,
        input.after ?? -1,
      ])
      .map(parseStoredEvent)

  const readBlobs = (input: {
    readonly aggregateID: string
    readonly sequences?: readonly number[]
    readonly beforeSeq?: number
  }): readonly BlobRow[] => {
    if (input.sequences !== undefined) {
      const rows: BlobRow[] = []
      for (let offset = 0; offset < input.sequences.length; offset += BLOB_QUERY_CHUNK) {
        const chunk = input.sequences.slice(offset, offset + BLOB_QUERY_CHUNK)
        const placeholders = chunk.map(() => "?").join(", ")
        for (const row of connection.all(
          `SELECT seq, hash, data, file_path FROM event_blob WHERE aggregate_id = ? AND seq IN (${placeholders})`,
          [input.aggregateID, ...chunk],
        )) {
          rows.push(parseBlobRow(row))
        }
      }
      return rows
    }
    if (input.beforeSeq !== undefined) {
      return connection
        .all(COLLECTABLE_BLOBS, [input.aggregateID, input.beforeSeq, input.aggregateID])
        .map(parseBlobRow)
    }
    return connection
      .all("SELECT seq, hash, file_path FROM event_blob WHERE aggregate_id = ?", [input.aggregateID])
      .map(parseBlobRow)
  }

  const blobReferences = (hash: string): number =>
    Number(connection.all("SELECT COUNT(*) AS count FROM event_blob WHERE hash = ?", [hash])[0]?.count ?? 0)

  const deleteEvents = (input: { readonly aggregateID: string; readonly beforeSeq?: number }): number =>
    input.beforeSeq === undefined
      ? connection.run("DELETE FROM event WHERE aggregate_id = ?", [input.aggregateID]).changes
      : connection.run("DELETE FROM event WHERE aggregate_id = ? AND seq < ?", [input.aggregateID, input.beforeSeq])
          .changes

  const deleteBlobs = (input: { readonly aggregateID: string; readonly beforeSeq?: number }): number =>
    input.beforeSeq === undefined
      ? connection.run("DELETE FROM event_blob WHERE aggregate_id = ?", [input.aggregateID]).changes
      : connection.run(DELETE_COLLECTABLE_BLOBS, [input.aggregateID, input.beforeSeq, input.aggregateID]).changes

  const deleteSequence = (aggregateID: string): number =>
    connection.run("DELETE FROM event_sequence WHERE aggregate_id = ?", [aggregateID]).changes

  return {
    workerLease,
    blobTable,
    ...(connection.filename === undefined ? {} : { filename: connection.filename }),
    sequence,
    appendSequence,
    advanceSequence,
    appendEvent,
    appendBlob,
    claimLease,
    renewLease,
    releaseLease,
    reclaimLease,
    releaseDeadLeases,
    claimSyncOwner,
    fillSyncOwner,
    readEvents,
    readBlobs,
    blobReferences,
    deleteEvents,
    deleteBlobs,
    deleteSequence,
    transaction: <T>(work: () => T) => inImmediateTransaction(connection, work),
    transactionAsync: <T>(work: () => T) => inImmediateTransactionAsync(connection, work),
  }
}

/**
 * The async twin of {@link createDurableRepository}: the same statements, issued through the Effect SQL
 * client EventV2's transaction owns.
 */
export function createEffectDurableRepository(
  client: SqlClient,
  workerLease: boolean,
): EffectDurableRepository {
  const rows = <A extends object>(sql: string, params: ReadonlyArray<unknown> = []) =>
    client.unsafe<A>(sql, params).pipe(Effect.orDie)
  const execute = (sql: string, params: ReadonlyArray<unknown> = []) =>
    rows<Record<string, unknown>>(sql, params).pipe(Effect.asVoid)

  return {
    workerLease,
    sequence: (aggregateID) =>
      rows<Record<string, unknown>>(workerLease ? SELECT_SEQUENCE_WITH_WORKER_LEASE : SELECT_SEQUENCE_LEGACY, [
        aggregateID,
      ]).pipe(Effect.map((result) => (result[0] === undefined ? undefined : parseSequence(result[0])))),
    appendSequence: (input) =>
      rows<Record<string, unknown>>(
        workerLease ? RESERVE_BLOCK_WITH_WORKER_LEASE : RESERVE_BLOCK,
        workerLease
          ? [input.aggregateID, input.block - 1]
          : [input.aggregateID, input.block - 1, input.syncOwnerID ?? null],
      ).pipe(Effect.map((reserved) => requireSequence(reserved[0]))),
    advanceSequence: (input) =>
      execute(ADVANCE_SEQUENCE, [input.aggregateID, input.seq, input.syncOwnerID ?? null]),
    appendEvent: (input) =>
      execute(INSERT_EVENT, [input.id, input.aggregateID, input.seq, input.type, JSON.stringify(input.data)]),
    appendBlob: (input) =>
      execute(INSERT_BLOB, [input.aggregateID, input.seq, input.type, input.hash, input.size, input.filePath]),
    renewLease: (input) => {
      if (!workerLease) return Effect.succeed(0)
      const now = input.now ?? Date.now()
      const expiresAt = input.leaseTimeoutMs > 0 ? now + input.leaseTimeoutMs : null
      return rows<Record<string, unknown>>(`${RENEW_LEASE} RETURNING aggregate_id`, [
        input.ownerID,
        now,
        expiresAt,
        input.aggregateID,
        input.ownerID,
      ]).pipe(Effect.map((updated) => updated.length))
    },
    fillSyncOwner: (input) => execute(FILL_SYNC_OWNER, [input.ownerID, input.aggregateID]),
    claimSyncOwner: (input) => execute(CLAIM_SYNC_OWNER, [input.ownerID, input.aggregateID]),
  }
}

export function create(options: Options): Interface {
  const { ownerID, cache } = options
  if (ownerID.length === 0) {
    throw new Error("DurableLog.create requires a non-empty ownerID")
  }
  const repository =
    options.repository ?? (options.connection ? createDurableRepository(options.connection) : undefined)
  if (repository === undefined) {
    throw new Error("DurableLog.create requires a connection or a DurableRepository")
  }
  const connection = options.connection
  const leaseTimeoutMs = options.leaseTimeoutMs ?? 0
  const scopeAggregateID = options.aggregateID
  // Older callers/tests create the event table without D16's blob table.
  const hasBlobTable = repository.blobTable
  // Without a file the DurableLog cannot prove which directory its rows belong to, so
  // it leaves `blobDir` unset and file-backed collection fails loud.
  const filename = repository.filename
  const derivedBlobDir = blobDirectoryFor(filename)
  const blobDir = options.blobDir ?? derivedBlobDir
  const blob: BlobStore = {
    ...(blobDir === undefined ? {} : { dir: blobDir }),
    limitBytes: options.blobLimitBytes ?? DEFAULT_INLINE_PAYLOAD_LIMIT_BYTES,
    supportsTable: hasBlobTable,
  }
  const assertAggregate = (aggregateID: string): void => {
    if (scopeAggregateID !== undefined && aggregateID !== scopeAggregateID) {
      throw new CrossAggregateError({ aggregateID, scopedAggregateID: scopeAggregateID })
    }
  }
  const workerLease = repository.workerLease
  const projectors = new Map<string, Projector[]>()
  const definitions = options.definitions === undefined ? undefined : Event.durable(options.definitions)

  const rawConnection = (): Connection => {
    if (connection === undefined) {
      throw new Error("DurableLog.transaction requires a raw connection; this DurableLog was built from a typed repository")
    }
    return connection
  }

  const publishInTransaction = (events: PublishEvents): readonly Event.Payload[] =>
    writeEvents(repository, ownerID, projectors, events, undefined, workerLease, leaseTimeoutMs, scopeAggregateID, blob)

  const transaction = <T>(work: (tx: Transaction) => T): T => {
    const target = rawConnection()
    const touched = new Set<string>()
    const tx: Transaction = {
      connection: target,
      all: target.all.bind(target),
      run: target.run.bind(target),
      publish: (events) =>
        writeEvents(
          repository,
          ownerID,
          projectors,
          events,
          touched,
          workerLease,
          leaseTimeoutMs,
          scopeAggregateID,
          blob,
        ),
    }
    const result = repository.transaction(() => work(tx))
    for (const aggregateID of touched) cache?.invalidate(aggregateID)
    return result
  }

  const transactionAsync = async <T>(work: (tx: Transaction) => T): Promise<T> => {
    const target = rawConnection()
    const touched = new Set<string>()
    const tx: Transaction = {
      connection: target,
      all: target.all.bind(target),
      run: target.run.bind(target),
      publish: (events) =>
        writeEvents(
          repository,
          ownerID,
          projectors,
          events,
          touched,
          workerLease,
          leaseTimeoutMs,
          scopeAggregateID,
          blob,
        ),
    }
    const result = await repository.transactionAsync(() => work(tx))
    for (const aggregateID of touched) cache?.invalidate(aggregateID)
    return result
  }

  // Top-level publish stages blob files before `BEGIN IMMEDIATE`, so no sync
  // filesystem write runs while the write lock is held.
  const publish = (events: PublishEvents): readonly Event.Payload[] => {
    const batch = prepareBatch(events, scopeAggregateID)
    stageBlobPayloads(batch, blob)
    const touched = new Set<string>()
    const result = repository.transaction(() =>
      writePrepared(repository, ownerID, projectors, batch, touched, workerLease, leaseTimeoutMs, blob),
    )
    for (const aggregateID of touched) cache?.invalidate(aggregateID)
    return result
  }

  const publishAsync = async (events: PublishEvents): Promise<readonly Event.Payload[]> => {
    const batch = prepareBatch(events, scopeAggregateID)
    stageBlobPayloads(batch, blob)
    const touched = new Set<string>()
    const result = await repository.transactionAsync(() =>
      writePrepared(repository, ownerID, projectors, batch, touched, workerLease, leaseTimeoutMs, blob),
    )
    for (const aggregateID of touched) cache?.invalidate(aggregateID)
    return result
  }

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

  const latest = (aggregateID: string): number => {
    assertAggregate(aggregateID)
    return repository.sequence(aggregateID)?.seq ?? -1
  }

  // Reads only.
  const ownerOf = (aggregateID: string): string | undefined => {
    assertAggregate(aggregateID)
    const current = repository.sequence(aggregateID)
    return workerLease ? current?.workerOwnerID : current?.syncOwnerID
  }

  const read = (input: { readonly aggregateID: string; readonly after?: number }): readonly Event.Payload[] => {
    assertAggregate(input.aggregateID)
    const events = repository.readEvents({ aggregateID: input.aggregateID, after: input.after })
    if (events.length === 0) return []
    // One blob query per page (chunked for SQLite's parameter bound) instead of
    // one query per event.
    const references = events.filter((event) => isBlobReference(event.data)).map((event) => event.seq)
    const blobRows = new Map<number, Record<string, unknown>>()
    for (const row of repository.readBlobs({ aggregateID: input.aggregateID, sequences: references })) {
      blobRows.set(row.seq, {
        seq: row.seq,
        hash: row.hash,
        file_path: row.filePath,
        ...(row.data === undefined ? {} : { data: row.data }),
      })
    }
    const files = new Map<string, string>()
    return events.map((event) => {
      const data = isBlobReference(event.data)
        ? resolveBlobData(event, blobRows.get(event.seq), blob, files)
        : event.data
      return decodeDurablePayload({ ...event, data }, definitions)
    })
  }

  const collectBlobs = (input: { readonly aggregateID: string; readonly beforeSeq: number }): number => {
    assertAggregate(input.aggregateID)
    if (!hasBlobTable) return 0
    const collected = repository.transaction(() =>
      collectBlobsLocked(repository, input.aggregateID, input.beforeSeq, blob),
    )
    // Rows are committed before any file disappears. A rollback therefore leaves
    // every row and every file intact instead of restoring rows over a hole.
    unlinkUnreferenced(repository, collected.hashes, blob)
    return collected.removed
  }

  const deleteAggregate = (aggregateID: string): number => {
    assertAggregate(aggregateID)
    const deleted = repository.transaction(() =>
      hasBlobTable
        ? deleteAggregateLocked(repository, aggregateID, blob)
        : { events: deleteInlineAggregateLocked(repository, aggregateID), hashes: new Set<string>() },
    )
    // Commit first, then unlink zero-reference files (see collectBlobs).
    unlinkUnreferenced(repository, deleted.hashes, blob)
    cache?.invalidate(aggregateID)
    return deleted.events
  }

  const claim = (aggregateID: string, nextOwnerID: string = ownerID): Lease => {
    assertAggregate(aggregateID)
    const lease = repository.transaction(() => {
      repository.claimLease({ aggregateID, ownerID: nextOwnerID, leaseTimeoutMs })
      // `owner_id` is v1 sync state only: a worker generation claims the
      // dedicated worker lease and never mirrors itself into the sync column.
      return { aggregateID, ownerID: nextOwnerID, seq: latest(aggregateID) }
    })
    cache?.invalidate(aggregateID)
    return lease
  }

  const reclaim = (input: ReclaimInput): Lease => {
    assertAggregate(input.aggregateID)
    const lease = repository.transaction(() => {
      repository.reclaimLease({
        aggregateID: input.aggregateID,
        expectedOwnerID: input.expectedOwnerID,
        expectedSeq: input.expectedSeq,
        nextOwnerID: input.nextOwnerID,
        leaseTimeoutMs,
        ...(input.now === undefined ? {} : { now: input.now }),
        ...(input.isDeadOwner === undefined ? {} : { isDeadOwner: input.isDeadOwner }),
      })
      return {
        aggregateID: input.aggregateID,
        ownerID: input.nextOwnerID,
        seq: repository.sequence(input.aggregateID)?.seq ?? -1,
      }
    })
    cache?.invalidate(input.aggregateID)
    return lease
  }

  const releaseDeadLeases = (input: ReleaseDeadLeasesInput = {}): number => {
    if (!workerLease) return 0
    const release = () =>
      repository.releaseDeadLeases({
        ...(scopeAggregateID === undefined ? {} : { aggregateID: scopeAggregateID }),
        ...(input.isDeadOwner === undefined ? {} : { isDeadOwner: input.isDeadOwner }),
      })
    const released = repository.transaction(release)
    if (released > 0 && scopeAggregateID !== undefined) cache?.invalidate(scopeAggregateID)
    return released
  }

  const heartbeat = (aggregateID: string): void => {
    assertAggregate(aggregateID)
    if (!workerLease) return
    if (repository.renewLease({ aggregateID, ownerID, leaseTimeoutMs }) !== 1) {
      throw new StaleOwnerError({
        message: `Aggregate ${aggregateID} worker lease is not held by ${ownerID}`,
      })
    }
  }

  const release = (aggregateID: string): void => {
    assertAggregate(aggregateID)
    if (!workerLease) return
    repository.releaseLease({ aggregateID, ownerID })
  }

  return {
    ...(connection === undefined ? {} : { connection }),
    ownerID,
    workerLease,
    publish,
    publishInTransaction,
    transaction,
    transactionAsync,
    publishAsync,
    project,
    latest,
    ownerOf,
    read,
    claim,
    reclaim,
    releaseDeadLeases,
    heartbeat,
    release,
    collectBlobs,
    deleteAggregate,
  }
}

/** Construct the `DurableLog` service from the same options {@link create} accepts. */
export function make(options: Options): Effect.Effect<Interface> {
  return Effect.sync(() => create(options))
}

/** Adapt one Effect SQL client to the synchronous connection a `DurableLog` owns. */
/** A connection for a database-less graph: probes read empty, writes change nothing. */
const INERT_CONNECTION: Connection = {
  all: () => [],
  run: () => ({ changes: 0 }),
  exec: () => {},
}

export function connectionFromClient(client: SqlClient): Connection {
  const all = (sql: string, params: ReadonlyArray<unknown> = []) =>
    Effect.runSync(client.unsafe<Record<string, unknown>>(sql, params))
  const filename = (client as SqlClient & { readonly config?: { readonly filename?: string } }).config?.filename
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

/** Install a `DurableLog` service into the context; `create(options)` remains the adapter. */
export function layer(options: Options): Layer.Layer<Service> {
  return Layer.effect(Service, make(options))
}

/** The `DurableLog` service over the `Database` service's SQL client. */
export const layerFromDatabase: Layer.Layer<Service, never, Database.Service> = Layer.effect(
  Service,
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const client = (db as Database.Interface["db"] & { readonly $client?: SqlClient }).$client
    // A boundary/degraded Database has no SQL client.
    return create({
      connection: client === undefined ? INERT_CONNECTION : connectionFromClient(client),
      ownerID: `durable-log-${process.pid}-${randomUUID()}`,
    })
  }),
)

/** The global graph node for {@link Service}, backed by the Database client. */
export const node: GlobalNode<Service> = makeGlobalNode({
  service: Service,
  layer: layerFromDatabase,
  deps: [Database.node],
})

/** The global graph node for {@link Service}, bound to one bus's options. */
export function nodeWith(options: Options): GlobalNode<Service> {
  return makeGlobalNode({ service: Service, layer: layer(options), deps: [] })
}

/** SQLite binds at most 999 parameters by default; chunk well below it. */
const BLOB_QUERY_CHUNK = 500

/** The `event_blob` lookup for one aggregate's positions. */
export function blobRowQuery(
  aggregateID: string,
  sequences: ReadonlyArray<number>,
): { readonly sql: string; readonly params: ReadonlyArray<unknown> } {
  const placeholders = sequences.map(() => "?").join(", ")
  return {
    sql: `SELECT seq, hash, data, file_path FROM event_blob WHERE aggregate_id = ? AND seq IN (${placeholders})`,
    params: [aggregateID, ...sequences],
  }
}

/** Index `event_blob` rows by their committed sequence. */
export function blobRowsBySeq(
  rows: ReadonlyArray<Record<string, unknown>>,
): ReadonlyMap<number, Record<string, unknown>> {
  const bySeq = new Map<number, Record<string, unknown>>()
  for (const row of rows) bySeq.set(Number(row.seq), row)
  return bySeq
}

/** Batch the `event_blob` lookup for one page of events. */
export function loadBlobRows(
  connection: Connection,
  aggregateID: string,
  sequences: ReadonlyArray<number>,
): ReadonlyMap<number, Record<string, unknown>> {
  const rows = new Map<number, Record<string, unknown>>()
  for (let offset = 0; offset < sequences.length; offset += BLOB_QUERY_CHUNK) {
    const query = blobRowQuery(aggregateID, sequences.slice(offset, offset + BLOB_QUERY_CHUNK))
    for (const [seq, row] of blobRowsBySeq(connection.all(query.sql, query.params))) rows.set(seq, row)
  }
  return rows
}

/** Resolve one durable event's body from its `event_blob` row. */
export function resolveBlobRow(
  event: { readonly type: string; readonly aggregateID: string; readonly seq: number },
  row: Record<string, unknown> | undefined,
  blob: BlobStore,
  files: Map<string, string> = new Map(),
): Record<string, unknown> | undefined {
  if (row === undefined) return undefined
  return resolveBlobData(
    {
      id: "" as SerializedEvent["id"],
      type: event.type,
      aggregateID: event.aggregateID,
      seq: event.seq,
      data: { __eventBlob: String(row.hash), size: Number(row.size) },
    },
    row,
    blob,
    files,
  )
}

/**
 * The D16 content-addressed directory for a database file, or `undefined` for an in-memory/unknown file.
 */
export function blobDirectoryFor(filename: string | undefined): string | undefined {
  if (filename === undefined || filename.length === 0) return undefined
  if (filename === ":memory:" || filename.startsWith("file::memory:")) return undefined
  return path.join(path.dirname(filename), "event-blobs")
}

/** Resolve `event.data` that points at `event_blob`. */
export function resolveBlobData(
  event: SerializedEvent,
  row: Record<string, unknown> | undefined,
  blob: BlobStore,
  files: Map<string, string>,
): Record<string, unknown> {
  if (!isBlobReference(event.data)) return event.data
  if (row === undefined) {
    throw new InvalidDurableEventError({
      type: event.type,
      message: `event ${event.aggregateID}/${event.seq} references a missing event_blob row`,
    })
  }
  const reference = requireBlobHash(event.data.__eventBlob, `event ${event.aggregateID}/${event.seq} blob reference`)
  const hash = requireBlobHash(row.hash, `event ${event.aggregateID}/${event.seq} blob row hash`)
  if (hash !== reference) {
    throw new InvalidDurableEventError({
      type: event.type,
      message: `event ${event.aggregateID}/${event.seq} blob row hash does not match its reference`,
    })
  }
  let payload: string
  if (typeof row.file_path === "string") {
    if (blob.dir === undefined) {
      throw new InvalidDurableEventError({
        type: event.type,
        message: `event ${event.aggregateID}/${event.seq} is file-backed but no blob directory is configured`,
      })
    }
    const cached = files.get(hash)
    if (cached !== undefined) {
      payload = cached
    } else {
      payload = fs.readFileSync(blobFile(blob.dir, row.file_path), "utf8")
      files.set(hash, payload)
    }
  } else {
    if (typeof row.data !== "string") {
      throw new InvalidDurableEventError({
        type: event.type,
        message: `event ${event.aggregateID}/${event.seq} blob row has neither a file path nor inline data`,
      })
    }
    payload = row.data
  }
  if (digest(payload) !== hash) {
    throw new InvalidDurableEventError({
      type: event.type,
      message: `event ${event.aggregateID}/${event.seq} blob hash does not match its content`,
    })
  }
  return parseStoredData(payload)
}

/** A blob row or its event is below the retention cursor, or the event is gone. */
const COLLECTABLE_BLOBS = `
  SELECT b.seq, b.hash, b.file_path
  FROM event_blob AS b
  WHERE b.aggregate_id = ?
    AND (
      b.seq < ?
      OR NOT EXISTS (
        SELECT 1 FROM event AS e WHERE e.aggregate_id = ? AND e.seq = b.seq
      )
    )
`

const DELETE_COLLECTABLE_BLOBS = `
  DELETE FROM event_blob
  WHERE aggregate_id = ?
    AND (
      seq < ?
      OR NOT EXISTS (
        SELECT 1 FROM event AS e WHERE e.aggregate_id = ? AND e.seq = event_blob.seq
      )
    )
`

/** Candidate file hashes whose rows a GC transaction removed. */
interface CollectedBlobs {
  readonly removed: number
  readonly hashes: ReadonlySet<string>
}

/** A whole-aggregate deletion: durable event rows plus candidate blob files. */
interface DeletedAggregate {
  readonly events: number
  readonly hashes: ReadonlySet<string>
}

/**
 * Delete the retained event rows first, then their blob rows, so an event row can never survive its blob.
 */
function collectBlobsLocked(
  repository: DurableRepository,
  aggregateID: string,
  beforeSeq: number,
  blob: BlobStore,
): CollectedBlobs {
  const candidates = repository.readBlobs({ aggregateID, beforeSeq })
  if (candidates.length === 0) return { removed: 0, hashes: new Set() }
  const hashes = hashesWithFiles(candidates)
  // Validate before any delete: a missing directory must roll the rows back,
  // not commit their removal and then fail to unlink their files.
  requireBlobDirectory(hashes, blob)
  repository.deleteEvents({ aggregateID, beforeSeq })
  const removed = repository.deleteBlobs({ aggregateID, beforeSeq })
  return { removed, hashes }
}

function deleteAggregateLocked(repository: DurableRepository, aggregateID: string, blob: BlobStore): DeletedAggregate {
  const candidates = repository.readBlobs({ aggregateID })
  const hashes = hashesWithFiles(candidates)
  requireBlobDirectory(hashes, blob)
  const events = repository.deleteEvents({ aggregateID })
  repository.deleteBlobs({ aggregateID })
  repository.deleteSequence(aggregateID)
  return { events, hashes }
}

/** The no-blob-table schema still owns `event` and `event_sequence` rows. */
function deleteInlineAggregateLocked(repository: DurableRepository, aggregateID: string): number {
  const events = repository.deleteEvents({ aggregateID })
  repository.deleteSequence(aggregateID)
  return events
}

/** The directory a collection needs, or undefined when every row is inline. */
function requireBlobDirectory(hashes: ReadonlySet<string>, blob: BlobStore): string | undefined {
  if (hashes.size === 0) return undefined
  if (blob.dir === undefined) {
    throw new InvalidDurableEventError({
      type: "event_blob",
      message: "cannot collect file-backed blobs without a blob directory",
    })
  }
  return blob.dir
}

/** Only rows that name a file need a directory; legacy inline rows do not. */
function hashesWithFiles(rows: ReadonlyArray<BlobRow>): ReadonlySet<string> {
  const hashes = new Set<string>()
  for (const row of rows) {
    if (row.filePath === null) continue
    hashes.add(requireBlobHash(row.hash, "event_blob.hash"))
  }
  return hashes
}

/** Unlink files whose deleting transaction has already committed. */
function unlinkUnreferenced(repository: DurableRepository, hashes: ReadonlySet<string>, blob: BlobStore): void {
  const directory = requireBlobDirectory(hashes, blob)
  if (directory === undefined) return
  repository.transaction(() => {
    for (const hash of hashes) {
      if (repository.blobReferences(hash) > 0) continue
      try {
        fs.unlinkSync(blobFile(directory, hash))
      } catch {
        // A missing file is already collected; the row is still gone.
      }
    }
  })
}
