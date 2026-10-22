// Phase 7 — writer core (plan tasks 7.1, 7.4, 7.5, 7.6, 7.7, 7.8, 7.9, 7.10,
// 7.11, 7.12, 7.13, 7.14, 7.15, 7.17, 7.18, 7.19).
//
// The writer core is the single point through which every database mutation and
// read flows. It is transport-agnostic: production drives it from a Worker isolate
// (see `./worker.ts`), tests drive it in-process against an in-memory connection.
//
// Invariants this module owns:
//
//   * ONE serialized queue. Every operation — query, transaction, publish, admin —
//     is admitted through `enqueue`, so all mutations serialize and per-aggregate
//     order is deterministic (7.1, 7.15). The bound governs admitted operations
//     (`depth()`); callers beyond it park in an overflow list that backpressures
//     rather than dropping work. Both counts are surfaced by `pending()`, which
//     reports the runnable `depth` alongside the `overflow` waiters.
//   * Reserved-handle transactions. One transaction may be pinned at a time; other
//     operations queue behind it (documented head-of-line blocking). A pinned
//     transaction is rolled back after `txIdleMs` of inactivity and on close (7.4).
//   * Sequence allocation at commit. The writer, never the caller, assigns
//     `event_sequence.seq = latest + 1` inside the commit transaction; an aborted or
//     failed commit consumes no sequence (7.10, 7.11).
//   * Durable projectors run in this isolate, inside the same transaction as the
//     event insert and sequence upsert (7.12). They cross the port as SQL data.
//   * `owner_id` fencing on live durable publishes; takeover only via `steal` (7.14).
//   * Whole-transaction `SQLITE_BUSY` retry with exponential backoff; exhaustion is
//     a typed reject (7.8).
//   * A write-through read cache invalidated by the writer's own mutations (7.18).
//   * Replay is idempotent: a sequence at or below the latest is never republished,
//     and a diverged replay fails loud (7.19).

import { isDeepStrictEqual } from "node:util"
import { Faults } from "@/testing/faults"
import { isSqliteBusy, type WriterConnection } from "./connection"

export interface CommittedEvent {
  readonly id: string
  readonly type: string
  readonly aggregateID: string
  readonly seq: number
  readonly data: Record<string, unknown>
}

/** A local projector: runs inside the commit transaction in the writer isolate. */
export type Projector = (event: CommittedEvent) => void | Promise<void>

export interface UncommittedEvent {
  readonly id: string
  readonly type: string
  readonly aggregateID: string
  readonly data: Record<string, unknown>
}

export interface PublishInput {
  readonly event: UncommittedEvent
  readonly durable: boolean
  readonly ownerID?: string
  readonly strictOwner?: boolean
  /** In-process projector (not transportable); runs atomically with the commit. */
  readonly commit?: Projector
  /** Id of a registered SQL projector to run atomically with the commit. */
  readonly commitId?: string
}

export interface PublishResult {
  readonly seq: number | null
  readonly committed: boolean
  /** Egress ordinal; `null` for frames that carry no durable event (non-durable, idempotent replay). */
  readonly ordinal: number | null
  readonly idempotent: boolean
}

export interface ReplayInput {
  readonly event: UncommittedEvent
  readonly seq: number
  readonly ownerID?: string
  readonly strictOwner?: boolean
}

export interface CacheStats {
  readonly hits: number
  readonly misses: number
  readonly entries: number
}

/** A read of one file-KV entry; `found` distinguishes absence from a stored `undefined`. */
export interface StorageReadResult {
  readonly found: boolean
  readonly content?: unknown
}

/**
 * The file-KV backend the writer serializes (plan task 7.13). The writer owns the
 * mutations; a worker's `Storage` client proxies here rather than touching the
 * filesystem, so file-KV never bypasses the single-writer boundary.
 */
export interface WriterStorage {
  read(key: ReadonlyArray<string>): Promise<StorageReadResult>
  write(key: ReadonlyArray<string>, content: unknown): Promise<void>
  /**
   * Atomic read-modify-write. The backend reads the current value, applies `fn` to
   * it in place, then persists it — all before the next queued operation runs, so a
   * concurrent update can never lose a write. `found: false` (no `fn`, no write)
   * when the entry is absent.
   */
  update(key: ReadonlyArray<string>, fn: (content: unknown) => void): Promise<StorageReadResult>
  remove(key: ReadonlyArray<string>): Promise<void>
  list(prefix: ReadonlyArray<string>): Promise<ReadonlyArray<ReadonlyArray<string>>>
}

export interface WriterCoreOptions {
  readonly connection: WriterConnection
  /** Maximum admitted-but-unfinished operations before callers backpressure. */
  readonly maxQueue?: number
  /** Whole-transaction retries on `SQLITE_BUSY` after the first attempt. */
  readonly busyRetries?: number
  readonly busyBaseDelayMs?: number
  readonly busyMaxDelayMs?: number
  /** Idle window after which a pinned transaction is rolled back. */
  readonly txIdleMs?: number
  readonly cache?: boolean
  readonly sleep?: (ms: number) => Promise<void>
  /** File-KV backend; storage frames reject until one is set (plan task 7.13). */
  readonly storage?: WriterStorage
}

// ---------------------------------------------------------------------------
// Typed errors — each carries a `tag` so the worker can serialize it faithfully.
// ---------------------------------------------------------------------------

export class WriterError extends Error {
  readonly tag: string
  constructor(tag: string, message: string) {
    super(message)
    this.name = tag
    this.tag = tag
  }
}

export class WriterBusyExhaustedError extends WriterError {
  readonly attempts: number
  constructor(point: string, attempts: number) {
    super("SQLITE_BUSY_EXHAUSTED", `SQLITE_BUSY retries exhausted at ${point} after ${attempts} attempts`)
    this.attempts = attempts
  }
}

export class OwnerMismatchError extends WriterError {
  constructor(message: string) {
    super("OWNER_MISMATCH", message)
  }
}

export class TransactionError extends WriterError {
  constructor(message: string) {
    super("TRANSACTION", message)
  }
}

export class AbortedWriteError extends WriterError {
  constructor(message: string) {
    super("ABORTED", message)
  }
}

export class SequenceError extends WriterError {
  constructor(message: string) {
    super("SEQUENCE", message)
  }
}

export class WriterClosedError extends WriterError {
  constructor() {
    super("CLOSED", "writer is closed")
  }
}

export function writerErrorInfo(error: unknown): { tag: string; message: string } {
  if (error instanceof WriterError) return { tag: error.tag, message: error.message }
  return { tag: "UNKNOWN", message: error instanceof Error ? error.message : String(error) }
}

// ---------------------------------------------------------------------------
// SQL helpers — table extraction powers the write-through cache.
// ---------------------------------------------------------------------------

const SELECT_QUERY = /^\s*(?:select|with)\b/i
const TABLE_REFERENCE = /\b(?:from|join|into|update)\s+[`"[]?([A-Za-z_][A-Za-z0-9_]*)/gi

/** Tables a statement reads or writes; used to invalidate cached reads. */
export function tablesTouched(sql: string): ReadonlyArray<string> {
  const tables = new Set<string>()
  TABLE_REFERENCE.lastIndex = 0
  let match: RegExpExecArray | null
  while ((match = TABLE_REFERENCE.exec(sql)) !== null) tables.add(match[1]!.toLowerCase())
  return [...tables]
}

function isReadQuery(method: string, sql: string): boolean {
  return (method === "all" || method === "values") && SELECT_QUERY.test(sql)
}

function isMutating(method: string, sql: string): boolean {
  return method === "run" || method === "exec" || !SELECT_QUERY.test(sql)
}

function cacheKey(method: string, sql: string, params: ReadonlyArray<unknown>): string {
  return `${method}\u0000${sql}\u0000${JSON.stringify(params)}`
}

/**
 * Named binding for registered SQL projectors. Projector SQL references the names
 * it needs (`$aggregate_id`, `$seq`, `$type`, `$id`, `$data`); unused names are
 * harmless, so a projector that only logs `(aggregate_id, seq, type)` binds three.
 */
export function projectorBinding(event: CommittedEvent): Record<string, unknown> {
  return {
    $aggregate_id: event.aggregateID,
    $seq: event.seq,
    $type: event.type,
    $id: event.id,
    $data: JSON.stringify(event.data),
  }
}

// ---------------------------------------------------------------------------
// Write-through cache
// ---------------------------------------------------------------------------

interface CacheEntry {
  readonly value: unknown
  readonly tables: ReadonlySet<string>
}

class WriteThroughCache {
  private readonly entries = new Map<string, CacheEntry>()
  private hits = 0
  private misses = 0

  get(key: string): unknown | undefined {
    const entry = this.entries.get(key)
    if (entry === undefined) {
      this.misses += 1
      return undefined
    }
    this.hits += 1
    return entry.value
  }

  set(key: string, value: unknown, tables: ReadonlyArray<string>): void {
    this.entries.set(key, { value, tables: new Set(tables) })
  }

  invalidateTables(tables: ReadonlyArray<string>): void {
    if (tables.length === 0) {
      this.entries.clear()
      return
    }
    const touched = new Set(tables)
    for (const [key, entry] of this.entries) {
      for (const table of entry.tables) {
        if (touched.has(table)) {
          this.entries.delete(key)
          break
        }
      }
    }
  }

  clear(): void {
    this.entries.clear()
  }

  stats(): CacheStats {
    return { hits: this.hits, misses: this.misses, entries: this.entries.size }
  }
}

// ---------------------------------------------------------------------------
// Serialized queue
// ---------------------------------------------------------------------------

interface Job {
  readonly txHandle?: string
  readonly reject: (error: unknown) => void
  readonly run: () => void
}

interface PinnedTransaction {
  readonly handle: string
  timer: ReturnType<typeof setTimeout>
}

const DEFAULT_MAX_QUEUE = 4096
const DEFAULT_BUSY_RETRIES = 5
const DEFAULT_BUSY_BASE_DELAY_MS = 10
const DEFAULT_BUSY_MAX_DELAY_MS = 1000
const DEFAULT_TX_IDLE_MS = 30_000

const defaultSleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms))

export class WriterCore {
  private readonly connection: WriterConnection
  private readonly maxQueue: number
  private readonly busyRetries: number
  private readonly busyBaseDelayMs: number
  private readonly busyMaxDelayMs: number
  private readonly txIdleMs: number
  private readonly cacheEnabled: boolean
  private readonly sleep: (ms: number) => Promise<void>
  private storage: WriterStorage | undefined

  private readonly jobs: Job[] = []
  private readonly overflow: Array<() => void> = []
  private readonly cache = new WriteThroughCache()
  private readonly projectors = new Map<string, Projector[]>()
  private readonly registeredProjectors = new Map<string, { readonly eventType: string; readonly sql: string }>()

  private draining = false
  private closed = false
  private pinned: PinnedTransaction | undefined
  private txCounter = 0
  private ordinalCounter = 0
  /** Settles when the currently-running job finishes; `close()` awaits it. */
  private inflight: Promise<void> | undefined

  constructor(options: WriterCoreOptions) {
    this.connection = options.connection
    this.maxQueue = options.maxQueue ?? DEFAULT_MAX_QUEUE
    this.busyRetries = options.busyRetries ?? DEFAULT_BUSY_RETRIES
    this.busyBaseDelayMs = options.busyBaseDelayMs ?? DEFAULT_BUSY_BASE_DELAY_MS
    this.busyMaxDelayMs = options.busyMaxDelayMs ?? DEFAULT_BUSY_MAX_DELAY_MS
    this.txIdleMs = options.txIdleMs ?? DEFAULT_TX_IDLE_MS
    this.cacheEnabled = options.cache ?? true
    this.sleep = options.sleep ?? defaultSleep
    this.storage = options.storage
  }

  /** Install the file-KV backend (opened by the writer-internal `admin.openStorage`). */
  setStorage(storage: WriterStorage): void {
    this.storage = storage
  }

  /** Monotonic frame ordinal stamped on committed durable publishes (egress). */
  ordinal(): number {
    return this.ordinalCounter
  }

  /** Allocate the next egress ordinal; every durable commit site calls this once. */
  private nextOrdinal(): number {
    return (this.ordinalCounter += 1)
  }

  /** Admitted-but-unfinished operations (queue length + the running one). */
  depth(): number {
    return this.jobs.length + (this.draining ? 1 : 0)
  }

  /**
   * Queue observability. `depth` is the admitted operation count that the bound
   * governs; `overflow` counts callers parked off-queue awaiting a free slot, so
   * total memory is `depth + overflow` even though only `depth` is bounded.
   */
  pending(): { readonly queue: number; readonly overflow: number; readonly running: boolean; readonly depth: number } {
    return { queue: this.jobs.length, overflow: this.overflow.length, running: this.draining, depth: this.depth() }
  }

  cacheStats(): CacheStats {
    return this.cache.stats()
  }

  // -- queue ----------------------------------------------------------------

  private enqueue<T>(task: () => Promise<T> | T, options: { readonly txHandle?: string } = {}): Promise<T> {
    if (this.closed) return Promise.reject(new WriterClosedError())
    return new Promise<T>((resolve, reject) => {
      const admit = () => {
        if (this.closed) {
          reject(new WriterClosedError())
          return
        }
        this.jobs.push({
          txHandle: options.txHandle,
          reject,
          run: () => {
            this.draining = true
            this.inflight = Promise.resolve()
              .then(task)
              .then(
                (value) => {
                  this.draining = false
                  resolve(value)
                  this.afterJob()
                },
                (error) => {
                  this.draining = false
                  reject(error)
                  this.afterJob()
                },
              )
          },
        })
        this.pump()
      }
      if (this.depth() >= this.maxQueue) this.overflow.push(admit)
      else admit()
    })
  }

  /**
   * A job may run when nothing is pinned or when it belongs to the pinned
   * transaction handle. Everything else — including another `begin` — waits for
   * the pin to clear, so concurrent top-level transactions serialize instead of
   * rejecting.
   */
  private isRunnable(job: Job): boolean {
    return this.pinned === undefined || job.txHandle === this.pinned.handle
  }

  private pump(): void {
    if (this.draining || this.closed) return
    // Unpinned: every job is runnable, so take the FIFO head in O(1). Pinned: only
    // the pinned handle's own jobs may run; scan for the first such job.
    let index = 0
    if (this.pinned !== undefined) {
      index = this.jobs.findIndex((job) => this.isRunnable(job))
      if (index < 0) return
    } else if (this.jobs.length === 0) {
      return
    }
    const [job] = this.jobs.splice(index, 1)
    job!.run()
  }

  private afterJob(): void {
    if (this.overflow.length > 0 && this.depth() < this.maxQueue) {
      this.overflow.shift()!()
    }
    this.pump()
  }

  // -- execution ------------------------------------------------------------

  private execute(method: string, sql: string, params: ReadonlyArray<unknown>): unknown {
    switch (method) {
      case "all":
        return this.connection.all(sql, params)
      case "values":
        return this.connection.values(sql, params)
      case "run":
        return this.connection.run(sql, params)
      case "exec":
        this.connection.exec(sql)
        return undefined
      default:
        throw new TransactionError(`unknown query method "${method}"`)
    }
  }

  private async runBusyRetried<T>(point: string, body: () => Promise<T>): Promise<T> {
    let delay = this.busyBaseDelayMs
    for (let attempt = 0; ; attempt++) {
      try {
        return await body()
      } catch (error) {
        if (!isSqliteBusy(error)) throw error
        if (attempt >= this.busyRetries) throw new WriterBusyExhaustedError(point, attempt + 1)
        await this.sleep(delay)
        delay = Math.min(this.busyMaxDelayMs, delay * 10)
      }
    }
  }

  private async inTransaction<T>(body: () => Promise<T>): Promise<T> {
    this.connection.run("BEGIN IMMEDIATE", [])
    let committed = false
    try {
      const result = await body()
      this.connection.run("COMMIT", [])
      committed = true
      return result
    } finally {
      if (!committed) {
        try {
          this.connection.run("ROLLBACK", [])
        } catch {
          // already rolled back by the connection
        }
      }
    }
  }

  // -- remote-query proxy ---------------------------------------------------

  query(
    method: string,
    sql: string,
    params: ReadonlyArray<unknown> = [],
    options: { readonly cache?: boolean } = {},
  ): Promise<unknown> {
    return this.enqueue(() => {
      const useCache = (options.cache ?? true) && this.cacheEnabled && isReadQuery(method, sql)
      const key = useCache ? cacheKey(method, sql, params) : undefined
      if (key !== undefined) {
        const cached = this.cache.get(key)
        if (cached !== undefined) return cached
      }
      const value = this.execute(method, sql, params)
      if (isMutating(method, sql)) this.cache.invalidateTables(tablesTouched(sql))
      else if (key !== undefined) this.cache.set(key, value, tablesTouched(sql))
      return value
    })
  }

  // -- reserved-handle transactions ----------------------------------------

  beginTransaction(): Promise<string> {
    // A `begin` is an ordinary queued job: when another transaction is pinned it
    // waits head-of-line instead of rejecting, so the worker's Drizzle database
    // can open concurrent top-level transactions safely.
    return this.enqueue(() => {
      if (this.pinned !== undefined) throw new TransactionError("a transaction is already pinned")
      this.connection.run("BEGIN IMMEDIATE", [])
      const handle = `tx-${++this.txCounter}`
      this.pinned = { handle, timer: this.armIdleTimer(handle) }
      return handle
    })
  }

  txQuery(
    handle: string,
    method: string,
    sql: string,
    params: ReadonlyArray<unknown> = [],
  ): Promise<unknown> {
    return this.enqueue(() => {
      this.assertPinned(handle)
      this.resetIdleTimer(handle)
      if (Faults.abortTxAfter("writer:tx")) {
        // The requester died mid-write: abandon the pinned transaction and release it.
        this.clearIdleTimer()
        this.connection.run("ROLLBACK", [])
        this.pinned = undefined
        throw new AbortedWriteError("requester died mid-write")
      }
      return this.execute(method, sql, params)
    }, { txHandle: handle })
  }

  commitTransaction(handle: string): Promise<void> {
    return this.enqueue(() => {
      this.assertPinned(handle)
      this.clearIdleTimer()
      // A mid-commit crash (only armed in a real isolate) fires before COMMIT, so
      // the open transaction is never committed; the isolate dying rolls it back
      // and nothing partial survives — the reserved-handle analogue of the
      // publish path's mid-commit crash.
      Faults.crashWorker("writer:tx-commit")
      this.connection.run("COMMIT", [])
      this.pinned = undefined
      this.cache.clear()
    }, { txHandle: handle })
  }

  rollbackTransaction(handle: string): Promise<void> {
    return this.enqueue(() => {
      this.assertPinned(handle)
      this.clearIdleTimer()
      this.connection.run("ROLLBACK", [])
      this.pinned = undefined
    }, { txHandle: handle })
  }

  private assertPinned(handle: string): void {
    if (this.pinned === undefined || this.pinned.handle !== handle) {
      throw new TransactionError(`no transaction pinned for handle "${handle}"`)
    }
  }

  private armIdleTimer(handle: string): ReturnType<typeof setTimeout> {
    const timer = setTimeout(() => {
      void this.rollbackTransaction(handle).catch(() => undefined)
    }, this.txIdleMs)
    if (typeof (timer as { unref?: () => void }).unref === "function") (timer as { unref: () => void }).unref()
    return timer
  }

  private resetIdleTimer(handle: string): void {
    if (this.pinned?.handle !== handle) return
    clearTimeout(this.pinned.timer)
    this.pinned.timer = this.armIdleTimer(handle)
  }

  private clearIdleTimer(): void {
    if (this.pinned !== undefined) clearTimeout(this.pinned.timer)
  }

  // -- projectors -----------------------------------------------------------

  /** Register an in-process closure projector (not transportable). */
  project(eventType: string, projector: Projector): void {
    const list = this.projectors.get(eventType) ?? []
    list.push(projector)
    this.projectors.set(eventType, list)
  }

  /** Register a SQL projector that crosses the port as data; run when named by `commitId`. */
  registerProjector(projectorId: string, eventType: string, sql: string): Promise<void> {
    return this.enqueue(() => {
      this.registeredProjectors.set(projectorId, { eventType, sql })
    })
  }

  private async runProjectors(event: CommittedEvent, input: PublishInput): Promise<void> {
    for (const projector of this.projectors.get(event.type) ?? []) await projector(event)
    if (input.commit) await input.commit(event)
    if (input.commitId !== undefined) {
      const registered = this.registeredProjectors.get(input.commitId)
      if (registered === undefined) throw new WriterError("PROJECTOR", `unknown projector "${input.commitId}"`)
      if (registered.eventType !== event.type) {
        throw new WriterError(
          "PROJECTOR",
          `projector "${input.commitId}" is registered for event type "${registered.eventType}", not "${event.type}"`,
        )
      }
      this.connection.run(registered.sql, projectorBinding(event))
    }
  }

  // -- publish --------------------------------------------------------------

  publish(input: PublishInput): Promise<PublishResult> {
    if (!input.durable) {
      // Fire-and-forget: no database work, no sequence, no ordinal, liveness only.
      return this.enqueue(() => ({ seq: null, committed: false, ordinal: null, idempotent: false }))
    }
    return this.enqueue(() =>
      this.runBusyRetried("writer:commit", () =>
        this.inTransaction(async () => {
          if (Faults.abortTxAfter("writer:tx")) throw new AbortedWriteError("requester died before commit")

          const { event } = input
          const latest = this.readLatest(event.aggregateID)
          this.fenceOwner(event.aggregateID, latest.owner, input.ownerID, input.strictOwner, "publish")

          const seq = latest.seq + 1
          const committed: CommittedEvent = { id: event.id, type: event.type, aggregateID: event.aggregateID, seq, data: event.data }
          await this.runProjectors(committed, input)

          const busy = Faults.failCommitAt("writer:commit")
          if (busy) throw busy

          this.writeSequence(event.aggregateID, seq, input.ownerID)
          this.writeEvent(committed)
          // A mid-commit crash (only armed in a real isolate) leaves the open
          // transaction to be rolled back by SQLite; nothing partial survives.
          Faults.crashWorker("writer:commit")

          this.cache.invalidateTables(["event", "event_sequence"])
          return { seq, committed: true, ordinal: this.nextOrdinal(), idempotent: false }
        }),
      ),
    )
  }

  // -- replay ---------------------------------------------------------------

  replay(input: ReplayInput): Promise<PublishResult> {
    return this.enqueue(() =>
      this.runBusyRetried("writer:commit", () =>
        this.inTransaction(async () => {
          const { event, seq } = input
          const latest = this.readLatest(event.aggregateID)
          this.fenceOwner(event.aggregateID, latest.owner, input.ownerID, input.strictOwner, "replay")

          if (seq <= latest.seq) {
            // Never republish a sequence at or below the latest: idempotent when
            // the stored row matches, a hard divergence otherwise.
            const stored = this.connection.all(
              "SELECT id, type, data FROM event WHERE aggregate_id = ? AND seq = ?",
              [event.aggregateID, seq],
            )[0]
            if (
              stored !== undefined &&
              stored.id === event.id &&
              stored.type === event.type &&
              this.sameData(stored.data, event.data)
            ) {
              return { seq, committed: false, ordinal: null, idempotent: true }
            }
            throw new SequenceError(`replay diverged at aggregate ${event.aggregateID} sequence ${seq}`)
          }

          if (seq !== latest.seq + 1) {
            throw new SequenceError(
              `sequence mismatch for aggregate ${event.aggregateID}: expected ${latest.seq + 1}, got ${seq}`,
            )
          }

          const committed: CommittedEvent = { id: event.id, type: event.type, aggregateID: event.aggregateID, seq, data: event.data }
          await this.runProjectors(committed, { event, durable: true, ownerID: input.ownerID })
          this.writeSequence(event.aggregateID, seq, input.ownerID)
          this.writeEvent(committed)
          this.cache.invalidateTables(["event", "event_sequence"])
          return { seq, committed: true, ordinal: this.nextOrdinal(), idempotent: false }
        }),
      ),
    )
  }

  // -- ownership ------------------------------------------------------------

  /** Last-wins assignment used by `steal`; `onlyIfUnowned` is the fenced `claim`. */
  private setOwner(aggregateID: string, ownerID: string, options: { readonly onlyIfUnowned: boolean }): void {
    const sql = options.onlyIfUnowned
      ? "UPDATE event_sequence SET owner_id = ? WHERE aggregate_id = ? AND owner_id IS NULL"
      : "UPDATE event_sequence SET owner_id = ? WHERE aggregate_id = ?"
    const outcome = this.connection.run(sql, [ownerID, aggregateID])
    if (options.onlyIfUnowned && outcome.changes === 0) {
      throw new OwnerMismatchError(
        `claim rejected for aggregate ${aggregateID}: already owned (or absent), cannot become ${ownerID}`,
      )
    }
    this.cache.invalidateTables(["event_sequence"])
  }

  /**
   * Fenced claim: only succeeds while the aggregate is unowned. Unlike `steal`, a
   * claim never overwrites an existing owner.
   */
  claim(aggregateID: string, ownerID: string): Promise<void> {
    return this.enqueue(() => this.setOwner(aggregateID, ownerID, { onlyIfUnowned: true }))
  }

  /** Explicit takeover: unlike a live publish, this unconditionally reassigns. */
  steal(aggregateID: string, ownerID: string): Promise<void> {
    return this.enqueue(() => this.setOwner(aggregateID, ownerID, { onlyIfUnowned: false }))
  }

  /**
   * Owner fencing shared by `publish` and `replay`. A strict operation rejects any
   * mismatch (including when the caller names no owner); a non-strict operation
   * rejects only when the caller names a different owner.
   */
  private fenceOwner(
    aggregateID: string,
    owner: string | null,
    ownerID: string | undefined,
    strict: boolean | undefined,
    what: string,
  ): void {
    if (owner === null) return
    if ((strict || ownerID !== undefined) && owner !== ownerID) {
      throw new OwnerMismatchError(
        `${what} owner mismatch for aggregate ${aggregateID}: held by ${owner}, ${what} by ${ownerID ?? "none"}`,
      )
    }
  }

  remove(aggregateID: string): Promise<void> {
    return this.enqueue(() =>
      this.runBusyRetried("writer:commit", () =>
        this.inTransaction(async () => {
          this.connection.run("DELETE FROM event WHERE aggregate_id = ?", [aggregateID])
          this.connection.run("DELETE FROM event_sequence WHERE aggregate_id = ?", [aggregateID])
          this.cache.invalidateTables(["event", "event_sequence"])
        }),
      ),
    )
  }

  // -- file-KV storage (non-SQL mutation family; plan 7.13) -----------------

  private requireStorage(): WriterStorage {
    if (this.storage === undefined) throw new WriterError("STORAGE", "file-KV storage is not open")
    return this.storage
  }

  storageRead(key: ReadonlyArray<string>): Promise<StorageReadResult> {
    return this.enqueue(() => this.requireStorage().read(key))
  }

  storageWrite(key: ReadonlyArray<string>, content: unknown): Promise<void> {
    return this.enqueue(() => this.requireStorage().write(key, content))
  }

  /**
   * Atomic read-modify-write through the writer's serialized queue: the read, the
   * mutation, and the write happen in one job, so concurrent updates cannot lose a
   * write. Returns `found: false` (and writes nothing) when the entry is absent.
   */
  storageUpdate(key: ReadonlyArray<string>, fn: (content: unknown) => void): Promise<StorageReadResult> {
    return this.enqueue(() => this.requireStorage().update(key, fn))
  }

  storageRemove(key: ReadonlyArray<string>): Promise<void> {
    return this.enqueue(() => this.requireStorage().remove(key))
  }

  storageList(prefix: ReadonlyArray<string>): Promise<ReadonlyArray<ReadonlyArray<string>>> {
    return this.enqueue(() => this.requireStorage().list(prefix))
  }

  // -- admin (writer-internal) ---------------------------------------------

  pragma(sql: string): Promise<void> {
    return this.enqueue(() => {
      this.connection.exec(sql)
      this.cache.clear()
    })
  }

  migrate(statements: ReadonlyArray<string>): Promise<void> {
    return this.enqueue(() =>
      this.runBusyRetried("writer:migrate", () =>
        this.inTransaction(async () => {
          for (const statement of statements) this.connection.exec(statement)
          this.cache.clear()
        }),
      ),
    )
  }

  checkpoint(): Promise<void> {
    return this.enqueue(() => {
      try {
        this.connection.exec("PRAGMA wal_checkpoint(TRUNCATE)")
      } catch {
        // in-memory / non-WAL databases have nothing to checkpoint
      }
    })
  }

  // -- lifecycle ------------------------------------------------------------

  async close(): Promise<void> {
    if (this.closed) return
    this.closed = true
    if (this.pinned !== undefined) {
      this.clearIdleTimer()
      try {
        this.connection.run("ROLLBACK", [])
      } catch {
        // best effort
      }
      this.pinned = undefined
    }
    // Let the running job finish before the connection goes away; otherwise a job
    // admitted just before close could resume as a microtask against a closed
    // connection. `closed` stops any further admission/pumping.
    if (this.inflight !== undefined) {
      try {
        await this.inflight
      } catch {
        // the job's own caller observes the failure
      }
    }
    // Reject every admitted-but-unfinished operation and admit the backpressured
    // callers so they observe the closure instead of hanging forever.
    for (const job of this.jobs.splice(0)) job.reject(new WriterClosedError())
    for (const admit of this.overflow.splice(0)) admit()
    this.connection.close()
  }

  // -- internals ------------------------------------------------------------

  private readLatest(aggregateID: string): { seq: number; owner: string | null } {
    const row = this.connection.all("SELECT seq, owner_id FROM event_sequence WHERE aggregate_id = ?", [aggregateID])[0]
    const seq = typeof row?.seq === "number" ? row.seq : -1
    const owner = typeof row?.owner_id === "string" ? row.owner_id : null
    return { seq, owner }
  }

  private writeSequence(aggregateID: string, seq: number, ownerID: string | undefined): void {
    this.connection.run(
      "INSERT INTO event_sequence (aggregate_id, seq, owner_id) VALUES (?, ?, ?) " +
        "ON CONFLICT(aggregate_id) DO UPDATE SET seq = excluded.seq, " +
        "owner_id = COALESCE(event_sequence.owner_id, excluded.owner_id)",
      [aggregateID, seq, ownerID ?? null],
    )
  }

  private writeEvent(event: CommittedEvent): void {
    this.connection.run("INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?, ?, ?, ?, ?)", [
      event.id,
      event.aggregateID,
      event.seq,
      event.type,
      JSON.stringify(event.data),
    ])
  }

  private sameData(stored: unknown, expected: Record<string, unknown>): boolean {
    if (typeof stored === "string") {
      try {
        return isDeepStrictEqual(JSON.parse(stored), expected)
      } catch {
        return false
      }
    }
    return isDeepStrictEqual(stored, expected)
  }
}

export function createWriterCore(options: WriterCoreOptions): WriterCore {
  return new WriterCore(options)
}

export * as WriterCoreModule from "./core"
