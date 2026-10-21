// Phase 6.6 — per-worker cache contract (plan decision lines 36–38).
//
// The database is the source of truth; the worker keeps a bounded in-isolate
// cache with three *classes* of entry, each with a different invalidation rule:
//
//   immutable      reference data (agents / skills / models). Never invalidated
//                  during the worker's lifetime.
//   stale-tolerant enumerated metadata (config / auth). May be served stale up
//                  to a TTL, and is invalidated wholesale when the hub bumps the
//                  snapshot epoch.
//   read-through   session / message / part / event. Filled from the writer and
//                  invalidated by the worker's own writes and by received durable
//                  events.
//
// A strict read-through cache would turn every hot read into a cross-thread RPC;
// the TTL window exists so enumerated metadata survives a burst of reads.

export type CacheClass = "immutable" | "stale-tolerant" | "read-through"

export interface CacheEntry<T = unknown> {
  readonly value: T
  readonly class: CacheClass
  readonly storedAt: number
  readonly epoch: number
  readonly ttlMs?: number
}

export interface WorkerCacheOptions {
  /** Clock seam; defaults to `Date.now`. */
  readonly now?: () => number
  /** Default stale-tolerant TTL in ms; defaults to 30_000. */
  readonly ttlMs?: number
  /** LRU bound; defaults to 1000. */
  readonly capacity?: number
  /** Initial snapshot epoch; defaults to 0. */
  readonly epoch?: number
}

const DEFAULT_TTL_MS = 30_000
const DEFAULT_CAPACITY = 1000

/**
 * Bounded per-worker cache. Entries are immutable snapshots: `get` never mutates
 * a stored value, it only decides whether the entry is still valid for the
 * requested class semantics.
 */
export class WorkerCache {
  private readonly entries = new Map<string, CacheEntry>()
  private readonly now: () => number
  private readonly defaultTtlMs: number
  private readonly capacity: number
  private currentEpoch: number

  constructor(options: WorkerCacheOptions = {}) {
    this.now = options.now ?? Date.now
    this.defaultTtlMs = options.ttlMs ?? DEFAULT_TTL_MS
    this.capacity = options.capacity ?? DEFAULT_CAPACITY
    this.currentEpoch = options.epoch ?? 0
  }

  get epoch(): number {
    return this.currentEpoch
  }

  get size(): number {
    return this.entries.size
  }

  /** Reference data that never expires. */
  defineImmutable<T>(key: string, value: T): void {
    this.store(key, { value, class: "immutable", storedAt: this.now(), epoch: this.currentEpoch })
  }

  /** Enumerated metadata that may be served stale up to `ttlMs`. */
  defineStale<T>(key: string, value: T, ttlMs = this.defaultTtlMs): void {
    this.store(key, { value, class: "stale-tolerant", storedAt: this.now(), epoch: this.currentEpoch, ttlMs })
  }

  /**
   * Return a valid entry value, or `undefined` when absent/expired.
   *
   * - immutable: always valid.
   * - stale-tolerant: valid while within TTL and the snapshot epoch matches.
   * - read-through: valid until explicitly invalidated.
   *
   * The value is `unknown` because one cache holds entries of unrelated shapes;
   * the caller narrows the value it asked for. A `get` hit promotes the entry.
   */
  get(key: string): unknown {
    const entry = this.validEntry(key)
    if (entry === undefined) return undefined
    this.touch(key, entry)
    return entry.value
  }

  /** Whether a valid entry exists. A `has` hit promotes the entry (LRU). */
  has(key: string): boolean {
    const entry = this.validEntry(key)
    if (entry === undefined) return false
    this.touch(key, entry)
    return true
  }

  /**
   * Read-through: return the cached value, or load it from the writer, cache it,
   * and return it. A stored `undefined` is a hit, so the loader runs once per
   * key. Concurrent callers may each load; the cache is write-through, not a
   * single-flight coordinator (the writer serializes anyway).
   */
  async readThrough<T>(key: string, loader: () => Promise<T>): Promise<T> {
    if (this.has(key)) return this.get(key) as T
    const value = await loader()
    this.store(key, { value, class: "read-through", storedAt: this.now(), epoch: this.currentEpoch })
    return value
  }

  /** Drop one entry (own write or received durable event). */
  invalidate(key: string): void {
    this.entries.delete(key)
  }

  /** Drop every read-through entry (a durable event touched unknown aggregates). */
  invalidateReadThrough(): void {
    for (const [key, entry] of this.entries) if (entry.class === "read-through") this.entries.delete(key)
  }

  /**
   * Adopt a new hub snapshot epoch. Stale-tolerant entries from older epochs are
   * dropped; immutable reference data and read-through entries are unaffected.
   */
  invalidateEpoch(epoch: number): void {
    if (epoch === this.currentEpoch) return
    this.currentEpoch = epoch
    for (const [key, entry] of this.entries) {
      if (entry.class === "stale-tolerant" && entry.epoch !== epoch) this.entries.delete(key)
    }
  }

  clear(): void {
    this.entries.clear()
  }

  private isValid(entry: CacheEntry): boolean {
    if (entry.class === "immutable") return true
    if (entry.class === "read-through") return true
    return entry.epoch === this.currentEpoch && this.now() - entry.storedAt <= (entry.ttlMs ?? this.defaultTtlMs)
  }

  /** Return a valid entry, evicting it when expired. */
  private validEntry(key: string): CacheEntry | undefined {
    const entry = this.entries.get(key)
    if (entry === undefined) return undefined
    if (!this.isValid(entry)) {
      this.entries.delete(key)
      return undefined
    }
    return entry
  }

  private store(key: string, entry: CacheEntry): void {
    // Re-inserting moves the key to the most-recently-used position (Map order).
    this.entries.delete(key)
    this.entries.set(key, entry)
    while (this.entries.size > this.capacity) {
      const oldest = this.entries.keys().next().value
      if (oldest === undefined) break
      this.entries.delete(oldest)
    }
  }

  private touch(key: string, entry: CacheEntry): void {
    this.entries.delete(key)
    this.entries.set(key, entry)
  }
}

export * as WorkerCacheModule from "./cache"
