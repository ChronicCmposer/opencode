// Single source of truth for the SQLite WAL tuning shared by the Effect read
// connection (`database.ts`) and the writer isolate (`session/writer/worker.ts`).
//
// The cap only works when every connection that checkpoints agrees on it, so the
// pragma list lives here rather than being duplicated per connection. The writer
// keeps a shorter busy timeout (it is synchronous inside an isolate); that is the
// only connection-specific value and it stays an explicit option.

const MEBIBYTE = 1024 * 1024

/** SQLite caps the WAL file to this many bytes after a checkpoint resets it. */
export const DEFAULT_JOURNAL_SIZE_LIMIT_BYTES = 64 * MEBIBYTE

/** Cadence of the writer's idle truncating checkpoint. */
export const DEFAULT_CHECKPOINT_INTERVAL_MS = 5 * 60 * 1000

/** Default driver-level `busy_timeout` applied immediately after open. */
export const DEFAULT_BUSY_TIMEOUT_MS = 5000

/**
 * Default page-cache budget for the hub's connection (64 MiB). A per-session
 * worker connection must not inherit this: eight workers would reserve half a
 * gigabyte of page cache. Worker connections use {@link DEFAULT_WORKER_CACHE_SIZE_KIB}.
 */
export const DEFAULT_CACHE_SIZE_KIB = 64_000

/** Default page-cache budget for one per-session worker connection (8 MiB). */
export const DEFAULT_WORKER_CACHE_SIZE_KIB = 8_192

export interface WalConfig {
  readonly journalSizeLimitBytes: number
  readonly checkpointIntervalMs: number
  /**
   * Driver `busy_timeout` resolved at the boundary. Optional so a caller may pass
   * only the fields it overrides (the writer keeps its own short timeout).
   */
  readonly busyTimeoutMs?: number
}

type Environment = Record<string, string | undefined>

/**
 * Parse an integer setting at the process boundary. Absent or malformed values
 * fall back to the default instead of failing startup; `minimum` encodes the
 * sentinel each setting uses to disable itself (`-1` for the size cap, `0` for
 * the checkpoint cadence).
 */
function parseInteger(value: string | undefined, fallback: number, minimum: number): number {
  if (value === undefined) return fallback
  const parsed = Number(value)
  return Number.isInteger(parsed) && parsed >= minimum ? parsed : fallback
}

export function resolveWalConfig(env: Environment = process.env): WalConfig {
  return {
    journalSizeLimitBytes: parseInteger(env.OPENCODE_DB_JOURNAL_SIZE_LIMIT, DEFAULT_JOURNAL_SIZE_LIMIT_BYTES, -1),
    checkpointIntervalMs: parseInteger(env.OPENCODE_DB_CHECKPOINT_INTERVAL_MS, DEFAULT_CHECKPOINT_INTERVAL_MS, 0),
    busyTimeoutMs: parseInteger(env.OPENCODE_DB_BUSY_TIMEOUT_MS, DEFAULT_BUSY_TIMEOUT_MS, 0),
  }
}

/**
 * Page-cache budget for a per-session worker connection. `OPENCODE_DB_WORKER_CACHE_SIZE_KIB`
 * overrides it; malformed/absent values fall back to 8 MiB, so a fleet of
 * workers cannot multiply the hub's 64 MiB budget by the worker cap.
 */
export function resolveWorkerCacheSizeKib(env: Environment = process.env): number {
  return parseInteger(env.OPENCODE_DB_WORKER_CACHE_SIZE_KIB, DEFAULT_WORKER_CACHE_SIZE_KIB, 1)
}

/** Bounded busy-retry budget for the main connection's SQL choke point. */
export const DEFAULT_BUSY_RETRIES = 5
export const DEFAULT_BUSY_BASE_DELAY_MS = 50
export const DEFAULT_BUSY_MAX_DELAY_MS = 1000

export interface BusyRetryConfig {
  readonly retries: number
  readonly baseDelayMs: number
  readonly maxDelayMs: number
}

/**
 * Resolve the busy-retry knobs at the process boundary. These are additive to
 * the fixed `busy_timeout`; a lock that outlasts `busy_timeout` is retried with
 * exponential backoff rather than surfacing immediately.
 */
export function resolveBusyRetryConfig(env: Environment = process.env): BusyRetryConfig {
  return {
    retries: parseInteger(env.OPENCODE_DB_BUSY_RETRIES, DEFAULT_BUSY_RETRIES, 0),
    baseDelayMs: parseInteger(env.OPENCODE_DB_BUSY_BASE_DELAY_MS, DEFAULT_BUSY_BASE_DELAY_MS, 0),
    maxDelayMs: parseInteger(env.OPENCODE_DB_BUSY_MAX_DELAY_MS, DEFAULT_BUSY_MAX_DELAY_MS, 0),
  }
}

/**
 * The resolved WAL settings a connection needs at open. Taken as a partial of
 * the canonical {@link WalConfig} so a connection can pass the whole resolved
 * config (including `checkpointIntervalMs`, which the writer core consumes) and
 * override only the connection-specific busy timeout.
 */
export interface WalPragmaOptions extends Partial<WalConfig> {
  /** Page-cache budget in KiB; defaults to the hub budget (64 MiB). */
  readonly cacheSizeKib?: number
}

/** Pragmas every connection applies immediately after open. */
export function walPragmas(options: WalPragmaOptions = {}): string[] {
  const journalSizeLimitBytes = options.journalSizeLimitBytes ?? DEFAULT_JOURNAL_SIZE_LIMIT_BYTES
  const busyTimeoutMs = options.busyTimeoutMs ?? DEFAULT_BUSY_TIMEOUT_MS
  const cacheSizeKib = options.cacheSizeKib ?? DEFAULT_CACHE_SIZE_KIB
  return [
    // Install the busy handler before switching the journal mode: the switch
    // needs a lock, so without a timeout an open racing another handle's
    // checkpoint fails immediately with SQLITE_BUSY instead of waiting.
    `PRAGMA busy_timeout = ${busyTimeoutMs}`,
    "PRAGMA journal_mode = WAL",
    "PRAGMA synchronous = NORMAL",
    `PRAGMA cache_size = -${cacheSizeKib}`,
    "PRAGMA foreign_keys = ON",
    `PRAGMA journal_size_limit = ${journalSizeLimitBytes}`,
  ]
}
