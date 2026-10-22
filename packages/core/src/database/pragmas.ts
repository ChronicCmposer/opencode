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

export interface WalConfig {
  readonly journalSizeLimitBytes: number
  readonly checkpointIntervalMs: number
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
  }
}

export interface WalPragmaOptions {
  readonly journalSizeLimitBytes?: number
  readonly busyTimeoutMs?: number
}

/** Pragmas every connection applies immediately after open. */
export function walPragmas(options: WalPragmaOptions = {}): string[] {
  const journalSizeLimitBytes = options.journalSizeLimitBytes ?? DEFAULT_JOURNAL_SIZE_LIMIT_BYTES
  const busyTimeoutMs = options.busyTimeoutMs ?? 5000
  return [
    "PRAGMA journal_mode = WAL",
    "PRAGMA synchronous = NORMAL",
    `PRAGMA busy_timeout = ${busyTimeoutMs}`,
    "PRAGMA cache_size = -64000",
    "PRAGMA foreign_keys = ON",
    `PRAGMA journal_size_limit = ${journalSizeLimitBytes}`,
  ]
}
