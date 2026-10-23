// Bounded busy-retry for the main SQLite connection's single choke point.
//
// Drizzle's `db.transaction` does not go through `SqlClient.transaction`: it
// reserves the `Connection` and issues `begin`/`commit`/`rollback` (and every
// statement) through that connection's methods. Wrapping those methods is
// therefore what lets both transactions and statements recover from a lock held
// by the worker writer. Retries are additive to the fixed `busy_timeout`: a busy
// failure is retried with bounded exponential backoff, and once the budget is
// spent the effect still fails loud with `BusyRetriesExhaustedError`.

import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import { isSqlError } from "effect/unstable/sql/SqlError"
import { resolveBusyRetryConfig, type BusyRetryConfig } from "./pragmas"

/**
 * Native SQLite code families that mean "the database is locked by someone else".
 * SQLite extends the base code with a suffix (`SQLITE_BUSY_SNAPSHOT`,
 * `SQLITE_LOCKED_SHAREDCACHE`, …), so membership is a prefix match on the
 * underscore boundary rather than an exact-code set.
 */
const BUSY_SQLITE_CODES: ReadonlyArray<string> = ["SQLITE_BUSY", "SQLITE_LOCKED"]

const LOCKED_MESSAGE = "database is locked"

/** True when `code` is a busy/locked code or an extended form of one. */
function isBusyCode(code: string): boolean {
  return BUSY_SQLITE_CODES.some((base) => code === base || code.startsWith(`${base}_`))
}

function hasBusyCode(error: object): boolean {
  const code = (error as { code?: unknown }).code
  if (typeof code === "string" && isBusyCode(code)) return true
  // The writer-side classifier reads `sqliteCode`; mirror that for parity.
  const sqliteCode = (error as { sqliteCode?: unknown }).sqliteCode
  return typeof sqliteCode === "string" && isBusyCode(sqliteCode)
}

/**
 * True when `error` carries a recognizable native-driver shape. The "database is
 * locked" message fallback is only trustworthy for these: an arbitrary error
 * whose message happens to contain that text must not trigger a retry.
 */
function isNativeSqliteError(error: object): boolean {
  const name = (error as { name?: unknown }).name
  if (name === "SQLiteError") return true
  if (typeof (error as { errno?: unknown }).errno === "number") return true
  if (typeof (error as { sqliteCode?: unknown }).sqliteCode === "string") return true
  const code = (error as { code?: unknown }).code
  return (typeof code === "string" && code.startsWith("SQLITE_")) || typeof code === "number"
}

function hasBusyMessage(error: object): boolean {
  if (!isNativeSqliteError(error)) return false
  const message = (error as { message?: unknown }).message
  if (typeof message !== "string") return false
  return message.includes("SQLITE_BUSY") || message.includes("SQLITE_LOCKED") || message.includes(LOCKED_MESSAGE)
}

/**
 * True when `error` is a lock/busy failure a retry can recover from. Recognizes
 * the `LockTimeoutError` reason produced by `classifySqliteError`, the native
 * `SQLITE_BUSY`/`SQLITE_LOCKED` code families (including extended forms), and a
 * native-driver error whose message says the database is locked. Mirrors the
 * writer isolate's `isSqliteBusy` without importing across packages (core stays
 * self-contained).
 */
export function isBusySqlError(error: unknown): boolean {
  if (typeof error !== "object" || error === null) return false
  if (isSqlError(error) && error.reason._tag === "LockTimeoutError") return true
  return hasBusyCode(error) || hasBusyMessage(error)
}

export interface BusyRetryOptions {
  readonly retries?: number
  readonly baseDelayMs?: number
  readonly maxDelayMs?: number
  readonly sleep?: (ms: number) => Effect.Effect<void>
}

/** Raised as a typed failure once the retry budget is spent. */
export class BusyRetriesExhaustedError extends Schema.TaggedErrorClass<BusyRetriesExhaustedError>()(
  "Busy.BusyRetriesExhaustedError",
  {
    attempts: Schema.Number,
    lastCause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `SQLite remained busy after ${this.attempts} attempt(s)`
  }
}

/** Resolve the optional knobs against the environment, without reading env unnecessarily. */
function resolveRetryConfig(options: BusyRetryOptions): BusyRetryConfig {
  if (options.retries !== undefined && options.baseDelayMs !== undefined && options.maxDelayMs !== undefined) {
    return { retries: options.retries, baseDelayMs: options.baseDelayMs, maxDelayMs: options.maxDelayMs }
  }
  const defaults = resolveBusyRetryConfig()
  return {
    retries: options.retries ?? defaults.retries,
    baseDelayMs: options.baseDelayMs ?? defaults.baseDelayMs,
    maxDelayMs: options.maxDelayMs ?? defaults.maxDelayMs,
  }
}

/**
 * Retry `effect` with bounded exponential backoff whenever it fails with a busy
 * SQL error. Non-busy failures and successes pass through unchanged. Once
 * `retries` additional attempts are exhausted the effect fails with the typed
 * `BusyRetriesExhaustedError` carrying the attempt count and the last cause, so
 * a caller can recover deliberately instead of observing an uncatchable defect.
 * The SQLite `Connection` adapters fold this reason back into their `SqlError`
 * channel at the driver boundary, where the interface requires it.
 */
export function withBusyRetry<A, E, R>(
  effect: Effect.Effect<A, E, R>,
  options: BusyRetryOptions = {},
): Effect.Effect<A, E | BusyRetriesExhaustedError, R> {
  const { retries, baseDelayMs, maxDelayMs } = resolveRetryConfig(options)
  const sleep = options.sleep ?? Effect.sleep

  const attempt = (retriesRemaining: number, attemptsMade: number): Effect.Effect<A, E | BusyRetriesExhaustedError, R> =>
    Effect.catch(effect, (error) => {
      if (!isBusySqlError(error)) return Effect.fail(error)
      if (retriesRemaining <= 0) {
        return Effect.fail(new BusyRetriesExhaustedError({ attempts: attemptsMade, lastCause: error }))
      }
      const delay = Math.min(baseDelayMs * 2 ** (attemptsMade - 1), maxDelayMs)
      return Effect.andThen(sleep(delay), attempt(retriesRemaining - 1, attemptsMade + 1))
    })

  return attempt(retries, 1)
}

export { resolveBusyRetryConfig } from "./pragmas"
export type { BusyRetryConfig } from "./pragmas"

export * as Busy from "./busy"
