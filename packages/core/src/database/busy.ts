// Bounded busy-retry for the main SQLite connection's single choke point.
//
// Drizzle's `db.transaction` does not go through `SqlClient.transaction`: it
// reserves the `Connection` and issues `begin`/`commit`/`rollback` (and every
// statement) through that connection's methods. Wrapping those methods is
// therefore what lets both transactions and statements recover from a lock held
// by a worker connection. Retries are additive to the fixed `busy_timeout`: a busy
// failure is retried with bounded exponential backoff, and once the budget is
// spent the effect still fails loud with `BusyExhaustedError`.

import { Effect, Schedule, Schema } from "effect"
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

/**
 * The recognizable shape of a native SQLite driver error. Both the bun
 * (`SQLiteError`) and node (`node:sqlite`) drivers surface a subset of these, so
 * narrowing to this interface lets the rest of the classifier read the fields
 * without repeated casts.
 */
export interface NativeSqliteError {
  readonly name?: unknown
  readonly code?: unknown
  readonly sqliteCode?: unknown
  /** Bun's `SQLiteError` extended code (`5` = BUSY, `6` = LOCKED). */
  readonly errno?: unknown
  /** `node:sqlite`'s extended code (`5` = BUSY, `6` = LOCKED). */
  readonly errcode?: unknown
  /** `node:sqlite`'s human-readable code text (`"database is locked"`). */
  readonly errstr?: unknown
  readonly message?: unknown
}

/**
 * True when `error` carries a recognizable native-driver shape. The "database is
 * locked" message fallback is only trustworthy for these: an arbitrary error
 * whose message happens to contain that text must not trigger a retry.
 *
 * This is the single guard: the Effect adapter and the synchronous transaction
 * helpers both narrow through it instead of re-implementing a driver-shape check.
 */
export function isNativeSqliteError(error: unknown): error is NativeSqliteError {
  if (typeof error !== "object" || error === null) return false
  const candidate = error as NativeSqliteError
  if (candidate.name === "SQLiteError") return true
  if (typeof candidate.errno === "number") return true
  // `node:sqlite` reports `errcode`/`errstr` and a generic `ERR_SQLITE_ERROR`
  // code instead of bun's `SQLITE_*` code + `errno`.
  if (typeof candidate.errcode === "number") return true
  if (typeof candidate.errstr === "string") return true
  if (typeof candidate.sqliteCode === "string") return true
  const code = candidate.code
  return (typeof code === "string" && code.startsWith("SQLITE_")) || typeof code === "number"
}

/** SQLITE_BUSY and SQLITE_LOCKED share this low byte; extended codes pack it. */
function isBusyNumericCode(code: number): boolean {
  const primary = code & 0xff
  return primary === 5 || primary === 6
}

function hasBusyCode(error: NativeSqliteError): boolean {
  const code = error.code
  if (typeof code === "string" && isBusyCode(code)) return true
  // Bun/`node:sqlite` may expose the family as `sqliteCode` instead of `code`.
  const sqliteCode = error.sqliteCode
  if (typeof sqliteCode === "string" && isBusyCode(sqliteCode)) return true
  if (typeof error.errno === "number" && isBusyNumericCode(error.errno)) return true
  if (typeof error.errcode === "number" && isBusyNumericCode(error.errcode)) return true
  return false
}

function hasBusyMessage(error: NativeSqliteError): boolean {
  const message = error.message
  if (typeof message !== "string") return false
  if (message.includes("SQLITE_BUSY") || message.includes("SQLITE_LOCKED") || message.includes(LOCKED_MESSAGE)) {
    return true
  }
  const errstr = error.errstr
  return typeof errstr === "string" && errstr.includes(LOCKED_MESSAGE)
}

/**
 * True when `error` is a lock/busy failure a retry can recover from. Recognizes
 * the `LockTimeoutError` reason produced by `classifySqliteError`, the native
 * `SQLITE_BUSY`/`SQLITE_LOCKED` code families (including extended forms), and a
 * native-driver error whose message says the database is locked. This is the
 * one busy classifier: the Effect adapter and the synchronous transaction
 * helpers both delegate here, so driver shapes cannot drift between them.
 */
export function isBusySqlError(error: unknown): boolean {
  if (typeof error !== "object" || error === null) return false
  if (isSqlError(error)) {
    if (error.reason._tag === "LockTimeoutError") return true
    // Effect's `classifySqliteError` does not recognize `node:sqlite`'s
    // `ERR_SQLITE_ERROR` shape, so it folds the raw driver error into an
    // `UnknownError` reason. Recover the native cause and classify it here.
    const cause = (error.reason as { readonly cause?: unknown }).cause
    if (cause !== undefined) return isNativeSqliteError(cause) && (hasBusyCode(cause) || hasBusyMessage(cause))
    return false
  }
  return isNativeSqliteError(error) && (hasBusyCode(error) || hasBusyMessage(error))
}

/** Typed terminal failure: the bounded busy-retry budget was spent. */
export class BusyExhaustedError extends Schema.TaggedErrorClass<BusyExhaustedError>("BusyExhaustedError")(
  "Database.BusyExhausted",
  {
    attempts: Schema.Number,
    lastCause: Schema.Unknown,
  },
) {
  readonly tag = "SQLITE_BUSY_EXHAUSTED"

  constructor(
    override readonly attempts: number,
    override readonly lastCause: unknown,
  ) {
    super({ attempts, lastCause })
    this.name = "BusyExhaustedError"
  }

  override get message() {
    return `SQLite remained busy after ${this.attempts} attempt(s)`
  }
}

/** Resolve the optional knobs against the environment, without reading env unnecessarily. */
function resolveRetryConfig(options: Partial<BusyRetryConfig>): BusyRetryConfig {
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

/** Growth factor between busy retries; the default budget sleeps about 3.1s in total. */
const BUSY_BACKOFF_FACTOR = 10

/** Delay before retry number `failedAttempt` (1-based): tenfold growth, capped at `maxDelayMs`. */
export function busyDelayMs(config: BusyRetryConfig, failedAttempt: number): number {
  return Math.min(config.baseDelayMs * BUSY_BACKOFF_FACTOR ** (failedAttempt - 1), config.maxDelayMs)
}

/**
 * The one busy-retry policy: tenfold-growing delays capped at `maxDelayMs`, at most
 * `retries` retries. Synchronous callers that cannot run a `Schedule` use the
 * same numbers through {@link busyDelayMs}.
 */
export function busyRetrySchedule(config: BusyRetryConfig) {
  return Schedule.exponential(config.baseDelayMs, BUSY_BACKOFF_FACTOR).pipe(
    Schedule.either(Schedule.spaced(config.maxDelayMs)),
    Schedule.both(Schedule.recurs(config.retries)),
  )
}

/**
 * Retry `effect` under {@link busyRetrySchedule} whenever it fails with a busy
 * SQL error. Non-busy failures and successes pass through unchanged. Once the
 * retries are spent the effect fails with the typed `BusyExhaustedError`
 * carrying the attempt count and the last cause. The SQLite `Connection`
 * adapters fold this error back into their `SqlError` channel at the driver
 * boundary, where the interface requires it.
 */
export function withBusyRetry<A, E, R>(
  effect: Effect.Effect<A, E, R>,
  options: Partial<BusyRetryConfig> = {},
): Effect.Effect<A, E | BusyExhaustedError, R> {
  const config = resolveRetryConfig(options)
  return effect.pipe(
    Effect.retry({ schedule: busyRetrySchedule(config), while: isBusySqlError }),
    Effect.mapError((error): E | BusyExhaustedError =>
      isBusySqlError(error) ? new BusyExhaustedError(config.retries + 1, error) : error,
    ),
  )
}

export { resolveBusyRetryConfig } from "./pragmas"
export type { BusyRetryConfig } from "./pragmas"

export * as Busy from "./busy"
