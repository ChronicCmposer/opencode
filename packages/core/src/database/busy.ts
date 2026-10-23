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

/** Native SQLite codes that mean "the database is locked by someone else". */
const BUSY_SQLITE_CODES: ReadonlySet<string> = new Set(["SQLITE_BUSY", "SQLITE_LOCKED"])

const LOCKED_MESSAGE = "database is locked"

function hasBusyCode(error: object): boolean {
  const code = (error as { code?: unknown }).code
  if (typeof code === "string" && BUSY_SQLITE_CODES.has(code)) return true
  // The writer-side classifier reads `sqliteCode`; mirror that for parity.
  const sqliteCode = (error as { sqliteCode?: unknown }).sqliteCode
  return typeof sqliteCode === "string" && BUSY_SQLITE_CODES.has(sqliteCode)
}

function hasBusyMessage(error: object): boolean {
  const message = (error as { message?: unknown }).message
  if (typeof message !== "string") return false
  return (
    message.includes("SQLITE_BUSY") || message.includes("SQLITE_LOCKED") || message.includes(LOCKED_MESSAGE)
  )
}

/**
 * True when `error` is a lock/busy failure a retry can recover from. Recognizes
 * the `LockTimeoutError` reason produced by `classifySqliteError`, the native
 * `SQLITE_BUSY`/`SQLITE_LOCKED` codes, and the native "database is locked"
 * message. Mirrors the writer isolate's `isSqliteBusy` without importing across
 * packages (core stays self-contained).
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

/** Raised (as a defect, not a typed failure) once the retry budget is spent. */
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
 * SQL error. Non-busy failures and successes pass through unchanged, so the
 * error type is preserved. Once `retries` additional attempts are exhausted the
 * effect dies with `BusyRetriesExhaustedError` carrying the attempt count and
 * the last cause — the caller cannot silently continue on a stuck lock.
 */
export function withBusyRetry<A, E, R>(
  effect: Effect.Effect<A, E, R>,
  options: BusyRetryOptions = {},
): Effect.Effect<A, E, R> {
  const { retries, baseDelayMs, maxDelayMs } = resolveRetryConfig(options)
  const sleep = options.sleep ?? Effect.sleep

  const attempt = (retriesRemaining: number, attemptsMade: number): Effect.Effect<A, E, R> =>
    Effect.catch(effect, (error) => {
      if (!isBusySqlError(error)) return Effect.fail(error)
      if (retriesRemaining <= 0) {
        return Effect.die(new BusyRetriesExhaustedError({ attempts: attemptsMade, lastCause: error }))
      }
      const delay = Math.min(baseDelayMs * 2 ** (attemptsMade - 1), maxDelayMs)
      return Effect.andThen(sleep(delay), attempt(retriesRemaining - 1, attemptsMade + 1))
    })

  return attempt(retries, 1)
}

export { resolveBusyRetryConfig } from "./pragmas"
export type { BusyRetryConfig } from "./pragmas"

export * as Busy from "./busy"
