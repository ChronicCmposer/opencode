// Bounded whole-transaction busy retry for synchronous SQLite connections.
//
// A worker writes with `BEGIN IMMEDIATE`; when another connection holds the
// write lock SQLite reports `SQLITE_BUSY`/`SQLITE_LOCKED`. Retrying a single
// statement would leave a half-applied transaction, so the retry boundary is
// always the whole transaction body. Once the fixed budget is spent the failure
// is typed and terminal; a caller must never retry a non-idempotent body on an
// ambiguous (non-busy) commit failure.

import type { Connection } from "./connection"

export interface BusyRetryConfig {
  /** Additional attempts after the first one. */
  readonly retries: number
  readonly baseDelayMs: number
  readonly maxDelayMs: number
}

/** Fixed by the writer-removal contract (5 retries, 10ms x10, capped at 1s). */
export const DEFAULT_BUSY_RETRY_CONFIG: BusyRetryConfig = Object.freeze({
  retries: 5,
  baseDelayMs: 10,
  maxDelayMs: 1000,
})

const BUSY_CODES = ["SQLITE_BUSY", "SQLITE_LOCKED"] as const

function isBusyCode(code: string): boolean {
  return BUSY_CODES.some((base) => code === base || code.startsWith(`${base}_`))
}

/**
 * True when `error` is a lock/busy failure a whole-transaction retry may
 * recover from. Recognizes native `code`/`sqliteCode` values (including
 * extended forms) and the native message text. An injected fault may carry
 * either form.
 */
export function isBusyError(error: unknown): boolean {
  if (typeof error !== "object" || error === null) return false
  const candidate = error as {
    readonly code?: unknown
    readonly sqliteCode?: unknown
    readonly message?: unknown
  }
  if (typeof candidate.sqliteCode === "string" && isBusyCode(candidate.sqliteCode)) return true
  if (typeof candidate.code === "string" && isBusyCode(candidate.code)) return true
  const message = candidate.message
  return (
    typeof message === "string" &&
    (message.includes("SQLITE_BUSY") || message.includes("SQLITE_LOCKED") || message.includes("database is locked"))
  )
}

/** Typed terminal failure: the fixed retry budget was spent. */
export class BusyExhaustedError extends Error {
  readonly tag = "SQLITE_BUSY_EXHAUSTED"
  readonly attempts: number
  readonly lastCause: unknown

  constructor(attempts: number, lastCause: unknown) {
    super(`SQLite remained busy after ${attempts} attempt(s)`)
    this.name = "BusyExhaustedError"
    this.attempts = attempts
    this.lastCause = lastCause
  }
}

/** A failed ROLLBACK leaves the transaction state unknown; the connection is poisoned. */
export class TransactionRollbackError extends Error {
  readonly tag = "DB_CORE_ROLLBACK_FAILED"
  override readonly cause: unknown
  readonly rollbackCause: unknown

  constructor(cause: unknown, rollbackCause: unknown) {
    super("transaction rollback failed; connection is poisoned")
    this.name = "TransactionRollbackError"
    this.cause = cause
    this.rollbackCause = rollbackCause
  }
}

export class TransactionPoisonedError extends Error {
  readonly tag = "DB_CORE_CONNECTION_POISONED"
  constructor() {
    super("database connection is poisoned by a failed rollback")
    this.name = "TransactionPoisonedError"
  }
}

const poisoned = new WeakSet<object>()

function assertNotPoisoned(connection: Connection): void {
  if (poisoned.has(connection)) throw new TransactionPoisonedError()
}

function poison(connection: Connection): void {
  poisoned.add(connection)
}

export function isPoisoned(connection: Connection): boolean {
  return poisoned.has(connection)
}

function sleepSync(ms: number): void {
  if (ms <= 0) return
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms)
}

function resolveConfig(options: Partial<BusyRetryConfig>): BusyRetryConfig {
  return {
    retries: options.retries ?? DEFAULT_BUSY_RETRY_CONFIG.retries,
    baseDelayMs: options.baseDelayMs ?? DEFAULT_BUSY_RETRY_CONFIG.baseDelayMs,
    maxDelayMs: options.maxDelayMs ?? DEFAULT_BUSY_RETRY_CONFIG.maxDelayMs,
  }
}

function backoff(config: BusyRetryConfig, failedAttempt: number): number {
  return Math.min(config.baseDelayMs * 10 ** (failedAttempt - 1), config.maxDelayMs)
}

/**
 * Retry a synchronous call (for example the Bus `publish`) whenever it fails
 * with a busy error. The whole call is retried; non-busy failures pass through
 * unchanged and exhaustion is typed.
 */
export function retryBusySync<T>(work: () => T, options: Partial<BusyRetryConfig> = {}): T {
  const config = resolveConfig(options)
  for (let attempt = 1; ; attempt += 1) {
    try {
      return work()
    } catch (error) {
      if (!isBusyError(error)) throw error
      if (attempt > config.retries) throw new BusyExhaustedError(attempt, error)
      sleepSync(backoff(config, attempt))
    }
  }
}

function commit(connection: Connection, config: BusyRetryConfig): void {
  for (let attempt = 1; ; attempt += 1) {
    try {
      connection.exec("COMMIT")
      return
    } catch (error) {
      if (!isBusyError(error)) throw error
      // A busy COMMIT did not commit; the transaction is still open, so the
      // correct recovery is to re-issue COMMIT, never to re-run the body.
      if (attempt > config.retries) throw new BusyExhaustedError(attempt, error)
      sleepSync(backoff(config, attempt))
    }
  }
}

function rollback(connection: Connection, cause: unknown): void {
  try {
    connection.exec("ROLLBACK")
  } catch (rollbackCause) {
    poison(connection)
    throw new TransactionRollbackError(cause, rollbackCause)
  }
}

/**
 * Run `work` inside `BEGIN IMMEDIATE`. The whole body (begin, work, commit) is
 * retried on busy; a busy COMMIT is retried in place without replaying the
 * body. Exhaustion is `BusyExhaustedError`; a failed ROLLBACK poisons the
 * connection.
 */
export function inImmediateTransaction<T>(
  connection: Connection,
  work: (tx: Connection) => T,
  options: Partial<BusyRetryConfig> = {},
): T {
  const config = resolveConfig(options)
  assertNotPoisoned(connection)

  for (let attempt = 1; ; attempt += 1) {
    let begun = false
    try {
      connection.exec("BEGIN IMMEDIATE")
      begun = true
      const result = work(connection)
      commit(connection, config)
      return result
    } catch (error) {
      if (begun) rollback(connection, error)
      if (!isBusyError(error)) throw error
      if (attempt > config.retries) throw new BusyExhaustedError(attempt, error)
      sleepSync(backoff(config, attempt))
    }
  }
}

/** Readable alias used by the Bus/db-core contract. */
export function transaction<T>(
  connection: Connection,
  work: (tx: Connection) => T,
  options: Partial<BusyRetryConfig> = {},
): T {
  return inImmediateTransaction(connection, work, options)
}
