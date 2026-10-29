// Bounded whole-transaction busy retry for synchronous SQLite connections.
//
// A worker writes with `BEGIN IMMEDIATE`; when another connection holds the
// write lock SQLite reports `SQLITE_BUSY`/`SQLITE_LOCKED`. Retrying a single
// statement would leave a half-applied transaction, so the retry boundary is
// always the whole transaction body. Once the fixed budget is spent the failure
// is typed and terminal; a caller must never retry a non-idempotent body on an
// ambiguous (non-busy) commit failure.

import { Schema } from "effect"
import type { Connection } from "./connection"
import { BusyExhaustedError, busyDelayMs, isBusySqlError } from "./busy"
import type { BusyRetryConfig } from "./pragmas"

export { BusyExhaustedError }
export type { BusyRetryConfig }

/**
 * The transaction helpers only ever call `exec`; taking this structural slice
 * (rather than the full native `Connection`) also lets the core DurableLog run over a
 * test double without pretending to own a real handle.
 */
export interface TransactionConnection {
  exec(sql: string): void
}

/** Default budget (5 retries, 10ms growing tenfold, capped at 1s). */
export const DEFAULT_BUSY_RETRY_CONFIG: BusyRetryConfig = Object.freeze({
  retries: 5,
  baseDelayMs: 10,
  maxDelayMs: 1000,
})

/**
 * Executor transactions retry at most three times. Each attempt waits the
 * executor's 1000ms `busy_timeout`, so the whole train is bounded near four
 * seconds instead of six 5s waits filling the 30s request deadline.
 */
export const EXECUTOR_BUSY_RETRY_CONFIG: BusyRetryConfig = Object.freeze({
  retries: 3,
  baseDelayMs: 10,
  maxDelayMs: 250,
})

/**
 * True when `error` is a lock/busy failure a whole-transaction retry may
 * recover from. This is the one classifier: the Effect-facing adapter and the
 * synchronous transaction helpers both use it, so a driver shape can never be
 * recognised by one path and missed by the other.
 */
export const isBusyError: (error: unknown) => boolean = isBusySqlError

/** A failed ROLLBACK leaves the transaction state unknown; the connection is poisoned. */
export class TransactionRollbackError extends Schema.TaggedErrorClass<TransactionRollbackError>(
  "TransactionRollbackError",
)("Database.TransactionRollback", {
  cause: Schema.optional(Schema.Defect()),
  rollbackCause: Schema.Unknown,
}) {
  readonly tag = "DB_CORE_ROLLBACK_FAILED"

  constructor(
    override readonly cause: unknown,
    override readonly rollbackCause: unknown,
  ) {
    super({ cause, rollbackCause })
    this.name = "TransactionRollbackError"
  }

  override get message() {
    return "transaction rollback failed; connection is poisoned"
  }
}

export class TransactionPoisonedError extends Schema.TaggedErrorClass<TransactionPoisonedError>(
  "TransactionPoisonedError",
)("Database.TransactionPoisoned", {}) {
  readonly tag = "DB_CORE_CONNECTION_POISONED"

  constructor() {
    super({})
    this.name = "TransactionPoisonedError"
  }

  override get message() {
    return "database connection is poisoned by a failed rollback"
  }
}

const poisoned = new WeakSet<object>()

function assertNotPoisoned(connection: object): void {
  if (poisoned.has(connection)) throw new TransactionPoisonedError()
}

function poison(connection: object): void {
  poisoned.add(connection)
}

export function isPoisoned(connection: object): boolean {
  return poisoned.has(connection)
}

/**
 * Async backoff used by the Effect-facing path. `Atomics.wait` is deliberately
 * NOT used anywhere in this module: it blocks the isolate, so a worker retrying
 * a busy write could not service cancellation or stream progress.
 */
function sleep(ms: number): Promise<void> {
  if (ms <= 0) return Promise.resolve()
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms)
    timer.unref?.()
  })
}

function resolveConfig(options: Partial<BusyRetryConfig>): BusyRetryConfig {
  return {
    retries: options.retries ?? DEFAULT_BUSY_RETRY_CONFIG.retries,
    baseDelayMs: options.baseDelayMs ?? DEFAULT_BUSY_RETRY_CONFIG.baseDelayMs,
    maxDelayMs: options.maxDelayMs ?? DEFAULT_BUSY_RETRY_CONFIG.maxDelayMs,
  }
}

/** How one failed attempt waits before the next; omitted means retry at once. */
type BusyWait = (failedAttempt: number) => void | Promise<void>

const retryImmediately: BusyWait = () => {}

/**
 * The one retry loop. `work` is retried while it fails with a busy error;
 * session-control, validation, and other non-busy failures pass through
 * unchanged, and a spent budget raises the typed `BusyExhaustedError`. `wait`
 * is synchronous for the blocking paths (the driver's `busy_timeout` already
 * waited) and returns a Promise for the yielding worker path.
 */
function retryBusy<T>(work: () => T, config: BusyRetryConfig, wait: BusyWait): T | Promise<T> {
  const attempt = (attemptsMade: number): T | Promise<T> => {
    try {
      return work()
    } catch (error) {
      if (!isBusyError(error)) throw error
      if (attemptsMade > config.retries) throw new BusyExhaustedError(attemptsMade, error)
      const waited = wait(attemptsMade)
      return waited instanceof Promise ? waited.then(() => attempt(attemptsMade + 1)) : attempt(attemptsMade + 1)
    }
  }
  return attempt(1)
}

/** Synchronous retry: the driver's blocking `busy_timeout` already waited. */
function retryBusyNow<T>(work: () => T, config: BusyRetryConfig): T {
  return retryBusy(work, config, retryImmediately) as T
}

/**
 * Yielding retry used by the worker path; the body itself stays synchronous.
 * Async so a non-busy (or zero-retry) failure is always a rejected Promise,
 * never a synchronous throw: callers may use either `await` or `.catch`.
 */
async function retryBusyLater<T>(work: () => T, config: BusyRetryConfig): Promise<T> {
  return retryBusy(work, config, (failedAttempt) => sleep(busyDelayMs(config, failedAttempt)))
}

/**
 * Retry a synchronous call (for example the DurableLog `publish`) whenever it fails
 * with a busy error. The whole call is retried; non-busy failures pass through
 * unchanged and exhaustion is typed.
 */
export function retryBusySync<T>(work: () => T, options: Partial<BusyRetryConfig> = {}): T {
  return retryBusyNow(work, resolveConfig(options))
}

function commit(connection: TransactionConnection, config: BusyRetryConfig): void {
  // A busy COMMIT did not commit; the transaction is still open, so the correct
  // recovery is to re-issue COMMIT, never to re-run the body.
  retryBusyNow(() => connection.exec("COMMIT"), config)
}

function rollback(connection: TransactionConnection, cause: unknown, config: BusyRetryConfig): void {
  // A busy ROLLBACK did not roll back; the transaction is still open and
  // recoverable, so re-issue ROLLBACK in place with the same bounded budget as
  // COMMIT. Only an exhausted/failed rollback poisons the connection.
  try {
    retryBusyNow(() => connection.exec("ROLLBACK"), config)
  } catch (rollbackCause) {
    poison(connection)
    throw new TransactionRollbackError(cause, rollbackCause)
  }
}

function transactionBody<T, C extends TransactionConnection>(
  connection: C,
  work: (tx: C) => T,
  config: BusyRetryConfig,
): () => T {
  return () => {
    let begun = false
    try {
      connection.exec("BEGIN IMMEDIATE")
      begun = true
      const result = work(connection)
      commit(connection, config)
      return result
    } catch (error) {
      if (begun) rollback(connection, error, config)
      throw error
    }
  }
}

/**
 * Run `work` inside `BEGIN IMMEDIATE`. The whole body (begin, work, commit) is
 * retried on busy; a busy COMMIT is retried in place without replaying the
 * body. Exhaustion is `BusyExhaustedError`; a failed ROLLBACK poisons the
 * connection.
 */
export function inImmediateTransaction<T, C extends TransactionConnection = Connection>(
  connection: C,
  work: (tx: C) => T,
  options: Partial<BusyRetryConfig> = {},
): T {
  assertNotPoisoned(connection)
  const config = resolveConfig(options)
  return retryBusyNow(transactionBody(connection, work, config), config)
}

/**
 * Async twin of {@link retryBusySync}: the body is still synchronous (so a
 * transaction cannot interleave), but the backoff between attempts yields to
 * the event loop. This is the path a worker DurableLog uses so cancellation and
 * streaming progress keep running while SQLite is busy.
 */
export function retryBusyAsync<T>(work: () => T, options: Partial<BusyRetryConfig> = {}): Promise<T> {
  return retryBusyLater(work, resolveConfig(options))
}

/**
 * Async twin of {@link inImmediateTransaction}: the transaction body is
 * synchronous and atomic; only the backoff between attempts yields.
 */
export async function inImmediateTransactionAsync<T, C extends TransactionConnection = Connection>(
  connection: C,
  work: (tx: C) => T,
  options: Partial<BusyRetryConfig> = {},
): Promise<T> {
  assertNotPoisoned(connection)
  const config = resolveConfig(options)
  return retryBusyLater(transactionBody(connection, work, config), config)
}

/** Readable alias used by the DurableLog/db-core contract. */
export function transaction<T, C extends TransactionConnection = Connection>(
  connection: C,
  work: (tx: C) => T,
  options: Partial<BusyRetryConfig> = {},
): T {
  return inImmediateTransaction(connection, work, options)
}
