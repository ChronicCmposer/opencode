// Shared Effect SQLite adapter used by both the bun and node drivers.
//
// The two drivers differ only in how a statement is prepared and executed
// (`bun:sqlite` query/safeIntegers vs `node:sqlite` prepare/setReadBigInts) and
// in which optional capabilities they expose (bun can `serialize`, both can
// `loadExtension`). Everything else — the busy-retry choke point, the single
// connection semaphore, the transaction acquirer, result transforms, and the
// span attributes — is identical and lives here so the drivers stay thin.

import { Context, Effect, Fiber, Scope, Semaphore, Stream } from "effect"
import { identity } from "effect/Function"
import { Statement } from "effect/unstable/sql"
import { make, SafeIntegers, type SqlClient } from "effect/unstable/sql/SqlClient"
import type { Connection } from "effect/unstable/sql/SqlConnection"
import { classifySqliteError, SqlError, UnknownError } from "effect/unstable/sql/SqlError"
import { BusyExhaustedError, withBusyRetry } from "./busy"
import { resolveBusyRetryConfig } from "./pragmas"

const ATTR_DB_SYSTEM_NAME = "db.system.name"

/**
 * Raw statement execution against a native SQLite driver. `safeIntegers` mirrors
 * the fiber's `SafeIntegers` context; the driver maps it to its own API.
 */
export interface SqliteNativeRunner {
  readonly all: (query: string, params: ReadonlyArray<unknown>, safeIntegers: boolean) => Array<Record<string, unknown>>
  readonly values: (
    query: string,
    params: ReadonlyArray<unknown>,
    safeIntegers: boolean,
  ) => ReadonlyArray<ReadonlyArray<unknown>>
}

/** Optional driver capabilities surfaced on the connection and client. */
export interface SqliteCapabilities {
  readonly serialize?: () => Uint8Array
  readonly loadExtension?: (path: string) => void
}

export interface SqliteAdapterOptions {
  readonly spanAttributes?: Record<string, unknown>
  readonly transformResultNames?: (str: string) => string
  readonly transformQueryNames?: (str: string) => string
}

export interface SqliteAdapterConnection extends Connection {
  readonly export?: Effect.Effect<Uint8Array, SqlError>
  readonly loadExtension?: (path: string) => Effect.Effect<void, SqlError>
}

export type SqliteAdapterClient = SqlClient & {
  readonly export?: Effect.Effect<Uint8Array, SqlError>
  readonly loadExtension?: (path: string) => Effect.Effect<void, SqlError>
}

/**
 * Tracks whether the single native connection is currently inside an explicit
 * transaction. Busy retry is only safe outside one: replaying a statement inside
 * a deferred transaction keeps the same snapshot, so a `SQLITE_BUSY_SNAPSHOT`
 * write can never succeed. Inside a transaction the failure must escape and the
 * transaction control statement (not the body) is retried at its boundary.
 *
 * A top-level `SAVEPOINT` opens a transaction exactly like `BEGIN`; the
 * `RELEASE` that closes that savepoint ends it. Inside `BEGIN`, `RELEASE` only
 * closes savepoint scope, and `ROLLBACK TO` never ends the transaction. The
 * depth and origin are tracked so a `RELEASE` can never leave the connection
 * stuck believing it is still inside a transaction (F18).
 */
export interface TransactionState {
  inTransaction: boolean
  savepointDepth: number
  /** Whether the open transaction came from `BEGIN` rather than a top-level `SAVEPOINT`. */
  openedByBegin: boolean
}

const OUTSIDE_TRANSACTION: TransactionState = Object.freeze({
  inTransaction: false,
  savepointDepth: 0,
  openedByBegin: false,
})

/** Which transaction-control statement a query issues; `undefined` means ordinary DML. */
export type TransactionControl = "begin" | "commit" | "rollback" | "savepoint" | "release" | "rollbackTo"

/** Classify a transaction-control statement; `undefined` means ordinary DML. */
export function transactionControl(query: string): TransactionControl | undefined {
  const statement = query.trim().toLowerCase()
  if (/^begin\b/.test(statement)) return "begin"
  if (/^(?:commit|end)(?:\s+transaction)?$/.test(statement)) return "commit"
  // `ROLLBACK TO` must win over the plain `ROLLBACK` form.
  if (/^rollback\s+to\b/.test(statement)) return "rollbackTo"
  if (/^rollback(?:\s+transaction)?$/.test(statement)) return "rollback"
  if (/^savepoint\b/.test(statement)) return "savepoint"
  if (/^release\b/.test(statement)) return "release"
  return undefined
}

/**
 * Pure transition for one successfully executed transaction-control statement.
 * Keeping it pure makes the savepoint depth a value rather than a set of
 * scattered mutations, so `RELEASE` and `ROLLBACK TO` cannot disagree.
 */
export function nextTransactionState(state: TransactionState, control: TransactionControl): TransactionState {
  switch (control) {
    case "begin":
      return { inTransaction: true, savepointDepth: 0, openedByBegin: true }
    case "savepoint":
      return {
        inTransaction: true,
        savepointDepth: state.savepointDepth + 1,
        // A savepoint opened inside `BEGIN` belongs to that transaction; one
        // opened outside it is the transaction's origin.
        openedByBegin: state.inTransaction ? state.openedByBegin : false,
      }
    case "release": {
      const savepointDepth = Math.max(0, state.savepointDepth - 1)
      // Only releasing the outermost savepoint of a savepoint-opened
      // transaction leaves it; a release inside `BEGIN` keeps the transaction.
      if (savepointDepth === 0 && !state.openedByBegin) return { ...OUTSIDE_TRANSACTION }
      return { ...state, savepointDepth }
    }
    case "rollbackTo":
      // `ROLLBACK TO` rewinds to the savepoint but keeps it (and the
      // transaction) open.
      return state
    case "commit":
    case "rollback":
      return { ...OUTSIDE_TRANSACTION }
  }
}

/**
 * Durable writers must take the write lock when the transaction starts. A
 * deferred transaction reads first and later upgrades; when another connection
 * commits in between, SQLite returns `SQLITE_BUSY_SNAPSHOT` and no retry of the
 * individual write can observe a newer snapshot. `BEGIN IMMEDIATE` serializes at
 * the beginning instead.
 */
function beginImmediate(query: string): string {
  return transactionControl(query) === "begin" ? "BEGIN IMMEDIATE" : query
}

/**
 * A spent retry budget is reported as a typed `SqlError` at the driver boundary,
 * where the `Connection` interface requires that channel. The last busy cause
 * stays reachable through the `UnknownError` reason.
 */
function foldBusyExhaustion<A, E, R>(
  effect: Effect.Effect<A, E | BusyExhaustedError, R>,
): Effect.Effect<A, SqlError | E, R> {
  return effect.pipe(
    Effect.mapError((error): SqlError | E =>
      error instanceof BusyExhaustedError
        ? new SqlError({ reason: new UnknownError({ cause: error, message: error.message }) })
        : error,
    ),
  )
}

/**
 * The raw `Connection`: driver execution plus transaction-state tracking, with
 * no retry policy. Busy policy is layered on the acquirers below so a statement
 * inside a transaction is never replayed on its own.
 */
const makeConnection = (
  runner: SqliteNativeRunner,
  capabilities: SqliteCapabilities,
  state: TransactionState,
): SqliteAdapterConnection => {
  const note = (query: string): void => {
    const control = transactionControl(query)
    if (control === undefined) return
    Object.assign(state, nextTransactionState(state, control))
  }

  const run = (query: string, params: ReadonlyArray<unknown> = []) =>
    Effect.withFiber<Array<Record<string, unknown>>, SqlError>((fiber) => {
      try {
        return Effect.succeed(runner.all(query, params, Context.get(fiber.context, SafeIntegers)))
      } catch (cause) {
        return Effect.fail(
          new SqlError({
            reason: classifySqliteError(cause, { message: "Failed to execute statement", operation: "execute" }),
          }),
        )
      }
    }).pipe(Effect.tap(() => Effect.sync(() => note(query))))

  const runValues = (query: string, params: ReadonlyArray<unknown> = []) =>
    Effect.withFiber<ReadonlyArray<ReadonlyArray<unknown>>, SqlError>((fiber) => {
      try {
        return Effect.succeed(runner.values(query, params, Context.get(fiber.context, SafeIntegers)))
      } catch (cause) {
        return Effect.fail(
          new SqlError({
            reason: classifySqliteError(cause, { message: "Failed to execute statement", operation: "execute" }),
          }),
        )
      }
    }).pipe(Effect.tap(() => Effect.sync(() => note(query))))

  return identity<SqliteAdapterConnection>({
    execute(query, params, transformRows) {
      return transformRows ? Effect.map(run(query, params), transformRows) : run(query, params)
    },
    executeRaw(query, params) {
      return run(query, params)
    },
    executeValues(query, params) {
      return runValues(query, params)
    },
    executeUnprepared(query, params, transformRows) {
      return transformRows ? Effect.map(run(query, params), transformRows) : run(query, params)
    },
    executeStream() {
      return Stream.die("executeStream not implemented")
    },
    ...(capabilities.serialize
      ? {
          export: Effect.try({
            try: capabilities.serialize,
            catch: (cause) =>
              new SqlError({
                reason: classifySqliteError(cause, { message: "Failed to export database", operation: "export" }),
              }),
          }),
        }
      : {}),
    ...(capabilities.loadExtension
      ? {
          loadExtension: (path: string) =>
            Effect.try({
              try: () => capabilities.loadExtension!(path),
              catch: (cause) =>
                new SqlError({
                  reason: classifySqliteError(cause, { message: "Failed to load extension", operation: "loadExtension" }),
                }),
            }),
        }
      : {}),
  })
}

/**
 * Wrap a raw connection with bounded busy retry for statements issued *outside*
 * a transaction. The wrapper reads the shared state when each statement runs, so
 * once a transaction has begun it deliberately stops retrying: replaying one
 * statement inside the same deferred transaction cannot refresh its snapshot.
 */
const makeRetryingConnection = (
  connection: SqliteAdapterConnection,
  state: TransactionState,
): SqliteAdapterConnection => {
  const busy = resolveBusyRetryConfig()
  // `withBusyRetry` reports exhaustion as a typed `BusyExhaustedError`;
  // the `Connection` interface only admits `SqlError`, so fold it back into that
  // channel here (the last busy cause is preserved as the reason's cause).
  const retryBusy = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    foldBusyExhaustion(withBusyRetry(effect, busy))
  const retryOutsideTransaction = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.suspend(() => (state.inTransaction ? effect : retryBusy(effect)))

  return identity<SqliteAdapterConnection>({
    ...connection,
    execute: (query, params, transformRows) => retryOutsideTransaction(connection.execute(query, params, transformRows)),
    executeRaw: (query, params) => retryOutsideTransaction(connection.executeRaw(query, params)),
    executeValues: (query, params) => retryOutsideTransaction(connection.executeValues(query, params)),
    executeUnprepared: (query, params, transformRows) =>
      retryOutsideTransaction(connection.executeUnprepared(query, params, transformRows)),
  })
}

/**
 * The connection a transaction owns. It is never handed out per statement and
 * never retries ordinary DML: replaying a statement inside a transaction keeps
 * the same snapshot, so the body owner must replay the whole transaction. Only
 * the transaction control statements are retried, and each at its own boundary:
 * a busy `BEGIN IMMEDIATE` was not begun, a busy `COMMIT` was not committed (so
 * the body already ran and must not run again), and a busy `ROLLBACK` was not
 * rolled back. `beginImmediate` also upgrades a plain/deferred `BEGIN` so the
 * write lock is taken before the first read.
 */
const makeTransactionConnection = (connection: SqliteAdapterConnection): SqliteAdapterConnection => {
  const busy = resolveBusyRetryConfig()
  const retriesAtBoundary = (query: string): boolean => {
    const control = transactionControl(query)
    return control === "begin" || control === "commit" || control === "rollback"
  }
  const retryAtBoundary = <A, E, R>(query: string, effect: Effect.Effect<A, E, R>) =>
    retriesAtBoundary(query) ? foldBusyExhaustion(withBusyRetry(effect, busy)) : effect
  const executeUnprepared: SqliteAdapterConnection["executeUnprepared"] = (query, params, transformRows) =>
    retryAtBoundary(query, connection.executeUnprepared(beginImmediate(query), params, transformRows))

  return identity<SqliteAdapterConnection>({
    ...connection,
    executeUnprepared,
  })
}

/**
 * Build the SQL client for a native driver. The driver supplies only the raw
 * execution runner and its capabilities; the connection, semaphore, transaction
 * acquirer, and client wiring are shared.
 */
export const makeSqliteClient = (
  options: SqliteAdapterOptions,
  runner: SqliteNativeRunner,
  capabilities: SqliteCapabilities = {},
) =>
  Effect.gen(function* () {
    const compiler = Statement.makeCompilerSqlite(options.transformQueryNames)
    const transformRows = options.transformResultNames
      ? Statement.defaultTransforms(options.transformResultNames).array
      : undefined
    const state: TransactionState = { ...OUTSIDE_TRANSACTION }
    const connection = makeConnection(runner, capabilities, state)
    const retryingConnection = makeRetryingConnection(connection, state)
    const transactionConnection = makeTransactionConnection(connection)

    const semaphore = yield* Semaphore.make(1)
    const acquirer = semaphore.withPermits(1)(Effect.succeed(retryingConnection))
    const transactionAcquirer = Effect.uninterruptibleMask((restore) => {
      const fiber = Fiber.getCurrent()!
      const scope = Context.getUnsafe(fiber.context, Scope.Scope)
      return Effect.as(
        Effect.tap(restore(semaphore.take(1)), () => Scope.addFinalizer(scope, semaphore.release(1))),
        transactionConnection,
      )
    })

    return Object.assign(
      (yield* make({
        acquirer,
        compiler,
        transactionAcquirer,
        spanAttributes: [
          ...(options.spanAttributes ? Object.entries(options.spanAttributes) : []),
          [ATTR_DB_SYSTEM_NAME, "sqlite"],
        ],
        transformRows,
      })) as SqliteAdapterClient,
      {
        ...(capabilities.serialize ? { export: Effect.flatMap(acquirer, (_) => _.export!) } : {}),
        ...(capabilities.loadExtension
          ? { loadExtension: (path: string) => Effect.flatMap(acquirer, (_) => _.loadExtension!(path)) }
          : {}),
      },
    )
  })

export { ATTR_DB_SYSTEM_NAME }
