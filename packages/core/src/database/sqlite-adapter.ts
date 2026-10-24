// Shared Effect SQLite adapter used by both the bun and node drivers.
//
// The two drivers differ only in how a statement is prepared and executed
// (`bun:sqlite` query/safeIntegers vs `node:sqlite` prepare/setReadBigInts) and
// in which optional capabilities they expose (bun can `serialize`, both can
// `loadExtension`). Everything else — the busy-retry choke point, the single
// connection semaphore, the transaction acquirer, result transforms, and the
// span attributes — is identical and lives here so the drivers stay thin.

import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import { identity } from "effect/Function"
import * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as Client from "effect/unstable/sql/SqlClient"
import type { Connection } from "effect/unstable/sql/SqlConnection"
import { classifySqliteError, SqlError, UnknownError } from "effect/unstable/sql/SqlError"
import * as Statement from "effect/unstable/sql/Statement"
import { BusyRetriesExhaustedError, withBusyRetry } from "./busy"
import { resolveBusyRetryConfig } from "./pragmas"

const ATTR_DB_SYSTEM_NAME = "db.system.name"

/**
 * Raw statement execution against a native SQLite driver. `safeIntegers` mirrors
 * the fiber's `Client.SafeIntegers` context; the driver maps it to its own API.
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

export type SqliteAdapterClient = Client.SqlClient & {
  readonly export?: Effect.Effect<Uint8Array, SqlError>
  readonly loadExtension?: (path: string) => Effect.Effect<void, SqlError>
}

/** The `Connection` every statement and Drizzle transaction statement flows through. */
const makeConnection = (runner: SqliteNativeRunner, capabilities: SqliteCapabilities): SqliteAdapterConnection => {
  const run = (query: string, params: ReadonlyArray<unknown> = []) =>
    Effect.withFiber<Array<Record<string, unknown>>, SqlError>((fiber) => {
      try {
        return Effect.succeed(runner.all(query, params, Context.get(fiber.context, Client.SafeIntegers)))
      } catch (cause) {
        return Effect.fail(
          new SqlError({
            reason: classifySqliteError(cause, { message: "Failed to execute statement", operation: "execute" }),
          }),
        )
      }
    })

  const runValues = (query: string, params: ReadonlyArray<unknown> = []) =>
    Effect.withFiber<ReadonlyArray<ReadonlyArray<unknown>>, SqlError>((fiber) => {
      try {
        return Effect.succeed(runner.values(query, params, Context.get(fiber.context, Client.SafeIntegers)))
      } catch (cause) {
        return Effect.fail(
          new SqlError({
            reason: classifySqliteError(cause, { message: "Failed to execute statement", operation: "execute" }),
          }),
        )
      }
    })

  // The `Connection` is the single choke point every statement and every Drizzle
  // transaction statement flows through, so busy-retry lives here.
  const busy = resolveBusyRetryConfig()
  // `withBusyRetry` reports exhaustion as a typed `BusyRetriesExhaustedError`;
  // the `Connection` interface only admits `SqlError`, so fold it back into that
  // channel here (the last busy cause is preserved as the reason's cause).
  const retryBusy = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    withBusyRetry(effect, busy).pipe(
      Effect.mapError((error) =>
        error instanceof BusyRetriesExhaustedError
          ? new SqlError({ reason: new UnknownError({ cause: error, message: error.message }) })
          : error,
      ),
    )

  return identity<SqliteAdapterConnection>({
    execute(query, params, transformRows) {
      return retryBusy(transformRows ? Effect.map(run(query, params), transformRows) : run(query, params))
    },
    executeRaw(query, params) {
      return retryBusy(run(query, params))
    },
    executeValues(query, params) {
      return retryBusy(runValues(query, params))
    },
    executeUnprepared(query, params, transformRows) {
      return this.execute(query, params, transformRows)
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
    const connection = makeConnection(runner, capabilities)

    const semaphore = yield* Semaphore.make(1)
    const acquirer = semaphore.withPermits(1)(Effect.succeed(connection))
    const transactionAcquirer = Effect.uninterruptibleMask((restore) => {
      const fiber = Fiber.getCurrent()!
      const scope = Context.getUnsafe(fiber.context, Scope.Scope)
      return Effect.as(
        Effect.tap(restore(semaphore.take(1)), () => Scope.addFinalizer(scope, semaphore.release(1))),
        connection,
      )
    })

    return Object.assign(
      (yield* Client.make({
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
