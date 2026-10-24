// o1-w1 — a writer-backed Effect `SqlClient`.
//
// The session worker holds no SQLite connection: every statement is forwarded to
// the hub-owned writer over the writer port (plan decision 24). This module turns
// that reduced writer surface into a genuine `effect/unstable/sql/SqlClient`, so
// `EffectDrizzleSqlite.make` builds a real Drizzle database inside the worker and
// `SessionPrompt` / `Session` / `EventV2` / `Todo` run unchanged.
//
// Two connections back the client:
//   * a stateless connection for ordinary statements (SELECT/INSERT/UPDATE/…);
//   * a stateful transaction connection handed out by `reserve`/`transactionAcquirer`.
//
// The driver routes every data statement through `connection.execute` /
// `connection.executeValues`, and only ever uses `executeUnprepared` on a
// *reserved* connection for transaction control. That is the seam where the
// writer's dedicated `begin`/`commit`/`rollback` primitives are driven, so
// transaction control never travels as raw SQL.

import { Effect, Layer, Ref, Stream } from "effect"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import type { Acquirer, Connection } from "effect/unstable/sql/SqlConnection"
import { classifySqliteError, SqlError } from "effect/unstable/sql/SqlError"
import * as Statement from "effect/unstable/sql/Statement"
import type { WorkerWriterClient } from "./writer-client"

const ATTR_DB_SYSTEM_NAME = "db.system.name"

/** The writer statement methods a connection dispatches to. */
type QueryMethod = "all" | "values" | "run" | "exec"

/**
 * The subset of the worker writer client a writer-backed `SqlClient` drives. The
 * transaction handle is an opaque string (`"tx-<n>"`) issued by `begin`, matching
 * the hub-owned writer client.
 */
export type WriterSqlHandle = Pick<WorkerWriterClient, "query" | "begin" | "txQuery" | "commit" | "rollback">

/** Map any writer rejection into the `SqlError` channel Drizzle callers expect. */
const sqlError =
  (operation: string) =>
  (cause: unknown): SqlError =>
    new SqlError({
      reason: classifySqliteError(cause, { message: "writer statement failed", operation }),
    })

const transactionNotOpen = (operation: string) =>
  sqlError(operation)(new Error("statement executed outside an open writer transaction"))

// The port is the trust boundary: the writer returns `unknown`, so the row shape
// is validated here (and a violation fails loud into the `SqlError` channel)
// rather than blindly cast into a shape the caller then trusts.
function asRows(value: unknown): ReadonlyArray<Record<string, unknown>> {
  if (!Array.isArray(value)) {
    throw new Error(`writer returned a non-array for a row query (got ${JSON.stringify(value)})`)
  }
  for (const row of value) {
    if (typeof row !== "object" || row === null || Array.isArray(row)) {
      throw new Error(`writer returned a non-object row (got ${JSON.stringify(row)})`)
    }
  }
  return value as ReadonlyArray<Record<string, unknown>>
}

function asValues(value: unknown): ReadonlyArray<ReadonlyArray<unknown>> {
  if (!Array.isArray(value)) {
    throw new Error(`writer returned a non-array for a value query (got ${JSON.stringify(value)})`)
  }
  for (const row of value) {
    if (!Array.isArray(row)) {
      throw new Error(`writer returned a non-array row for a value query (got ${JSON.stringify(row)})`)
    }
  }
  return value as ReadonlyArray<ReadonlyArray<unknown>>
}

const writerQuery = <A>(
  writer: WriterSqlHandle,
  method: QueryMethod,
  sql: string,
  params: ReadonlyArray<unknown>,
  parse: (value: unknown) => A,
): Effect.Effect<A, SqlError> =>
  Effect.tryPromise({
    try: async () => parse(await writer.query(method, sql, params)),
    catch: sqlError("execute"),
  })

const writerTxQuery = <A>(
  writer: WriterSqlHandle,
  handle: string,
  method: QueryMethod,
  sql: string,
  params: ReadonlyArray<unknown>,
  parse: (value: unknown) => A,
): Effect.Effect<A, SqlError> =>
  Effect.tryPromise({
    try: async () => parse(await writer.txQuery(handle, method, sql, params)),
    catch: sqlError("execute"),
  })

/** Ordinary statements: forwarded unchanged, nothing cached or buffered locally. */
const makeStatelessConnection = (writer: WriterSqlHandle): Connection => ({
  execute: (sql, params, transformRows) => {
    const rows = writerQuery(writer, "all", sql, params, asRows)
    return transformRows ? Effect.map(rows, transformRows) : rows
  },
  executeRaw: (sql, params) => writerQuery(writer, "all", sql, params, asRows),
  executeValues: (sql, params) => writerQuery(writer, "values", sql, params, asValues),
  executeUnprepared: (sql, params, transformRows) => {
    const rows = writerQuery(writer, "all", sql, params, asRows)
    return transformRows ? Effect.map(rows, transformRows) : rows
  },
  executeStream: () => Stream.die("writer SqlClient does not support streaming"),
})

/**
 * The transaction connection: pins one writer transaction and routes every data
 * statement through `txQuery`. `executeUnprepared` is only reached for
 * transaction control, so begin/commit/rollback map onto the writer primitives
 * while savepoint forms (nested transactions) pass through as `txQuery`.
 */
const makeTransactionConnection = (writer: WriterSqlHandle, handle: Ref.Ref<string | undefined>): Connection => {
  const requireHandle = Effect.flatMap(Ref.get(handle), (tx) =>
    tx === undefined ? Effect.fail(transactionNotOpen("execute")) : Effect.succeed(tx),
  )

  const txRows = (method: QueryMethod, sql: string, params: ReadonlyArray<unknown>) =>
    Effect.flatMap(requireHandle, (tx) => writerTxQuery(writer, tx, method, sql, params, asRows))

  const txValues = (sql: string, params: ReadonlyArray<unknown>) =>
    Effect.flatMap(requireHandle, (tx) => writerTxQuery(writer, tx, "values", sql, params, asValues))

  return {
    execute: (sql, params, transformRows) => {
      const rows = txRows("all", sql, params)
      return transformRows ? Effect.map(rows, transformRows) : rows
    },
    executeRaw: (sql, params) => txRows("all", sql, params),
    executeValues: (sql, params) => txValues(sql, params),
    executeUnprepared: (sql, params, transformRows) =>
      Effect.gen(function* () {
        const tx = yield* Ref.get(handle)
        const statement = sql.trim().toLowerCase()

        if (statement.startsWith("begin")) {
          if (tx !== undefined) {
            return yield* Effect.fail(sqlError("begin")(new Error("writer transaction is already open")))
          }
          const opened = yield* Effect.tryPromise({ try: () => writer.begin(), catch: sqlError("begin") })
          yield* Ref.set(handle, opened)
          return []
        }

        if (statement.startsWith("commit")) {
          if (tx === undefined) return yield* Effect.fail(transactionNotOpen("commit"))
          yield* Effect.tryPromise({ try: () => writer.commit(tx), catch: sqlError("commit") })
          yield* Ref.set(handle, undefined)
          return []
        }

        if (statement.startsWith("rollback") && !statement.startsWith("rollback to savepoint")) {
          if (tx === undefined) return yield* Effect.fail(transactionNotOpen("rollback"))
          yield* Effect.tryPromise({ try: () => writer.rollback(tx), catch: sqlError("rollback") })
          yield* Ref.set(handle, undefined)
          return []
        }

        const rows = yield* txRows("all", sql, params)
        return transformRows === undefined ? rows : transformRows(rows)
      }),
    executeStream: () => Stream.die("writer SqlClient does not support streaming"),
  }
}

/** Reserve a writer transaction for the lifetime of the surrounding scope. */
const acquireTransactionConnection = (writer: WriterSqlHandle): Acquirer =>
  Effect.gen(function* () {
    const handle = yield* Ref.make<string | undefined>(undefined)
    yield* Effect.addFinalizer(() =>
      Ref.get(handle).pipe(
        Effect.flatMap((tx) =>
          tx === undefined
            ? Effect.void
            : Effect.tryPromise({ try: () => writer.rollback(tx), catch: () => undefined }).pipe(Effect.ignore),
        ),
      ),
    )
    return makeTransactionConnection(writer, handle)
  })

/**
 * Build a `SqlClient` whose statements execute on the hub-owned writer. The
 * resulting client satisfies the exact surface `EffectDrizzleSqlite.make`
 * consumes, so it type-checks as the flag-OFF database without a cast.
 */
export function writerSqlClient(
  writer: WriterSqlHandle,
): Effect.Effect<SqlClient.SqlClient, never, Reactivity.Reactivity> {
  return SqlClient.make({
    acquirer: Effect.succeed(makeStatelessConnection(writer)),
    compiler: Statement.makeCompilerSqlite(),
    transactionAcquirer: acquireTransactionConnection(writer),
    spanAttributes: [[ATTR_DB_SYSTEM_NAME, "sqlite"]],
  })
}

/** `writerSqlClient` as a layer, with the `Reactivity` requirement satisfied. */
export function writerSqlClientLayer(writer: WriterSqlHandle): Layer.Layer<SqlClient.SqlClient> {
  return Layer.effect(SqlClient.SqlClient, writerSqlClient(writer)).pipe(Layer.provide(Reactivity.layer))
}

export * as WriterSqlClient from "./writer-sql-client"
