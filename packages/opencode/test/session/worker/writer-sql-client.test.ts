// o1-w1 — writer-backed Effect `SqlClient` tests.
//
// The writer SqlClient must behave exactly like the flag-OFF driver: every
// statement is forwarded to the writer handle, and interactive transactions map
// onto the writer's `begin` / `commit` / `rollback` primitives. The reference is
// a real in-memory SQLite `SqlClient` so the comparison is apples-to-apples.

import { Database } from "bun:sqlite"
import { describe, expect, test } from "bun:test"
import { Effect, Exit, Stream } from "effect"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import type { Connection } from "effect/unstable/sql/SqlConnection"
import { classifySqliteError, SqlError } from "effect/unstable/sql/SqlError"
import * as Statement from "effect/unstable/sql/Statement"
import { sql } from "drizzle-orm"
import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core"
import { Database as CoreDatabase } from "@opencode-ai/core/database/database"
import { EffectDrizzleSqlite } from "../../../../effect-drizzle-sqlite/src/index"
import { writerSqlClient, type WriterSqlHandle } from "@/session/worker/writer-sql-client"

const widget = sqliteTable("widget", {
  id: integer("id").primaryKey(),
  name: text("name").notNull(),
})

/**
 * A real in-memory SQLite `SqlClient`, mirroring the flag-OFF Bun driver. Used
 * both as the writer's backing store and as the reference the writer client is
 * compared against.
 */
function realSqliteClient(native: Database): Effect.Effect<SqlClient.SqlClient, never, Reactivity.Reactivity> {
  const rows = (query: string, params: ReadonlyArray<unknown> = []) =>
    Effect.try({
      try: () => native.query(query).all(...(params as never[])) as Array<Record<string, unknown>>,
      catch: (cause) =>
        new SqlError({
          reason: classifySqliteError(cause, { message: "reference sqlite failed", operation: "execute" }),
        }),
    })

  const values = (query: string, params: ReadonlyArray<unknown> = []) =>
    Effect.try({
      try: () => native.query(query).values(...(params as never[])) as Array<Array<unknown>>,
      catch: (cause) =>
        new SqlError({
          reason: classifySqliteError(cause, { message: "reference sqlite failed", operation: "execute" }),
        }),
    })

  const connection: Connection = {
    execute: (query, params, transformRows) =>
      transformRows ? Effect.map(rows(query, params), transformRows) : rows(query, params),
    executeRaw: (query, params) => rows(query, params),
    executeValues: (query, params) => values(query, params),
    executeUnprepared: (query, params, transformRows) =>
      transformRows ? Effect.map(rows(query, params), transformRows) : rows(query, params),
    executeStream: () => Stream.die("reference sqlite streaming is not implemented"),
  }

  return SqlClient.make({
    acquirer: Effect.succeed(connection),
    compiler: Statement.makeCompilerSqlite(),
    transactionAcquirer: Effect.succeed(connection),
    spanAttributes: [["db.system.name", "sqlite"]],
  })
}

/** A writer handle backed by the real SQLite client, so transaction control is real. */
function writerOver(reference: SqlClient.SqlClient): WriterSqlHandle {
  let nextTxId = 1
  const runQuery = (method: string, sql: string, params?: readonly unknown[]) => {
    const statement = reference.unsafe(sql, params)
    return Effect.runPromise(method === "values" ? statement.values : statement.withoutTransform)
  }
  return {
    query: (method, sql, params) => runQuery(method, sql, params),
    begin: async () => {
      await Effect.runPromise(reference.unsafe("BEGIN IMMEDIATE").withoutTransform)
      return `tx-${nextTxId++}`
    },
    txQuery: (_handle, method, sql, params) => runQuery(method, sql, params),
    commit: async () => {
      await Effect.runPromise(reference.unsafe("COMMIT").withoutTransform)
    },
    rollback: async () => {
      await Effect.runPromise(reference.unsafe("ROLLBACK").withoutTransform)
    },
  }
}

const failingWriter = (): WriterSqlHandle => {
  const explode = async (): Promise<never> => {
    throw new Error("writer exploded")
  }
  return {
    query: explode,
    begin: explode,
    txQuery: explode,
    commit: explode,
    rollback: explode,
  }
}

/** Builds the Drizzle database the same way the session worker layer will. */
const drizzleOver = (client: SqlClient.SqlClient) =>
  EffectDrizzleSqlite.makeWithDefaults().pipe(Effect.provideService(SqlClient.SqlClient, client))

const run = <A, E>(effect: Effect.Effect<A, E, Reactivity.Reactivity>) =>
  Effect.runPromise(effect.pipe(Effect.provide(Reactivity.layer)))

describe("writerSqlClient", () => {
  test("SELECT rows match a real in-memory SQLite SqlClient", async () => {
    const native = new Database(":memory:")
    try {
      const result = await run(
        Effect.gen(function* () {
          const reference = yield* realSqliteClient(native)
          yield* reference.unsafe("CREATE TABLE widget (id INTEGER PRIMARY KEY, name TEXT NOT NULL)").withoutTransform
          yield* reference.unsafe("INSERT INTO widget (id, name) VALUES (?, ?)", [1, "alpha"]).withoutTransform
          yield* reference.unsafe("INSERT INTO widget (id, name) VALUES (?, ?)", [2, "beta"]).withoutTransform

          const client = yield* writerSqlClient(writerOver(reference))
          const expected = yield* reference
            .unsafe<Record<string, unknown>>("SELECT id, name FROM widget ORDER BY id")
            .withoutTransform
          const actual = yield* client
            .unsafe<Record<string, unknown>>("SELECT id, name FROM widget ORDER BY id")
            .withoutTransform
          return { expected, actual }
        }),
      )

      expect(result.actual).toEqual(result.expected)
      expect(result.actual).toEqual([
        { id: 1, name: "alpha" },
        { id: 2, name: "beta" },
      ])
    } finally {
      native.close()
    }
  })

  test("parameterized statements bind through the writer", async () => {
    const native = new Database(":memory:")
    try {
      const rows = await run(
        Effect.gen(function* () {
          const reference = yield* realSqliteClient(native)
          yield* reference.unsafe("CREATE TABLE widget (id INTEGER PRIMARY KEY, name TEXT NOT NULL)").withoutTransform
          yield* reference.unsafe("INSERT INTO widget (id, name) VALUES (?, ?)", [7, "gamma"]).withoutTransform

          const client = yield* writerSqlClient(writerOver(reference))
          return yield* client
            .unsafe<Record<string, unknown>>("SELECT name FROM widget WHERE id = ?", [7])
            .withoutTransform
        }),
      )

      expect(rows).toEqual([{ name: "gamma" }])
    } finally {
      native.close()
    }
  })

  test("interactive transaction commits through begin/commit", async () => {
    const native = new Database(":memory:")
    try {
      const rows = await run(
        Effect.gen(function* () {
          const reference = yield* realSqliteClient(native)
          const client = yield* writerSqlClient(writerOver(reference))
          const db = yield* drizzleOver(client)

          yield* db.run(sql`CREATE TABLE widget (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`)
          yield* db.transaction((tx) => tx.insert(widget).values({ id: 1, name: "alpha" }).run())
          return yield* db.select().from(widget).all()
        }),
      )

      expect(rows).toEqual([{ id: 1, name: "alpha" }])
    } finally {
      native.close()
    }
  })

  test("interactive transaction rolls back atomically on failure", async () => {
    const native = new Database(":memory:")
    try {
      const result = await run(
        Effect.gen(function* () {
          const reference = yield* realSqliteClient(native)
          const client = yield* writerSqlClient(writerOver(reference))
          const db = yield* drizzleOver(client)

          yield* db.run(sql`CREATE TABLE widget (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`)
          const exit = yield* db
            .transaction((tx) =>
              Effect.gen(function* () {
                yield* tx.insert(widget).values({ id: 2, name: "beta" }).run()
                yield* Effect.fail(new Error("boom"))
              }),
            )
            .pipe(Effect.exit)
          const rows = yield* db.select().from(widget).all()
          return { exit, rows }
        }),
      )

      expect(Exit.isFailure(result.exit)).toBe(true)
      expect(result.rows).toEqual([])
    } finally {
      native.close()
    }
  })

  test("writer failures surface as typed SqlError", async () => {
    const error = await run(
      Effect.gen(function* () {
        const client = yield* writerSqlClient(failingWriter())
        return yield* client.unsafe("SELECT 1").withoutTransform.pipe(Effect.flip)
      }),
    )

    expect(error).toBeInstanceOf(SqlError)
  })

  test("nested transactions savepoint one pinned writer transaction", async () => {
    const native = new Database(":memory:")
    try {
      const result = await run(
        Effect.gen(function* () {
          const reference = yield* realSqliteClient(native)
          let begins = 0
          const base = writerOver(reference)
          const tracking: WriterSqlHandle = {
            ...base,
            begin: async () => {
              begins++
              return base.begin()
            },
          }
          const client = yield* writerSqlClient(tracking)
          const db = yield* drizzleOver(client)

          yield* db.run(sql`CREATE TABLE widget (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`)
          yield* db.transaction((tx) =>
            Effect.gen(function* () {
              yield* tx.insert(widget).values({ id: 1, name: "outer" }).run()
              yield* db.transaction((inner) => inner.insert(widget).values({ id: 2, name: "inner" }).run())
            }),
          )
          const rows = yield* db.select().from(widget).orderBy(widget.id).all()
          return { begins, rows }
        }),
      )

      expect(result.begins).toBe(1)
      expect(result.rows).toEqual([
        { id: 1, name: "outer" },
        { id: 2, name: "inner" },
      ])
    } finally {
      native.close()
    }
  })

  test("SqlClient.withTransaction drives the writer primitives too", async () => {
    const native = new Database(":memory:")
    try {
      const rows = await run(
        Effect.gen(function* () {
          const reference = yield* realSqliteClient(native)
          yield* reference.unsafe("CREATE TABLE widget (id INTEGER PRIMARY KEY, name TEXT NOT NULL)").withoutTransform
          const client = yield* writerSqlClient(writerOver(reference))

          yield* client.withTransaction(
            client.unsafe("INSERT INTO widget (id, name) VALUES (1, 'alpha')").withoutTransform,
          )
          return yield* client.unsafe<Record<string, unknown>>("SELECT id, name FROM widget").withoutTransform
        }),
      )

      expect(rows).toEqual([{ id: 1, name: "alpha" }])
    } finally {
      native.close()
    }
  })

  test("the driver database satisfies Database.Interface without a cast", async () => {
    const native = new Database(":memory:")
    try {
      const same = await run(
        Effect.gen(function* () {
          const reference = yield* realSqliteClient(native)
          const client = yield* writerSqlClient(writerOver(reference))
          const db = yield* drizzleOver(client)
          const database: CoreDatabase.Interface = { db }
          return database.db === db
        }),
      )

      expect(same).toBe(true)
    } finally {
      native.close()
    }
  })
})
