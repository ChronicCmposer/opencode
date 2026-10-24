import { DatabaseSync, type SQLInputValue } from "node:sqlite"
import { drizzle } from "drizzle-orm/node-sqlite"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as Client from "effect/unstable/sql/SqlClient"
import type { SqlError } from "effect/unstable/sql/SqlError"
import { makeSqliteClient } from "./sqlite-adapter"
import { Sqlite } from "./sqlite"

const TypeId = "~@opencode-ai/core/database/SqliteNode" as const
type TypeId = typeof TypeId

interface SqliteClient extends Client.SqlClient {
  readonly [TypeId]: TypeId
  readonly config: Config
  readonly loadExtension: (path: string) => Effect.Effect<void, SqlError>
  readonly updateValues: never
}

interface Config {
  readonly filename: string
  readonly readonly?: boolean
  readonly create?: boolean
  readonly readwrite?: boolean
  readonly disableWAL?: boolean
  readonly timeout?: number
  readonly allowExtension?: boolean
  readonly spanAttributes?: Record<string, unknown>
  readonly transformResultNames?: (str: string) => string
  readonly transformQueryNames?: (str: string) => string
}

const make = (options: Config) =>
  Effect.gen(function* () {
    const native = (yield* Sqlite.Native) as DatabaseSync

    const client = yield* makeSqliteClient(
      {
        spanAttributes: options.spanAttributes,
        transformResultNames: options.transformResultNames,
        transformQueryNames: options.transformQueryNames,
      },
      {
        all: (query, params, safeIntegers) => {
          const statement = native.prepare(query)
          statement.setReadBigInts(safeIntegers)
          return statement.all(...(params as SQLInputValue[])) as Array<Record<string, unknown>>
        },
        values: (query, params, safeIntegers) => {
          const statement = native.prepare(query)
          statement.setReadBigInts(safeIntegers)
          statement.setReturnArrays(true)
          return statement.all(...(params as SQLInputValue[])) as unknown as ReadonlyArray<ReadonlyArray<unknown>>
        },
      },
      {
        loadExtension: (path) => native.loadExtension(path),
      },
    )

    return Object.assign(client, { [TypeId]: TypeId, config: options }) as SqliteClient
  })

const nativeLayer = (config: Config) =>
  Layer.effect(
    Sqlite.Native,
    Effect.gen(function* () {
      const native = new DatabaseSync(config.filename, {
        readOnly: config.readonly,
        timeout: config.timeout,
        allowExtension: config.allowExtension,
        enableForeignKeyConstraints: true,
        open: true,
      })
      yield* Effect.addFinalizer(() => Effect.sync(() => native.close()))
      if (config.disableWAL !== true && config.readonly !== true) native.exec("PRAGMA journal_mode = WAL;")
      return native
    }),
  )

const sqliteLayer = (config: Config) => Layer.effect(Client.SqlClient, make(config))

const drizzleLayer = Layer.effect(
  Sqlite.Drizzle,
  Effect.gen(function* () {
    return drizzle({ client: (yield* Sqlite.Native) as DatabaseSync }) as unknown as Sqlite.DrizzleClient
  }),
)

export const layer = (config: Config) => {
  const native = nativeLayer(config)
  return Layer.merge(native, Layer.merge(sqliteLayer(config), drizzleLayer).pipe(Layer.provide(native))).pipe(
    Layer.provide(Reactivity.layer),
  )
}
