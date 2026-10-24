import { Database } from "bun:sqlite"
import { drizzle } from "drizzle-orm/bun-sqlite"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as Client from "effect/unstable/sql/SqlClient"
import type { SqlError } from "effect/unstable/sql/SqlError"
import { makeSqliteClient } from "./sqlite-adapter"
import { Sqlite } from "./sqlite"

const TypeId = "~@opencode-ai/core/database/SqliteBun" as const
type TypeId = typeof TypeId

interface SqliteClient extends Client.SqlClient {
  readonly [TypeId]: TypeId
  readonly config: Config
  readonly export: Effect.Effect<Uint8Array, SqlError>
  readonly loadExtension: (path: string) => Effect.Effect<void, SqlError>
  readonly updateValues: never
}

interface Config {
  readonly filename: string
  readonly readonly?: boolean
  readonly create?: boolean
  readonly readwrite?: boolean
  readonly disableWAL?: boolean
  readonly spanAttributes?: Record<string, unknown>
  readonly transformResultNames?: (str: string) => string
  readonly transformQueryNames?: (str: string) => string
}

const make = (options: Config) =>
  Effect.gen(function* () {
    const native = (yield* Sqlite.Native) as Database

    const client = yield* makeSqliteClient(
      {
        spanAttributes: options.spanAttributes,
        transformResultNames: options.transformResultNames,
        transformQueryNames: options.transformQueryNames,
      },
      {
        all: (query, params, safeIntegers) => {
          const statement = native.query(query)
          // @ts-ignore bun-types missing safeIntegers method, fixed in https://github.com/oven-sh/bun/pull/26627
          statement.safeIntegers(safeIntegers)
          return (statement.all(...(params as any)) ?? []) as Array<Record<string, unknown>>
        },
        values: (query, params, safeIntegers) => {
          const statement = native.query(query)
          // @ts-ignore bun-types missing safeIntegers method, fixed in https://github.com/oven-sh/bun/pull/26627
          statement.safeIntegers(safeIntegers)
          return (statement.values(...(params as any)) ?? []) as ReadonlyArray<ReadonlyArray<unknown>>
        },
      },
      {
        serialize: () => native.serialize(),
        loadExtension: (path) => native.loadExtension(path),
      },
    )

    return Object.assign(client, { [TypeId]: TypeId, config: options }) as SqliteClient
  })

const nativeLayer = (config: Config) =>
  Layer.effect(
    Sqlite.Native,
    Effect.gen(function* () {
      // Bun rejects a readonly handle opened with `readwrite`/`create`, and the
      // node driver's readonly path implies the same, so default both to the
      // inverse of `readonly` (matching node, which only forwards `readOnly`).
      const native = new Database(config.filename, {
        readonly: config.readonly,
        readwrite: config.readwrite ?? config.readonly !== true,
        create: config.create ?? config.readonly !== true,
      })
      yield* Effect.addFinalizer(() => Effect.sync(() => native.close()))
      // A readonly connection must not attempt `journal_mode = WAL`: the pragma
      // writes to the database header, so it fails (or is a no-op) on a readonly
      // handle. Match the node driver's guard exactly.
      if (config.disableWAL !== true && config.readonly !== true) native.run("PRAGMA journal_mode = WAL;")
      return native
    }),
  )

const sqliteLayer = (config: Config) => Layer.effect(Client.SqlClient, make(config))

const drizzleLayer = Layer.effect(
  Sqlite.Drizzle,
  Effect.gen(function* () {
    return drizzle({ client: (yield* Sqlite.Native) as Database })
  }),
)

export const layer = (config: Config) => {
  const native = nativeLayer(config)
  return Layer.merge(native, Layer.merge(sqliteLayer(config), drizzleLayer).pipe(Layer.provide(native))).pipe(
    Layer.provide(Reactivity.layer),
  )
}
