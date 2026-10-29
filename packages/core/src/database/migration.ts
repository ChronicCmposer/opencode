export * as DatabaseMigration from "./migration"

import { sql } from "drizzle-orm"
import { Effect, Semaphore } from "effect"
import type { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { migrations } from "./migration.gen"
import schema from "./schema.gen"

type Database = EffectDrizzleSqlite.EffectSQLiteDatabase
type Transaction = Parameters<Parameters<Database["transaction"]>[0]>[0]
/** The read/write surface shared by the database handle and its transaction. */
type JournalConnection = {
  run: Database["run"]
  all: Database["all"]
  get: Database["get"]
}
const lock = Semaphore.makeUnsafe(1)

/** Every migration batch takes the write lock up front (`BEGIN IMMEDIATE`). */
const IMMEDIATE = { behavior: "immediate" } as const

export type Migration = {
  id: string
  up: (tx: Transaction) => Effect.Effect<void, unknown>
}

type JournalRow = { readonly id: string }

function ensureJournal(connection: JournalConnection) {
  return connection.run(
    sql`CREATE TABLE IF NOT EXISTS ${sql.identifier("migration")} (id TEXT PRIMARY KEY, time_completed INTEGER NOT NULL)`,
  )
}

function readJournal(connection: JournalConnection) {
  return Effect.map(
    connection.all<JournalRow>(sql`SELECT id FROM ${sql.identifier("migration")}`),
    (rows) => new Set(rows.map((row) => row.id)),
  )
}

function record(connection: JournalConnection, migration: Migration) {
  return connection.run(
    sql`INSERT INTO ${sql.identifier("migration")} (id, time_completed) VALUES (${migration.id}, ${Date.now()})`,
  )
}

function recordAll(connection: JournalConnection, input: readonly Migration[]) {
  return Effect.forEach(input, (migration) => record(connection, migration))
}

/**
 * Import Drizzle's old journal when the new one is still empty. Existing
 * installs used Drizzle's migration table, so seeding it once keeps TypeScript
 * migrations from replaying old SQL.
 */
function seedLegacyJournal(connection: JournalConnection, input: readonly Migration[]) {
  return Effect.gen(function* () {
    const legacy = yield* connection.get(
      sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ${"__drizzle_migrations"}`,
    )
    if (legacy === undefined) return new Set<string>()

    const named = (yield* connection.all<{ name: string }>(
      sql`SELECT name FROM pragma_table_info('__drizzle_migrations')`,
    )).some((column) => column.name === "name")

    if (named) {
      yield* connection.run(sql`
        INSERT OR IGNORE INTO ${sql.identifier("migration")} (id, time_completed)
        SELECT name, ${Date.now()}
        FROM ${sql.identifier("__drizzle_migrations")}
        WHERE name IS NOT NULL
      `)
    } else {
      const entries = yield* connection.all<{ created_at: number; prefix: string | null }>(sql`
        SELECT created_at, strftime('%Y%m%d%H%M%S', created_at / 1000, 'unixepoch') AS prefix
        FROM ${sql.identifier("__drizzle_migrations")}
        WHERE created_at IS NOT NULL
      `)

      for (const entry of entries) {
        const migration = input.find((item) => item.id.startsWith(`${entry.prefix}_`))
        if (!migration) {
          return yield* Effect.die(
            new Error(`Legacy migration timestamp ${entry.created_at} does not match any known migration`),
          )
        }
        yield* connection.run(sql`
          INSERT OR IGNORE INTO ${sql.identifier("migration")} (id, time_completed)
          VALUES (${migration.id}, ${Date.now()})
        `)
      }
    }

    return yield* readJournal(connection)
  })
}

/**
 * Apply one migration and its journal row inside the caller's already-open
 * transaction. Re-reading the journal here is what makes a second process that
 * passed the gate earlier (or a peer racing inside one batch) a safe no-op: the
 * decision and the record share the same `BEGIN IMMEDIATE` transaction.
 */
function applyMigration(connection: Transaction, migration: Migration) {
  return Effect.gen(function* () {
    const recorded = yield* connection.get(
      sql`SELECT id FROM ${sql.identifier("migration")} WHERE id = ${migration.id}`,
    )
    if (recorded !== undefined) return
    yield* migration.up(connection)
    yield* record(connection, migration)
  })
}

/** Incremental journal application used by the plain `Database` layer. */
export function applyOnly(db: Database, input: Migration[]) {
  return Effect.gen(function* () {
    yield* ensureJournal(db)
    let completed = yield* readJournal(db)
    if (completed.size === 0) completed = yield* seedLegacyJournal(db, input)

    for (const migration of input) {
      if (completed.has(migration.id)) continue
      yield* db.transaction((tx) => applyMigration(tx, migration), IMMEDIATE)
    }
  })
}

/**
 * The same incremental flow for the session hub's off-loop executor path: one
 * outer `BEGIN IMMEDIATE` transaction reads the journal, applies every pending
 * migration, and records it. A failure rolls the whole batch back, so the
 * executor never observes a half-applied schema.
 */
export function applyBatched(db: Database, input: readonly Migration[] = migrations) {
  return lock.withPermit(
    db.transaction(
      (tx) =>
        Effect.gen(function* () {
          const tables = yield* tx.all<{ name: string }>(
            sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`,
          )
          if (tables.some((table) => table.name === "session")) {
            yield* ensureJournal(tx)
            const completed = yield* readJournal(tx)
            if (completed.size === 0) yield* seedLegacyJournal(tx, input)
            yield* Effect.forEach(input, (migration) => applyMigration(tx, migration))
            return
          }
          if (tables.length > 0) return yield* Effect.die("Database is not empty and has no session table")

          yield* schema.up(tx)
          yield* ensureJournal(tx)
          yield* recordAll(tx, input)
        }),
      IMMEDIATE,
    ),
  )
}

export function apply(db: Database) {
  return lock.withPermit(
    Effect.gen(function* () {
      const tables = yield* db.all<{ name: string }>(
        sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`,
      )
      if (tables.some((table) => table.name === "session")) return yield* applyOnly(db, migrations)
      if (tables.length > 0) return yield* Effect.die("Database is not empty and has no session table")
      yield* db.transaction(
        (tx) =>
          Effect.gen(function* () {
            yield* schema.up(tx)
            yield* ensureJournal(tx)
            yield* recordAll(tx, migrations)
          }),
        IMMEDIATE,
      )
    }),
  )
}
