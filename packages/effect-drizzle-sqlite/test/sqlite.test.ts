import { mkdir, mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { Database } from "bun:sqlite"
import { expect, test } from "bun:test"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { eq, sql } from "drizzle-orm"
import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core"
import { Effect } from "effect"
import type { SqlClient as SqlClientService } from "effect/unstable/sql/SqlClient"
import { isSqlError } from "effect/unstable/sql/SqlError"
// effect-drizzle-sqlite is intentionally generic and does not depend on
// @opencode-ai/core, so the integration test reaches the retry helper directly.
import { BusyRetriesExhaustedError, withBusyRetry } from "../../core/src/database/busy"
import { EffectDrizzleSqlite } from "../src"

const users = sqliteTable("users", {
  id: integer().primaryKey({ autoIncrement: true }),
  name: text().notNull(),
})

const run = <A, E>(effect: Effect.Effect<A, E, SqlClientService>) =>
  Effect.runPromise(
    effect.pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped),
  )

const makeDb = Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* db.run(sql`create table users (id integer primary key autoincrement, name text not null)`)
  return db
})

const createMigrationsFolder = async () => {
  const migrationsFolder = await mkdtemp(join(tmpdir(), "effect-drizzle-sqlite-"))
  await mkdir(join(migrationsFolder, "20240101000000_create_migrated_users"), { recursive: true })
  await Bun.write(
    join(migrationsFolder, "20240101000000_create_migrated_users", "migration.sql"),
    "create table migrated_users (id integer primary key autoincrement, name text not null);",
  )
  return migrationsFolder
}

test("selects rows through Effect-yieldable query builders", async () => {
  await run(
    Effect.gen(function* () {
      const db = yield* makeDb
      yield* db.insert(users).values({ name: "Ada" })

      expect(yield* db.select().from(users)).toEqual([{ id: 1, name: "Ada" }])
      expect(yield* db.select({ id: users.id }).from(users).where(eq(users.name, "Ada")).get()).toEqual({ id: 1 })
    }),
  )
})

test("commits successful transactions", async () => {
  await run(
    Effect.gen(function* () {
      const db = yield* makeDb

      yield* db.transaction((tx) => tx.insert(users).values({ name: "Grace" }), { behavior: "immediate" })

      expect(yield* db.select().from(users)).toEqual([{ id: 1, name: "Grace" }])
    }),
  )
})

test("rolls back failed transactions", async () => {
  await run(
    Effect.gen(function* () {
      const db = yield* makeDb

      yield* db
        .transaction((tx) =>
          tx
            .insert(users)
            .values({ name: "Linus" })
            .pipe(Effect.andThen(Effect.fail("boom"))),
        )
        .pipe(Effect.ignore)

      expect(yield* db.select().from(users)).toEqual([])
    }),
  )
})

test("rolls back explicit transaction rollback", async () => {
  await run(
    Effect.gen(function* () {
      const db = yield* makeDb

      yield* db
        .transaction((tx) =>
          tx
            .insert(users)
            .values({ name: "Barbara" })
            .pipe(Effect.andThen(Effect.fail(tx.rollback()))),
        )
        .pipe(Effect.ignore)

      expect(yield* db.select().from(users)).toEqual([])
    }),
  )
})

test("preserves failed transaction begin errors", async () => {
  const dir = await mkdtemp(join(tmpdir(), "effect-drizzle-sqlite-"))
  const filename = join(dir, "locked.db")
  const holder = new Database(filename)

  try {
    holder.run("create table users (id integer primary key autoincrement, name text not null)")
    holder.run("pragma busy_timeout = 0")
    holder.run("begin immediate")

    await Effect.runPromise(
      Effect.gen(function* () {
        const db = yield* EffectDrizzleSqlite.makeWithDefaults()
        yield* db.run(sql`pragma busy_timeout = 0`)

        const error = yield* db
          .transaction((tx) => tx.insert(users).values({ name: "Blocked" }), { behavior: "immediate" })
          .pipe(Effect.flip)

        if (!isSqlError(error)) throw new Error("Expected SqlError")
        expect(error.reason._tag).toBe("LockTimeoutError")
        expect(error.reason.cause instanceof Error ? error.reason.cause.message : "").toContain("database is locked")
      }).pipe(Effect.provide(SqliteClient.layer({ filename, disableWAL: true })), Effect.scoped),
    )
  } finally {
    if (holder.inTransaction) holder.run("rollback")
    holder.close()
    await rm(dir, { recursive: true, force: true })
  }
})

test("recovers a contended transactional write under bounded busy retry", async () => {
  const dir = await mkdtemp(join(tmpdir(), "effect-drizzle-sqlite-"))
  const filename = join(dir, "busy-retry.db")
  const holder = new Database(filename)

  try {
    holder.run("create table users (id integer primary key autoincrement, name text not null)")
    holder.run("pragma busy_timeout = 0")
    holder.run("begin immediate")

    let released = false
    const release = (): void => {
      if (released) return
      released = true
      holder.run("rollback")
    }

    await Effect.runPromise(
      Effect.gen(function* () {
        const db = yield* EffectDrizzleSqlite.makeWithDefaults()
        yield* db.run(sql`pragma busy_timeout = 0`)

        // The holder owns the write lock until the first backoff sleep releases
        // it, so the first attempt fails busy and a later attempt commits.
        yield* withBusyRetry(
          db.transaction((tx) => tx.insert(users).values({ name: "Recovered" }), { behavior: "immediate" }),
          { retries: 10, baseDelayMs: 1, maxDelayMs: 5, sleep: () => Effect.sync(release) },
        )

        expect(yield* db.select().from(users)).toEqual([{ id: 1, name: "Recovered" }])
      }).pipe(Effect.provide(SqliteClient.layer({ filename, disableWAL: true })), Effect.scoped),
    )
  } finally {
    if (holder.inTransaction) holder.run("rollback")
    holder.close()
    await rm(dir, { recursive: true, force: true })
  }
})

test("fails loudly when the busy retry budget is exhausted", async () => {
  const dir = await mkdtemp(join(tmpdir(), "effect-drizzle-sqlite-"))
  const filename = join(dir, "busy-exhausted.db")
  const holder = new Database(filename)

  try {
    holder.run("create table users (id integer primary key autoincrement, name text not null)")
    holder.run("pragma busy_timeout = 0")
    holder.run("begin immediate")

    const outcome = await Effect.runPromise(
      Effect.gen(function* () {
        const db = yield* EffectDrizzleSqlite.makeWithDefaults()
        yield* db.run(sql`pragma busy_timeout = 0`)

        return yield* withBusyRetry(
          db.transaction((tx) => tx.insert(users).values({ name: "Blocked" }), { behavior: "immediate" }),
          { retries: 2, baseDelayMs: 1, maxDelayMs: 1, sleep: () => Effect.void },
        ).pipe(Effect.catchDefect((cause) => Effect.succeed(cause)))
      }).pipe(Effect.provide(SqliteClient.layer({ filename, disableWAL: true })), Effect.scoped),
    )

    expect(outcome).toBeInstanceOf(BusyRetriesExhaustedError)
    const error = outcome as BusyRetriesExhaustedError
    expect(error.attempts).toBe(3)
    expect(isSqlError(error.lastCause)).toBe(true)
  } finally {
    if (holder.inTransaction) holder.run("rollback")
    holder.close()
    await rm(dir, { recursive: true, force: true })
  }
})

test("supports returning and rejects empty update sets", async () => {
  await run(
    Effect.gen(function* () {
      const db = yield* makeDb

      const inserted = yield* db.insert(users).values({ name: "Ada" }).returning({ id: users.id, name: users.name })
      expect(inserted).toEqual([{ id: 1, name: "Ada" }])

      const updated = yield* db.update(users).set({ name: "Grace" }).where(eq(users.id, 1)).returning()
      expect(updated).toEqual([{ id: 1, name: "Grace" }])

      const deleted = yield* db.delete(users).where(eq(users.id, 1)).returning({ id: users.id })
      expect(deleted).toEqual([{ id: 1 }])

      expect(() => db.update(users).set({ name: undefined })).toThrow("No values to set")
    }),
  )
})

test("runs migrations once and records migration metadata", async () => {
  const migrationsFolder = await createMigrationsFolder()
  try {
    await run(
      Effect.gen(function* () {
        const db = yield* EffectDrizzleSqlite.makeWithDefaults()

        yield* EffectDrizzleSqlite.migrate(db, { migrationsFolder })
        yield* EffectDrizzleSqlite.migrate(db, { migrationsFolder })
        yield* db.run(sql`insert into migrated_users (name) values ('Margaret')`)

        expect(yield* db.all<{ name: string }>(sql`select name from migrated_users`)).toEqual([{ name: "Margaret" }])
        expect(yield* db.all<{ name: string | null }>(sql`select name from __drizzle_migrations`)).toEqual([
          { name: "20240101000000_create_migrated_users" },
        ])
      }),
    )
  } finally {
    await rm(migrationsFolder, { recursive: true, force: true })
  }
})
