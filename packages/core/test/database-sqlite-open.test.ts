// The bun and node SQLite layers must make the same open-time decision about
// `journal_mode = WAL`. Issuing that pragma on a readonly handle writes the
// database header, so it must be skipped — the node driver already guarded on
// `readonly`, the bun driver did not. These tests pin the bun parity.

import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { Database } from "bun:sqlite"
import { DatabaseSync } from "node:sqlite"
import path from "node:path"
import { Sqlite } from "../src/database/sqlite"
import { layer } from "../src/database/sqlite.bun"
import { PREPARED_STATEMENT_CACHE_LIMIT, openNodeConnection } from "../src/database/connection"
import { tmpdir } from "./fixture/tmpdir"

const withFile = async <A>(run: (filename: string) => Promise<A>): Promise<A> => {
  await using tmp = await tmpdir()
  const filename = path.join(tmp.path, "open.db")
  const seed = new Database(filename, { create: true })
  try {
    seed.exec("CREATE TABLE t (id INTEGER PRIMARY KEY)")
  } finally {
    seed.close()
  }
  return run(filename)
}

describe("node prepared-statement cache", () => {
  test("reuses a prepared statement per connection and never across connections", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "cache.db")
    const original = DatabaseSync.prototype.prepare
    let prepares = 0
    DatabaseSync.prototype.prepare = function (this: DatabaseSync, sql: string) {
      prepares += 1
      return original.call(this, sql)
    }

    let first: ReturnType<typeof openNodeConnection> | undefined
    let second: ReturnType<typeof openNodeConnection> | undefined
    try {
      first = openNodeConnection({ filename, pragmas: [] })
      second = openNodeConnection({ filename, pragmas: [] })
      const before = prepares
      expect(first.all("SELECT 1 AS n")).toEqual([{ n: 1 }])
      expect(first.all("SELECT 1 AS n")).toEqual([{ n: 1 }])
      // The same SQL text is prepared once for this handle.
      expect(prepares - before).toBe(1)

      // A row-returning and an array-returning statement need different driver
      // shapes, so they are cached under separate keys.
      expect(first.values("SELECT 1 AS n")).toEqual([[1]])
      expect(first.values("SELECT 1 AS n")).toEqual([[1]])
      expect(prepares - before).toBe(2)

      // Another connection has its own cache; statements never cross handles.
      expect(second.all("SELECT 1 AS n")).toEqual([{ n: 1 }])
      expect(prepares - before).toBe(3)
    } finally {
      first?.close()
      second?.close()
      DatabaseSync.prototype.prepare = original
    }
  })

  test("evicts the oldest statement when the per-connection cache is full", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "bounded.db")
    const original = DatabaseSync.prototype.prepare
    let prepares = 0
    DatabaseSync.prototype.prepare = function (this: DatabaseSync, sql: string) {
      prepares += 1
      return original.call(this, sql)
    }

    let connection: ReturnType<typeof openNodeConnection> | undefined
    try {
      connection = openNodeConnection({ filename, pragmas: [] })
      for (let index = 0; index <= PREPARED_STATEMENT_CACHE_LIMIT; index += 1) {
        connection.all(`SELECT ${index} AS n`)
      }
      const before = prepares
      // The first SQL was evicted by the bounded LRU cache, so it is prepared
      // again rather than growing without limit.
      connection.all("SELECT 0 AS n")
      expect(prepares - before).toBe(1)
    } finally {
      connection?.close()
      DatabaseSync.prototype.prepare = original
    }
  })
})

describe("sqlite open-time pragmas", () => {
  test("a readonly bun connection opens without attempting journal_mode = WAL", async () => {
    await withFile(async (filename) => {
      const journalMode = await Effect.runPromise(
        Effect.gen(function* () {
          const native = yield* Sqlite.Native
          return (native as Database).query("PRAGMA journal_mode").get() as { journal_mode: string }
        }).pipe(Effect.scoped, Effect.provide(layer({ filename, readonly: true }))),
      )

      expect(journalMode.journal_mode).not.toBe("wal")
    })
  })

  test("a writable bun connection still enables WAL", async () => {
    await withFile(async (filename) => {
      const journalMode = await Effect.runPromise(
        Effect.gen(function* () {
          const native = yield* Sqlite.Native
          return (native as Database).query("PRAGMA journal_mode").get() as { journal_mode: string }
        }).pipe(Effect.scoped, Effect.provide(layer({ filename }))),
      )

      expect(journalMode.journal_mode).toBe("wal")
    })
  })
})
