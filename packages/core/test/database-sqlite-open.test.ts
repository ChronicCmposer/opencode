// The bun and node SQLite layers must make the same open-time decision about
// `journal_mode = WAL`. Issuing that pragma on a readonly handle writes the
// database header, so it must be skipped — the node driver already guarded on
// `readonly`, the bun driver did not. These tests pin the bun parity.

import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { Database } from "bun:sqlite"
import path from "node:path"
import { Sqlite } from "../src/database/sqlite"
import { layer } from "../src/database/sqlite.bun"
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
