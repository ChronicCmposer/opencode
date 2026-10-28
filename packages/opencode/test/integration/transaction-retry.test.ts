// Whole-transaction busy retry.
//
// A worker writes with `BEGIN IMMEDIATE`; a concurrent writer makes SQLite
// report busy. Retrying one statement would leave a half-applied transaction, so
// the retry boundary must be the whole body (begin, statements, commit). These
// tests drive the real transaction helper against a real SQLite file for the
// begin-busy case, and a fault-injected connection for a busy failure in the
// middle of the body, proving the body replays and no partial row survives.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { Database as BunDatabase } from "bun:sqlite"
import { afterEach, describe, expect, test } from "bun:test"
import { Effect } from "effect"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import { openConnection, type Connection } from "@opencode-ai/core/database/connection"
import { isBusySqlError, withBusyRetry } from "@opencode-ai/core/database/busy"
import { makeSqliteClient, type SqliteNativeRunner } from "@opencode-ai/core/database/sqlite-adapter"
import {
  BusyExhaustedError,
  EXECUTOR_BUSY_RETRY_CONFIG,
  inImmediateTransaction,
  inImmediateTransactionAsync,
  isBusyError,
  retryBusyAsync,
  retryBusySync,
  type TransactionConnection,
} from "@opencode-ai/core/database/transaction"

const directories: string[] = []
const connections: Connection[] = []

function workspace(): { filename: string } {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-transaction-retry-"))
  directories.push(directory)
  return { filename: path.join(directory, "retry.db") }
}

function open(filename: string, busyTimeoutMs = 0): Connection {
  const connection = openConnection({ filename, busyTimeoutMs })
  connections.push(connection)
  return connection
}

function busy(): Error {
  return Object.assign(new Error("database is locked"), { name: "SQLiteError", code: "SQLITE_BUSY", errno: 5 })
}

afterEach(() => {
  for (const connection of connections.splice(0)) connection.close()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("one busy classifier", () => {
  test("the transaction helpers reuse the shared classifier instead of copying it", () => {
    expect(isBusyError).toBe(isBusySqlError)
  })

  test("the transaction module keeps no second busy-code recognizer", () => {
    const source = fs.readFileSync(
      path.resolve(import.meta.dir, "../../../core/src/database/transaction.ts"),
      "utf8",
    )
    expect(source).not.toContain("isBusyNumericCode")
    expect(source).not.toContain('["SQLITE_BUSY", "SQLITE_LOCKED"]')
  })
})

describe("executor retry budget", () => {
  test("is three retries (four attempts), so the total stays below the request deadline", () => {
    expect(EXECUTOR_BUSY_RETRY_CONFIG.retries).toBe(3)

    let attempts = 0
    const error = (() => {
      try {
        retryBusySync(
          () => {
            attempts += 1
            throw busy()
          },
          EXECUTOR_BUSY_RETRY_CONFIG,
        )
      } catch (cause) {
        return cause
      }
    })()

    expect(error).toBeInstanceOf(BusyExhaustedError)
    expect(attempts).toBe(4)
    expect((error as BusyExhaustedError).attempts).toBe(4)
  })
})

describe("real SQLite begin-busy retry", () => {
  test("the whole transaction retries until the lock is released", async () => {
    const { filename } = workspace()
    const holder = open(filename, 5_000)
    const writer = open(filename, 0)
    holder.exec("CREATE TABLE item (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")

    holder.exec("BEGIN IMMEDIATE")
    holder.run("INSERT INTO item (name) VALUES (?)", ["holder"])

    let begins = 0
    const counted: TransactionConnection = {
      exec(sql) {
        if (sql === "BEGIN IMMEDIATE") begins += 1
        writer.exec(sql)
      },
    }

    // Release the lock while the retry loop is sleeping between attempts.
    const release = setTimeout(() => {
      holder.exec("COMMIT")
    }, 25)

    try {
      const committed = await inImmediateTransactionAsync(
        counted,
        () => {
          writer.run("INSERT INTO item (name) VALUES (?)", ["writer"])
          return "committed"
        },
        { retries: 10, baseDelayMs: 10, maxDelayMs: 10 },
      )
      expect(committed).toBe("committed")
      expect(begins).toBeGreaterThan(1)
      expect(writer.all<{ name: string }>("SELECT name FROM item ORDER BY id")).toEqual([
        { name: "holder" },
        { name: "writer" },
      ])
    } finally {
      clearTimeout(release)
    }
  })
})

describe("mid-transaction busy failure", () => {
  test("a busy statement rolls back and replays the entire body exactly once", () => {
    const statements: string[] = []
    let attempts = 0
    const connection: TransactionConnection = {
      exec(sql) {
        statements.push(sql)
      },
    }
    const body = (tx: TransactionConnection): void => {
      attempts += 1
      tx.exec("INSERT INTO item VALUES (1)")
      // The second statement is busy only on the first attempt.
      if (attempts === 1) throw busy()
      tx.exec("INSERT INTO item VALUES (2)")
    }

    expect(inImmediateTransaction(connection, body, { retries: 2 })).toBeUndefined()
    expect(attempts).toBe(2)
    // First attempt: begin, first insert, rollback. Second: begin, both, commit.
    expect(statements).toEqual([
      "BEGIN IMMEDIATE",
      "INSERT INTO item VALUES (1)",
      "ROLLBACK",
      "BEGIN IMMEDIATE",
      "INSERT INTO item VALUES (1)",
      "INSERT INTO item VALUES (2)",
      "COMMIT",
    ])
  })

  test("exhaustion is typed and the connection is rolled back", () => {
    const statements: string[] = []
    const connection: TransactionConnection = {
      exec(sql) {
        statements.push(sql)
        if (sql === "BEGIN IMMEDIATE") return
        if (sql.startsWith("INSERT")) throw busy()
      },
    }
    expect(() =>
      inImmediateTransaction(
        connection,
        (tx) => {
          tx.exec("INSERT INTO item VALUES (1)")
        },
        { retries: 1 },
      ),
    ).toThrow(/remained busy/)
    expect(statements.filter((sql) => sql === "ROLLBACK")).toHaveLength(2)
  })
})

/**
 * A real `bun:sqlite` runner for the shared adapter. `onStatement` may inject a
 * failure before the statement reaches SQLite, which lets the test observe how
 * many times the adapter re-issues a statement inside a transaction.
 */
function adapterRunner(native: BunDatabase, statements: string[], intercept?: (sql: string) => void): SqliteNativeRunner {
  return {
    all(query, params) {
      statements.push(query)
      intercept?.(query)
      return native.query(query).all(...(params as never[])) as Array<Record<string, unknown>>
    },
    values(query, params) {
      statements.push(query)
      intercept?.(query)
      return native.query(query).values(...(params as never[])) as ReadonlyArray<ReadonlyArray<unknown>>
    },
  }
}

describe("adapter transaction retry boundary", () => {
  test("a busy statement inside a transaction is not retried; the whole body replays", async () => {
    const { filename } = workspace()
    const native = new BunDatabase(filename)
    native.exec("PRAGMA journal_mode = WAL")
    native.exec("PRAGMA busy_timeout = 0")
    native.exec("CREATE TABLE item (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)")
    const statements: string[] = []
    let inserts = 0
    const client = await Effect.runPromise(
      makeSqliteClient(
        {},
        adapterRunner(native, statements, (sql) => {
          if (!sql.trim().toUpperCase().startsWith("INSERT")) return
          inserts += 1
          if (inserts === 1) throw busy()
        }),
      ).pipe(Effect.provide(Reactivity.layer)),
    )

    try {
      const transaction = client.withTransaction(
        client.unsafe("INSERT INTO item (name) VALUES (?)", ["row"]).withoutTransform.pipe(Effect.asVoid),
      )

      const error = await Effect.runPromise(transaction.pipe(Effect.flip))
      expect(isBusySqlError(error)).toBe(true)
      // Per-statement retry is suppressed inside the transaction: one BEGIN and
      // one body attempt, even though the busy budget allows more.
      expect(statements.filter((sql) => sql === "BEGIN IMMEDIATE")).toHaveLength(1)
      expect(inserts).toBe(1)

      // Retrying the *whole* transaction re-begins and replays the body.
      await Effect.runPromise(withBusyRetry(transaction, { retries: 2, sleep: () => Effect.void }))
      expect(statements.filter((sql) => sql === "BEGIN IMMEDIATE")).toHaveLength(2)
      expect(inserts).toBe(2)
      expect(native.query("SELECT name FROM item").all()).toEqual([{ name: "row" }])
    } finally {
      native.close()
    }
  })

  test("a transaction takes BEGIN IMMEDIATE and does not replay a busy begin", async () => {
    const { filename } = workspace()
    const holder = new BunDatabase(filename)
    const writer = new BunDatabase(filename)
    try {
      holder.exec("PRAGMA journal_mode = WAL")
      holder.exec("CREATE TABLE item (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)")
      writer.exec("PRAGMA busy_timeout = 0")
      holder.exec("BEGIN IMMEDIATE")

      const statements: string[] = []
      const client = await Effect.runPromise(
        makeSqliteClient({}, adapterRunner(writer, statements)).pipe(Effect.provide(Reactivity.layer)),
      )
      // Reserve the transaction-scoped connection directly: the adapter rewrites
      // its `BEGIN` to `BEGIN IMMEDIATE`, which is where the busy result arrives.
      const begin = Effect.scoped(
        Effect.gen(function* () {
          const connection = yield* client.reserve
          yield* connection.executeUnprepared("BEGIN", [], undefined)
          yield* connection.executeUnprepared("ROLLBACK", [], undefined)
        }),
      )

      const error = await Effect.runPromise(begin.pipe(Effect.flip))
      expect(isBusySqlError(error)).toBe(true)
      expect(statements).toEqual(["BEGIN IMMEDIATE"])
      expect(writer.query("SELECT COUNT(*) AS count FROM item").get()).toEqual({ count: 0 })

      holder.exec("COMMIT")
      await Effect.runPromise(begin)
      expect(statements).toEqual(["BEGIN IMMEDIATE", "BEGIN IMMEDIATE", "ROLLBACK"])
    } finally {
      writer.close()
      holder.close()
    }
  })
})

describe("busy COMMIT recovery", () => {
  test("a busy COMMIT is retried in place without replaying the body", () => {
    const statements: string[] = []
    let commits = 0
    let bodies = 0
    const connection: TransactionConnection = {
      exec(sql) {
        statements.push(sql)
        if (sql !== "COMMIT") return
        commits += 1
        if (commits === 1) throw busy()
      },
    }

    inImmediateTransaction(
      connection,
      () => {
        bodies += 1
      },
      { retries: 1 },
    )

    // The body ran once; only COMMIT was re-issued after the transient lock.
    expect(bodies).toBe(1)
    expect(commits).toBe(2)
    expect(statements).toEqual(["BEGIN IMMEDIATE", "COMMIT", "COMMIT"])
  })
})

describe("async helpers keep the Promise contract", () => {
  test("retryBusyAsync rejects a non-busy failure instead of throwing synchronously", async () => {
    let pending: Promise<never> | undefined
    expect(() => {
      pending = retryBusyAsync(() => {
        throw new Error("validation failed")
      })
    }).not.toThrow()

    await pending!.catch((error) => {
      expect(error).toBeInstanceOf(Error)
      expect((error as Error).message).toBe("validation failed")
    })
  })

  test("retryBusyAsync rejects with the typed exhaustion error when retries are zero", async () => {
    let pending: Promise<never> | undefined
    expect(() => {
      pending = retryBusyAsync(
        () => {
          throw busy()
        },
        { retries: 0 },
      )
    }).not.toThrow()

    await pending!.catch((error) => {
      expect(error).toBeInstanceOf(BusyExhaustedError)
      expect((error as BusyExhaustedError).attempts).toBe(1)
    })
  })

  test("inImmediateTransactionAsync rejects a non-busy body failure instead of throwing synchronously", async () => {
    const statements: string[] = []
    const connection: TransactionConnection = {
      exec(sql) {
        statements.push(sql)
        if (sql !== "INSERT INTO item VALUES (1)") return
        throw new Error("non-busy statement failure")
      },
    }
    let pending: Promise<unknown> | undefined
    expect(() => {
      pending = inImmediateTransactionAsync(connection, () => {
        connection.exec("INSERT INTO item VALUES (1)")
        return undefined
      })
    }).not.toThrow()

    await pending!.catch((error) => {
      expect(error).toBeInstanceOf(Error)
      expect((error as Error).message).toBe("non-busy statement failure")
    })
    expect(statements).toContain("ROLLBACK")
  })

  test("inImmediateTransactionAsync rejects with typed exhaustion when retries are zero", async () => {
    const connection: TransactionConnection = {
      exec(sql) {
        if (sql === "BEGIN IMMEDIATE") throw busy()
      },
    }
    let pending: Promise<unknown> | undefined
    expect(() => {
      pending = inImmediateTransactionAsync(connection, () => "unreachable", { retries: 0 })
    }).not.toThrow()

    await pending!.catch((error) => {
      expect(error).toBeInstanceOf(BusyExhaustedError)
      expect((error as BusyExhaustedError).attempts).toBe(1)
    })
  })
})
