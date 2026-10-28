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
import { afterEach, describe, expect, test } from "bun:test"
import { openConnection, type Connection } from "@opencode-ai/core/database/connection"
import {
  inImmediateTransaction,
  inImmediateTransactionAsync,
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
