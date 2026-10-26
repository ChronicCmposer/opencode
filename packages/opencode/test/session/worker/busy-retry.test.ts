// W1.2 — BEGIN IMMEDIATE plus bounded whole-transaction retry (D5).
//
// A busy write must never be retried one statement at a time: either the whole
// `BEGIN IMMEDIATE … COMMIT` unit succeeds or it is retried as a unit. A busy
// COMMIT is the one exception that re-issues COMMIT in place, because the
// transaction is still open and replaying the body could duplicate effects.

import { describe, expect, test } from "bun:test"
import path from "node:path"
import { openConnection, type Connection } from "@opencode-ai/core/database/connection"
import {
  BusyExhaustedError,
  TransactionPoisonedError,
  TransactionRollbackError,
  inImmediateTransaction,
  isBusyError,
  retryBusySync,
} from "@opencode-ai/core/database/transaction"
import { tmpdir } from "../../fixture/fixture"

const busy = () => Object.assign(new Error("SQLITE_BUSY: database is locked"), { code: "SQLITE_BUSY" })

type Behaviour = "ok" | "busy" | "boom"

/** Minimal Connection double with a per-statement behaviour script. */
class ScriptedConnection implements Connection {
  readonly owner = "scripted"
  readonly filename = ":memory:"
  readonly closed = false
  readonly execs: string[] = []
  bodies = 0

  private readonly occurrences = new Map<string, number>()

  constructor(private readonly behaviour: (sql: string, occurrence: number) => Behaviour) {}

  private step(sql: string): void {
    this.execs.push(sql)
    const key = sql.trim().toLowerCase()
    const occurrence = (this.occurrences.get(key) ?? 0) + 1
    this.occurrences.set(key, occurrence)
    const result = this.behaviour(sql, occurrence)
    if (result === "busy") throw busy()
    if (result === "boom") throw new Error("constraint failed")
  }

  all<T = Record<string, unknown>>(): ReadonlyArray<T> {
    return []
  }

  values<T = ReadonlyArray<unknown>>(): ReadonlyArray<T> {
    return []
  }

  run(sql: string): { changes: number; lastInsertRowid: number } {
    if (!/^\s*(begin|commit|rollback)/i.test(sql)) this.bodies += 1
    this.step(sql)
    return { changes: 1, lastInsertRowid: 1 }
  }

  exec(sql: string): void {
    this.step(sql)
  }

  pragma(): unknown {
    return undefined
  }

  journalMode(): string {
    return "memory"
  }

  close(): void {}

  count(prefix: string): number {
    return this.execs.filter((sql) => sql.trim().toLowerCase().startsWith(prefix)).length
  }
}

describe("busy classification", () => {
  test("recognizes native codes, extended codes, and injected sqliteCode", () => {
    expect(isBusyError({ code: "SQLITE_BUSY" })).toBe(true)
    expect(isBusyError({ code: "SQLITE_BUSY_SNAPSHOT" })).toBe(true)
    expect(isBusyError({ sqliteCode: "SQLITE_LOCKED_SHAREDCACHE" })).toBe(true)
    expect(isBusyError(new Error("database is locked"))).toBe(true)
  })

  test("does not classify constraints, missing tables, or non-objects as busy", () => {
    expect(isBusyError({ code: "SQLITE_CONSTRAINT" })).toBe(false)
    expect(isBusyError(new Error("no such table: session"))).toBe(false)
    expect(isBusyError("SQLITE_BUSY")).toBe(false)
  })
})

describe("retryBusySync", () => {
  test("retries the whole call and returns the eventual result", () => {
    let attempts = 0
    const result = retryBusySync(
      () => {
        attempts += 1
        if (attempts < 3) throw busy()
        return "committed"
      },
      { retries: 5, baseDelayMs: 1, maxDelayMs: 2 },
    )

    expect(result).toBe("committed")
    expect(attempts).toBe(3)
  })

  test("exhaustion is typed, carries the attempt count, and is terminal", () => {
    let attempts = 0
    const error = (() => {
      try {
        retryBusySync(
          () => {
            attempts += 1
            throw busy()
          },
          { retries: 2, baseDelayMs: 1, maxDelayMs: 1 },
        )
      } catch (cause) {
        return cause
      }
    })()

    expect(error).toBeInstanceOf(BusyExhaustedError)
    expect((error as BusyExhaustedError).tag).toBe("SQLITE_BUSY_EXHAUSTED")
    expect((error as BusyExhaustedError).attempts).toBe(3)
    expect(attempts).toBe(3)
  })

  test("a non-busy failure is never retried", () => {
    let attempts = 0
    expect(() =>
      retryBusySync(
        () => {
          attempts += 1
          throw new Error("no such table: session")
        },
        { retries: 5, baseDelayMs: 1, maxDelayMs: 1 },
      ),
    ).toThrow(/no such table/)
    expect(attempts).toBe(1)
  })
})

describe("inImmediateTransaction", () => {
  test("retries the whole body when a statement is busy", () => {
    const connection = new ScriptedConnection((sql, occurrence) =>
      sql.startsWith("INSERT") && occurrence === 1 ? "busy" : "ok",
    )

    const result = inImmediateTransaction(
      connection,
      (tx) => {
        tx.run("INSERT INTO item VALUES (1)")
        return "done"
      },
      { retries: 1, baseDelayMs: 1, maxDelayMs: 1 },
    )

    expect(result).toBe("done")
    expect(connection.count("begin immediate")).toBe(2)
    expect(connection.bodies).toBe(2)
    expect(connection.count("commit")).toBe(1)
  })

  test("a busy COMMIT is re-issued without replaying the body", () => {
    const connection = new ScriptedConnection((sql, occurrence) =>
      sql.trim().toLowerCase() === "commit" && occurrence === 1 ? "busy" : "ok",
    )

    const result = inImmediateTransaction(
      connection,
      (tx) => {
        tx.run("INSERT INTO item VALUES (1)")
        return "done"
      },
      { retries: 2, baseDelayMs: 1, maxDelayMs: 1 },
    )

    expect(result).toBe("done")
    expect(connection.count("begin immediate")).toBe(1)
    expect(connection.bodies).toBe(1)
    expect(connection.count("commit")).toBe(2)
  })

  test("commit exhaustion rolls back and surfaces the typed error", () => {
    const connection = new ScriptedConnection((sql) => (sql.trim().toLowerCase() === "commit" ? "busy" : "ok"))

    const error = (() => {
      try {
        inImmediateTransaction(connection, () => "done", { retries: 1, baseDelayMs: 1, maxDelayMs: 1 })
      } catch (cause) {
        return cause
      }
    })()

    expect(error).toBeInstanceOf(BusyExhaustedError)
    expect(connection.count("rollback")).toBe(1)
    expect(connection.count("commit")).toBe(2)
  })

  test("a non-busy body failure rolls back and is not retried", () => {
    const connection = new ScriptedConnection((sql) => (sql.startsWith("INSERT") ? "boom" : "ok"))
    let bodies = 0

    expect(() =>
      inImmediateTransaction(
        connection,
        (tx) => {
          bodies += 1
          tx.run("INSERT INTO item VALUES (1)")
        },
        { retries: 5, baseDelayMs: 1, maxDelayMs: 1 },
      ),
    ).toThrow(/constraint failed/)

    expect(bodies).toBe(1)
    expect(connection.count("begin immediate")).toBe(1)
    expect(connection.count("rollback")).toBe(1)
  })

  test("a failed ROLLBACK poisons the connection for later transactions", () => {
    const connection = new ScriptedConnection((sql) => {
      if (sql.startsWith("INSERT")) return "boom"
      if (sql.trim().toLowerCase() === "rollback") return "boom"
      return "ok"
    })

    const first = (() => {
      try {
        inImmediateTransaction(connection, (tx) => tx.run("INSERT INTO item VALUES (1)"), {
          retries: 0,
          baseDelayMs: 1,
          maxDelayMs: 1,
        })
      } catch (cause) {
        return cause
      }
    })()
    expect(first).toBeInstanceOf(TransactionRollbackError)

    expect(() => inImmediateTransaction(connection, () => "unreachable")).toThrow(TransactionPoisonedError)
  })

  test("a forced SQLITE_BUSY on a real file leaves no partial write", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "busy.db")
    const holder = openConnection({ filename, busyTimeoutMs: 1 })
    const contender = openConnection({ filename, busyTimeoutMs: 1 })
    try {
      holder.exec("CREATE TABLE item (id TEXT PRIMARY KEY)")
      holder.exec("BEGIN IMMEDIATE")
      holder.run("INSERT INTO item (id) VALUES (?)", ["held"])

      const error = (() => {
        try {
          inImmediateTransaction(
            contender,
            (tx) => tx.run("INSERT INTO item (id) VALUES (?)", ["contender"]),
            { retries: 2, baseDelayMs: 1, maxDelayMs: 1 },
          )
        } catch (cause) {
          return cause
        }
      })()

      expect(error).toBeInstanceOf(BusyExhaustedError)
      holder.exec("ROLLBACK")
      expect(contender.all("SELECT id FROM item")).toEqual([])
    } finally {
      holder.close()
      contender.close()
    }
  })
})
