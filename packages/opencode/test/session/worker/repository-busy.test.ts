// Worker repository busy retry (D5).
//
// A repository write must retry the whole outermost savepoint unit after a
// clean rollback, never a single statement inside an open transaction. These
// tests pin the retry boundary, the typed exhaustion, and the two cases where
// replaying would be unsafe: a non-busy failure and a failed rollback.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { openConnection, type Connection } from "@opencode-ai/core/database/connection"
import { BusyExhaustedError } from "@opencode-ai/core/database/transaction"
import type { RepositoryConnection } from "@/session/worker/repository/capability"
import { connectedRepository } from "@/session/worker/repository/write"

const busy = () => Object.assign(new Error("SQLITE_BUSY: database is locked"), { code: "SQLITE_BUSY" })

const directories: string[] = []
const connections: Connection[] = []

afterEach(() => {
  for (const connection of connections.splice(0)) connection.close()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

function temporaryDatabase(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-repository-busy-"))
  directories.push(directory)
  return path.join(directory, "busy.db")
}

const TODO_DDL = `
  CREATE TABLE todo (
    session_id TEXT NOT NULL,
    content TEXT NOT NULL,
    status TEXT NOT NULL,
    priority TEXT NOT NULL,
    position INTEGER NOT NULL,
    time_created INTEGER NOT NULL,
    time_updated INTEGER NOT NULL,
    PRIMARY KEY (session_id, position)
  );
`

const todo = {
  session_id: "ses_busy",
  content: "retry me",
  status: "pending",
  priority: "high",
  position: 1,
  time_created: 1,
  time_updated: 1,
}

function open(filename: string, busyTimeoutMs: number): Connection {
  const connection = openConnection({ filename, busyTimeoutMs })
  connections.push(connection)
  return connection
}

/** A real connection that reports busy on its first `run`, then behaves normally. */
function busyOnce(connection: Connection): { readonly connection: RepositoryConnection; readonly execs: string[] } {
  const execs: string[] = []
  let failed = false
  const wrapper: RepositoryConnection = {
    all: connection.all.bind(connection),
    values: connection.values.bind(connection),
    exec(sql) {
      execs.push(sql)
      connection.exec(sql)
    },
    run(sql, params) {
      execs.push(sql)
      if (!failed) {
        failed = true
        throw busy()
      }
      return connection.run(sql, params)
    },
  }
  return { execs, connection: wrapper }
}

/** Minimal scripted connection for failures a real SQLite file cannot force deterministically. */
class ScriptedRepository {
  readonly execs: string[] = []
  runs = 0
  private readonly occurrences = new Map<string, number>()

  constructor(private readonly behaviour: (sql: string, occurrence: number, kind: "run" | "exec") => "ok" | "busy" | "boom") {}

  private step = (sql: string, kind: "run" | "exec"): void => {
    this.execs.push(sql)
    const key = `${kind}:${sql.trim().toLowerCase()}`
    const occurrence = (this.occurrences.get(key) ?? 0) + 1
    this.occurrences.set(key, occurrence)
    const result = this.behaviour(sql, occurrence, kind)
    if (result === "busy") throw busy()
    if (result === "boom") throw new Error("injected failure")
  }

  all = (): ReadonlyArray<unknown> => []

  values = (): ReadonlyArray<unknown> => []

  run = (sql: string): { changes: number; lastInsertRowid: number } => {
    this.runs += 1
    this.step(sql, "run")
    return { changes: 1, lastInsertRowid: this.runs }
  }

  exec = (sql: string): void => {
    this.step(sql, "exec")
  }

  count = (prefix: string): number =>
    this.execs.filter((sql) => sql.trim().toLowerCase().startsWith(prefix)).length
}

function repositoryOver(connection: RepositoryConnection) {
  return connectedRepository(connection)
}

describe("repository busy retry", () => {
  test("a standalone insert that is busy once retries and commits the row", () => {
    const filename = temporaryDatabase()
    const native = open(filename, 1)
    native.exec(TODO_DDL)
    const { connection } = busyOnce(native)
    const repository = repositoryOver(connection)

    const outcome = repository.insert("todo", todo)

    expect(outcome.changes).toBe(1)
    expect(native.all("SELECT content FROM todo")).toEqual([{ content: "retry me" }])
  })

  test("a standalone atomic that is busy once replays the whole unit", () => {
    const filename = temporaryDatabase()
    const native = open(filename, 1)
    native.exec(TODO_DDL)
    const { connection, execs } = busyOnce(native)
    const repository = repositoryOver(connection)
    let units = 0

    const outcome = repository.atomic(() => {
      units += 1
      return repository.insert("todo", todo)
    })

    expect(outcome.changes).toBe(1)
    // The whole savepoint unit ran twice, and the failed attempt rolled back.
    expect(units).toBe(2)
    expect(execs.filter((sql) => sql.startsWith("SAVEPOINT"))).toHaveLength(2)
    expect(execs.filter((sql) => sql.startsWith("ROLLBACK TO"))).toHaveLength(1)
    expect(native.all("SELECT content FROM todo")).toEqual([{ content: "retry me" }])
  })

  test("exhaustion is the typed BusyExhaustedError, never a raw database-is-locked error", () => {
    const filename = temporaryDatabase()
    const holder = open(filename, 0)
    holder.exec(TODO_DDL)
    const contender = open(filename, 1)
    const repository = repositoryOver(contender)
    holder.exec("BEGIN IMMEDIATE")

    let failure: unknown
    try {
      repository.insert("todo", todo)
    } catch (error) {
      failure = error
    } finally {
      holder.exec("ROLLBACK")
    }

    expect(failure).toBeInstanceOf(BusyExhaustedError)
    expect((failure as BusyExhaustedError).tag).toBe("SQLITE_BUSY_EXHAUSTED")
    expect((failure as Error).message).not.toContain("database is locked")
    expect(contender.all("SELECT content FROM todo")).toEqual([])
  })

  test("a non-busy failure is never retried", () => {
    const scripted = new ScriptedRepository((_sql, _occurrence, kind) => (kind === "run" ? "boom" : "ok"))
    const repository = repositoryOver(scripted as unknown as RepositoryConnection)

    expect(() => repository.insert("todo", todo)).toThrow("injected failure")
    expect(scripted.runs).toBe(1)
  })

  test("a failed rollback stops the retry with a typed transaction state error", () => {
    const scripted = new ScriptedRepository((sql, occurrence, kind) => {
      if (kind === "run" && occurrence === 1) return "busy"
      if (kind === "exec" && sql.startsWith("ROLLBACK TO")) return "boom"
      return "ok"
    })
    const repository = repositoryOver(scripted as unknown as RepositoryConnection)

    let failure: unknown
    try {
      repository.atomic(() => repository.insert("todo", todo))
    } catch (error) {
      failure = error
    }

    expect(failure).toMatchObject({
      name: "RepositoryTransactionStateError",
      tag: "DB_REPOSITORY_TRANSACTION_STATE",
    })
    // The unit was not replayed after the cleanup fault.
    expect(scripted.count("savepoint")).toBe(1)
    expect(scripted.runs).toBe(1)
  })

  test("a nested atomic joins the outer retry instead of retrying independently", () => {
    const scripted = new ScriptedRepository((_sql, occurrence, kind) =>
      kind === "run" && occurrence === 1 ? "busy" : "ok",
    )
    const repository = repositoryOver(scripted as unknown as RepositoryConnection)

    const outcome = repository.atomic(() => repository.atomic(() => repository.insert("todo", todo)))

    expect(outcome.changes).toBe(1)
    // Two savepoints per attempt, two attempts: the inner unit never retried itself.
    expect(scripted.count("savepoint")).toBe(4)
    expect(scripted.runs).toBe(2)
  })
})
