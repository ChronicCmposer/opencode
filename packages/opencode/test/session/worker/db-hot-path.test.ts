// Hot-path guarantees for the worker SQL surface.
//
// The per-statement path must not re-parse or re-prepare work it has already
// seen: SQL classification is memoized, node statements are cached per handle,
// and a single-statement repository write does not pay for a savepoint it cannot
// need. The executor must also bound its busy budget and retire the old thread
// before a replacement starts, so two SQLite handles never overlap.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { pathToFileURL } from "node:url"
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test"
import { openConnection, type Connection } from "@opencode-ai/core/database/connection"
import { migrationStatements } from "@/session/writer/migration.statements.gen"
import { classifyWorkerStatement } from "@/session/worker/db"
import {
  DbExecutorTimeoutError,
  createDbExecutor,
  transactionBoundary,
  type DbExecutor,
} from "@/session/worker/db-executor"
import { connectedRepository, sameJsonValue } from "@/session/worker/repository/write"
import { pollUntil } from "../../lib/poll"
import { DEFAULT_TEST_TIMEOUT_MS } from "../../lib/worker-timeouts"

setDefaultTimeout(DEFAULT_TEST_TIMEOUT_MS)

const directories: string[] = []
const connections: Connection[] = []
const executors: DbExecutor[] = []

function workspace(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-db-hot-path-"))
  directories.push(directory)
  return directory
}

function migratedConnection(): Connection {
  const connection = openConnection({ filename: path.join(workspace(), "opencode.db") })
  connections.push(connection)
  for (const statement of migrationStatements) connection.exec(statement)
  connection.run(
    "INSERT INTO project (id, name, worktree, sandboxes, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
    ["project-1", "project", "/tmp/hot-path", "[]", 1, 1],
  )
  connection.run(
    "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    ["session-a", "project-1", "a", "/tmp/hot-path", "a", "1", 1, 1],
  )
  connection.run("INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)", [
    "message-a",
    "session-a",
    1,
    1,
    "{}",
  ])
  return connection
}

afterEach(async () => {
  for (const executor of executors.splice(0)) await executor.close()
  for (const connection of connections.splice(0)) connection.close()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("SQL classification", () => {
  test("classifies a statement once and reuses the same result for the same SQL", () => {
    const sql = "/* hot */ INSERT INTO message (id) VALUES ('x')"
    const first = classifyWorkerStatement(sql)
    const second = classifyWorkerStatement(sql)

    expect(first.keyword).toBe("insert")
    expect(first.multipleStatements).toBe(false)
    // Identity, not just equality: the second call hit the memoized entry.
    expect(second).toBe(first)
    expect(classifyWorkerStatement("SELECT 1; DROP TABLE message").multipleStatements).toBe(true)
  })

  test("classifies a trailing COMMIT; and a commented BEGIN as single statements", () => {
    expect(classifyWorkerStatement("COMMIT;")).toMatchObject({ keyword: "commit", multipleStatements: false })
    expect(classifyWorkerStatement("/* note */ BEGIN")).toMatchObject({ keyword: "begin", multipleStatements: false })
    expect(classifyWorkerStatement("-- note\nBEGIN")).toMatchObject({ keyword: "begin", multipleStatements: false })
    expect(classifyWorkerStatement("BEGIN; COMMIT;").multipleStatements).toBe(true)
  })
})

describe("JSON value comparison", () => {
  test("compares metadata values by JSON semantics, not JS object identity", () => {
    // Object key order is irrelevant; array order is not.
    expect(sameJsonValue({ depth: 1, note: "kept" }, { note: "kept", depth: 1 })).toBe(true)
    expect(sameJsonValue([1, 2], [2, 1])).toBe(false)
    // JSON drops undefined object members and turns undefined array items into
    // null; non-finite numbers serialize as null; -0 and 0 are the same number.
    expect(sameJsonValue({ depth: undefined }, {})).toBe(true)
    expect(sameJsonValue([undefined], [null])).toBe(true)
    expect(sameJsonValue(Number.NaN, null)).toBe(true)
    expect(sameJsonValue(-0, 0)).toBe(true)
    // Non-JSON objects must not compare structurally equal to an empty object.
    expect(sameJsonValue(new Date(0), new Date(0))).toBe(false)
    // A JSON type mismatch is a change.
    expect(sameJsonValue({ depth: 1 }, { depth: "1" })).toBe(false)
  })
})

describe("repository hot path", () => {
  test("a single-statement write does not open a redundant savepoint", () => {
    const connection = migratedConnection()
    const executed: string[] = []
    const repository = connectedRepository({
      all: (sql, params) => connection.all(sql, params),
      values: (sql, params) => connection.values(sql, params),
      run: (sql, params) => {
        executed.push(sql)
        return connection.run(sql, params)
      },
      exec: (sql) => {
        executed.push(sql)
        connection.exec(sql)
      },
    })

    repository.update("message", { data: '{"text":"updated"}' }, { id: "message-a" })
    expect(executed.filter((sql) => /^\s*savepoint\b/i.test(sql))).toEqual([])
    expect(executed.some((sql) => /^UPDATE main\."message"/.test(sql))).toBe(true)
  })

  test("an update validated against immutable session columns runs inside a savepoint", () => {
    const connection = migratedConnection()
    const executed: string[] = []
    const repository = connectedRepository({
      all: (sql, params) => connection.all(sql, params),
      values: (sql, params) => connection.values(sql, params),
      run: (sql, params) => {
        executed.push(sql)
        return connection.run(sql, params)
      },
      exec: (sql) => {
        executed.push(sql)
        connection.exec(sql)
      },
    })

    repository.update("session", { title: "renamed" }, { id: "session-a" })

    const savepoint = executed.findIndex((sql) => /^\s*savepoint\b/i.test(sql))
    const write = executed.findIndex((sql) => /^UPDATE main\."session"/.test(sql))
    const release = executed.findIndex((sql) => /^\s*release\b/i.test(sql))
    expect(savepoint).toBeGreaterThanOrEqual(0)
    expect(write).toBeGreaterThan(savepoint)
    expect(release).toBeGreaterThan(write)
  })

  test("a multi-statement batch still keeps its savepoint", () => {
    const connection = migratedConnection()
    const executed: string[] = []
    const repository = connectedRepository({
      all: (sql, params) => connection.all(sql, params),
      values: (sql, params) => connection.values(sql, params),
      run: (sql, params) => {
        executed.push(sql)
        return connection.run(sql, params)
      },
      exec: (sql) => {
        executed.push(sql)
        connection.exec(sql)
      },
    })

    repository.insertMany("message", [
      { id: "message-b", session_id: "session-a", time_created: 2, time_updated: 2, data: "{}" },
      { id: "message-c", session_id: "session-a", time_created: 3, time_updated: 3, data: "{}" },
    ])

    expect(executed.some((sql) => /^\s*savepoint\b/i.test(sql))).toBe(true)
  })
})

describe("executor hot path", () => {
  test("opens with the bounded busy timeout and the 16 MiB page cache", async () => {
    const executor = createDbExecutor({ filename: ":memory:" })
    executors.push(executor)

    expect(await executor.query("PRAGMA busy_timeout")).toEqual([{ timeout: 1_000 }])
    expect(await executor.query("PRAGMA cache_size")).toEqual([{ cache_size: -16_384 }])
  })
})

const BUSY_ONCE_WORKER_SOURCE = `
import { parentPort, threadId } from "node:worker_threads"

let runs = 0
let queries = 0
const busy = (id: number) =>
  parentPort!.postMessage({
    type: "failure",
    id,
    error: { name: "SQLiteError", message: "database is locked", code: "SQLITE_BUSY" },
  })

parentPort!.postMessage({ type: "ready", threadId })
parentPort!.on("message", (message: { type: string; id: number }) => {
  if (message.type === "close") {
    parentPort!.postMessage({ type: "closed", id: message.id })
    return
  }
  if (message.type === "run") {
    runs += 1
    if (runs === 1) return busy(message.id)
    parentPort!.postMessage({ type: "result", id: message.id, value: { changes: 1, lastInsertRowid: 1 } })
    return
  }
  if (message.type === "query") {
    queries += 1
    if (queries === 1) return busy(message.id)
    parentPort!.postMessage({ type: "result", id: message.id, value: [{ n: 1 }] })
  }
})
`

describe("standalone executor busy retry", () => {
  test("retries a busy run and query within the bounded budget", async () => {
    const directory = workspace()
    const worker = path.join(directory, "busy-once-worker.ts")
    await Bun.write(worker, BUSY_ONCE_WORKER_SOURCE)

    const executor = createDbExecutor({
      filename: ":memory:",
      entry: pathToFileURL(worker),
      timeoutMs: 2_000,
    })
    executors.push(executor)

    // Both calls receive SQLITE_BUSY on their first attempt; the executor must
    // retry the standalone statement instead of surfacing the transient lock.
    expect(await executor.run("INSERT INTO item VALUES (1)")).toEqual({ changes: 1, lastInsertRowid: 1 })
    expect(await executor.query("SELECT 1")).toEqual([{ n: 1 }])
  })
})

const EXECUTOR_WORKER_SOURCE = `
import fs from "node:fs"
import { parentPort, threadId, workerData } from "node:worker_threads"

const log = String((workerData as { filename?: string }).filename ?? "")
const append = (line: string) => fs.appendFileSync(log, line + "\\n")

append("ready:" + threadId)
parentPort!.postMessage({ type: "ready", threadId })
parentPort!.on("message", (message: { type: string; id: number; sql?: string }) => {
  if (message.type === "close") {
    // Deliberately slow: the replacement must wait for this acknowledgement.
    setTimeout(() => {
      append("closed:" + threadId)
      parentPort!.postMessage({ type: "closed", id: message.id })
    }, 500)
    return
  }
  if (message.type === "query") {
    if (message.sql === "SLOW") return
    parentPort!.postMessage({ type: "result", id: message.id, value: [{ n: 1 }] })
    return
  }
  if (message.type === "run") {
    parentPort!.postMessage({ type: "result", id: message.id, value: { changes: 0, lastInsertRowid: 0 } })
    return
  }
  if (message.type === "transaction") {
    parentPort!.postMessage({ type: "result", id: message.id, value: [] })
  }
})
`

describe("executor replacement", () => {
  test("retires the timed-out thread before spawning its replacement", async () => {
    const directory = workspace()
    const log = path.join(directory, "executor-events.log")
    const worker = path.join(directory, "executor-worker.ts")
    await Bun.write(worker, EXECUTOR_WORKER_SOURCE)

    const executor = createDbExecutor({
      filename: log,
      entry: pathToFileURL(worker),
      timeoutMs: 100,
    })
    executors.push(executor)

    expect(await executor.query("SELECT 1")).toEqual([{ n: 1 }])
    const firstThread = executor.threadId
    await expect(executor.query("SLOW")).rejects.toBeInstanceOf(DbExecutorTimeoutError)

    // This request must not start until the old isolate acknowledged close.
    expect(await executor.query("SELECT 1")).toEqual([{ n: 1 }])
    const secondThread = executor.threadId
    expect(secondThread).not.toBe(firstThread)

    const events = fs.readFileSync(log, "utf8").trim().split("\n")
    const closed = events.indexOf(`closed:${firstThread}`)
    const ready = events.lastIndexOf(`ready:${secondThread}`)
    expect(closed).toBeGreaterThanOrEqual(0)
    expect(ready, JSON.stringify({ events, firstThread, secondThread })).toBeGreaterThan(closed)
  })

  test("classifies statements that open or close a caller-driven transaction", () => {
    expect(transactionBoundary("BEGIN IMMEDIATE")).toBe("begin")
    expect(transactionBoundary("  begin")).toBe("begin")
    expect(transactionBoundary("COMMIT")).toBe("end")
    expect(transactionBoundary("ROLLBACK")).toBe("end")
    expect(transactionBoundary("ROLLBACK TO sp")).toBeUndefined()
    expect(transactionBoundary("SELECT 1")).toBeUndefined()
  })

  test("the periodic checkpoint cannot interleave with an open migration transaction", async () => {
    const outcomes: string[] = []
    const executor = createDbExecutor({
      filename: path.join(workspace(), "checkpoint.db"),
      checkpointIntervalMs: 5,
      onCheckpoint: (outcome) => void outcomes.push(outcome),
    })
    executors.push(executor)
    await executor.query("SELECT 1")
    await pollUntil(() => outcomes.includes("issued"), { message: "no checkpoint was issued while idle" })

    await executor.queryInTransaction!("BEGIN IMMEDIATE")
    outcomes.length = 0
    await pollUntil(() => outcomes.includes("deferred"), { message: "checkpoint was not deferred during the transaction" })
    expect(outcomes).not.toContain("issued")

    await executor.queryInTransaction!("COMMIT")
    outcomes.length = 0
    await pollUntil(() => outcomes.includes("issued"), { message: "checkpoint did not resume after commit" })
  })
})
