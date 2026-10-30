// Typed-session migration (RED-first).
//
// This lane owns the worker's remaining direct session/message/part/ledger
// writes. The inventory is:
//
//   * session/message/part mutations are event-sourced (`Session.update*` ->
//     durable event -> core SessionProjector), so there is no local DML in
//     `session.ts`/`prompt.ts`/`processor.ts` to migrate;
//   * the tool-call ledger is the one raw writer (`INSERT`/`UPDATE tool_call`,
//     plus the `finishWithResult` savepoint) and must move to the typed
//     repository surface from `@/session/worker/repository`;
//   * `SessionTools.resolve` must derive that ledger from
//     `WorkerDb.repository`, never the raw scoped connection.
//
// The suite therefore asserts two things against production code: no raw write
// survives in the lane's write surface, and the typed ledger keeps the durable
// start/finish/indeterminate/result-transaction contract on a real database.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { openConnection } from "@opencode-ai/core/database/connection"
import { migrationStatements } from "@/session/writer/migration.statements.gen"
import { markSchemaIdentity, openWorkerDatabase, type WorkerDatabase } from "@/session/worker/db"
import { createBunSqliteConnection } from "@/session/writer/connection"
import {
  LEDGER_SQL,
  ToolCallAlreadyStartedError,
  ToolCallNotStartedError,
  createToolCallLedger,
} from "@/session/ledger/tool-call"

const sourceRoot = path.resolve(import.meta.dir, "../../src")
const readSource = (relative: string) => fs.readFileSync(path.join(sourceRoot, relative), "utf8")

const directories: string[] = []
const cleanups: Array<() => void | Promise<void>> = []

function workspace(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-typed-session-"))
  directories.push(directory)
  return directory
}

function migratedDatabase(): string {
  const filename = path.join(workspace(), "opencode.db")
  const connection = openConnection({ filename })
  try {
    for (const statement of migrationStatements) connection.exec(statement)
    markSchemaIdentity(connection)
    connection.run(
      "INSERT INTO project (id, name, worktree, sandboxes, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
      ["project-1", "project", "/tmp/typed-session", "[]", 1, 1],
    )
    connection.run(
      "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      ["ses_typed", "project-1", "typed", "/tmp/typed-session", "typed", "1", 1, 1],
    )
  } finally {
    connection.close()
  }
  return filename
}

async function openBound(filename: string, sessionID = "ses_typed"): Promise<WorkerDatabase> {
  const database = await Effect.runPromise(openWorkerDatabase({ filename, sessionID }))
  cleanups.push(() => Effect.runPromise(database.close))
  return database
}

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

function ledgerOnlyConnection() {
  const connection = createBunSqliteConnection({ filename: ":memory:" })
  connection.exec(LEDGER_SQL)
  return connection
}

describe("no raw writes in the lane's write surface", () => {
  test("the tool-call ledger has no row DML or raw connection write", () => {
    const file = readSource("session/ledger/tool-call.ts")
    expect(file).not.toMatch(/\bINSERT\s+INTO\b/i)
    // The single row update is the discard-time abandonment sweep, conditional
    // on `state = 'started'` and the message key prefix.
    const updates = file.match(/\bUPDATE\s+tool_call\b/gi) ?? []
    expect(updates).toHaveLength(1)
    expect(file).toMatch(/outcome = 'abandoned'/)
    expect(file).not.toMatch(/DELETE\s+FROM\s+tool_call/i)
    expect(file).not.toMatch(/\bDELETE\s+FROM\b/i)
    expect(file).not.toMatch(/connection\.run\s*\(/)
    // The result/finish pair is scoped by the repository capability, not raw SQL.
    expect(file).toMatch(/\.atomic\s*\(/)
    expect(file).not.toMatch(/exec\s*\(\s*[`"]\s*SAVEPOINT/i)
    expect(file).not.toMatch(/exec\s*\(\s*[`"]\s*ROLLBACK\s+TO/i)
    expect(file).not.toMatch(/\.exec\s*\(\s*[`"]BEGIN/i)
    expect(file).not.toMatch(/\.exec\s*\(\s*[`"]COMMIT/i)
  })

  test("tools.ts derives the ledger from the typed repository", () => {
    const file = readSource("session/tools.ts")
    expect(file).toMatch(/workerDb\.value\.repository/)
    expect(file).not.toMatch(/createToolCallLedger\s*\(\s*connection\s*\)/)
  })

  test("session writes stay event-sourced, not ad-hoc DML", () => {
    for (const file of ["session/session.ts", "session/prompt.ts", "session/processor.ts"]) {
      const source = readSource(file)
      expect(source).not.toMatch(/\bINSERT\s+INTO\s+(session|message|part)\b/i)
      expect(source).not.toMatch(/\bUPDATE\s+(session|message|part)\b/i)
      expect(source).not.toMatch(/\bDELETE\s+FROM\s+(session|message|part)\b/i)
    }
  })
})

describe("the typed ledger keeps the durable contract", () => {
  test("start/finish/indeterminate over a ledger-only DB", () => {
    const connection = ledgerOnlyConnection()
    const ledger = createToolCallLedger(connection)

    ledger.start({ callID: "call_1", sessionID: "ses_1", tool: "write", timeStarted: 1 })
    expect(() => ledger.start({ callID: "call_1", sessionID: "ses_1", tool: "write" })).toThrow(
      ToolCallAlreadyStartedError,
    )
    expect(ledger.list("ses_1")[0]).toMatchObject({ callID: "call_1", state: "started" })
    expect(ledger.indeterminate("ses_1")[0]).toMatchObject({ callID: "call_1", state: "indeterminate" })
    expect(ledger.hasSideEffectingStarted("ses_1")).toBe(true)

    expect(() => ledger.finish({ callID: "missing", sessionID: "ses_1", outcome: "ok" })).toThrow(
      ToolCallNotStartedError,
    )
    expect(ledger.finish({ callID: "call_1", sessionID: "ses_1", outcome: "ok", timeFinished: 2 })).toMatchObject({
      state: "finished",
      outcome: "ok",
    })
    expect(ledger.indeterminate("ses_1")).toEqual([])
  })

  test("finishWithResult commits and rolls back the finish with the result", () => {
    const connection = ledgerOnlyConnection()
    const ledger = createToolCallLedger(connection)
    ledger.start({ callID: "call_1", sessionID: "ses_1", tool: "write" })

    expect(() =>
      ledger.finishWithResult({ callID: "call_1", sessionID: "ses_1", outcome: "ok" }, () => {
        throw new Error("result write failed")
      }),
    ).toThrow("result write failed")
    expect(ledger.list("ses_1")[0]?.state).toBe("started")

    ledger.finishWithResult({ callID: "call_1", sessionID: "ses_1", outcome: "ok" }, () => undefined)
    expect(ledger.list("ses_1")[0]?.state).toBe("finished")
  })

  test("a bound WorkerDb repository scopes the ledger to its session", async () => {
    const filename = migratedDatabase()
    const database = await openBound(filename)
    const ledger = createToolCallLedger(database.repository)

    ledger.start({ callID: "call_bound", sessionID: "ses_typed", tool: "write" })
    expect(ledger.list("ses_typed")).toEqual([expect.objectContaining({ callID: "call_bound", state: "started" })])

    // Another session's id is not writable through the bound repository.
    expect(() => ledger.start({ callID: "call_foreign", sessionID: "ses_other", tool: "write" })).toThrow()
    expect(ledger.list("ses_typed").map((call) => call.callID)).toEqual(["call_bound"])

    // The result/finish pair uses the repository's own transaction scope.
    ledger.finishWithResult({ callID: "call_bound", sessionID: "ses_typed", outcome: "ok" }, () => undefined)
    expect(ledger.list("ses_typed")[0]?.state).toBe("finished")
  })
})
