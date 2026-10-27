// Typed-repository hardening (wave 3 security follow-up).
//
// These cases are RED-first against the production surfaces:
//
//   1. `insertMany("session")` must not mint a session root (the per-row guard
//      that `insertOne`/`upsert` already apply must cover batches too);
//   2. `repository.atomic` must not hold the `withMain` authorizer bracket across
//      an arbitrary callback, and a callback cannot COMMIT/ROLLBACK out of it;
//   3. a session update may only touch worker-owned projection columns, never
//      billing/share/revert authority columns;
//   4. foreign `pluginTool.execute`/`pluginTool.abort` and `cancelSession` calls
//      are rejected by the bound hub authorities;
//   5. a second `ensureProductionSchema` on an already-current database returns
//      the identity with live workers, because the zero-live guard only applies
//      when migration/DDL is actually needed.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { openConnection } from "@opencode-ai/core/database/connection"
import { Permission } from "@/permission"
import { Question } from "@/question"
import { SessionID } from "@/session/schema"
import { SessionStatus } from "@/session/status"
import {
  currentSchemaIdentity,
  ensureProductionSchema,
  markSchemaIdentity,
  resetSchemaEnsureCache,
} from "@/session/db/schema"
import { openWorkerDatabase, type WorkerDatabase } from "@/session/worker/db"
import { hubProxyAuthoritiesFor, type HubProxyRunner } from "@/session/worker/hub"
import type { PluginToolExecuteInput } from "@/session/worker/plugin-tool-wire"
import { migrationStatements } from "@/session/writer/migration.statements.gen"
import { instantRunner } from "../lib/hub-proxy"

const PROJECT = "project-hardening"
const DIRECTORY = "/tmp/hardening"
const SESSION_A = "session-a"
const SESSION_B = "session-b"

const directories: string[] = []
const cleanups: Array<() => void | Promise<void>> = []

function workspace(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-typed-hardening-"))
  directories.push(directory)
  return directory
}

function migratedDatabase(): string {
  const filename = path.join(workspace(), "opencode.db")
  const connection = openConnection({ filename })
  try {
    for (const statement of migrationStatements) connection.exec(statement)
    markSchemaIdentity(connection)
  } finally {
    connection.close()
  }
  return filename
}

function seed(filename: string): void {
  const connection = openConnection({ filename })
  try {
    connection.run(
      "INSERT INTO project (id, name, worktree, sandboxes, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
      [PROJECT, "hardening", DIRECTORY, "[]", 1, 1],
    )
    for (const id of [SESSION_A, SESSION_B]) {
      connection.run(
        "INSERT INTO session (id, project_id, slug, directory, title, version, metadata, cost, share_url, revert, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
          id,
          PROJECT,
          id,
          DIRECTORY,
          id,
          "1",
          JSON.stringify({ depth: id === SESSION_A ? 1 : 2 }),
          10,
          `https://share.test/${id}`,
          "{}",
          1,
          1,
        ],
      )
      connection.run(
        "INSERT INTO session_share (session_id, id, secret, url, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
        [id, `share-${id}`, `secret-${id}`, `https://example.test/${id}`, 1, 1],
      )
      connection.run("INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)", [
        `message-${id}`,
        id,
        1,
        1,
        JSON.stringify({ owner: id }),
      ])
    }
  } finally {
    connection.close()
  }
}

async function openBound(filename: string, sessionID = SESSION_A): Promise<WorkerDatabase> {
  const database = await Effect.runPromise(openWorkerDatabase({ filename, sessionID }))
  cleanups.push(() => Effect.runPromise(database.close))
  return database
}

function admin<Row = Record<string, unknown>>(
  filename: string,
  sql: string,
  params: ReadonlyArray<unknown> = [],
): ReadonlyArray<Row> {
  const connection = openConnection({ filename })
  try {
    return connection.all<Row>(sql, params as never)
  } finally {
    connection.close()
  }
}

interface Outcome {
  readonly ok: boolean
  readonly value?: unknown
  readonly error?: unknown
}

async function settle(operation: () => unknown): Promise<Outcome> {
  try {
    const value = operation()
    if (Effect.isEffect(value)) {
      try {
        return { ok: true, value: await Effect.runPromise(value as Effect.Effect<unknown, unknown, never>) }
      } catch (error) {
        return { ok: false, error }
      }
    }
    if (value instanceof Promise) {
      try {
        return { ok: true, value: await value }
      } catch (error) {
        return { ok: false, error }
      }
    }
    return { ok: true, value }
  } catch (error) {
    return { ok: false, error }
  }
}

function snapshot(filename: string) {
  return {
    session: admin(
      filename,
      "SELECT id, project_id, parent_id, slug, directory, title, metadata, cost, tokens_input, share_url, revert FROM session ORDER BY id",
    ),
    message: admin(filename, "SELECT id, session_id, data FROM message ORDER BY id"),
  }
}

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
  resetSchemaEnsureCache()
})

describe("session root minting", () => {
  test("insertMany cannot mint a missing root for the bound session id", async () => {
    const filename = migratedDatabase()
    const connection = openConnection({ filename })
    try {
      connection.run(
        "INSERT INTO project (id, name, worktree, sandboxes, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
        [PROJECT, "hardening", DIRECTORY, "[]", 1, 1],
      )
    } finally {
      connection.close()
    }

    // No `session-minted` row exists yet; binding to that id is exactly the
    // worker-without-a-root case a batch mint would forge.
    const database = await openBound(filename, "session-minted")
    const outcome = await settle(() =>
      database.repository.insertMany("session", [
        {
          id: "session-minted",
          project_id: PROJECT,
          slug: "minted",
          directory: DIRECTORY,
          title: "minted",
          version: "1",
          parent_id: "session-forged-parent",
          metadata: JSON.stringify({ depth: 0 }),
          time_created: 1,
          time_updated: 1,
        },
      ]),
    )

    expect(outcome.ok, "insertMany minted a session root").toBe(false)
    expect(admin(filename, "SELECT id FROM session WHERE id = 'session-minted'")).toEqual([])
  })
})

describe("atomic callback authority", () => {
  test("a callback cannot read a foreign session's main row", async () => {
    const filename = migratedDatabase()
    seed(filename)
    const database = await openBound(filename)

    const outcome = await settle(() =>
      database.repository.atomic(() =>
        database.connection.all("SELECT session_id, secret FROM main.session_share WHERE session_id = ?", [SESSION_B]),
      ),
    )
    expect(outcome.ok, "atomic allowed a raw main read of a foreign session_share row").toBe(false)

    // The live own-session view remains readable outside the bracket.
    expect(database.connection.all("SELECT session_id, secret FROM session_share ORDER BY session_id")).toEqual([
      { session_id: SESSION_A, secret: `secret-${SESSION_A}` },
    ])
  })

  test("a callback cannot COMMIT its write out of the savepoint", async () => {
    const filename = migratedDatabase()
    seed(filename)
    const database = await openBound(filename)
    const before = snapshot(filename)

    const outcome = await settle(() =>
      database.repository.atomic(() => {
        database.repository.insert("message", {
          id: "message-commit",
          session_id: SESSION_A,
          time_created: 1,
          time_updated: 1,
          data: "{}",
        })
        database.connection.exec("COMMIT")
      }),
    )

    expect(outcome.ok, "atomic accepted a COMMIT from its callback").toBe(false)
    expect(admin(filename, "SELECT id FROM message WHERE id = 'message-commit'")).toEqual([])
    expect(snapshot(filename)).toEqual(before)
  })

  test("a callback cannot ROLLBACK the caller's outer transaction", async () => {
    const filename = migratedDatabase()
    seed(filename)
    const database = await openBound(filename)

    database.connection.exec("BEGIN")
    const outer = await settle(() =>
      database.repository.insert("message", {
        id: "message-outer",
        session_id: SESSION_A,
        time_created: 1,
        time_updated: 1,
        data: "{}",
      }),
    )
    expect(outer.ok, `outer write failed: ${String(outer.error)}`).toBe(true)

    const inner = await settle(() =>
      database.repository.atomic(() => {
        database.repository.insert("message", {
          id: "message-inner",
          session_id: SESSION_A,
          time_created: 1,
          time_updated: 1,
          data: "{}",
        })
        database.connection.exec("ROLLBACK")
      }),
    )
    await settle(() => database.connection.exec("COMMIT"))

    expect(inner.ok, "atomic accepted a ROLLBACK from its callback").toBe(false)
    expect(admin(filename, "SELECT id FROM message WHERE id = 'message-outer'")).toEqual([{ id: "message-outer" }])
    expect(admin(filename, "SELECT id FROM message WHERE id = 'message-inner'")).toEqual([])
  })
})

describe("session update columns", () => {
  test("billing, share and revert authority columns stay hub-owned", async () => {
    const filename = migratedDatabase()
    seed(filename)
    const database = await openBound(filename)
    const before = snapshot(filename)

    const attempts: ReadonlyArray<Record<string, unknown>> = [
      { cost: 999 },
      { tokens_input: 999 },
      { tokens_output: 999 },
      { tokens_reasoning: 999 },
      { tokens_cache_read: 999 },
      { tokens_cache_write: 999 },
      { share_url: "https://evil.test" },
      { revert: '{"messageID":"forged"}' },
    ]
    for (const values of attempts) {
      const outcome = await settle(() => database.repository.update("session", values, { id: SESSION_A }))
      expect(outcome.ok, `session update accepted ${JSON.stringify(values)}`).toBe(false)
    }

    // A worker-owned projection column still updates normally.
    expect(database.repository.update("session", { title: "hardened" }, { id: SESSION_A }).changes).toBe(1)
    expect(admin(filename, "SELECT title FROM session WHERE id = ?", [SESSION_A])).toEqual([{ title: "hardened" }])
    expect(snapshot(filename)).not.toEqual(before)
  })
})

describe("bound hub authorities", () => {
  const idleStatus: SessionStatus.Interface = {
    get: () => Effect.succeed({ type: "idle" }),
    set: () => Effect.void,
    list: () => Effect.succeed(new Map()),
  }

  interface Recorders {
    readonly executes: PluginToolExecuteInput[]
    readonly aborts: { sessionID: string; callID: string }[]
    readonly permissionCancels: string[]
    readonly questionCancels: string[]
  }

  function authoritiesWith(recorders: Recorders) {
    const permission = {
      ask: () => Effect.void,
      reply: () => Effect.void,
      list: () => Effect.succeed([]),
      cancelSession: (sessionID: string) => {
        recorders.permissionCancels.push(sessionID)
        return Effect.void
      },
    } as unknown as Permission.Interface

    const question = {
      ask: () => Effect.succeed([]),
      reply: () => Effect.void,
      reject: () => Effect.void,
      list: () => Effect.succeed([]),
      cancelSession: (sessionID: string) => {
        recorders.questionCancels.push(sessionID)
        return Effect.void
      },
    } as unknown as Question.Interface

    return hubProxyAuthoritiesFor({
      run: instantRunner as HubProxyRunner,
      boundSessionID: SessionID.make(SESSION_A),
      subagentDepth: 1,
      recordDepth: () => {},
      registerChild: () => {},
      sessionStatus: idleStatus,
      permission,
      question,
      pluginTool: async (input) => {
        recorders.executes.push(input)
        return { title: "ran", output: `HUB:${input.sessionID}`, metadata: {} }
      },
      pluginToolAbort: (input) => {
        recorders.aborts.push(input)
      },
    })
  }

  test("foreign pluginTool execute/abort are rejected", async () => {
    const recorders: Recorders = { executes: [], aborts: [], permissionCancels: [], questionCancels: [] }
    const authorities = authoritiesWith(recorders)
    const execute = authorities.pluginTool
    const abort = authorities.pluginToolAbort
    expect(execute, "pluginTool authority is missing").toBeDefined()
    expect(abort, "pluginToolAbort authority is missing").toBeDefined()

    const foreign = await settle(() =>
      execute!({
        toolID: "hardening",
        sessionID: SESSION_B,
        messageID: "message-hardening",
        callID: "call-hardening",
        agent: "build",
        args: {},
      }, () => {}),
    )
    expect(foreign.ok, "a foreign pluginTool.execute reached the hub plugin").toBe(false)
    expect(recorders.executes).toEqual([])

    const foreignAbort = await settle(() => abort!({ sessionID: SESSION_B, callID: "call-hardening" }))
    expect(foreignAbort.ok, "a foreign pluginTool.abort reached the hub registry").toBe(false)
    expect(recorders.aborts).toEqual([])

    const own = await settle(() =>
      execute!({
        toolID: "hardening",
        sessionID: SESSION_A,
        messageID: "message-hardening",
        callID: "call-hardening",
        agent: "build",
        args: {},
      }, () => {}),
    )
    expect(own.ok, `own pluginTool.execute failed: ${String(own.error)}`).toBe(true)
    await settle(() => abort!({ sessionID: SESSION_A, callID: "call-hardening" }))
    expect(recorders.executes.map((call) => call.sessionID)).toEqual([SESSION_A])
    expect(recorders.aborts).toEqual([{ sessionID: SESSION_A, callID: "call-hardening" }])
  })

  test("foreign cancelSession is rejected, own session still cancels", async () => {
    const recorders: Recorders = { executes: [], aborts: [], permissionCancels: [], questionCancels: [] }
    const authorities = authoritiesWith(recorders)
    const permissionCancel = authorities.permission.cancelSession
    const questionCancel = authorities.question.cancelSession
    expect(permissionCancel, "permission.cancelSession is missing").toBeDefined()
    expect(questionCancel, "question.cancelSession is missing").toBeDefined()

    const foreignPermission = await settle(() => instantRunner(permissionCancel!(SessionID.make(SESSION_B))))
    const foreignQuestion = await settle(() => instantRunner(questionCancel!(SessionID.make(SESSION_B))))
    expect(foreignPermission.ok, "a foreign permission.cancelSession was accepted").toBe(false)
    expect(foreignQuestion.ok, "a foreign question.cancelSession was accepted").toBe(false)
    expect(recorders.permissionCancels).toEqual([])
    expect(recorders.questionCancels).toEqual([])

    const ownPermission = await settle(() => instantRunner(permissionCancel!(SessionID.make(SESSION_A))))
    const ownQuestion = await settle(() => instantRunner(questionCancel!(SessionID.make(SESSION_A))))
    expect(ownPermission.ok, `own permission.cancelSession failed: ${String(ownPermission.error)}`).toBe(true)
    expect(ownQuestion.ok, `own question.cancelSession failed: ${String(ownQuestion.error)}`).toBe(true)
    expect(recorders.permissionCancels).toEqual([SESSION_A])
    expect(recorders.questionCancels).toEqual([SESSION_A])
  })
})

describe("production schema re-ensure", () => {
  test("a second ensure on an already-current database succeeds with live workers", async () => {
    const filename = migratedDatabase()
    seed(filename)

    expect(ensureProductionSchema({ filename, liveWorkers: 0 })).toEqual(currentSchemaIdentity())
    const database = await openBound(filename)
    expect(database.identity).toEqual(currentSchemaIdentity())

    expect(
      ensureProductionSchema({ filename, liveWorkers: 1 }),
      "second ensure re-ran the zero-live guard on an already-current schema",
    ).toEqual(currentSchemaIdentity())

    // A fresh process cannot use the cache, but it can still probe a current
    // database without taking the zero-live migration guard.
    resetSchemaEnsureCache()
    expect(ensureProductionSchema({ filename, liveWorkers: 1 })).toEqual(currentSchemaIdentity())
  })

  test("a database that still needs migration refuses to ensure with live workers", () => {
    const filename = path.join(workspace(), "needs-migration.db")
    expect(() => ensureProductionSchema({ filename, liveWorkers: 1 })).toThrow(/zero live workers/)
    expect(fs.existsSync(filename)).toBe(false)
  })
})
