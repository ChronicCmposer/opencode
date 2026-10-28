// Typed-repository security follow-up — adversarial cases.
//
// Every case drives a REAL migrated SQLite file and the REAL production
// surface under test: `openWorkerDatabase({ sessionID })` for the typed
// repository/connection, `EventV2` + `SessionProjector` for the durable-event
// path, the production hub authority map for the control port, and a real
// `Bus` over a migrated file for the blob paths. No parser-only assertions and
// no mocks of the exact boundary being verified.
//
// Cases covered:
//   1. Forged Session.Created/Updated/Deleted/MessageUpdated/PartUpdated events
//      published by a bound worker must not reset depth/immutable identity, mint
//      or delete the session root, or write another session's message/part.
//   2. `insertMany("session")` must not mint a session root.
//   3. `repository.atomic` must not hand the callback a raw `main` capability,
//      and a callback cannot COMMIT/ROLLBACK out of the savepoint.
//   4. Foreign `pluginTool.execute`/`pluginTool.abort` and `cancelSession` must
//      be rejected by the bound hub authorities.
//   5. A second `ensureProductionSchema` on an already-current DB succeeds with
//      live workers (the zero-live guard only applies when migrating).
//   6. A legacy inline `event_blob` row still reads, and a GC rollback does not
//      unlink a file that the rolled-back rows still reference.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Context, Effect, Exit, Layer, Schema, Scope } from "effect"
import { Bus } from "@opencode-ai/core/bus"
import { openConnection } from "@opencode-ai/core/database/connection"
import { EventV2 } from "@opencode-ai/core/event"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Event } from "@opencode-ai/schema/event"
import { Permission } from "@/permission"
import { Question } from "@/question"
import { SessionStatus } from "@/session/status"
import { SessionID } from "@/session/schema"
import {
  currentSchemaIdentity,
  ensureProductionSchema,
  markSchemaIdentity,
  resetSchemaEnsureCache,
} from "@/session/db/schema"
import { openWorkerDatabase, type WorkerDatabase } from "@/session/worker/db"
import {
  buildHubProxyHandlers,
  hubProxyAuthoritiesFor,
  type HubProxyRunner,
} from "@/session/worker/hub"
import type { PluginToolExecuteInput } from "@/session/worker/plugin-tool-wire"
import { migrationStatements } from "@/session/writer/migration.statements.gen"
import { ModelClient, buildSessionWorkerLayer } from "@/session/worker/layer"
import { instantRunner, serveHubProxyFor } from "../lib/hub-proxy"
import { workerSnapshot } from "../lib/snapshot"

const PROJECT = "project-wave3"
const DIRECTORY = "/tmp/wave3"
const SESSION_A = "session-a"
const SESSION_B = "session-b"

const directories: string[] = []
const cleanups: Array<() => void | Promise<void>> = []

function workspace(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-typed-wave3-"))
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

function seedProject(filename: string): void {
  const connection = openConnection({ filename })
  try {
    connection.run(
      "INSERT INTO project (id, name, worktree, sandboxes, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
      [PROJECT, "wave3", DIRECTORY, "[]", 1, 1],
    )
  } finally {
    connection.close()
  }
}

function seedWorld(filename: string): void {
  const connection = openConnection({ filename })
  try {
    connection.run(
      "INSERT INTO project (id, name, worktree, sandboxes, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
      [PROJECT, "wave3", DIRECTORY, "[]", 1, 1],
    )
    connection.run(
      "INSERT INTO session (id, project_id, slug, directory, title, version, parent_id, metadata, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      [SESSION_A, PROJECT, "a", DIRECTORY, "a", "1", null, JSON.stringify({ depth: 1 }), 1, 1],
    )
    connection.run(
      "INSERT INTO session (id, project_id, slug, directory, title, version, parent_id, metadata, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      [SESSION_B, PROJECT, "b", DIRECTORY, "b", "1", SESSION_A, JSON.stringify({ depth: 2 }), 1, 1],
    )
    for (const id of [SESSION_A, SESSION_B]) {
      connection.run("INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)", [
        `msg-${id}`,
        id,
        1,
        1,
        JSON.stringify({ owner: id }),
      ])
      connection.run(
        "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?)",
        [`prt-${id}`, `msg-${id}`, id, 1, 1, JSON.stringify({ owner: id })],
      )
      connection.run("INSERT INTO session_share (session_id, id, secret, url, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)", [
        id,
        `share-${id}`,
        `secret-${id}`,
        `https://example.test/${id}`,
        1,
        1,
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
    if (typeof (value as { then?: unknown } | null)?.then === "function") {
      try {
        return { ok: true, value: await (value as Promise<unknown>) }
      } catch (error) {
        return { ok: false, error }
      }
    }
    return { ok: true, value }
  } catch (error) {
    return { ok: false, error }
  }
}

function errorTag(error: unknown): string | undefined {
  if (error !== null && typeof error === "object" && "_tag" in error) {
    return String((error as { _tag: unknown })._tag)
  }
  return undefined
}

function snapshot(filename: string) {
  return {
    session: admin(
      filename,
      "SELECT id, project_id, parent_id, slug, directory, title, metadata FROM session ORDER BY id",
    ),
    message: admin(filename, "SELECT id, session_id, data FROM message ORDER BY id"),
    part: admin(filename, "SELECT id, message_id, session_id, data FROM part ORDER BY id"),
  }
}

function sessionInfo(id: string, overrides: Record<string, unknown> = {}): SessionV1.SessionInfo {
  return {
    id,
    slug: id,
    projectID: PROJECT,
    directory: DIRECTORY,
    title: id,
    version: "1",
    metadata: { depth: 1 },
    time: { created: 1, updated: 1 },
    ...overrides,
  } as unknown as SessionV1.SessionInfo
}

function userMessage(id: string, sessionID: string, overrides: Record<string, unknown> = {}): SessionV1.Info {
  return {
    id,
    sessionID,
    role: "user",
    time: { created: 1 },
    agent: "build",
    model: { providerID: "test", modelID: "test" },
    ...overrides,
  } as unknown as SessionV1.Info
}

function textPart(id: string, messageID: string, sessionID: string): SessionV1.Part {
  return { id, messageID, sessionID, type: "text", text: "forged" } as unknown as SessionV1.Part
}

async function projectorContext(database: WorkerDatabase, filename: string): Promise<Context.Context<never>> {
  // The production bound-worker graph, not a projector-only harness: the
  // projection policy is a non-graph seam provided only when the worker has a
  // durable session aggregate.
  const built = buildSessionWorkerLayer({
    snapshot: workerSnapshot({ directory: path.dirname(filename) }),
    env: { values: {} },
    clients: {
      writer: { query: async () => [], publish: async () => ({ seq: null, committed: false }) },
      egress: { emit: () => {} },
      backgroundJob: { wait: async () => null, cancel: async () => {} },
      database,
    },
    modelClient: Layer.succeed(ModelClient, { complete: () => Effect.succeed({ text: "unused" }) }),
    boundSessionID: SessionID.make(database.sessionID ?? SESSION_A),
  })
  const scope = Effect.runSync(Scope.make())
  cleanups.push(() => Effect.runPromise(Scope.close(scope, Exit.void)))
  return (await Effect.runPromise(Layer.buildWithScope(built.layer, scope))) as Context.Context<never>
}

async function publish(
  context: Context.Context<never>,
  definition: EventV2.Definition,
  data: never,
): Promise<Outcome> {
  return settle(() =>
    Effect.gen(function* () {
      const events = yield* EventV2.Service
      return yield* events.publish(definition, data)
    }).pipe(Effect.provide(context)),
  )
}

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
  resetSchemaEnsureCache()
})

describe("forged Session events through a real bound worker", () => {
  test("Session.Updated cannot reset depth or immutable session identity", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const context = await projectorContext(database, filename)

    // Prove the projector is wired before asking it to resist a forged payload.
    const control = await publish(context, SessionV1.Event.Updated, {
      sessionID: SESSION_A,
      info: sessionInfo(SESSION_A, { slug: "a", title: "renamed" }),
    } as never)
    expect(control.ok, `the control Session.Updated did not project: ${String(control.error)}`).toBe(true)
    expect(admin<{ title: string }>(filename, "SELECT title FROM session WHERE id = ?", [SESSION_A])).toEqual([
      { title: "renamed" },
    ])
    const before = snapshot(filename)

    const outcome = await publish(context, SessionV1.Event.Updated, {
      sessionID: SESSION_A,
      info: sessionInfo(SESSION_A, {
        slug: "forged",
        directory: "/tmp/forged",
        parentID: SESSION_B,
        metadata: { depth: 0 },
      }),
    } as never)

    // The hub owns depth and session identity; whether the event is rejected or
    // sanitized, the persisted authority row may not move.
    expect(snapshot(filename), `forged Session.Updated applied: ${String(outcome.error)}`).toEqual(before)
  })

  test("Session.Created cannot mint the bound session root", async () => {
    const filename = migratedDatabase()
    seedProject(filename)
    const database = await openBound(filename)
    const context = await projectorContext(database, filename)

    const outcome = await publish(context, SessionV1.Event.Created, {
      sessionID: SESSION_A,
      info: sessionInfo(SESSION_A, { metadata: { depth: 0 } }),
    } as never)

    expect(
      admin(filename, "SELECT id FROM session WHERE id = ?", [SESSION_A]),
      `the worker minted its session root: ${String(outcome.error)}`,
    ).toEqual([])
  })

  test("Session.Deleted cannot delete the bound session root", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const context = await projectorContext(database, filename)
    const before = snapshot(filename)

    const outcome = await publish(context, SessionV1.Event.Deleted, {
      sessionID: SESSION_A,
      info: sessionInfo(SESSION_A),
    } as never)

    expect(snapshot(filename), `forged Session.Deleted applied: ${String(outcome.error)}`).toEqual(before)
  })

  test("MessageUpdated cannot insert or overwrite another session's message", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const context = await projectorContext(database, filename)

    const control = await publish(context, SessionV1.Event.MessageUpdated, {
      sessionID: SESSION_A,
      info: userMessage(`msg-${SESSION_A}`, SESSION_A),
    } as never)
    expect(control.ok, `the control MessageUpdated did not project: ${String(control.error)}`).toBe(true)
    const controlRow = admin<{ data: string }>(filename, "SELECT data FROM message WHERE id = ?", [
      `msg-${SESSION_A}`,
    ])[0]
    expect(controlRow?.data).toContain('"role":"user"')
    expect(controlRow?.data).not.toContain('"owner"')
    const before = snapshot(filename)

    // The durable aggregate is the bound session; the forged payload aims the
    // projection at session-b's row by id.
    const overwrite = await publish(context, SessionV1.Event.MessageUpdated, {
      sessionID: SESSION_A,
      info: userMessage(`msg-${SESSION_B}`, SESSION_A, { agent: "forged" }),
    } as never)
    expect(snapshot(filename), `a forged MessageUpdated overwrote session-b: ${String(overwrite.error)}`).toEqual(before)

    const insert = await publish(context, SessionV1.Event.MessageUpdated, {
      sessionID: SESSION_A,
      info: userMessage("msg-forged", SESSION_B),
    } as never)
    expect(
      admin(filename, "SELECT id FROM message WHERE id = 'msg-forged'"),
      `a forged MessageUpdated inserted into session-b: ${String(insert.error)}`,
    ).toEqual([])
    expect(snapshot(filename)).toEqual(before)
  })

  test("PartUpdated cannot insert or overwrite another session's part", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const context = await projectorContext(database, filename)

    const control = await publish(context, SessionV1.Event.PartUpdated, {
      sessionID: SESSION_A,
      part: { id: `prt-${SESSION_A}`, messageID: `msg-${SESSION_A}`, sessionID: SESSION_A, type: "text", text: "control" },
      time: 2,
    } as never)
    expect(control.ok, `the control PartUpdated did not project: ${String(control.error)}`).toBe(true)
    const controlRow = admin<{ data: string }>(filename, "SELECT data FROM part WHERE id = ?", [
      `prt-${SESSION_A}`,
    ])[0]
    expect(controlRow?.data).toContain('"text":"control"')
    const before = snapshot(filename)

    const overwrite = await publish(context, SessionV1.Event.PartUpdated, {
      sessionID: SESSION_A,
      part: textPart(`prt-${SESSION_B}`, `msg-${SESSION_B}`, SESSION_B),
      time: 2,
    } as never)
    expect(snapshot(filename), `a forged PartUpdated overwrote session-b: ${String(overwrite.error)}`).toEqual(before)

    const insert = await publish(context, SessionV1.Event.PartUpdated, {
      sessionID: SESSION_A,
      part: textPart("prt-forged", `msg-${SESSION_B}`, SESSION_B),
      time: 2,
    } as never)
    expect(
      admin(filename, "SELECT id FROM part WHERE id = 'prt-forged'"),
      `a forged PartUpdated inserted into session-b: ${String(insert.error)}`,
    ).toEqual([])
    expect(snapshot(filename)).toEqual(before)
  })
})

describe("typed repository hardening", () => {
  test("insertMany('session') cannot mint a session root", async () => {
    const filename = migratedDatabase()
    seedProject(filename)
    const database = await openBound(filename)

    const outcome = await settle(() =>
      database.repository.insertMany("session", [
        {
          id: SESSION_A,
          project_id: PROJECT,
          slug: "minted",
          directory: DIRECTORY,
          title: "minted",
          version: "1",
          metadata: JSON.stringify({ depth: 0 }),
          time_created: 1,
          time_updated: 1,
        },
      ]),
    )

    expect(outcome.ok, "insertMany minted the session root").toBe(false)
    expect(admin(filename, "SELECT id FROM session WHERE id = ?", [SESSION_A])).toEqual([])

    // Not vacuous: the same batch API still writes an own-session table on a
    // database whose bound session root actually exists.
    const controlFilename = migratedDatabase()
    seedWorld(controlFilename)
    const controlDatabase = await openBound(controlFilename)
    const control = await settle(() =>
      controlDatabase.repository.insertMany("message", [
        { id: "msg-wave3-batch", session_id: SESSION_A, time_created: 1, time_updated: 1, data: "{}" },
      ]),
    )
    expect(control.ok, `an own-session insertMany failed: ${String(control.error)}`).toBe(true)
    expect(admin(controlFilename, "SELECT id FROM message WHERE id = 'msg-wave3-batch'")).toEqual([
      { id: "msg-wave3-batch" },
    ])
  })

  test("repository.atomic does not expose a raw main capability to its callback", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)

    // Inside the savepoint the callback may only use the typed repository; the
    // bound read surfaces stay scoped to session-a.
    const outcome = await settle(() =>
      database.repository.atomic(() =>
        database.connection.all("SELECT session_id, secret FROM main.session_share WHERE session_id = ?", [SESSION_B]),
      ),
    )
    expect(outcome.ok, "atomic() allowed a raw main read of a foreign session_share row").toBe(false)

    // The live own-session view is still readable outside the raw-main bracket.
    expect(
      database.connection.all("SELECT session_id, secret FROM session_share ORDER BY session_id"),
    ).toEqual([{ session_id: SESSION_A, secret: `secret-${SESSION_A}` }])

    // Not vacuous: an ordinary atomic write still commits.
    const control = await settle(() =>
      database.repository.atomic(() =>
        database.repository.insert("message", {
          id: "msg-wave3-atomic-ok",
          session_id: SESSION_A,
          time_created: 1,
          time_updated: 1,
          data: "{}",
        }),
      ),
    )
    expect(control.ok, `a legitimate atomic write failed: ${String(control.error)}`).toBe(true)
    expect(admin(filename, "SELECT id FROM message WHERE id = 'msg-wave3-atomic-ok'")).toEqual([
      { id: "msg-wave3-atomic-ok" },
    ])
  })

  test("a callback cannot COMMIT its write out of the repository savepoint", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)
    const before = snapshot(filename)

    const outcome = await settle(() =>
      database.repository.atomic(() => {
        database.repository.insert("message", {
          id: "wave3-atomic-commit",
          session_id: SESSION_A,
          time_created: 1,
          time_updated: 1,
          data: "{}",
        })
        database.connection.exec("COMMIT")
      }),
    )

    expect(outcome.ok, "atomic() accepted a COMMIT from its callback").toBe(false)
    expect(
      admin(filename, "SELECT id FROM message WHERE id = 'wave3-atomic-commit'"),
      "the callback COMMIT persisted a write the atomic failed to release",
    ).toEqual([])
    expect(snapshot(filename)).toEqual(before)
  })

  test("a callback cannot ROLLBACK the caller's outer transaction", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)
    const database = await openBound(filename)

    database.connection.exec("BEGIN")
    const outer = await settle(() =>
      database.repository.insert("message", {
        id: "wave3-atomic-outer",
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
          id: "wave3-atomic-inner",
          session_id: SESSION_A,
          time_created: 1,
          time_updated: 1,
          data: "{}",
        })
        database.connection.exec("ROLLBACK")
      }),
    )
    await settle(() => database.connection.exec("COMMIT"))

    expect(inner.ok, "atomic() accepted a ROLLBACK from its callback").toBe(false)
    expect(
      admin(filename, "SELECT id FROM message WHERE id = 'wave3-atomic-outer'"),
      "the callback ROLLBACK discarded the caller's outer transaction",
    ).toEqual([{ id: "wave3-atomic-outer" }])
    expect(admin(filename, "SELECT id FROM message WHERE id = 'wave3-atomic-inner'")).toEqual([])
  })
})

describe("bound hub authorities", () => {
  const idleStatus: SessionStatus.Interface = {
    get: () => Effect.succeed({ type: "idle" }),
    set: () => Effect.void,
    list: () => Effect.succeed(new Map()),
  }

  interface AuthorityRecorders {
    readonly executes: PluginToolExecuteInput[]
    readonly aborts: { sessionID: string; callID: string }[]
    readonly permissionCancels: string[]
    readonly questionCancels: string[]
  }

  function authoritiesWith(recorders: AuthorityRecorders) {
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

  test("foreign pluginTool.execute and pluginTool.abort are rejected", async () => {
    const recorders: AuthorityRecorders = { executes: [], aborts: [], permissionCancels: [], questionCancels: [] }
    const authorities = authoritiesWith(recorders)
    const handlers = buildHubProxyHandlers(authorities, instantRunner)
    await using served = serveHubProxyFor(handlers)

    const input: PluginToolExecuteInput = {
      toolID: "wave3",
      sessionID: SESSION_B,
      messageID: "message-wave3",
      callID: "call-wave3",
      agent: "build",
      args: {},
    }
    await expect(served.client.call("pluginTool.execute", [input])).rejects.toThrow()
    expect(recorders.executes, "a foreign pluginTool.execute reached the hub plugin").toEqual([])

    await expect(
      served.client.call("pluginTool.abort", [{ sessionID: SESSION_B, callID: "call-wave3" }]),
    ).rejects.toThrow()
    expect(recorders.aborts, "a foreign pluginTool.abort reached the hub registry").toEqual([])

    // The bound session itself still reaches both authorities.
    await served.client.call("pluginTool.execute", [{ ...input, sessionID: SESSION_A }])
    await served.client.call("pluginTool.abort", [{ sessionID: SESSION_A, callID: "call-wave3" }])
    expect(recorders.executes.map((call) => call.sessionID)).toEqual([SESSION_A])
    expect(recorders.aborts).toEqual([{ sessionID: SESSION_A, callID: "call-wave3" }])
  })

  test("foreign cancelSession is rejected by the bound permission/question authorities", async () => {
    const recorders: AuthorityRecorders = { executes: [], aborts: [], permissionCancels: [], questionCancels: [] }
    const authorities = authoritiesWith(recorders)

    const permissionCancel = authorities.permission.cancelSession
    const questionCancel = authorities.question.cancelSession
    expect(permissionCancel, "permission.cancelSession is required by the release path").toBeDefined()
    expect(questionCancel, "question.cancelSession is required by the release path").toBeDefined()

    const foreignPermission = await settle(() => instantRunner(permissionCancel!(SessionID.make(SESSION_B))))
    expect(foreignPermission.ok, "a foreign permission.cancelSession was accepted").toBe(false)
    const foreignQuestion = await settle(() => instantRunner(questionCancel!(SessionID.make(SESSION_B))))
    expect(foreignQuestion.ok, "a foreign question.cancelSession was accepted").toBe(false)
    expect(recorders.permissionCancels, "a foreign permission.cancelSession reached the hub").toEqual([])
    expect(recorders.questionCancels, "a foreign question.cancelSession reached the hub").toEqual([])

    // The bound session still cancels its own pending requests.
    const ownPermission = await settle(() => instantRunner(permissionCancel!(SessionID.make(SESSION_A))))
    const ownQuestion = await settle(() => instantRunner(questionCancel!(SessionID.make(SESSION_A))))
    expect(ownPermission.ok, `own permission.cancelSession failed: ${String(ownPermission.error)}`).toBe(true)
    expect(ownQuestion.ok, `own question.cancelSession failed: ${String(ownQuestion.error)}`).toBe(true)
    expect(recorders.permissionCancels).toEqual([SESSION_A])
    expect(recorders.questionCancels).toEqual([SESSION_A])
  })
})

describe("production schema re-ensure", () => {
  test("a second ensure on an already-current DB succeeds with live workers", async () => {
    const filename = migratedDatabase()
    seedWorld(filename)

    expect(ensureProductionSchema({ filename, liveWorkers: 0 })).toEqual(currentSchemaIdentity())
    const database = await openBound(filename)
    expect(database.identity).toEqual(currentSchemaIdentity())

    expect(
      ensureProductionSchema({ filename, liveWorkers: 1 }),
      "second ensure re-ran the zero-live guard on an already-current schema",
    ).toEqual(currentSchemaIdentity())

    // A fresh process with live workers must also probe-and-return, not reject:
    // no DDL/migration is needed, so the zero-live guard does not apply.
    resetSchemaEnsureCache()
    expect(ensureProductionSchema({ filename, liveWorkers: 1 })).toEqual(currentSchemaIdentity())

    // Not vacuous: a genuinely missing/migrating database still refuses live
    // workers, so the guard was not simply deleted.
    const fresh = path.join(workspace(), "fresh.db")
    expect(() => ensureProductionSchema({ filename: fresh, liveWorkers: 1 })).toThrow()
  })
})

// ---------------------------------------------------------------------------
// Durable blob paths (legacy inline read + GC rollback).
// ---------------------------------------------------------------------------

const Note = Event.define({
  type: "test.typed.wave3.note",
  durable: { version: 1, aggregate: "sessionID" },
  schema: { sessionID: Schema.String, text: Schema.String },
})

function durableBus(
  filename: string,
  blobDir: string,
  options: { readonly pragmas?: string[]; readonly limitBytes?: number } = {},
) {
  const connection = openConnection({
    filename,
    ...(options.pragmas === undefined ? {} : { pragmas: options.pragmas }),
  })
  cleanups.push(() => connection.close())
  const bus = Bus.create({
    connection,
    ownerID: "wave3-bus",
    definitions: [Note],
    blobDir,
    blobLimitBytes: options.limitBytes ?? 64,
  })
  return { connection, bus }
}

describe("durable blob reads and rollback", () => {
  test("a legacy inline event_blob row still reads", () => {
    const filename = migratedDatabase()
    const blobDir = path.join(path.dirname(filename), "event-blobs")
    const { connection, bus } = durableBus(filename, blobDir)
    const text = `legacy-inline:${"x".repeat(256)}`
    bus.publish([{ definition: Note, data: { sessionID: SESSION_A, text } }])

    const row = connection.all<{ seq: number; hash: string; file_path: string }>(
      "SELECT seq, hash, file_path FROM event_blob WHERE aggregate_id = ?",
      [SESSION_A],
    )[0]
    expect(row, "the oversized payload was not offloaded").toBeDefined()
    const payload = fs.readFileSync(path.join(blobDir, row.file_path), "utf8")

    // Rewrite the row to the pre-file schema: the JSON payload lives inline in
    // `data` and no file exists. This is a real legacy row, not a stub.
    connection.run("UPDATE event_blob SET data = ?, file_path = NULL WHERE aggregate_id = ? AND seq = ?", [
      payload,
      SESSION_A,
      row.seq,
    ])
    fs.rmSync(path.join(blobDir, row.file_path))
    expect(
      connection.all("SELECT data, file_path FROM event_blob WHERE aggregate_id = ? AND seq = ?", [
        SESSION_A,
        row.seq,
      ]),
    ).toEqual([{ data: payload, file_path: null }])

    const legacy = Bus.create({ connection, ownerID: "wave3-legacy", definitions: [Note], blobDir })
    const events = legacy.read({ aggregateID: SESSION_A })
    expect(events).toHaveLength(1)
    expect(events[0]?.data).toEqual({ sessionID: SESSION_A, text })
  })

  test("a GC rollback does not unlink a file the restored rows still reference", () => {
    const filename = migratedDatabase()
    const blobDir = path.join(path.dirname(filename), "event-blobs")
    const pragmas = ["PRAGMA journal_mode = DELETE", "PRAGMA busy_timeout = 0", "PRAGMA foreign_keys = ON"]
    const { connection, bus } = durableBus(filename, blobDir, { pragmas })

    bus.publish([
      { definition: Note, data: { sessionID: SESSION_A, text: `gc:${"x".repeat(256)}` } },
      { definition: Note, data: { sessionID: SESSION_A, text: "live" } },
    ])
    const row = connection.all<{ hash: string; file_path: string }>(
      "SELECT hash, file_path FROM event_blob WHERE aggregate_id = ? AND seq = 0",
      [SESSION_A],
    )[0]
    const file = path.join(blobDir, row.file_path)
    expect(fs.existsSync(file), "the oversized payload file was not staged").toBe(true)

    // Hold a rollback-journal reader so the GC transaction can write and then
    // fails at COMMIT. The GC unlinks inside the transaction; a rollback must
    // restore the rows AND leave their file alive.
    const blocker = openConnection({ filename, pragmas })
    cleanups.push(() => blocker.close())
    blocker.exec("BEGIN")
    blocker.all("SELECT COUNT(*) AS count FROM event")

    const collect = () => bus.collectBlobs({ aggregateID: SESSION_A, beforeSeq: 1 })
    let failure: unknown
    try {
      collect()
    } catch (error) {
      failure = error
    }
    expect(failure, "the GC committed despite the held reader").toBeDefined()
    blocker.exec("COMMIT")

    expect(
      admin(filename, "SELECT hash FROM event_blob WHERE aggregate_id = ? AND seq = 0", [SESSION_A]),
      "the rolled-back GC left the blob row deleted",
    ).toEqual([{ hash: row.hash }])
    expect(fs.existsSync(file), "the rolled-back GC unlinked a file its restored rows still reference").toBe(true)

    // Not vacuous: once the reader is gone the same GC commits and collects.
    expect(collect()).toBe(1)
    expect(admin(filename, "SELECT hash FROM event_blob WHERE aggregate_id = ? AND seq = 0", [SESSION_A])).toEqual([])
    expect(fs.existsSync(file), "the committed GC left the file behind").toBe(false)
  })
})
