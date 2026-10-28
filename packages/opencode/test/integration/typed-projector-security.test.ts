// Projector identity/authority boundary for a REAL bound worker.
//
// A worker may publish only events about its own durable aggregate, and the
// projector must never trust a nested `info.id` / `info.sessionID` /
// `part.sessionID` over that aggregate. The session root itself is hub
// authority: a worker cannot mint or delete it, and a `session.updated` may not
// rewrite immutable columns or hub-owned `metadata.depth`.
//
// Every case below drives the production `buildSessionWorkerLayer` over a real
// per-worker SQLite connection and publishes through the graph's `EventV2`.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Context, Effect, Exit, Layer, Scope } from "effect"
import { openConnection } from "@opencode-ai/core/database/connection"
import { EventV2 } from "@opencode-ai/core/event"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { SessionEvent } from "@opencode-ai/core/session/event"
import { markSchemaIdentity } from "@/session/db/schema"
import { SessionID } from "@/session/schema"
import { ModelClient, buildSessionWorkerLayer } from "@/session/worker/layer"
import { openWorkerDatabase, type WorkerDatabase } from "@/session/worker/db"
import { migrationStatements } from "@/session/writer/migration.statements.gen"
import { workerSnapshot } from "../lib/snapshot"

const ROOT = SessionID.make("ses_projector_root")
const FOREIGN = SessionID.make("ses_projector_foreign")
const MINTED = SessionID.make("ses_projector_minted")
const ROOT_MESSAGE = "msg_projector_root"
const FOREIGN_MESSAGE = "msg_projector_foreign"
const ROOT_PART = "prt_projector_root"
const FOREIGN_PART = "prt_projector_foreign"

const directories: string[] = []
const databases: WorkerDatabase[] = []

const ROOT_INFO = {
  id: ROOT,
  slug: "root",
  projectID: "project-1",
  directory: "/tmp/projector",
  title: "root",
  version: "1",
  metadata: { depth: 1, keep: "root" },
  time: { created: 1, updated: 1 },
}

const info = (overrides: Record<string, unknown> = {}) => ({ ...ROOT_INFO, ...overrides }) as never

const messageInfo = (overrides: Record<string, unknown> = {}) =>
  ({
    id: ROOT_MESSAGE,
    sessionID: ROOT,
    role: "user",
    time: { created: 1 },
    agent: "build",
    model: { providerID: "provider", modelID: "model" },
    ...overrides,
  }) as never

const textPart = (overrides: Record<string, unknown> = {}) =>
  ({
    id: ROOT_PART,
    sessionID: ROOT,
    messageID: ROOT_MESSAGE,
    type: "text",
    text: "root part",
    ...overrides,
  }) as never

function workspace(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-projector-security-"))
  directories.push(directory)
  return directory
}

function seed(filename: string): void {
  const connection = openConnection({ filename })
  try {
    for (const statement of migrationStatements) connection.exec(statement)
    markSchemaIdentity(connection)
    connection.run(
      "INSERT INTO project (id, name, worktree, sandboxes, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
      ["project-1", "projector", "/tmp/projector", "[]", 1, 1],
    )
    connection.run(
      "INSERT INTO session (id, project_id, slug, directory, title, version, metadata, permission, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      [
        ROOT,
        "project-1",
        "root",
        "/tmp/projector",
        "root",
        "1",
        JSON.stringify({ depth: 1, keep: "root" }),
        JSON.stringify([]),
        1,
        1,
      ],
    )
    connection.run(
      "INSERT INTO session (id, project_id, slug, directory, title, version, metadata, permission, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      [
        FOREIGN,
        "project-1",
        "foreign",
        "/tmp/projector/foreign",
        "foreign",
        "1",
        JSON.stringify({ depth: 3 }),
        JSON.stringify([{ permission: "*", pattern: "*", action: "allow" }]),
        1,
        1,
      ],
    )
    for (const [id, sessionID] of [
      [ROOT_MESSAGE, ROOT],
      [FOREIGN_MESSAGE, FOREIGN],
    ] as const) {
      connection.run(
        "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)",
        [
          id,
          sessionID,
          1,
          1,
          JSON.stringify({
            role: "user",
            time: { created: 1 },
            agent: "build",
            model: { providerID: "provider", modelID: "model" },
          }),
        ],
      )
    }
    for (const [id, messageID, sessionID] of [
      [ROOT_PART, ROOT_MESSAGE, ROOT],
      [FOREIGN_PART, FOREIGN_MESSAGE, FOREIGN],
    ] as const) {
      connection.run(
        "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?)",
        [id, messageID, sessionID, 1, 1, JSON.stringify({ type: "text", text: `${sessionID} part` })],
      )
    }
  } finally {
    connection.close()
  }
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

const SESSION_HUB_COLUMNS = [
  "cost",
  "tokens_input",
  "tokens_output",
  "tokens_reasoning",
  "tokens_cache_read",
  "tokens_cache_write",
  "revert",
  "share_url",
  "version",
  "time_created",
] as const

function sessionSnapshot(filename: string, sessionID: string = ROOT): ReadonlyArray<Record<string, unknown>> {
  return admin(
    filename,
    `SELECT id, ${SESSION_HUB_COLUMNS.join(", ")}, permission FROM session WHERE id = ?`,
    [sessionID],
  )
}

const stepFinishPart = (overrides: Record<string, unknown> = {}) =>
  ({
    id: ROOT_PART,
    sessionID: ROOT,
    messageID: ROOT_MESSAGE,
    type: "step-finish",
    reason: "stop",
    cost: 2,
    tokens: { input: 2, output: 2, reasoning: 2, cache: { read: 2, write: 2 } },
    ...overrides,
  }) as never

interface Harness {
  readonly filename: string
  readonly events: EventV2.Interface
  readonly run: <A, E>(effect: Effect.Effect<A, E, never>) => Promise<A>
  readonly close: () => Promise<void>
}

async function harness(options: { readonly bound?: boolean } = {}): Promise<Harness> {
  const directory = workspace()
  const filename = path.join(directory, "opencode.db")
  seed(filename)
  const bound = options.bound ?? true
  const database = await Effect.runPromise(
    openWorkerDatabase(bound ? { filename, sessionID: ROOT } : { filename }),
  )
  databases.push(database)
  const built = buildSessionWorkerLayer({
    snapshot: workerSnapshot({ directory }),
    env: { values: {} },
    clients: {
      writer: { query: async () => [], publish: async () => ({ seq: null, committed: false }) },
      egress: { emit: () => {} },
      backgroundJob: { wait: async () => null, cancel: async () => {} },
      database,
    },
    modelClient: Layer.succeed(ModelClient, { complete: () => Effect.succeed({ text: "unused" }) }),
    ...(bound ? { boundSessionID: ROOT } : {}),
  })
  const scope = Effect.runSync(Scope.make())
  const context = await Effect.runPromise(Layer.buildWithScope(built.layer, scope))
  return {
    filename,
    events: Context.get(context, EventV2.Service),
    run: <A, E>(effect: Effect.Effect<A, E, never>) => Effect.runPromise(effect.pipe(Effect.provide(context))),
    close: () => Effect.runPromise(Scope.close(scope, Exit.void)),
  }
}

async function outcome(
  testHarness: Harness,
  operation: (events: EventV2.Interface) => Effect.Effect<unknown, unknown, never>,
): Promise<{ readonly ok: boolean; readonly error?: unknown }> {
  try {
    await testHarness.run(operation(testHarness.events))
    return { ok: true }
  } catch (error) {
    return { ok: false, error }
  }
}

afterEach(async () => {
  for (const database of databases.splice(0)) await Effect.runPromise(database.close)
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("bound worker projector authority", () => {
  test("session.updated cannot rewrite immutable columns or metadata.depth", async () => {
    const testHarness = await harness()
    try {
      const result = await outcome(testHarness, (events) =>
        events.publish(SessionV1.Event.Updated, {
          sessionID: ROOT,
          info: info({
            directory: "/tmp/evil",
            parentID: FOREIGN,
            permission: [{ permission: "*", pattern: "*", action: "deny" }],
            metadata: { depth: 0, keep: "root" },
          }),
        }),
      )

      expect(result.ok).toBe(false)
      expect(admin(testHarness.filename, "SELECT directory, parent_id, permission, metadata FROM session WHERE id = ?", [
        ROOT,
      ])).toEqual([
        {
          directory: "/tmp/projector",
          parent_id: null,
          permission: JSON.stringify([]),
          metadata: JSON.stringify({ depth: 1, keep: "root" }),
        },
      ])
    } finally {
      await testHarness.close()
    }
  })

  test("session.created cannot mint a session root", async () => {
    const testHarness = await harness()
    try {
      const result = await outcome(testHarness, (events) =>
        events.publish(SessionV1.Event.Created, {
          sessionID: ROOT,
          info: info({ id: MINTED, slug: "minted", directory: "/tmp/minted", title: "minted" }),
        }),
      )

      expect(result.ok).toBe(false)
      expect(admin(testHarness.filename, "SELECT id FROM session WHERE id = ?", [MINTED])).toEqual([])
      expect(admin(testHarness.filename, "SELECT id FROM session WHERE id = ?", [ROOT])).toHaveLength(1)
    } finally {
      await testHarness.close()
    }
  })

  test("session.deleted cannot delete the worker's root", async () => {
    const testHarness = await harness()
    try {
      const result = await outcome(testHarness, (events) =>
        events.publish(SessionV1.Event.Deleted, { sessionID: ROOT, info: info() }),
      )

      expect(result.ok).toBe(false)
      expect(admin(testHarness.filename, "SELECT id FROM session WHERE id = ?", [ROOT])).toHaveLength(1)
    } finally {
      await testHarness.close()
    }
  })

  test("session.next.moved cannot relocate the worker's root", async () => {
    const testHarness = await harness()
    try {
      const result = await outcome(testHarness, (events) =>
        events.publish(SessionEvent.Moved, {
          sessionID: ROOT,
          timestamp: 2,
          location: { directory: "/tmp/evil" },
        } as never),
      )

      expect(result.ok).toBe(false)
      expect(admin(testHarness.filename, "SELECT directory FROM session WHERE id = ?", [ROOT])).toEqual([
        { directory: "/tmp/projector" },
      ])
    } finally {
      await testHarness.close()
    }
  })

  test("message.updated cannot write another session's message", async () => {
    const testHarness = await harness()
    try {
      const result = await outcome(testHarness, (events) =>
        events.publish(SessionV1.Event.MessageUpdated, {
          sessionID: ROOT,
          info: messageInfo({ id: "msg_forged_cross_session", sessionID: FOREIGN }),
        }),
      )

      expect(result.ok).toBe(false)
      expect(
        admin(testHarness.filename, "SELECT id FROM message WHERE id = ?", ["msg_forged_cross_session"]),
      ).toEqual([])
      expect(admin(testHarness.filename, "SELECT session_id FROM message WHERE id = ?", [FOREIGN_MESSAGE])).toEqual([
        { session_id: FOREIGN },
      ])
    } finally {
      await testHarness.close()
    }
  })

  test("message.updated cannot repaint a foreign message through an id collision", async () => {
    const testHarness = await harness()
    try {
      const before = admin<{ data: string; session_id: string }>(
        testHarness.filename,
        "SELECT data, session_id FROM message WHERE id = ?",
        [FOREIGN_MESSAGE],
      )

      await outcome(testHarness, (events) =>
        events.publish(SessionV1.Event.MessageUpdated, {
          sessionID: ROOT,
          info: messageInfo({ id: FOREIGN_MESSAGE, sessionID: ROOT }),
        }),
      )

      // The scoped view cannot see the foreign row, but the upsert's setWhere
      // must keep the collision from repainting it.
      expect(
        admin(testHarness.filename, "SELECT data, session_id FROM message WHERE id = ?", [FOREIGN_MESSAGE]),
      ).toEqual(before)
    } finally {
      await testHarness.close()
    }
  })

  test("message.part.updated cannot write another session's part", async () => {
    const testHarness = await harness()
    try {
      const result = await outcome(testHarness, (events) =>
        events.publish(SessionV1.Event.PartUpdated, {
          sessionID: ROOT,
          part: textPart({ id: "prt_forged_cross_session", sessionID: FOREIGN, messageID: FOREIGN_MESSAGE }),
          time: 2,
        }),
      )

      expect(result.ok).toBe(false)
      expect(
        admin(testHarness.filename, "SELECT id FROM part WHERE id = ?", ["prt_forged_cross_session"]),
      ).toEqual([])
    } finally {
      await testHarness.close()
    }
  })

  test("message.part.updated cannot repaint a foreign part through an id collision", async () => {
    const testHarness = await harness()
    try {
      const before = admin<{ data: string; message_id: string; session_id: string }>(
        testHarness.filename,
        "SELECT data, message_id, session_id FROM part WHERE id = ?",
        [FOREIGN_PART],
      )

      await outcome(testHarness, (events) =>
        events.publish(SessionV1.Event.PartUpdated, {
          sessionID: ROOT,
          part: textPart({ id: FOREIGN_PART, sessionID: ROOT, messageID: ROOT_MESSAGE, text: "forged" }),
          time: 2,
        }),
      )

      expect(
        admin(testHarness.filename, "SELECT data, message_id, session_id FROM part WHERE id = ?", [FOREIGN_PART]),
      ).toEqual(before)
    } finally {
      await testHarness.close()
    }
  })

  test("message.part.updated cannot attach a part to another session's message", async () => {
    const testHarness = await harness()
    try {
      const result = await outcome(testHarness, (events) =>
        events.publish(SessionV1.Event.PartUpdated, {
          sessionID: ROOT,
          part: textPart({ id: "prt_forged_message_owner", messageID: FOREIGN_MESSAGE }),
          time: 2,
        }),
      )

      expect(result.ok).toBe(false)
      expect(
        admin(testHarness.filename, "SELECT id FROM part WHERE id = ?", ["prt_forged_message_owner"]),
      ).toEqual([])
    } finally {
      await testHarness.close()
    }
  })

  test("a legitimate own-session update still projects", async () => {
    const testHarness = await harness()
    try {
      const result = await outcome(testHarness, (events) =>
        events.publish(SessionV1.Event.Updated, {
          sessionID: ROOT,
          info: info({ title: "renamed root", time: { created: 1, updated: 2 } }),
        }),
      )

      expect(result.ok).toBe(true)
      expect(admin(testHarness.filename, "SELECT title, metadata FROM session WHERE id = ?", [ROOT])).toEqual([
        { title: "renamed root", metadata: JSON.stringify({ depth: 1, keep: "root" }) },
      ])
    } finally {
      await testHarness.close()
    }
  })

  test("session.updated cannot rewrite hub billing, share, revert or version columns", async () => {
    const testHarness = await harness()
    try {
      const before = sessionSnapshot(testHarness.filename)
      const attempts: ReadonlyArray<Record<string, unknown>> = [
        { cost: 999 },
        { tokens: { input: 9, output: 9, reasoning: 9, cache: { read: 9, write: 9 } } },
        { revert: { messageID: "msg_forged_revert" } },
        { share: { url: "https://evil.test/share" } },
        { version: "999" },
        { time: { created: 99, updated: 99 } },
      ]

      for (const attempt of attempts) {
        const result = await outcome(testHarness, (events) =>
          events.publish(SessionV1.Event.Updated, { sessionID: ROOT, info: info(attempt) }),
        )
        expect(result.ok, `session.updated accepted ${JSON.stringify(attempt)}`).toBe(false)
      }

      expect(sessionSnapshot(testHarness.filename)).toEqual(before)
    } finally {
      await testHarness.close()
    }
  })

  test("a stale session.updated repeating unchanged hub columns still projects", async () => {
    const testHarness = await harness()
    try {
      // The hub owns these columns and has already advanced them. A stale
      // `Session.Info` that merely repeats the persisted values must be a no-op,
      // not a policy violation: comparing the whole row would reject every
      // unrelated worker update whenever billing/share/revert state is non-default.
      admin(
        testHarness.filename,
        "UPDATE session SET cost = 7, tokens_input = 3, tokens_output = 4, tokens_reasoning = 1, tokens_cache_read = 2, tokens_cache_write = 5, share_url = ?, revert = ?, version = '2', time_created = 11 WHERE id = ?",
        ["https://share.test/root", JSON.stringify({ messageID: ROOT_MESSAGE }), ROOT],
      )
      const before = sessionSnapshot(testHarness.filename)

      const result = await outcome(testHarness, (events) =>
        events.publish(SessionV1.Event.Updated, {
          sessionID: ROOT,
          info: info({
            cost: 7,
            tokens: { input: 3, output: 4, reasoning: 1, cache: { read: 2, write: 5 } },
            share: { url: "https://share.test/root" },
            revert: { messageID: ROOT_MESSAGE },
            version: "2",
            title: "renamed after stale",
            time: { created: 11, updated: 2 },
          }),
        }),
      )

      expect(result.ok, `stale unchanged hub columns rejected: ${String(result.error)}`).toBe(true)
      expect(sessionSnapshot(testHarness.filename)).toEqual(before)
      expect(admin(testHarness.filename, "SELECT title FROM session WHERE id = ?", [ROOT])).toEqual([
        { title: "renamed after stale" },
      ])
    } finally {
      await testHarness.close()
    }
  })

  test("a delta patch ignores stale unemitted hub columns", async () => {
    const testHarness = await harness()
    try {
      // The hub owns billing and has advanced it after the worker read its
      // `Session.Info` snapshot. A worker title update must not be rejected (or
      // resurrect the stale cost) just because the carried image is old.
      admin(
        testHarness.filename,
        "UPDATE session SET cost = 7, tokens_input = 3, tokens_output = 4, tokens_reasoning = 1, tokens_cache_read = 2, tokens_cache_write = 5, share_url = ?, revert = ?, version = '2', time_created = 11 WHERE id = ?",
        ["https://share.test/root", JSON.stringify({ messageID: ROOT_MESSAGE }), ROOT],
      )
      const before = sessionSnapshot(testHarness.filename)

      const result = await outcome(testHarness, (events) =>
        events.publish(
          SessionV1.Event.Updated,
          {
            sessionID: ROOT,
            // A stale full image: cost/tokens/revert/share/version/time.created
            // all predate the hub advance above.
            info: info({ title: "renamed from a stale snapshot", time: { created: 1, updated: 2 } }),
          },
          { metadata: { sessionPatch: ["title", "time_updated"] } },
        ),
      )

      expect(result.ok, `delta patch rejected: ${String(result.error)}`).toBe(true)
      expect(sessionSnapshot(testHarness.filename)).toEqual(before)
      expect(admin(testHarness.filename, "SELECT title FROM session WHERE id = ?", [ROOT])).toEqual([
        { title: "renamed from a stale snapshot" },
      ])
    } finally {
      await testHarness.close()
    }
  })

  test("session.updated cannot widen permission in a bound worker", async () => {
    const testHarness = await harness()
    try {
      const before = sessionSnapshot(testHarness.filename)
      const result = await outcome(testHarness, (events) =>
        events.publish(SessionV1.Event.Updated, {
          sessionID: ROOT,
          info: info({
            permission: [{ permission: "*", pattern: "*", action: "allow" }],
            time: { created: 1, updated: 2 },
          }),
        }),
      )

      expect(result.ok).toBe(false)
      expect(sessionSnapshot(testHarness.filename)).toEqual(before)
    } finally {
      await testHarness.close()
    }
  })

  test("the hub still applies the setPermission projection", async () => {
    // Tool restrictions are resolved on the hub (`prompt.ts` runs there), so an
    // unbound/hub projector has no worker policy and must keep projecting the
    // permission column. This is the legitimate `sessions.setPermission()` path.
    const testHarness = await harness({ bound: false })
    try {
      const permission = [{ permission: "bash", pattern: "*", action: "deny" }]
      const result = await outcome(testHarness, (events) =>
        events.publish(SessionV1.Event.Updated, {
          sessionID: ROOT,
          info: info({ permission, time: { created: 1, updated: 2 } }),
        }),
      )

      expect(result.ok).toBe(true)
      expect(admin(testHarness.filename, "SELECT permission FROM session WHERE id = ?", [ROOT])).toEqual([
        { permission: JSON.stringify(permission) },
      ])
    } finally {
      await testHarness.close()
    }
  })

  test("a suppressed part conflict does not move usage", async () => {
    // The hub projector has no worker policy, so a colliding part id from
    // another session reaches the `setWhere`-guarded upsert. A suppressed write
    // must not reverse the foreign row's usage or add usage to this session.
    const testHarness = await harness({ bound: false })
    try {
      const usage = { cost: 5, tokens: { input: 5, output: 5, reasoning: 5, cache: { read: 5, write: 5 } } }
      admin(testHarness.filename, "UPDATE part SET data = ? WHERE id = ?", [
        JSON.stringify({ type: "step-finish", reason: "stop", ...usage }),
        FOREIGN_PART,
      ])
      admin(
        testHarness.filename,
        "UPDATE session SET cost = 5, tokens_input = 5, tokens_output = 5, tokens_reasoning = 5, tokens_cache_read = 5, tokens_cache_write = 5 WHERE id IN (?, ?)",
        [ROOT, FOREIGN],
      )
      const before = {
        root: sessionSnapshot(testHarness.filename, ROOT),
        foreign: sessionSnapshot(testHarness.filename, FOREIGN),
      }

      const result = await outcome(testHarness, (events) =>
        events.publish(SessionV1.Event.PartUpdated, {
          sessionID: ROOT,
          part: stepFinishPart({ id: FOREIGN_PART, sessionID: ROOT, messageID: FOREIGN_MESSAGE, cost: 3 }),
          time: 9,
        }),
      )

      expect(result.ok).toBe(true)
      expect(sessionSnapshot(testHarness.filename, ROOT)).toEqual(before.root)
      expect(sessionSnapshot(testHarness.filename, FOREIGN)).toEqual(before.foreign)
      expect(admin(testHarness.filename, "SELECT session_id, message_id FROM part WHERE id = ?", [FOREIGN_PART])).toEqual(
        [{ session_id: FOREIGN, message_id: FOREIGN_MESSAGE }],
      )
    } finally {
      await testHarness.close()
    }
  })

  test("a genuine step-finish part still applies usage", async () => {
    const testHarness = await harness({ bound: false })
    try {
      const result = await outcome(testHarness, (events) =>
        events.publish(SessionV1.Event.PartUpdated, {
          sessionID: ROOT,
          part: stepFinishPart(),
          time: 9,
        }),
      )

      expect(result.ok).toBe(true)
      expect(
        admin(
          testHarness.filename,
          "SELECT cost, tokens_input, tokens_output, tokens_reasoning, tokens_cache_read, tokens_cache_write FROM session WHERE id = ?",
          [ROOT],
        ),
      ).toEqual([
        {
          cost: 2,
          tokens_input: 2,
          tokens_output: 2,
          tokens_reasoning: 2,
          tokens_cache_read: 2,
          tokens_cache_write: 2,
        },
      ])
    } finally {
      await testHarness.close()
    }
  })
})
