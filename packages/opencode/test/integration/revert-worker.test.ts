// Worker-mode revert cleanup.
//
// A worker that has a staged revert must clear it through HUB authority before
// answering the next prompt. The old path called `Session.clearRevert`, which
// publishes a worker-authored `session.updated` carrying `revert: null`; the
// projection policy correctly refuses that as a hub-owned change, so
// `SessionRevert.cleanup` died and every prompt after a revert failed.
//
// This suite boots the production `buildSessionWorkerLayer` for a BOUND session
// over a real migrated SQLite file and a real hub-proxy `MessagePort`, stages a
// revert, runs `revert.cleanup`, then runs a real prompt through
// `createSessionWork` against the mock provider.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Context, Effect, Exit, Layer, Scope } from "effect"
import { openConnection } from "@opencode-ai/core/database/connection"
import { SessionID } from "@/session/schema"
import { markSchemaIdentity } from "@/session/db/schema"
import { Session } from "@/session/session"
import { SessionRevert } from "@/session/revert"
import { InstanceRef } from "@/effect/instance-ref"
import { createSessionWork } from "@/session/worker/session-worker"
import {
  ModelClient,
  buildSessionWorkerLayer,
  createHubProxyClient,
  llmModelClientLayer,
  reducedLlmLayer,
  reducedModelProviderLayer,
  serveHubProxy,
  type HubProxyClient,
  type HubProxyClients,
  type HubProxyHandlers,
  type WriterClientHandle,
  type WorkerModelConfig,
} from "@/session/worker/layer"
import type { HubProxyAuthorities } from "@/session/worker/hub"
import { buildHubProxyHandlers } from "@/session/worker/hub"
import { openWorkerDatabase, type WorkerDatabase } from "@/session/worker/db"
import { migrationStatements } from "@/session/writer/migration.statements.gen"
import { Provider } from "@/provider/provider"
import { Permission } from "@/permission"
import { Question } from "@/question"
import { SessionStatus } from "@/session/status"
import { startMockProvider } from "../lib/mock-provider"
import { instantRunner } from "../lib/hub-proxy"
import { parityProject } from "../lib/parity-project"
import { workerSnapshot } from "../lib/snapshot"

const SESSION = SessionID.make("ses_revert_worker")
const USER_MESSAGE = "msg_revert_worker_user"
const PROJECT = "e2e"

const directories: string[] = []
const databases: WorkerDatabase[] = []
const disposes: Array<() => void | Promise<void>> = []

function workspace(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-revert-worker-"))
  directories.push(directory)
  return directory
}

function migrated(directory: string): string {
  const filename = path.join(directory, "opencode.db")
  const connection = openConnection({ filename })
  try {
    for (const statement of migrationStatements) connection.exec(statement)
    markSchemaIdentity(connection)
  } finally {
    connection.close()
  }
  return filename
}

function admin(filename: string, sql: string, params: ReadonlyArray<unknown> = []): void {
  const connection = openConnection({ filename })
  try {
    connection.run(sql, params as never)
  } finally {
    connection.close()
  }
}

function adminRows<Row = Record<string, unknown>>(
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

/** One session with a staged revert and the message the revert points at. */
function seed(filename: string, model: WorkerModelConfig): void {
  admin(filename, "INSERT INTO project (id, worktree, sandboxes, time_created, time_updated) VALUES (?, ?, ?, ?, ?)", [
    PROJECT,
    "/tmp",
    "[]",
    0,
    0,
  ])
  admin(
    filename,
    "INSERT INTO session (id, project_id, slug, directory, title, version, metadata, permission, model, agent, revert, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    [
      SESSION,
      PROJECT,
      "revert-worker",
      "/tmp/revert-worker",
      "revert worker",
      "1",
      JSON.stringify({ depth: 1 }),
      JSON.stringify([]),
      JSON.stringify({ id: model.modelID, providerID: model.providerID }),
      "build",
      JSON.stringify({ messageID: USER_MESSAGE }),
      1,
      1,
    ],
  )
  admin(filename, "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)", [
    USER_MESSAGE,
    SESSION,
    1,
    1,
    JSON.stringify({
      role: "user",
      time: { created: 1 },
      agent: "build",
      model: { providerID: model.providerID, modelID: model.modelID },
    }),
  ])
}

const modelConfig = (provider: Awaited<ReturnType<typeof startMockProvider>>): WorkerModelConfig => ({
  providerID: "mock",
  modelID: provider.model,
  apiId: provider.model,
  headers: {},
  npm: "@ai-sdk/openai-compatible",
  baseUrl: provider.baseUrl,
  apiKey: "test",
})

interface Harness {
  readonly filename: string
  readonly context: Context.Context<unknown>
  readonly instance: {
    readonly directory: string
    readonly worktree: string
    readonly project: ReturnType<typeof parityProject>
  }
  /** Session ids the hub authority was asked to clear. */
  readonly cleared: readonly string[]
  /** The worker-local writer seam `createSessionWork` reads through. */
  readonly writer: WriterClientHandle
  readonly close: () => Promise<void>
}

/**
 * A hub-proxy port whose `session.discardReverted` authority performs the hub's
 * real write: it deletes the reverted rows and clears the persisted `revert`
 * column in one transaction outside the worker.
 */
function hubPort(
  filename: string,
  cleared: string[],
  options: { readonly failClear?: boolean } = {},
): { clients: HubProxyClients; dispose: () => void } {
  const authorities: HubProxyAuthorities = {
    sessionStatus: {
      get: () => Effect.succeed({ type: "idle" }),
      set: () => Effect.void,
      list: () => Effect.succeed(new Map()),
    },
    permission: {
      ask: () => Effect.void,
      reply: () => Effect.void,
      list: () => Effect.succeed([]),
    } as unknown as Permission.Interface,
    question: {
      ask: () => Effect.succeed([]),
      reply: () => Effect.void,
      reject: () => Effect.void,
      list: () => Effect.succeed([]),
    } as unknown as Question.Interface,
  }
  const handlers: HubProxyHandlers = {
    ...buildHubProxyHandlers(authorities, instantRunner),
    "session.discardReverted": (request) => {
      const { sessionID, messageIDs } = request as { sessionID: string; messageIDs: ReadonlyArray<string> }
      if (options.failClear) throw new Error("hub discard failed")
      cleared.push(sessionID)
      // The hub's single transaction: delete the reverted rows and clear `revert` together.
      const connection = openConnection({ filename })
      try {
        connection.exec("BEGIN")
        for (const id of messageIDs) connection.run("DELETE FROM message WHERE id = ?", [id] as never)
        connection.run("UPDATE session SET revert = NULL, time_updated = ? WHERE id = ?", [Date.now(), sessionID] as never)
        connection.exec("COMMIT")
      } finally {
        connection.close()
      }
    },
  }
  const channel = new MessageChannel()
  const disposeServer = serveHubProxy(handlers, channel.port2)
  const client: HubProxyClient = createHubProxyClient(channel.port1)
  return {
    clients: { sessionStatus: client, permission: client, question: client },
    dispose() {
      disposeServer()
      client.dispose()
      channel.port1.close()
      channel.port2.close()
    },
  }
}

async function harness(
  provider: Awaited<ReturnType<typeof startMockProvider>>,
  options: { readonly failClear?: boolean } = {},
): Promise<Harness> {
  const directory = workspace()
  const filename = migrated(directory)
  const model = modelConfig(provider)
  seed(filename, model)

  const database = await Effect.runPromise(openWorkerDatabase({ filename, sessionID: SESSION }))
  databases.push(database)

  const cleared: string[] = []
  const hub = hubPort(filename, cleared, options)
  disposes.push(hub.dispose)

  const snapshot = workerSnapshot({ directory })
  const built = buildSessionWorkerLayer({
    snapshot,
    env: { values: {} },
    clients: {
      writer: { query: async () => [], publish: async () => ({ seq: 1, committed: true }) },
      egress: { emit: () => {} },
      backgroundJob: { wait: async () => null, cancel: async () => {} },
      database,
    },
    modelClient: llmModelClientLayer(model).pipe(Layer.provide(reducedLlmLayer(snapshot, model, hub.clients))),
    boundSessionID: SESSION,
    hubProxies: hub.clients,
    // The main graph's `Provider` is hub-owned; the worker serves it from the
    // same frozen model config the reduced LLM sub-graph uses.
    replacements: [[Provider.node, reducedModelProviderLayer(model)]],
  })

  const scope = Effect.runSync(Scope.make())
  const context = await Effect.runPromise(Layer.buildWithScope(built.layer, scope))
  const instance = { directory, worktree: directory, project: parityProject(PROJECT) }
  disposes.push(() => Effect.runPromise(Scope.close(scope, Exit.void)))

  const writer = {
    query: async () => [],
    publish: async () => ({ seq: 1, committed: true }),
  }
  return {
    filename,
    context,
    instance,
    cleared,
    writer,
    close: async () => {
      for (const dispose of disposes.splice(0).reverse()) await dispose()
    },
  }
}

function assistantText(result: unknown): string {
  const value = (result as { value: { parts: ReadonlyArray<{ type: string; text?: string }> } }).value
  return value.parts
    .filter((part) => part.type === "text")
    .map((part) => part.text ?? "")
    .join("")
}

function revertUpdates(filename: string): number {
  const rows = adminRows<{ n: number }>(filename, "SELECT COUNT(*) AS n FROM event WHERE type = 'session.updated'")
  return Number(rows[0]?.n ?? 0)
}

afterEach(async () => {
  for (const dispose of disposes.splice(0).reverse()) await dispose()
  for (const database of databases.splice(0)) await Effect.runPromise(database.close)
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("worker-mode revert cleanup", () => {
  test("cleanup clears the hub-owned revert and the next prompt succeeds", async () => {
    await using provider = startMockProvider({ chunks: ["reco", "vered"] })
    const testHarness = await harness(provider)
    try {
      const before = revertUpdates(testHarness.filename)

      // 1. `revert.cleanup` must not die on a worker-authored `revert: null`.
      await Effect.runPromise(
        Effect.gen(function* () {
          const sessions = yield* Session.Service
          const revert = yield* SessionRevert.Service
          const session = yield* sessions.get(SESSION)
          expect(session.revert).toBeDefined()
          yield* revert.cleanup(session)
        }).pipe(
          Effect.provide(testHarness.context),
          Effect.provideService(InstanceRef, testHarness.instance),
        ),
      )

      // 2. The hub authority performed the clear...
      expect(testHarness.cleared).toEqual([SESSION])
      expect(adminRows(testHarness.filename, "SELECT revert FROM session WHERE id = ?", [SESSION])).toEqual([
        { revert: null },
      ])
      // ...and the worker published no `session.updated` of its own.
      expect(revertUpdates(testHarness.filename)).toBe(before)

      // 3. A real prompt after the revert completes instead of failing the
      //    projection policy on `revert: null`.
      const work = createSessionWork({
        context: testHarness.context,
        writer: testHarness.writer,
        threadId: 1,
        prompt: true,
        instance: testHarness.instance,
      })
      const result = await Effect.runPromise(
        work(SESSION, { sessionID: SESSION, parts: [{ type: "text", text: "after the revert" }] }),
      )
      expect(assistantText(result)).toBe("recovered")
      expect(adminRows(testHarness.filename, "SELECT revert FROM session WHERE id = ?", [SESSION])).toEqual([
        { revert: null },
      ])
    } finally {
      await testHarness.close()
    }
  }, 60_000)

  test("a failed hub discard leaves every row and the revert untouched", async () => {
    await using provider = startMockProvider({ chunks: ["unused"] })
    const testHarness = await harness(provider, { failClear: true })
    try {
      // A message after the revert point that cleanup would delete.
      admin(testHarness.filename, "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)", [
        "msg_revert_worker_tail",
        SESSION,
        2,
        2,
        JSON.stringify({
          role: "assistant",
          time: { created: 2 },
          agent: "build",
          model: { providerID: "mock", modelID: "mock" },
          parentID: USER_MESSAGE,
        }),
      ])

      const cleanup = Effect.gen(function* () {
        const sessions = yield* Session.Service
        const revert = yield* SessionRevert.Service
        yield* revert.cleanup(yield* sessions.get(SESSION))
      }).pipe(
        Effect.provide(testHarness.context),
        Effect.provideService(InstanceRef, testHarness.instance),
      )

      // The hub discard fails as one unit: no row is deleted and the revert pointer stays valid.
      await expect(Effect.runPromise(cleanup)).rejects.toThrow()
      const ids = adminRows<{ id: string }>(testHarness.filename, "SELECT id FROM message WHERE session_id = ? ORDER BY id", [
        SESSION,
      ]).map((row) => row.id)
      expect([...ids].sort()).toEqual([USER_MESSAGE, "msg_revert_worker_tail"].sort())
      expect(adminRows(testHarness.filename, "SELECT revert FROM session WHERE id = ?", [SESSION])).toEqual([
        { revert: JSON.stringify({ messageID: USER_MESSAGE }) },
      ])
    } finally {
      await testHarness.close()
    }
  }, 60_000)
})
