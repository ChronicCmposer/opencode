// Wave 2 — reduced session layer runtime tests (TDD plan steps 23–26).
//
//   * 23 writer-backed Drizzle Database
//   * 24 hub-proxy authorities (SessionStatus / Permission / Question)
//   * 25 reduced in-worker Provider (real call against the mock provider)
//   * 26 LLM.stream-backed ModelClient

import { Database as BunDatabase } from "bun:sqlite"
import { describe, expect, test } from "bun:test"
import { Effect, Layer, Stream } from "effect"
import { eq, sql } from "drizzle-orm"
import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core"
import { Database } from "@opencode-ai/core/database/database"
import { streamText } from "ai"
import type { LLMEvent } from "@opencode-ai/llm"
import { SessionID } from "@/session/schema"
import { SessionStatus } from "@/session/status"
import { Permission } from "@/permission"
import { Question } from "@/question"
import {
  ModelClient,
  boundaryReplacements,
  createHubProxyClient,
  hubProxyReplacements,
  llmModelClientLayer,
  permissionProxyLayer,
  questionProxyLayer,
  reducedLlmLayer,
  reducedModelProviderLayer,
  serveHubProxy,
  sessionStatusProxyLayer,
  writerDatabaseLayer,
  type HubProxyClients,
  type WorkerModelConfig,
  type WriterClientHandle,
} from "@/session/worker/layer"
import { Provider } from "@/provider/provider"
import { LLM } from "@/session/llm"
import { startMockProvider } from "../../lib/mock-provider"
import { workerSnapshot } from "../../lib/snapshot"

const sid = (value: string) => SessionID.make(value)

// ---------------------------------------------------------------------------
// Step 23 — writer-backed Drizzle Database.
//
// `writerDatabaseLayer(writer)` yields a real Drizzle `Database` whose
// `select` / `insert` / `update` / `delete` return Effects. The writer handle
// here is backed by a real in-memory SQLite connection so the round-trip runs
// against genuine storage rather than a mock.
// ---------------------------------------------------------------------------

const widget = sqliteTable("widget", {
  id: integer("id").primaryKey(),
  name: text("name").notNull(),
})

/**
 * A writer handle backed by an in-memory SQLite connection. The stateless
 * `query` path serves ordinary statements; `begin` / `txQuery` / `commit` /
 * `rollback` mirror the production writer so the reserved-connection path is
 * real too.
 */
function writerOverSqlite(native: BunDatabase): WriterClientHandle {
  let nextTxId = 1
  const run = (method: string, statement: string, params: readonly unknown[] = []) => {
    const query = native.query(statement)
    return method === "values" ? query.values(...(params as never[])) : query.all(...(params as never[]))
  }
  return {
    query: async (method, statement, params) => run(method, statement, params),
    publish: async () => ({ seq: null, committed: false }),
    begin: async () => {
      native.run("BEGIN IMMEDIATE")
      return `tx-${nextTxId++}`
    },
    txQuery: async (_handle, method, statement, params) => run(method, statement, params),
    commit: async () => {
      native.run("COMMIT")
    },
    rollback: async () => {
      native.run("ROLLBACK")
    },
  }
}

describe("writerDatabaseLayer (step 23)", () => {
  test("yields a real Drizzle Database that round-trips CRUD through the writer", async () => {
    const native = new BunDatabase(":memory:")
    try {
      const rows = await Effect.runPromise(
        Effect.gen(function* () {
          const database = yield* Database.Service
          const db = database.db
          yield* db.run(sql`CREATE TABLE widget (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`)
          yield* db
            .insert(widget)
            .values([
              { id: 1, name: "alpha" },
              { id: 2, name: "beta" },
            ])
            .run()
          yield* db.update(widget).set({ name: "ALPHA" }).where(eq(widget.id, 1)).run()
          yield* db.delete(widget).where(eq(widget.id, 2)).run()
          return yield* db.select().from(widget).all()
        }).pipe(Effect.provide(writerDatabaseLayer(writerOverSqlite(native)))),
      )

      expect(rows).toEqual([{ id: 1, name: "ALPHA" }])
    } finally {
      native.close()
    }
  })

  test("boundaryReplacements installs the writer proxy, not the throwing stub", async () => {
    const native = new BunDatabase(":memory:")
    try {
      const entry = boundaryReplacements(workerSnapshot(), writerOverSqlite(native)).find(
        ([node]) => node.name === Database.node.name,
      )
      expect(entry).toBeDefined()

      const rows = await Effect.runPromise(
        Effect.gen(function* () {
          const database = yield* Database.Service
          yield* database.db.run(sql`CREATE TABLE widget (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`)
          yield* database.db.insert(widget).values({ id: 7, name: "seven" }).run()
          return yield* database.db.select().from(widget).all()
        }).pipe(Effect.provide(entry![1] as Layer.Layer<Database.Service>)),
      )

      expect(rows).toEqual([{ id: 7, name: "seven" }])
    } finally {
      native.close()
    }
  })
})

// ---------------------------------------------------------------------------
// Step 24 — hub-proxy authorities.
// ---------------------------------------------------------------------------

describe("hub-proxy nodes (step 24)", () => {
  test("SessionStatus round-trips get/set/list over a port", async () => {
    const channel = new MessageChannel()
    const store = new Map<string, SessionStatus.Info>()
    serveHubProxy(
      {
        get: (sessionID) => store.get(sessionID as string) ?? { type: "idle" },
        set: (sessionID, status) => {
          store.set(sessionID as string, status as SessionStatus.Info)
        },
        list: () => [...store.entries()],
      },
      channel.port2,
    )
    const layer = sessionStatusProxyLayer(createHubProxyClient(channel.port1))

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const status = yield* SessionStatus.Service
        yield* status.set(sid("ses_x"), { type: "busy" })
        return {
          get: yield* status.get(sid("ses_x")),
          list: yield* status.list(),
        }
      }).pipe(Effect.provide(layer)),
    )

    expect(result.get).toEqual({ type: "busy" })
    expect([...result.list]).toEqual([[sid("ses_x"), { type: "busy" }]])
  })

  test("Permission and Question round-trip over a port", async () => {
    const permissionChannel = new MessageChannel()
    const seen: string[] = []
    serveHubProxy(
      {
        ask: (input) => {
          seen.push((input as { permission: string }).permission)
        },
        reply: () => {},
        list: () => [],
      },
      permissionChannel.port2,
    )
    const questionChannel = new MessageChannel()
    serveHubProxy(
      {
        ask: () => [["yes"]],
        reply: () => {},
        reject: () => {},
        list: () => [],
      },
      questionChannel.port2,
    )

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const permission = yield* Permission.Service
        yield* permission.ask({ permission: "bash" } as never)
        const question = yield* Question.Service
        return yield* question.ask({ sessionID: sid("ses_q"), questions: [] })
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            permissionProxyLayer(createHubProxyClient(permissionChannel.port1)),
            questionProxyLayer(createHubProxyClient(questionChannel.port1)),
          ),
        ),
      ),
    )

    expect(seen).toEqual(["bash"])
    expect(result).toEqual([["yes"]])
  })

  test("an unknown hub-proxy method surfaces as a remote error", async () => {
    const channel = new MessageChannel()
    serveHubProxy({}, channel.port2)
    const client = createHubProxyClient(channel.port1)
    await expect(client.call("nope", [])).rejects.toThrow(/unknown hub-proxy method/)
  })

  test("hubProxyReplacements covers the three authorities", () => {
    const clients = {
      sessionStatus: { call: async () => null, dispose: () => {} },
      permission: { call: async () => null, dispose: () => {} },
      question: { call: async () => null, dispose: () => {} },
    } satisfies HubProxyClients
    expect(hubProxyReplacements(clients)).toHaveLength(3)
  })
})

// ---------------------------------------------------------------------------
// Step 25 — reduced in-worker Provider.
// ---------------------------------------------------------------------------

const modelConfig = (model: {
  baseUrl: string
  apiKey: string
  model: string
}): WorkerModelConfig => ({
  providerID: "mock",
  modelID: model.model,
  apiId: model.model,
  npm: "@ai-sdk/openai-compatible",
  baseUrl: model.baseUrl,
  apiKey: model.apiKey,
  headers: {},
})

describe("reducedModelProviderLayer (step 25)", () => {
  test("getLanguage produces a real model call against the mock provider", async () => {
    await using provider = startMockProvider({ chunks: ["hel", "lo"] })
    const config = modelConfig(provider)

    const language = await Effect.runPromise(
      Effect.gen(function* () {
        const service = yield* Provider.Service
        const model = yield* service.getModel("mock" as never, provider.model as never)
        return yield* service.getLanguage(model)
      }).pipe(Effect.provide(reducedModelProviderLayer(config))),
    )

    const response = streamText({ model: language, prompt: "hi" })
    expect(await response.text).toBe("hello")
    expect(provider.requests.length).toBe(1)
    expect(provider.requests[0].authorization).toBe(`Bearer ${provider.apiKey}`)
  })

  test("a request for another model fails loud instead of mis-serving", async () => {
    await using provider = startMockProvider({})
    const config = modelConfig(provider)
    await expect(
      Effect.runPromise(
        Effect.gen(function* () {
          const service = yield* Provider.Service
          return yield* service.getModel("other" as never, "elsewhere" as never)
        }).pipe(Effect.provide(reducedModelProviderLayer(config))),
      ),
    ).rejects.toThrow(/Model not found/)
  })
})

// ---------------------------------------------------------------------------
// Step 26 — LLM.stream-backed ModelClient.
// ---------------------------------------------------------------------------

describe("llmModelClientLayer (step 26)", () => {
  test("concatenates the streamed text-delta events of one LLM turn", async () => {
    const events = [
      { type: "text-delta", id: "t1", text: "wor" },
      { type: "text-delta", id: "t1", text: "ker" },
    ] as unknown as ReadonlyArray<LLMEvent>

    const stubLlm = Layer.succeed(LLM.Service, {
      stream: () => Stream.fromIterable(events),
    } as unknown as LLM.Interface)

    const config = modelConfig({ baseUrl: "http://127.0.0.1:1/v1", apiKey: "k", model: "m" })
    const completion = await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* ModelClient
        return yield* client.complete({ sessionID: "s", messages: [] })
      }).pipe(Effect.provide(llmModelClientLayer(config).pipe(Layer.provide(stubLlm)))),
    )

    expect(completion.text).toBe("worker")
  })

  test("reducedLlmLayer + llmModelClientLayer make a real worker-local call", async () => {
    await using provider = startMockProvider({ text: "hub-zero" })
    const config = modelConfig(provider)
    const client = llmModelClientLayer(config).pipe(Layer.provide(reducedLlmLayer(workerSnapshot(), config)))

    const completion = await Effect.runPromise(
      Effect.gen(function* () {
        const model = yield* ModelClient
        return yield* model.complete({ sessionID: "s", messages: [{ role: "user", content: "hi" }] })
      }).pipe(Effect.provide(client)),
    )

    expect(completion.text).toBe("hub-zero")
  })
})
