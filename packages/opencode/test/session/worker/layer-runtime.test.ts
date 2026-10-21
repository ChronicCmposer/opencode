// Wave 2 — reduced session layer runtime tests (TDD plan steps 23–26).
//
//   * 23 writer-proxy Database
//   * 24 hub-proxy authorities (SessionStatus / Permission / Question)
//   * 25 reduced in-worker Provider (real call against the mock provider)
//   * 26 LLM.stream-backed ModelClient

import { describe, expect, test } from "bun:test"
import { Effect, Layer, Stream } from "effect"
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
  type WorkerDatabaseHandle,
  type WorkerModelConfig,
} from "@/session/worker/layer"
import { Provider } from "@/provider/provider"
import { LLM } from "@/session/llm"
import { startMockProvider } from "../../lib/mock-provider"
import { workerSnapshot } from "../../lib/snapshot"

const sid = (value: string) => SessionID.make(value)

// ---------------------------------------------------------------------------
// Step 23 — writer-proxy Database.
// ---------------------------------------------------------------------------

describe("writerDatabaseLayer (step 23)", () => {
  test("routes reads and writes through the writer client", async () => {
    const calls: Array<{ method: string; sql: string; params?: readonly unknown[] }> = []
    const writer = {
      query: async (method: string, sql: string, params?: readonly unknown[]) => {
        calls.push({ method, sql, params })
        return [{ ok: 1 }]
      },
      publish: async () => ({ seq: 1, committed: true }),
    }

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const database = yield* Database.Service
        const db = database.db as unknown as WorkerDatabaseHandle
        return {
          run: yield* Effect.promise(() => db.run("UPDATE t SET x = 1", [1])),
          all: yield* Effect.promise(() => db.all("SELECT * FROM t", [2])),
          get: yield* Effect.promise(() => db.get("SELECT * FROM t WHERE id = ?", [3])),
        }
      }).pipe(Effect.provide(writerDatabaseLayer(writer))),
    )

    expect(calls).toEqual([
      { method: "run", sql: "UPDATE t SET x = 1", params: [1] },
      { method: "all", sql: "SELECT * FROM t", params: [2] },
      { method: "values", sql: "SELECT * FROM t WHERE id = ?", params: [3] },
    ])
    expect(result.all).toEqual([{ ok: 1 }])
  })

  test("boundaryReplacements installs the writer proxy, not the throwing stub", async () => {
    const writer = {
      query: async () => [{ routed: true }],
      publish: async () => ({ seq: 1, committed: true }),
    }
    const entry = boundaryReplacements(workerSnapshot(), writer).find(([node]) => node.name === Database.node.name)
    expect(entry).toBeDefined()

    const rows = await Effect.runPromise(
      Effect.gen(function* () {
        const database = yield* Database.Service
        return yield* Effect.promise(() =>
          (database.db as unknown as WorkerDatabaseHandle).all("SELECT 1"),
        )
      }).pipe(Effect.provide(entry![1] as Layer.Layer<Database.Service>)),
    )
    expect(rows).toEqual([{ routed: true }])
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
