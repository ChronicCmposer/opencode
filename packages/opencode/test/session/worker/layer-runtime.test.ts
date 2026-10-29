// Reduced session layer runtime tests.
//
//   * 23 worker-owned Drizzle Database
//   * 24 hub-proxy authorities (SessionStatus / Permission / Question)
//   * 25 reduced in-worker Provider (real call against the mock provider)
//   * 26 LLM.stream-backed ModelClient

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { describe, expect, test } from "bun:test"
import { Effect, Layer, Stream } from "effect"
import { eq } from "drizzle-orm"
import { openConnection } from "@opencode-ai/core/database/connection"
import { Database } from "@opencode-ai/core/database/database"
import { ProjectV2 } from "@opencode-ai/core/project"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { streamText } from "ai"
import type { LLMEvent } from "@opencode-ai/llm"
import { SessionID } from "@/session/schema"
import { SessionStatus } from "@/session/status"
import { Permission } from "@/permission"
import { Question } from "@/question"
import { migrationStatements } from "@/session/writer/migration.statements.gen"
import { markSchemaIdentity, openWorkerDatabase } from "@/session/worker/db"
import {
  ModelClient,
  boundaryReplacements,
  createHubProxyClient,
  hubProxyReplacements,
  llmBoundaryReplacements,
  llmModelClientLayer,
  permissionProxyLayer,
  questionProxyLayer,
  reducedLlmLayer,
  reducedModelProviderLayer,
  serveHubProxy,
  sessionStatusProxyLayer,
  workerDatabaseLayer,
  type HubProxyClient,
  type HubProxyClients,
  type WorkerModelConfig,
} from "@/session/worker/layer"
import type { HubProxyMethod } from "@/session/worker/hub-proxy-wire"
import { Provider } from "@/provider/provider"
import { LLM } from "@/session/llm"
import { createWorkerPluginHost } from "@/session/worker/plugin-host"
import { startMockProvider, type MockProviderHandle } from "../../lib/mock-provider"
import { workerSnapshot } from "../../lib/snapshot"

const sid = (value: string) => SessionID.make(value)

// ---------------------------------------------------------------------------
// Worker-owned Drizzle Database (D5).
//
// `workerDatabaseLayer(database)` yields a real Drizzle `Database` over the
// worker's own WAL connection. The file is migrated first so the production
// schema gate opens it exactly as a worker does.
// ---------------------------------------------------------------------------

function temporaryWorkerFile(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-runtime-db-"))
  return path.join(directory, "opencode.db")
}

function migrateWorkerFile(): string {
  const filename = temporaryWorkerFile()
  const connection = openConnection({ filename })
  try {
    for (const statement of migrationStatements) connection.exec(statement)
    markSchemaIdentity(connection)
    connection.run(
      "INSERT INTO project (id, worktree, sandboxes, time_created, time_updated) VALUES (?, ?, ?, ?, ?)",
      ["prj_worker", "/tmp/project-a", "[]", 1, 1],
    )
  } finally {
    connection.close()
  }
  return filename
}

function removeWorkerFile(filename: string): void {
  fs.rmSync(path.dirname(filename), { recursive: true, force: true })
}

const sessionRow = (id: string, title: string) => ({
  id: SessionID.make(id),
  project_id: ProjectV2.ID.make("prj_worker"),
  slug: id,
  directory: "/tmp/project-a",
  title,
  version: "1",
})

describe("workerDatabaseLayer", () => {
  test("yields a real Drizzle Database that round-trips CRUD through the worker connection", async () => {
    const filename = migrateWorkerFile()
    const worker = await Effect.runPromise(openWorkerDatabase({ filename }))
    try {
      const rows = await Effect.runPromise(
        Effect.gen(function* () {
          const database = yield* Database.Service
          const db = database.db
          yield* db
            .insert(SessionTable)
            .values([sessionRow("ses_1", "alpha"), sessionRow("ses_2", "beta")])
            .run()
          yield* db
            .update(SessionTable)
            .set({ title: "ALPHA" })
            .where(eq(SessionTable.id, SessionID.make("ses_1")))
            .run()
          yield* db.delete(SessionTable).where(eq(SessionTable.id, SessionID.make("ses_2"))).run()
          return yield* db.select({ id: SessionTable.id, title: SessionTable.title }).from(SessionTable).all()
        }).pipe(Effect.provide(workerDatabaseLayer(worker))),
      )

      expect(rows).toEqual([{ id: SessionID.make("ses_1"), title: "ALPHA" }])
    } finally {
      await Effect.runPromise(worker.close)
      removeWorkerFile(filename)
    }
  })

  test("boundaryReplacements installs the worker Database, not the throwing stub", async () => {
    const filename = migrateWorkerFile()
    const worker = await Effect.runPromise(openWorkerDatabase({ filename }))
    try {
      const entry = boundaryReplacements(workerSnapshot(), worker).find(
        ([node]) => node.name === Database.node.name,
      )
      expect(entry).toBeDefined()

      const rows = await Effect.runPromise(
        Effect.gen(function* () {
          const database = yield* Database.Service
          yield* database.db.insert(SessionTable).values(sessionRow("ses_boundary", "boundary")).run()
          return yield* database.db.select({ id: SessionTable.id }).from(SessionTable).all()
        }).pipe(Effect.provide(entry![1] as Layer.Layer<Database.Service>)),
      )

      expect(rows).toEqual([{ id: SessionID.make("ses_boundary") }])
    } finally {
      await Effect.runPromise(worker.close)
      removeWorkerFile(filename)
    }
  })
})

// ---------------------------------------------------------------------------
// Hub-proxy authorities.
// ---------------------------------------------------------------------------

describe("hub-proxy nodes", () => {
  test("SessionStatus round-trips get/set/list over a port", async () => {
    const channel = new MessageChannel()
    const store = new Map<string, SessionStatus.Info>()
    serveHubProxy(
      {
        "sessionStatus.get": (sessionID) => store.get(sessionID as string) ?? { type: "idle" },
        "sessionStatus.set": (sessionID, status) => {
          store.set(sessionID as string, status as SessionStatus.Info)
        },
        "sessionStatus.list": () => [...store.entries()],
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
        "permission.ask": (input) => {
          seen.push((input as { permission: string }).permission)
        },
        "permission.reply": () => {},
        "permission.list": () => [],
      },
      permissionChannel.port2,
    )
    const questionChannel = new MessageChannel()
    serveHubProxy(
      {
        "question.ask": () => [["yes"]],
        "question.reply": () => {},
        "question.reject": () => {},
        "question.list": () => [],
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
    await expect(client.call("nope" as HubProxyMethod, [])).rejects.toThrow(/unknown hub-proxy method/)
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
// Reduced in-worker Provider.
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

describe("reducedModelProviderLayer", () => {
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
// LLM.stream-backed ModelClient.
// ---------------------------------------------------------------------------

describe("llmModelClientLayer", () => {
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

// ---------------------------------------------------------------------------
// The reduced LLM sub-graph must route `Permission` to the hub authority
// (fail loud on a denial) and never install a silent no-op.
// ---------------------------------------------------------------------------

describe("permission boundary in the reduced LLM sub-graph", () => {
  const permissionLayerFor = (hubProxies?: HubProxyClients): Layer.Layer<Permission.Service> => {
    const config = modelConfig({ baseUrl: "http://127.0.0.1:1/v1", apiKey: "k", model: "m" })
    const entry = llmBoundaryReplacements(workerSnapshot(), config, hubProxies).find(
      ([node]) => node.name === Permission.node.name,
    )
    if (entry === undefined) throw new Error("reduced LLM sub-graph has no Permission replacement")
    return entry[1] as Layer.Layer<Permission.Service>
  }

  test("a denied authorization fails loud through the hub-proxy port", async () => {
    const denied = {
      call: async () => {
        throw new Error("permission denied by policy")
      },
      dispose: () => {},
    } as unknown as HubProxyClient
    const hubProxies: HubProxyClients = { sessionStatus: denied, permission: denied, question: denied }
    const run = Effect.gen(function* () {
      const permission = yield* Permission.Service
      return yield* permission.ask(undefined as never)
    }).pipe(Effect.provide(permissionLayerFor(hubProxies)))
    await expect(Effect.runPromise(run)).rejects.toThrow(/permission denied by policy/)
  })

  test("without a hub control plane an ask fails loud with a boundary error (never a mock)", async () => {
    const run = Effect.gen(function* () {
      const permission = yield* Permission.Service
      return yield* permission.ask(undefined as never)
    }).pipe(Effect.provide(permissionLayerFor(undefined)))
    // Specifically a `SessionLayerBoundaryError`, not a `Layer.mock`'s
    // `Unimplemented method "ask"` (which would still name Permission).
    await expect(Effect.runPromise(run)).rejects.toThrow(/session worker boundary "Permission"/)
  })
})

// ---------------------------------------------------------------------------
// Phases 2-3 — the worker's outgoing request params match flag-off.
//
// A descriptor with `capabilities.temperature:false` must OMIT temperature even
// for a model family `ProviderTransform.temperature` would force to 1.0
// (`transform.ts:530-547`), and must use the descriptor's `limit.output`
// instead of the old synthesized 4096 cap. A worker-hosted plugin's
// `chat.params` transform must apply.
// ---------------------------------------------------------------------------

const descriptorConfig = (provider: MockProviderHandle, overrides: Partial<WorkerModelConfig> = {}): WorkerModelConfig => ({
  ...modelConfig(provider),
  capabilities: { temperature: false, reasoning: true },
  limit: { context: 200_000, output: 1_234 },
  ...overrides,
})

const completeWith = (config: WorkerModelConfig, plugins?: Parameters<typeof reducedLlmLayer>[3]) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const model = yield* ModelClient
      return yield* model.complete({ sessionID: "s", messages: [{ role: "user", content: "hi" }] })
    }).pipe(
      Effect.provide(
        llmModelClientLayer(config).pipe(Layer.provide(reducedLlmLayer(workerSnapshot(), config, undefined, plugins))),
      ),
    ),
  )

describe("reduced worker request parity (phases 2-3)", () => {
  test("omits temperature for a temperature-less descriptor (glm-4.6 would otherwise be 1.0)", async () => {
    await using provider = startMockProvider({ text: "ok" })
    await completeWith(descriptorConfig(provider, { apiId: "glm-4.6" }))

    expect(provider.requests[0].body.temperature).toBeUndefined()
  })

  test("sends the transform temperature when the descriptor declares temperature support", async () => {
    await using provider = startMockProvider({ text: "ok" })
    await completeWith(
      descriptorConfig(provider, { apiId: "glm-4.6", capabilities: { temperature: true, reasoning: true } }),
    )

    expect(provider.requests[0].body.temperature).toBe(1)
  })

  test("uses the descriptor limit.output, not the synthesized 4096 cap", async () => {
    await using provider = startMockProvider({ text: "ok" })
    await completeWith(descriptorConfig(provider))

    const body = provider.requests[0].body
    expect(body.max_tokens ?? body.max_completion_tokens).toBe(1_234)
  })

  test("applies a worker-hosted plugin's chat.params transform", async () => {
    await using provider = startMockProvider({ text: "ok" })
    const plugins = createWorkerPluginHost([
      {
        name: "sampling",
        hooks: {
          "chat.params": (_input, output) => {
            output.temperature = 0.25
          },
        },
      },
    ])
    await completeWith(descriptorConfig(provider, { capabilities: { temperature: true, reasoning: false } }), plugins)

    expect(provider.requests[0].body.temperature).toBe(0.25)
  })
})
