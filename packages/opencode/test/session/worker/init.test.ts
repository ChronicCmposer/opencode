// The worker model bootstrap carries the REAL model descriptor.
//
// The hub used to ship a bare identity (`apiId/npm/baseUrl/apiKey/headers`), so
// the worker's reduced provider invented `capabilities`/`limit`/`options` and
// diverged from flag-OFF sampling. These tests pin the producer contract:
//   * the resolved descriptor is serialized verbatim (capabilities, limit,
//     model options, variants, provider options + headers);
//   * the whole payload survives `structuredClone`;
//   * a non-cloneable value anywhere in the shipped surface is rejected at hub
//     build with `WorkerModelNotSerializableError`;
//   * the dispatching SESSION's model wins over config/default.

import { describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import type { ConfigV1 } from "@opencode-ai/core/v1/config/config"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { Auth } from "@/auth"
import { Config } from "@/config/config"
import { Provider } from "@/provider/provider"
import {
  WorkerModelNotSerializableError,
  WorkerPluginNotSerializableError,
  buildSessionWorkerInit,
  resolveWorkerModel,
  workerModelIncompatibility,
  resolveWorkerPlugins,
} from "@/session/worker/init"
import { createWorkerPluginHost } from "@/session/worker/plugin-host"
import type { WorkerModel } from "@/session/worker/init"
import { InstanceRef } from "@/effect/instance-ref"
import { ModelsDev } from "@opencode-ai/core/models-dev"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { assertStructuredCloneable } from "../../lib/session-worker-init"

const FIXTURE_PROVIDER = "fixture"
const FIXTURE_MODEL = "fixture-model"
const DEFAULT_PROVIDER = "default-provider"
const DEFAULT_MODEL = "default-model"

/** A real-shaped model whose descriptor differs from the old hardcoded stub. */
const fixtureModel = {
  id: FIXTURE_MODEL,
  providerID: FIXTURE_PROVIDER,
  api: { id: "fixture-api-id", url: "https://fallback.example/v1", npm: "@ai-sdk/anthropic" },
  name: "Fixture Model",
  family: "fixture",
  capabilities: {
    temperature: false,
    reasoning: true,
    attachment: false,
    toolcall: true,
    input: { text: true, audio: false, image: true, video: false, pdf: false },
    output: { text: true, audio: false, image: false, video: false, pdf: false },
    interleaved: { field: "reasoning_content" },
  },
  cost: { input: 1, output: 2, cache: { read: 0, write: 0 } },
  limit: { context: 200_000, output: 32_000 },
  status: "active",
  options: { temperature: 0.2, topP: 0.9 },
  headers: { "x-model": "fixture" },
  release_date: "2025-01-01",
  variants: { high: { reasoningEffort: "high" } },
} as unknown as Provider.Model

const defaultModelRecord = {
  ...fixtureModel,
  id: DEFAULT_MODEL,
  providerID: DEFAULT_PROVIDER,
  limit: { context: 1_000, output: 1_000 },
} as unknown as Provider.Model

const providerInfo = {
  id: FIXTURE_PROVIDER,
  name: "Fixture",
  source: "config",
  env: [],
  key: "k",
  options: {
    baseURL: "https://api.fixture/v1",
    headers: { "x-provider": "1" },
    useCompletionUrls: true,
    includeUsage: true,
  },
  models: {},
} as unknown as Provider.Info

interface ModelLayerInput {
  readonly providerOptions?: Record<string, unknown>
  readonly modelOptions?: Record<string, unknown>
  readonly modelVariants?: Record<string, unknown>
  readonly modelCapabilities?: Record<string, unknown>
  readonly config?: Partial<ConfigV1.Info>
}

const modelLayers = (input: ModelLayerInput = {}) =>
  Layer.mergeAll(
    Layer.mock(Provider.Service, {
      defaultModel: () =>
        Effect.succeed({ providerID: ProviderV2.ID.make(FIXTURE_PROVIDER), modelID: ModelV2.ID.make(FIXTURE_MODEL) }),
      getProvider: (providerID) =>
        Effect.succeed({
          ...providerInfo,
          id: providerID,
          options: input.providerOptions ?? providerInfo.options,
        } as unknown as Provider.Info),
      getModel: (providerID, modelID) =>
        Effect.succeed({
          ...(modelID === DEFAULT_MODEL ? defaultModelRecord : fixtureModel),
          id: modelID,
          providerID,
          options: input.modelOptions ?? fixtureModel.options,
          variants: input.modelVariants ?? fixtureModel.variants,
          capabilities: input.modelCapabilities ?? fixtureModel.capabilities,
        } as unknown as Provider.Model),
    }),
    Layer.mock(Auth.Service, {
      get: () => Effect.succeed(new Auth.Api({ type: "api", key: "sk-auth" })),
    }),
    Layer.mock(Config.Service, {
      get: () => Effect.succeed((input.config ?? {}) as ConfigV1.Info),
      directories: () => Effect.succeed(["/cfg/opencode"]),
    }),
  )

const runResolve = (input: ModelLayerInput = {}, options: Parameters<typeof resolveWorkerModel>[0] = {}) =>
  Effect.runPromise(resolveWorkerModel(options).pipe(Effect.provide(modelLayers(input))))

const runResolveResult = (input: ModelLayerInput = {}, options: Parameters<typeof resolveWorkerModel>[0] = {}) =>
  Effect.runPromise(Effect.result(resolveWorkerModel(options).pipe(Effect.provide(modelLayers(input)))))

describe("resolveWorkerModel descriptor", () => {
  test("serializes the real capabilities, limit, model options and variants", async () => {
    const model = await runResolve()

    // The two values that used to be invented (`temperature: true`,
    // `limit.output: 4096`) now come from the real model record.
    expect(model.capabilities.temperature).toBe(false)
    expect(model.capabilities.reasoning).toBe(true)
    expect(model.capabilities.attachment).toBe(false)
    expect(model.capabilities.toolcall).toBe(true)
    expect(model.capabilities.interleaved as unknown).toEqual({ field: "reasoning_content" })
    expect(model.limit).toEqual({ context: 200_000, output: 32_000 })
    expect(model.options).toEqual({ temperature: 0.2, topP: 0.9 })
    expect(model.variants).toEqual({ high: { reasoningEffort: "high" } })
  })

  test("serializes provider-level options and merges provider + model headers", async () => {
    const model = await runResolve()

    expect(model.providerOptions).toEqual({
      baseURL: "https://api.fixture/v1",
      headers: { "x-provider": "1" },
      useCompletionUrls: true,
      includeUsage: true,
    })
    expect(model.headers).toEqual({ "x-provider": "1", "x-model": "fixture" })
  })

  test("the full descriptor survives a structuredClone round-trip unchanged", async () => {
    const model = await runResolve()

    expect(assertStructuredCloneable(model)).toEqual(model)
  })

  test("rejects a function in model options", async () => {
    const result = await runResolveResult({ modelOptions: { temperature: () => 0.2 } })

    expect(result._tag).toBe("Failure")
    if (result._tag !== "Failure") return
    expect(result.failure).toBeInstanceOf(WorkerModelNotSerializableError)
    expect((result.failure as WorkerModelNotSerializableError).field).toBe("temperature")
    expect((result.failure as WorkerModelNotSerializableError).kind).toBe("function")
  })

  test("rejects a symbol nested in model variants", async () => {
    const result = await runResolveResult({ modelVariants: { high: { reasoningEffort: Symbol("high") } } })

    expect(result._tag).toBe("Failure")
    if (result._tag !== "Failure") return
    expect(result.failure).toBeInstanceOf(WorkerModelNotSerializableError)
    expect((result.failure as WorkerModelNotSerializableError).field).toBe("variants.high.reasoningEffort")
    expect((result.failure as WorkerModelNotSerializableError).kind).toBe("symbol")
  })

  test("rejects a function nested in model capabilities", async () => {
    const result = await runResolveResult({
      modelCapabilities: { temperature: () => false, reasoning: true },
    })

    expect(result._tag).toBe("Failure")
    if (result._tag !== "Failure") return
    expect(result.failure).toBeInstanceOf(WorkerModelNotSerializableError)
    expect((result.failure as WorkerModelNotSerializableError).field).toBe("capabilities.temperature")
    expect((result.failure as WorkerModelNotSerializableError).kind).toBe("function")
  })

  test("rejects a function in provider options", async () => {
    const result = await runResolveResult({ providerOptions: { fetch: () => {} } })

    expect(result._tag).toBe("Failure")
    if (result._tag !== "Failure") return
    expect(result.failure).toBeInstanceOf(WorkerModelNotSerializableError)
    expect((result.failure as WorkerModelNotSerializableError).field).toBe("fetch")
  })
})

describe("workerModelIncompatibility", () => {
  const incompatibility = (input: ModelLayerInput = {}, auth?: Auth.Info) =>
    Effect.runPromise(
      workerModelIncompatibility({}).pipe(
        Effect.provide(
          Layer.mergeAll(
            modelLayers(input),
            ...(auth === undefined ? [] : [Layer.mock(Auth.Service, { get: () => Effect.succeed(auth) })]),
          ),
        ),
      ),
    )

  test("accepts a serializable model on a supported SDK with an API key", async () => {
    expect(await incompatibility()).toBeUndefined()
  })

  test("rejects provider options that carry a fetch function", async () => {
    expect(await incompatibility({ providerOptions: { fetch: () => {} } })).toContain("fetch")
  })

  test("rejects an OAuth credential", async () => {
    const oauth = new Auth.Oauth({ type: "oauth", refresh: "r", access: "a", expires: 0 })
    expect(await incompatibility({}, oauth)).toContain("OAuth")
  })

  test("rejects an SDK package the worker cannot build", async () => {
    const result = await Effect.runPromise(
      workerModelIncompatibility({}).pipe(
        Effect.provide(
          Layer.mergeAll(
            modelLayers(),
            Layer.mock(Provider.Service, {
              defaultModel: () =>
                Effect.succeed({
                  providerID: ProviderV2.ID.make(FIXTURE_PROVIDER),
                  modelID: ModelV2.ID.make(FIXTURE_MODEL),
                }),
              getProvider: () => Effect.succeed(providerInfo),
              getModel: () =>
                Effect.succeed({
                  ...fixtureModel,
                  api: { ...fixtureModel.api, npm: "@ai-sdk/amazon-bedrock" },
                } as unknown as Provider.Model),
            }),
          ),
        ),
      ),
    )
    expect(result).toContain("@ai-sdk/amazon-bedrock")
  })
})

describe("resolveWorkerModel session selection", () => {
  test("resolves the dispatching session's model instead of config/default", async () => {
    const model = await runResolve(
      { config: { model: `${DEFAULT_PROVIDER}/${DEFAULT_MODEL}` } },
      { session: { providerID: FIXTURE_PROVIDER, modelID: FIXTURE_MODEL } },
    )

    expect(model.providerID).toBe(FIXTURE_PROVIDER)
    expect(model.modelID).toBe(FIXTURE_MODEL)
    expect(model.limit.output).toBe(32_000)
  })

  test("without a session model it keeps the config/default fallback", async () => {
    const model = await runResolve({ config: { model: `${DEFAULT_PROVIDER}/${DEFAULT_MODEL}` } })

    expect(model.providerID).toBe(DEFAULT_PROVIDER)
    expect(model.modelID).toBe(DEFAULT_MODEL)
  })
})

// ---------------------------------------------------------------------------
// `buildSessionWorkerInit` ships the descriptor under the worker's wire names.
// ---------------------------------------------------------------------------

const initLayers = (input: ModelLayerInput = {}) =>
  Layer.mergeAll(
    modelLayers(input),
    Layer.mock(ModelsDev.Service, { get: () => Effect.succeed({}) }),
    RuntimeFlags.layer({ experimentalSessionWorkers: true }),
    Layer.mock(Auth.Service, {
      get: () => Effect.succeed(new Auth.Api({ type: "api", key: "sk-auth" })),
      all: () => Effect.succeed({}),
    }),
  )

const instanceCtx = {
  directory: "/tmp/opencode-descriptor",
  worktree: "/tmp/opencode-descriptor",
  project: { id: "prj_descriptor", worktree: "/tmp/opencode-descriptor", vcs: "git" } as never,
}

// ---------------------------------------------------------------------------
// The hub resolves plugin hooks into structuredClone-safe data
// (a hook *function* can never cross `workerData`).
// ---------------------------------------------------------------------------

const workerModel = (): WorkerModel => ({
  providerID: "mock",
  modelID: "mock-model",
  apiId: "mock-api",
  npm: "@ai-sdk/openai-compatible",
  baseUrl: "http://127.0.0.1:0/v1",
  apiKey: "sk-mock",
  capabilities: { temperature: true, reasoning: false },
  limit: { context: 128_000, output: 4096 },
  options: { topK: 5 },
})

const runPlugins = (sources: Parameters<typeof resolveWorkerPlugins>[0]["sources"]) =>
  Effect.runPromise(resolveWorkerPlugins({ model: workerModel(), sources }))

describe("resolveWorkerPlugins", () => {
  test("ships a chat.params mutation as structuredClone-safe resolved data", async () => {
    const plugins = await runPlugins([
      {
        name: "sampling",
        hooks: {
          "chat.params": (_input, output) => {
            output.temperature = 0.25
          },
        },
      },
    ])

    expect(plugins).toHaveLength(1)
    expect(plugins[0].hooks).toEqual({})
    expect(plugins[0].resolved?.["chat.params"]).toEqual({ temperature: 0.25 })
    // The resolved effect must survive the isolate boundary.
    expect(assertStructuredCloneable(plugins)).toEqual(plugins)

    // Applied over a worker baseline, the plugin's value wins.
    const host = createWorkerPluginHost(plugins)
    const params = await host.chatParams(
      {},
      { temperature: 1, topP: 1, topK: 1, maxOutputTokens: 100, options: {} },
    )
    expect(params.temperature).toBe(0.25)
  })

  test("ships a chat.headers mutation and merges it over the worker's headers", async () => {
    const plugins = await runPlugins([
      {
        name: "headers",
        hooks: {
          "chat.headers": (_input, output) => {
            output.headers["x-session-id"] = "ses-1"
          },
        },
      },
    ])

    expect(plugins[0].resolved?.["chat.headers"]).toEqual({ headers: { "x-session-id": "ses-1" } })
    const host = createWorkerPluginHost(plugins)
    const headers = await host.chatHeaders({}, { headers: { existing: "y" } })
    expect(headers.headers).toEqual({ existing: "y", "x-session-id": "ses-1" })
  })

  test("captures an options mutation and a cleared maxOutputTokens", async () => {
    const plugins = await runPlugins([
      {
        name: "options",
        hooks: {
          "chat.params": (_input, output) => {
            output.maxOutputTokens = undefined
            output.options.toolStreaming = false
          },
        },
      },
    ])

    const resolved = plugins[0].resolved?.["chat.params"]
    expect(resolved?.maxOutputTokens).toBeNull()
    expect(resolved?.options).toEqual({ toolStreaming: false })

    const host = createWorkerPluginHost(plugins)
    const params = await host.chatParams(
      {},
      { temperature: 1, topP: 1, topK: 1, maxOutputTokens: 100, options: {} },
    )
    expect(params.maxOutputTokens).toBeUndefined()
    expect(params.options).toEqual({ toolStreaming: false })
  })

  test("a throwing hook fails loud with WorkerPluginNotSerializableError", async () => {
    const result = await Effect.runPromise(
      Effect.result(
        resolveWorkerPlugins({
          model: workerModel(),
          sources: [
            {
              name: "bad",
              hooks: {
                "chat.params": () => {
                  throw new Error("boom")
                },
              },
            },
          ],
        }),
      ),
    )

    expect(result._tag).toBe("Failure")
    if (result._tag !== "Failure") return
    expect(result.failure).toBeInstanceOf(WorkerPluginNotSerializableError)
    expect((result.failure as WorkerPluginNotSerializableError).plugin).toBe("bad")
  })

  test("passes the real agent and last user message into hooks", async () => {
    const plugins = await Effect.runPromise(
      resolveWorkerPlugins({
        model: workerModel(),
        sources: [
          {
            name: "request",
            hooks: {
              "chat.headers": (input, output) => {
                // The real per-request input, exactly as `LLMRequestPrep` builds it
                // flag-off: `input.agent` + `input.message` (a `UserMessage`).
                const real = input as { agent: string; message: { id: string } }
                output.headers["x-agent"] = real.agent
                output.headers["x-message"] = real.message.id
              },
            },
          },
        ],
        agent: "title",
        message: { id: "msg_real", sessionID: "ses_real", role: "user" },
      }),
    )

    expect(plugins[0].resolved?.["chat.headers"]).toEqual({
      headers: { "x-agent": "title", "x-message": "msg_real" },
    })
    // The agent/message input never crosses the isolate; only the resolved diff does.
    expect(assertStructuredCloneable(plugins)).toEqual(plugins)
  })

  test("buildSessionWorkerInit resolves and ships the plugin effect", async () => {
    const init = await Effect.runPromise(
      buildSessionWorkerInit({
        model: { session: { providerID: FIXTURE_PROVIDER, modelID: FIXTURE_MODEL } },
        plugins: (model) =>
          resolveWorkerPlugins({
            model,
            sources: [
              {
                name: "sampling",
                hooks: {
                  "chat.params": (_input, output) => {
                    output.temperature = 0.5
                  },
                },
              },
            ],
          }),
      }).pipe(
        Effect.provide(initLayers()),
        Effect.provideService(InstanceRef, instanceCtx),
      ),
    )

    expect(init.plugins).toHaveLength(1)
    expect(init.plugins?.[0].resolved?.["chat.params"]).toEqual({ temperature: 0.5 })
    expect(assertStructuredCloneable(init)).toEqual(init)
  })
})

describe("buildSessionWorkerInit descriptor", () => {
  test("ships the session model descriptor and survives structuredClone", async () => {
    const init = await Effect.runPromise(
      buildSessionWorkerInit({
        model: { session: { providerID: FIXTURE_PROVIDER, modelID: FIXTURE_MODEL } },
      }).pipe(
        Effect.provide(initLayers({ config: { model: `${DEFAULT_PROVIDER}/${DEFAULT_MODEL}` } })),
        Effect.provideService(InstanceRef, instanceCtx),
      ),
    )

    expect(init.model.modelID).toBe(FIXTURE_MODEL)
    expect(init.model.capabilities.temperature).toBe(false)
    expect(init.model.limit.output).toBe(32_000)
    expect(init.model.options).toEqual({ temperature: 0.2, topP: 0.9 })
    expect(init.model.providerOptions).toMatchObject({ includeUsage: true })
    expect(assertStructuredCloneable(init)).toEqual(init)
  })

  test("populates the hub-assigned workerID and survives structuredClone", async () => {
    const init = await Effect.runPromise(
      buildSessionWorkerInit({
        workerID: 4,
        model: { session: { providerID: FIXTURE_PROVIDER, modelID: FIXTURE_MODEL } },
      }).pipe(
        Effect.provide(initLayers()),
        Effect.provideService(InstanceRef, instanceCtx),
      ),
    )

    // The pool slot id is first-class in the init payload, so the worker reports
    // the real id (never a stale/default) on its `pid-report` frames.
    expect(init.workerID).toBe(4)
    expect(assertStructuredCloneable(init).workerID).toBe(4)
  })
})
