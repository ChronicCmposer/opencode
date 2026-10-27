// W4.1 / D19 — a hook added after startup is a typed admission failure on the
// next worker run.
//
// This suite drives the PRODUCTION assembly (`buildSessionWorkerInit` with the
// hub's `(model) => resolveWorkerPlugins(...)` resolver), not the resolver in
// isolation. Admission therefore runs exactly where the hub runs it, and the
// assertions read the payload a worker actually receives.

import { describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import { ModelsDev } from "@opencode-ai/core/models-dev"
import type { ConfigV1 } from "@opencode-ai/core/v1/config/config"
import { Auth } from "@/auth"
import { Config } from "@/config/config"
import { InstanceRef } from "@/effect/instance-ref"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Provider } from "@/provider/provider"
import { UnclassifiedPluginHookError, UnsupportedPluginHookError } from "@/plugin/routes"
import { buildSessionWorkerInit, resolveWorkerPlugins } from "@/session/worker/init"

const modelLayers = () =>
  Layer.mergeAll(
    Layer.mock(Provider.Service, {
      defaultModel: () =>
        Effect.succeed({ providerID: ProviderV2.ID.make("mock"), modelID: ModelV2.ID.make("mock-model") }),
      getProvider: () =>
        Effect.succeed({ id: "mock", name: "Mock", source: "config", env: [], key: "k", options: {}, models: {} } as never),
      getModel: () =>
        Effect.succeed({
          id: "mock-model",
          providerID: "mock",
          api: { id: "mock-api", url: "http://127.0.0.1:0/v1", npm: "@ai-sdk/openai-compatible" },
          name: "Mock",
          family: "mock",
          capabilities: { temperature: true, reasoning: false },
          cost: { input: 0, output: 0, cache: { read: 0, write: 0 } },
          limit: { context: 128_000, output: 4096 },
          status: "active",
          options: {},
          headers: {},
          release_date: "2025-01-01",
        } as never),
    }),
    Layer.mock(Auth.Service, {
      get: () => Effect.succeed(new Auth.Api({ type: "api", key: "sk-auth" })),
      all: () => Effect.succeed({}),
    }),
    Layer.mock(Config.Service, {
      get: () => Effect.succeed({} as ConfigV1.Info),
      directories: () => Effect.succeed(["/cfg/opencode"]),
    }),
    Layer.mock(ModelsDev.Service, { get: () => Effect.succeed({}) }),
    RuntimeFlags.layer({ experimentalSessionWorkers: true }),
  )

const instance = {
  directory: "/tmp/opencode-plugin-admission",
  worktree: "/tmp/opencode-plugin-admission",
  project: { id: "prj_plugin_admission", worktree: "/tmp/opencode-plugin-admission", vcs: "git" },
} as never

/** Assemble a worker init exactly as the hub does: resolver fed to the factory. */
const resolve = (hooks: Record<string, unknown>) =>
  Effect.runPromise(
    Effect.result(
      buildSessionWorkerInit({
        model: { session: { providerID: "mock", modelID: "mock-model" } },
        plugins: (model) => resolveWorkerPlugins({ model, sources: [{ name: "dynamic", hooks } as never] }),
      }).pipe(Effect.provide(modelLayers()), Effect.provideService(InstanceRef, instance)),
    ),
  )

describe("worker plugin hook admission", () => {
  test("rejects an unsupported hook added after startup", async () => {
    const result = await resolve({ "chat.message": async () => {} })
    expect(result._tag).toBe("Failure")
    if (result._tag !== "Failure") throw new Error("expected admission failure")
    expect(result.failure).toBeInstanceOf(UnsupportedPluginHookError)
    expect((result.failure as UnsupportedPluginHookError).hook).toBe("chat.message")
  })

  test("rejects an unclassified hook added after startup", async () => {
    const result = await resolve({ "future.trigger": async () => {} })
    expect(result._tag).toBe("Failure")
    if (result._tag !== "Failure") throw new Error("expected admission failure")
    expect(result.failure).toBeInstanceOf(UnclassifiedPluginHookError)
    expect((result.failure as UnclassifiedPluginHookError).hook).toBe("future.trigger")
  })

  test("admits routed hooks and ships their resolved effect", async () => {
    const result = await resolve({
      "chat.params": (_input: unknown, output: { temperature?: number }) => {
        output.temperature = 0.4
      },
    })
    expect(result._tag).toBe("Success")
    if (result._tag !== "Success") throw new Error("expected admission")
    expect(result.success.plugins?.[0]?.resolved?.["chat.params"]).toEqual({ temperature: 0.4 })
  })

  test("admits hub-proxied hooks without dropping them", async () => {
    const result = await resolve({
      "tool.execute.before": async () => {},
      "shell.env": async () => {},
    })
    expect(result._tag).toBe("Success")
    if (result._tag !== "Success") throw new Error("expected admission")
    expect(result.success.plugins?.[0]?.proxied).toEqual(
      expect.arrayContaining(["tool.execute.before", "shell.env"]),
    )
  })

  test("admits declared-only and non-trigger hooks", async () => {
    const result = await resolve({
      "permission.ask": async () => {},
      event: async () => {},
      tool: {},
    })
    expect(result._tag).toBe("Success")
  })
})
