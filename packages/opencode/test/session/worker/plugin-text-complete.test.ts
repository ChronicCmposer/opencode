// `experimental.text.complete` runs on every text-end (`processor.ts`), and a
// worker gets a plugin host whenever `init.plugins` is an array — including when
// no loaded plugin implements that hook. An undeclared unsupported hook is a
// true no-op: the text passes through unchanged. A plugin that *does* declare
// the unsupported hook still fails typed when the worker dispatches it.
//
// This drives the production chain, not the route helpers in isolation:
//   hub resolver (`resolveWorkerPlugins`) → `buildSessionWorkerInit` (the payload
//   the hub ships) → `parseSessionWorkerInit` (the worker boundary) →
//   `createWorkerPluginHost` → `buildSessionWorkerLayer` → `Plugin.Service.trigger`.

import { describe, expect, test } from "bun:test"
import { Cause, Effect, Exit, Layer } from "effect"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import { ModelsDev } from "@opencode-ai/core/models-dev"
import type { ConfigV1 } from "@opencode-ai/core/v1/config/config"
import { Auth } from "@/auth"
import { Config } from "@/config/config"
import { InstanceRef } from "@/effect/instance-ref"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Plugin } from "@/plugin"
import { Provider } from "@/provider/provider"
import { ModelClient, buildSessionWorkerLayer } from "@/session/worker/layer"
import { buildSessionWorkerInit, resolveWorkerPlugins } from "@/session/worker/init"
import { createWorkerPluginHost, type WorkerPluginHooks } from "@/session/worker/plugin-host"
import { parseSessionWorkerInit } from "@/session/worker/session-worker"

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
  directory: "/tmp/opencode-plugin-text-complete",
  worktree: "/tmp/opencode-plugin-text-complete",
  project: { id: "prj_plugin_text_complete", worktree: "/tmp/opencode-plugin-text-complete", vcs: "git" },
} as never

/** Assemble the hub payload exactly as production does, then parse it. */
const buildInit = (hooks: Record<string, unknown>) =>
  Effect.runPromise(
    Effect.result(
      buildSessionWorkerInit({
        model: { session: { providerID: "mock", modelID: "mock-model" } },
        plugins: (model) =>
          resolveWorkerPlugins({ model, sources: [{ name: "configured", hooks: hooks as WorkerPluginHooks }] }),
      }).pipe(Effect.provide(modelLayers()), Effect.provideService(InstanceRef, instance)),
    ),
  )

/** Build the worker graph from the parsed init, as the production entry does. */
function bootWorker(init: ReturnType<typeof parseSessionWorkerInit>) {
  const plugins = createWorkerPluginHost(init.plugins ?? [])
  return buildSessionWorkerLayer({
    snapshot: init.snapshot,
    env: init.env,
    clients: {
      writer: { query: async () => [], publish: async () => ({ seq: 1, committed: true }) },
      egress: { emit: () => {} },
      backgroundJob: { wait: async () => null, cancel: async () => {} },
    },
    modelClient: Layer.succeed(ModelClient, { complete: () => Effect.succeed({ text: "stub" }) }),
    plugins,
  })
}

/** Dispatch one hook through the worker's `Plugin.Service`, exactly as the processor does. */
function triggerTextComplete(
  layer: ReturnType<typeof bootWorker>["layer"],
  text: string,
): Promise<Exit.Exit<unknown, unknown>> {
  return Effect.runPromise(
    Effect.gen(function* () {
      const plugin = (yield* Plugin.Service) as Plugin.Interface
      const output = yield* plugin.trigger(
        "experimental.text.complete" as never,
        { sessionID: "ses", messageID: "msg", partID: "prt" } as never,
        { text } as never,
      )
      return output as unknown
    }).pipe(Effect.provide(layer), Effect.exit),
  )
}

describe("experimental.text.complete in a worker", () => {
  test("a plugin list without the hook passes the text through unchanged", async () => {
    // A loaded plugin that only declares a supported hook: the host exists, but
    // nothing implements `experimental.text.complete`.
    const result = await buildInit({ "chat.params": async () => {} })
    expect(result._tag).toBe("Success")
    if (result._tag !== "Success") throw new Error("expected startup admission")

    const init = parseSessionWorkerInit(result.success)
    expect(init.plugins?.map((plugin) => plugin.name)).toEqual(["configured"])

    const exit = await triggerTextComplete(bootWorker(init).layer, "streamed text")
    expect(Exit.isSuccess(exit)).toBe(true)
    if (!Exit.isSuccess(exit)) throw new Error(String(Cause.squash(exit.cause)))
    expect(exit.value).toEqual({ text: "streamed text" })
  })

  test("a declared text.complete hook no longer fails the turn", async () => {
    const result = await buildInit({ "experimental.text.complete": async () => {} })
    expect(result._tag).toBe("Success")
    if (result._tag !== "Success") throw new Error("expected startup admission")

    const exit = await triggerTextComplete(bootWorker(parseSessionWorkerInit(result.success)).layer, "streamed text")
    if (Exit.isFailure(exit)) throw new Error(Cause.pretty(exit.cause))
  })
})
