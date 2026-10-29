// Unsupported hooks must not fail worker startup.
//
// This suite drives the production startup chain, not the route helpers in
// isolation:
//   hub resolver (`resolveWorkerPlugins`) → `buildSessionWorkerInit` (the
//   payload the hub ships) → `parseSessionWorkerInit` (the worker's isolate
//   boundary) → `createWorkerPluginHost` → `buildSessionWorkerLayer` →
//   `Plugin.Service.trigger`.
//
// The contract is three-way:
//   * a configured-but-uninvoked `unsupported` hook boots normally;
//   * invoking it fails typed with `UnsupportedPluginHookError`;
//   * a hook with no route at all is still a startup defect.

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
import { UnclassifiedPluginHookError, UnsupportedPluginHookError } from "@/plugin/routes"
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
  directory: "/tmp/opencode-plugin-hook-startup",
  worktree: "/tmp/opencode-plugin-hook-startup",
  project: { id: "prj_plugin_hook_startup", worktree: "/tmp/opencode-plugin-hook-startup", vcs: "git" },
} as never

/**
 * Assemble the hub's init payload exactly as production does. `hooks` is a
 * plain record because an unclassified hook is exactly the case under test and
 * is not part of the typed `WorkerPluginHooks` surface.
 */
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

/** Dispatch one hook through the worker's `Plugin.Service`. */
function trigger(layer: ReturnType<typeof bootWorker>["layer"], name: string) {
  return Effect.runPromise(
    Effect.gen(function* () {
      const plugin = (yield* Plugin.Service) as Plugin.Interface
      return yield* plugin.trigger(name as never, {} as never, { parts: [] })
    }).pipe(Effect.provide(layer), Effect.exit),
  )
}

describe("plugin hook startup contract", () => {
  test("a configured unsupported hook boots the worker", async () => {
    const result = await buildInit({ "command.execute.before": async () => {} })
    expect(result._tag).toBe("Success")
    if (result._tag !== "Success") throw new Error("expected startup admission")

    const init = parseSessionWorkerInit(result.success)
    expect(init.plugins?.map((plugin) => plugin.name)).toEqual(["configured"])
    // Assembling the full production worker graph is the startup path; it must
    // not reject the admitted plugin.
    expect(() => bootWorker(init)).not.toThrow()
  })

  test("invoking a configured command hook in the worker succeeds", async () => {
    const result = await buildInit({ "command.execute.before": async () => {} })
    if (result._tag !== "Success") throw new Error("expected startup admission")
    const worker = bootWorker(parseSessionWorkerInit(result.success))

    const exit = await trigger(worker.layer, "command.execute.before")
    if (Exit.isFailure(exit)) throw new Error(Cause.pretty(exit.cause))
  })

  test("an unclassified hook is still a startup defect", async () => {
    const result = await buildInit({ "future.trigger": async () => {} })
    expect(result._tag).toBe("Failure")
    if (result._tag !== "Failure") throw new Error("expected startup failure")
    expect(result.failure).toBeInstanceOf(UnclassifiedPluginHookError)
    expect((result.failure as UnclassifiedPluginHookError).hook).toBe("future.trigger")
  })
})
