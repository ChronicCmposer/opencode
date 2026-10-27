// D19 / C4 — routed plugin hooks are REAL in a worker.
//
// The worker cannot run a plugin function itself (the plugin instance lives on
// the hub), so `resolveWorkerPlugins` must never drop a routed hook and the
// worker must forward it over the hub-proxy control port. This suite drives a
// real plugin module written to disk, the production resolver, the production
// worker `Plugin.Service`, a real `MessageChannel` control port, and the
// production hub authority. Nothing is stubbed at the boundary under test.

import { describe, expect, test } from "bun:test"
import { Cause, Effect, Exit, Layer, Option } from "effect"
import { mkdtemp, rm } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { pathToFileURL } from "node:url"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import { ModelsDev } from "@opencode-ai/core/models-dev"
import type { ConfigV1 } from "@opencode-ai/core/v1/config/config"
import type { Hooks } from "@opencode-ai/plugin"
import { Auth } from "@/auth"
import { Config } from "@/config/config"
import { InstanceRef } from "@/effect/instance-ref"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Provider } from "@/provider/provider"
import { Plugin } from "@/plugin"
import { UnsupportedPluginHookError } from "@/plugin/routes"
import { ToolRegistry } from "@/tool/registry"
import {
  buildHubProxyHandlers,
  pluginTriggerAuthority,
} from "@/session/worker/hub"
import {
  ModelClient,
  buildSessionWorkerLayer,
  type SessionLayerOptions,
} from "@/session/worker/layer"
import {
  buildSessionWorkerInit,
  resolveWorkerPlugins,
  type WorkerModel,
} from "@/session/worker/init"
import { createWorkerPluginHost, type WorkerPluginHooks } from "@/session/worker/plugin-host"
import { hubProxyPluginTrigger } from "@/session/worker/session-worker"
import { instantRunner, recordingHubAuthorities, serveHubProxyFor } from "../../lib/hub-proxy"
import { workerSnapshot } from "../../lib/snapshot"

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

const instance = {
  directory: "/tmp/opencode-plugin-hub-routing",
  worktree: "/tmp/opencode-plugin-hub-routing",
  project: { id: "prj_plugin_hub_routing", worktree: "/tmp/opencode-plugin-hub-routing", vcs: "git" },
} as never

/** A real plugin module on disk; its default export returns the real hooks. */
const PLUGIN_SOURCE = `
export default async () => ({
  "tool.execute.before": (input, output) => {
    if (input.tool === "bash" && output.args && output.args.command === "rm -rf /") {
      throw new Error("policy blocked: rm -rf /")
    }
    output.args = { ...(output.args ?? {}), injected: true }
  },
  "tool.execute.after": (_input, output) => {
    output.output = output.output + "!"
  },
  "shell.env": (_input, output) => {
    output.env.PLUGIN_FLAG = "1"
  },
  "tool.definition": (_input, output) => {
    output.description = output.description + " [plugin]"
  },
  "chat.params": (input, output) => {
    output.temperature = input?.model?.id === "other-model" ? 0.75 : 0.25
  },
  "chat.headers": (input, output) => {
    output.headers["x-plugin-model"] = String(input?.model?.id ?? "none")
  },
  "chat.message": (_input, output) => {
    output.message.agent = "plugin-agent"
    output.parts.push({ type: "text", text: "from-plugin" })
  },
})
`

async function loadRealPlugin() {
  const dir = await mkdtemp(path.join(os.tmpdir(), "opencode-plugin-hub-routing-"))
  const file = path.join(dir, "plugin.ts")
  await Bun.write(file, PLUGIN_SOURCE)
  const mod = (await import(`${pathToFileURL(file).href}?v=${Date.now()}`)) as {
    default: () => Promise<Hooks>
  }
  const hooks = await mod.default()
  return { hooks, dispose: () => rm(dir, { recursive: true, force: true }) }
}

/** The hub-side `Plugin.Service` the authority runs (the real loaded hooks). */
function hubPlugin(hooks: Hooks): Plugin.Interface {
  return {
    trigger: ((name: string, input: unknown, output: unknown) =>
      Effect.promise(async () => {
        const fn = (hooks as Record<string, unknown>)[name] as
          | ((input: unknown, output: unknown) => void | Promise<void>)
          | undefined
        if (fn !== undefined) await fn(input, output)
        return output
      })) as never,
    list: () => Effect.succeed([hooks]),
    init: () => Effect.void,
  } as unknown as Plugin.Interface
}

function boundary(hooks: Hooks) {
  const recording = recordingHubAuthorities({
    pluginTrigger: pluginTriggerAuthority({
      plugin: () => Option.some(hubPlugin(hooks)),
      run: instantRunner,
    }),
  })
  const served = serveHubProxyFor(buildHubProxyHandlers(recording.authorities, instantRunner))
  const bridge = hubProxyPluginTrigger(served.client)
  if (bridge === undefined) throw new Error("a real hub client must yield a plugin-hook bridge")
  return { bridge, calls: recording.calls, served }
}

const clients = (): SessionLayerOptions["clients"] => ({
  writer: { query: async () => [], publish: async () => ({ seq: 1, committed: true }) },
  egress: { emit: () => {} },
  backgroundJob: { wait: async () => null, cancel: async () => {} },
})

function workerLayer(plugins: ReturnType<typeof createWorkerPluginHost>) {
  return buildSessionWorkerLayer({
    snapshot: workerSnapshot(),
    env: { values: {} },
    clients: clients(),
    modelClient: Layer.succeed(ModelClient, { complete: () => Effect.succeed({ text: "stub" }) }),
    plugins,
  }).layer
}

const trigger = (
  layer: Layer.Layer<unknown, unknown>,
  name: string,
  input: unknown,
  output: unknown,
) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const plugin = yield* Plugin.Service
      return yield* plugin.trigger(name as never, input as never, output as never)
    }).pipe(Effect.provide(layer), Effect.provideService(InstanceRef, instance), Effect.exit),
  )

/** The exact production bridge: resolve with the shipped resolver, host the worker. */
async function productionWorker(hooks: Hooks) {
  const resolved = await Effect.runPromise(
    resolveWorkerPlugins({ model: workerModel(), sources: [{ name: "real", hooks: hooks as WorkerPluginHooks }] }),
  )
  const wired = boundary(hooks)
  const host = createWorkerPluginHost(resolved, { hubHook: wired.bridge })
  return { resolved, host, layer: workerLayer(host), wired }
}

describe("tool.execute.before/after are real in a worker", () => {
  test("a plugin source mutates and denies tool args across the control port", async () => {
    const real = await loadRealPlugin()
    try {
      const { resolved, layer, wired } = await productionWorker(real.hooks)
      // C4: the routed hook is not silently dropped; the function stays hub-side.
      expect(resolved[0]?.hooks).toEqual({})
      expect(resolved[0]?.proxied).toContain("tool.execute.before")
      expect(resolved[0]?.proxied).toContain("tool.execute.after")

      const before = await trigger(layer, "tool.execute.before", { tool: "bash" }, { args: { command: "ls" } })
      expect(Exit.isSuccess(before)).toBe(true)
      if (!Exit.isSuccess(before)) throw new Error("before hook failed")
      expect((before.value as { args: unknown }).args).toEqual({ command: "ls", injected: true })

      const denied = await trigger(layer, "tool.execute.before", { tool: "bash" }, { args: { command: "rm -rf /" } })
      expect(Exit.isFailure(denied)).toBe(true)
      if (Exit.isFailure(denied)) {
        expect(String(Cause.squash(denied.cause))).toContain("policy blocked")
      }

      const after = await trigger(layer, "tool.execute.after", { tool: "bash" }, { title: "t", output: "done", metadata: {} })
      expect(Exit.isSuccess(after)).toBe(true)
      if (!Exit.isSuccess(after)) throw new Error("after hook failed")
      expect((after.value as { output: string }).output).toBe("done!")

      expect(wired.calls.filter((call) => call.method === "pluginTrigger")).toHaveLength(3)
    } finally {
      await real.dispose()
    }
  })

  test("mutations land on the caller's objects, not just the returned clone", async () => {
    const real = await loadRealPlugin()
    try {
      const { layer } = await productionWorker(real.hooks)

      // `tool.execute.before`: the call sites keep the args object they passed
      // and ignore the trigger return, so the injected field must be observable
      // on that original reference.
      const beforeOutput = { args: { command: "ls" } as Record<string, unknown> }
      await trigger(layer, "tool.execute.before", { tool: "bash" }, beforeOutput)
      expect(beforeOutput.args).toEqual({ command: "ls", injected: true })

      // `tool.execute.after`: same contract for the result object.
      const afterOutput = { title: "t", output: "done", metadata: {} }
      await trigger(layer, "tool.execute.after", { tool: "bash" }, afterOutput)
      expect(afterOutput.output).toBe("done!")

      // `tool.definition`: the registry reads `output` after the trigger.
      const definition = { description: "base", parameters: {}, jsonSchema: {} }
      await trigger(layer, "tool.definition", { toolID: "bash" }, definition)
      expect(definition.description).toBe("base [plugin]")

      // `chat.message`: both the message and the parts array are reconciled in
      // place, preserving the identities the prompt path keeps using.
      const message = { agent: "build" }
      const parts: unknown[] = []
      const chatOutput = { message, parts }
      await trigger(layer, "chat.message", { sessionID: "ses" }, chatOutput)
      expect(message.agent).toBe("plugin-agent")
      expect(parts).toEqual([{ type: "text", text: "from-plugin" }])
    } finally {
      await real.dispose()
    }
  })

  test("chat.params and chat.headers resolve against the request's current model", async () => {
    const real = await loadRealPlugin()
    try {
      const { layer } = await productionWorker(real.hooks)
      const baseline = () => ({ temperature: 0.1, topP: 1, topK: 1, maxOutputTokens: 100, options: {} })

      const first = await trigger(layer, "chat.params", { model: { id: "mock-model" } }, baseline())
      const second = await trigger(layer, "chat.params", { model: { id: "other-model" } }, baseline())
      if (!Exit.isSuccess(first) || !Exit.isSuccess(second)) throw new Error("chat.params failed")
      // The init-resolved diff was 0.25; the second request must not be frozen to it.
      expect((first.value as { temperature?: number }).temperature).toBe(0.25)
      expect((second.value as { temperature?: number }).temperature).toBe(0.75)

      const headersA = await trigger(layer, "chat.headers", { model: { id: "mock-model" } }, { headers: {} })
      const headersB = await trigger(layer, "chat.headers", { model: { id: "other-model" } }, { headers: {} })
      if (!Exit.isSuccess(headersA) || !Exit.isSuccess(headersB)) throw new Error("chat.headers failed")
      expect((headersA.value as { headers: Record<string, string> }).headers["x-plugin-model"]).toBe("mock-model")
      expect((headersB.value as { headers: Record<string, string> }).headers["x-plugin-model"]).toBe("other-model")
    } finally {
      await real.dispose()
    }
  })

  test("the hub-side bridge rejects a hook that is not hub-proxy", async () => {
    const real = await loadRealPlugin()
    try {
      const { wired } = await productionWorker(real.hooks)
      const error = await wired.bridge("command.execute.before", {}, { parts: [] }).catch((failure: unknown) => failure)
      // The wire reconstructs the remote tagged error (the `_tag` metadata is
      // carried so callers can branch on the domain failure, not a bare string).
      expect((error as { tag?: string }).tag).toBe("UnsupportedPluginHookError")
      expect(String((error as Error).message)).toContain("command.execute.before")
    } finally {
      await real.dispose()
    }
  })
})

describe("unsupported hooks are never invoked in a worker", () => {
  test("listing tools and starting a shell does not raise UnsupportedPluginHookError", async () => {
    const real = await loadRealPlugin()
    try {
      const { layer, wired } = await productionWorker(real.hooks)

      // The REAL tool registry call site (`tool.definition`) runs in the worker.
      const listed = await Effect.runPromise(
        Effect.gen(function* () {
          const registry = yield* ToolRegistry.Service
          return yield* registry.tools({
            providerID: ProviderV2.ID.make("mock"),
            modelID: ModelV2.ID.make("mock-model"),
            agent: { name: "build", mode: "primary", permission: [] } as never,
          })
        }).pipe(Effect.provide(layer), Effect.provideService(InstanceRef, instance), Effect.exit),
      )
      if (Exit.isFailure(listed)) throw new Error(Cause.pretty(listed.cause))
      expect(Exit.isSuccess(listed)).toBe(true)
      if (!Exit.isSuccess(listed)) throw new Error("registry.tools failed")
      // The registry ignores the trigger return and reads `output`; the plugin's
      // description mutation must still be visible there.
      expect(listed.value.some((tool) => tool.description.includes("[plugin]"))).toBe(true)

      // The shell tool's env call site forwards to the hub instead of dispatching
      // an unsupported trigger locally.
      const env = await trigger(layer, "shell.env", { cwd: "/tmp" }, { env: {} })
      expect(Exit.isSuccess(env)).toBe(true)
      if (!Exit.isSuccess(env)) throw new Error(String(Cause.squash(env.cause)))
      expect((env.value as { env: Record<string, string> }).env.PLUGIN_FLAG).toBe("1")

      const forwarded = wired.calls.filter((call) => call.method === "pluginTrigger")
      expect(forwarded.some((call) => call.args[0] === "tool.definition")).toBe(true)
      expect(forwarded.some((call) => call.args[0] === "shell.env")).toBe(true)
    } finally {
      await real.dispose()
    }
  })
})

describe("admission through the production init factory", () => {
  const modelLayers = () =>
    Layer.mergeAll(
      Layer.mock(Provider.Service, {
        defaultModel: () =>
          Effect.succeed({ providerID: ProviderV2.ID.make("mock"), modelID: ModelV2.ID.make("mock-model") }),
        getProvider: () =>
          Effect.succeed({
            id: "mock",
            name: "Mock",
            source: "config",
            env: [],
            key: "k",
            options: {},
            models: {},
          } as never),
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

  const build = (hooks: Hooks) =>
    Effect.runPromise(
      Effect.result(
        buildSessionWorkerInit({
          model: { session: { providerID: "mock", modelID: "mock-model" } },
          plugins: (model) =>
            resolveWorkerPlugins({ model, sources: [{ name: "real", hooks: hooks as WorkerPluginHooks }] }),
        }).pipe(Effect.provide(modelLayers()), Effect.provideService(InstanceRef, instance)),
      ),
    )

  test("a routed plugin source ships through buildSessionWorkerInit", async () => {
    const real = await loadRealPlugin()
    try {
      const result = await build(real.hooks)
      expect(result._tag).toBe("Success")
      if (result._tag !== "Success") throw new Error(String(result.failure))
      const shipped = result.success.plugins ?? []
      expect(shipped[0]?.resolved?.["chat.params"]).toEqual({ temperature: 0.25 })
      expect(shipped[0]?.proxied).toContain("tool.execute.before")
    } finally {
      await real.dispose()
    }
  })

  test("an unsupported hook added after startup fails the factory admission", async () => {
    const real = await loadRealPlugin()
    try {
      const result = await build({ ...real.hooks, "command.execute.before": (() => {}) as never })
      expect(result._tag).toBe("Failure")
      if (result._tag !== "Failure") throw new Error("expected admission failure")
      expect(result.failure).toBeInstanceOf(UnsupportedPluginHookError)
    } finally {
      await real.dispose()
    }
  })
})
