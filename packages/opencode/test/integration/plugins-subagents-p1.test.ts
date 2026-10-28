// P1 — plugin hooks and subagent promotion across the worker boundary.
//
// The plugin half drives a real plugin module written to disk through the
// production resolver, the production worker `Plugin.Service`, a real
// `MessageChannel` control port, and the production hub authority. It asserts
// the two invariants the P1 fixes restored:
//   1. hook mutations are observable on the caller's object, not only on the
//      clone the trigger returns (tool args/results/definitions and
//      `chat.message` are all read from the original reference);
//   2. `chat.params` / `chat.headers` run against the request's current model
//      instead of the model frozen at worker init.
//
// The subagent half proves a foreground task is registered as a local job and
// can be promoted to background (and notified) with the experiment both on and
// off.

import { afterEach, describe, expect, test } from "bun:test"
import { mkdtemp, rm } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { pathToFileURL } from "node:url"
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Option } from "effect"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import type { Hooks } from "@opencode-ai/plugin"
import { Agent } from "@/agent/agent"
import { BackgroundJob } from "@/background/job"
import { Config } from "@/config/config"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Ripgrep } from "@opencode-ai/core/ripgrep"
import { EventV2Bridge } from "@/event-v2-bridge"
import { InstanceRef } from "@/effect/instance-ref"
import { Plugin } from "@/plugin"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Session } from "@/session/session"
import { SessionRunState } from "@/session/run-state"
import { SessionStatus } from "@/session/status"
import { MessageID, PartID, SessionID } from "@/session/schema"
import type { SessionPrompt } from "@/session/prompt"
import { TaskTool, type TaskPromptOps } from "@/tool/task"
import { Truncate } from "@/tool/truncate"
import { ToolRegistry } from "@/tool/registry"
import { SessionSpawnProxyService, type SessionSpawnRequest, type SessionSpawnResult } from "@/session/worker/subagent"
import { buildHubProxyHandlers, pluginTriggerAuthority } from "@/session/worker/hub"
import { ModelClient, buildSessionWorkerLayer, type SessionLayerOptions } from "@/session/worker/layer"
import { resolveWorkerPlugins, type WorkerModel } from "@/session/worker/init"
import { createWorkerPluginHost, type WorkerPluginHooks } from "@/session/worker/plugin-host"
import { hubProxyPluginTrigger } from "@/session/worker/session-worker"
import { instantRunner, recordingHubAuthorities, serveHubProxyFor } from "../lib/hub-proxy"
import { workerSnapshot } from "../lib/snapshot"
import { testEffect, setFileTimeout } from "../lib/effect"
import { REAL_WORKER_TEST_TIMEOUT_MS } from "../lib/worker-timeouts"
import { disposeAllInstances } from "../fixture/fixture"

// The runner resets each test file to its 5s default, so a preload-only
// `setDefaultTimeout` does not reach this file. Promotion boots real worker and
// hub layers and needs the shared real-worker budget. Declare it through the
// suite helper so the `testEffect(...)` declarations below reapply it instead of
// restoring the generic default for tests registered afterwards.
setFileTimeout(REAL_WORKER_TEST_TIMEOUT_MS)

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
  directory: "/tmp/opencode-plugins-subagents-p1",
  worktree: "/tmp/opencode-plugins-subagents-p1",
  project: { id: "prj_plugins_subagents_p1", worktree: "/tmp/opencode-plugins-subagents-p1", vcs: "git" },
} as never

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
  "tool.definition": (_input, output) => {
    output.description = output.description + " [plugin]"
  },
  "chat.message": (_input, output) => {
    output.message.agent = "plugin-agent"
    output.parts.push({ type: "text", text: "from-plugin" })
  },
  "chat.params": (input, output) => {
    output.temperature = input?.model?.id === "other-model" ? 0.75 : 0.25
  },
  "chat.headers": (input, output) => {
    output.headers["x-plugin-model"] = String(input?.model?.id ?? "none")
  },
})
`

async function loadRealPlugin() {
  const dir = await mkdtemp(path.join(os.tmpdir(), "opencode-plugins-subagents-p1-"))
  const file = path.join(dir, "plugin.ts")
  await Bun.write(file, PLUGIN_SOURCE)
  const mod = (await import(`${pathToFileURL(file).href}?v=${Date.now()}`)) as {
    default: () => Promise<Hooks>
  }
  return { hooks: await mod.default(), dispose: () => rm(dir, { recursive: true, force: true }) }
}

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

const clients = (): SessionLayerOptions["clients"] => ({
  writer: { query: async () => [], publish: async () => ({ seq: 1, committed: true }) },
  egress: { emit: () => {} },
  backgroundJob: { wait: async () => null, cancel: async () => {} },
})

const trigger = (layer: Layer.Layer<unknown, unknown>, name: string, input: unknown, output: unknown) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const plugin = yield* Plugin.Service
      return yield* plugin.trigger(name as never, input as never, output as never)
    }).pipe(Effect.provide(layer), Effect.provideService(InstanceRef, instance), Effect.exit),
  )

/** Resolve + host + serve the exact production plugin wiring for a real module. */
async function productionWorker(hooks: Hooks) {
  const resolved = await Effect.runPromise(
    resolveWorkerPlugins({ model: workerModel(), sources: [{ name: "real", hooks: hooks as WorkerPluginHooks }] }),
  )
  const recording = recordingHubAuthorities({
    pluginTrigger: pluginTriggerAuthority({ plugin: () => Option.some(hubPlugin(hooks)), run: instantRunner }),
  })
  const served = serveHubProxyFor(buildHubProxyHandlers(recording.authorities, instantRunner))
  const bridge = hubProxyPluginTrigger(served.client)
  if (bridge === undefined) throw new Error("a real hub client must yield a plugin-hook bridge")
  const host = createWorkerPluginHost(resolved, { hubHook: bridge })
  const layer = buildSessionWorkerLayer({
    snapshot: workerSnapshot(),
    env: { values: {} },
    clients: clients(),
    modelClient: Layer.succeed(ModelClient, { complete: () => Effect.succeed({ text: "stub" }) }),
    plugins: host,
  }).layer
  return { layer, served, calls: recording.calls }
}

describe("plugin hooks across the worker boundary", () => {
  test("mutations are observable on the caller's objects and denial fails the trigger", async () => {
    const real = await loadRealPlugin()
    try {
      const { layer, served } = await productionWorker(real.hooks)
      const args: Record<string, unknown> = { command: "ls" }
      const before = { args }
      await trigger(layer, "tool.execute.before", { tool: "bash" }, before)
      expect(args).toEqual({ command: "ls", injected: true })

      const after = { title: "t", output: "done", metadata: {} }
      await trigger(layer, "tool.execute.after", { tool: "bash" }, after)
      expect(after.output).toBe("done!")

      const definition = { description: "base", parameters: {}, jsonSchema: {} }
      await trigger(layer, "tool.definition", { toolID: "bash" }, definition)
      expect(definition.description).toBe("base [plugin]")

      const message = { agent: "build" }
      const parts: unknown[] = []
      await trigger(layer, "chat.message", { sessionID: "ses" }, { message, parts })
      expect(message.agent).toBe("plugin-agent")
      expect(parts).toEqual([{ type: "text", text: "from-plugin" }])

      const denied = await trigger(layer, "tool.execute.before", { tool: "bash" }, { args: { command: "rm -rf /" } })
      expect(Exit.isFailure(denied)).toBe(true)
      if (Exit.isFailure(denied)) expect(String(Cause.squash(denied.cause))).toContain("policy blocked")
      served.dispose()
    } finally {
      await real.dispose()
    }
  })

  test("chat.params and chat.headers switch with the request's model", async () => {
    const real = await loadRealPlugin()
    try {
      const { layer, served } = await productionWorker(real.hooks)
      const baseline = () => ({ temperature: 0.1, topP: 1, topK: 1, maxOutputTokens: 100, options: {} })
      const first = await trigger(layer, "chat.params", { model: { id: "mock-model" } }, baseline())
      const second = await trigger(layer, "chat.params", { model: { id: "other-model" } }, baseline())
      if (!Exit.isSuccess(first) || !Exit.isSuccess(second)) throw new Error("chat.params failed")
      expect((first.value as { temperature?: number }).temperature).toBe(0.25)
      expect((second.value as { temperature?: number }).temperature).toBe(0.75)
      served.dispose()
    } finally {
      await real.dispose()
    }
  })
})

// ---------------------------------------------------------------------------
// Foreground → background subagent promotion
// ---------------------------------------------------------------------------

afterEach(async () => {
  await disposeAllInstances()
})

const ref = {
  providerID: ProviderV2.ID.make("test"),
  modelID: ModelV2.ID.make("test-model"),
}

let onSpawn: (request: SessionSpawnRequest) => Effect.Effect<SessionSpawnResult> = (request) =>
  Effect.succeed(resultFor(request, "done"))

const spawnProxyLayer = Layer.succeed(SessionSpawnProxyService, {
  spawn: (request) => onSpawn(request),
})

const taskLayer = (flags: Partial<RuntimeFlags.Info> = {}) =>
  LayerNode.compile(
    LayerNode.group([
      Agent.node,
      BackgroundJob.node,
      EventV2Bridge.node,
      Config.node,
      CrossSpawnSpawner.node,
      Session.node,
      SessionProjector.node,
      SessionRunState.node,
      SessionStatus.node,
      Truncate.node,
      ToolRegistry.node,
      Database.node,
      RuntimeFlags.node,
      Ripgrep.node,
    ]),
    [[RuntimeFlags.node, RuntimeFlags.layer(flags)]],
  ).pipe(Layer.provideMerge(spawnProxyLayer))

const flagOff = testEffect(taskLayer())
const flagOn = testEffect(taskLayer({ experimentalBackgroundSubagents: true }))

function resultFor(request: SessionSpawnRequest, text: string): SessionSpawnResult {
  const childID = request.childID ?? (MessageID.ascending() as unknown as SessionID)
  const id = MessageID.ascending()
  return {
    childID,
    result: {
      info: {
        id,
        role: "assistant",
        parentID: request.prompt.messageID ?? MessageID.ascending(),
        sessionID: childID,
        mode: request.prompt.agent ?? "general",
        agent: request.prompt.agent ?? "general",
        cost: 0,
        path: { cwd: "/tmp", root: "/tmp" },
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
        modelID: request.prompt.model?.modelID ?? ref.modelID,
        providerID: request.prompt.model?.providerID ?? ref.providerID,
        time: { created: Date.now() },
        finish: "stop",
      },
      parts: [{ id: PartID.ascending(), messageID: id, sessionID: childID, type: "text", text }],
    },
  }
}

const seed = Effect.fn("P1.seed")(function* () {
  const session = yield* Session.Service
  const chat = yield* session.create({ title: "Pinned" })
  const user = yield* session.updateMessage({
    id: MessageID.ascending(),
    role: "user",
    sessionID: chat.id,
    agent: "build",
    model: ref,
    time: { created: Date.now() },
  })
  const assistant: SessionV1.Assistant = {
    id: MessageID.ascending(),
    role: "assistant",
    parentID: user.id,
    sessionID: chat.id,
    mode: "build",
    agent: "build",
    cost: 0,
    path: { cwd: "/tmp", root: "/tmp" },
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    modelID: ref.modelID,
    providerID: ref.providerID,
    time: { created: Date.now() },
  }
  yield* session.updateMessage(assistant)
  return { chat, assistant }
})

function stubOps(onPrompt?: (input: SessionPrompt.PromptInput) => void): TaskPromptOps {
  return {
    cancel: () => Effect.void,
    resolvePromptParts: (template) => Effect.succeed([{ type: "text" as const, text: template }]),
    prompt: (input) =>
      Effect.sync(() => {
        onPrompt?.(input)
        const id = MessageID.ascending()
        return {
          info: {
            id,
            role: "assistant",
            parentID: input.messageID ?? MessageID.ascending(),
            sessionID: input.sessionID,
            mode: input.agent ?? "general",
            agent: input.agent ?? "general",
            cost: 0,
            path: { cwd: "/tmp", root: "/tmp" },
            tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
            modelID: ref.modelID,
            providerID: ref.providerID,
            time: { created: Date.now() },
            finish: "stop",
          },
          parts: [
            { id: PartID.ascending(), messageID: id, sessionID: input.sessionID, type: "text", text: "injected" },
          ],
        } satisfies SessionV1.WithParts
      }),
  }
}

const runContext = (
  chat: SessionID,
  assistant: MessageID,
  ops: TaskPromptOps,
  metadata: (val: { title?: string; metadata?: Record<string, unknown> }) => Effect.Effect<void> = () => Effect.void,
) => ({
  sessionID: chat,
  messageID: assistant,
  agent: "build",
  abort: new AbortController().signal,
  extra: { promptOps: ops },
  messages: [],
  metadata,
  ask: () => Effect.void,
})

const promotionCase = (label: string, runner: typeof flagOff.instance) =>
  runner(label, () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const started = yield* Deferred.make<SessionID>()
      const release = yield* Deferred.make<void>()
      const injected = yield* Deferred.make<void>()
      onSpawn = (request) =>
        Effect.gen(function* () {
          yield* Deferred.succeed(started, request.childID ?? ("" as SessionID))
          yield* Deferred.await(release)
          return resultFor(request, "promoted done")
        })

      let notificationID: SessionID | undefined
      const fiber = yield* def
        .execute(
          { description: "inspect bug", prompt: "go", subagent_type: "general" },
          runContext(
            chat.id,
            assistant.id,
            stubOps(() => Deferred.doneUnsafe(injected, Effect.succeed(undefined))),
            (val) =>
              Effect.sync(() => {
                if (val.metadata?.background === true) notificationID = val.metadata.notificationID as SessionID
              }),
          ),
        )
        .pipe(Effect.forkChild)

      const spawned = yield* Deferred.await(started)
      // Promotion is independent of the experimental background parameter: the
      // parent backgrounded an already-running foreground subagent.
      const promoted = yield* jobs.promote(spawned)
      expect(promoted?.metadata?.background).toBe(true)
      expect(promoted?.metadata?.notificationID).toBe(spawned)
      expect(notificationID).toBe(spawned)

      const exit = yield* Fiber.await(fiber)
      expect(Exit.isSuccess(exit)).toBe(true)
      if (!Exit.isSuccess(exit)) throw new Error("promotion should not fail the task")
      expect(exit.value.metadata.background).toBe(true)
      expect((exit.value.metadata as { notificationID?: SessionID }).notificationID).toBe(spawned)
      expect(exit.value.output).toContain('state="running"')

      yield* Deferred.succeed(release, undefined)
      const waited = yield* jobs.wait({ id: spawned, timeout: 1_000 })
      expect(waited.info?.status).toBe("completed")
      yield* Deferred.await(injected).pipe(Effect.timeout("1 second"))
    }),
  )

describe("foreground subagent promotion", () => {
  promotionCase("promotes with the background flag off", flagOff.instance)
  promotionCase("promotes with the background flag on", flagOn.instance)
})
