// DCP parity end-to-end: a plugin tool executes on the hub, and the
// `experimental.chat.messages.transform` that runs NEXT (also on the hub) sees
// the mutation.
//
// `@tarquinen/opencode-dcp` coordinates its `compress` tool and its
// `experimental.chat.messages.transform` hook through ONE in-memory `state`
// object: the tool writes the compression blocks, the transform reads them to
// rewrite the outbound messages. Under session workers plugins live on the hub,
// so BOTH sides must run there. The worker only ships clone-safe descriptors and
// forwards each tool call back over the control port (`plugin-tools-design.md`
// §0/§8).
//
// This file proves that end to end against the REAL boundary and the REAL
// package (loaded through a temporary opencode config, never a mock DCP):
//   * a real `MessageChannel` with `serveHubProxy` (hub) + `createHubProxyClient`
//     (worker);
//   * the real `@tarquinen/opencode-dcp` plugin's `compress` tool and
//     `messages.transform` sharing one hub state;
//   * the worker built with `pluginTools: { descriptors, executor }`, so the real
//     `ToolRegistry` registers the remote `compress` tool;
//   * invoking the tool through the worker crosses the port, mutates the HUB
//     state, and changes the NEXT transform's output.
//
// Two tests: the smallest real version (registry-level, no provider) and the
// full prompt turn whose SECOND outbound provider request reflects the mutation.

import { describe, expect, test } from "bun:test"
import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { pathToFileURL } from "node:url"
import { Effect, Layer, Option } from "effect"
import type { Hooks, PluginInput } from "@opencode-ai/plugin"
import { Plugin } from "@/plugin"
import { PluginLoader } from "@/plugin/loader"
import { MessageID, PartID, type SessionID } from "@/session/schema"
import { Session } from "@/session/session"
import { SessionPrompt } from "@/session/prompt"
import { InstanceRef } from "@/effect/instance-ref"
import { Permission } from "@/permission"
import { Agent } from "@/agent/agent"
import { Truncate } from "@/tool/truncate"
import { pluginToolJsonSchema } from "@/tool/registry"
import { ModelClient } from "@/session/worker/layer"
import {
  buildHubProxyHandlers,
  messagesTransformAuthority,
  pluginToolAuthority,
  type HubProxyAuthorities,
  type HubProxyRunner,
} from "@/session/worker/hub"
import { createWorkerPluginHost, type MessagesTransformOutput } from "@/session/worker/plugin-host"
import { hubProxyMessagesTransform, hubProxyPluginTool, workerGraphReplacements } from "@/session/worker/session-worker"
import {
  buildSessionWorkerLayer,
  createHubProxyClient,
  llmModelClientLayer,
  reducedLlmLayer,
  serveHubProxy,
  type WorkerModelConfig,
  type WorkerSnapshot,
} from "@/session/worker/layer"
import {
  isRemotePluginToolDefinition,
  type PluginToolDescriptor,
  type PluginToolExecuteInput,
} from "@/session/worker/plugin-tool-wire"
import { recordingHubAuthorities } from "../../lib/hub-proxy"
import { createE2EContext, memoizeWorkerLayer, type E2EContext, type MemoizedWorkerLayer } from "../../lib/e2e-context"
import { pluginPassThroughReplacement } from "../../lib/worker-plugin-routes"
import { startMockProvider, type MockProviderHandle } from "../../lib/mock-provider"
import { workerSnapshot } from "../../lib/snapshot"

/** The exact sentinel `@tarquinen/opencode-dcp` writes in place of pruned output. */
const SENTINEL = "[Output removed to save context - information superseded or no longer needed]"

/** The older completed tool output the real DCP deduplication must prune. */
const OLD_OUTPUT = "TOOL_OUTPUT_ORIGINAL"

/** The newer duplicate tool output; DCP keeps the newest duplicate. */
const NEW_OUTPUT = "TOOL_OUTPUT_NEW"

/** The user text of the seeded turn; used to locate the outbound request. */
const TURN_TEXT = "third"

/** The compression summary the real DCP `compress` tool stores. */
const SUMMARY = "SUMMARY_OF_SECOND_TURN"

/** Real plugin fixtures: the patched local DCP build and the workspace profile plugin. */
const DCP_PLUGIN = "/var/lib/opencode/dev/opencode-dcp/dist/index.js"
const WORKSPACE_PLUGIN = "/var/lib/opencode/.config/opencode/profiles/ws/plugins/workspace-plugin.ts"

/** The serializable model bootstrap the worker talks to. */
const modelConfig = (provider: MockProviderHandle): WorkerModelConfig => ({
  providerID: "mock",
  modelID: provider.model,
  apiId: provider.model,
  npm: "@ai-sdk/openai-compatible",
  baseUrl: provider.baseUrl,
  apiKey: provider.apiKey,
  headers: {},
})

/** A temporary opencode config listing both real plugins (never the user's config). */
interface TempPluginConfig {
  readonly directory: string
  readonly file: string
  dispose(): void
}

function tempPluginConfig(): TempPluginConfig {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "oc-real-plugins-"))
  const file = path.join(directory, "opencode.json")
  fs.writeFileSync(
    file,
    JSON.stringify(
      {
        $schema: "https://opencode.ai/config.json",
        plugin: [pathToFileURL(DCP_PLUGIN).href, pathToFileURL(WORKSPACE_PLUGIN).href],
      },
      null,
      2,
    ),
  )
  return {
    directory,
    file,
    dispose: () => fs.rmSync(directory, { recursive: true, force: true }),
  }
}

/** The hub client the real DCP plugin captures; messages are bound after the worker layer exists. */
interface SessionMessagesClient {
  readonly client: PluginInput["client"]
  bind(resolve: () => Promise<ReadonlyArray<unknown>>): void
}

function sessionMessagesClient(initial: ReadonlyArray<unknown> = []): SessionMessagesClient {
  let resolveMessages: () => Promise<ReadonlyArray<unknown>> = async () => initial
  const client = {
    session: {
      messages: async () => ({ data: await resolveMessages() }),
      get: async ({ path: target }: { path: { id: string } }) => ({ data: { id: target.id } }),
      prompt: async () => ({ data: {} }),
    },
    tui: { showToast: async () => {} },
    app: { log: async () => {} },
  }
  return {
    client: client as unknown as PluginInput["client"],
    bind(resolve) {
      resolveMessages = resolve
    },
  }
}

function pluginInput(client: PluginInput["client"], directory: string): PluginInput {
  return {
    client,
    project: { id: "dcp-tool-e2e-project" },
    worktree: directory,
    directory,
    experimental_workspace: { register() {} },
    serverUrl: new URL("http://localhost:4096"),
  } as unknown as PluginInput
}

/** Load both real plugins through the real loader and the temp config. */
async function loadRealPlugins(
  config: TempPluginConfig,
  directory: string,
  client: PluginInput["client"],
): Promise<{ dcp: Hooks; workspace: Hooks }> {
  const parsed = JSON.parse(fs.readFileSync(config.file, "utf8")) as { plugin: string[] }
  const loaded = await PluginLoader.loadExternal({
    items: parsed.plugin.map((spec) => ({ spec, source: config.file, scope: "local" as const })),
    kind: "server",
  })
  if (loaded.length !== parsed.plugin.length) {
    throw new Error(`expected ${parsed.plugin.length} real plugins, loaded ${loaded.length}`)
  }
  const input = pluginInput(client, directory)
  const hooks = new Map<string, Hooks>()
  for (const row of loaded) {
    const value = row.mod.default
    const server = typeof value === "function" ? value : (value as { server?: unknown } | undefined)?.server
    if (typeof server !== "function") throw new Error(`plugin ${row.spec} has no server entrypoint`)
    const loadedHooks = await (server as (input: PluginInput) => Promise<Hooks>)(input)
    hooks.set(row.spec.includes("opencode-dcp") ? "dcp" : "workspace", loadedHooks)
  }
  const dcp = hooks.get("dcp")
  const workspace = hooks.get("workspace")
  if (dcp === undefined || workspace === undefined) throw new Error("real DCP/workspace plugin did not load")
  return { dcp, workspace }
}

/** Wrap real hooks as the hub `Plugin.Interface` production exposes. */
function asPluginInterface(hooks: Hooks): Plugin.Interface {
  return {
    trigger: ((name: string, input: unknown, output: unknown) =>
      Effect.promise(async () => {
        const hook = (hooks as Record<string, unknown>)[name]
        if (typeof hook === "function") {
          await (hook as (input: unknown, output: unknown) => Promise<void>)(input, output)
        }
        return output
      })) as Plugin.Interface["trigger"],
    list: () => Effect.succeed([hooks]),
    init: () => Effect.void,
  }
}

/** The real DCP `compress` descriptor the hub would ship to a worker. */
function compressDescriptorOf(hooks: Hooks): PluginToolDescriptor {
  const tool = hooks.tool?.compress
  if (tool === undefined) throw new Error("real DCP plugin did not expose the compress tool")
  return {
    id: "compress",
    description: tool.description,
    jsonSchema: pluginToolJsonSchema(tool),
  }
}

/** The hub-resident services `pluginToolAuthority` reads from its runner. */
const hubServices = Layer.mergeAll(
  Layer.mock(Permission.Service, { ask: () => Effect.void }),
  Layer.mock(Truncate.Service, {
    output: (text: string) => Effect.succeed({ content: text, truncated: false as const }),
  }),
  Layer.mock(Agent.Service, { get: () => Effect.succeed({ permission: [] } as unknown as Agent.Info) }),
  Layer.mock(Session.Service, { messages: () => Effect.succeed([]), get: () => Effect.succeed({ permission: [] } as unknown as Session.Info) }),
)

/** Run a hub authority effect with the hub-resident services provided. */
const hubRunner: HubProxyRunner = (effect) => Effect.runPromise(Effect.provide(effect, hubServices))

/** A served real hub-proxy pair plus the hub-side plugin-tool call log. */
interface HubProxyFixture {
  readonly client: ReturnType<typeof createHubProxyClient>
  /** Every `pluginTool.execute` request the hub authority received. */
  readonly pluginToolCalls: ReadonlyArray<PluginToolExecuteInput>
  dispose(): void
}

/**
 * Stand up the genuine boundary: a real `MessageChannel` with `serveHubProxy`
 * (hub) on port2 and `createHubProxyClient` (worker) on port1. The hub authority
 * map wires BOTH the plugin-tool execute authority and the messages-transform
 * authority to the SAME real DCP plugin instance, so they share `state`.
 */
function startHubProxy(plugin: Plugin.Interface): HubProxyFixture {
  const recorded = recordingHubAuthorities()
  const transform = messagesTransformAuthority({ plugin: () => Option.some(plugin), run: hubRunner })
  const execute = pluginToolAuthority({ plugin: () => Option.some(plugin), run: hubRunner })
  const pluginToolCalls: PluginToolExecuteInput[] = []

  const authorities: HubProxyAuthorities = {
    ...recorded.authorities,
    messagesTransform: (input, output) => transform(input, output),
    pluginTool: (input) => {
      pluginToolCalls.push(input)
      return execute(input)
    },
  }

  const channel = new MessageChannel()
  const disposeServer = serveHubProxy(buildHubProxyHandlers(authorities, hubRunner), channel.port2)
  const client = createHubProxyClient(channel.port1)
  return {
    client,
    pluginToolCalls,
    dispose() {
      disposeServer()
      client.dispose()
      channel.port1.close()
      channel.port2.close()
    },
  }
}

const silentClients = (): Parameters<typeof buildSessionWorkerLayer>[0]["clients"] => ({
  writer: { query: async () => [], publish: async () => ({ seq: 1, committed: true }) },
  egress: { emit: () => {} },
  backgroundJob: { wait: async () => null, cancel: async () => {} },
})

/** The full worker graph over the E2E worker database, with the real hub-proxy host. */
function workerLayer(
  snapshot: WorkerSnapshot,
  provider: MockProviderHandle,
  database: E2EContext["database"],
  writer: Parameters<typeof buildSessionWorkerLayer>[0]["clients"]["writer"],
  plugins: ReturnType<typeof createWorkerPluginHost>,
  pluginTools: NonNullable<Parameters<typeof buildSessionWorkerLayer>[0]["pluginTools"]>,
): MemoizedWorkerLayer {
  const config = modelConfig(provider)
  return memoizeWorkerLayer(
    buildSessionWorkerLayer({
      snapshot,
      env: { values: {} },
      clients: {
        writer,
        database,
        egress: { emit: () => {}, emitDurable: () => {} },
        backgroundJob: { wait: async () => null, cancel: async () => {} },
      },
      modelClient: llmModelClientLayer(config).pipe(Layer.provide(reducedLlmLayer(snapshot, config))),
      plugins,
      pluginTools,
      replacements: [...workerGraphReplacements(config), pluginPassThroughReplacement(plugins, pluginTools)],
    }).layer,
  )
}

/** A minimal worker graph (no provider) for the registry-level cross-port test. */
function minimalWorkerLayer(
  plugins: ReturnType<typeof createWorkerPluginHost>,
  pluginTools: NonNullable<Parameters<typeof buildSessionWorkerLayer>[0]["pluginTools"]>,
): Layer.Layer<unknown, unknown> {
  return buildSessionWorkerLayer({
    snapshot: workerSnapshot(),
    env: { values: {} },
    clients: silentClients(),
    modelClient: Layer.succeed(ModelClient, { complete: () => Effect.succeed({ text: "stub" }) }),
    plugins,
    pluginTools,
  }).layer
}

/** Resolve the worker's listed plugin hooks (what `ToolRegistry` reads). */
function listWorkerPlugins(layer: Layer.Layer<unknown, unknown>) {
  return Effect.runPromise(
    Effect.gen(function* () {
      const plugin = (yield* Plugin.Service) as Plugin.Interface
      return yield* plugin.list()
    }).pipe(Effect.provide(layer)),
  )
}

/** Run the worker's `experimental.chat.messages.transform` trigger (the hub proxy). */
function transformViaWorker(layer: Layer.Layer<unknown, unknown>, output: MessagesTransformOutput) {
  return Effect.runPromise(
    Effect.gen(function* () {
      const plugin = (yield* Plugin.Service) as Plugin.Interface
      return yield* plugin.trigger("experimental.chat.messages.transform", {}, output)
    }).pipe(Effect.provide(layer)),
  )
}

/** A hand-built message history with two duplicate tool calls (DCP keeps the newest). */
function duplicateToolHistory() {
  const base = { sessionID: "ses_dcp", agent: "build", model: { providerID: "mock", modelID: "test-model" } }
  const message = (id: string, role: string, parts: unknown[]) => ({
    info: { ...base, id, role, modelID: "test-model", providerID: "mock", time: { created: Number(id.slice(4)) } },
    parts,
  })
  const tool = (callID: string, output: string) => ({
    type: "tool",
    callID,
    tool: "bash",
    state: {
      status: "completed",
      input: { command: "echo duplicate" },
      output,
      title: "done",
      metadata: {},
      time: { start: 1, end: 1 },
    },
  })
  return [
    message("msg_1", "user", [{ type: "text", text: "first" }]),
    message("msg_2", "assistant", [tool("call-A", OLD_OUTPUT)]),
    message("msg_3", "user", [{ type: "text", text: "second" }]),
    message("msg_4", "assistant", [tool("call-B", NEW_OUTPUT)]),
    message("msg_5", "user", [{ type: "text", text: TURN_TEXT }]),
  ]
}

/** Append the compress tool result the real prompt turn persists before the next transform. */
function withCompressResult(messages: ReadonlyArray<unknown>, output: string) {
  const next = structuredClone(messages) as Array<Record<string, unknown>>
  next.push({
    info: {
      id: "msg_6",
      sessionID: "ses_dcp",
      role: "assistant",
      agent: "build",
      modelID: "test-model",
      providerID: "mock",
      time: { created: 6 },
    },
    parts: [
      {
        type: "tool",
        callID: "call-compress",
        tool: "compress",
        state: {
          status: "completed",
          input: {},
          output,
          title: "compressed",
          metadata: {},
          time: { start: 9, end: 9 },
        },
      },
    ],
  })
  return next
}

describe("DCP parity: a hub plugin tool and the next transform share hub state", () => {
  test("a remote tool call mutates hub state that the NEXT transform observes, across the real port", async () => {
    const config = tempPluginConfig()
    const messages = duplicateToolHistory()
    const dcpClient = sessionMessagesClient(messages)
    const { dcp, workspace } = await loadRealPlugins(config, "/tmp", dcpClient.client)
    const descriptor = compressDescriptorOf(dcp)
    const plugin = asPluginInterface(dcp)
    const proxy = startHubProxy(plugin)
    try {
      // The real workspace plugin loaded from the same config alongside DCP.
      expect(workspace.tool?.plan_save).toBeDefined()

      const executor = hubProxyPluginTool(proxy.client)
      if (executor === undefined) throw new Error("hub proxy client did not yield a plugin-tool executor")
      const bridge = hubProxyMessagesTransform(proxy.client)
      if (bridge === undefined) throw new Error("hub proxy client did not yield a messages-transform bridge")

      const layer = minimalWorkerLayer(
        createWorkerPluginHost(
          [{ name: "dcp", hooks: {}, proxied: ["experimental.chat.messages.transform"] }],
          { messagesTransform: bridge },
        ),
        { descriptors: [descriptor], executor },
      )

      // The worker's real `Plugin.Service.list()` exposes the hub-owned tool as a
      // remote stub, so the worker's registry would register `compress`.
      const hooks = await listWorkerPlugins(layer)
      const def = (hooks[0]?.tool as Record<string, unknown> | undefined)?.compress
      if (!isRemotePluginToolDefinition(def)) throw new Error("compress was not a remote plugin tool")

      // BEFORE: populate the real DCP tool cache (as the first provider request
      // would); no compression exists yet, so no sentinel/summary.
      const before = await transformViaWorker(layer, { messages: structuredClone(messages) })
      expect(JSON.stringify(before)).not.toContain(SENTINEL)

      // The call crosses the real control port; the REAL DCP `compress` tool runs
      // on the hub and writes the compression block.
      const result = await def.execute(
        {
          topic: "compress second turn",
          content: [{ startId: "m0003", endId: "m0004", summary: SUMMARY }],
        },
        { sessionID: "ses_dcp", messageID: "msg_2", callID: "call_dcp", agent: "build" },
      )
      expect(result.output).toContain("Compressed 2 messages")
      expect(proxy.pluginToolCalls).toHaveLength(1)
      expect(proxy.pluginToolCalls[0]?.toolID).toBe("compress")

      // AFTER: the same real DCP instance's transform sees the block and the
      // deduplication prune, exactly as the next provider request would.
      const after = await transformViaWorker(layer, { messages: withCompressResult(messages, result.output) })
      const serialized = JSON.stringify(after)
      expect(serialized).toContain(SENTINEL)
      expect(serialized).toContain(SUMMARY)
      expect(serialized).not.toContain(OLD_OUTPUT)
      expect(serialized).not.toContain(NEW_OUTPUT)
    } finally {
      proxy.dispose()
      config.dispose()
    }
  })

  test("a full prompt turn: the tool runs on the hub and the NEXT provider request is pruned", async () => {
    const provider = startMockProvider({
      scripts: [
        {
          toolCalls: [
            {
              id: "call-compress-e2e",
              name: "compress",
              arguments: JSON.stringify({
                topic: "compress second turn",
                content: [{ startId: "m0003", endId: "m0004", summary: SUMMARY }],
              }),
            },
          ],
        },
        { text: "done" },
      ],
    })
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "oc-pt5-dcp-"))
    const snapshot = workerSnapshot({ directory })
    const ctx = createE2EContext({ snapshot, modelConfig: modelConfig(provider) })
    const config = tempPluginConfig()
    const dcpClient = sessionMessagesClient()
    let proxy: HubProxyFixture | undefined
    let layer: MemoizedWorkerLayer | undefined
    try {
      const { dcp } = await loadRealPlugins(config, directory, dcpClient.client)
      const descriptor = compressDescriptorOf(dcp)
      const plugin = asPluginInterface(dcp)
      proxy = startHubProxy(plugin)
      const executor = hubProxyPluginTool(proxy.client)
      if (executor === undefined) throw new Error("hub proxy client did not yield a plugin-tool executor")
      const bridge = hubProxyMessagesTransform(proxy.client)
      if (bridge === undefined) throw new Error("hub proxy client did not yield a messages-transform bridge")

      layer = workerLayer(
        snapshot,
        provider,
        ctx.database,
        ctx.writer,
        createWorkerPluginHost(
          [{ name: "dcp", hooks: {}, proxied: ["experimental.chat.messages.transform"] }],
          { messagesTransform: bridge },
        ),
        { descriptors: [descriptor], executor },
      )
      const seeded = await Effect.runPromise(seed(layer.layer, ctx.instance, directory, provider.model))
      bindSessionClient(dcpClient, layer.layer, ctx.instance, seeded.sessionID)
      await Effect.runPromise(
        Effect.gen(function* () {
          const prompt = yield* SessionPrompt.Service
          yield* prompt.loop({ sessionID: seeded.sessionID })
        }).pipe(Effect.provide(layer.layer), Effect.provideService(InstanceRef, ctx.instance)),
      )

      // Wait on the provider's own observation, never a sleep: the turn issues
      // at least the tool-call request and the follow-up request.
      await provider.wait(2)

      // The tool actually crossed the port and ran on the hub.
      expect(proxy.pluginToolCalls.some((call) => call.toolID === "compress")).toBe(true)

      // The FIRST outbound request predates the compression; a LATER request must
      // carry the pruned sentinel and the compression summary (never the original
      // or the newer duplicate output).
      const first = JSON.stringify(provider.requests[0]?.body ?? {})
      expect(first).not.toContain(SENTINEL)
      const laterBodies = provider.requests.slice(1).map((request) => JSON.stringify(request.body))
      expect(laterBodies.some((body) => body.includes(SENTINEL))).toBe(true)
      expect(laterBodies.some((body) => body.includes(SUMMARY))).toBe(true)
      expect(laterBodies.some((body) => body.includes(OLD_OUTPUT))).toBe(false)
    } finally {
      await layer?.dispose()
      proxy?.dispose()
      config.dispose()
      await ctx.dispose()
      await provider.stop()
      fs.rmSync(directory, { recursive: true, force: true })
    }
  }, 60_000)
})

/**
 * Seed a session whose history contains two identical completed tool results
 * followed by a fresh user turn, so the real DCP prune and compression have
 * something to rewrite.
 */
function seed(
  layer: Layer.Layer<unknown, unknown>,
  instance: E2EContext["instance"],
  directory: string,
  modelID: string,
) {
  return Effect.gen(function* () {
    const sessions = yield* Session.Service
    const session = yield* sessions.create({
      title: "pt5-dcp",
      agent: "build",
      permission: [{ permission: "*", pattern: "*", action: "allow" }],
    })
    const first = (yield* sessions.updateMessage({
      id: MessageID.ascending(),
      role: "user",
      sessionID: session.id,
      agent: "build",
      model: { providerID: "mock", modelID },
      time: { created: Date.now() },
    } as never)) as { id: MessageID }
    yield* sessions.updatePart({
      id: PartID.ascending(),
      messageID: first.id,
      sessionID: session.id,
      type: "text",
      text: "first",
    } as never)
    const assistantA = (yield* sessions.updateMessage({
      id: MessageID.ascending(),
      role: "assistant",
      sessionID: session.id,
      parentID: first.id,
      agent: "build",
      mode: "build",
      modelID,
      providerID: "mock",
      path: { cwd: directory, root: directory },
      cost: 0,
      tokens: { output: 0, input: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      time: { created: Date.now() },
      finish: "end_turn",
    } as never)) as { id: MessageID }
    yield* sessions.updatePart({
      id: PartID.ascending(),
      messageID: assistantA.id,
      sessionID: session.id,
      type: "tool",
      callID: "call-fixed-1",
      tool: "bash",
      state: {
        status: "completed",
        input: { command: "echo duplicate" },
        output: OLD_OUTPUT,
        title: "done",
        metadata: {},
        time: { start: Date.now(), end: Date.now() },
      },
    } as never)
    const second = (yield* sessions.updateMessage({
      id: MessageID.ascending(),
      role: "user",
      sessionID: session.id,
      agent: "build",
      model: { providerID: "mock", modelID },
      time: { created: Date.now() },
    } as never)) as { id: MessageID }
    yield* sessions.updatePart({
      id: PartID.ascending(),
      messageID: second.id,
      sessionID: session.id,
      type: "text",
      text: "second",
    } as never)
    const assistantB = (yield* sessions.updateMessage({
      id: MessageID.ascending(),
      role: "assistant",
      sessionID: session.id,
      parentID: second.id,
      agent: "build",
      mode: "build",
      modelID,
      providerID: "mock",
      path: { cwd: directory, root: directory },
      cost: 0,
      tokens: { output: 0, input: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      time: { created: Date.now() },
      finish: "end_turn",
    } as never)) as { id: MessageID }
    yield* sessions.updatePart({
      id: PartID.ascending(),
      messageID: assistantB.id,
      sessionID: session.id,
      type: "tool",
      callID: "call-fixed-2",
      tool: "bash",
      state: {
        status: "completed",
        input: { command: "echo duplicate" },
        output: NEW_OUTPUT,
        title: "done",
        metadata: {},
        time: { start: Date.now(), end: Date.now() },
      },
    } as never)
    const turn = (yield* sessions.updateMessage({
      id: MessageID.ascending(),
      role: "user",
      sessionID: session.id,
      agent: "build",
      model: { providerID: "mock", modelID },
      time: { created: Date.now() },
    } as never)) as { id: MessageID }
    yield* sessions.updatePart({
      id: PartID.ascending(),
      messageID: turn.id,
      sessionID: session.id,
      type: "text",
      text: TURN_TEXT,
    } as never)
    return { sessionID: session.id as SessionID, messages: yield* sessions.messages({ sessionID: session.id }) }
  }).pipe(Effect.provide(layer), Effect.provideService(InstanceRef, instance))
}

/** Bind the real DCP client to the worker's durable session reads. */
function bindSessionClient(
  client: SessionMessagesClient,
  layer: Layer.Layer<unknown, unknown>,
  instance: E2EContext["instance"],
  sessionID: SessionID,
) {
  const provide = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.runPromise(
      effect.pipe(Effect.provide(layer), Effect.provideService(InstanceRef, instance)) as Effect.Effect<A, E>,
    )
  client.bind(() =>
    provide(
      Effect.gen(function* () {
        const sessions = yield* Session.Service
        return yield* sessions.messages({ sessionID })
      }),
    ),
  )
}
