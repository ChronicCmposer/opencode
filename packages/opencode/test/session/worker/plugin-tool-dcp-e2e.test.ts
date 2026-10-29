// DCP parity end-to-end: a plugin tool executes on the hub, and the
// `experimental.chat.messages.transform` that runs NEXT (also on the hub) sees
// the mutation.
//
// `@tarquinen/opencode-dcp` coordinates its `compress` tool and its
// `experimental.chat.messages.transform` hook through ONE in-memory `state`
// object: the tool writes the
// compression blocks, the transform reads them to rewrite the outbound
// messages. Under session workers plugins live on the hub, so BOTH sides must
// run there. The worker only ships clone-safe descriptors and forwards each tool
// call back over the control port (`plugin-tools-design.md` §0/§8).
//
// This file proves that end to end against the REAL boundary:
//   * a real `MessageChannel` with `serveHubProxy` (hub) + `createHubProxyClient`
//     (worker);
//   * a hub-resident stub plugin whose `tool.compress.execute` mutates shared
//     state and whose `messages.transform` reads it;
//   * the worker built with `pluginTools: { descriptors, executor }`, so the real
//     `ToolRegistry` registers the remote stub;
//   * invoking the tool through the worker crosses the port, mutates the HUB
//     state, and changes the NEXT transform's output.
//
// Two tests: the smallest real version (registry-level, no provider) and the
// full prompt turn whose SECOND outbound provider request reflects the mutation.

import { describe, expect, test } from "bun:test"
import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { Effect, Layer, Option } from "effect"
import { Plugin } from "@/plugin"
import { MessageID, PartID, type SessionID } from "@/session/schema"
import { Session } from "@/session/session"
import { SessionPrompt } from "@/session/prompt"
import { InstanceRef } from "@/effect/instance-ref"
import { Permission } from "@/permission"
import { Agent } from "@/agent/agent"
import { Truncate } from "@/tool/truncate"
import { ModelClient } from "@/session/worker/layer"
import {
  buildHubProxyHandlers,
  messagesTransformAuthority,
  pluginToolAuthority,
  type HubProxyAuthorities,
  type HubProxyRunner,
} from "@/session/worker/hub"
import {
  createWorkerPluginHost,
  type MessagesTransformOutput,
} from "@/session/worker/plugin-host"
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

const hubTransformPlugin = { name: "hub-transform", hooks: {}, proxied: ["experimental.chat.messages.transform"] }
import { workerSnapshot } from "../../lib/snapshot"

/** The exact sentinel `@tarquinen/opencode-dcp` writes in place of pruned output. */
const SENTINEL = "[Output removed to save context - information superseded or no longer needed]"

/** The old completed tool output the stub transform must prune once compressed. */
const OLD_OUTPUT = "TOOL_OUTPUT_ORIGINAL"

/** The user text of the seeded turn; used to locate the outbound request. */
const TURN_TEXT = "second"

/** The hub-resident plugin state the tool writes and the transform reads. */
interface HubState {
  readonly compressed: boolean
}

type ToolStateLike = { status?: string; output?: string; error?: string }
type PartLike = { type?: string; state?: ToolStateLike }
type MessageLike = { parts?: PartLike[] }

/**
 * The DCP-shaped prune: clear completed tool-result content outside the most
 * recent message. Mutates the list in place (the hub handler clones on reply).
 */
function pruneOldToolResults(messages: MessageLike[]): void {
  const keepFrom = Math.max(0, messages.length - 1)
  for (let index = 0; index < keepFrom; index++) {
    for (const part of messages[index].parts ?? []) {
      if (part.type !== "tool") continue
      if (part.state?.status === "completed") part.state.output = SENTINEL
      else if (part.state?.status === "error") part.state.error = SENTINEL
    }
  }
}

/**
 * The stub DCP plugin, hosted on the hub. Its `list()` exposes a `compress` tool
 * whose `execute` flips the shared state; its `trigger` prunes the outbound
 * messages when that state is set. Both close over the SAME `state` object, which
 * is the whole point: they must run in the same isolate.
 */
function makeHubPlugin(state: HubState): Plugin.Interface {
  const plugin = {
    trigger: (name: string, _input: unknown, output: unknown) => {
      if (name === "experimental.chat.messages.transform" && state.compressed) {
        pruneOldToolResults((output as { messages: MessageLike[] }).messages)
      }
      return Effect.succeed(output)
    },
    list: () =>
      Effect.succeed([
        {
          tool: {
            compress: {
              description: "hub compress",
              args: {},
              execute: async () => {
                // `HubState` is readonly; the hub owns the mutation and it is
                // exactly what the next transform observes.
                ;(state as { compressed: boolean }).compressed = true
                return { title: "compressed", output: "HUB_COMPRESSED", metadata: { ranOnHub: true } }
              },
            },
          },
        },
      ] as never),
    init: () => Effect.void,
  }
  return plugin as unknown as Plugin.Interface
}

const compressDescriptor: PluginToolDescriptor = {
  id: "compress",
  description: "hub compress",
  jsonSchema: { type: "object", properties: {}, additionalProperties: false },
}

/** The hub-resident services `pluginToolAuthority` reads from its runner. */
const hubServices = Layer.mergeAll(
  Layer.mock(Permission.Service, { ask: () => Effect.void }),
  Layer.mock(Truncate.Service, {
    output: (text: string) => Effect.succeed({ content: text, truncated: false as const }),
  }),
  Layer.mock(Agent.Service, { get: () => Effect.succeed({ permission: [] } as unknown as Agent.Info) }),
  Layer.mock(Session.Service, { get: () => Effect.succeed({ permission: [] } as unknown as Session.Info) }),
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
 * authority to the SAME stub plugin instance, so they share `state`.
 */
function startHubProxy(state: HubState): HubProxyFixture {
  const plugin = makeHubPlugin(state)
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

/**
 * A history whose OLD completed tool result is followed by a newer message, so
 * the DCP-shaped prune (everything before the last message) has something to
 * clear.
 */
function historyWithOldOutput(): MessagesTransformOutput {
  return {
    messages: [
      {
        parts: [
          {
            type: "tool",
            state: { status: "completed", output: OLD_OUTPUT },
          },
        ],
      },
      { parts: [{ type: "text", text: "newer" }] },
    ],
  }
}

describe("DCP parity: a hub plugin tool and the next transform share hub state", () => {
  test("a remote tool call mutates hub state that the NEXT transform observes, across the real port", async () => {
    const state: HubState = { compressed: false }
    const proxy = startHubProxy(state)
    try {
      const executor = hubProxyPluginTool(proxy.client)
      if (executor === undefined) throw new Error("hub proxy client did not yield a plugin-tool executor")
      const bridge = hubProxyMessagesTransform(proxy.client)
      if (bridge === undefined) throw new Error("hub proxy client did not yield a messages-transform bridge")

      const layer = minimalWorkerLayer(createWorkerPluginHost([hubTransformPlugin], { messagesTransform: bridge }), {
        descriptors: [compressDescriptor],
        executor,
      })

      // The worker's real `Plugin.Service.list()` exposes the hub-owned tool as a
      // remote stub, so the worker's registry would register `compress`.
      const hooks = await listWorkerPlugins(layer)
      const def = (hooks[0]?.tool as Record<string, unknown> | undefined)?.compress
      if (!isRemotePluginToolDefinition(def)) throw new Error("compress was not a remote plugin tool")

      // BEFORE: the hub state is untouched, so the transform is a no-op.
      const before = await transformViaWorker(layer, historyWithOldOutput())
      expect(JSON.stringify(before)).not.toContain(SENTINEL)

      // The call crosses the real control port; the HUB plugin's execute runs and
      // flips the hub state.
      const result = await def.execute(
        {},
        { sessionID: "ses_dcp", messageID: "msg_dcp", callID: "call_dcp", agent: "build" },
      )
      expect(result.output).toBe("HUB_COMPRESSED")
      expect(proxy.pluginToolCalls).toHaveLength(1)
      expect(proxy.pluginToolCalls[0]?.toolID).toBe("compress")
      expect(state.compressed).toBe(true)

      // AFTER: the same hub plugin instance's transform sees the mutation.
      const after = await transformViaWorker(layer, historyWithOldOutput())
      expect(JSON.stringify(after)).toContain(SENTINEL)
      expect(JSON.stringify(after)).not.toContain(OLD_OUTPUT)
    } finally {
      proxy.dispose()
    }
  })

  test("a full prompt turn: the tool runs on the hub and the NEXT provider request is pruned", async () => {
    const state: HubState = { compressed: false }
    const provider = startMockProvider({
      scripts: [
        { toolCalls: [{ id: "call-compress-e2e", name: "compress", arguments: "{}" }] },
        { text: "done" },
      ],
    })
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "oc-pt5-dcp-"))
    const snapshot = workerSnapshot({ directory })
    const ctx = createE2EContext({ snapshot, modelConfig: modelConfig(provider) })
    const proxy = startHubProxy(state)
    let layer: MemoizedWorkerLayer | undefined
    try {
      const executor = hubProxyPluginTool(proxy.client)
      if (executor === undefined) throw new Error("hub proxy client did not yield a plugin-tool executor")
      const bridge = hubProxyMessagesTransform(proxy.client)
      if (bridge === undefined) throw new Error("hub proxy client did not yield a messages-transform bridge")

      layer = workerLayer(
        snapshot,
        provider,
        ctx.database,
        ctx.writer,
        createWorkerPluginHost([hubTransformPlugin], { messagesTransform: bridge }),
        { descriptors: [compressDescriptor], executor },
      )
      const sessionID = await Effect.runPromise(seed(layer.layer, ctx.instance, directory, provider.model))
      await Effect.runPromise(
        Effect.gen(function* () {
          const prompt = yield* SessionPrompt.Service
          yield* prompt.loop({ sessionID })
        }).pipe(Effect.provide(layer.layer), Effect.provideService(InstanceRef, ctx.instance)),
      )

      // Wait on the provider's own observation, never a sleep: the turn issues at
      // least the tool-call request and the follow-up request.
      await provider.wait(2)

      // The tool actually crossed the port and ran on the hub.
      expect(proxy.pluginToolCalls.some((call) => call.toolID === "compress")).toBe(true)
      expect(state.compressed).toBe(true)

      // The FIRST outbound request predates the compression; a LATER request must
      // carry the pruned sentinel (never the original output).
      const first = JSON.stringify(provider.requests[0]?.body ?? {})
      expect(first).not.toContain(SENTINEL)
      const laterBodies = provider.requests.slice(1).map((request) => JSON.stringify(request.body))
      expect(laterBodies.some((body) => body.includes(SENTINEL))).toBe(true)
    } finally {
      await layer?.dispose()
      proxy.dispose()
      await ctx.dispose()
      await provider.stop()
      fs.rmSync(directory, { recursive: true, force: true })
    }
  }, 60_000)
})

/**
 * Seed a session whose history contains one completed tool result followed by a
 * fresh user turn, so the DCP-shaped prune has an "old" result to clear.
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
    const user = (yield* sessions.updateMessage({
      id: MessageID.ascending(),
      role: "user",
      sessionID: session.id,
      agent: "build",
      model: { providerID: "mock", modelID },
      time: { created: Date.now() },
    } as never)) as { id: MessageID }
    yield* sessions.updatePart({
      id: PartID.ascending(),
      messageID: user.id,
      sessionID: session.id,
      type: "text",
      text: "first",
    } as never)
    const assistant = (yield* sessions.updateMessage({
      id: MessageID.ascending(),
      role: "assistant",
      sessionID: session.id,
      parentID: user.id,
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
      messageID: assistant.id,
      sessionID: session.id,
      type: "tool",
      callID: "call-fixed-1",
      tool: "bash",
      state: {
        status: "completed",
        input: {},
        output: OLD_OUTPUT,
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
    return session.id as SessionID
  }).pipe(Effect.provide(layer), Effect.provideService(InstanceRef, instance))
}
