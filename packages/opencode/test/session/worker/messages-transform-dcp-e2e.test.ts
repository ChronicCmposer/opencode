// DCP regression guard for `experimental.chat.messages.transform` under workers.
//
// The original bug: `@tarquinen/opencode-dcp` prunes old tool results via
// `experimental.chat.messages.transform`. With `OPENCODE_EXPERIMENTAL_SESSION_WORKERS=true`
// the hook is a hub proxy that returns a FRESH `{ messages }`; the call sites used
// to discard that return and rely on in-place mutation, so the pruning silently
// vanished before the provider request.
//
// This file encodes that bug against the REAL packages, not a stub:
//   * the real `@tarquinen/opencode-dcp` package is loaded through a temporary
//     opencode config (the same `PluginLoader` production uses);
//   * the real workspace plugin is loaded from the same config, proving both
//     plugins coexist;
//   * the real DCP `compress` tool writes its compression/prune state, and a
//     later real `experimental.chat.messages.transform` must deliver the
//     sentinel/summary through a full worker prompt turn and a compaction turn.
//
// The durable session keeps the original output, proving the sentinel came from
// the transform rather than the persisted history.

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
import { SessionCompaction } from "@/session/compaction"
import { InstanceRef } from "@/effect/instance-ref"
import { createWorkerPluginHost, type WorkerPluginHost } from "@/session/worker/plugin-host"
import type { WriterClientHandle } from "@/session/worker/layer"
import {
  buildSessionWorkerLayer,
  createHubProxyClient,
  llmModelClientLayer,
  reducedLlmLayer,
  serveHubProxy,
  type WorkerModelConfig,
  type WorkerSnapshot,
} from "@/session/worker/layer"
import { buildHubProxyHandlers, messagesTransformAuthority } from "@/session/worker/hub"
import { hubProxyMessagesTransform, workerGraphReplacements } from "@/session/worker/session-worker"
import { createE2EContext, memoizeWorkerLayer, type E2EContext, type MemoizedWorkerLayer } from "../../lib/e2e-context"
import { instantRunner, recordingHubAuthorities } from "../../lib/hub-proxy"
import { pluginPassThroughReplacement } from "../../lib/worker-plugin-routes"
import { startMockProvider, type MockProviderHandle } from "../../lib/mock-provider"
import { workerSnapshot } from "../../lib/snapshot"

/** The exact sentinel `@tarquinen/opencode-dcp` writes in place of pruned output. */
const SENTINEL = "[Output removed to save context - information superseded or no longer needed]"

/** The seeded old tool output the real DCP prune must replace. */
const OLD_OUTPUT = "TOOL_OUTPUT_ORIGINAL"

/** The newer duplicate tool output; DCP keeps the newest duplicate. */
const NEW_OUTPUT = "TOOL_OUTPUT_NEW"

/** The user text of the turn the loop runs; used to locate the outbound request. */
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

function sessionMessagesClient(): SessionMessagesClient {
  let resolveMessages: (() => Promise<ReadonlyArray<unknown>>) | undefined
  const client = {
    session: {
      messages: async () => ({ data: resolveMessages === undefined ? [] : await resolveMessages() }),
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
    project: { id: "dcp-e2e-project" },
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

/** The full worker graph over the E2E worker database, with the real hub-proxy host. */
function workerLayer(
  snapshot: WorkerSnapshot,
  provider: MockProviderHandle,
  database: E2EContext["database"],
  writer: WriterClientHandle,
  plugins: WorkerPluginHost,
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
      replacements: [...workerGraphReplacements(config), pluginPassThroughReplacement(plugins)],
    }).layer,
  )
}

/**
 * Seed a session whose history contains two identical completed tool results
 * (DCP's deduplication keeps the newest) followed by a fresh user turn, so the
 * real plugin has an old result to prune and a later range to compress.
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
      title: "b1-dcp",
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
    const messages = yield* sessions.messages({ sessionID: session.id })
    return { sessionID: session.id as SessionID, messages }
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

/**
 * Run the real DCP `compress` tool directly on the hub, then return the state it
 * wrote. The transform is the subject of this file; the worker-tool path is
 * covered by `plugin-tool-dcp-e2e.test.ts`.
 */
async function compressDirectly(
  hooks: Hooks,
  sessionID: SessionID,
  messages: ReadonlyArray<unknown>,
  compressMessageID: string,
) {
  const tool = hooks.tool?.compress
  if (tool === undefined) throw new Error("real DCP plugin did not expose the compress tool")
  const ctx = {
    sessionID,
    messageID: compressMessageID,
    callID: "call-direct-compress",
    agent: "build",
    directory: "/tmp",
    worktree: "/tmp",
    abort: new AbortController().signal,
    messages,
    metadata: () => {},
    ask: async () => {},
  }
  const result = await tool.execute(
    {
      topic: "compress second turn",
      content: [{ startId: "m0003", endId: "m0004", summary: SUMMARY }],
    } as never,
    ctx as never,
  )
  return typeof result === "string" ? result : result.output
}

/** A served hub-proxy pair plus its disposer. */
interface HubProxyFixture {
  readonly client: ReturnType<typeof createHubProxyClient>
  dispose(): void
}

/** Stand up the genuine boundary with the real DCP plugin as the hub authority. */
function startRealHubProxy(plugin: Plugin.Interface): HubProxyFixture {
  const channel = new MessageChannel()
  const { authorities } = recordingHubAuthorities({
    messagesTransform: messagesTransformAuthority({ plugin: () => Option.some(plugin), run: instantRunner }),
  })
  const disposeServer = serveHubProxy(buildHubProxyHandlers(authorities, instantRunner), channel.port2)
  const client = createHubProxyClient(channel.port1)
  return {
    client,
    dispose() {
      disposeServer()
      client.dispose()
      channel.port1.close()
      channel.port2.close()
    },
  }
}

/** The outbound request bodies that belong to the seeded turn. */
function turnBodies(provider: MockProviderHandle): ReadonlyArray<Record<string, unknown>> {
  return provider.requests
    .map((request) => request.body)
    .filter((body) => JSON.stringify(body).includes(TURN_TEXT))
}

/** A temp directory shared by the harness and the seeded session's `path`. */
async function withDirectory(run: (snapshot: WorkerSnapshot) => Promise<void>): Promise<void> {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "oc-b1-dcp-"))
  try {
    await run(workerSnapshot({ directory }))
  } finally {
    fs.rmSync(directory, { recursive: true, force: true })
  }
}

describe("real DCP transform survives a worker prompt turn", () => {
  test("the real plugin's pruned result reaches the provider request (not the original output)", async () => {
    await withDirectory(async (snapshot) => {
      const provider = startMockProvider({ chunks: ["ok"] })
      const ctx = createE2EContext({ snapshot, modelConfig: modelConfig(provider) })
      const config = tempPluginConfig()
      const dcpClient = sessionMessagesClient()
      let proxy: HubProxyFixture | undefined
      let worker: MemoizedWorkerLayer | undefined
      try {
        const { dcp, workspace } = await loadRealPlugins(config, snapshot.directory, dcpClient.client)
        // The real workspace plugin must load from the same temp config and run.
        const system = { system: [] as string[] }
        await workspace["experimental.chat.system.transform"]?.({ agent: "build" } as never, system)
        expect(system.system.join("\n")).toContain("<date-awareness>")
        expect(workspace.tool?.plan_save).toBeDefined()

        const plugin = asPluginInterface(dcp)
        proxy = startRealHubProxy(plugin)
        const bridge = hubProxyMessagesTransform(proxy.client)
        if (bridge === undefined) throw new Error("hub proxy client did not yield a messages transform bridge")
        worker = workerLayer(
          snapshot,
          provider,
          ctx.database,
          ctx.writer,
          createWorkerPluginHost(
            [{ name: "dcp", hooks: {}, proxied: ["experimental.chat.messages.transform"] }],
            { messagesTransform: bridge },
          ),
        )
        const seeded = await Effect.runPromise(seed(worker.layer, ctx.instance, snapshot.directory, provider.model))
        bindSessionClient(dcpClient, worker.layer, ctx.instance, seeded.sessionID)

        // The first transform populates DCP's tool cache (as the first provider
        // request would), then the real compress tool records the block.
        await dcp["experimental.chat.messages.transform"]?.(
          {} as never,
          { messages: structuredClone(seeded.messages) } as never,
        )
        const assistantA = seeded.messages[1]
        if (assistantA === undefined) throw new Error("seed produced no first assistant message")
        const compressed = await compressDirectly(
          dcp,
          seeded.sessionID,
          structuredClone(seeded.messages),
          assistantA.info.id,
        )
        expect(compressed).toContain("Compressed 2 messages")

        const persisted = await Effect.runPromise(
          Effect.gen(function* () {
            const prompt = yield* SessionPrompt.Service
            yield* prompt.loop({ sessionID: seeded.sessionID })
            const sessions = yield* Session.Service
            return yield* sessions.messages({ sessionID: seeded.sessionID })
          }).pipe(Effect.provide(worker.layer), Effect.provideService(InstanceRef, ctx.instance)),
        )

        const bodies = turnBodies(provider)
        expect(bodies.length).toBeGreaterThanOrEqual(1)
        const serialized = JSON.stringify(bodies)
        expect(serialized).toContain(SENTINEL)
        expect(serialized).toContain(SUMMARY)
        expect(serialized).not.toContain(OLD_OUTPUT)
        // The transform only rewrites the outbound request; the durable history
        // keeps the real tool output.
        expect(JSON.stringify(persisted)).toContain(OLD_OUTPUT)
      } finally {
        await worker?.dispose()
        proxy?.dispose()
        config.dispose()
        await ctx.dispose()
        await provider.stop()
      }
    })
  }, 60_000)

  test("the real plugin's pruned result reaches the compaction conversation", async () => {
    await withDirectory(async (snapshot) => {
      const provider = startMockProvider({ chunks: ["summary"] })
      const ctx = createE2EContext({ snapshot, modelConfig: modelConfig(provider) })
      const config = tempPluginConfig()
      const dcpClient = sessionMessagesClient()
      let proxy: HubProxyFixture | undefined
      let worker: MemoizedWorkerLayer | undefined
      try {
        const { dcp } = await loadRealPlugins(config, snapshot.directory, dcpClient.client)
        const plugin = asPluginInterface(dcp)
        proxy = startRealHubProxy(plugin)
        const bridge = hubProxyMessagesTransform(proxy.client)
        if (bridge === undefined) throw new Error("hub proxy client did not yield a messages transform bridge")
        worker = workerLayer(
          snapshot,
          provider,
          ctx.database,
          ctx.writer,
          createWorkerPluginHost(
            [{ name: "dcp", hooks: {}, proxied: ["experimental.chat.messages.transform"] }],
            { messagesTransform: bridge },
          ),
        )
        const seeded = await Effect.runPromise(seed(worker.layer, ctx.instance, snapshot.directory, provider.model))
        bindSessionClient(dcpClient, worker.layer, ctx.instance, seeded.sessionID)

        await dcp["experimental.chat.messages.transform"]?.(
          {} as never,
          { messages: structuredClone(seeded.messages) } as never,
        )
        const assistantA = seeded.messages[1]
        if (assistantA === undefined) throw new Error("seed produced no first assistant message")
        await compressDirectly(dcp, seeded.sessionID, structuredClone(seeded.messages), assistantA.info.id)

        await Effect.runPromise(
          Effect.gen(function* () {
            const sessions = yield* Session.Service
            const compaction = yield* SessionCompaction.Service
            const messages = yield* sessions.messages({ sessionID: seeded.sessionID })
            const parent = messages.findLast((message) => message.info.role === "user")
            if (parent === undefined) throw new Error("seed produced no user message")
            yield* compaction.process({ parentID: parent.info.id, messages, sessionID: seeded.sessionID, auto: false })
          }).pipe(Effect.provide(worker.layer), Effect.provideService(InstanceRef, ctx.instance)),
        )

        const bodies = turnBodies(provider)
        expect(bodies.length).toBeGreaterThanOrEqual(1)
        const serialized = JSON.stringify(bodies)
        expect(serialized).toContain(`[Tool result]: ${SENTINEL}`)
        expect(serialized).not.toContain(OLD_OUTPUT)
      } finally {
        await worker?.dispose()
        proxy?.dispose()
        config.dispose()
        await ctx.dispose()
        await provider.stop()
      }
    })
  }, 60_000)
})
