import { afterEach, describe, expect } from "bun:test"
import path from "path"
import fs from "fs/promises"
import { fileURLToPath, pathToFileURL } from "url"
import { Cause, Effect, Exit, Fiber, Layer, Result, Schema } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { ToolRegistry, pluginToolJsonSchema } from "@/tool/registry"
import { Tool } from "@/tool/tool"
import { disposeAllInstances, TestInstance } from "../fixture/fixture"
import { pollWithTimeout, testEffect } from "../lib/effect"
import { TestConfig } from "../fixture/config"
import { Config } from "@/config/config"
import { Plugin } from "@/plugin"
import { Agent } from "@/agent/agent"
import { InstanceState } from "@/effect/instance-state"

import { ToolJsonSchema } from "@/tool/json-schema"
import { ToolRoute } from "@/tool/route"
import { MessageID, SessionID } from "@/session/schema"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import { MCP } from "@/mcp"
import type { Tool as MCPToolDef } from "@modelcontextprotocol/sdk/types.js"
import type { Hooks } from "@opencode-ai/plugin"
import {
  remotePluginToolHooks,
  type PluginToolDescriptor,
  type PluginToolExecuteInput,
  type PluginToolExecuteResult,
  type PluginToolExecutor,
  type RemotePluginToolContext,
} from "@/session/worker/plugin-tool-wire"
import { PluginToolIDCollisionError, createHubProxyClient, serveHubProxy } from "@/session/worker/layer"
import { buildHubProxyHandlers, type HubProxyAuthorities } from "@/session/worker/hub"
import { hubProxyPluginTool, hubProxyPluginToolAbort } from "@/session/worker/session-worker"
import { HubProxyRemoteError } from "@/session/worker/hub-proxy-wire"

const configLayer = TestConfig.layer({
  directories: () => InstanceState.directory.pipe(Effect.map((dir) => [path.join(dir, ".opencode")])),
})

// Fake Plugin.Service that returns a single plugin whose `tool` map contains
// one definition with `args: undefined`. Used to exercise the plugin entry
// point of `fromPlugin` for the #27451 / #27630 regression.
const brokenDef = {
  description: "plugin tool with missing args",
  args: undefined as unknown as Record<string, never>,
  execute: async () => "ok",
}
const brokenPluginLayer = Layer.succeed(
  Plugin.Service,
  Plugin.Service.of({
    init: () => Effect.void,
    trigger: ((_name: unknown, _input: unknown, output: unknown) =>
      Effect.succeed(output)) as Plugin.Interface["trigger"],
    list: () => Effect.succeed([{ tool: { broken_plugin_tool: brokenDef } }]),
  }),
)

// A fake hub-proxied plugin whose `Plugin.Service.list()` exposes a
// synthetic `Hooks` built by `remotePluginToolHooks` — exactly the stub the
// session worker ships. The worker `ToolRegistry` must register the id and route
// its execution through the executor (never a local `fromPlugin` wrapper).
const remoteToolCalls: PluginToolExecuteInput[] = []
const remoteToolResult: PluginToolExecuteResult = {
  title: "remote title",
  output: "hub output",
  metadata: { ranOnHub: true },
}
const remoteExecutor: PluginToolExecutor = {
  execute: async (input) => {
    remoteToolCalls.push(input)
    return remoteToolResult
  },
}
const remoteDescriptors: readonly PluginToolDescriptor[] = [
  {
    id: "remote_compress",
    description: "remote compress tool",
    jsonSchema: { type: "object", properties: { reason: { type: "string" } }, required: ["reason"] },
  },
]
const remotePluginLayer = Layer.succeed(
  Plugin.Service,
  Plugin.Service.of({
    init: () => Effect.void,
    trigger: ((_name: unknown, _input: unknown, output: unknown) =>
      Effect.succeed(output)) as Plugin.Interface["trigger"],
    list: () => Effect.succeed([remotePluginToolHooks(remoteDescriptors, remoteExecutor) as unknown as Hooks]),
  }),
)

// L2 F2: a hand-built remote definition that emits progress mid-execute. It is
// built directly (not via `remotePluginToolHooks`) so the optional third
// `onProgress` argument is observable by the test.
const progressMetadata: Array<{ title?: string; metadata?: unknown }> = []
const progressDef = {
  description: "remote progress tool",
  jsonSchema: { type: "object", properties: {} },
  execute: async (
    _args: unknown,
    _ctx: RemotePluginToolContext,
    onProgress?: (progress: { title?: string; metadata?: unknown }) => unknown,
  ): Promise<PluginToolExecuteResult> => {
    await onProgress?.({ title: "working", metadata: { pct: 42 } })
    await onProgress?.({ title: "almost", metadata: { pct: 90 } })
    return { title: "done", output: "FINISHED", metadata: { final: true } }
  },
}
const progressPluginLayer = Layer.succeed(
  Plugin.Service,
  Plugin.Service.of({
    init: () => Effect.void,
    trigger: ((_name: unknown, _input: unknown, output: unknown) =>
      Effect.succeed(output)) as Plugin.Interface["trigger"],
    list: () => Effect.succeed([{ tool: { remote_progress: progressDef } } as unknown as Hooks]),
  }),
)

// L2 F1: a remote definition exposing an abort bridge. `execute` stays pending
// until the test releases it, so the abort subscription can be observed and the
// listener-removal finalizer exercised.
const abortBridgeCalls: RemotePluginToolContext[] = []
let notifyAbortExecuteStarted: (() => void) | undefined
let releaseAbortExecute: ((result: PluginToolExecuteResult) => void) | undefined
const abortDef = {
  description: "remote abort tool",
  jsonSchema: { type: "object", properties: {} },
  execute: (_args: unknown, _ctx: RemotePluginToolContext): Promise<PluginToolExecuteResult> =>
    new Promise((resolve) => {
      notifyAbortExecuteStarted?.()
      releaseAbortExecute = resolve
    }),
  abort: (ctx: RemotePluginToolContext) => {
    abortBridgeCalls.push(ctx)
  },
}
const abortPluginLayer = Layer.succeed(
  Plugin.Service,
  Plugin.Service.of({
    init: () => Effect.void,
    trigger: ((_name: unknown, _input: unknown, output: unknown) =>
      Effect.succeed(output)) as Plugin.Interface["trigger"],
    list: () => Effect.succeed([{ tool: { remote_abort: abortDef } } as unknown as Hooks]),
  }),
)

// L2 F7: a remote descriptor whose id shadows the built-in `read` tool.
const collisionDescriptors: readonly PluginToolDescriptor[] = [
  { id: "read", description: "shadows the built-in read", jsonSchema: { type: "object", properties: {} } },
]
const collisionPluginLayer = Layer.succeed(
  Plugin.Service,
  Plugin.Service.of({
    init: () => Effect.void,
    trigger: ((_name: unknown, _input: unknown, output: unknown) =>
      Effect.succeed(output)) as Plugin.Interface["trigger"],
    list: () => Effect.succeed([remotePluginToolHooks(collisionDescriptors, remoteExecutor) as unknown as Hooks]),
  }),
)

// INT F1: the FULL abort path. The remote definition's abort bridge is the REAL
// hub-proxy client (`remotePluginToolHooks` adapts it), so aborting the local
// `toolCtx` crosses a MessageChannel to the hub's `pluginTool.abort` authority —
// not a hand-built bridge. `execute` stays pending until the test releases it.
const hubAbortCalls: { sessionID: string; callID: string }[] = []
let releaseHubExecute: ((result: PluginToolExecuteResult) => void) | undefined
let notifyHubExecuteStarted: (() => void) | undefined
const hubProxyDescriptors: readonly PluginToolDescriptor[] = [
  { id: "remote_hub_abort", description: "hub proxy abort tool", jsonSchema: { type: "object", properties: {} } },
]
const hubProxyClient = (() => {
  const channel = new MessageChannel()
  const map: HubProxyAuthorities = {
    sessionStatus: {} as never,
    permission: {} as never,
    question: {} as never,
    pluginTool: () =>
      new Promise<PluginToolExecuteResult>((resolve) => {
        notifyHubExecuteStarted?.()
        releaseHubExecute = resolve
      }),
    pluginToolAbort: (input) => {
      hubAbortCalls.push(input)
    },
  }
  serveHubProxy(
    buildHubProxyHandlers(map, (effect) => Promise.resolve(effect as never)),
    channel.port2,
  )
  return createHubProxyClient(channel.port1)
})()
const hubProxyPluginLayer = Layer.succeed(
  Plugin.Service,
  Plugin.Service.of({
    init: () => Effect.void,
    trigger: ((_name: unknown, _input: unknown, output: unknown) =>
      Effect.succeed(output)) as Plugin.Interface["trigger"],
    list: () =>
      Effect.succeed([
        remotePluginToolHooks(hubProxyDescriptors, hubProxyPluginTool(hubProxyClient)!, {
          abort: hubProxyPluginToolAbort(hubProxyClient),
        }) as unknown as Hooks,
      ]),
  }),
)

// INT F3: a stale descriptor whose execute reports the hub's
// `UnknownPluginToolError`. `refresh` re-fetches the hub's (mutated) descriptor
// set; the stub must additionally invalidate the cached registry state so the
// next build re-registers the fresh descriptor.
const staleDescriptor: PluginToolDescriptor = {
  id: "stale_plugin_tool",
  description: "stale descriptor",
  jsonSchema: { type: "object", properties: {} },
}
const freshDescriptor: PluginToolDescriptor = {
  id: "fresh_plugin_tool",
  description: "fresh descriptor",
  jsonSchema: { type: "object", properties: {} },
}
let healDescriptors: readonly PluginToolDescriptor[] = [staleDescriptor]
const refreshCalls: string[] = []
const staleExecutor: PluginToolExecutor = {
  execute: async () => {
    throw new HubProxyRemoteError('unknown plugin tool "stale_plugin_tool"', { tag: "UnknownPluginToolError" })
  },
}
const healPluginLayer = Layer.succeed(
  Plugin.Service,
  Plugin.Service.of({
    init: () => Effect.void,
    trigger: ((_name: unknown, _input: unknown, output: unknown) =>
      Effect.succeed(output)) as Plugin.Interface["trigger"],
    list: () =>
      Effect.succeed([
        remotePluginToolHooks(healDescriptors, staleExecutor, {
          refresh: () => {
            refreshCalls.push("refresh")
            healDescriptors = [freshDescriptor]
          },
        }) as unknown as Hooks,
      ]),
  }),
)

// Build a `Tool.Context` with sane defaults; tests override the fields they probe.
const makeToolContext = (agent: string, overrides: Partial<Tool.Context> = {}): Tool.Context => ({
  sessionID: SessionID.make("ses_remote"),
  messageID: MessageID.make("msg_remote"),
  callID: "call_remote",
  agent,
  abort: new AbortController().signal,
  messages: [],
  metadata: () => Effect.void,
  ask: () => Effect.void,
  ...overrides,
})

const root = LayerNode.group([ToolRegistry.node, Agent.node])
const replacements = [
  [Config.node, configLayer],
  [RuntimeFlags.node, RuntimeFlags.layer()],
] as const

const it = testEffect(LayerNode.compile(root, replacements))
const withCodeMode = testEffect(
  LayerNode.compile(root, [
    [Config.node, configLayer],
    [RuntimeFlags.node, RuntimeFlags.layer({ experimentalCodeMode: true })],
    [
      MCP.node,
      Layer.mock(MCP.Service, {
        tools: () =>
          Effect.succeed({
            weather_current: {
              def: {
                name: "current",
                description: "current weather",
                inputSchema: { type: "object", properties: { city: { type: "string" } }, required: ["city"] },
              } as MCPToolDef,
              client: {} as MCP.McpTool["client"],
            },
          }),
        clients: () => Effect.succeed({ weather: {} as any }),
      }),
    ],
  ]),
)
const withEmptyCodeMode = testEffect(
  LayerNode.compile(root, [
    [Config.node, configLayer],
    [RuntimeFlags.node, RuntimeFlags.layer({ experimentalCodeMode: true })],
    [
      MCP.node,
      Layer.mock(MCP.Service, {
        tools: () => Effect.succeed({}),
        clients: () => Effect.succeed({}),
      }),
    ],
  ]),
)
const withBrokenPlugin = testEffect(LayerNode.compile(root, [...replacements, [Plugin.node, brokenPluginLayer]]))
const withRemotePlugin = testEffect(LayerNode.compile(root, [...replacements, [Plugin.node, remotePluginLayer]]))
const withRemoteProgress = testEffect(LayerNode.compile(root, [...replacements, [Plugin.node, progressPluginLayer]]))
const withRemoteAbort = testEffect(LayerNode.compile(root, [...replacements, [Plugin.node, abortPluginLayer]]))
const withRemoteCollision = testEffect(LayerNode.compile(root, [...replacements, [Plugin.node, collisionPluginLayer]]))
const withRemoteHubAbort = testEffect(LayerNode.compile(root, [...replacements, [Plugin.node, hubProxyPluginLayer]]))
const withRefreshHeal = testEffect(LayerNode.compile(root, [...replacements, [Plugin.node, healPluginLayer]]))

afterEach(async () => {
  await disposeAllInstances()
  remoteToolCalls.length = 0
  progressMetadata.length = 0
  abortBridgeCalls.length = 0
  notifyAbortExecuteStarted = undefined
  releaseAbortExecute = undefined
  hubAbortCalls.length = 0
  notifyHubExecuteStarted = undefined
  releaseHubExecute = undefined
  refreshCalls.length = 0
  healDescriptors = [staleDescriptor]
})

describe("tool.registry", () => {
  it.instance("does not expose task_status", () =>
    Effect.gen(function* () {
      const registry = yield* ToolRegistry.Service
      const ids = yield* registry.ids()

      expect(ids).not.toContain("task_status")
    }),
  )

  it.instance("does not expose execute unless code mode is enabled", () =>
    Effect.gen(function* () {
      const registry = yield* ToolRegistry.Service
      const ids = yield* registry.ids()

      expect(ids).not.toContain("execute")
    }),
  )

  withCodeMode.instance("exposes execute when code mode is enabled", () =>
    Effect.gen(function* () {
      const registry = yield* ToolRegistry.Service
      const agents = yield* Agent.Service
      const ids = yield* registry.ids()
      const tools = yield* registry.tools({
        providerID: ProviderV2.ID.opencode,
        modelID: ModelV2.ID.make("test"),
        agent: yield* agents.defaultInfo(),
      })
      const execute = tools.find((tool) => tool.id === "execute")

      expect(ids).toContain("execute")
      expect(tools.map((tool) => tool.id)).toContain("execute")
      expect(execute?.description).toContain("tools.weather.current(input: {\n  city: string,\n})")
    }),
  )

  withEmptyCodeMode.instance("does not expose execute when code mode has no visible tools", () =>
    Effect.gen(function* () {
      const registry = yield* ToolRegistry.Service
      const agents = yield* Agent.Service
      const tools = yield* registry.tools({
        providerID: ProviderV2.ID.opencode,
        modelID: ModelV2.ID.make("test"),
        agent: yield* agents.defaultInfo(),
      })

      expect(tools.map((tool) => tool.id)).not.toContain("execute")
    }),
  )

  it.instance("hides task background parameter unless experimental background subagents are enabled", () =>
    Effect.gen(function* () {
      const registry = yield* ToolRegistry.Service
      const agent = yield* Agent.Service
      const build = yield* agent.get("build")
      if (!build) throw new Error("build agent not found")
      const task = (yield* registry.tools({
        providerID: ProviderV2.ID.opencode,
        modelID: ModelV2.ID.make("test"),
        agent: build,
      })).find((tool) => tool.id === "task")

      expect(task?.jsonSchema).toBeDefined()
      expect((task?.jsonSchema?.properties as Record<string, unknown> | undefined)?.background).toBeUndefined()
    }),
  )

  it.instance("loads tools from .opencode/tool (singular)", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const opencode = path.join(test.directory, ".opencode")
      const tool = path.join(opencode, "tool")
      yield* Effect.promise(() => fs.mkdir(tool, { recursive: true }))
      yield* Effect.promise(() =>
        Bun.write(
          path.join(tool, "hello.ts"),
          [
            "export default {",
            "  description: 'hello tool',",
            "  args: {},",
            "  execute: async () => {",
            "    return 'hello world'",
            "  },",
            "}",
            "",
          ].join("\n"),
        ),
      )
      const registry = yield* ToolRegistry.Service
      const ids = yield* registry.ids()
      expect(ids).toContain("hello")
    }),
  )

  it.instance("ignores non-tool exports in .opencode/tool files", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const tool = path.join(test.directory, ".opencode", "tool")
      yield* Effect.promise(() => fs.mkdir(tool, { recursive: true }))
      yield* Effect.promise(() =>
        Bun.write(
          path.join(tool, "mixed.ts"),
          [
            "export const helper = 'not a tool'",
            "export default {",
            "  description: 'mixed tool',",
            "  args: {},",
            "  execute: async () => 'ok',",
            "}",
            "",
          ].join("\n"),
        ),
      )

      const registry = yield* ToolRegistry.Service
      const ids = yield* registry.ids()
      expect(ids).toContain("mixed")
      expect(ids).not.toContain("mixed_helper")
    }),
  )

  // Regression for #27451 / #27630: a custom tool that omits `args` must not
  // crash registry initialization with
  // `Object.entries requires that input parameter not be null or undefined`.
  // Pre-1.14.49 the code path was `z.object(def.args)`, and `z.object(undefined)`
  // silently produced an empty schema — so the tool registered as no-args.
  // Preserve that tolerance.
  it.instance("tolerates a custom tool exporting null/undefined args (no-args fallback)", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const tool = path.join(test.directory, ".opencode", "tool")
      yield* Effect.promise(() => fs.mkdir(tool, { recursive: true }))
      yield* Effect.promise(() =>
        Bun.write(
          path.join(tool, "noargs.ts"),
          [
            "export default {",
            "  description: 'tool with no args',",
            "  args: undefined,",
            "  execute: async () => 'ok',",
            "}",
            "",
          ].join("\n"),
        ),
      )

      const registry = yield* ToolRegistry.Service
      const ids = yield* registry.ids()
      // Built-in tools must still load — a single malformed custom tool must
      // not poison the whole registry.
      expect(ids).toContain("read")
      const loaded = (yield* registry.all()).find((t) => t.id === "noargs")
      if (!loaded) throw new Error("noargs tool was not loaded")
      expect(loaded.jsonSchema).toMatchObject({ type: "object", properties: {} })
    }),
  )

  // Same regression, plugin entry point. The original reports (#27451, #27630)
  // came in through `plugin.list()` — `oh-my-opencode` was registering a tool
  // with `args: undefined` and crashing every message submit. The file-scan
  // and plugin-list loops both funnel through `fromPlugin`, but covering both
  // entry points means a future refactor that splits them won't silently lose
  // protection.
  withBrokenPlugin.instance("tolerates a plugin tool registered with null/undefined args", () =>
    Effect.gen(function* () {
      const registry = yield* ToolRegistry.Service
      const ids = yield* registry.ids()
      expect(ids).toContain("read")
      expect(ids).toContain("broken_plugin_tool")
      // Schema parity: the registry's `fromPlugin` and the hub's descriptor
      // extraction share `pluginToolJsonSchema`, so they cannot drift.
      const loaded = (yield* registry.all()).find((tool) => tool.id === "broken_plugin_tool")
      expect(loaded?.jsonSchema).toEqual(pluginToolJsonSchema(brokenDef))
    }),
  )

  // A hub-executed plugin tool ships only clone-safe descriptors; the
  // worker's `Plugin.Service.list()` exposes a `RemotePluginToolDefinition`
  // whose `execute` proxies to the hub. The registry must register it and route
  // `execute` through the executor (never `fromPlugin`, which would call a local
  // `def.execute` and re-truncate).
  withRemotePlugin.instance("registers a remote plugin tool from a clone-safe descriptor", () =>
    Effect.gen(function* () {
      const registry = yield* ToolRegistry.Service
      const ids = yield* registry.ids()
      expect(ids).toContain("remote_compress")

      const loaded = (yield* registry.all()).find((tool) => tool.id === "remote_compress")
      if (!loaded) throw new Error("remote_compress tool was not loaded")
      expect(loaded.description).toBe("remote compress tool")
      // The hub-computed schema passes through verbatim; no local decoder.
      expect(loaded.jsonSchema).toEqual(remoteDescriptors[0].jsonSchema as Tool.Def["jsonSchema"])
      expect(loaded.parameters).toBe(Schema.Unknown)
    }),
  )

  // A remote stub's constructed source matches the opinionated route
  // table exactly — the stub itself runs in the worker and forwards the call.
  withRemotePlugin.instance("the constructed source for a remote stub is remote-plugin", () =>
    Effect.gen(function* () {
      const registry = yield* ToolRegistry.Service
      const agents = yield* Agent.Service
      const tools = yield* registry.tools({
        providerID: ProviderV2.ID.opencode,
        modelID: ModelV2.ID.make("test"),
        agent: yield* agents.defaultInfo(),
      })
      const remote = tools.find((tool) => tool.id === "remote_compress")

      expect(remote?.source).toBe("remote-plugin")
      expect(ToolRoute.hostForSource(remote!.source)).toBe("worker")
      expect(ToolRoute.runsOn(ToolRoute.routeTool({ id: remote!.id, source: remote!.source }), "worker")).toBe(true)
    }),
  )

  withRemotePlugin.instance("a remote stub proxies execute to the hub executor and returns its result", () =>
    Effect.gen(function* () {
      const registry = yield* ToolRegistry.Service
      const agents = yield* Agent.Service
      const loaded = (yield* registry.all()).find((tool) => tool.id === "remote_compress")
      if (!loaded) throw new Error("remote_compress tool was not loaded")

      const args = { reason: "compact the history" }
      const result = yield* loaded.execute(args, {
        sessionID: SessionID.make("ses_remote"),
        messageID: MessageID.make("msg_remote"),
        callID: "call_remote",
        agent: (yield* agents.defaultInfo()).name,
        abort: new AbortController().signal,
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      } satisfies Tool.Context)

      expect(remoteToolCalls).toEqual([
        {
          toolID: "remote_compress",
          sessionID: "ses_remote",
          messageID: "msg_remote",
          callID: "call_remote",
          agent: (yield* agents.defaultInfo()).name,
          args,
        },
      ])
      expect(result.title).toBe(remoteToolResult.title)
      expect(result.output).toBe(remoteToolResult.output)
      expect(result.metadata).toEqual({ ranOnHub: true })
    }),
  )

  // L2 F2: a hub-executed plugin tool that emits progress mid-execute must have
  // each update reach the live tool part through `toolCtx.metadata`, not only the
  // final result metadata.
  withRemoteProgress.instance("forwards remote plugin progress to toolCtx.metadata", () =>
    Effect.gen(function* () {
      const registry = yield* ToolRegistry.Service
      const agents = yield* Agent.Service
      const loaded = (yield* registry.all()).find((tool) => tool.id === "remote_progress")
      if (!loaded) throw new Error("remote_progress tool was not loaded")

      const result = yield* loaded.execute(
        {},
        makeToolContext((yield* agents.defaultInfo()).name, {
          metadata: (input) =>
            Effect.sync(() => {
              progressMetadata.push(input)
            }),
        }),
      )

      expect(progressMetadata).toEqual([
        { title: "working", metadata: { pct: 42 } },
        { title: "almost", metadata: { pct: 90 } },
      ])
      expect(result.output).toBe("FINISHED")
      expect(result.metadata).toEqual({ final: true })
    }),
  )

  // L2 F1: aborting the stub context must invoke the remote definition's hub
  // abort bridge, and the listener must be removed once the call settles.
  withRemoteAbort.instance("invokes the hub abort bridge on abort, then unsubscribes", () =>
    Effect.gen(function* () {
      const registry = yield* ToolRegistry.Service
      const agents = yield* Agent.Service
      const loaded = (yield* registry.all()).find((tool) => tool.id === "remote_abort")
      if (!loaded) throw new Error("remote_abort tool was not loaded")

      const started = new Promise<void>((resolve) => {
        notifyAbortExecuteStarted = resolve
      })
      const controller = new AbortController()
      const fiber = yield* loaded
        .execute({}, makeToolContext((yield* agents.defaultInfo()).name, { abort: controller.signal }))
        .pipe(Effect.forkChild)

      // Wait for the remote execute to start (the abort subscription is registered
      // before the RPC), so the abort is deterministic rather than racing startup.
      yield* Effect.promise(() => started)
      expect(abortBridgeCalls).toEqual([])

      controller.abort()
      expect(abortBridgeCalls).toEqual([
        {
          sessionID: "ses_remote",
          messageID: "msg_remote",
          callID: "call_remote",
          agent: (yield* agents.defaultInfo()).name,
        },
      ])

      releaseAbortExecute?.({ title: "done", output: "DONE", metadata: {} })
      const result = yield* Fiber.join(fiber)
      expect(result.output).toBe("DONE")

      // The listener was removed when the call settled: a repeat abort is inert.
      controller.abort()
      expect(abortBridgeCalls.length).toBe(1)
    }),
  )

  // L2 F7: a remote plugin tool id must not silently shadow a built-in (or any
  // other custom) tool. The failure is loud and names the offending id.
  withRemoteCollision.instance("fails loud when a remote plugin tool id collides with a built-in tool", () =>
    Effect.gen(function* () {
      const registry = yield* ToolRegistry.Service
      const exit = yield* registry.ids().pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      const message = Exit.isFailure(exit) ? Cause.pretty(exit.cause) : ""
      expect(message).toContain('"read"')
      expect(message).toMatch(/collides with a locally registered tool/)
      // The registry mirrors `PluginToolIDCollisionError`'s `reserved` wording
      // exactly (it cannot import layer.ts without an ESM cycle).
      expect(message).toContain(new PluginToolIDCollisionError("read", "reserved").message)
    }),
  )

  // INT F1: abort must actually cross the hub proxy end-to-end. The remote
  // definition's abort bridge is built from the real `hubProxyPluginToolAbort`
  // client, so a local toolCtx abort reaches the hub's abort authority.
  withRemoteHubAbort.instance("abort crosses the hub proxy to the hub abort authority", () =>
    Effect.gen(function* () {
      const registry = yield* ToolRegistry.Service
      const agents = yield* Agent.Service
      const loaded = (yield* registry.all()).find((tool) => tool.id === "remote_hub_abort")
      if (!loaded) throw new Error("remote_hub_abort tool was not loaded")

      const started = new Promise<void>((resolve) => {
        notifyHubExecuteStarted = resolve
      })
      const controller = new AbortController()
      const fiber = yield* loaded
        .execute(
          {},
          makeToolContext((yield* agents.defaultInfo()).name, {
            sessionID: SessionID.make("ses_hub_abort"),
            callID: "call_hub_abort",
            abort: controller.signal,
          }),
        )
        .pipe(Effect.forkChild)

      yield* Effect.promise(() => started)
      expect(hubAbortCalls).toEqual([])

      controller.abort()
      yield* pollWithTimeout(
        Effect.sync(() => (hubAbortCalls.length > 0 ? true : undefined)),
        "the hub never received the pluginTool.abort RPC",
      )

      expect(hubAbortCalls).toEqual([{ sessionID: "ses_hub_abort", callID: "call_hub_abort" }])

      releaseHubExecute?.({ title: "done", output: "DONE", metadata: {} })
      const result = yield* Fiber.join(fiber)
      expect(result.output).toBe("DONE")
    }),
  )

  // INT F3: a stale descriptor self-heals. The stub refreshes the hub's set and
  // invalidates the cached registration, so the next build lists the fresh tool.
  withRefreshHeal.instance("refreshes and invalidates its cached state after UnknownPluginToolError", () =>
    Effect.gen(function* () {
      const registry = yield* ToolRegistry.Service
      const agents = yield* Agent.Service

      expect(yield* registry.ids()).toContain("stale_plugin_tool")

      const loaded = (yield* registry.all()).find((tool) => tool.id === "stale_plugin_tool")
      if (!loaded) throw new Error("stale_plugin_tool was not loaded")

      const exit = yield* loaded.execute({}, makeToolContext((yield* agents.defaultInfo()).name)).pipe(Effect.exit)
      expect(Exit.isFailure(exit)).toBe(true)
      expect(refreshCalls).toEqual(["refresh"])

      const ids = yield* registry.ids()
      expect(ids).toContain("fresh_plugin_tool")
      expect(ids).not.toContain("stale_plugin_tool")
    }),
  )

  it.instance("loads tools from .opencode/tools (plural)", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const opencode = path.join(test.directory, ".opencode")
      const tools = path.join(opencode, "tools")
      yield* Effect.promise(() => fs.mkdir(tools, { recursive: true }))
      yield* Effect.promise(() =>
        Bun.write(
          path.join(tools, "hello.ts"),
          [
            "export default {",
            "  description: 'hello tool',",
            "  args: {},",
            "  execute: async () => {",
            "    return 'hello world'",
            "  },",
            "}",
            "",
          ].join("\n"),
        ),
      )
      const registry = yield* ToolRegistry.Service
      const ids = yield* registry.ids()
      expect(ids).toContain("hello")
    }),
  )

  it.instance("loads Zod-schema custom tools with JSON Schema and validation", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const customTools = path.join(test.directory, ".opencode", "tools")
      const pluginTool = pathToFileURL(path.resolve(import.meta.dir, "../../../plugin/src/tool.ts")).href
      yield* Effect.promise(() => fs.mkdir(customTools, { recursive: true }))
      yield* Effect.promise(() =>
        Bun.write(
          path.join(customTools, "sql.ts"),
          [
            `import { tool } from ${JSON.stringify(pluginTool)}`,
            "export default tool({",
            "  description: 'query database',",
            "  args: { query: tool.schema.string().describe('SQL query to execute') },",
            "  execute: async ({ query }) => query,",
            "})",
            "",
          ].join("\n"),
        ),
      )

      const registry = yield* ToolRegistry.Service
      const loaded = (yield* registry.all()).find((tool) => tool.id === "sql")
      if (!loaded) throw new Error("custom sql tool was not loaded")
      expect(loaded?.jsonSchema).toMatchObject({
        type: "object",
        properties: {
          query: { type: "string", description: "SQL query to execute" },
        },
        required: ["query"],
      })
      expect(Result.isSuccess(Schema.decodeUnknownResult(loaded.parameters)({ query: "select 1" }))).toBe(true)
      expect(Result.isSuccess(Schema.decodeUnknownResult(loaded.parameters)({}))).toBe(false)

      const agents = yield* Agent.Service
      const promptTools = yield* registry.tools({
        providerID: ProviderV2.ID.opencode,
        modelID: ModelV2.ID.make("test"),
        agent: yield* agents.defaultInfo(),
      })
      const promptTool = promptTools.find((tool) => tool.id === "sql")
      if (!promptTool) throw new Error("custom sql tool was not returned for prompts")
      expect(ToolJsonSchema.fromTool(promptTool)).toMatchObject({
        properties: {
          query: { type: "string", description: "SQL query to execute" },
        },
        required: ["query"],
      })
    }),
  )

  it.instance(
    "preserves Zod arg descriptions from older config-scoped plugin packages",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const opencode = path.join(test.directory, ".opencode")
        const customTools = path.join(opencode, "tools")
        const plugin = path.join(opencode, "node_modules", "@opencode-ai", "plugin")
        yield* Effect.promise(() => fs.mkdir(path.join(plugin, "dist"), { recursive: true }))
        yield* Effect.promise(() => fs.mkdir(customTools, { recursive: true }))
        yield* Effect.promise(() =>
          fs.cp(path.dirname(fileURLToPath(import.meta.resolve("zod"))), path.join(opencode, "node_modules", "zod"), {
            dereference: true,
            recursive: true,
          }),
        )
        yield* Effect.promise(() =>
          Bun.write(
            path.join(plugin, "package.json"),
            JSON.stringify({ name: "@opencode-ai/plugin", type: "module", exports: { ".": "./dist/index.js" } }),
          ),
        )
        yield* Effect.promise(() =>
          Bun.write(
            path.join(plugin, "dist", "index.js"),
            [
              "import { z } from 'zod'",
              "export function tool(input) {",
              "  return input",
              "}",
              "tool.schema = z",
              "",
            ].join("\n"),
          ),
        )
        yield* Effect.promise(() =>
          Bun.write(
            path.join(customTools, "addition.ts"),
            [
              'import { tool } from "@opencode-ai/plugin"',
              "export default tool({",
              "  description: 'Use this tool to add two numbers and return their sum.',",
              "  args: {",
              "    left: tool.schema.number().describe('The first number to add'),",
              "    right: tool.schema.number().describe('The second number to add'),",
              "  },",
              "  execute: async (args) => `${args.left} + ${args.right} = ${args.left + args.right}`,",
              "})",
              "",
            ].join("\n"),
          ),
        )

        const registry = yield* ToolRegistry.Service
        const loaded = (yield* registry.all()).find((tool) => tool.id === "addition")
        if (!loaded) throw new Error("custom addition tool was not loaded")

        expect(ToolJsonSchema.fromTool(loaded)).toMatchObject({
          properties: {
            left: { type: "number", description: "The first number to add" },
            right: { type: "number", description: "The second number to add" },
          },
        })
      }),
    20_000,
  )

  it.instance("preserves attachments from structured custom tool results", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const customTools = path.join(test.directory, ".opencode", "tools")
      const pluginTool = pathToFileURL(path.resolve(import.meta.dir, "../../../plugin/src/tool.ts")).href
      yield* Effect.promise(() => fs.mkdir(customTools, { recursive: true }))
      yield* Effect.promise(() =>
        Bun.write(
          path.join(customTools, "image.ts"),
          [
            `import { tool } from ${JSON.stringify(pluginTool)}`,
            "export default tool({",
            "  description: 'image tool',",
            "  args: {},",
            "  execute: async () => ({",
            "    output: 'here is an image',",
            "    attachments: [{ type: 'file', mime: 'image/png', filename: 'picture.png', url: 'data:image/png;base64,AAAA' }],",
            "  }),",
            "})",
            "",
          ].join("\n"),
        ),
      )

      const registry = yield* ToolRegistry.Service
      const loaded = (yield* registry.all()).find((tool) => tool.id === "image")
      if (!loaded) throw new Error("custom image tool was not loaded")
      const agents = yield* Agent.Service
      const result = yield* loaded.execute({}, {
        sessionID: SessionID.make("ses_test"),
        messageID: MessageID.make("msg_test"),
        agent: (yield* agents.defaultInfo()).name,
        abort: new AbortController().signal,
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      } satisfies Tool.Context)

      expect(result.output).toBe("here is an image")
      expect(result.attachments).toEqual([
        { type: "file", mime: "image/png", filename: "picture.png", url: "data:image/png;base64,AAAA" },
      ])
    }),
  )

  it.instance("loads legacy JSON-schema-shaped custom tools with wire schema", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const tools = path.join(test.directory, ".opencode", "tools")
      yield* Effect.promise(() => fs.mkdir(tools, { recursive: true }))
      yield* Effect.promise(() =>
        Bun.write(
          path.join(tools, "legacy.ts"),
          [
            "export default {",
            "  description: 'legacy schema tool',",
            "  args: { text: { type: 'string', description: 'Text to render' } },",
            "  execute: async ({ text }) => text,",
            "}",
            "",
          ].join("\n"),
        ),
      )

      const registry = yield* ToolRegistry.Service
      const loaded = (yield* registry.all()).find((tool) => tool.id === "legacy")
      if (!loaded) throw new Error("legacy custom tool was not loaded")
      expect(ToolJsonSchema.fromTool(loaded)).toMatchObject({
        type: "object",
        properties: {
          text: { type: "string", description: "Text to render" },
        },
        required: ["text"],
      })
    }),
  )

  it.instance("loads tools with external dependencies without crashing", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const opencode = path.join(test.directory, ".opencode")
      const tools = path.join(opencode, "tools")
      yield* Effect.promise(() => fs.mkdir(tools, { recursive: true }))
      yield* Effect.promise(() =>
        Bun.write(
          path.join(opencode, "package.json"),
          JSON.stringify({
            name: "custom-tools",
            dependencies: {
              "@opencode-ai/plugin": "^0.0.0",
              cowsay: "^1.6.0",
            },
          }),
        ),
      )
      yield* Effect.promise(() =>
        Bun.write(
          path.join(opencode, "package-lock.json"),
          JSON.stringify({
            name: "custom-tools",
            lockfileVersion: 3,
            packages: {
              "": {
                dependencies: {
                  "@opencode-ai/plugin": "^0.0.0",
                  cowsay: "^1.6.0",
                },
              },
            },
          }),
        ),
      )

      const cowsay = path.join(opencode, "node_modules", "cowsay")
      yield* Effect.promise(() => fs.mkdir(cowsay, { recursive: true }))
      yield* Effect.promise(() =>
        Bun.write(
          path.join(cowsay, "package.json"),
          JSON.stringify({
            name: "cowsay",
            type: "module",
            exports: "./index.js",
          }),
        ),
      )
      yield* Effect.promise(() =>
        Bun.write(
          path.join(cowsay, "index.js"),
          ["export function say({ text }) {", "  return `moo ${text}`", "}", ""].join("\n"),
        ),
      )
      yield* Effect.promise(() =>
        Bun.write(
          path.join(tools, "cowsay.ts"),
          [
            "import { say } from 'cowsay'",
            "export default {",
            "  description: 'tool that imports cowsay at top level',",
            "  args: { text: { type: 'string' } },",
            "  execute: async ({ text }: { text: string }) => {",
            "    return say({ text })",
            "  },",
            "}",
            "",
          ].join("\n"),
        ),
      )
      const registry = yield* ToolRegistry.Service
      const ids = yield* registry.ids()
      expect(ids).toContain("cowsay")
    }),
  )
})
