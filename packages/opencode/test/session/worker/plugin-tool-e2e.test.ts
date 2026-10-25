// PT-4 — plugin-provided tools end to end over a REAL hub-proxy port.
//
// The worker cannot run a plugin tool itself: the plugin instance (and therefore
// its `execute`) lives on the hub (decision 70). This suite wires the genuine
// boundary — no stand-in:
//
//   worker `ToolRegistry` (`fromRemotePluginTool` stub)
//     → `hubProxyPluginTool(createHubProxyClient(port1))`   (real worker bridge)
//       → `MessageChannel`                                  (real clone boundary)
//         → `serveHubProxy(buildHubProxyHandlers(authorities), port2)`
//           → `pluginToolAuthority` → stub hub plugin `execute`
//
// It pins three things the unit suites cannot:
//   * the worker registry LISTS the hub tool id (descriptors + stub);
//   * executing that id crosses the port and returns the HUB result, including
//     the hub-observed call frame (proving it ran on the hub, not in-worker);
//   * degraded boot (no executor ⇒ no `pluginTools`) lists no remote tool.

import { describe, expect, test } from "bun:test"
import { Effect, Layer, Option, Schema } from "effect"
import { Agent } from "@/agent/agent"
import { InstanceRef } from "@/effect/instance-ref"
import { Permission } from "@/permission"
import { Plugin } from "@/plugin"
import { MessageID, SessionID } from "@/session/schema"
import { Session } from "@/session/session"
import { ToolRegistry } from "@/tool/registry"
import { Truncate } from "@/tool/truncate"
import type { Tool } from "@/tool/tool"
import {
  buildHubProxyHandlers,
  pluginToolAuthority,
  type HubProxyRunner,
} from "@/session/worker/hub"
import {
  ModelClient,
  buildSessionWorkerLayer,
  createHubProxyClient,
  serveHubProxy,
  type SessionLayerOptions,
} from "@/session/worker/layer"
import { hubProxyPluginTool } from "@/session/worker/session-worker"
import type { PluginToolDescriptor } from "@/session/worker/plugin-tool-wire"
import { instantRunner, recordingHubAuthorities, type HubAuthorityCall } from "../../lib/hub-proxy"
import { workerSnapshot } from "../../lib/snapshot"

/** The directory the HUB-side `PluginToolContext` must observe (never the worker's). */
const HUB_DIRECTORY = "/hub/plugin-tool-project"

/** The directory the worker registry's `InstanceState` is keyed by. */
const WORKER_DIRECTORY = "/worker/plugin-tool-project"

const hubInstance = {
  directory: HUB_DIRECTORY,
  worktree: HUB_DIRECTORY,
  project: { id: "prj_hub", worktree: HUB_DIRECTORY, vcs: "git" },
} as never

const workerInstance = {
  directory: WORKER_DIRECTORY,
  worktree: WORKER_DIRECTORY,
  project: { id: "prj_worker", worktree: WORKER_DIRECTORY, vcs: "git" },
} as never

/** The clone-safe descriptor the hub ships for the fake `compress` tool. */
const descriptor: PluginToolDescriptor = {
  id: "compress",
  description: "compress the conversation history",
  jsonSchema: {
    type: "object",
    properties: { range: { type: "string" } },
    additionalProperties: false,
  },
}

/** The hub-resident services `pluginToolExecute` reads from the captured context. */
const hubServices = Layer.mergeAll(
  Layer.mock(Permission.Service, { ask: () => Effect.void }),
  Layer.mock(Truncate.Service, {
    output: (text: string) => Effect.succeed({ content: text, truncated: false as const }),
  }),
  Layer.mock(Agent.Service, { get: () => Effect.succeed({ permission: [] } as unknown as Agent.Info) }),
  Layer.mock(Session.Service, { get: () => Effect.succeed({ permission: [] } as unknown as Session.Info) }),
)

/** Run a hub authority effect with the hub services + the HUB instance context. */
const run: HubProxyRunner = (effect) =>
  Effect.runPromise(effect.pipe(Effect.provide(hubServices), Effect.provideService(InstanceRef, hubInstance)))

type ToolHooks = { readonly tool?: Record<string, unknown> }

/** A `Plugin.Interface` double whose `list()` returns `hooks`. */
function fakePlugin(hooks: readonly ToolHooks[]): Plugin.Interface {
  return {
    trigger: () => Effect.succeed({} as never),
    list: () => Effect.succeed(hooks as never),
    init: () => Effect.void,
  } as unknown as Plugin.Interface
}

/** The worker graph seams (no real IO: the registry never calls them here). */
const clients = (): SessionLayerOptions["clients"] => ({
  writer: { query: async () => [], publish: async () => ({ seq: 1, committed: true }) },
  egress: { emit: () => {} },
  broker: { call: async () => null },
  backgroundJob: { wait: async () => null, cancel: async () => {} },
})

const stubModel = () => Layer.succeed(ModelClient, { complete: () => Effect.succeed({ text: "stub" }) })

/** The real hub-proxy boundary: served handlers + the worker-side client end. */
interface PluginToolBoundary {
  readonly client: ReturnType<typeof createHubProxyClient>
  readonly calls: ReadonlyArray<HubAuthorityCall>
  dispose(): void
}

/**
 * Stand up a real `MessageChannel` whose hub side serves `pluginToolAuthority`
 * for a stub plugin exposing `compress`. `execute` records that it ran and
 * echoes the HUB-observed call frame, so a worker-local stand-in cannot pass.
 */
function pluginToolBoundary(): PluginToolBoundary {
  const executed: string[] = []
  const compress = {
    description: descriptor.description,
    args: {},
    execute: async (_args: unknown, ctx: { sessionID: string; directory: string }) => {
      executed.push(ctx.sessionID)
      return {
        title: "compressed",
        output: "HUB_RAN",
        metadata: { sessionID: ctx.sessionID, directory: ctx.directory },
      }
    },
  }
  const authority = pluginToolAuthority({
    plugin: () => Option.some(fakePlugin([{ tool: { compress } }])),
    run,
  })
  const { authorities, calls } = recordingHubAuthorities({ pluginTool: authority })
  const channel = new MessageChannel()
  const disposeServer = serveHubProxy(buildHubProxyHandlers(authorities, instantRunner), channel.port2)
  const client = createHubProxyClient(channel.port1)
  return {
    client,
    calls,
    dispose() {
      disposeServer()
      client.dispose()
      channel.port1.close()
      channel.port2.close()
    },
  }
}

/** Build the worker graph, optionally with the hub-executed plugin tool wired. */
function workerLayer(pluginTools?: SessionLayerOptions["pluginTools"]) {
  return buildSessionWorkerLayer({
    snapshot: workerSnapshot({ directory: WORKER_DIRECTORY }),
    env: { values: {} },
    clients: clients(),
    modelClient: stubModel(),
    ...(pluginTools === undefined ? {} : { pluginTools }),
  }).layer
}

/** Resolve the worker registry and read its ids exactly as the model-facing path does. */
const registryIds = (layer: Layer.Layer<unknown, unknown>) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const registry = yield* ToolRegistry.Service
      return yield* registry.ids()
    }).pipe(Effect.provide(layer), Effect.provideService(InstanceRef, workerInstance)),
  )

/** Resolve the remote `compress` tool from the worker registry and execute it. */
const executeCompress = (layer: Layer.Layer<unknown, unknown>) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const registry = yield* ToolRegistry.Service
      const tool = (yield* registry.all()).find((candidate) => candidate.id === descriptor.id)
      if (tool === undefined) throw new Error(`worker registry did not expose "${descriptor.id}"`)

      const result = yield* tool.execute(
        { range: "*" },
        {
          sessionID: SessionID.make("ses_plugin_tool_e2e"),
          messageID: MessageID.make("msg_plugin_tool_e2e"),
          callID: "call_plugin_tool_e2e",
          agent: "build",
          abort: new AbortController().signal,
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        } satisfies Tool.Context,
      )
      return { tool, result }
    }).pipe(Effect.provide(layer), Effect.provideService(InstanceRef, workerInstance)),
  )

describe("plugin tools cross the REAL hub-proxy boundary", () => {
  test("the worker registry lists the remote tool and executes it on the hub", async () => {
    const boundary = pluginToolBoundary()
    const executor = hubProxyPluginTool(boundary.client)
    if (executor === undefined) throw new Error("a real hub client must yield a plugin-tool executor")
    try {
      const layer = workerLayer({ descriptors: [descriptor], executor })

      // (a) Discovery: the real ToolRegistry registers the hub tool id.
      expect(await registryIds(layer)).toContain(descriptor.id)

      // (b) Execution: the call crosses the port and returns the HUB result.
      const { tool, result } = await executeCompress(layer)
      expect(boundary.calls.some((call) => call.method === "pluginTool.execute")).toBe(true)
      expect(result.title).toBe("compressed")
      expect(result.output).toBe("HUB_RAN")
      // The hub reconstructed the context from ITS instance, so a worker-local
      // stand-in (which only knows WORKER_DIRECTORY) cannot produce this.
      expect(result.metadata).toEqual({
        sessionID: "ses_plugin_tool_e2e",
        directory: HUB_DIRECTORY,
        truncated: false,
      })

      // The descriptor's clone-safe fields reach the registry verbatim.
      expect(tool.description).toBe(descriptor.description)
      expect(tool.jsonSchema).toEqual(descriptor.jsonSchema as Tool.Def["jsonSchema"])
      expect(tool.parameters).toBe(Schema.Unknown)
    } finally {
      boundary.dispose()
    }
  }, 30_000)

  test("degraded boot (no executor) lists no plugin tool", async () => {
    // `hubProxyPluginTool` is the production seam: no hub client ⇒ no executor,
    // and `bootstrapSessionWorker` then omits `pluginTools` entirely.
    expect(hubProxyPluginTool(undefined)).toBeUndefined()

    const ids = await registryIds(workerLayer())
    expect(ids).not.toContain(descriptor.id)
    // The worker still owns its builtins; degraded means "no remote tools", not
    // "broken graph".
    expect(ids).toContain("read")
  }, 30_000)
})
