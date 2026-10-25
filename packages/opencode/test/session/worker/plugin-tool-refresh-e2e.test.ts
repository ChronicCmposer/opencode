// F3 — descriptor refresh self-heals across the REAL hub-proxy boundary.
//
// A worker ships plugin-tool descriptors once at init. When the hub's plugin
// set changes (a reload, or a stale descriptor), the worker must re-fetch the
// live descriptors on demand and re-register them. This suite pins the genuine
// path — no stand-in:
//
//   worker registry holds the stale `plugin_x` stub
//     → executing it reaches the hub, which now exposes only `plugin_y`
//       → hub replies `UnknownPluginToolError` over the control port
//         → registry `def.refresh()` = `refreshablePluginTools.refreshFromHub`
//           → `hubProxyPluginToolList` → `pluginTool.list` RPC → live descriptors
//             → `refreshablePluginTools.refresh([...])`
//               → registry `InstanceState.invalidate`
//                 → the SAME registry service rebuilds and exposes `plugin_y`,
//                   dropping `plugin_x` (and the fresh tool executes over the port)

import { describe, expect, test } from "bun:test"
import { Effect, Exit, Layer, Option } from "effect"
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
  buildHubProxyStreamHandlers,
  pluginToolAuthority,
  type HubProxyRunner,
} from "@/session/worker/hub"
import {
  ModelClient,
  buildSessionWorkerLayer,
  createHubProxyClient,
  refreshablePluginTools,
  serveHubProxy,
  type SessionLayerOptions,
} from "@/session/worker/layer"
import { hubProxyPluginTool, hubProxyPluginToolList } from "@/session/worker/session-worker"
import type { PluginToolDescriptor } from "@/session/worker/plugin-tool-wire"
import { recordingHubAuthorities } from "../../lib/hub-proxy"
import { workerSnapshot } from "../../lib/snapshot"

/** The directory the HUB-side plugin context must observe (never the worker's). */
const HUB_DIRECTORY = "/hub/plugin-tool-refresh"

/** The directory the worker registry's `InstanceState` is keyed by. */
const WORKER_DIRECTORY = "/worker/plugin-tool-refresh"

const hubInstance = {
  directory: HUB_DIRECTORY,
  worktree: HUB_DIRECTORY,
  project: { id: "prj_hub_refresh", worktree: HUB_DIRECTORY, vcs: "git" },
} as never

const workerInstance = {
  directory: WORKER_DIRECTORY,
  worktree: WORKER_DIRECTORY,
  project: { id: "prj_worker_refresh", worktree: WORKER_DIRECTORY, vcs: "git" },
} as never

const STALE_ID = "plugin_x"
const FRESH_ID = "plugin_y"

const xTool = {
  description: "the stale hub tool",
  args: {},
  execute: async () => ({ title: "x", output: "HUB_X", metadata: {} }),
}
const yTool = {
  description: "the fresh hub tool",
  args: {},
  execute: async () => ({ title: "y", output: "HUB_Y", metadata: { refreshed: true } }),
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

/** A plugin whose `list()` reads the CURRENT hub tool map on every call. */
function fakePlugin(tools: () => Record<string, unknown>): Plugin.Interface {
  return {
    trigger: () => Effect.succeed({} as never),
    list: () => Effect.succeed([{ tool: tools() }] as never),
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

function workerLayer(pluginTools: SessionLayerOptions["pluginTools"]) {
  return buildSessionWorkerLayer({
    snapshot: workerSnapshot({ directory: WORKER_DIRECTORY }),
    env: { values: {} },
    clients: clients(),
    modelClient: stubModel(),
    pluginTools,
  }).layer
}

/** The clone-safe descriptor the hub publishes for a given tool id. */
function descriptorFor(id: string, description: string): PluginToolDescriptor {
  return {
    id,
    description,
    jsonSchema: { type: "object", properties: {}, additionalProperties: false },
  }
}

/** A served real hub-proxy pair plus the hub's current tool map and list calls. */
interface RefreshBoundary {
  readonly client: ReturnType<typeof createHubProxyClient>
  /** The live hub tool map; the test swaps it to simulate a plugin reload. */
  replaceHubTools(tools: Record<string, unknown>): void
  /** Every `pluginTool.list` snapshot the hub served, in order. */
  readonly listCalls: ReadonlyArray<ReadonlyArray<string>>
  dispose(): void
}

/**
 * Stand up the real boundary. The hub plugin's `list()` and the `pluginTool.list`
 * authority both read the SAME mutable tool map, so swapping it is exactly a
 * plugin reload: an execute of the old id becomes unknown, and a list returns
 * the new id.
 */
function refreshBoundary(): RefreshBoundary {
  let hubTools: Record<string, unknown> = { [STALE_ID]: xTool }
  const listCalls: string[][] = []

  const authority = pluginToolAuthority({ plugin: () => Option.some(fakePlugin(() => hubTools)), run })
  const { authorities } = recordingHubAuthorities({ pluginTool: authority })
  const all = {
    ...authorities,
    // The exact authority production wires: the live descriptor set per call.
    pluginToolList: async () => {
      const ids = Object.keys(hubTools)
      listCalls.push(ids)
      return ids.map((id) => descriptorFor(id, id === STALE_ID ? xTool.description : yTool.description))
    },
  }

  const channel = new MessageChannel()
  const disposeServer = serveHubProxy(buildHubProxyHandlers(all, run), channel.port2, buildHubProxyStreamHandlers(all))
  const client = createHubProxyClient(channel.port1)
  return {
    client,
    replaceHubTools: (tools) => {
      hubTools = tools
    },
    listCalls,
    dispose() {
      disposeServer()
      client.dispose()
      channel.port1.close()
      channel.port2.close()
    },
  }
}

/** A worker `Tool.Context` for the remote stub (only the frame is read). */
function toolContext(callID: string): Tool.Context {
  return {
    sessionID: SessionID.make("ses_plugin_tool_refresh_e2e"),
    messageID: MessageID.make("msg_plugin_tool_refresh_e2e"),
    callID,
    agent: "build",
    abort: new AbortController().signal,
    messages: [],
    metadata: () => Effect.void,
    ask: () => Effect.void,
  }
}

describe("plugin-tool refresh crosses the REAL hub-proxy boundary", () => {
  test("a stale execute refreshes the hub set, invalidates, and the next build exposes Y not X", async () => {
    const boundary = refreshBoundary()
    const executor = hubProxyPluginTool(boundary.client)
    const list = hubProxyPluginToolList(boundary.client)
    if (executor === undefined) throw new Error("a real hub client must yield a plugin-tool executor")
    if (list === undefined) throw new Error("a real hub client must yield a plugin-tool list bridge")

    // The worker boots on the stale descriptor set and keeps the live handle.
    const tools = refreshablePluginTools([descriptorFor(STALE_ID, xTool.description)], executor, { list })
    const layer = workerLayer(tools)

    try {
      const outcome = await Effect.runPromise(
        Effect.gen(function* () {
          const registry = yield* ToolRegistry.Service

          const before = yield* registry.ids()
          const stale = (yield* registry.all()).find((tool) => tool.id === STALE_ID)
          if (stale === undefined) throw new Error(`worker registry did not expose stale "${STALE_ID}"`)

          // A hub-side plugin reload: only the fresh tool now exists.
          boundary.replaceHubTools({ [FRESH_ID]: yTool })

          // Executing the stale descriptor fails; the failure itself heals the set.
          const exit = yield* stale.execute({}, toolContext("call_stale")).pipe(Effect.exit)

          const after = yield* registry.ids()
          const fresh = (yield* registry.all()).find((tool) => tool.id === FRESH_ID)
          const freshResult = fresh === undefined ? undefined : yield* fresh.execute({}, toolContext("call_fresh"))

          return { before, exit, after, freshResult }
        }).pipe(Effect.provide(layer), Effect.provideService(InstanceRef, workerInstance)),
      )

      expect(outcome.before).toContain(STALE_ID)
      expect(outcome.before).not.toContain(FRESH_ID)

      // The stale call crossed the port, hit the hub's UnknownPluginToolError and
      // settled as a failure (it cannot be retried transparently).
      expect(Exit.isFailure(outcome.exit)).toBe(true)

      // `pluginTool.list` was fetched over the port, after the reload.
      expect(boundary.listCalls).toEqual([[FRESH_ID]])

      // The SAME registry service rebuilt from the refreshed live descriptors.
      expect(outcome.after).toContain(FRESH_ID)
      expect(outcome.after).not.toContain(STALE_ID)

      // The fresh stub is real: it executes on the hub through the same port.
      expect(outcome.freshResult?.output).toBe("HUB_Y")
      expect(outcome.freshResult?.metadata).toEqual({ refreshed: true, truncated: false })
    } finally {
      boundary.dispose()
    }
  }, 30_000)
})
