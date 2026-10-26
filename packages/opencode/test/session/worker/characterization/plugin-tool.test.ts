// Characterization: a hub-executed plugin tool (`compress`) under session workers.
//
// Plugins live on the hub (decision 70): the worker registry ships a clone-safe
// descriptor and forwards execution back over the real hub-proxy port. This
// test pins the two observable guarantees the per-session worker refactor must
// keep:
//   * the worker `ToolRegistry` LISTS `compress` with its descriptor fields;
//   * executing it crosses the `MessageChannel` and returns the HUB result (a
//     worker-local stand-in cannot observe the hub directory).
//
// It is the registry-level slice of `plugin-tool-e2e.test.ts`; no provider or
// filesystem is required, so it stays fast and deterministic.

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
import { instantRunner, recordingHubAuthorities, type HubAuthorityCall } from "../../../lib/hub-proxy"
import { workerSnapshot } from "../../../lib/snapshot"

const HUB_DIRECTORY = "/hub/plugin-tool-project"
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

const descriptor: PluginToolDescriptor = {
  id: "compress",
  description: "compress the conversation history",
  jsonSchema: {
    type: "object",
    properties: { range: { type: "string" } },
    additionalProperties: false,
  },
}

const hubServices = Layer.mergeAll(
  Layer.mock(Permission.Service, { ask: () => Effect.void }),
  Layer.mock(Truncate.Service, {
    output: (text: string) => Effect.succeed({ content: text, truncated: false as const }),
  }),
  Layer.mock(Agent.Service, { get: () => Effect.succeed({ permission: [] } as unknown as Agent.Info) }),
  Layer.mock(Session.Service, { get: () => Effect.succeed({ permission: [] } as unknown as Session.Info) }),
)

const run: HubProxyRunner = (effect) =>
  Effect.runPromise(effect.pipe(Effect.provide(hubServices), Effect.provideService(InstanceRef, hubInstance)))

function fakePlugin(hooks: ReadonlyArray<{ readonly tool?: Record<string, unknown> }>): Plugin.Interface {
  return {
    trigger: () => Effect.succeed({} as never),
    list: () => Effect.succeed(hooks as never),
    init: () => Effect.void,
  } as unknown as Plugin.Interface
}

const clients = (): SessionLayerOptions["clients"] => ({
  writer: { query: async () => [], publish: async () => ({ seq: 1, committed: true }) },
  egress: { emit: () => {} },
  backgroundJob: { wait: async () => null, cancel: async () => {} },
})

const stubModel = () => Layer.succeed(ModelClient, { complete: () => Effect.succeed({ text: "stub" }) })

interface Boundary {
  readonly client: ReturnType<typeof createHubProxyClient>
  readonly calls: ReadonlyArray<HubAuthorityCall>
  dispose(): void
}

/** A real hub-proxy boundary whose `compress` executor echoes the HUB context. */
function pluginToolBoundary(): Boundary {
  const compress = {
    description: descriptor.description,
    args: {},
    execute: async (_args: unknown, context: { sessionID: string; directory: string }) => ({
      title: "compressed",
      output: "HUB_RAN",
      metadata: { sessionID: context.sessionID, directory: context.directory },
    }),
  }
  const authority = pluginToolAuthority({ plugin: () => Option.some(fakePlugin([{ tool: { compress } }])), run })
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

function workerLayer(pluginTools?: SessionLayerOptions["pluginTools"]) {
  return buildSessionWorkerLayer({
    snapshot: workerSnapshot({ directory: WORKER_DIRECTORY }),
    env: { values: {} },
    clients: clients(),
    modelClient: stubModel(),
    ...(pluginTools === undefined ? {} : { pluginTools }),
  }).layer
}

const registryIds = (layer: Layer.Layer<unknown, unknown>) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const registry = yield* ToolRegistry.Service
      return yield* registry.ids()
    }).pipe(Effect.provide(layer), Effect.provideService(InstanceRef, workerInstance)),
  )

const executeCompress = (layer: Layer.Layer<unknown, unknown>) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const registry = yield* ToolRegistry.Service
      const tool = (yield* registry.all()).find((candidate) => candidate.id === descriptor.id)
      if (tool === undefined) throw new Error(`worker registry did not expose "${descriptor.id}"`)
      const result = yield* tool.execute(
        { range: "*" },
        {
          sessionID: SessionID.make("ses_plugin_tool_characterization"),
          messageID: MessageID.make("msg_plugin_tool_characterization"),
          callID: "call_plugin_tool_characterization",
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

describe("plugin tool (compress) under session workers", () => {
  test("the worker registry lists compress and executing it runs on the hub", async () => {
    const boundary = pluginToolBoundary()
    const executor = hubProxyPluginTool(boundary.client)
    if (executor === undefined) throw new Error("a real hub client must yield a plugin-tool executor")
    try {
      const layer = workerLayer({ descriptors: [descriptor], executor })

      expect(await registryIds(layer)).toContain(descriptor.id)

      const { tool, result } = await executeCompress(layer)
      expect(boundary.calls.some((call) => call.method === "pluginTool.execute")).toBe(true)
      expect(result.title).toBe("compressed")
      expect(result.output).toBe("HUB_RAN")
      // The hub-owned metadata is the contract; the truncate boundary may add
      // its own fields (e.g. `truncated`), which are not worker-observable policy.
      expect(result.metadata).toMatchObject({
        sessionID: "ses_plugin_tool_characterization",
        directory: HUB_DIRECTORY,
      })
      expect(tool.description).toBe(descriptor.description)
      expect(tool.jsonSchema).toEqual(descriptor.jsonSchema as Tool.Def["jsonSchema"])
      expect(tool.parameters).toBe(Schema.Unknown)
    } finally {
      boundary.dispose()
    }
  }, 30_000)

  test("without a hub executor the worker lists no remote tool but keeps builtins", async () => {
    expect(hubProxyPluginTool(undefined)).toBeUndefined()
    const ids = await registryIds(workerLayer())
    expect(ids).not.toContain(descriptor.id)
    expect(ids).toContain("read")
  }, 30_000)
})
