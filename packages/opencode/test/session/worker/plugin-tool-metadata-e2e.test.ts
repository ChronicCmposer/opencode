// F2 — live plugin-tool metadata crosses the REAL hub-proxy boundary.
//
// A hub-owned plugin tool reports progress through its `ToolContext.metadata`
// while it runs. The worker cannot see that context: it only owns a remote
// stub. This suite pins the genuine path — no stand-in:
//
//   hub plugin `execute(args, ctx)` → `ctx.metadata({ title, metadata })`
//     → `pluginToolAuthority`'s `emit`
//       → `serveHubProxy(..., streaming)` `pluginTool.metadata` event frame
//         → `MessageChannel`
//           → `hubProxyPluginTool` onEvent
//             → worker registry `toolCtx.metadata` (the live tool part)
//
// The hub `execute` is gated AFTER publishing metadata, so the worker MUST
// observe the update while the call is still in flight. Only when the test has
// observed it does the gate release; the final result metadata must still win.

import { describe, expect, test } from "bun:test"
import { Effect, Layer, Option } from "effect"
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
  serveHubProxy,
  type SessionLayerOptions,
} from "@/session/worker/layer"
import { hubProxyPluginTool } from "@/session/worker/session-worker"
import type { PluginToolDescriptor } from "@/session/worker/plugin-tool-wire"
import { awaitWithTimeout } from "../../lib/effect"
import { recordingHubAuthorities } from "../../lib/hub-proxy"
import { workerSnapshot } from "../../lib/snapshot"

/** The directory the HUB-side plugin context must observe (never the worker's). */
const HUB_DIRECTORY = "/hub/plugin-tool-metadata"

/** The directory the worker registry's `InstanceState` is keyed by. */
const WORKER_DIRECTORY = "/worker/plugin-tool-metadata"

const hubInstance = {
  directory: HUB_DIRECTORY,
  worktree: HUB_DIRECTORY,
  project: { id: "prj_hub_metadata", worktree: HUB_DIRECTORY, vcs: "git" },
} as never

const workerInstance = {
  directory: WORKER_DIRECTORY,
  worktree: WORKER_DIRECTORY,
  project: { id: "prj_worker_metadata", worktree: WORKER_DIRECTORY, vcs: "git" },
} as never

const descriptor: PluginToolDescriptor = {
  id: "progress_tool",
  description: "reports live progress",
  jsonSchema: { type: "object", properties: {}, additionalProperties: false },
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

/** Await a published readiness/reply signal with a descriptive timeout. */
const awaitSignal = <A>(signal: Promise<A>, message: string): Promise<A> =>
  Effect.runPromise(
    awaitWithTimeout(
      Effect.promise(() => signal),
      message,
      "5 seconds",
    ),
  )

function workerLayer(pluginTools: SessionLayerOptions["pluginTools"]) {
  return buildSessionWorkerLayer({
    snapshot: workerSnapshot({ directory: WORKER_DIRECTORY }),
    env: { values: {} },
    clients: clients(),
    modelClient: stubModel(),
    pluginTools,
  }).layer
}

/** A served real hub-proxy pair plus the observed hub metadata publications. */
interface MetadataBoundary {
  readonly client: ReturnType<typeof createHubProxyClient>
  /** Resolves the first time the hub plugin publishes progress. */
  readonly published: Promise<void>
  /** Releases the gated hub `execute`; called only after the worker observed it. */
  release(): void
  dispose(): void
}

/**
 * Stand up the real streaming boundary. The hub plugin publishes one progress
 * update, then blocks on a gate so the worker's observation provably happens
 * BEFORE `execute` returns.
 */
function metadataBoundary(): MetadataBoundary {
  let notifyPublished: () => void = () => {}
  const published = new Promise<void>((resolve) => (notifyPublished = resolve))
  let release: () => void = () => {}
  const gate = new Promise<void>((resolve) => (release = resolve))

  const progressTool = {
    description: descriptor.description,
    args: {},
    execute: async (_args: unknown, ctx: { metadata: (input: unknown) => void }) => {
      ctx.metadata({ title: "working", metadata: { pct: 10 } })
      notifyPublished()
      await gate
      return { title: "done", output: "HUB_RAN", metadata: { final: true } }
    },
  }

  const authority = pluginToolAuthority({
    plugin: () => Option.some(fakePlugin([{ tool: { progress_tool: progressTool } }])),
    run,
  })
  const { authorities } = recordingHubAuthorities({ pluginTool: authority })
  const channel = new MessageChannel()
  const disposeServer = serveHubProxy(
    buildHubProxyHandlers(authorities, run),
    channel.port2,
    buildHubProxyStreamHandlers(authorities),
  )
  const client = createHubProxyClient(channel.port1)
  return {
    client,
    published,
    release: () => release(),
    dispose() {
      release()
      disposeServer()
      client.dispose()
      channel.port1.close()
      channel.port2.close()
    },
  }
}

/** Resolve the remote tool from the worker registry and execute it with `metadata`. */
function executeProgressTool(
  layer: Layer.Layer<unknown, unknown>,
  metadata: Tool.Context["metadata"],
): Promise<Tool.ExecuteResult> {
  return Effect.runPromise(
    Effect.gen(function* () {
      const registry = yield* ToolRegistry.Service
      const tool = (yield* registry.all()).find((candidate) => candidate.id === descriptor.id)
      if (tool === undefined) throw new Error(`worker registry did not expose "${descriptor.id}"`)
      return yield* tool.execute({}, {
        sessionID: SessionID.make("ses_plugin_tool_metadata_e2e"),
        messageID: MessageID.make("msg_plugin_tool_metadata_e2e"),
        callID: "call_plugin_tool_metadata_e2e",
        agent: "build",
        abort: new AbortController().signal,
        messages: [],
        metadata,
        ask: () => Effect.void,
      } satisfies Tool.Context)
    }).pipe(Effect.provide(layer), Effect.provideService(InstanceRef, workerInstance)),
  )
}

describe("plugin-tool metadata crosses the REAL hub-proxy boundary", () => {
  test("worker Tool.Context.metadata sees progress before execute returns; final metadata wins", async () => {
    const boundary = metadataBoundary()
    const executor = hubProxyPluginTool(boundary.client)
    if (executor === undefined) throw new Error("a real hub client must yield a plugin-tool executor")

    const observed: Array<{ title?: string; metadata?: unknown }> = []
    let executeSettled = false
    let notifyObserved: () => void = () => {}
    const observedByWorker = new Promise<void>((resolve) => (notifyObserved = resolve))
    const metadata: Tool.Context["metadata"] = (input) =>
      Effect.sync(() => {
        observed.push(input)
        notifyObserved()
      })

    try {
      const layer = workerLayer({ descriptors: [descriptor], executor })
      const pending = executeProgressTool(layer, metadata)
      void pending.then(
        () => {
          executeSettled = true
        },
        () => {
          executeSettled = true
        },
      )

      // The hub has published the frame; wait for the WORKER to apply it to the
      // live tool part while the hub `execute` is still blocked on its gate.
      await awaitSignal(boundary.published, "the hub never published metadata")
      await awaitSignal(observedByWorker, "the worker never observed plugin metadata")
      expect(executeSettled).toBe(false)
      expect(observed).toEqual([{ title: "working", metadata: { pct: 10 } }])

      boundary.release()
      const result = await awaitSignal(pending, "the plugin tool never returned after metadata")

      // The final result metadata is authoritative; progress did not overwrite it.
      expect(result.output).toBe("HUB_RAN")
      expect(result.metadata).toEqual({ final: true, truncated: false })
    } finally {
      boundary.dispose()
    }
  }, 30_000)
})
