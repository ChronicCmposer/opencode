// F1 — plugin-tool abort crosses the REAL hub-proxy boundary in both directions.
//
// A hub-owned plugin tool's `execute` runs on the hub under a per-call
// `AbortController`. The worker owns only a remote stub, so cancelling the
// worker's `Tool.Context.abort` must reach the hub and unwind the real plugin
// call. This suite pins the genuine path — no stand-in:
//
//   worker `toolCtx.abort` (AbortController)
//     → registry `fromRemotePluginTool` abort subscription
//       → `hubProxyPluginToolAbort` → `pluginTool.abort` RPC
//         → `MessageChannel` → `createPluginToolAbortRegistry.abort`
//           → hub plugin `PluginToolContext.abort` signal fires
//             → the gated `execute` rejects, replying `ok:false`
//               → the worker call settles as an error
//
// After that failure the SAME control port must still serve a normal execute
// (a stuck or half-closed port would surface here).

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
  createPluginToolAbortRegistry,
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
import { hubProxyPluginTool, hubProxyPluginToolAbort } from "@/session/worker/session-worker"
import type { PluginToolDescriptor } from "@/session/worker/plugin-tool-wire"
import { awaitWithTimeout } from "../../lib/effect"
import { recordingHubAuthorities } from "../../lib/hub-proxy"
import { workerSnapshot } from "../../lib/snapshot"

/** The directory the HUB-side plugin context must observe (never the worker's). */
const HUB_DIRECTORY = "/hub/plugin-tool-abort"

/** The directory the worker registry's `InstanceState` is keyed by. */
const WORKER_DIRECTORY = "/worker/plugin-tool-abort"

const hubInstance = {
  directory: HUB_DIRECTORY,
  worktree: HUB_DIRECTORY,
  project: { id: "prj_hub_abort", worktree: HUB_DIRECTORY, vcs: "git" },
} as never

const workerInstance = {
  directory: WORKER_DIRECTORY,
  worktree: WORKER_DIRECTORY,
  project: { id: "prj_worker_abort", worktree: WORKER_DIRECTORY, vcs: "git" },
} as never

const descriptor: PluginToolDescriptor = {
  id: "abortable_tool",
  description: "a long-running hub tool",
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

/** A served real hub-proxy pair plus readiness/signal observations. */
interface AbortBoundary {
  readonly client: ReturnType<typeof createHubProxyClient>
  /** Resolves once the hub plugin's `execute` attached its abort listener. */
  readonly started: Promise<void>
  /** The hub plugin's `PluginToolContext.abort` signal, once it is running. */
  readonly hubSignal: () => AbortSignal | undefined
  dispose(): void
}

/**
 * Stand up the real boundary. `execute({ mode: "hang" })` publishes `started`,
 * then waits for `ctx.abort` and rejects when it fires (a real plugin honouring
 * cancellation). `execute({ mode: "quick" })` returns at once, proving the port
 * still serves requests after the aborted call.
 */
function abortBoundary(): AbortBoundary {
  const aborts = createPluginToolAbortRegistry()
  let notifyStarted: () => void = () => {}
  const started = new Promise<void>((resolve) => (notifyStarted = resolve))
  let seenSignal: AbortSignal | undefined

  const abortableTool = {
    description: descriptor.description,
    args: {},
    execute: async (args: unknown, ctx: { abort: AbortSignal }) => {
      seenSignal = ctx.abort
      if ((args as { mode?: string }).mode === "quick") {
        return { title: "quick", output: "HUB_QUICK", metadata: { quick: true } }
      }
      const aborted = new Promise<never>((_resolve, reject) => {
        const fail = () => reject(new Error("HUB_TOOL_ABORTED"))
        if (ctx.abort.aborted) fail()
        else ctx.abort.addEventListener("abort", fail, { once: true })
      })
      notifyStarted()
      await aborted
      return { title: "never", output: "HUB_NEVER", metadata: {} }
    },
  }

  const authority = pluginToolAuthority({
    plugin: () => Option.some(fakePlugin([{ tool: { abortable_tool: abortableTool } }])),
    run,
    registerAbort: (sessionID, callID) => aborts.registerAbort(sessionID, callID),
  })
  const { authorities } = recordingHubAuthorities({ pluginTool: authority })
  const all = {
    ...authorities,
    // The exact authority production wires: `(sessionID, callID)` → abort registry.
    pluginToolAbort: (input: { readonly sessionID: string; readonly callID: string }) =>
      aborts.abort(input.sessionID, input.callID),
  }

  const channel = new MessageChannel()
  const disposeServer = serveHubProxy(buildHubProxyHandlers(all, run), channel.port2, buildHubProxyStreamHandlers(all))
  const client = createHubProxyClient(channel.port1)
  return {
    client,
    started,
    hubSignal: () => seenSignal,
    dispose() {
      disposeServer()
      client.dispose()
      channel.port1.close()
      channel.port2.close()
    },
  }
}

/** Resolve the remote tool from the worker registry and execute it with `args`. */
function executeTool(
  layer: Layer.Layer<unknown, unknown>,
  args: unknown,
  abort: AbortSignal,
): Promise<Tool.ExecuteResult> {
  return Effect.runPromise(
    Effect.gen(function* () {
      const registry = yield* ToolRegistry.Service
      const tool = (yield* registry.all()).find((candidate) => candidate.id === descriptor.id)
      if (tool === undefined) throw new Error(`worker registry did not expose "${descriptor.id}"`)
      return yield* tool.execute(args as never, {
        sessionID: SessionID.make("ses_plugin_tool_abort_e2e"),
        messageID: MessageID.make("msg_plugin_tool_abort_e2e"),
        callID: "call_plugin_tool_abort_e2e",
        agent: "build",
        abort,
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      } satisfies Tool.Context)
    }).pipe(Effect.provide(layer), Effect.provideService(InstanceRef, workerInstance)),
  )
}

describe("plugin-tool abort crosses the REAL hub-proxy boundary", () => {
  test("worker abort unwinds the hub plugin's ctx.abort; the port still serves afterward", async () => {
    const boundary = abortBoundary()
    const executor = hubProxyPluginTool(boundary.client)
    const abort = hubProxyPluginToolAbort(boundary.client)
    if (executor === undefined) throw new Error("a real hub client must yield a plugin-tool executor")
    if (abort === undefined) throw new Error("a real hub client must yield a plugin-tool abort bridge")

    const layer = workerLayer({ descriptors: [descriptor], executor, abort })
    try {
      const controller = new AbortController()
      const pending = executeTool(layer, { mode: "hang" }, controller.signal)

      // Readiness: the hub plugin is waiting on its own context signal, with the
      // abort registry entry already registered by `pluginToolExecute`.
      await awaitSignal(boundary.started, "the hub plugin never started its execute")
      expect(boundary.hubSignal()?.aborted).toBe(false)

      controller.abort()

      const settled = pending.then(
        (value) => ({ ok: true as const, value }),
        (error: unknown) => ({ ok: false as const, error }),
      )
      const outcome = await awaitSignal(settled, "the aborted plugin tool call never settled")
      expect(outcome.ok).toBe(false)
      if (outcome.ok) throw new Error("the aborted call unexpectedly resolved")
      expect(String(outcome.error)).toContain("HUB_TOOL_ABORTED")
      expect(boundary.hubSignal()?.aborted).toBe(true)

      // Same port, same service: a normal call still succeeds after the abort.
      const result = await awaitSignal(
        executeTool(layer, { mode: "quick" }, new AbortController().signal),
        "the port did not serve a call after the abort",
      )
      expect(result.output).toBe("HUB_QUICK")
      expect(result.metadata).toEqual({ quick: true, truncated: false })
    } finally {
      boundary.dispose()
    }
  }, 30_000)
})
