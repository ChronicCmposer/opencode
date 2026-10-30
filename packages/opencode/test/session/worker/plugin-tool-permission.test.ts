// Plugin-tool permission resolution on the HUB.
//
// A plugin tool runs on the hub, so its `PluginToolContext.ask` must resolve
// against the HUB-RESIDENT `Permission.Service` (never by re-entering the worker
// control port, which would deadlock the in-flight request). The hub rebuilds the
// ruleset from the calling agent + session, exactly like the registry's local
// `fromPlugin` path.
//
// This suite runs the real `Permission.Service` node graph and a real
// `MessageChannel` boundary, then asserts:
//   * a DENY rule blocks execution: the plugin tool starts, `ask` rejects with
//     `PermissionDeniedError`, and the worker sees a `HubProxyRemoteError` whose
//     `tag` preserves the domain error (the tool never completes);
//   * an ALLOW rule lets the tool complete and return its result;
//   * in both cases `ask` receives the merged ruleset and the reconstructed call
//     frame (`sessionID` / `tool.messageID` / `tool.callID`).

import { describe, expect } from "bun:test"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import type { ToolContext as PluginToolContext } from "@opencode-ai/plugin"
import { Effect, Layer, Option } from "effect"
import { Agent } from "@/agent/agent"
import { InstanceRef } from "@/effect/instance-ref"
import { EventV2Bridge } from "@/event-v2-bridge"
import { Permission } from "@/permission"
import { Plugin } from "@/plugin"
import { InstanceBootstrap } from "@/project/bootstrap"
import type { InstanceContext } from "@/project/instance-context"
import { InstanceStore } from "@/project/instance-store"
import { Session } from "@/session/session"
import { buildHubProxyHandlers, pluginToolAuthority, type HubProxyRunner } from "@/session/worker/hub"
import { HubProxyRemoteError } from "@/session/worker/hub-proxy-wire"
import { createHubProxyClient, serveHubProxy } from "@/session/worker/layer"
import type { PluginToolExecuteInput } from "@/session/worker/plugin-tool-wire"
import { Truncate } from "@/tool/truncate"
import { testEffect } from "../../lib/effect"
import { TestInstance } from "../../fixture/fixture"
import { instantRunner, recordingHubAuthorities } from "../../lib/hub-proxy"

/**
 * The real `Permission.Service` graph (mirrors `test/permission/next.test.ts`):
 * `Permission` → `EventV2Bridge` → `InstanceStore`, with the bootstrap no-op'd so
 * the suite does not need the full project graph.
 */
const noopBootstrap = Layer.succeed(InstanceBootstrap.Service, InstanceBootstrap.Service.of({ run: Effect.void }))
const env = AppNodeBuilder.build(
  LayerNode.group([Permission.node, EventV2Bridge.node, CrossSpawnSpawner.node, InstanceStore.node]),
  [[InstanceStore.bootstrapNode, noopBootstrap]],
)
const it = testEffect(env)

/** The worker→hub call frame for the plugin tool under test. */
const wireInput: PluginToolExecuteInput = {
  toolID: "compress",
  sessionID: "ses_plugin_permission",
  messageID: "msg_plugin_permission",
  callID: "call_plugin_permission",
  agent: "build",
  args: { range: "*" },
}

type ToolHooks = { readonly tool?: Record<string, unknown> }

/** A `Plugin.Interface` double whose `list()` returns `hooks`. */
function fakePlugin(hooks: readonly ToolHooks[]): Plugin.Interface {
  return {
    trigger: () => Effect.succeed({} as never),
    list: () => Effect.succeed(hooks as never),
    init: () => Effect.void,
  } as unknown as Plugin.Interface
}

/** The outcome of one proxied tool call: the error (if denied) and whether it completed. */
interface ProxiedOutcome {
  readonly asks: ReadonlyArray<PermissionV1.AskInput>
  readonly completed: boolean
  readonly value?: unknown
  readonly error?: unknown
}

interface ProxyToolOptions {
  /** The real hub-resident permission service, wrapped to record every `ask`. */
  readonly permission: Permission.Interface
  /** The hub instance context the authority reconstructs for `PluginToolContext`. */
  readonly instance: InstanceContext
  /** The agent's ruleset; the authority merges it with the (empty) session ruleset. */
  readonly ruleset: PermissionV1.Ruleset
}

/**
 * Stand up the real port, proxy one `pluginTool.execute`, and record every
 * `Permission.ask` the hub authority makes. The plugin tool calls `ctx.ask`
 * before doing any work, so a denied ask must abort before `completed` flips.
 */
async function proxyTool(options: ProxyToolOptions): Promise<ProxiedOutcome> {
  const asks: PermissionV1.AskInput[] = []
  const permission: Permission.Interface = {
    ask: (request) => {
      asks.push(request)
      return options.permission.ask(request)
    },
    reply: (request) => options.permission.reply(request),
    list: () => options.permission.list(),
  }

  let completed = false
  const compress = {
    description: "compress the conversation history",
    args: {},
    execute: async (_args: unknown, ctx: PluginToolContext) => {
      await ctx.ask({ permission: "compress", patterns: ["*"], always: [], metadata: {} })
      completed = true
      return { title: "compressed", output: "ALLOWED", metadata: {} }
    },
  }

  const run: HubProxyRunner = (effect) =>
    Effect.runPromise(
      effect.pipe(
        Effect.provide(
          Layer.mergeAll(
            Layer.succeed(Permission.Service, permission),
            Layer.mock(Truncate.Service, {
              output: (text: string) => Effect.succeed({ content: text, truncated: false as const }),
            }),
            Layer.mock(Agent.Service, {
              get: () => Effect.succeed({ permission: options.ruleset } as unknown as Agent.Info),
            }),
            Layer.mock(Session.Service, { messages: () => Effect.succeed([]), get: () => Effect.succeed({ permission: [] } as unknown as Session.Info) }),
          ),
        ),
        Effect.provideService(InstanceRef, options.instance),
      ),
    )

  const authority = pluginToolAuthority({
    plugin: () => Option.some(fakePlugin([{ tool: { compress } }])),
    run,
  })
  const { authorities } = recordingHubAuthorities({ pluginTool: authority })
  const channel = new MessageChannel()
  const disposeServer = serveHubProxy(buildHubProxyHandlers(authorities, instantRunner), channel.port2)
  const client = createHubProxyClient(channel.port1)
  try {
    try {
      const value = await client.call("pluginTool.execute", [wireInput])
      return { asks, completed, value }
    } catch (error) {
      return { asks, completed, error }
    }
  } finally {
    disposeServer()
    client.dispose()
    channel.port1.close()
    channel.port2.close()
  }
}

/** The instance context the hub authority reconstructs for `PluginToolContext`. */
function hubInstance(directory: string): InstanceContext {
  return {
    directory,
    worktree: directory,
    project: { id: "prj_plugin_permission", worktree: directory, vcs: "git" },
  } as InstanceContext
}

describe("plugin tool permission (hub-resident Permission.Service)", () => {
  it.instance(
    "a DENY rule blocks the tool and preserves the domain error across the port",
    () =>
      Effect.gen(function* () {
        const permission = yield* Permission.Service
        const test = yield* TestInstance
        const outcome = yield* Effect.promise(() =>
          proxyTool({
            permission,
            instance: hubInstance(test.directory),
            ruleset: [{ permission: "compress", pattern: "*", action: "deny" }],
          }),
        )

        // The tool started but never completed: `ask` aborted it.
        expect(outcome.completed).toBe(false)
        expect(outcome.error).toBeInstanceOf(HubProxyRemoteError)
        expect((outcome.error as HubProxyRemoteError).tag).toBe("PermissionDeniedError")

        // `ask` ran on the hub with the merged ruleset and the rebuilt call frame.
        expect(outcome.asks).toHaveLength(1)
        expect(outcome.asks[0]).toMatchObject({
          sessionID: wireInput.sessionID,
          permission: "compress",
          patterns: ["*"],
          tool: { messageID: wireInput.messageID, callID: wireInput.callID },
          ruleset: [{ permission: "compress", pattern: "*", action: "deny" }],
        })
      }),
    { git: true },
  )

  it.instance(
    "an ALLOW rule lets the tool complete and return the hub result",
    () =>
      Effect.gen(function* () {
        const permission = yield* Permission.Service
        const test = yield* TestInstance
        const outcome = yield* Effect.promise(() =>
          proxyTool({
            permission,
            instance: hubInstance(test.directory),
            ruleset: [{ permission: "compress", pattern: "*", action: "allow" }],
          }),
        )

        expect(outcome.error).toBeUndefined()
        expect(outcome.completed).toBe(true)
        expect(outcome.value).toMatchObject({ title: "compressed", output: "ALLOWED" })

        expect(outcome.asks).toHaveLength(1)
        expect(outcome.asks[0].ruleset).toEqual([{ permission: "compress", pattern: "*", action: "allow" }])
      }),
    { git: true },
  )
})
