// `pluginToolAuthority` + `resolveContextPluginToolDescriptors` — the hub half of
// plugin-provided tools.
//
// `buildHubProxyHandlers` serves the wire (see `hub-proxy-plugin-tool.test.ts`);
// this suite pins the two resolution helpers:
//   * `pluginToolAuthority` finds the hub-owned definition by id among
//     `Plugin.list()` → `tool` entries, runs it with a reconstructed
//     `PluginToolContext`, and returns the normalized, bounded result;
//   * an unknown id REJECTS fail-loud rather than returning an empty success;
//   * `resolveContextPluginToolDescriptors` maps `Plugin.list()` tool entries to
//     the clone-safe `{ id, description, jsonSchema }` descriptors the worker
//     init ships.

import { describe, expect, test } from "bun:test"
import { Context, Effect, Layer, Option } from "effect"
import { pluginToolAuthority, resolveContextPluginToolDescriptors, type HubProxyRunner } from "@/session/worker/hub"
import { Plugin } from "@/plugin"
import { Permission } from "@/permission"
import { Session } from "@/session/session"
import { Truncate } from "@/tool/truncate"
import { Agent } from "@/agent/agent"
import type { ToolContext as PluginToolContext } from "@opencode-ai/plugin"
import type { PluginToolExecuteInput } from "@/session/worker/plugin-tool-wire"
import { Provider } from "@/provider/provider"

/** The session history the hub reads for the plugin's `messages`. */
const history = [{ info: { id: "msg_previous" }, parts: [] }]

/** The hub-resident services the authority reads from the captured context. */
const services = Layer.mergeAll(
  Layer.mock(Permission.Service, { ask: () => Effect.void }),
  Layer.mock(Truncate.Service, {
    output: (text: string) => Effect.succeed({ content: text, truncated: false as const }),
  }),
  Layer.mock(Agent.Service, { get: () => Effect.succeed({ permission: [] } as unknown as Agent.Info) }),
  Layer.mock(Session.Service, {
    get: () => Effect.succeed({ permission: [] } as unknown as Session.Info),
    messages: () => Effect.succeed(history as never),
  }),
  Layer.mock(Provider.Service, {
    getModel: (providerID: string, modelID: string) => Effect.succeed({ providerID, id: modelID } as never),
  }),
)

/** Run a hub-authority effect with the hub-resident services provided. */
const run: HubProxyRunner = (effect) => Effect.runPromise(Effect.provide(effect, services))

type ToolHooks = { readonly tool?: Record<string, unknown> }

/** A `Plugin.Interface` double whose `list()` returns `hooks`. */
function fakePlugin(hooks: readonly ToolHooks[]): Plugin.Interface {
  return {
    trigger: () => Effect.succeed({} as never),
    list: () => Effect.succeed(hooks as never),
    init: () => Effect.void,
  } as unknown as Plugin.Interface
}

const input: PluginToolExecuteInput = {
  toolID: "compress",
  sessionID: "ses_authority",
  messageID: "msg_authority",
  callID: "call_authority",
  agent: "build",
  args: { range: "*" },
}

describe("pluginToolAuthority", () => {
  test("find-by-id: runs the hub definition with a reconstructed context and normalizes the result", async () => {
    let seen: PluginToolContext | undefined
    const compress = {
      description: "compress a range",
      args: {},
      execute: async (_args: unknown, ctx: PluginToolContext) => {
        seen = ctx
        return { title: "compressed", output: "HUB_RAN", metadata: { sessionID: ctx.sessionID } }
      },
    }
    const authority = pluginToolAuthority({
      plugin: () => Option.some(fakePlugin([{ tool: { compress } }])),
      run,
    })

    const result = await authority(input)

    expect(result.title).toBe("compressed")
    expect(result.output).toBe("HUB_RAN")
    // The hub bounded the result: `truncated` is stamped by `Truncate.boundResult`.
    expect(result.metadata).toEqual({ sessionID: "ses_authority", truncated: false })
    // The reconstructed context carries the call frame the worker shipped.
    expect(seen?.sessionID).toBe("ses_authority")
    expect(seen?.messageID).toBe("msg_authority")
    expect(seen?.agent).toBe("build")
    expect(seen?.abort).toBeInstanceOf(AbortSignal)
  })

  test("the context carries callID, messages and extra like the in-process registry spread", async () => {
    let seen: (PluginToolContext & { callID?: string; messages?: unknown; extra?: Record<string, unknown> }) | undefined
    const compress = {
      description: "compress a range",
      args: {},
      execute: async (_args: unknown, ctx: PluginToolContext) => {
        seen = ctx
        return "ok"
      },
    }
    const authority = pluginToolAuthority({
      plugin: () => Option.some(fakePlugin([{ tool: { compress } }])),
      run,
    })

    await authority({ ...input, extra: { modelRef: { providerID: "p1", modelID: "m1" }, bypassAgentCheck: true } })

    expect(seen?.callID).toBe("call_authority")
    expect(seen?.messages).toEqual(history)
    expect(seen?.extra).toEqual({ model: { providerID: "p1", id: "m1" }, bypassAgentCheck: true })
  })

  test("missing-id: rejects fail-loud instead of an empty success", async () => {
    const authority = pluginToolAuthority({
      plugin: () => Option.some(fakePlugin([{ tool: { compress: { description: "d", args: {}, execute: async () => "" } } }])),
      run,
    })

    await expect(authority({ ...input, toolID: "not-a-tool" })).rejects.toThrow(/unknown plugin tool/)
  })

  test("no plugin at all: rejects fail-loud", async () => {
    const authority = pluginToolAuthority({ plugin: () => Option.none(), run })

    await expect(authority(input)).rejects.toThrow(/unknown plugin tool/)
  })
})

describe("resolveContextPluginToolDescriptors", () => {
  test("maps every plugin tool entry to a clone-safe descriptor", async () => {
    const plugin = fakePlugin([
      {
        tool: {
          compress: { description: "compress a range", args: {}, execute: async () => "" },
          prune: { description: "prune context", args: {}, execute: async () => "" },
        },
      },
    ])
    const context = Context.make(Plugin.Service, plugin) as unknown as Context.Context<never>

    const descriptors = await Effect.runPromise(resolveContextPluginToolDescriptors(context))

    expect(descriptors.map((descriptor) => descriptor.id)).toEqual(["compress", "prune"])
    expect(descriptors[0]?.description).toBe("compress a range")
    expect(descriptors[0]?.jsonSchema).toMatchObject({ type: "object" })
  })

  test("no context or no plugin: resolves to no descriptors", async () => {
    expect(await Effect.runPromise(resolveContextPluginToolDescriptors(undefined))).toEqual([])

    const context = Context.make(Plugin.Service, fakePlugin([])) as unknown as Context.Context<never>
    expect(await Effect.runPromise(resolveContextPluginToolDescriptors(context))).toEqual([])
  })
})
