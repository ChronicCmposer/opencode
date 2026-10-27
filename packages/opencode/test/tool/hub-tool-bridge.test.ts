import { describe, expect, test } from "bun:test"
import { Context, Effect, Layer, Option } from "effect"
import { Agent } from "@/agent/agent"
import { MCP } from "@/mcp"
import { Permission } from "@/permission"
import { Session } from "@/session/session"
import { resolveContextHubToolDescriptors, pluginToolAuthority, type HubProxyRunner } from "@/session/worker/hub"
import type { PluginToolExecuteInput } from "@/session/worker/plugin-tool-wire"
import { Plugin } from "@/plugin"
import { Truncate } from "@/tool/truncate"

const plugin = {
  trigger: () => Effect.succeed({} as never),
  list: () => Effect.succeed([] as never),
  init: () => Effect.void,
} as unknown as Plugin.Interface

const mcpCalls: Array<{ name: string; args: unknown }> = []
const mcpTool = {
  def: {
    name: "mcp_search",
    description: "search the connected MCP server",
    inputSchema: { type: "object", properties: { q: { type: "string" } }, required: ["q"] },
  },
  client: {
    callTool: async (request: { name: string; arguments: Record<string, unknown> }) => {
      mcpCalls.push({ name: request.name, args: request.arguments })
      return { content: [{ type: "text", text: `searched:${String(request.arguments.q)}` }] }
    },
  },
}

const mcp = MCP.Service.of({
  tools: () => Effect.succeed({ mcp_search: mcpTool }),
  clients: () => Effect.succeed({}),
} as unknown as MCP.Interface)

const asked: unknown[] = []
const services = Layer.mergeAll(
  Layer.mock(Permission.Service, {
    ask: (input: unknown) => Effect.sync(() => void asked.push(input)),
  }),
  Layer.mock(Truncate.Service, {
    output: (text: string) => Effect.succeed({ content: text, truncated: false as const }),
  }),
  Layer.mock(Agent.Service, { get: () => Effect.succeed({ permission: [] } as unknown as Agent.Info) }),
  Layer.mock(Session.Service, { get: () => Effect.succeed({ permission: [] } as unknown as Session.Info) }),
  Layer.succeed(MCP.Service, mcp),
)

const run: HubProxyRunner = (effect) => Effect.runPromise(Effect.provide(effect, services))

const input: PluginToolExecuteInput = {
  toolID: "mcp_search",
  sessionID: "ses_bridge",
  messageID: "msg_bridge",
  callID: "call_bridge",
  agent: "build",
  args: { q: "needle" },
}

describe("MCP/LSP hub bridge", () => {
  test("the hub ships MCP and LSP tool descriptors to the worker", async () => {
    const context = Context.make(Plugin.Service, plugin).pipe(Context.add(MCP.Service, mcp))

    const descriptors = await resolveContextHubToolDescriptors(context as unknown as Context.Context<never>)
    const ids = descriptors.map((descriptor) => descriptor.id)

    expect(ids).toContain("mcp_search")
    expect(ids).toContain("lsp")
    const search = descriptors.find((descriptor) => descriptor.id === "mcp_search")
    expect(search?.description).toBe("search the connected MCP server")
    expect(search?.jsonSchema).toMatchObject({ type: "object" })
    const lsp = descriptors.find((descriptor) => descriptor.id === "lsp")
    expect(lsp?.jsonSchema).toMatchObject({ type: "object" })
  })

  test("a worker invocation of an MCP id executes on the hub", async () => {
    const authority = pluginToolAuthority({
      plugin: () => Option.some(plugin),
      run,
    })

    const result = await authority(input)

    expect(mcpCalls).toEqual([{ name: "mcp_search", args: { q: "needle" } }])
    expect(result.output).toBe("searched:needle")
    expect(asked).toHaveLength(1)
  })

  test("a non-MCP, non-LSP id still rejects fail-loud", async () => {
    const authority = pluginToolAuthority({ plugin: () => Option.some(plugin), run })

    await expect(authority({ ...input, toolID: "not_a_tool" })).rejects.toThrow(/unknown plugin tool/)
  })
})
