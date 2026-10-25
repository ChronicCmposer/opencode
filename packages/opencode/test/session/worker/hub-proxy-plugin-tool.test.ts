// The hub-proxy `pluginTool.execute` RPC.
//
// A session worker cannot run a plugin-provided tool itself: the plugin instance
// (and therefore its `execute`) lives on the hub (decision 70). The worker ships
// only clone-safe descriptors and forwards each call to the hub over the existing
// multiplexed control port. This suite pins the wire contract:
//   * `buildHubProxyHandlers` always serves `pluginTool.execute`;
//   * a caller-supplied authority runs on the hub and its clone-safe result
//     returns to the worker;
//   * absent an authority the method is STILL served and REJECTS with a
//     descriptive error — never an "unknown hub-proxy method" (there is no
//     identity pass-through for an execute, unlike the transforms).

import { describe, expect, test } from "bun:test"
import { buildHubProxyHandlers, type HubProxyAuthorities } from "@/session/worker/hub"
import { createHubProxyClient, serveHubProxy } from "@/session/worker/layer"
import { HubProxyRemoteError } from "@/session/worker/hub-proxy-wire"
import type { PluginToolExecuteInput, PluginToolExecuteResult } from "@/session/worker/plugin-tool-wire"

const instantRunner = (effect: unknown): Promise<never> => Promise.resolve(effect as never)

/** The authority map `buildHubProxyHandlers` accepts, with only pluginTool wired. */
function authorities(
  pluginTool?: (input: PluginToolExecuteInput) => Promise<PluginToolExecuteResult>,
): HubProxyAuthorities {
  return {
    sessionStatus: {} as never,
    permission: {} as never,
    question: {} as never,
    ...(pluginTool === undefined ? {} : { pluginTool }),
  }
}

/** Serve `handlers` on a fresh channel and return the client end. */
function clientFor(handlers: Parameters<typeof serveHubProxy>[0]): ReturnType<typeof createHubProxyClient> {
  const channel = new MessageChannel()
  serveHubProxy(handlers, channel.port2)
  return createHubProxyClient(channel.port1)
}

const input: PluginToolExecuteInput = {
  toolID: "compress",
  sessionID: "ses_plugin_tool",
  messageID: "msg_plugin_tool",
  callID: "call_plugin_tool",
  agent: "build",
  args: { range: "*" },
}

describe("hub-proxy pluginTool.execute RPC", () => {
  test("the hub authority runs and its clone-safe result returns to the worker", async () => {
    const authority = async (received: PluginToolExecuteInput): Promise<PluginToolExecuteResult> => ({
      title: `ran ${received.toolID}`,
      output: `HUB:${received.sessionID}`,
      metadata: { args: received.args },
    })
    const client = clientFor(buildHubProxyHandlers(authorities(authority), instantRunner))

    try {
      // The string literal type-checks: `"pluginTool.execute"` is part of `HubProxyMethod`.
      const returned = (await client.call("pluginTool.execute", [input])) as PluginToolExecuteResult

      expect(returned).toEqual({
        title: "ran compress",
        output: "HUB:ses_plugin_tool",
        metadata: { args: { range: "*" } },
      })
    } finally {
      client.dispose()
    }
  })

  test("absent an authority the method is served and rejects descriptively (not unknown method)", async () => {
    const handlers = buildHubProxyHandlers(authorities(), instantRunner)

    // The handler exists — the failure is an authority miss, not a missing method.
    expect(Object.hasOwn(handlers, "pluginTool.execute")).toBe(true)

    const client = clientFor(handlers)
    try {
      let caught: unknown
      try {
        await client.call("pluginTool.execute", [input])
      } catch (error) {
        caught = error
      }

      expect(caught).toBeInstanceOf(HubProxyRemoteError)
      const message = (caught as Error).message
      expect(message).toContain("pluginTool")
      expect(message).not.toContain("unknown hub-proxy method")
    } finally {
      client.dispose()
    }
  })
})
