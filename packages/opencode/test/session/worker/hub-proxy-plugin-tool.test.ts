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
import { buildHubProxyHandlers, buildHubProxyStreamHandlers, type HubProxyAuthorities } from "@/session/worker/hub"
import { createHubProxyClient, serveHubProxy } from "@/session/worker/layer"
import { HubProxyRemoteError } from "@/session/worker/hub-proxy-wire"
import type {
  PluginToolDescriptor,
  PluginToolExecuteInput,
  PluginToolExecuteResult,
  PluginToolProgress,
} from "@/session/worker/plugin-tool-wire"

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

/** Serve `handlers` + `streaming` on a fresh channel and return the client end. */
function streamingClientFor(
  handlers: Parameters<typeof serveHubProxy>[0],
  streaming: Parameters<typeof serveHubProxy>[2],
): ReturnType<typeof createHubProxyClient> {
  const channel = new MessageChannel()
  serveHubProxy(handlers, channel.port2, streaming)
  return createHubProxyClient(channel.port1)
}

const descriptors: readonly PluginToolDescriptor[] = [
  { id: "compress", description: "compress a range", jsonSchema: { type: "object" } },
]

describe("hub-proxy pluginTool.list RPC", () => {
  test("the list authority's descriptors return to the worker", async () => {
    const map: HubProxyAuthorities = {
      sessionStatus: {} as never,
      permission: {} as never,
      question: {} as never,
      pluginToolList: async () => descriptors,
    }
    const client = clientFor(buildHubProxyHandlers(map, instantRunner))
    try {
      expect(await client.call("pluginTool.list", [])).toEqual(descriptors)
    } finally {
      client.dispose()
    }
  })

  test("absent a list authority the method is served and rejects descriptively", async () => {
    const handlers = buildHubProxyHandlers(authorities(), instantRunner)
    expect(Object.hasOwn(handlers, "pluginTool.list")).toBe(true)

    const client = clientFor(handlers)
    try {
      let caught: unknown
      try {
        await client.call("pluginTool.list", [])
      } catch (error) {
        caught = error
      }
      expect(caught).toBeInstanceOf(HubProxyRemoteError)
      expect((caught as Error).message).toContain("pluginTool")
      expect((caught as Error).message).not.toContain("unknown hub-proxy method")
    } finally {
      client.dispose()
    }
  })
})

describe("hub-proxy pluginTool.abort RPC", () => {
  test("the abort authority receives the {sessionID, callID} pair", async () => {
    const aborts: { sessionID: string; callID: string }[] = []
    const map: HubProxyAuthorities = {
      sessionStatus: {} as never,
      permission: {} as never,
      question: {} as never,
      pluginToolAbort: (abortInput) => {
        aborts.push(abortInput)
      },
    }
    const client = clientFor(buildHubProxyHandlers(map, instantRunner))
    try {
      await client.call("pluginTool.abort", [{ sessionID: "ses_1", callID: "call_1" }])
      expect(aborts).toEqual([{ sessionID: "ses_1", callID: "call_1" }])
    } finally {
      client.dispose()
    }
  })

  test("absent an abort authority the method is served as a no-op", async () => {
    const handlers = buildHubProxyHandlers(authorities(), instantRunner)
    expect(Object.hasOwn(handlers, "pluginTool.abort")).toBe(true)

    const client = clientFor(handlers)
    try {
      // Best-effort: nothing to abort is not an error (unlike a missing execute).
      expect(await client.call("pluginTool.abort", [{ sessionID: "ses_1", callID: "call_1" }])).toBeNull()
    } finally {
      client.dispose()
    }
  })
})

describe("hub-proxy streaming pluginTool.metadata", () => {
  test("the authority's emit surfaces as pluginTool.metadata events before the reply", async () => {
    const authority = async (
      received: PluginToolExecuteInput,
      emit: (progress: PluginToolProgress) => void,
    ): Promise<PluginToolExecuteResult> => {
      emit({ title: "step 1" })
      emit({ title: "step 2", metadata: { sessionID: received.sessionID } })
      return { title: "done", output: "HUB_RAN", metadata: {} }
    }
    const map: HubProxyAuthorities = {
      sessionStatus: {} as never,
      permission: {} as never,
      question: {} as never,
      pluginTool: authority,
    }
    const handlers = buildHubProxyHandlers(map, instantRunner)
    const streaming = buildHubProxyStreamHandlers(map)
    const client = streamingClientFor(handlers, streaming)

    const seen: [string, unknown][] = []
    try {
      const returned = (await client.call("pluginTool.execute", [input], (method, payload) =>
        seen.push([method, payload]),
      )) as PluginToolExecuteResult

      expect(returned).toEqual({ title: "done", output: "HUB_RAN", metadata: {} })
      expect(seen).toEqual([
        ["pluginTool.metadata", { title: "step 1" }],
        ["pluginTool.metadata", { title: "step 2", metadata: { sessionID: "ses_plugin_tool" } }],
      ])
    } finally {
      client.dispose()
    }
  })
})
