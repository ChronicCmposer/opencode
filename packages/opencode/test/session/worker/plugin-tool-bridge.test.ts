// W — the worker-side plugin-tool bridge follow-ups: progress forwarding, the
// abort bridge and the descriptor-list seam.
//
// The worker's real plugin-tool executor proxies a call to the hub over the
// multiplexed control port. These tests pin the three follow-up behaviours
// against a recording `HubProxyClient` (no real port needed):
//   * `hubProxyPluginTool` forwards hub `pluginTool.metadata` event frames to the
//     caller's `onProgress` sink and drops every other event method;
//   * `hubProxyPluginToolAbort` calls `pluginTool.abort` with the call frame;
//   * `hubProxyPluginToolList` calls `pluginTool.list` and returns descriptors;
//   * all three degrade to `undefined` without a hub client.

import { describe, expect, test } from "bun:test"
import type { HubProxyClient, HubProxyMethod } from "@/session/worker/hub-proxy-wire"
import {
  hubProxyPluginTool,
  hubProxyPluginToolAbort,
  hubProxyPluginToolList,
} from "@/session/worker/session-worker"
import type {
  PluginToolDescriptor,
  PluginToolExecuteInput,
  PluginToolExecuteResult,
  PluginToolProgress,
} from "@/session/worker/plugin-tool-wire"

/** One call observed by the fake client. */
interface RecordedCall {
  readonly method: HubProxyMethod
  readonly args: readonly unknown[]
}

/** One scripted hub event a fake `pluginTool.execute` replays to `onEvent`. */
interface ScriptedEvent {
  readonly method: string
  readonly payload: unknown
}

/**
 * A fake `HubProxyClient` that records every call and, for `pluginTool.execute`,
 * replays the scripted event frames to `onEvent` before resolving the result.
 */
function fakeHubProxy(
  options: {
    readonly result?: PluginToolExecuteResult
    readonly events?: ReadonlyArray<ScriptedEvent>
    readonly descriptors?: readonly PluginToolDescriptor[]
  } = {},
): { readonly client: HubProxyClient; readonly calls: RecordedCall[] } {
  const calls: RecordedCall[] = []
  const client: HubProxyClient = {
    call: async (method, args, onEvent) => {
      calls.push({ method, args })
      switch (method) {
        case "pluginTool.execute":
          for (const event of options.events ?? []) onEvent?.(event.method, event.payload)
          return options.result ?? { title: "remote", output: "HUB_OUTPUT", metadata: {} }
        case "pluginTool.list":
          return options.descriptors ?? []
        case "pluginTool.abort":
          return null
        default:
          return null
      }
    },
    dispose: () => {},
  }
  return { client, calls }
}

const input: PluginToolExecuteInput = {
  toolID: "compress",
  sessionID: "ses_bridge",
  messageID: "msg_bridge",
  callID: "call_bridge",
  agent: "build",
  args: { reason: "prune" },
}

describe("hubProxyPluginTool progress forwarding", () => {
  test("forwards pluginTool.metadata events to onProgress and drops other methods", async () => {
    const progress: PluginToolProgress[] = []
    const { client, calls } = fakeHubProxy({
      result: { title: "remote", output: "HUB_DONE", metadata: { finished: true } },
      events: [
        { method: "pluginTool.metadata", payload: { title: "working" } },
        { method: "pluginTool.other", payload: { ignored: true } },
        { method: "pluginTool.metadata", payload: { metadata: { percent: 50 } } },
      ],
    })

    const executor = hubProxyPluginTool(client)
    if (executor === undefined) throw new Error("expected an executor when a hub client is wired")

    const result = await executor.execute(input, (update) => progress.push(update))

    expect(calls).toEqual([{ method: "pluginTool.execute", args: [input] }])
    expect(result).toEqual({ title: "remote", output: "HUB_DONE", metadata: { finished: true } })
    expect(progress).toEqual([{ title: "working" }, { metadata: { percent: 50 } }])
  })

  test("executes without a progress sink (the sink is optional)", async () => {
    const { client, calls } = fakeHubProxy({ events: [{ method: "pluginTool.metadata", payload: { title: "x" } }] })

    const executor = hubProxyPluginTool(client)
    if (executor === undefined) throw new Error("expected an executor when a hub client is wired")

    await executor.execute(input)

    expect(calls).toEqual([{ method: "pluginTool.execute", args: [input] }])
  })
})

describe("hubProxyPluginToolAbort", () => {
  test("calls pluginTool.abort with the (sessionID, callID) frame", async () => {
    const { client, calls } = fakeHubProxy()

    const abort = hubProxyPluginToolAbort(client)
    if (abort === undefined) throw new Error("expected an abort bridge when a hub client is wired")

    await abort({ sessionID: "ses_bridge", callID: "call_bridge" })

    expect(calls).toEqual([{ method: "pluginTool.abort", args: [{ sessionID: "ses_bridge", callID: "call_bridge" }] }])
  })
})

describe("hubProxyPluginToolList", () => {
  test("calls pluginTool.list and returns the descriptors", async () => {
    const descriptors: PluginToolDescriptor[] = [
      { id: "compress", description: "compress tool", jsonSchema: { type: "object" } },
    ]
    const { client, calls } = fakeHubProxy({ descriptors })

    const list = hubProxyPluginToolList(client)
    if (list === undefined) throw new Error("expected a list bridge when a hub client is wired")

    expect(await list()).toEqual(descriptors)
    expect(calls).toEqual([{ method: "pluginTool.list", args: [] }])
  })
})

describe("degraded boot", () => {
  test("every bridge is undefined without a hub client", () => {
    expect(hubProxyPluginTool(undefined)).toBeUndefined()
    expect(hubProxyPluginToolAbort(undefined)).toBeUndefined()
    expect(hubProxyPluginToolList(undefined)).toBeUndefined()
  })
})
