// The hub-proxy streaming event channel (`onEvent`) + the plugin-tool control
// methods.
//
// A hub-side handler for a long-running request (e.g. `pluginTool.execute`,
// `pluginTool.list`) may need to push progress back to the awaiting worker
// before it replies. The multiplexed control port already correlates replies by
// numeric id; this suite pins the unsolicited `kind: "event"` frame that reuses
// the same id:
//   * events emitted by a streaming handler reach the caller's `onEvent` in
//     order, before the reply settles;
//   * an event for an unknown or already-settled id is dropped silently;
//   * a two-argument `call` (no `onEvent`) keeps the historical behaviour, even
//     against a streaming handler;
//   * `pluginTool.list` / `pluginTool.abort` are first-class wire methods.

import { describe, expect, test } from "bun:test"
import {
  createHubProxyClient,
  serveHubProxy,
  type HubProxyEventSink,
  type HubProxyMethod,
  type HubProxyStreamHandler,
} from "@/session/worker/hub-proxy-wire"
import { WORKER_FRAME_VERSION } from "@/session/worker/frame"

type SeenEvent = [method: string, payload: unknown]

describe("hub-proxy streaming events", () => {
  test("a streaming handler's events reach with their onEvent in order, and the reply resolves to its value", async () => {
    const channel = new MessageChannel()
    const streaming: Readonly<Record<string, HubProxyStreamHandler>> = {
      "pluginTool.list": (sink, args) => {
        sink.emit("pluginTool.progress", { step: 1 })
        sink.emit("pluginTool.progress", { step: 2 })
        return { ok: true, args }
      },
    }
    serveHubProxy({}, channel.port2, streaming)
    const client = createHubProxyClient(channel.port1)
    const seen: SeenEvent[] = []
    try {
      const value = await client.call("pluginTool.list", [], (method, payload) => seen.push([method, payload]))

      expect(value).toEqual({ ok: true, args: [] })
      // Both events must have been routed before the reply settled: a reply-first
      // ordering would have unregistered the handler and dropped them.
      expect(seen).toEqual([
        ["pluginTool.progress", { step: 1 }],
        ["pluginTool.progress", { step: 2 }],
      ])
    } finally {
      client.dispose()
    }
  })

  test("events for an unknown or settled request id are dropped without throwing", async () => {
    const channel = new MessageChannel()
    let capturedSink: HubProxyEventSink | undefined
    serveHubProxy({}, channel.port2, {
      "pluginTool.list": (sink) => {
        capturedSink = sink
        return { done: true }
      },
    })
    const client = createHubProxyClient(channel.port1)
    const seen: SeenEvent[] = []
    try {
      const value = await client.call("pluginTool.list", [], (method, payload) => seen.push([method, payload]))
      expect(value).toEqual({ done: true })
      expect(seen).toEqual([])

      // The request is settled; an event on its id must be dropped.
      capturedSink?.emit("late", { leaked: true })

      // An event frame naming an id that was never requested must be dropped too.
      channel.port2.postMessage(
        JSON.stringify({
          v: WORKER_FRAME_VERSION,
          port: "hub-proxy",
          kind: "event",
          id: 987654,
          payload: { kind: "event", id: 987654, method: "stray", payload: { leaked: true } },
        }),
      )

      // A later call on the same channel still resolves and sees only its own
      // events: the two dropped frames neither threw nor leaked.
      const laterSeen: SeenEvent[] = []
      const later = await client.call("pluginTool.list", [], (method, payload) => laterSeen.push([method, payload]))
      expect(later).toEqual({ done: true })
      expect(laterSeen).toEqual([])
      expect(seen).toEqual([])
    } finally {
      client.dispose()
    }
  })

  test("call without onEvent keeps the two-argument (non-streaming) behaviour", async () => {
    const channel = new MessageChannel()
    serveHubProxy({ messagesTransform: (_input, output) => output }, channel.port2)
    const client = createHubProxyClient(channel.port1)
    try {
      const returned = (await client.call("messagesTransform", [{}, { messages: ["base"] }])) as {
        messages: readonly string[]
      }
      expect(returned).toEqual({ messages: ["base"] })
    } finally {
      client.dispose()
    }
  })

  test("a streaming handler called without onEvent still resolves and drops its events", async () => {
    const channel = new MessageChannel()
    serveHubProxy({}, channel.port2, {
      "pluginTool.list": (sink) => {
        sink.emit("pluginTool.progress", { ignored: true })
        return { done: true }
      },
    })
    const client = createHubProxyClient(channel.port1)
    try {
      const value = await client.call("pluginTool.list", [])
      expect(value).toEqual({ done: true })
    } finally {
      client.dispose()
    }
  })

  test("pluginTool.list and pluginTool.abort dispatch through the wire", async () => {
    const methods: readonly HubProxyMethod[] = ["pluginTool.list", "pluginTool.abort"]
    const channel = new MessageChannel()
    serveHubProxy(
      {
        "pluginTool.list": () => [{ id: "compress" }],
        "pluginTool.abort": (sessionID) => ({ aborted: sessionID }),
      },
      channel.port2,
    )
    const client = createHubProxyClient(channel.port1)
    try {
      expect(methods).toHaveLength(2)
      expect(await client.call("pluginTool.list", [])).toEqual([{ id: "compress" }])
      expect(await client.call("pluginTool.abort", ["ses_1"])).toEqual({ aborted: "ses_1" })
    } finally {
      client.dispose()
    }
  })
})
