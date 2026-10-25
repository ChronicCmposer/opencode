// The hub-proxy `messagesTransform` RPC.
//
// A session worker's reduced `Plugin` cannot run `experimental.chat.messages.transform`
// because plugins are loaded on the hub (decision 70). The worker reaches the hub
// over the existing multiplexed control port instead. This suite pins the wire
// contract:
//   * `buildHubProxyHandlers` always serves `messagesTransform`;
//   * a caller-supplied authority runs on the hub and its mutated `{messages}` is
//     returned to the worker;
//   * absent an authority (flag-off / no plugins) the handler returns `output`
//     unchanged — never a transport rejection.

import { describe, expect, test } from "bun:test"
import { buildHubProxyHandlers, type HubProxyAuthorities } from "@/session/worker/hub"
import { createHubProxyClient, serveHubProxy } from "@/session/worker/layer"
import type { MessagesTransformInput, MessagesTransformOutput } from "@/session/worker/plugin-host"

const instantRunner = (effect: unknown): Promise<never> => Promise.resolve(effect as never)

/** The authority map `buildHubProxyHandlers` accepts, with only messagesTransform wired. */
function authorities(
  messagesTransform?: (
    input: MessagesTransformInput,
    output: MessagesTransformOutput,
  ) => Promise<MessagesTransformOutput>,
): HubProxyAuthorities {
  return {
    sessionStatus: {} as never,
    permission: {} as never,
    question: {} as never,
    ...(messagesTransform === undefined ? {} : { messagesTransform }),
  }
}

/** Serve `handlers` on a fresh channel and return the client end. */
function clientFor(handlers: Parameters<typeof serveHubProxy>[0]): ReturnType<typeof createHubProxyClient> {
  const channel = new MessageChannel()
  serveHubProxy(handlers, channel.port2)
  return createHubProxyClient(channel.port1)
}

const input: MessagesTransformInput = { sessionID: "ses_messages" }

describe("hub-proxy messagesTransform RPC", () => {
  test("the hub authority runs and its mutated {messages} returns to the worker", async () => {
    // A fake plugin: appends to the shared messages array, exactly as the real
    // `Plugin.trigger` contract mutates its `output` in place.
    const plugin = async (
      _input: MessagesTransformInput,
      output: MessagesTransformOutput,
    ): Promise<MessagesTransformOutput> => {
      output.messages.push("appended-by-plugin")
      return output
    }
    const client = clientFor(buildHubProxyHandlers(authorities(plugin), instantRunner))

    try {
      const output: MessagesTransformOutput = { messages: ["base"] }
      const returned = (await client.call("messagesTransform", [input, output])) as MessagesTransformOutput

      expect(returned).toEqual({ messages: ["base", "appended-by-plugin"] })
    } finally {
      client.dispose()
    }
  })

  test("absent an authority the handler returns the input unchanged (degraded)", async () => {
    const handlers = buildHubProxyHandlers(authorities(), instantRunner)

    // Identity is the contract: with no authority the handler hands back the very
    // object it was given — no copy, no mutation, so call sites need no
    // reconciliation. Assert at the handler boundary: the RPC reply is
    // structured-cloned, so object identity cannot survive the port.
    const output: MessagesTransformOutput = { messages: ["base"] }
    expect(handlers.messagesTransform?.(input, output)).toBe(output)

    // The wire path still serves the method and round-trips an equivalent value
    // instead of rejecting with "unknown hub-proxy method".
    const client = clientFor(handlers)
    try {
      const returned = (await client.call("messagesTransform", [input, output])) as MessagesTransformOutput
      expect(returned).toEqual({ messages: ["base"] })
    } finally {
      client.dispose()
    }
  })
})
