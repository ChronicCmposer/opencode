// messages-transform / ws3 — the worker plugin host proxies
// `experimental.chat.messages.transform` to the hub over the control port.
//
// The hub owns plugin loading, so the messages rewrite runs on the hub and
// returns the mutated `{ messages }` to the isolate. `session-worker.ts` derives
// that bridge (`hubProxyMessagesTransform`) from the shared hub-proxy client;
// these tests pin the bridge and the degraded (no client) pass-through.

import { describe, expect, test } from "bun:test"
import { type HubProxyClient } from "@/session/worker/layer"
import { type MessagesTransformInput, type MessagesTransformOutput } from "@/session/worker/plugin-host"
import { hubProxyMessagesTransform } from "@/session/worker/session-worker"

describe("worker messages.transform hub proxy (messages-transform-ws3)", () => {
  test("forwards the frozen proxy shape and returns the hub's result", async () => {
    const calls: Array<{ method: string; args: readonly unknown[] }> = []
    // A fake hub-proxy client: the hub runs the loaded plugins and returns the
    // mutated messages array. All authorities multiplex one port, so the worker
    // hands this single client to the plugin host.
    const client: HubProxyClient = {
      call: async (method, args) => {
        calls.push({ method, args })
        const [, output] = args as [MessagesTransformInput, MessagesTransformOutput]
        return { messages: [...output.messages, { role: "assistant", content: "hub-added" }] }
      },
      dispose: () => {},
    }
    const messagesTransform = hubProxyMessagesTransform(client)
    expect(messagesTransform).toBeDefined()

    const input: MessagesTransformInput = { sessionID: "ses_1", model: { providerID: "mock", modelID: "m" } }
    const output: MessagesTransformOutput = { messages: [{ role: "user", content: "base" }] }
    const result = await messagesTransform!(input, output)

    expect(result).toEqual({
      messages: [
        { role: "user", content: "base" },
        { role: "assistant", content: "hub-added" },
      ],
    })
    // The worker sent the exact frozen proxy shape: method + [input, output].
    expect(calls).toEqual([{ method: "messagesTransform", args: [input, output] }])
  })

  test("the bridge is undefined without a hub client (degraded boot)", () => {
    expect(hubProxyMessagesTransform(undefined)).toBeUndefined()
  })
})
