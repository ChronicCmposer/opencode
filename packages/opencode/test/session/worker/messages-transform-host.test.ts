// A1 — worker plugin host: `experimental.chat.messages.transform` hub-proxy tests.
//
// Mirrors the `experimental.chat.system.transform` cases in
// `layer-plugin.test.ts`: plugin function hooks run first (registration order),
// then the injected `options.messagesTransform`; absent both, the input passes
// through by identity.

import { describe, expect, test } from "bun:test"
import {
  createWorkerPluginHost,
  type MessagesTransformInput,
  type MessagesTransformOutput,
  type WorkerPlugin,
} from "@/session/worker/plugin-host"

describe("worker plugin host: messages.transform", () => {
  test("the injected options.messagesTransform runs after the plugin function hooks", async () => {
    const order: string[] = []
    const host = createWorkerPluginHost(
      [
        {
          name: "prune",
          hooks: {
            "experimental.chat.messages.transform": (_input, output) => {
              order.push("plugin")
              output.messages = [...output.messages, { role: "user", content: "plugin" }]
            },
          },
        },
      ],
      {
        messagesTransform: async (_input, output) => {
          order.push("options")
          return { messages: [...output.messages, { role: "user", content: "options" }] }
        },
      },
    )

    const input: MessagesTransformInput = { sessionID: "s", model: {} }
    const result = await host.messagesTransform(input, { messages: [{ role: "system", content: "base" }] })

    expect(order).toEqual(["plugin", "options"])
    expect(result.messages).toEqual([
      { role: "system", content: "base" },
      { role: "user", content: "plugin" },
      { role: "user", content: "options" },
    ])
  })

  test("absent hook and absent option returns the input object by identity", async () => {
    const output = { messages: [{ role: "system", content: "base" }] }
    const host = createWorkerPluginHost([{ name: "empty", hooks: {} }])
    const result = await host.messagesTransform({}, output)
    expect(result).toBe(output)
    expect(result.messages).toBe(output.messages)
  })

  test("a typed function hook reads input.sessionID and its output.messages mutation flows through", async () => {
    const seenSessionID: string[] = []
    const hook: (
      input: MessagesTransformInput,
      output: MessagesTransformOutput,
    ) => void = (input, output) => {
      if (input.sessionID !== undefined) seenSessionID.push(input.sessionID)
      output.messages.push({ role: "user", content: "in-place" })
    }
    const plugin: WorkerPlugin = {
      name: "mutate",
      hooks: { "experimental.chat.messages.transform": hook },
    }
    const host = createWorkerPluginHost([plugin])

    const output: MessagesTransformOutput = { messages: [{ role: "system", content: "base" }] }
    const result = await host.messagesTransform({ sessionID: "s1", model: {} }, output)

    expect(seenSessionID).toEqual(["s1"])
    expect(result.messages).toEqual([
      { role: "system", content: "base" },
      { role: "user", content: "in-place" },
    ])
  })
})
