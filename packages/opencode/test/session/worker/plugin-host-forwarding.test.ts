// Only hooks a hub plugin declares cross the port; oversized declared payloads are
// covered by the hub-proxy spill tests.

import { describe, expect, test } from "bun:test"
import { createWorkerPluginHost, type WorkerPluginHubHook } from "@/session/worker/plugin-host"

const declaring = [
  {
    name: "hub",
    hooks: {},
    proxied: ["chat.message", "experimental.chat.messages.transform", "experimental.chat.system.transform"],
  },
]

const fiveMB = `data:image/png;base64,${"A".repeat(5 * 1024 * 1024)}`

/** A hub that records every forwarded hook. */
function recordingHub(calls: string[]): WorkerPluginHubHook {
  return async (name, _input, output) => {
    calls.push(name)
    return output
  }
}

describe("hub hook forwarding", () => {
  test("hooks no plugin declares are never forwarded", async () => {
    const calls: string[] = []
    const host = createWorkerPluginHost([{ name: "other", hooks: {}, proxied: ["shell.env"] }], {
      hubHook: recordingHub(calls),
      messagesTransform: async () => {
        calls.push("messages")
        return { messages: [] }
      },
    })
    const output = { message: { id: "m" }, parts: [{ type: "file", url: fiveMB }] }
    const result = await host.chatMessage({ sessionID: "s" }, output)
    const messages = { messages: [{ info: {}, parts: [{ url: fiveMB }] }] as never }
    expect(await host.messagesTransform({}, messages)).toBe(messages)
    expect(calls).toEqual([])
    expect((result.parts[0] as { url: string }).url).toBe(fiveMB)
  })

  test("small payloads cross unchanged", async () => {
    let seen: unknown
    const host = createWorkerPluginHost(declaring, {
      hubHook: async (_name, _input, output) => {
        seen = output
        return output
      },
    })
    const output = { message: { id: "m" }, parts: [{ type: "text", text: "hi" }] }
    await host.chatMessage({ sessionID: "s" }, output)
    expect(seen).toEqual(output)
  })
})
