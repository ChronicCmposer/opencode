// Oversized hook payloads must not fail a turn: undeclared hooks never cross the
// port, and declared ones hold large `data:` URLs back from the frame.

import { describe, expect, test } from "bun:test"
import { createWorkerPluginHost, type WorkerPluginHubHook } from "@/session/worker/plugin-host"
import { MAX_WORKER_FRAME_BYTES } from "@/session/worker/frame"

const declaring = [
  {
    name: "hub",
    hooks: {},
    proxied: ["chat.message", "experimental.chat.messages.transform", "experimental.chat.system.transform"],
  },
]

const fiveMB = `data:image/png;base64,${"A".repeat(5 * 1024 * 1024)}`

/** A hub that, like the real frame, rejects anything over the cap, and tags the text part. */
function cappedHub(calls: string[]): WorkerPluginHubHook {
  return async (name, input, output) => {
    calls.push(name)
    const wire = JSON.stringify({ name, input, output })
    if (Buffer.byteLength(wire) > MAX_WORKER_FRAME_BYTES) throw new Error("frame too large")
    return { ...(output as object), tagged: true }
  }
}

describe("hub hook payload cap", () => {
  test("chat.message with a 5 MB attachment reaches a declaring plugin and keeps the attachment", async () => {
    const calls: string[] = []
    const host = createWorkerPluginHost(declaring, { hubHook: cappedHub(calls) })
    const output = {
      message: { id: "m" },
      parts: [
        { type: "text", text: "see file" },
        { type: "file", url: fiveMB, mime: "image/png" },
      ],
    }
    const result = await host.chatMessage({ sessionID: "s" }, output)
    expect(calls).toEqual(["chat.message"])
    expect((result.parts[1] as { url: string }).url).toBe(fiveMB)
  })

  test("messages.transform over the cap restores the attachment in the returned messages", async () => {
    const calls: string[] = []
    const messagesTransform = async (_input: unknown, output: { messages: unknown[] }) => {
      const wire = JSON.stringify(output)
      if (Buffer.byteLength(wire) > MAX_WORKER_FRAME_BYTES) throw new Error("frame too large")
      calls.push("messages")
      return { messages: structuredClone(output.messages) }
    }
    const host = createWorkerPluginHost(declaring, { messagesTransform: messagesTransform as never })
    const messages = [{ info: { id: "m" }, parts: [{ type: "file", url: fiveMB }] }]
    const result = await host.messagesTransform({}, { messages: messages as never })
    expect(calls).toEqual(["messages"])
    expect((result.messages[0] as unknown as { parts: { url: string }[] }).parts[0].url).toBe(fiveMB)
  })

  test("hooks no plugin declares are never forwarded", async () => {
    const calls: string[] = []
    const host = createWorkerPluginHost([{ name: "other", hooks: {}, proxied: ["shell.env"] }], {
      hubHook: cappedHub(calls),
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
