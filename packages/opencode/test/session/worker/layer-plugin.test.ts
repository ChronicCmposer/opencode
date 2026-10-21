// Phase 6.3 — worker plugin host tests (`chat.params` / `chat.headers` in-worker).

import { describe, expect, test } from "bun:test"
import { DuplicatePluginError, createWorkerPluginHost, type WorkerPlugin } from "@/session/worker/plugin-host"

describe("worker plugin host", () => {
  test("chat.params hooks run in registration order and compose mutations", async () => {
    const order: string[] = []
    const plugins: WorkerPlugin[] = [
      {
        name: "a",
        hooks: {
          "chat.params": (_input, output) => {
            order.push("a")
            output.temperature = 0.5
          },
        },
      },
      {
        name: "b",
        hooks: {
          "chat.params": (_input, output) => {
            order.push("b")
            output.options = { ...output.options, reasoning: true }
          },
        },
      },
    ]
    const host = createWorkerPluginHost(plugins)
    const result = await host.chatParams(
      { sessionID: "s", agent: "build" },
      { temperature: 0, topP: 1, topK: 1, maxOutputTokens: undefined, options: {} },
    )

    expect(order).toEqual(["a", "b"])
    expect(result.temperature).toBe(0.5)
    expect(result.options).toEqual({ reasoning: true })
    expect(host.names).toEqual(["a", "b"])
  })

  test("chat.headers merges plugin headers (later wins)", async () => {
    const host = createWorkerPluginHost([
      { name: "a", hooks: { "chat.headers": (_input, output) => void (output.headers["x-a"] = "1") } },
      {
        name: "b",
        hooks: {
          "chat.headers": (_input, output) => {
            output.headers["x-a"] = "2"
            output.headers["x-b"] = "3"
          },
        },
      },
    ])
    const result = await host.chatHeaders({}, { headers: {} })
    expect(result.headers).toEqual({ "x-a": "2", "x-b": "3" })
  })

  test("tool.execute.before/after hooks mutate args and result", async () => {
    const host = createWorkerPluginHost([
      {
        name: "rewrite",
        hooks: {
          "tool.execute.before": (_input, output) => {
            output.args = { ...(output.args as object), injected: true }
          },
          "tool.execute.after": (_input, output) => {
            output.output = `${output.output}!`
          },
        },
      },
    ])

    const before = await host.toolBefore({ tool: "bash" }, { args: { command: "ls" } })
    expect(before.args).toEqual({ command: "ls", injected: true })

    const after = await host.toolAfter({ tool: "bash" }, { title: "t", output: "done", metadata: {} })
    expect(after.output).toBe("done!")
  })

  test("a throwing hook aborts the chain (fail loud)", async () => {
    const host = createWorkerPluginHost([
      { name: "bad", hooks: { "chat.params": () => Promise.reject(new Error("hook exploded")) } },
    ])
    await expect(
      host.chatParams({}, { temperature: 0, topP: 1, topK: 1, maxOutputTokens: undefined, options: {} }),
    ).rejects.toThrow("hook exploded")
  })

  test("a plugin with no matching hook is a no-op", async () => {
    const host = createWorkerPluginHost([{ name: "empty", hooks: {} }])
    const result = await host.chatParams({}, { temperature: 1, topP: 1, topK: 1, maxOutputTokens: undefined, options: {} })
    expect(result.temperature).toBe(1)
  })

  test("duplicate plugin names are rejected (fail loud)", () => {
    expect(() =>
      createWorkerPluginHost([
        { name: "dup", hooks: {} },
        { name: "dup", hooks: {} },
      ]),
    ).toThrow(DuplicatePluginError)
  })

  test("names are snapshotted once at construction", () => {
    const plugins: WorkerPlugin[] = [{ name: "a", hooks: {} }]
    const host = createWorkerPluginHost(plugins)
    plugins.push({ name: "b", hooks: {} })
    expect(host.names).toEqual(["a"])
  })
})
