// Phase 6.4 — worker tool registry + execute hooks tests.

import { describe, expect, test } from "bun:test"
import { createWorkerPluginHost } from "@/session/worker/plugin-host"
import {
  DuplicateToolError,
  UnknownToolError,
  createWorkerToolHost,
  type WorkerTool,
} from "@/session/worker/tool-host"

const echoTool = (name = "echo"): WorkerTool => ({
  name,
  execute: async (args) => ({ title: name, output: JSON.stringify(args), metadata: { args } }),
})

describe("worker tool host", () => {
  test("register / list / has", () => {
    const host = createWorkerToolHost()
    host.register(echoTool())
    expect(host.list()).toEqual(["echo"])
    expect(host.has("echo")).toBe(true)
    expect(host.has("missing")).toBe(false)
  })

  test("executes a registered tool", async () => {
    const host = createWorkerToolHost()
    host.register(echoTool())
    const result = await host.execute("echo", { a: 1 }, { sessionID: "s", callID: "c" })
    expect(result.output).toBe('{"a":1}')
  })

  test("fires tool.execute.before (rewrites args) and after (rewrites result)", async () => {
    const plugins = createWorkerPluginHost([
      {
        name: "inject",
        hooks: {
          "tool.execute.before": (_input, output) => {
            output.args = { ...(output.args as object), injected: true }
          },
          "tool.execute.after": (_input, output) => {
            output.metadata = { ...(output.metadata as object), stamped: true }
          },
        },
      },
    ])
    const host = createWorkerToolHost({ plugins })
    host.register(echoTool())

    const result = await host.execute("echo", { a: 1 }, { sessionID: "s", callID: "c" })
    expect(result.output).toBe('{"a":1,"injected":true}')
    expect(result.metadata).toEqual({ args: { a: 1, injected: true }, stamped: true })
  })

  test("before/after hooks receive the same input shape, including the live args", async () => {
    const seen: Array<{ phase: string; tool: string; sessionID: string; callID: string; args: unknown }> = []
    const plugins = createWorkerPluginHost([
      {
        name: "spy",
        hooks: {
          "tool.execute.before": (input) => {
            const value = input as { tool: string; sessionID: string; callID: string; args: unknown }
            seen.push({ phase: "before", ...value })
          },
          "tool.execute.after": (input) => {
            const value = input as { tool: string; sessionID: string; callID: string; args: unknown }
            seen.push({ phase: "after", ...value })
          },
        },
      },
    ])
    const host = createWorkerToolHost({ plugins })
    host.register(echoTool())

    await host.execute("echo", { a: 1 }, { sessionID: "s", callID: "c" })

    expect(seen).toEqual([
      { phase: "before", tool: "echo", sessionID: "s", callID: "c", args: { a: 1 } },
      { phase: "after", tool: "echo", sessionID: "s", callID: "c", args: { a: 1 } },
    ])
  })

  test("unknown tool fails loud", async () => {
    const host = createWorkerToolHost()
    await expect(host.execute("nope", {}, { sessionID: "s", callID: "c" })).rejects.toBeInstanceOf(UnknownToolError)
  })

  test("duplicate registration fails loud", () => {
    const host = createWorkerToolHost()
    host.register(echoTool())
    expect(() => host.register(echoTool())).toThrow(DuplicateToolError)
  })
})
