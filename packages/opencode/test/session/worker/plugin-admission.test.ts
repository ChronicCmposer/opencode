// W4.1 / D19 — a hook added after startup is a typed admission failure on the
// next worker run. The run resolver is the admission boundary: it rejects an
// unsupported or unclassified trigger before any worker init is built.

import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { UnclassifiedPluginHookError, UnsupportedPluginHookError } from "@/plugin/routes"
import { resolveWorkerPlugins, type WorkerModel } from "@/session/worker/init"

const model: WorkerModel = {
  providerID: "mock",
  modelID: "mock-model",
  apiId: "mock-api",
  npm: "@ai-sdk/openai-compatible",
  baseUrl: "http://127.0.0.1:0/v1",
  apiKey: "sk-mock",
  capabilities: { temperature: true, reasoning: false },
  limit: { context: 128_000, output: 4096 },
  options: { topK: 5 },
}

const resolve = (hooks: Record<string, unknown>) =>
  Effect.runPromise(
    Effect.result(
      resolveWorkerPlugins({
        model,
        sources: [{ name: "dynamic", hooks } as never],
      }),
    ),
  )

describe("worker plugin hook admission", () => {
  test("rejects an unsupported hook added after startup", async () => {
    const result = await resolve({ "shell.env": async () => {} })
    expect(result._tag).toBe("Failure")
    if (result._tag !== "Failure") throw new Error("expected admission failure")
    expect(result.failure).toBeInstanceOf(UnsupportedPluginHookError)
    expect((result.failure as UnsupportedPluginHookError).hook).toBe("shell.env")
  })

  test("rejects an unclassified hook added after startup", async () => {
    const result = await resolve({ "future.trigger": async () => {} })
    expect(result._tag).toBe("Failure")
    if (result._tag !== "Failure") throw new Error("expected admission failure")
    expect(result.failure).toBeInstanceOf(UnclassifiedPluginHookError)
    expect((result.failure as UnclassifiedPluginHookError).hook).toBe("future.trigger")
  })

  test("admits routed hooks", async () => {
    const result = await resolve({
      "chat.params": (_input: unknown, output: { temperature?: number }) => {
        output.temperature = 0.4
      },
    })
    expect(result._tag).toBe("Success")
    if (result._tag !== "Success") throw new Error("expected admission")
    expect(result.success[0]?.resolved?.["chat.params"]).toEqual({ temperature: 0.4 })
  })

  test("admits declared-only and non-trigger hooks", async () => {
    const result = await resolve({
      "permission.ask": async () => {},
      event: async () => {},
      tool: {},
    })
    expect(result._tag).toBe("Success")
  })
})
