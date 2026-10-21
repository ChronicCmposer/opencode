// Step 3 — standalone OpenAI-compatible streaming mock provider.
//
// Contract: `startMockProvider()` serves a real TCP `/chat/completions` endpoint
// returning an OpenAI SSE stream, and yields the `{baseUrl, apiKey, model}`
// bootstrap a session worker consumes. The frames are verified by driving
// `worker-mock-llm.ts`'s own parser (`consumeChat`), so the producer and the
// consumer can never drift.

import { describe, expect, test } from "bun:test"
import { startMockProvider, type MockProviderHandle } from "./mock-provider"
import { consumeChat, type MockLlmBootstrap, type MockLlmChatRequest } from "./worker-mock-llm"

function bootstrapFor(provider: MockProviderHandle): MockLlmBootstrap {
  return {
    baseUrl: provider.baseUrl,
    provider: { id: "test", name: "Test" },
    model: { id: provider.model, name: "Test Model" },
    apiKey: provider.apiKey,
  }
}

function chat(id: string): MockLlmChatRequest {
  return { type: "chat", id, messages: [{ role: "user", content: "hi" }] }
}

describe("startMockProvider", () => {
  test("streams an OpenAI-compatible reply the worker parser consumes", async () => {
    await using provider = startMockProvider({ text: "hello streaming", usage: { input: 3, output: 2 } })

    expect(provider.baseUrl).toMatch(/^http:\/\/127\.0\.0\.1:\d+\/v1$/)
    expect(provider.apiKey).toBe("test-key")
    expect(provider.model).toBe("test-model")

    const result = await consumeChat(bootstrapFor(provider), chat("c1"))

    expect(result.type).toBe("chat-result")
    expect(result.id).toBe("c1")
    expect(result.text).toBe("hello streaming")
    expect(result.usage).toEqual({ input: 3, output: 2 })
  })

  test("splits the reply into the requested content deltas", async () => {
    await using provider = startMockProvider({ chunks: ["a", "b", "c"] })

    const result = await consumeChat(bootstrapFor(provider), chat("c2"))

    expect(result.text).toBe("abc")
    expect(provider.requests).toHaveLength(1)
  })

  test("streams tool calls in OpenAI delta form", async () => {
    await using provider = startMockProvider({
      toolCalls: [{ id: "call_1", name: "lookup", arguments: '{"query":"x"}' }],
    })

    const result = await consumeChat(bootstrapFor(provider), chat("c3"))

    expect(result.type).toBe("chat-result")
    expect(result.toolCalls).toEqual([{ id: "call_1", name: "lookup", arguments: '{"query":"x"}' }])
  })

  test("surfaces a scripted HTTP error status", async () => {
    await using provider = startMockProvider({ status: 500, errorBody: { error: "boom" } })

    await expect(consumeChat(bootstrapFor(provider), chat("c4"))).rejects.toThrow(/500/)
  })

  test("records requests and supports wait(count)", async () => {
    await using provider = startMockProvider({ text: "ok" })

    const pending = provider.wait(1)
    await consumeChat(bootstrapFor(provider), chat("c5"))
    await pending

    expect(provider.requests).toHaveLength(1)
    expect(provider.requests[0]?.body.model).toBe("test-model")
    expect(provider.requests[0]?.authorization).toBe("Bearer test-key")
  })
})
