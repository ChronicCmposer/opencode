// Phase 3.5 — worker-reachable mock provider / SSE (plan task 3.5, BLOCKS 4–14).
//
// Contract under test: the hub hosts a real `TestLLMServer` (TCP HTTP); a Bun
// worker reaches it with plain `fetch` using a bootstrap payload that carries
// only the base URL plus provider/model metadata (a `Response`/`ReadableStream`
// cannot cross the isolate boundary, a URL string can). The worker parses the
// SSE stream itself and posts plain JSON back.

import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { testEffect } from "./effect"
import { TestLLMServer } from "./llm-server"
import { createWorkerHarness } from "./worker-harness"
import {
  type MockLlmBootstrap,
  type MockLlmChatRequest,
  type MockLlmChatResult,
  type MockLlmError,
  type MockLlmInit,
  type MockLlmOutbound,
  type MockLlmReady,
} from "./worker-mock-llm"

const it = testEffect(TestLLMServer.layer)
const MOCK_LLM_WORKER = new URL("../fixture/mock-llm-worker.ts", import.meta.url)

type WorkerOut = MockLlmReady | MockLlmChatResult | MockLlmError
type WorkerIn = MockLlmInit | MockLlmChatRequest

function bootstrapFor(baseUrl: string): MockLlmBootstrap {
  return {
    baseUrl,
    provider: { id: "test", name: "Test" },
    model: { id: "test-model", name: "Test Model" },
    apiKey: "test-key",
  }
}

function runWorkerChat(
  llm: TestLLMServer["Service"],
  request: MockLlmChatRequest,
): Effect.Effect<MockLlmChatResult | MockLlmError> {
  return Effect.gen(function* () {
    const harness = createWorkerHarness()
    return yield* Effect.gen(function* () {
      const worker = harness.spawn<WorkerOut, WorkerIn>(MOCK_LLM_WORKER)
      worker.post({ type: "init", bootstrap: bootstrapFor(llm.url) })
      yield* Effect.promise(() => worker.waitFor("ready"))
      worker.post(request)
      const result = yield* Effect.promise(() => worker.waitFor((message) => message.type !== "ready"))
      return result as MockLlmChatResult | MockLlmError
    }).pipe(Effect.ensuring(Effect.promise(() => harness.dispose())))
  })
}

describe("worker mock llm", () => {
  it.live("consumes a streamed text reply from the hub's SSE endpoint", () =>
    Effect.gen(function* () {
      const llm = yield* TestLLMServer
      yield* llm.text("hello from the worker", { usage: { input: 11, output: 5 } })

      const result = yield* runWorkerChat(llm, {
        type: "chat",
        id: "c1",
        messages: [{ role: "user", content: "hi" }],
      })

      expect(result.type).toBe("chat-result")
      if (result.type !== "chat-result") return
      expect(result.text).toBe("hello from the worker")
      expect(result.usage).toEqual({ input: 11, output: 5 })

      yield* llm.wait(1)
      const inputs = yield* llm.inputs
      expect(inputs[0]?.model).toBe("test-model")
    }),
  )

  it.live("parses streamed tool calls from the mock provider", () =>
    Effect.gen(function* () {
      const llm = yield* TestLLMServer
      yield* llm.tool("lookup", { query: "x" })

      const result = yield* runWorkerChat(llm, {
        type: "chat",
        id: "c2",
        messages: [{ role: "user", content: "find x" }],
      })

      expect(result.type).toBe("chat-result")
      if (result.type !== "chat-result") return
      expect(result.toolCalls).toEqual([{ id: "call_1", name: "lookup", arguments: '{"query":"x"}' }])
    }),
  )

  it.live("surfaces a mock provider HTTP error as a chat-error", () =>
    Effect.gen(function* () {
      const llm = yield* TestLLMServer
      yield* llm.error(500, { error: "boom" })

      const result = yield* runWorkerChat(llm, {
        type: "chat",
        id: "c3",
        messages: [{ role: "user", content: "hi" }],
      })

      expect(result.type).toBe("chat-error")
      if (result.type !== "chat-error") return
      expect(result.message).toContain("500")
    }),
  )
})

describe("worker mock llm contract", () => {
  test("bootstrap payload carries only a URL and metadata", () => {
    const bootstrap = bootstrapFor("http://127.0.0.1:1234/v1")
    expect(structuredClone(bootstrap)).toEqual(bootstrap)
  })
})
