// Phase 3.5 — worker-reachable mock provider / SSE (plan task 3.5, BLOCKS 4–14).
//
// `TestLLMServer` is a real TCP HTTP server that lives on the hub. A `Response`
// or `ReadableStream` cannot cross a Bun worker boundary, but the base URL
// string can. So the hub sends a typed **bootstrap payload** — just the base URL
// plus provider/model metadata — and the worker does the HTTP itself: it opens
// the SSE stream with `fetch`, parses the frames in-isolate, and posts back
// plain JSON. No Effect, no model SDK, no shared memory.

import { readSseStream } from "./sse"

/** Everything the worker needs to reach the hub's mock provider. */
export interface MockLlmBootstrap {
  readonly baseUrl: string
  readonly provider: { readonly id: string; readonly name: string }
  readonly model: { readonly id: string; readonly name: string }
  readonly apiKey: string
}

export interface MockLlmMessage {
  readonly role: string
  readonly content: string
}

export interface MockLlmChatRequest {
  readonly type: "chat"
  readonly id: string
  readonly messages: readonly MockLlmMessage[]
}

export interface MockLlmToolCall {
  readonly id: string
  readonly name: string
  readonly arguments: string
}

export interface MockLlmUsage {
  readonly input: number
  readonly output: number
}

export interface MockLlmChatResult {
  readonly type: "chat-result"
  /** Echoes the originating `MockLlmChatRequest.id` so the hub can correlate. */
  readonly id: string
  readonly text: string
  usage?: MockLlmUsage
  toolCalls?: readonly MockLlmToolCall[]
}

export interface MockLlmError {
  readonly type: "chat-error"
  /** Echoes the originating `MockLlmChatRequest.id` so the hub can correlate. */
  readonly id: string
  readonly message: string
}

export interface MockLlmInit {
  readonly type: "init"
  readonly bootstrap: MockLlmBootstrap
}

export type MockLlmInbound = MockLlmInit | MockLlmChatRequest
export type MockLlmOutbound = MockLlmReady | MockLlmChatResult | MockLlmError

export interface MockLlmReady {
  readonly type: "ready"
}

/** The subset of the worker global scope this module needs. */
export interface MockLlmWorkerScope {
  onmessage: ((event: { data: unknown }) => void) | null
  postMessage(message: unknown): void
}

class MockProviderError extends Error {
  readonly status: number

  constructor(status: number) {
    super(`mock provider responded ${status}`)
    this.name = "MockProviderError"
    this.status = status
  }
}

/** Read the mock provider's SSE body into a `MockLlmChatResult` via the shared accumulator. */
async function readChatStream(response: Response, id: string): Promise<MockLlmChatResult> {
  if (!response.body) throw new Error("mock provider returned an empty body")
  const accumulator = await readSseStream(response.body)

  const result: MockLlmChatResult = { type: "chat-result", id, text: accumulator.text }
  if (accumulator.usage) result.usage = accumulator.usage
  if (accumulator.toolCalls.length > 0) result.toolCalls = accumulator.toolCalls
  return result
}

/**
 * Fetch one chat completion from `bootstrap.baseUrl` and parse the SSE reply into
 * a `MockLlmChatResult`. Exported for tests that drive the exact worker-side
 * parser against a mock provider, so producer and consumer cannot drift.
 */
export async function consumeChat(
  bootstrap: MockLlmBootstrap,
  request: MockLlmChatRequest,
): Promise<MockLlmChatResult> {
  const response = await fetch(`${bootstrap.baseUrl}/chat/completions`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${bootstrap.apiKey}`,
    },
    body: JSON.stringify({
      model: bootstrap.model.id,
      stream: true,
      messages: request.messages,
    }),
  })
  if (!response.ok) throw new MockProviderError(response.status)
  return readChatStream(response, request.id)
}

function describeError(error: unknown): string {
  if (error instanceof Error) return error.message
  return String(error)
}

/**
 * Wire the mock-LLM protocol onto a worker global scope: `init` stores the
 * bootstrap and answers `ready`; each `chat` fetches the hub's SSE endpoint and
 * posts a `chat-result` (or `chat-error`).
 */
export function installMockLlmWorker(scope: MockLlmWorkerScope): void {
  let bootstrap: MockLlmBootstrap | undefined

  scope.onmessage = (event) => {
    const message = event.data as MockLlmInbound | undefined
    if (!message) return

    if (message.type === "init") {
      bootstrap = message.bootstrap
      scope.postMessage({ type: "ready" } satisfies MockLlmReady)
      return
    }

    if (message.type === "chat") {
      if (!bootstrap) {
        scope.postMessage({
          type: "chat-error",
          id: message.id,
          message: "worker received chat before init",
        } satisfies MockLlmError)
        return
      }
      const current = bootstrap
      void consumeChat(current, message).then(
        (result) => scope.postMessage(result),
        (error) =>
          scope.postMessage({
            type: "chat-error",
            id: message.id,
            message: describeError(error),
          } satisfies MockLlmError),
      )
    }
  }
}
