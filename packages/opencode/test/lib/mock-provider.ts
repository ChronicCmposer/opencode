// Standalone OpenAI-compatible streaming mock provider.
//
// `startMockProvider()` serves a real TCP `/chat/completions` endpoint that emits
// an OpenAI-style SSE stream, and returns the `{baseUrl, apiKey, model}`
// bootstrap a session worker consumes (`session-worker.ts` fetches
// `${baseUrl}/chat/completions`). The frames match what `worker-mock-llm.ts`
// parses, and its `consumeChat` parser is reused in tests to prove it.

import { chatChunk, sseData } from "./openai-frames"

/** Token usage in OpenAI `prompt_tokens`/`completion_tokens` terms. */
export interface MockProviderUsage {
  readonly input: number
  readonly output: number
}

/** A streamed tool call in OpenAI delta form. */
export interface MockProviderToolCall {
  readonly id: string
  readonly name: string
  readonly arguments: string
}

/** One scripted reply: content deltas, tool calls, usage, or an HTTP error. */
export interface MockProviderScript {
  /** Single content delta; ignored when `chunks` is supplied. */
  readonly text?: string
  /** Ordered content deltas (overrides `text`). */
  readonly chunks?: readonly string[]
  readonly toolCalls?: readonly MockProviderToolCall[]
  readonly usage?: MockProviderUsage
  /** When set, respond with this HTTP status and `errorBody` instead of a stream. */
  readonly status?: number
  readonly errorBody?: unknown
}

export interface MockProviderOptions extends MockProviderScript {
  readonly model?: string
  readonly apiKey?: string
  /** Per-request scripts; the last one repeats once the queue drains. */
  readonly scripts?: readonly MockProviderScript[]
  readonly hostname?: string
  /** Pick a script from the request body; `undefined` falls back to the queued scripts. */
  readonly select?: (body: Record<string, unknown>) => MockProviderScript | undefined
}

/** One observed request, for assertions. */
export interface MockProviderRequest {
  readonly url: string
  readonly authorization: string | undefined
  readonly body: Record<string, unknown>
}

export interface MockProviderHandle extends AsyncDisposable {
  /** `http://127.0.0.1:<port>/v1` — feed straight into `SessionWorkerModel.baseUrl`. */
  readonly baseUrl: string
  readonly apiKey: string
  readonly model: string
  readonly requests: readonly MockProviderRequest[]
  /** Resolve once at least `count` requests have been observed. */
  wait(count: number, timeoutMs?: number): Promise<void>
  stop(): Promise<void>
}

// --- SSE frame construction (OpenAI chat.completion.chunk) ------------------

interface ChatChunk {
  readonly delta: Record<string, unknown>
  readonly finish?: string
  readonly usage?: MockProviderUsage
}

function frameOf(chunk: ChatChunk): string {
  return sseData(chatChunk({ id: "chatcmpl-mock", delta: chunk.delta, finish: chunk.finish, usage: chunk.usage }))
}

/** Resolve a script's content deltas (chunks win over a single text). */
function contentDeltas(script: MockProviderScript): string[] {
  if (script.chunks !== undefined) return [...script.chunks]
  return [script.text ?? "ok"]
}

/** OpenAI delta frames for a script's tool calls (start frame, then arguments). */
function toolCallFrames(calls: readonly MockProviderToolCall[]): string[] {
  const frames: string[] = []
  for (const call of calls) {
    frames.push(
      frameOf({
        delta: { tool_calls: [{ index: 0, id: call.id, type: "function", function: { name: call.name, arguments: "" } }] },
      }),
    )
    frames.push(frameOf({ delta: { tool_calls: [{ index: 0, function: { arguments: call.arguments } }] } }))
  }
  return frames
}

/** Build the ordered SSE frames for one scripted reply. */
function framesFor(script: MockProviderScript): string[] {
  const deltas = contentDeltas(script).map((content) => frameOf({ delta: { content } }))
  const tools = toolCallFrames(script.toolCalls ?? [])
  const finish = (script.toolCalls?.length ?? 0) > 0 ? "tool_calls" : "stop"
  return [frameOf({ delta: { role: "assistant" } }), ...deltas, ...tools, frameOf({ delta: {}, finish, usage: script.usage })]
}

/** OpenAI-compatible SSE body for one scripted reply, terminated by `[DONE]`. */
function streamBody(script: MockProviderScript): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder()
  const frames = framesFor(script)
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (const frame of frames) controller.enqueue(encoder.encode(frame))
      controller.enqueue(encoder.encode("data: [DONE]\n\n"))
      controller.close()
    },
  })
}

// --- Live server ------------------------------------------------------------

const DEFAULT_MODEL = "test-model"
const DEFAULT_API_KEY = "test-key"
const DEFAULT_HOSTNAME = "127.0.0.1"
const DEFAULT_WAIT_TIMEOUT_MS = 5_000

/** A caller blocked in `wait(count)` until enough requests have arrived. */
interface PendingWait {
  readonly count: number
  readonly resolve: () => void
  readonly reject: (error: Error) => void
  readonly timer: ReturnType<typeof setTimeout>
}

/** The single script built from the top-level options when `scripts` is omitted. */
function optionsScript(options: MockProviderOptions): MockProviderScript {
  return {
    text: options.text,
    chunks: options.chunks,
    toolCalls: options.toolCalls,
    usage: options.usage,
    status: options.status,
    errorBody: options.errorBody,
  }
}

/**
 * Parse a request body into a plain record. Anything that is not a non-null,
 * non-array object (malformed JSON, a JSON scalar) becomes `{}`, so the recorded
 * request body is always a trusted record (Law of "Parse, Don't Validate").
 */
function requestBody(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return {}
  return Object.fromEntries(Object.entries(value))
}

/** Select the script for request `index`; the last script repeats once drained. */
function scriptFor(scripts: readonly MockProviderScript[], index: number): MockProviderScript {
  return scripts[Math.min(index, scripts.length - 1)] ?? {}
}

/**
 * Start a real TCP OpenAI-compatible streaming provider and return its bootstrap.
 * The port is bound synchronously (Bun binds in `serve`), so the returned handle
 * is usable immediately; `stop()` (or `await using`) releases it.
 */
export function startMockProvider(options: MockProviderOptions = {}): MockProviderHandle {
  const apiKey = options.apiKey ?? DEFAULT_API_KEY
  const model = options.model ?? DEFAULT_MODEL
  const hostname = options.hostname ?? DEFAULT_HOSTNAME
  const scripts = options.scripts && options.scripts.length > 0 ? options.scripts : [optionsScript(options)]

  const requests: MockProviderRequest[] = []
  let waits: PendingWait[] = []

  const settleWaits = (): void => {
    const remaining: PendingWait[] = []
    for (const wait of waits) {
      if (requests.length >= wait.count) {
        clearTimeout(wait.timer)
        wait.resolve()
      } else {
        remaining.push(wait)
      }
    }
    waits = remaining
  }

  const server = Bun.serve({
    port: 0,
    hostname,
    async fetch(request) {
      const url = new URL(request.url)
      if (request.method !== "POST" || !url.pathname.endsWith("/chat/completions")) {
        return new Response("not found", { status: 404 })
      }

      const body = requestBody(await request.json().catch(() => ({})))
      requests.push({ url: url.pathname, authorization: request.headers.get("authorization") ?? undefined, body })
      settleWaits()

      const script = options.select?.(body) ?? scriptFor(scripts, requests.length - 1)
      if (script.status !== undefined) {
        return new Response(JSON.stringify(script.errorBody ?? {}), {
          status: script.status,
          headers: { "content-type": "application/json" },
        })
      }
      return new Response(streamBody(script), { headers: { "content-type": "text/event-stream" } })
    },
  })

  const stop = async (): Promise<void> => {
    for (const wait of waits) {
      clearTimeout(wait.timer)
      wait.reject(new Error("mock provider stopped before wait(count) resolved"))
    }
    waits = []
    await server.stop(true)
  }

  return {
    get baseUrl() {
      return `http://${hostname}:${server.port}/v1`
    },
    apiKey,
    model,
    get requests() {
      return [...requests]
    },
    wait(count: number, timeoutMs = DEFAULT_WAIT_TIMEOUT_MS) {
      if (requests.length >= count) return Promise.resolve()
      return new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => {
          waits = waits.filter((entry) => entry.timer !== timer)
          reject(new Error(`mock provider did not receive ${count} request(s) within ${timeoutMs}ms`))
        }, timeoutMs)
        waits.push({ count, resolve, reject, timer })
      })
    },
    stop,
    [Symbol.asyncDispose]: stop,
  }
}
