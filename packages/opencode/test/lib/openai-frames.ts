// Shared OpenAI chat.completion.chunk frame building.
export interface FrameUsage {
  readonly input: number
  readonly output: number
  readonly cacheRead?: number
}

export interface ChatChunkInput {
  readonly delta?: Record<string, unknown>
  readonly finish?: string
  readonly usage?: FrameUsage
  /** Completion id; defaults to "chatcmpl-test". */
  readonly id?: string
}

const DEFAULT_CHUNK_ID = "chatcmpl-test"

/** Serialize one payload as an SSE frame. */
export function sseData(payload: unknown): string {
  return "data: " + JSON.stringify(payload) + "\n\n"
}

/** OpenAI `usage` shape from an input/output pair plus optional cache-read tokens. */
export function usageTokens(usage: FrameUsage): Record<string, unknown> {
  return {
    prompt_tokens: usage.input,
    completion_tokens: usage.output,
    total_tokens: usage.input + usage.output,
    ...(usage.cacheRead === undefined
      ? {}
      : { prompt_tokens_details: { cached_tokens: usage.cacheRead } }),
  }
}

/** One `chat.completion.chunk` payload. */
export function chatChunk(input: ChatChunkInput = {}): Record<string, unknown> {
  return {
    id: input.id ?? DEFAULT_CHUNK_ID,
    object: "chat.completion.chunk",
    choices: [
      {
        delta: input.delta ?? {},
        ...(input.finish === undefined ? {} : { finish_reason: input.finish }),
      },
    ],
    ...(input.usage === undefined ? {} : { usage: usageTokens(input.usage) }),
  }
}
