// Shared SSE stream parsing (plan consolidation).
//
// Both the mock-SSE session worker and the worker-side mock-LLM consumer parse
// an OpenAI-compatible SSE stream into the same shape, so the reader and the
// accumulator live here. Additive: existing consumers are migrated in wave 2.

export interface SseUsage {
  readonly input: number
  readonly output: number
}

export interface SseToolCall {
  readonly id: string
  readonly name: string
  readonly arguments: string
}

export interface SseAccumulator {
  text: string
  chunks: number
  usage?: SseUsage
  readonly toolCalls: SseToolCall[]
}

export function createSseAccumulator(): SseAccumulator {
  return { text: "", chunks: 0, toolCalls: [] }
}

function applyUsage(accumulator: SseAccumulator, payload: Record<string, unknown>): void {
  const usage = payload.usage
  if (usage === null || typeof usage !== "object") return
  const record = usage as Record<string, unknown>
  if (typeof record.prompt_tokens === "number" && typeof record.completion_tokens === "number") {
    accumulator.usage = { input: record.prompt_tokens, output: record.completion_tokens }
  }
}

function applyToolCalls(accumulator: SseAccumulator, delta: Record<string, unknown>): void {
  if (!Array.isArray(delta.tool_calls)) return
  for (const tool of delta.tool_calls) {
    if (tool === null || typeof tool !== "object") continue
    const call = tool as Record<string, unknown>
    const fn =
      call.function !== null && typeof call.function === "object"
        ? (call.function as Record<string, unknown>)
        : undefined
    if (typeof call.id === "string" && fn !== undefined && typeof fn.name === "string") {
      accumulator.toolCalls.push({ id: call.id, name: fn.name, arguments: "" })
    }
    const current = accumulator.toolCalls[accumulator.toolCalls.length - 1]
    if (current !== undefined && fn !== undefined && typeof fn.arguments === "string" && fn.arguments !== "") {
      accumulator.toolCalls[accumulator.toolCalls.length - 1] = {
        ...current,
        arguments: current.arguments + fn.arguments,
      }
    }
  }
}

/** Apply one decoded SSE JSON payload to the accumulator. Ignores unknown shapes. */
export function applySsePayload(accumulator: SseAccumulator, payload: unknown): void {
  if (payload === null || typeof payload !== "object") return
  const record = payload as Record<string, unknown>
  applyUsage(accumulator, record)
  if (!Array.isArray(record.choices)) return
  const choice = record.choices[0]
  if (choice === null || typeof choice !== "object") return
  const delta = (choice as Record<string, unknown>).delta
  if (delta === null || typeof delta !== "object") return
  const fields = delta as Record<string, unknown>
  if (typeof fields.content === "string") {
    accumulator.text += fields.content
    accumulator.chunks += 1
  }
  applyToolCalls(accumulator, fields)
}

/** Apply one raw SSE frame (which may hold several `data:` lines). */
export function applySseFrame(accumulator: SseAccumulator, frame: string): void {
  for (const rawLine of frame.split("\n")) {
    const line = rawLine.trim()
    if (!line.startsWith("data:")) continue
    const data = line.slice(5).trim()
    if (data.length === 0 || data === "DONE") continue
    try {
      applySsePayload(accumulator, JSON.parse(data))
    } catch {
      continue
    }
  }
}

/** Read an SSE body, applying every `data:` payload to `accumulator`. */
export async function readSseStream(
  body: ReadableStream<Uint8Array>,
  accumulator: SseAccumulator = createSseAccumulator(),
): Promise<SseAccumulator> {
  const reader = body.getReader()
  const decoder = new TextDecoder()
  let buffer = ""
  for (;;) {
    const { done, value } = await reader.read()
    if (done) break
    buffer += decoder.decode(value, { stream: true })
    let boundary = buffer.indexOf("\n\n")
    while (boundary !== -1) {
      applySseFrame(accumulator, buffer.slice(0, boundary))
      buffer = buffer.slice(boundary + 2)
      boundary = buffer.indexOf("\n\n")
    }
  }
  buffer += decoder.decode()
  if (buffer.trim() !== "") applySseFrame(accumulator, buffer)
  return accumulator
}
