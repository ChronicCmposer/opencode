// Shared SSE stream parsing (plan consolidation).
//
// The single reader/accumulator for OpenAI-compatible SSE streams. Every test
// consumer — the worker-side mock-LLM chat parser, the mock-SSE session worker,
// the session-layer worker fixture and the packaged-binary smoke gate — decodes
// frames through here, so producer and consumer cannot drift.
//
// `parseSseFrame` decodes one frame's `data:` payloads; `readSsePayloads` streams
// a body to a per-payload callback; `readSseStream` accumulates into the shared
// `SseAccumulator`.

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

/** SSE stream terminators that carry no JSON payload. */
const DONE_MARKERS: ReadonlySet<string> = new Set(["DONE", "[DONE]"])

/**
 * Decode the JSON payloads carried by one raw SSE frame (which may hold several
 * `data:` lines). Non-`data:` lines, terminators and malformed JSON are skipped,
 * so a consumer can treat the result as trusted payloads.
 */
export function parseSseFrame(frame: string): unknown[] {
  const payloads: unknown[] = []
  for (const rawLine of frame.split("\n")) {
    const line = rawLine.trim()
    if (!line.startsWith("data:")) continue
    const data = line.slice("data:".length).trim()
    if (data.length === 0 || DONE_MARKERS.has(data)) continue
    try {
      payloads.push(JSON.parse(data))
    } catch {
      // Ignore malformed frames; SSE payloads are expected to be JSON.
    }
  }
  return payloads
}

/** Apply one raw SSE frame (which may hold several `data:` lines). */
export function applySseFrame(accumulator: SseAccumulator, frame: string): void {
  for (const payload of parseSseFrame(frame)) applySsePayload(accumulator, payload)
}

/**
 * Read an SSE body, invoking `onPayload` for every decoded `data:` payload as it
 * arrives. This is the streaming primitive both the accumulator and consumers
 * that react to each event (rather than collect them) build on.
 */
export async function readSsePayloads(
  body: ReadableStream<Uint8Array>,
  onPayload: (payload: unknown) => void,
): Promise<void> {
  const reader = body.getReader()
  const decoder = new TextDecoder()
  let buffer = ""
  const drain = (frame: string): void => {
    for (const payload of parseSseFrame(frame)) onPayload(payload)
  }
  for (;;) {
    const { done, value } = await reader.read()
    if (done) break
    buffer += decoder.decode(value, { stream: true })
    let boundary = buffer.indexOf("\n\n")
    while (boundary !== -1) {
      drain(buffer.slice(0, boundary))
      buffer = buffer.slice(boundary + 2)
      boundary = buffer.indexOf("\n\n")
    }
  }
  buffer += decoder.decode()
  if (buffer.trim() !== "") drain(buffer)
}

/** Read an SSE body, applying every `data:` payload to `accumulator`. */
export async function readSseStream(
  body: ReadableStream<Uint8Array>,
  accumulator: SseAccumulator = createSseAccumulator(),
): Promise<SseAccumulator> {
  await readSsePayloads(body, (payload) => applySsePayload(accumulator, payload))
  return accumulator
}
