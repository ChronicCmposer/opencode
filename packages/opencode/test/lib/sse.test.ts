import { describe, expect, test } from "bun:test"
import { applySsePayload, createSseAccumulator, readSseStream } from "./sse"

function streamOf(text: string): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder()
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(encoder.encode(text))
      controller.close()
    },
  })
}

describe("shared SSE parsing", () => {
  test("accumulates text, usage and skips DONE", async () => {
    const body = [
      'data: {"choices":[{"delta":{"content":"Hel"}}]}',
      'data: {"choices":[{"delta":{"content":"lo"}}]}',
      'data: {"usage":{"prompt_tokens":3,"completion_tokens":2}}',
      "",
      "",
    ].join("\n\n")
    const acc = await readSseStream(streamOf(body))
    expect(acc.text).toBe("Hello")
    expect(acc.chunks).toBe(2)
    expect(acc.usage).toEqual({ input: 3, output: 2 })
  })

  test("joins tool-call argument fragments", () => {
    const acc = createSseAccumulator()
    applySsePayload(acc, { choices: [{ delta: { tool_calls: [{ id: "c1", function: { name: "lookup", arguments: '{"q"' } }] } }] })
    applySsePayload(acc, { choices: [{ delta: { tool_calls: [{ function: { arguments: ':"x"}' } }] } }] })
    expect(acc.toolCalls).toEqual([{ id: "c1", name: "lookup", arguments: '{"q":"x"}' }])
  })

  test("ignores comment and malformed frames", async () => {
    const body = ': note\n\ndata: not-json\n\ndata: {"choices":[{"delta":{"content":"ok"}}]}\n\n'
    const acc = await readSseStream(streamOf(body))
    expect(acc.text).toBe("ok")
  })
})
