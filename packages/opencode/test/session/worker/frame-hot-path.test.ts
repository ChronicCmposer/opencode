// Frame hot-path budget.
//
// One encode/decode per frame, no per-frame `TextEncoder` allocation, and a
// bounded p95 for both small control frames and a 4 MiB payload. The byte length
// comes from `Buffer.byteLength`, so the hot path never constructs an encoder.

import { describe, expect, test } from "bun:test"
import {
  MAX_WORKER_FRAME_BYTES,
  WORKER_FRAME_VERSION,
  decodeWorkerFrame,
  encodeWorkerFrame,
  workerFrameByteLength,
} from "@/session/worker/frame"

function eventsFrame(value: string) {
  return {
    v: WORKER_FRAME_VERSION as 1,
    port: "events" as const,
    kind: "event" as const,
    payload: { event: { id: "evt_1", type: "test", properties: { value } } },
  }
}

function p95(samples: number[]): number {
  const sorted = [...samples].sort((a, b) => a - b)
  return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * 0.95) - 1)]!
}

function benchmark(value: string, iterations: number): number[] {
  const frame = eventsFrame(value)
  const samples: number[] = []
  for (let i = 0; i < iterations; i += 1) {
    const start = performance.now()
    const text = encodeWorkerFrame(frame)
    decodeWorkerFrame(text)
    samples.push(performance.now() - start)
  }
  return samples
}

describe("frame hot path", () => {
  test("byte length uses Buffer.byteLength with no TextEncoder allocation", () => {
    const original = globalThis.TextEncoder
    let constructed = 0
    class CountingEncoder extends original {
      constructor() {
        super()
        constructed += 1
      }
    }
    globalThis.TextEncoder = CountingEncoder as typeof TextEncoder
    try {
      expect(workerFrameByteLength("héllo 🌍")).toBe(Buffer.byteLength("héllo 🌍", "utf8"))
      const text = encodeWorkerFrame(eventsFrame("small"))
      expect(decodeWorkerFrame(text).kind).toBe("event")
      expect(constructed).toBe(0)
    } finally {
      globalThis.TextEncoder = original
    }
  })

  test("small frames stay well under the hot-path budget", () => {
    const samples = benchmark("small", 500)
    expect(p95(samples)).toBeLessThan(5)
  })

  test("a 4 MiB frame stays under the transport budget", () => {
    const value = "x".repeat(MAX_WORKER_FRAME_BYTES - 256)
    const samples = benchmark(value, 20)
    expect(p95(samples)).toBeLessThan(500)
  })
})
