import { describe, expect, test } from "bun:test"
import {
  MOCK_SSE_API_KEY_ENV,
  MOCK_SSE_BURN_ENV,
  MOCK_SSE_MODEL_ENV,
  MOCK_SSE_URL_ENV,
  MOCK_SSE_WORKER_ENTRY,
  mockSseEnv,
} from "./mock-sse-worker"
import { throughputPerSec, waitForWorkerBoot } from "./session-parallelism"
import { createWorkerHarness } from "./worker-harness"

const NO_PORTS_FIXTURE = new URL("../fixture/no-name-worker.ts", import.meta.url)

describe("waitForWorkerBoot", () => {
  test("resolves the ready handshake with the transferred egress port", async () => {
    const harness = createWorkerHarness({ timeoutMs: 15_000 })
    try {
      const handle = harness.spawn(MOCK_SSE_WORKER_ENTRY)
      const boot = await waitForWorkerBoot(handle, { timeoutMs: 15_000 })

      expect(boot.threadId).toBeGreaterThan(0)
      expect(boot.egress).toBeInstanceOf(MessagePort)
      boot.egress?.close()
    } finally {
      await harness.dispose()
    }
  })

  test("fails loud instead of hanging when the worker transfers no ports", async () => {
    const harness = createWorkerHarness({ timeoutMs: 2_000 })
    try {
      const handle = harness.spawn(NO_PORTS_FIXTURE)
      await expect(waitForWorkerBoot(handle, { timeoutMs: 2_000 })).rejects.toThrow(
        "timed out waiting for worker message",
      )
    } finally {
      await harness.dispose()
    }
  })
})

describe("session-parallelism helpers", () => {
  test("throughputPerSec records a finite value for an instantaneous run", () => {
    expect(throughputPerSec(4, 1000)).toBe(4)
    expect(throughputPerSec(4, 0)).toBe(0)
    expect(throughputPerSec(4, -5)).toBe(0)
    expect(Number.isFinite(throughputPerSec(1, 0))).toBe(true)
  })

  test("mockSseEnv always wires the base URL and burn count", () => {
    const env = mockSseEnv("http://127.0.0.1:1/v1", 123)
    expect(env[MOCK_SSE_URL_ENV]).toBe("http://127.0.0.1:1/v1")
    expect(env[MOCK_SSE_BURN_ENV]).toBe("123")
    expect(env[MOCK_SSE_MODEL_ENV]).toBeUndefined()
    expect(env[MOCK_SSE_API_KEY_ENV]).toBeUndefined()
  })

  test("mockSseEnv forwards the optional model and apiKey when supplied", () => {
    const env = mockSseEnv("http://127.0.0.1:1/v1", 1, "mock-model", "secret-key")
    expect(env[MOCK_SSE_MODEL_ENV]).toBe("mock-model")
    expect(env[MOCK_SSE_API_KEY_ENV]).toBe("secret-key")
  })
})
