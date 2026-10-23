import { describe, expect, test } from "bun:test"
import {
  MOCK_SSE_API_KEY_ENV,
  MOCK_SSE_BURN_ENV,
  MOCK_SSE_MODEL_ENV,
  MOCK_SSE_URL_ENV,
  mockSseEnv,
} from "./mock-sse-worker"
import { throughputPerSec } from "./session-parallelism"

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
