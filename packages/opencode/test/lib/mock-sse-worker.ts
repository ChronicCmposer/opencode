// Worker-side mock-SSE consumer (split from the former mock-sse-harness).
//
// `consumeMockSse` fetches the hub's mock provider and parses the SSE stream
// in-isolate via the shared `readSseStream`; `burnCpu` runs a synchronous CPU
// fold that simulates the per-delta assembly cost. Because the fold is
// synchronous, a busy session pins exactly one worker thread — the "one hot
// thread" baseline.
//
// `readMockSseConfig` / `mockSseEnv` are the two halves of the hub→worker
// bootstrap contract (the env overlay keys below).

import { readSseStream } from "./sse"

/** Work kind the session-worker fixture registers for the SSE workload. */
export const MOCK_SSE_WORK_KIND = "sse"

/** Env overlay keys the hub uses to hand the worker its bootstrap (kind is a string only). */
export const MOCK_SSE_URL_ENV = "OPENCODE_MOCK_SSE_URL"
export const MOCK_SSE_BURN_ENV = "OPENCODE_MOCK_SSE_BURN"
export const MOCK_SSE_MODEL_ENV = "OPENCODE_MOCK_SSE_MODEL"
export const MOCK_SSE_API_KEY_ENV = "OPENCODE_MOCK_SSE_API_KEY"

/** CPU iterations per session; ~40 ms of synchronous work on the reference host. */
export const DEFAULT_BURN_ITERATIONS = 20_000_000

/** The real session-worker fixture the harness spawns. */
export const MOCK_SSE_WORKER_ENTRY = new URL("../fixture/mock-sse-session-worker.ts", import.meta.url)

export interface MockSseBootstrap {
  readonly baseUrl: string
  readonly model: string
  readonly apiKey: string
}

export interface MockSseConfig extends MockSseBootstrap {
  readonly burnIterations: number
}

export interface MockSseMessage {
  readonly role: string
  readonly content: string
}

export interface MockSseChatResult {
  readonly text: string
  readonly chunks: number
  readonly usage?: { readonly input: number; readonly output: number }
}

function parsePositiveInteger(raw: string | undefined): number | undefined {
  if (raw === undefined || raw.trim() === "") return undefined
  const parsed = Number(raw)
  return Number.isInteger(parsed) && parsed > 0 ? parsed : undefined
}

/**
 * Resolve the worker's mock-SSE config from the spawn environment. Fails loudly
 * when the hub forgot to hand over the base URL.
 */
export function readMockSseConfig(env: Record<string, string | undefined>): MockSseConfig {
  const baseUrl = env[MOCK_SSE_URL_ENV]
  if (baseUrl === undefined || baseUrl.trim() === "") {
    throw new Error(`${MOCK_SSE_URL_ENV} is not set (the hub must pass the mock SSE base URL)`)
  }
  return {
    baseUrl,
    model: env[MOCK_SSE_MODEL_ENV] ?? "mock-model",
    apiKey: env[MOCK_SSE_API_KEY_ENV] ?? "test-key",
    burnIterations: parsePositiveInteger(env[MOCK_SSE_BURN_ENV]) ?? DEFAULT_BURN_ITERATIONS,
  }
}

/** Build the worker environment overlay handed to a spawned session worker. */
export function mockSseEnv(
  baseUrl: string,
  burnIterations: number,
  model?: string,
  apiKey?: string,
): Record<string, string> {
  return {
    [MOCK_SSE_URL_ENV]: baseUrl,
    [MOCK_SSE_BURN_ENV]: String(burnIterations),
    ...(model === undefined ? {} : { [MOCK_SSE_MODEL_ENV]: model }),
    ...(apiKey === undefined ? {} : { [MOCK_SSE_API_KEY_ENV]: apiKey }),
  }
}

/**
 * A deterministic, synchronous integer fold. It is intentionally opaque to the
 * optimiser (the running state feeds the next multiply) so the CPU cost is real
 * and a busy session genuinely pins its worker thread.
 */
export function burnCpu(iterations: number, seed: number): number {
  let state = seed >>> 0
  for (let index = 0; index < iterations; index++) {
    state = (Math.imul(state ^ (state >>> 15), 2246822519) + 0x9e3779b9) | 0
    state ^= state << 13
  }
  return state >>> 0
}

/** Consume the mock provider's streamed chat completion, entirely in-isolate. */
export async function consumeMockSse(
  bootstrap: MockSseBootstrap,
  messages: ReadonlyArray<MockSseMessage>,
): Promise<MockSseChatResult> {
  const response = await fetch(`${bootstrap.baseUrl}/chat/completions`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${bootstrap.apiKey}`,
    },
    body: JSON.stringify({ model: bootstrap.model, stream: true, messages }),
  })
  if (!response.ok) throw new Error(`mock SSE provider responded ${response.status}`)
  if (response.body === null) throw new Error("mock SSE provider returned an empty body")

  const accumulator = await readSseStream(response.body)
  return {
    text: accumulator.text,
    chunks: accumulator.chunks,
    ...(accumulator.usage === undefined ? {} : { usage: accumulator.usage }),
  }
}
