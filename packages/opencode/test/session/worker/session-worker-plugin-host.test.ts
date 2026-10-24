// Task 4 (Wave 1 follow-up) — the degraded (no hub-port) `ModelClient` path must
// run the worker-hosted plugin hooks, not the no-op pass-through, so a plugin's
// `chat.params` / `chat.headers` mutations reach the reduced LLM.
//
// Split out of `session-worker.test.ts` (mega-file item 23): this concern boots
// the degraded executor and asserts the reduced model path, with no shared state
// from the bootstrap/client/registry tests.

import { describe, expect, test } from "bun:test"
import { bootstrapSessionWorker, type SessionWorkerInit } from "@/session/worker/session-worker"
import type { WorkerPlugin } from "@/session/worker/plugin-host"
import { workerSnapshot } from "../../lib/snapshot"
import { startMockProvider } from "../../lib/mock-provider"

/** Poll a predicate until it is true, failing loud on timeout. */
async function waitUntil(
  predicate: () => boolean | Promise<boolean>,
  message: string,
  timeoutMs = 15_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await predicate()) return
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
  throw new Error(`timed out: ${message}`)
}

/** Run one degraded-boot session turn through the installed executor. */
async function runDegradedTurn(init: SessionWorkerInit): Promise<{ readonly type: string }> {
  const frames: Array<{ type?: string }> = []
  const scope = {
    postMessage(data: unknown) {
      frames.push(data as { type?: string })
    },
    onmessage: null as ((event: { data: unknown }) => void) | null,
  }
  bootstrapSessionWorker(init, scope)
  scope.onmessage!({ data: { type: "run", id: 1, sessionID: "ses_plugin", kind: "session" } })
  await waitUntil(
    () => frames.some((frame) => frame.type === "done" || frame.type === "error"),
    "executor never settled",
  )
  return frames.find((frame) => frame.type === "done" || frame.type === "error") as { type: string }
}

const degradedInit = (
  provider: { baseUrl: string; apiKey: string; model: string },
  plugins?: readonly WorkerPlugin[],
): SessionWorkerInit => ({
  snapshot: workerSnapshot(),
  env: { values: {} },
  role: "session",
  model: {
    providerID: "mock",
    modelID: provider.model,
    apiId: provider.model,
    npm: "@ai-sdk/openai-compatible",
    baseUrl: provider.baseUrl,
    apiKey: provider.apiKey,
    capabilities: { temperature: true, reasoning: false },
    limit: { context: 128_000, output: 4096 },
  },
  ...(plugins === undefined ? {} : { plugins }),
})

describe("degraded ModelClient worker plugin host (task 4)", () => {
  test("applies a worker-hosted chat.params hook in the reduced model path", async () => {
    await using provider = startMockProvider({ text: "ok" })
    const outcome = await runDegradedTurn(
      degradedInit(provider, [
        {
          name: "sampling",
          hooks: {
            "chat.params": (_input, output) => {
              output.temperature = 0.25
            },
          },
        },
      ]),
    )

    expect(outcome.type).toBe("done")
    expect(provider.requests[0].body.temperature).toBe(0.25)
  })

  test("uses the no-op Plugin when no hooks are shipped", async () => {
    await using provider = startMockProvider({ text: "ok" })
    const outcome = await runDegradedTurn(degradedInit(provider))

    expect(outcome.type).toBe("done")
    // Without shipped hooks the reduced path uses the no-op Plugin, so the
    // plugin's mutation is absent.
    expect(provider.requests[0].body.temperature).not.toBe(0.25)
  })
})
