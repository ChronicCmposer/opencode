// Phase 6.1 / 6.2 — reduced session layer inside a real worker isolate.
//
// Proves the reduced layer (node table + client nodes + snapshot) boots in a
// Bun worker and runs the pipeline against the hub's mock provider, and that
// two workers resolve their snapshots independently without touching
// `process.env`.

import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { testEffect } from "../../lib/effect"
import { TestLLMServer } from "../../lib/llm-server"
import { createWorkerHarness } from "../../lib/worker-harness"
import { workerSnapshot } from "../../lib/snapshot"
import type { WorkerSnapshot } from "@/session/worker/layer"

const it = testEffect(TestLLMServer.layer)
const LAYER_WORKER = new URL("../../fixture/session-layer-worker.ts", import.meta.url)

interface PipelineResult {
  readonly text: string
  readonly publishedSeq: number | null
  readonly emitted: number
}

type WorkerOut =
  | { type: "ready" }
  | { type: "result"; id: number; value: PipelineResult }
  | { type: "snapshot-result"; id: number; value: { epoch: number; directory: string; env: Record<string, string> } }
  | { type: "error"; id: number; message: string }

const initMessage = (llmUrl: string, snapshotValue: WorkerSnapshot) => ({
  type: "init" as const,
  snapshot: snapshotValue,
  env: { values: { OPENCODE_TEST_WORKER_KEY: "worker-local" } },
  model: { baseUrl: llmUrl, apiKey: "test-key", model: "test-model" },
})

describe("reduced session layer in a worker (6.1)", () => {
  it.live("boots and runs the pipeline against the hub's mock provider", () =>
    Effect.gen(function* () {
      const llm = yield* TestLLMServer
      yield* llm.text("hello from the reduced layer")

      const result = yield* Effect.gen(function* () {
        const harness = createWorkerHarness()
        try {
          const worker = harness.spawn<WorkerOut, unknown>(LAYER_WORKER)
          worker.post(initMessage(llm.url, workerSnapshot()))
          yield* Effect.promise(() => worker.waitFor("ready"))

          worker.post({
            type: "run",
            id: 1,
            sessionID: "s1",
            messages: [{ role: "user", content: "hi" }],
          })
          const message = yield* Effect.promise(() => worker.waitFor((m) => m.type === "result" || m.type === "error"))
          if (message.type === "error") throw new Error(`worker failed: ${message.message}`)
          if (message.type !== "result") throw new Error("worker did not return a result")
          return message.value
        } finally {
          yield* Effect.promise(() => harness.dispose())
        }
      })

      expect(result.text).toBe("hello from the reduced layer")
      expect(result.publishedSeq).toBe(1)
      expect(result.emitted).toBe(1)
      yield* llm.wait(1)
    }),
  )
})

describe("per-worker snapshot isolation (6.2)", () => {
  it.live("two workers resolve different snapshots with no process.env bleed", () =>
    Effect.gen(function* () {
      const llm = yield* TestLLMServer
      const key = "OPENCODE_TEST_WORKER_KEY"
      const before = process.env[key]

      const readSnapshot = (directory: string, epoch: number) =>
        Effect.gen(function* () {
          const harness = createWorkerHarness()
          try {
            const worker = harness.spawn<WorkerOut, unknown>(LAYER_WORKER)
            worker.post(initMessage(llm.url, workerSnapshot({ directory, epoch })))
            yield* Effect.promise(() => worker.waitFor("ready"))
            worker.post({ type: "snapshot", id: 1 })
            const message = yield* Effect.promise(() =>
              worker.waitFor((m) => m.type === "snapshot-result" || m.type === "error"),
            )
            if (message.type === "error") throw new Error(`worker failed: ${message.message}`)
            if (message.type !== "snapshot-result") throw new Error("worker did not return a snapshot")
            return message.value
          } finally {
            yield* Effect.promise(() => harness.dispose())
          }
        })

      const [a, b] = yield* Effect.all([readSnapshot("/tmp/worker-a", 1), readSnapshot("/tmp/worker-b", 2)], {
        concurrency: "unbounded",
      })

      expect(a).toEqual({ epoch: 1, directory: "/tmp/worker-a", env: { [key]: "worker-local" } })
      expect(b).toEqual({ epoch: 2, directory: "/tmp/worker-b", env: { [key]: "worker-local" } })
      expect(process.env[key]).toBe(before)
    }),
  )
})

describe("worker fixture contract", () => {
  test("the snapshot payload is structured-cloneable", () => {
    const value = workerSnapshot()
    expect(structuredClone(value)).toEqual(value)
  })
})

// ---------------------------------------------------------------------------
// Bootstrap integration seam — the real session-worker entry must transfer its
// dedicated egress CONTROL port alongside the data port, so the egress mesh
// (durable ack/resync) is live in production and not silently dead.
// ---------------------------------------------------------------------------

const SESSION_WORKER_ENTRY = new URL("../../../src/session/worker/session-worker.ts", import.meta.url)

type BootstrapOut =
  | { type: "ready" }
  | { type: "job-port"; port: MessagePort }
  | { type: "ports"; egress: MessagePort; egressControl: MessagePort }
  | { type: "hub-proxy"; port: MessagePort }

describe("session-worker bootstrap isolate (egress control)", () => {
  it.live("the entry transfers a dedicated egress control port alongside the data port", () =>
    Effect.gen(function* () {
      const harness = createWorkerHarness()
      try {
        const worker = harness.spawn<BootstrapOut, never>(SESSION_WORKER_ENTRY, {
          workerData: {
            snapshot: workerSnapshot(),
            env: { values: {} },
            role: "session",
            model: {
              providerID: "mock",
              modelID: "m",
              apiId: "m",
              baseUrl: "http://127.0.0.1:1/v1",
              apiKey: "k",
              capabilities: { temperature: true, reasoning: false },
              limit: { context: 128_000, output: 4096 },
            },
            capabilities: { sessionWorkerPorts: true },
          },
        })
        const ports = yield* Effect.promise(() => worker.waitFor("ports"))
        expect(ports.egress).toBeInstanceOf(MessagePort)
        expect(ports.egressControl).toBeInstanceOf(MessagePort)
        expect(ports.egressControl).not.toBe(ports.egress)
        yield* Effect.promise(() => worker.waitFor("ready"))
      } finally {
        yield* Effect.promise(() => harness.dispose())
      }
    }),
  )
})
