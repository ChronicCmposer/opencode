// A mid-session model switch must respawn the worker with the new
// model instead of surfacing `WorkerModelMismatchError` on the next turn.
//
// Observable seams:
//   * `init` is re-resolved when the session's model changes (the resolver is the
//     only place the persisted model is read);
//   * the fixture worker's module-level `executions` counter resets to 1 only
//     when a *fresh isolate* serves the run, so `execution === 1` after the
//     switch proves the old isolate was retired rather than reused with a stale
//     reduced provider.

import { describe, expect, test } from "bun:test"
import { createProductionWorkerPool } from "@/session/worker/hub"
import { sid } from "../lib/async"
import { sessionWorkerInit } from "../lib/session-worker-init"

const EXECUTOR_FIXTURE = new URL("../fixture/session-execute-worker.ts", import.meta.url)

const executionOf = (value: unknown): number | undefined =>
  typeof value === "object" && value !== null && "execution" in value
    ? Number((value as { execution: unknown }).execution)
    : undefined

describe("model switch mid-session", () => {
  test("respawns the worker with the new model and reuses it while the model is stable", async () => {
    const sessionID = sid("ses_model_switch")
    let modelID = "model-a"
    const resolved: string[] = []
    const pool = createProductionWorkerPool({
      init: (id) => {
        const resolvedModel = String(id) === String(sessionID) ? modelID : `${modelID}-other`
        resolved.push(resolvedModel)
        return sessionWorkerInit({
          model: { providerID: resolvedModel, modelID: resolvedModel, apiId: resolvedModel },
        })
      },
      entry: EXECUTOR_FIXTURE,
      workKind: "instant",
      timeoutMs: 15_000,
    })
    try {
      const worker = pool.spawn()

      await worker.execute({ sessionID, depth: 0 })
      expect(resolved.every((model) => model === "model-a")).toBe(true)
      expect(executionOf(pool.resultFor(sessionID)?.value)).toBe(1)

      // Same model: the isolate is reused, so its execution counter advances.
      await worker.execute({ sessionID, depth: 0 })
      expect(resolved.every((model) => model === "model-a")).toBe(true)
      expect(executionOf(pool.resultFor(sessionID)?.value)).toBe(2)

      // The user switches the model mid-session. The next dispatch must resolve
      // the new model and serve it from a fresh isolate (counter resets).
      modelID = "model-b"
      await worker.execute({ sessionID, depth: 0 })
      expect(resolved.at(-1)).toBe("model-b")
      expect(executionOf(pool.resultFor(sessionID)?.value)).toBe(1)
    } finally {
      await pool.terminateAll()
    }
  }, 30_000)

  test("a switch back to the original model respawns again", async () => {
    const sessionID = sid("ses_model_switch_back")
    let modelID = "model-a"
    const pool = createProductionWorkerPool({
      init: () => sessionWorkerInit({ model: { providerID: modelID, modelID, apiId: modelID } }),
      entry: EXECUTOR_FIXTURE,
      workKind: "instant",
      timeoutMs: 15_000,
    })
    try {
      const worker = pool.spawn()
      await worker.execute({ sessionID, depth: 0 })
      expect(executionOf(pool.resultFor(sessionID)?.value)).toBe(1)

      modelID = "model-b"
      await worker.execute({ sessionID, depth: 0 })
      expect(executionOf(pool.resultFor(sessionID)?.value)).toBe(1)

      modelID = "model-a"
      await worker.execute({ sessionID, depth: 0 })
      expect(executionOf(pool.resultFor(sessionID)?.value)).toBe(1)
    } finally {
      await pool.terminateAll()
    }
  }, 30_000)
})
