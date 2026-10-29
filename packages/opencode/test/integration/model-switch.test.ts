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
import { Effect, Scope } from "effect"
import { createSessionBoundExecutorWorker, makeProduction } from "@/session/worker/hub"
import type { ProductionWorkerPoolOptions } from "@/session/worker/hub"
import type { ExecutorWorkerPool } from "@/session/worker/execute"
import type { ExecutorWorker } from "@/session/worker/execute"
import { pollUntil, sid } from "../lib/async"
import { sessionWorkerInit } from "../lib/session-worker-init"

const EXECUTOR_FIXTURE = new URL("../fixture/session-execute-worker.ts", import.meta.url)

/** The one scoped production pool; `terminateAll` closes the isolate cache. */
async function createProductionPool(options: ProductionWorkerPoolOptions): Promise<ExecutorWorkerPool> {
  const scope = Effect.runSync(Scope.make())
  const pools = await Effect.runPromise(Scope.provide(scope)(makeProduction(options)))
  return pools.executorPool
}

const executionOf = (value: unknown): number | undefined =>
  typeof value === "object" && value !== null && "execution" in value
    ? Number((value as { execution: unknown }).execution)
    : undefined

describe("model switch mid-session", () => {
  test("respawns the worker with the new model and reuses it while the model is stable", async () => {
    const sessionID = sid("ses_model_switch")
    let modelID = "model-a"
    const resolved: string[] = []
    const pool = await createProductionPool({
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
    const pool = await createProductionPool({
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

describe("production init single-flight", () => {
  test("concurrent first turns share one init build", async () => {
    const sessionID = sid("ses_init_single_flight")
    let builds = 0
    let releaseBuild!: () => void
    const buildGate = new Promise<void>((resolve) => {
      releaseBuild = resolve
    })
    const pool = await createProductionPool({
      init: async () => {
        builds += 1
        await buildGate
        return sessionWorkerInit()
      },
      entry: EXECUTOR_FIXTURE,
      workKind: "instant",
      timeoutMs: 15_000,
    })
    try {
      const first = pool.spawn()
      const second = pool.spawn()
      const runs = [
        first.execute({ sessionID, depth: 0 }),
        second.execute({ sessionID, depth: 0 }),
      ]
      await pollUntil(() => builds === 1, { message: "the init build never started" })
      releaseBuild()
      await Promise.all(runs)
      expect(builds).toBe(1)
    } finally {
      await pool.terminateAll()
    }
  }, 30_000)

  test("a failed init is shared by concurrent callers and retried by the next turn", async () => {
    const sessionID = sid("ses_init_retry")
    let builds = 0
    let shouldFail = true
    let releaseBuild!: () => void
    const buildGate = new Promise<void>((resolve) => {
      releaseBuild = resolve
    })
    const pool = await createProductionPool({
      init: async () => {
        builds += 1
        await buildGate
        if (shouldFail) {
          shouldFail = false
          throw new Error("init build failed")
        }
        return sessionWorkerInit()
      },
      entry: EXECUTOR_FIXTURE,
      workKind: "instant",
      timeoutMs: 15_000,
    })
    try {
      const first = pool.spawn()
      const second = pool.spawn()
      const failed = Promise.allSettled([
        first.execute({ sessionID, depth: 0 }),
        second.execute({ sessionID, depth: 0 }),
      ])
      await pollUntil(() => builds === 1, { message: "the init build never started" })
      releaseBuild()

      // Both concurrent callers see the same typed failure from one build.
      const outcomes = await failed
      expect(outcomes.map((outcome) => outcome.status)).toEqual(["rejected", "rejected"])
      for (const outcome of outcomes) {
        expect(outcome.status === "rejected" ? String(outcome.reason) : "").toContain("init build failed")
      }
      expect(builds).toBe(1)

      // The failure is not cached: the next turn rebuilds and succeeds.
      await first.execute({ sessionID, depth: 0 })
      expect(builds).toBe(2)
      expect(pool.resultFor(sessionID)?.value).toBeDefined()
    } finally {
      await pool.terminateAll()
    }
  }, 30_000)
})

describe("generation changes retire the isolate even within one session", () => {
  test("a same-session generation change calls the full generation retirement hook", async () => {
    const sessionID = sid("ses_same_session_generation")
    const retired: Array<[string, number | undefined]> = []
    const rebound: string[] = []
    const spawned: ExecutorWorker[] = []
    let generation = "model-a"
    const worker = createSessionBoundExecutorWorker(
      3,
      async (id) => {
        const created: ExecutorWorker = {
          id,
          alive: true,
          execute: async () => {},
          cancel: async () => {},
          assertNotBusy: async () => false,
          resultFor: () => undefined,
          terminate: async () => {},
        }
        spawned.push(created)
        return created
      },
      undefined,
      (id) => rebound.push(String(id)),
      undefined,
      async () => ({ generation }),
      (id, workerID) => {
        retired.push([String(id), workerID])
      },
    )
    try {
      await worker.execute({ sessionID, depth: 0 })
      expect(spawned).toHaveLength(1)
      generation = "model-b"
      await worker.execute({ sessionID, depth: 0 })
      // The old isolate was retired for the new model through the full
      // generation path (not the session-rebind path), with its worker id.
      expect(spawned).toHaveLength(2)
      expect(retired).toEqual([[String(sessionID), 3]])
      expect(rebound).toEqual([])
    } finally {
      await worker.terminate()
    }
  })

  test("a production pool routes a same-session generation change through the generation hook", async () => {
    const sessionID = sid("ses_production_generation_retire")
    const retired: Array<[string, number | undefined]> = []
    let generation = "model-a"
    const pool = await createProductionPool({
      init: () =>
        sessionWorkerInit({ model: { providerID: generation, modelID: generation, apiId: generation } }),
      generationOf: () => generation,
      onGenerationRetired: (id, workerID) => {
        retired.push([String(id), workerID])
      },
      entry: EXECUTOR_FIXTURE,
      workKind: "instant",
      timeoutMs: 15_000,
    })
    try {
      const worker = pool.spawn()
      await worker.execute({ sessionID, depth: 0 })
      generation = "model-b"
      await worker.execute({ sessionID, depth: 0 })
      expect(retired).toEqual([[String(sessionID), worker.id]])
    } finally {
      await pool.terminateAll()
    }
  }, 30_000)

  test("the cheap generation probe re-resolves the init after a model switch", async () => {
    const sessionID = sid("ses_probe_model_switch")
    // Simulates the production probe: the persisted last-user-message model is
    // read without the full init build. No requested model is carried.
    let persisted = "model-a"
    const resolved: string[] = []
    const pool = await createProductionPool({
      init: () => {
        resolved.push(persisted)
        return sessionWorkerInit({ model: { providerID: persisted, modelID: persisted, apiId: persisted } })
      },
      generationOf: () => persisted,
      entry: EXECUTOR_FIXTURE,
      workKind: "instant",
      timeoutMs: 15_000,
    })
    try {
      const worker = pool.spawn()
      await worker.execute({ sessionID, depth: 0 })
      expect(resolved).toEqual(["model-a"])
      expect(executionOf(pool.resultFor(sessionID)?.value)).toBe(1)

      persisted = "model-b"
      await worker.execute({ sessionID, depth: 0 })
      // The generation changed, so the init must be rebuilt for the NEW model,
      // never reused from the previous generation's cache entry.
      expect(resolved).toEqual(["model-a", "model-b"])
      expect(executionOf(pool.resultFor(sessionID)?.value)).toBe(1)
    } finally {
      await pool.terminateAll()
    }
  }, 30_000)

  test("the requested model keys the generation and the init is built once", async () => {
    const sessionID = sid("ses_requested_model")
    let builds = 0
    const pool = await createProductionPool({
      init: (_id, requested) => {
        builds += 1
        const selection = requested ?? { providerID: "model-a", modelID: "model-a" }
        return sessionWorkerInit({
          model: { providerID: selection.providerID, modelID: selection.modelID, apiId: selection.modelID },
        })
      },
      generationOf: (_id, requested) =>
        requested === undefined ? "model-a" : `${requested.providerID}/${requested.modelID}`,
      entry: EXECUTOR_FIXTURE,
      workKind: "instant",
      timeoutMs: 15_000,
    })
    try {
      const worker = pool.spawn()
      // The first turn after a model switch uses the requested model even though
      // the persisted session row still names the old one.
      await worker.execute({ sessionID, depth: 0, model: { providerID: "model-b", modelID: "model-b" } })
      expect(executionOf(pool.resultFor(sessionID)?.value)).toBe(1)
      expect(builds).toBe(1)

      // A repeated turn on the same generation reuses the cached init.
      await worker.execute({ sessionID, depth: 0, model: { providerID: "model-b", modelID: "model-b" } })
      expect(executionOf(pool.resultFor(sessionID)?.value)).toBe(2)
      expect(builds).toBe(1)
    } finally {
      await pool.terminateAll()
    }
  }, 30_000)
})
