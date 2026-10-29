// A model switch replaces one worker generation in place: the successor reuses
// the same `workerID`, so the retired generation's lease release must be scoped
// to the EXACT owner recorded when the generation claimed its lease. Re-reading
// the row at release time would match the successor's `worker-<id>-…` owner and
// zero its live lease.

import { describe, expect, test } from "bun:test"
import type {
  ExecutorRun,
  ExecutorWorker,
  ExecutorWorkerPool,
  SessionExecutionResult,
} from "@/session/worker/execute"
import { createSessionHub } from "@/session/worker/hub"
import { createTestBarrier } from "./lib/run-barrier-test"
import type { DbExecutor } from "@/session/worker/db-executor"
import { sid } from "./lib/async"

function poolWith(execute: (run: ExecutorRun) => Promise<void>): ExecutorWorkerPool {
  const results = new Map<string, SessionExecutionResult>()
  const workers: ExecutorWorker[] = []
  const makeWorker = (id: number): ExecutorWorker => ({
    id,
    alive: true,
    async execute(run) {
      await execute(run)
      results.set(String(run.sessionID), { sessionID: String(run.sessionID), value: {} } as SessionExecutionResult)
    },
    async cancel() {},
    async assertNotBusy() {
      return false
    },
    resultFor: (sessionID) => results.get(String(sessionID)),
    async terminate() {},
  })
  return {
    get workers() {
      return workers
    },
    spawn: (id) => {
      const worker = makeWorker(id ?? 7)
      workers.push(worker)
      return worker
    },
    terminateAll: async () => {},
    byID: (id) => workers.find((worker) => worker.id === id),
    resultFor: (sessionID) => results.get(String(sessionID)),
  }
}

/**
 * A fake `event_sequence` row whose owner can be swapped by the release hook.
 * The conditional update only zeroes the row when its CURRENT owner equals the
 * `worker_owner_id` parameter, exactly like `releaseRetiredWorkerLease`.
 */
function ownerDb(state: { owner: string; zeroed: Array<ReadonlyArray<unknown>> }): DbExecutor {
  return {
    threadId: 0,
    query: async (sql) => (sql.includes("event_sequence") ? [{ worker_owner_id: state.owner }] : []),
    run: async (sql, params) => {
      if (!sql.includes("worker_expires_at = 0") || params === undefined || !Array.isArray(params)) {
        return { changes: 0, lastInsertRowid: 0 }
      }
      const [sessionParam, ownerParam] = params
      if (ownerParam !== state.owner) return { changes: 0, lastInsertRowid: 0 }
      state.zeroed.push([sessionParam, ownerParam])
      state.owner = ""
      return { changes: 1, lastInsertRowid: 0 }
    },
    transaction: async () => [],
    close: async () => {},
  }
}

describe("generation retirement captures the exact retired lease owner", () => {
  test("a successor reusing the worker slot is never zeroed", async () => {
    const sessionID = sid("ses_generation_owner_capture")
    const state = { owner: "worker-1-1-old", zeroed: [] as Array<ReadonlyArray<unknown>> }
    const hub = createSessionHub({ ...{ barrier: createTestBarrier() },
      pool: poolWith(async () => {}),
      db: ownerDb(state),
      onGenerationReleased: () => {
        // Simulate the successor claiming the aggregate before the release task
        // runs. Its owner shares the retired generation's `worker-<id>-` prefix.
        state.owner = "worker-1-2-successor"
      },
      onWorkerCrashed: () => {},
    })
    try {
      // The generation claims its lease during its first dispatch; the hub
      // records that exact owner then, so retirement never re-reads a row the
      // successor may already hold.
      await hub.ensureRunning(sessionID)
      const workerID = hub.dispatcher.registry.entry(sessionID)!.workerID
      await hub.releaseGeneration(sessionID, workerID)
      expect(state.zeroed).toEqual([])
      expect(state.owner).toBe("worker-1-2-successor")
    } finally {
      await hub.terminateAll()
    }
  })

  test("the retired generation's exact owner is zeroed when it still holds the row", async () => {
    const sessionID = sid("ses_generation_owner_exact")
    const state = { owner: "worker-7-1-old", zeroed: [] as Array<ReadonlyArray<unknown>> }
    const hub = createSessionHub({ ...{ barrier: createTestBarrier() },
      pool: poolWith(async () => {}),
      db: ownerDb(state),
      onGenerationReleased: () => {},
      onWorkerCrashed: () => {},
    })
    try {
      await hub.releaseGeneration(sessionID, 7)
      expect(state.zeroed).toEqual([[sessionID, "worker-7-1-old"]])
    } finally {
      await hub.terminateAll()
    }
  })
})
