// A generation retirement's synchronous invariants must not sit behind the
// bounded async prelude. This test stalls the retired-owner read past the lease
// UPDATE deadline and proves:
//   - the ask epoch advances at retirement admission (the old ask dies at once),
//   - the successor is not admitted until the exact-owner capture lands,
//   - the (late) conditional lease UPDATE can never zero a successor's owner,
//   - the successor's ask survives the old generation's retirement.

import { describe, expect, test } from "bun:test"
import { Effect, Fiber } from "effect"
import type {
  ExecutorRun,
  ExecutorWorker,
  ExecutorWorkerPool,
  SessionExecutionResult,
} from "@/session/worker/execute"
import { createSessionHub } from "@/session/worker/hub"
import { createTestBarrier } from "./lib/run-barrier-test"
import type { DbExecutor } from "@/session/worker/db-executor"
import { askInGeneration, createGenerationAskTracker } from "@/session/worker/production-hub"
import { flush, sid } from "./lib/async"

function deferred<T>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((settle) => {
    resolve = settle
  })
  return { promise, resolve }
}

function poolWith(): ExecutorWorkerPool & { readonly spawns: number } {
  const results = new Map<string, SessionExecutionResult>()
  const workers: ExecutorWorker[] = []
  let spawns = 0
  const makeWorker = (id: number): ExecutorWorker => ({
    id,
    alive: true,
    async execute(run: ExecutorRun) {
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
    get spawns() {
      return spawns
    },
    get workers() {
      return workers
    },
    spawn: (id) => {
      spawns += 1
      const worker = makeWorker(id ?? 7)
      workers.push(worker)
      return worker
    },
    terminateAll: async () => {},
    byID: (id) => workers.find((worker) => worker.id === id),
    resultFor: (sessionID) => results.get(String(sessionID)),
  }
}

describe("generation retirement ordering", () => {
  test("a stalled owner capture delays admission and never zeroes the successor", async () => {
    const sessionID = sid("ses_generation_stalled_prelude")
    const tracker = createGenerationAskTracker()
    const ownerRead = deferred<ReadonlyArray<Record<string, unknown>>>()
    const zeroed: Array<ReadonlyArray<unknown>> = []
    const state = { owner: "" }
    const db: DbExecutor = {
      threadId: 0,
      query: async (sql) => (sql.includes("event_sequence") ? ownerRead.promise : []),
      run: async (sql, params) => {
        if (!sql.includes("worker_expires_at = 0") || params === undefined) {
          return { changes: 0, lastInsertRowid: 0 }
        }
        const values = Array.isArray(params) ? params : []
        if (values[1] !== state.owner) return { changes: 0, lastInsertRowid: 0 }
        zeroed.push([...values])
        state.owner = ""
        return { changes: 1, lastInsertRowid: 0 }
      },
      transaction: async () => [],
      close: async () => {},
    }
    const pool = poolWith()
    const hub = createSessionHub({ ...{ barrier: createTestBarrier() },
      pool,
      db,
      onGenerationReleased: (id) => {
        tracker.retire(id)
      },
      onWorkerCrashed: () => {},
      // Deliberately tiny: the capture stall exceeds this bound, proving the
      // barrier is not gated by the lease-UPDATE deadline.
      retiredLeaseReleaseTimeoutMs: 20,
    })
    try {
      await hub.dispatcher.dispatch({ sessionID })
      const workerID = hub.dispatcher.registry.entry(sessionID)!.workerID
      state.owner = `worker-${workerID}-1-old`

      // Generation A's ask is live before retirement.
      const oldAsk = Effect.runFork(askInGeneration(tracker, sessionID, Effect.never, () => "retired"))
      await flush()
      expect(tracker.pending(sessionID)).toBe(1)

      let admitted = false
      const retirement = hub.dispatcher.retireGeneration(sessionID, workerID).then(() => {
        admitted = true
      })
      await flush()

      // Epoch advanced synchronously, before the stalled capture resolved, and
      // the successor is NOT admitted while the prelude is pending.
      expect(tracker.current(sessionID)).toBe(1)
      expect(admitted).toBe(false)
      expect(tracker.pending(sessionID)).toBe(0)

      const oldExit = await Effect.runPromise(Fiber.await(oldAsk))
      expect(oldExit._tag).toBe("Failure")

      // Generation B's ask, begun after the advance, must survive A's retirement.
      const successorAsk = Effect.runFork(
        askInGeneration(tracker, sessionID, Effect.never, () => "retired"),
      )
      await flush()
      expect(tracker.pending(sessionID)).toBe(1)

      // A successor claimed the aggregate while the retired-owner read was
      // stalled. The late UPDATE is scoped to the captured old owner, so it
      // must leave the successor's live lease untouched.
      const successorOwner = `worker-${workerID}-2-successor`
      state.owner = successorOwner
      ownerRead.resolve([{ worker_owner_id: `worker-${workerID}-1-old` }])

      await retirement
      expect(admitted).toBe(true)
      expect(zeroed).toEqual([])
      expect(state.owner).toBe(successorOwner)
      expect(tracker.pending(sessionID)).toBe(1)

      await Effect.runPromise(Fiber.interrupt(successorAsk))
      expect(tracker.pending(sessionID)).toBe(0)
    } finally {
      await hub.terminateAll()
    }
  })
})
