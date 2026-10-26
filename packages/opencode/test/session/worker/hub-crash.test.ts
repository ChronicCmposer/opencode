import { describe, expect, test } from "bun:test"
import type { SessionID } from "@/session/schema"
import { WorkerCrashedError } from "@/session/worker/dispatch"
import type {
  ExecutorRun,
  ExecutorWorker,
  ExecutorWorkerPool,
  SessionExecutionResult,
} from "@/session/worker/execute"
import { createSessionHub, isWorkerCrashed, workerCrashedFrom, type WorkerCrashed } from "@/session/worker/hub"

const payload = (sessionID: string): WorkerCrashed => ({
  sessionID,
  workerID: 7,
  phase: "stream",
  lastCommittedMessageID: "msg_1",
  inFlightCallIDs: ["call_1"],
  retryable: false,
  reason: "isolate exited",
})

const crashPool = (crash: unknown): ExecutorWorkerPool => {
  const results = new Map<SessionID, SessionExecutionResult>()
  const worker: ExecutorWorker = {
    id: 7,
    alive: true,
    async execute(_run: ExecutorRun) {
      throw crash
    },
    async cancel() {},
    async assertNotBusy() {
      return false
    },
    resultFor: (sessionID) => results.get(sessionID),
    async terminate() {},
  }
  return {
    workers: [worker],
    spawn: () => worker,
    terminateAll: async () => {},
    byID: (id) => (id === worker.id ? worker : undefined),
    resultFor: (sessionID) => results.get(sessionID),
  }
}

describe("D8 hub crash payload", () => {
  test("parses the typed contract and rejects malformed payloads", () => {
    expect(isWorkerCrashed(payload("ses_1"))).toBe(true)
    expect(isWorkerCrashed({ ...payload("ses_1"), retryable: true })).toBe(false)
    expect(isWorkerCrashed({ ...payload("ses_1"), phase: "unknown" })).toBe(false)
    expect(isWorkerCrashed({ ...payload("ses_1"), inFlightCallIDs: [1] })).toBe(false)
    expect(workerCrashedFrom({ cause: { workerCrashed: payload("ses_2") } })).toEqual(payload("ses_2"))
    expect(workerCrashedFrom(new Error("no payload"))).toBeUndefined()
  })

  test("surfaces and persists a typed crash payload while preserving the rejection", async () => {
    const sessionID = "ses_crash_contract"
    const expected = payload(sessionID)
    const crash = Object.assign(new WorkerCrashedError({ message: "worker 7 crashed", workerID: 7 }), {
      workerCrashed: expected,
    })
    const persisted: WorkerCrashed[] = []
    const hub = createSessionHub({
      pool: crashPool(crash),
      onWorkerCrashed: (value) => void persisted.push(value),
    })
    try {
      const settled = await hub.ensureRunning(sessionID as SessionID).then(
        () => ({ rejected: false as const }),
        (error: unknown) => ({ rejected: true as const, error }),
      )
      expect(settled.rejected).toBe(true)
      if (settled.rejected) {
        // The dispatcher wraps the crash so the typed payload is first-class
        // while the original isolate error stays reachable as `cause`.
        expect(settled.error).toBeInstanceOf(WorkerCrashedError)
        expect((settled.error as WorkerCrashedError).cause).toBe(crash)
        expect(workerCrashedFrom(settled.error)).toEqual(expected)
      }
      expect(persisted).toEqual([expected])
    } finally {
      await hub.terminateAll()
    }
  })
})
