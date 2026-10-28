// The dispatcher's asynchronous resume decision is
// bounded and cancel-safe, and a retirement's lease release is generation-scoped.
//
// A crash used to call `resumeDecision` and wait forever when the off-loop ledger
// read stalled: the session's next dispatch stayed parked and the dead run's
// promise never settled. The hub now bounds that decision; an unproven ledger
// read refuses auto-resume (`indeterminate`) so a side-effecting call is never
// replayed blind. Cancel always wins over a decision that resolves late.

import { describe, expect, test } from "bun:test"
import { WorkerCrashedError } from "@/session/worker/dispatch"
import type {
  ExecutorRun,
  ExecutorWorker,
  ExecutorWorkerPool,
  SessionExecutionResult,
} from "@/session/worker/execute"
import {
  boundedResumeDecision,
  createSessionHub,
  type WorkerCrashed,
} from "@/session/worker/hub"
import type { DbExecutor } from "@/session/worker/db-executor"
import type { SessionID } from "@/session/schema"
import { sid } from "../lib/async"
import { pollUntil } from "../lib/poll"

const SESSION = "ses_resume_deadline"

function crashPayload(sessionID: string): WorkerCrashed {
  return {
    sessionID,
    workerID: 7,
    phase: "stream",
    lastCommittedMessageID: "msg_committed",
    inFlightCallIDs: [],
    retryable: false,
    reason: "isolate exited",
  }
}

function crashError(sessionID: string): WorkerCrashedError {
  return Object.assign(new WorkerCrashedError({ message: "worker 7 crashed", workerID: 7 }), {
    workerCrashed: crashPayload(sessionID),
  })
}

/** A pool whose one worker settles `execute` through the supplied function. */
function poolWith(execute: (run: ExecutorRun) => Promise<void>): ExecutorWorkerPool & {
  readonly executed: SessionID[]
} {
  const executed: SessionID[] = []
  const worker: ExecutorWorker = {
    id: 7,
    alive: true,
    async execute(run) {
      executed.push(run.sessionID)
      return execute(run)
    },
    async cancel() {},
    async assertNotBusy() {
      return false
    },
    resultFor: () => undefined,
    async terminate() {},
  }
  return {
    executed,
    workers: [worker],
    spawn: () => worker,
    terminateAll: async () => {},
    byID: (id) => (id === worker.id ? worker : undefined),
    resultFor: () => undefined,
  }
}

function fakeDb(query: (sql: string) => Promise<ReadonlyArray<Record<string, unknown>>>): DbExecutor {
  return {
    run: async () => ({ changes: 0, lastInsertRowid: 0 }),
    query,
    transaction: async () => [],
    close: async () => {},
    threadId: 0,
  }
}

/** A promise plus its resolve/reject, so a test controls the exact ordering. */
function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (error: unknown) => void
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

describe("boundedResumeDecision", () => {
  test("passes a fast decision through unchanged", async () => {
    const allowed = {
      resume: true as const,
      sessionID: SESSION,
      attempt: 1,
      phase: "stream" as const,
      fromMessageID: "msg_committed",
    }
    expect(await boundedResumeDecision(async () => allowed, 1_000, () => allowed)).toBe(allowed)
  })

  test("an unresolved decision is refused as indeterminate, never parked", async () => {
    const started = Date.now()
    const decision = await boundedResumeDecision(
      () => new Promise(() => {}),
      25,
      () => ({ resume: false as const, sessionID: SESSION, phase: "stream" as const, reason: "indeterminate" as const }),
    )
    expect(decision).toEqual({ resume: false, sessionID: SESSION, phase: "stream", reason: "indeterminate" })
    expect(Date.now() - started).toBeLessThan(1_000)
  })
})

describe("a crashed run settles inside the resume deadline", () => {
  test("a stalled ledger read refuses auto-resume and rejects the run", async () => {
    const pool = poolWith(() => Promise.reject(crashError(SESSION)))
    const hub = createSessionHub({
      pool,
      // The retired-lease release reads `event_sequence`; only the crash-truth
      // read stalls, which is exactly the unresolved decision under test.
      db: fakeDb((sql) => (sql.includes("tool_call") ? new Promise(() => {}) : Promise.resolve([]))),
      resumeDecisionTimeoutMs: 25,
      onWorkerCrashed: () => {},
    })
    try {
      const started = Date.now()
      const outcome = await hub.ensureRunning(sid(SESSION)).then(
        () => ({ rejected: false as const }),
        (error: unknown) => ({ rejected: true as const, error }),
      )
      expect(outcome.rejected).toBe(true)
      if (outcome.rejected) expect(outcome.error).toBeInstanceOf(WorkerCrashedError)
      expect(Date.now() - started).toBeLessThan(1_000)
      // No auto-resume replayed the crashed run.
      expect(pool.executed.map(String)).toEqual(["ses_resume_deadline"])
    } finally {
      await hub.terminateAll()
    }
  })
})

describe("a resume decision that resolves after cancel cannot dispatch", () => {
  test("cancel wins over the late decision", async () => {
    const ledger = deferred<ReadonlyArray<Record<string, unknown>>>()
    const execute = deferred<never>()
    const pool = poolWith(() => execute.promise)
    const hub = createSessionHub({
      pool,
      db: fakeDb((sql) => (sql.includes("tool_call") ? ledger.promise : Promise.resolve([]))),
      resumeDecisionTimeoutMs: 1_000,
      onWorkerCrashed: () => {},
    })
    try {
      const run = hub.ensureRunning(sid(SESSION)).then(
        () => ({ rejected: false as const }),
        (error: unknown) => ({ rejected: true as const, error }),
      )
      await pollUntil(() => pool.executed.length === 1, { message: "run never reached the worker" })

      // The cancel lands while the resume decision is in flight.
      hub.lifecycle.cancel(SESSION)
      execute.reject(crashError(SESSION))
      ledger.resolve([])

      const outcome = await run
      expect(outcome.rejected).toBe(true)
      expect(hub.lifecycle.cancelled(SESSION)).toBe(true)
      // The late decision must not re-dispatch the cancelled run.
      expect(pool.executed.map(String)).toEqual(["ses_resume_deadline"])
    } finally {
      await hub.terminateAll()
    }
  })
})
