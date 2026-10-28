// UNIT-SCOPED auto-resume policy. The refusals (indeterminate, cancelled, cap,
// boot/no committed message) and the allow paths all go through the real
// `createSessionHub` resume wiring, but the executor pool is a structural fake
// and the test prepares the tracker directly. Real coverage (a real worker turn
// that commits/crashes, the durable `tool_call` row, and the duplicate-side-
// effect window) is `test/integration/resume-ledger.test.ts`.

import { describe, expect, test } from "bun:test"
import type { SessionID } from "@/session/schema"
import type { AutoResumeDecision } from "@/session/ledger/resume"
import { AUTO_RESUME_CAP } from "@/session/ledger/resume"
import { createLifecycleTracker, type LifecycleTracker } from "@/session/ledger/lifecycle"
import type { DbExecutor } from "@/session/worker/db-executor"
import { WorkerCrashedError } from "@/session/worker/dispatch"
import type { ExecutorRun, ExecutorWorker, ExecutorWorkerPool, SessionExecutionResult } from "@/session/worker/execute"
import { createSessionHub } from "@/session/worker/hub"
import { sid } from "../../lib/async"

type RunHook = (sessionID: SessionID, lifecycle: LifecycleTracker) => void

function crashPool(lifecycle: LifecycleTracker, hook: RunHook): ExecutorWorkerPool {
  const workers: ExecutorWorker[] = []
  const make = (): ExecutorWorker => {
    const id = workers.length + 1
    const results = new Map<SessionID, SessionExecutionResult>()
    const worker: ExecutorWorker = {
      id,
      alive: false,
      async execute(run: ExecutorRun) {
        hook(run.sessionID, lifecycle)
        throw new WorkerCrashedError({ message: `worker ${id} crashed`, workerID: id })
      },
      async cancel() {},
      async assertNotBusy() {
        return false
      },
      resultFor: (sessionID) => results.get(sessionID),
      async terminate() {},
    }
    workers.push(worker)
    return worker
  }
  return {
    get workers() {
      return workers
    },
    spawn: () => make(),
    async terminateAll() {},
    byID: (id) => workers.find((worker) => worker.id === id),
    resultFor: () => undefined,
  }
}

async function settle(fn: () => Promise<unknown>): Promise<void> {
  await fn().catch(() => undefined)
}

function fakeDb(rows: ReadonlyArray<Record<string, unknown>>): DbExecutor {
  return {
    threadId: 0,
    run: async () => ({ changes: 0, lastInsertRowid: 0 }),
    query: async () => rows,
    transaction: async () => [],
    close: async () => {},
  }
}

async function runCrash(
  hook: RunHook,
  prepare: (lifecycle: LifecycleTracker, sessionID: SessionID) => void,
  resume: (decision: Extract<AutoResumeDecision, { resume: true }>) => void,
  db?: DbExecutor,
): Promise<void> {
  const lifecycle = createLifecycleTracker()
  const sessionID = sid("ses_resume")
  prepare(lifecycle, sessionID)
  const hub = createSessionHub({
    pool: crashPool(lifecycle, hook),
    lifecycle,
    resumeRun: (decision) => resume(decision),
    ...(db === undefined ? {} : { db }),
  })
  try {
    await settle(() => hub.ensureRunning(sessionID))
  } finally {
    await hub.terminateAll()
  }
}

const phaseStream: RunHook = (sessionID, lifecycle) => lifecycle.phase(sessionID, "stream")

describe("hub auto-resume wiring (D9)", () => {
  test("resumes from the last committed message when no tool started", async () => {
    const decisions: Parameters<NonNullable<Parameters<typeof createSessionHub>[0]["resumeRun"]>>[0][] = []
    await runCrash(
      phaseStream,
      (lifecycle, sessionID) => lifecycle.commit(sessionID, "msg_committed"),
      (decision) => decisions.push(decision),
    )
    expect(decisions).toEqual([
      { resume: true, fromMessageID: "msg_committed", attempt: 1, sessionID: "ses_resume", phase: "stream" },
    ])
  })

  test("an indeterminate side-effecting call blocks the resume", async () => {
    const decisions: string[] = []
    await runCrash(
      (sessionID, lifecycle) => {
        lifecycle.commit(sessionID, "msg_committed")
        lifecycle.toolStarted(sessionID, "call_write")
        lifecycle.phase(sessionID, "tool")
      },
      (lifecycle, sessionID) => lifecycle.commit(sessionID, "msg_committed"),
      (decision) => decisions.push(decision.fromMessageID),
    )
    expect(decisions).toEqual([])
  })

  test("cancel wins over an otherwise resumable crash", async () => {
    const decisions: string[] = []
    await runCrash(
      (sessionID, lifecycle) => {
        lifecycle.cancel(sessionID)
        lifecycle.phase(sessionID, "stream")
      },
      (lifecycle, sessionID) => lifecycle.commit(sessionID, "msg_committed"),
      (decision) => decisions.push(decision.fromMessageID),
    )
    expect(decisions).toEqual([])
  })

  test("the cap is ten attempts per run", async () => {
    const decisions: string[] = []
    const lifecycle = createLifecycleTracker()
    const sessionID = sid("ses_cap")
    lifecycle.commit(sessionID, "msg_committed")
    for (let attempt = 0; attempt < AUTO_RESUME_CAP; attempt++) lifecycle.recordAttempt(sessionID)
    const hub = createSessionHub({
      pool: crashPool(lifecycle, phaseStream),
      lifecycle,
      resumeRun: (decision) => {
        decisions.push(decision.fromMessageID)
      },
    })
    try {
      await settle(() => hub.ensureRunning(sessionID))
    } finally {
      await hub.terminateAll()
    }
    expect(decisions).toEqual([])
    expect(lifecycle.attempts(sessionID)).toBe(AUTO_RESUME_CAP)
  })

  test("a crash with no committed message never resumes", async () => {
    const decisions: string[] = []
    await runCrash(phaseStream, () => undefined, (decision) => decisions.push(decision.fromMessageID))
    expect(decisions).toEqual([])
  })

  test("boot never resumes; materialize/stream/tool/commit resume from the committed message", async () => {
    for (const phase of ["boot", "materialize", "stream", "tool", "commit"] as const) {
      const decisions: string[] = []
      await runCrash(
        (sessionID, lifecycle) => lifecycle.phase(sessionID, phase),
        (lifecycle, sessionID) => lifecycle.commit(sessionID, "msg_committed"),
        (decision) => decisions.push(decision.phase),
      )
      expect(decisions).toEqual(phase === "boot" ? [] : [phase])
    }
  })

  test("the durable started rows override a stale in-memory mirror", async () => {
    const decisions: string[] = []
    await runCrash(
      (sessionID, lifecycle) => {
        lifecycle.toolStarted(sessionID, "call_done")
        lifecycle.phase(sessionID, "stream")
      },
      (lifecycle, sessionID) => lifecycle.commit(sessionID, "msg_committed"),
      (decision) => decisions.push(decision.fromMessageID),
      fakeDb([]),
    )
    expect(decisions).toEqual(["msg_committed"])
  })

  test("an orphan durable started row blocks resume with an empty mirror", async () => {
    const decisions: string[] = []
    await runCrash(
      phaseStream,
      (lifecycle, sessionID) => lifecycle.commit(sessionID, "msg_committed"),
      (decision) => decisions.push(decision.fromMessageID),
      fakeDb([{ call_id: "call_orphan", side_effecting: 1 }]),
    )
    expect(decisions).toEqual([])
  })
})

describe("async resume ownership (D9)", () => {
  test("a dispatch while the resume decision is pending parks instead of racing the crash", async () => {
    const lifecycle = createLifecycleTracker()
    const sessionID = sid("ses_pending_decision")
    lifecycle.commit(sessionID, "msg_committed")
    lifecycle.phase(sessionID, "stream")

    let decisionStarted!: () => void
    const started = new Promise<void>((resolve) => {
      decisionStarted = resolve
    })
    let resolveRows!: (rows: ReadonlyArray<Record<string, unknown>>) => void
    const pendingRows = new Promise<ReadonlyArray<Record<string, unknown>>>((resolve) => {
      resolveRows = resolve
    })
    const db: DbExecutor = {
      threadId: 0,
      run: async () => ({ changes: 0, lastInsertRowid: 0 }),
      query: async () => {
        decisionStarted()
        return pendingRows
      },
      transaction: async () => [],
      close: async () => {},
    }

    const event = { starts: 0, active: 0, maxActive: 0 }
    const spawned: ExecutorWorker[] = []
    const pool: ExecutorWorkerPool = {
      get workers() {
        return spawned
      },
      spawn: () => {
        const id = spawned.length + 1
        const worker: ExecutorWorker = {
          id,
          alive: true,
          async execute() {
            event.starts += 1
            event.active += 1
            event.maxActive = Math.max(event.maxActive, event.active)
            try {
              if (event.starts === 1) {
                throw new WorkerCrashedError({ message: `worker ${id} crashed`, workerID: id })
              }
            } finally {
              event.active -= 1
            }
          },
          async cancel() {},
          async assertNotBusy() {
            return false
          },
          resultFor: () => undefined,
          async terminate() {},
        }
        spawned.push(worker)
        return worker
      },
      async terminateAll() {},
      byID: (id) => spawned.find((worker) => worker.id === id),
      resultFor: () => undefined,
    }

    const hub = createSessionHub({ pool, lifecycle, resumeRun: () => {}, db })
    try {
      const crashed = hub.ensureRunning(sessionID).catch((error: unknown) => error)
      await started

      // The worker is retired and the durable decision is still pending. A
      // different run must park behind the crashed generation, never start.
      const parked = hub.dispatcher.dispatch({ sessionID, depth: 1 })
      expect(event.starts).toBe(1)
      expect(hub.dispatcher.queuedCount).toBe(1)

      resolveRows([])
      expect(await crashed).toBeInstanceOf(WorkerCrashedError)
      await parked

      // The decision settles the crash exactly once, then promotes the waiter.
      expect(event.starts).toBe(2)
      expect(event.maxActive).toBe(1)
      expect(hub.dispatcher.queuedCount).toBe(0)
    } finally {
      resolveRows([])
      await hub.terminateAll()
    }
  })
})
