// UNIT-SCOPED auto-resume policy. The four refusals (indeterminate, cancelled,
// cap, no committed message) and the one allow path all go through the real
// `createSessionHub`, but the executor pool is a structural fake and the test
// prepares the tracker directly. Production coverage (a real worker turn that
// commits/crashes) is `test/integration/lifecycle-production.test.ts`.

import { describe, expect, test } from "bun:test"
import type { SessionID } from "@/session/schema"
import type { AutoResumeDecision } from "@/session/ledger/resume"
import { AUTO_RESUME_CAP } from "@/session/ledger/resume"
import { createLifecycleTracker, type LifecycleTracker } from "@/session/ledger/lifecycle"
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

async function runCrash(
  hook: RunHook,
  prepare: (lifecycle: LifecycleTracker, sessionID: SessionID) => void,
  resume: (decision: Extract<AutoResumeDecision, { resume: true }>) => void,
): Promise<void> {
  const lifecycle = createLifecycleTracker()
  const sessionID = sid("ses_resume")
  prepare(lifecycle, sessionID)
  const hub = createSessionHub({
    pool: crashPool(lifecycle, hook),
    lifecycle,
    resumeRun: (decision) => resume(decision),
  })
  try {
    await settle(() => hub.ensureRunning(sessionID))
  } finally {
    await hub.terminateAll()
  }
}

const phaseStream: RunHook = (sessionID, lifecycle) => lifecycle.phase(sessionID, "stream")

describe("hub auto-resume wiring (C7 / D9)", () => {
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
})
