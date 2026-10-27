// UNIT-SCOPED hub dispatch policy. The hub's dispatcher must supply the crash
// phase, the committed-message truth and the in-flight tool calls instead of
// falling back to `phase: "stream"`. This drives the real `createSessionHub`
// factory, but the executor pool is a structural fake and the test feeds the
// real tracker directly. Production coverage (a real worker reporting into the
// hub) lives in `test/integration/lifecycle-production.test.ts`.

import { describe, expect, test } from "bun:test"
import type { SessionID } from "@/session/schema"
import type { ExecutorRun, ExecutorWorker, ExecutorWorkerPool, SessionExecutionResult } from "@/session/worker/execute"
import { WorkerCrashedError } from "@/session/worker/dispatch"
import { createSessionHub, type WorkerCrashed } from "@/session/worker/hub"
import { WORKER_CRASH_PHASES, type WorkerCrashPhase } from "@/session/ledger/crash"
import { createLifecycleTracker, type LifecycleTracker } from "@/session/ledger/lifecycle"
import { sid } from "../../lib/async"

interface CrashOptions {
  readonly sessionID: SessionID
  readonly lifecycle: LifecycleTracker
  readonly phase: WorkerCrashPhase
  readonly inFlight?: readonly string[]
}

/** An executor worker that records lifecycle truth, then crashes in `phase`. */
function crashWorker(id: number, options: CrashOptions): ExecutorWorker {
  const results = new Map<SessionID, SessionExecutionResult>()
  return {
    id,
    alive: false,
    async execute(run: ExecutorRun) {
      options.lifecycle.phase(run.sessionID, options.phase)
      for (const callID of options.inFlight ?? []) options.lifecycle.toolStarted(run.sessionID, callID)
      throw new WorkerCrashedError({ message: `worker ${id} crashed`, workerID: id })
    },
    async cancel() {},
    async assertNotBusy() {
      return false
    },
    resultFor: (sessionID) => results.get(sessionID),
    async terminate() {},
  }
}

function crashPool(options: Omit<CrashOptions, "sessionID">): ExecutorWorkerPool {
  const workers: ExecutorWorker[] = []
  return {
    get workers() {
      return workers
    },
    spawn() {
      const worker = crashWorker(workers.length + 1, { ...options, sessionID: sid("ses_unused") })
      workers.push(worker)
      return worker
    },
    async terminateAll() {},
    byID: (id) => workers.find((worker) => worker.id === id),
    resultFor: () => undefined,
  }
}

describe("hub lifecycle wiring (M3 / D8)", () => {
  test("every crash phase reaches the D8 payload from the hub's dispatcher", async () => {
    for (const phase of WORKER_CRASH_PHASES) {
      const lifecycle = createLifecycleTracker()
      const pool = crashPool({ lifecycle, phase })
      const persisted: WorkerCrashed[] = []
      const hub = createSessionHub({
        pool,
        lifecycle,
        onWorkerCrashed: (payload) => void persisted.push(payload),
      })
      const sessionID = sid(`ses_phase_${phase}`)
      if (phase !== "boot") lifecycle.commit(sessionID, "msg_committed")

      try {
        const error = await hub.ensureRunning(sessionID).then(
          () => undefined,
          (failure: unknown) => failure,
        )
        expect(error).toBeInstanceOf(WorkerCrashedError)
        const payload = (error as WorkerCrashedError).payload
        expect(payload).toMatchObject({
          sessionID,
          phase,
          retryable: false,
          lastCommittedMessageID: phase === "boot" ? null : "msg_committed",
        })
        expect(persisted.at(-1)).toMatchObject({ sessionID, phase })
      } finally {
        await hub.terminateAll()
      }
    }
  })

  test("a crash after commit carries the committed id and the in-flight tool call", async () => {
    const lifecycle = createLifecycleTracker()
    const pool = crashPool({ lifecycle, phase: "tool", inFlight: ["call_write"] })
    const hub = createSessionHub({ pool, lifecycle })
    const sessionID = sid("ses_after_commit")
    lifecycle.commit(sessionID, "msg_9")

    try {
      const error = await hub.ensureRunning(sessionID).then(
        () => undefined,
        (failure: unknown) => failure,
      )
      const payload = (error as WorkerCrashedError).payload
      expect(payload).toMatchObject({
        phase: "tool",
        lastCommittedMessageID: "msg_9",
        inFlightCallIDs: ["call_write"],
      })
      expect(lifecycle.crashState(sessionID).inFlightCallIDs).toEqual(["call_write"])
    } finally {
      await hub.terminateAll()
    }
  })
})
