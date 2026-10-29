// Step 5: the dispatch core is a plain-Map/FiberSet/FiberMap Effect engine, the
// retired Deferred is owned by the retirement fiber's onExit, cancellation is
// classified from the cause, and the run's commit outcome rides `done` (F11).

import { describe, expect, test } from "bun:test"
import { readFileSync } from "node:fs"
import path from "node:path"
import { Cause, Effect } from "effect"
import type { SessionID } from "@/session/schema"
import type { RunBarrierCommitOutcome, RunBarrierRun } from "@/session/run-barrier"
import {
  Cancelled,
  cancelledFromCause,
  createDispatcher,
  type WorkerPool,
} from "@/session/worker/dispatch"
import {
  createSessionHub,
  type SessionHub,
} from "@/session/worker/hub"
import { createFakeExecutorPool, FakeSessionWorker } from "../../lib/fake-worker-pool"
import { capture, flush, pollUntil, sid } from "../../lib/async"
import { withParts } from "../../lib/session-parts"

const dispatchSource = readFileSync(path.join(import.meta.dir, "../../../src/session/worker/dispatch.ts"), "utf8")

describe("Step 5 dispatch idiom", () => {
  test("dispatch.ts has no unmanaged async: no Effect.run, new Promise or setTimeout", () => {
    expect(dispatchSource).not.toMatch(/Effect\.run/)
    expect(dispatchSource).not.toMatch(/new Promise/)
    expect(dispatchSource).not.toMatch(/setTimeout/)
    for (const removed of ["bridgeDeferred", "atomically", "trackFiber", "updatePhase"]) {
      expect(dispatchSource).not.toContain(removed)
    }
    expect(dispatchSource).toContain("FiberSet")
    expect(dispatchSource).toContain("FiberMap")
  })

  test("the retired Deferred completes once after release, and a successor waits for it", async () => {
    let confirmRelease!: () => void
    const releaseGate = new Promise<void>((resolve) => {
      confirmRelease = resolve
    })
    let releaseCalls = 0
    const spawned: FakeSessionWorker[] = []
    const pool: WorkerPool = {
      spawn: (id) => {
        const worker = new FakeSessionWorker(id ?? spawned.length + 1)
        spawned.push(worker)
        return worker
      },
      terminateAll: async () => {},
    }
    const dispatcher = createDispatcher({
      pool,
      onSessionReleased: async () => {
        await releaseGate
        releaseCalls += 1
      },
    })
    const sessionID = sid("ses_step5_retired_once")

    const first = dispatcher.dispatch({ sessionID })
    await flush()
    spawned[0].finish(sessionID)
    await first

    void dispatcher.retire(sessionID)
    const releaseA = dispatcher.awaitRelease(sessionID)
    const releaseB = dispatcher.awaitRelease(sessionID)
    const successor = capture(dispatcher.dispatch({ sessionID }))
    await flush()
    expect(releaseCalls).toBe(0)
    expect(spawned).toHaveLength(1)

    confirmRelease()
    await Promise.all([releaseA, releaseB])
    expect(releaseCalls).toBe(1)
    await pollUntil(() => spawned.length === 2, { message: "the successor never started after release" })
    spawned[1].finish(sessionID)
    expect(await successor).toEqual({ ok: true, value: undefined })
    // A later barrier observes the still-installed retirement without a second release.
    await dispatcher.awaitRelease(sessionID)
    expect(releaseCalls).toBe(1)
    await dispatcher.terminateAll()
  })

  test("cancellation is classified from Cause.hasInterruptsOnly, not an error string", () => {
    expect(cancelledFromCause(Cause.interrupt())).toBeInstanceOf(Cancelled)
    expect(cancelledFromCause(Cause.fail(new Error("cooperative stop")))).toBeUndefined()
    expect(cancelledFromCause(Cause.fail(new Error("All fibers interrupted without error")))).toBeUndefined()
  })

  test("F11: the barrier and the done reply carry the worker's committed outcome", async () => {
    const sessionID = sid("ses_step5_commit_done_first")
    const commit = "msg_done_before_events"
    const reports: Array<RunBarrierCommitOutcome | undefined> = []
    const runHandle: RunBarrierRun = {
      sessionID,
      report: () => true,
      commit: (messageID) => {
        reports.push({ committed: messageID })
        return true
      },
      projector: () => true,
      tool: () => true,
      noTools: () => true,
      noCommit: () => {
        reports.push({ committed: null })
        return true
      },
      promotion: () => true,
      reported: () => false,
      missing: () => ["commits", "tools"],
      commitOutcome: () => reports[reports.length - 1],
    }
    // A barrier that settles immediately: the test observes the report the hub
    // hands it, not a real 5 s drain.
    const barrier = {
      beginRun: () => Effect.succeed(runHandle),
      wait: () => Effect.succeed({ sessionID, status: "drained" as const }),
      cancel: () => Effect.void,
      state: () => "idle" as const,
      reset: () => Effect.void,
      abandon: () => Effect.void,
      get size() {
        return 0
      },
    }
    const pool = createFakeExecutorPool(1, { mode: "auto", value: withParts(sessionID) })
    // The worker's `done` reply arrives before any lifecycle.commit report; the
    // pool's resultFor is the one authoritative message the barrier consumes.
    pool.resultFor = (id: SessionID) => ({ sessionID: id, value: withParts(id), committed: commit })
    const hub: SessionHub = createSessionHub({ pool, barrier })

    const result = await hub.ensureRunning(sessionID)

    expect(reports).toEqual([{ committed: commit }])
    expect(result.committed).toBe(commit)
    await hub.terminateAll()
  })
})
