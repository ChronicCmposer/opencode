// Phase 9.6 / 14 — parent CRASH = implicit cancel (plan task 9.6, decision line 53).
//
// A subagent parent that CRASHES must implicitly cancel its children, and the
// cascade is children-first, bounded by the same grace window as an explicit
// cancel. The dispatcher reads the `parentID → children` index through the
// landed `CancellationConfig.childrenOf` seam (wired to the Phase 9 job
// registry), so no new index is introduced here.
//
// These tests drive the dispatcher against an in-memory `WorkerPool` fake, so
// the crash/cascade ordering is deterministic and no isolates are spawned.

import { describe, expect, test } from "bun:test"
import type { SessionID } from "@/session/schema"
import {
  createDispatcher,
  SessionCancelledError,
  WorkerCrashedError,
  type CancellationConfig,
  type SessionRun,
  type SessionWorker,
  type WorkerKind,
  type WorkerPool,
} from "@/session/worker/dispatch"

const sid = (value: string) => value as SessionID
const flush = () => new Promise<void>((resolve) => setTimeout(resolve, 0))
/** Attach a rejection handler up-front so a later settle cannot go unhandled. */
const capture = (promise: Promise<unknown>): Promise<unknown> =>
  promise.then(
    () => undefined,
    (error: unknown) => error,
  )

interface PendingRun {
  readonly resolve: () => void
  readonly reject: (error: unknown) => void
}

/** A worker whose runs the test completes/crashes explicitly (no real threads). */
class FakeWorker implements SessionWorker {
  alive = true
  readonly runs: SessionRun[] = []
  private readonly pending = new Map<SessionID, PendingRun>()

  constructor(readonly id: number) {}

  execute(run: SessionRun): Promise<void> {
    this.runs.push(run)
    return new Promise<void>((resolve, reject) => {
      this.pending.set(run.sessionID, { resolve, reject })
    })
  }

  crash(sessionID: SessionID): void {
    this.alive = false
    const pending = this.pending.get(sessionID)
    this.pending.delete(sessionID)
    pending?.reject(new WorkerCrashedError({ message: `worker ${this.id} crashed`, workerID: this.id }))
  }

  async terminate(): Promise<void> {
    this.alive = false
    const pending = [...this.pending.values()]
    this.pending.clear()
    for (const run of pending) run.reject(new SessionCancelledError({ message: "terminated", sessionID: "" }))
  }
}

class FakePool implements WorkerPool {
  readonly workers: FakeWorker[]
  readonly spawned: FakeWorker[] = []
  private nextSpawnId = 100

  constructor(k: number) {
    this.workers = Array.from({ length: k }, (_, index) => new FakeWorker(index + 1))
  }

  spawn(_kind: WorkerKind): SessionWorker {
    const worker = new FakeWorker(this.nextSpawnId++)
    this.spawned.push(worker)
    return worker
  }

  async terminateAll(): Promise<void> {
    await Promise.all(this.workers.map((worker) => worker.terminate()))
  }
}

/** A `childrenOf` index backed by a plain map, plus the cancelled-session log. */
function cancellation(children: Record<string, string[]>): {
  config: CancellationConfig
  cancelled: string[]
} {
  const cancelled: string[] = []
  const config: CancellationConfig = {
    childrenOf: (parent) => (children[parent] ?? []).map(sid),
    onCancelled: (event) => cancelled.push(event.sessionID),
  }
  return { config, cancelled }
}

describe("9.6 parent CRASH implicitly cancels its children (children-first)", () => {
  test("a crashed parent cancels its running subagent child", async () => {
    const pool = new FakePool(1)
    const { config, cancelled } = cancellation({ ses_parent: ["ses_child"] })
    const dispatcher = createDispatcher({ pool, cancellation: config })

    const parent = capture(dispatcher.dispatch({ sessionID: sid("ses_parent") }))
    const child = capture(dispatcher.dispatch({ sessionID: sid("ses_child"), depth: 1 }))
    await flush()
    expect(dispatcher.headroomSlotCount).toBe(1)

    pool.workers[0].crash(sid("ses_parent"))

    expect(await parent).toBeInstanceOf(WorkerCrashedError)
    expect(await child).toBeInstanceOf(SessionCancelledError)
    expect(dispatcher.registry.has(sid("ses_child"))).toBe(false)
    expect(cancelled).toContain(sid("ses_child"))

    await dispatcher.terminateAll()
  })

  test("the cascade reaches grandchildren (children-first recursion)", async () => {
    const pool = new FakePool(1)
    const { config, cancelled } = cancellation({
      ses_parent: ["ses_child"],
      ses_child: ["ses_grandchild"],
    })
    const dispatcher = createDispatcher({ pool, cancellation: config })

    const parent = capture(dispatcher.dispatch({ sessionID: sid("ses_parent") }))
    const child = capture(dispatcher.dispatch({ sessionID: sid("ses_child"), depth: 1 }))
    const grandchild = capture(dispatcher.dispatch({ sessionID: sid("ses_grandchild"), depth: 2 }))
    await flush()
    expect(dispatcher.headroomSlotCount).toBe(2)

    pool.workers[0].crash(sid("ses_parent"))

    expect(await parent).toBeInstanceOf(WorkerCrashedError)
    expect(await child).toBeInstanceOf(SessionCancelledError)
    expect(await grandchild).toBeInstanceOf(SessionCancelledError)
    expect(cancelled).toContain(sid("ses_child"))
    expect(cancelled).toContain(sid("ses_grandchild"))

    await dispatcher.terminateAll()
  })

  test("no cancellation config means a crash never cascades (flag-off behavior)", async () => {
    const pool = new FakePool(1)
    const dispatcher = createDispatcher({ pool })

    const parent = capture(dispatcher.dispatch({ sessionID: sid("ses_parent") }))
    await flush()

    pool.workers[0].crash(sid("ses_parent"))
    expect(await parent).toBeInstanceOf(WorkerCrashedError)

    await dispatcher.terminateAll()
  })
})
