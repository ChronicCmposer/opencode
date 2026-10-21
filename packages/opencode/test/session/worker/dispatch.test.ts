// Phase 4.3–4.9 — dispatch scheduler core (plan tasks 4.3–4.9).
//
// One active session per worker; K busy queues FIFO; the admission queue is
// bounded; on-demand headroom is reserved for depth-1 children; a crash rejects
// at-most-once and respawns; the key→worker map tracks the reservation
// lifecycle. The dispatcher is exercised against the `WorkerPool` interface via
// an in-memory fake, plus a real-Bun smoke test (task 4.9).

import { describe, expect, test } from "bun:test"
import type { SessionID } from "@/session/schema"
import {
  createBunWorkerPool,
  createDispatcher,
  deriveHeadroom,
  SessionCancelledError,
  WorkerCrashedError,
  type Scheduler,
  type SessionRun,
  type SessionWorker,
  type WorkerKind,
  type WorkerPool,
} from "@/session/worker/dispatch"
import { SessionQueueFullError } from "@/session/worker/queue"
import { createWorkerHarness, type HarnessWorker, type WorkerHarness } from "../../lib/worker-harness"
import { faultWorkerEntry, withFaults } from "../../lib/faults"

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

  finish(sessionID: SessionID): void {
    const pending = this.pending.get(sessionID)
    this.pending.delete(sessionID)
    pending?.resolve()
  }

  crash(sessionID: SessionID): void {
    this.alive = false
    const pending = this.pending.get(sessionID)
    this.pending.delete(sessionID)
    pending?.reject(new WorkerCrashedError({ message: `worker ${this.id} crashed`, workerID: this.id }))
  }

  fail(sessionID: SessionID, error: unknown): void {
    const pending = this.pending.get(sessionID)
    this.pending.delete(sessionID)
    pending?.reject(error)
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

interface TestTimer {
  readonly delayMs: number
  readonly run: () => void
  cancelled: boolean
}

/** A scheduler that records timers so tests can fire them deterministically. */
function createTestScheduler() {
  const timers: TestTimer[] = []
  const schedule: Scheduler = (delayMs, run) => {
    const timer: TestTimer = { delayMs, run, cancelled: false }
    timers.push(timer)
    return () => {
      timer.cancelled = true
    }
  }
  return {
    schedule,
    timers,
    pending: () => timers.filter((timer) => !timer.cancelled),
  }
}

describe("4.3 one active session per worker", () => {
  test("extra sessions queue instead of multiplexing a busy worker", async () => {
    const pool = new FakePool(2)
    const dispatcher = createDispatcher({ pool })
    const first = dispatcher.dispatch({ sessionID: sid("ses_1") })
    const second = dispatcher.dispatch({ sessionID: sid("ses_2") })
    const third = dispatcher.dispatch({ sessionID: sid("ses_3") })

    expect(pool.workers[0].runs.map((run) => run.sessionID)).toEqual([sid("ses_1")])
    expect(pool.workers[1].runs.map((run) => run.sessionID)).toEqual([sid("ses_2")])
    expect(dispatcher.activeCount).toBe(2)
    expect(dispatcher.queuedCount).toBe(1)

    pool.workers[0].finish(sid("ses_1"))
    await first
    await flush()
    expect(pool.workers[0].runs.map((run) => run.sessionID)).toEqual([sid("ses_1"), sid("ses_3")])

    pool.workers[0].finish(sid("ses_3"))
    pool.workers[1].finish(sid("ses_2"))
    await Promise.all([second, third])
    expect(dispatcher.activeCount).toBe(0)
  })
})

describe("4.4 K busy → K+1 queues FIFO and runs on release", () => {
  test("queued sessions run in arrival order as workers free up", async () => {
    const pool = new FakePool(1)
    const dispatcher = createDispatcher({ pool })
    const first = dispatcher.dispatch({ sessionID: sid("ses_1") })
    const second = dispatcher.dispatch({ sessionID: sid("ses_2") })
    const third = dispatcher.dispatch({ sessionID: sid("ses_3") })

    expect(pool.workers[0].runs.map((run) => run.sessionID)).toEqual([sid("ses_1")])
    expect(dispatcher.queuedCount).toBe(2)

    pool.workers[0].finish(sid("ses_1"))
    await first
    await flush()
    expect(pool.workers[0].runs.map((run) => run.sessionID)).toEqual([sid("ses_1"), sid("ses_2")])

    pool.workers[0].finish(sid("ses_2"))
    await second
    await flush()
    expect(pool.workers[0].runs.map((run) => run.sessionID)).toEqual([sid("ses_1"), sid("ses_2"), sid("ses_3")])

    pool.workers[0].finish(sid("ses_3"))
    await third
  })
})

describe("4.5 bounded admission queue", () => {
  test("overflow rejects with SessionQueueFullError", async () => {
    const pool = new FakePool(1)
    const dispatcher = createDispatcher({ pool, admissionCapacity: 2 })
    const first = dispatcher.dispatch({ sessionID: sid("ses_1") })
    const second = dispatcher.dispatch({ sessionID: sid("ses_2") })
    const third = dispatcher.dispatch({ sessionID: sid("ses_3") })

    expect(dispatcher.queuedCount).toBe(2)
    await expect(dispatcher.dispatch({ sessionID: sid("ses_4") })).rejects.toBeInstanceOf(SessionQueueFullError)

    pool.workers[0].finish(sid("ses_1"))
    await first
    await flush()
    pool.workers[0].finish(sid("ses_2"))
    await second
    await flush()
    pool.workers[0].finish(sid("ses_3"))
    await third
  })
})

describe("4.6 on-demand headroom (depth-1 children only)", () => {
  test("derives max(2, ceil(K/2))", () => {
    expect(deriveHeadroom(1)).toBe(2)
    expect(deriveHeadroom(2)).toBe(2)
    expect(deriveHeadroom(4)).toBe(2)
    expect(deriveHeadroom(6)).toBe(3)
    expect(deriveHeadroom(8)).toBe(4)
  })

  test("foreground dispatch never borrows headroom; children spawn it on demand", async () => {
    const pool = new FakePool(1)
    const dispatcher = createDispatcher({ pool })

    const parent = dispatcher.dispatch({ sessionID: sid("ses_parent") })
    const sibling = dispatcher.dispatch({ sessionID: sid("ses_sibling") })
    await flush()

    // K=1 is busy: the sibling queues and no headroom worker is spawned.
    expect(dispatcher.queuedCount).toBe(1)
    expect(dispatcher.headroomCount).toBe(0)
    expect(pool.spawned).toHaveLength(0)

    const child = dispatcher.dispatch({ sessionID: sid("ses_child"), depth: 1 })
    await flush()
    expect(dispatcher.headroomCount).toBe(1)
    expect(pool.spawned).toHaveLength(1)
    expect(pool.spawned[0].runs.map((run) => run.sessionID)).toEqual([sid("ses_child")])

    // A foreground dispatch while headroom is free is still refused headroom.
    const late = dispatcher.dispatch({ sessionID: sid("ses_late") })
    await flush()
    expect(pool.spawned).toHaveLength(1)
    expect(dispatcher.queuedCount).toBe(2)

    pool.workers[0].finish(sid("ses_parent"))
    await parent
    await flush()
    expect(pool.workers[0].runs.map((run) => run.sessionID)).toEqual([sid("ses_parent"), sid("ses_sibling")])

    pool.workers[0].finish(sid("ses_sibling"))
    await sibling
    await flush()
    pool.workers[0].finish(sid("ses_late"))
    await late
    pool.spawned[0].finish(sid("ses_child"))
    await child
  })

  test("headroom grows on demand up to its bound, then children queue", async () => {
    const pool = new FakePool(1)
    const dispatcher = createDispatcher({ pool })
    const parent = dispatcher.dispatch({ sessionID: sid("ses_parent") })

    const children = [0, 1, 2].map((index) =>
      dispatcher.dispatch({ sessionID: sid(`ses_child_${index}`), depth: 1 }),
    )
    await flush()

    // headroom = 2 for K=1: two children run, the third queues.
    expect(dispatcher.headroomCount).toBe(2)
    expect(pool.spawned).toHaveLength(2)
    expect(dispatcher.queuedCount).toBe(1)

    pool.spawned[0].finish(sid("ses_child_0"))
    await children[0]
    await flush()
    // the freed headroom slot picks up the queued third child
    expect(pool.spawned[0].runs.map((run) => run.sessionID)).toEqual([sid("ses_child_0"), sid("ses_child_2")])

    pool.spawned[1].finish(sid("ses_child_1"))
    await children[1]
    await flush()
    pool.spawned[0].finish(sid("ses_child_2"))
    await children[2]
    pool.workers[0].finish(sid("ses_parent"))
    await parent
  })

  test("the child queue is bounded then rejects", async () => {
    const pool = new FakePool(1)
    const dispatcher = createDispatcher({ pool, headroom: 0, childCapacity: 2 })
    const parent = dispatcher.dispatch({ sessionID: sid("ses_parent") })
    const childA = dispatcher.dispatch({ sessionID: sid("ses_child_a"), depth: 1 })
    const childB = dispatcher.dispatch({ sessionID: sid("ses_child_b"), depth: 1 })

    expect(dispatcher.queuedCount).toBe(2)
    await expect(dispatcher.dispatch({ sessionID: sid("ses_child_c"), depth: 1 })).rejects.toBeInstanceOf(
      SessionQueueFullError,
    )

    await dispatcher.terminateAll()
    await Promise.allSettled([parent, childA, childB])
  })

  test("idle headroom is reaped after the idle window", async () => {
    const pool = new FakePool(1)
    const scheduler = createTestScheduler()
    const dispatcher = createDispatcher({ pool, schedule: scheduler.schedule, headroomIdleReapMs: 60_000 })

    const parent = dispatcher.dispatch({ sessionID: sid("ses_parent") })
    const child = dispatcher.dispatch({ sessionID: sid("ses_child"), depth: 1 })
    await flush()

    const headroom = pool.spawned[0]
    expect(dispatcher.headroomCount).toBe(1)

    headroom.finish(sid("ses_child"))
    await child
    await flush()

    const reap = scheduler.pending().find((timer) => timer.delayMs === 60_000)
    expect(reap).toBeDefined()
    reap?.run()
    await flush()

    expect(dispatcher.headroomCount).toBe(0)
    expect(headroom.alive).toBe(false)

    pool.workers[0].finish(sid("ses_parent"))
    await parent
  })
})

describe("4.7 worker crash → at-most-once reject + respawn", () => {
  test("rejects the caller, does not requeue, respawns and stays healthy", async () => {
    const pool = new FakePool(1)
    const scheduler = createTestScheduler()
    const dispatcher = createDispatcher({ pool, schedule: scheduler.schedule })

    const crashed = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()

    pool.workers[0].crash(sid("ses_1"))
    await expect(crashed).rejects.toBeInstanceOf(WorkerCrashedError)

    // at-most-once: the crashed run is not requeued, the key is released
    expect(pool.workers[0].runs.map((run) => run.sessionID)).toEqual([sid("ses_1")])
    expect(dispatcher.queuedCount).toBe(0)
    expect(dispatcher.registry.has(sid("ses_1"))).toBe(false)

    // a respawn is scheduled with the initial 1s backoff, not spawned yet
    const pending = scheduler.pending()
    expect(pending).toHaveLength(1)
    expect(pending[0].delayMs).toBe(1_000)
    expect(pool.spawned).toHaveLength(0)

    pending[0].run()
    await flush()
    expect(pool.spawned).toHaveLength(1)

    // the pool is healthy: a new session runs on the replacement worker
    const healthy = dispatcher.dispatch({ sessionID: sid("ses_2") })
    await flush()
    expect(pool.spawned[0].runs.map((run) => run.sessionID)).toEqual([sid("ses_2")])
    pool.spawned[0].finish(sid("ses_2"))
    await healthy
    expect(dispatcher.activeCount).toBe(0)
  })

  test("a crash-loop degrades the slot (K−1) instead of respawning forever", async () => {
    const pool = new FakePool(1)
    const scheduler = createTestScheduler()
    const degraded: number[] = []
    const dispatcher = createDispatcher({ pool, schedule: scheduler.schedule, onDegraded: (id) => degraded.push(id) })

    for (let attempt = 0; attempt < 5; attempt++) {
      const run = dispatcher.dispatch({ sessionID: sid(`ses_${attempt}`) })
      await flush()
      const worker = attempt === 0 ? pool.workers[0] : pool.spawned[attempt - 1]
      worker.crash(sid(`ses_${attempt}`))
      await expect(run).rejects.toBeInstanceOf(WorkerCrashedError)
      const pending = scheduler.pending()
      if (pending.length > 0) {
        pending[pending.length - 1].run()
        await flush()
      }
    }

    expect(degraded).toHaveLength(1)
    expect(dispatcher.degradedCount).toBe(1)
    // capacity is reduced: no free foreground slot remains
    expect(dispatcher.headroomCount).toBe(0)
    await dispatcher.terminateAll()
  })

  test("a session failure (worker survives) releases the worker without respawn", async () => {
    const pool = new FakePool(1)
    const scheduler = createTestScheduler()
    const dispatcher = createDispatcher({ pool, schedule: scheduler.schedule })

    const failing = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()

    pool.workers[0].fail(sid("ses_1"), new Error("boom"))
    await expect(failing).rejects.toThrow("boom")

    expect(pool.workers[0].alive).toBe(true)
    expect(pool.spawned).toHaveLength(0)
    expect(scheduler.pending()).toHaveLength(0)

    const next = dispatcher.dispatch({ sessionID: sid("ses_2") })
    await flush()
    expect(pool.workers[0].runs.map((run) => run.sessionID)).toEqual([sid("ses_1"), sid("ses_2")])
    pool.workers[0].finish(sid("ses_2"))
    await next
  })

  test("a real isolate crash injected via the OPENCODE_TEST_FAULTS seam rejects at-most-once", async () => {
    const scheduler = createTestScheduler()
    await withFaults("crashWorker@dispatch:run", async () => {
      await using harness = createWorkerHarness({ cores: 1 })
      const pool = new FaultPool(harness, 1)
      const dispatcher = createDispatcher({ pool, schedule: scheduler.schedule })

      const crashed = dispatcher.dispatch({ sessionID: sid("ses_crash") })
      await expect(crashed).rejects.toBeInstanceOf(WorkerCrashedError)

      expect(dispatcher.registry.has(sid("ses_crash"))).toBe(false)
      expect(dispatcher.queuedCount).toBe(0)
      expect(pool.spawned).toHaveLength(0)
      expect(scheduler.pending().map((timer) => timer.delayMs)).toEqual([1_000])

      await dispatcher.terminateAll()
    })
  })
})

/** A `SessionWorker` backed by the real `test/fixture/fault-worker.ts` isolate. */
type FaultWorkerOut = { type: "ready" } | { type: "crashing"; point: string } | { type: "survived"; point: string }
type FaultWorkerIn = { type: "crash"; point: string }

const FAULT_POINT = "dispatch:run"

class FaultWorker implements SessionWorker {
  constructor(
    private readonly handle: HarnessWorker<FaultWorkerOut, FaultWorkerIn>,
    readonly id: number,
  ) {}

  get alive(): boolean {
    return this.handle.alive
  }

  async execute(_run: SessionRun): Promise<void> {
    await this.handle.waitFor("ready")
    this.handle.post({ type: "crash", point: FAULT_POINT })
    await this.handle.waitFor("crashing")
    throw new WorkerCrashedError({ message: `worker ${this.id} crashed via the fault seam`, workerID: this.id })
  }

  async terminate(): Promise<void> {
    await this.handle.terminate()
  }
}

class FaultPool implements WorkerPool {
  readonly workers: FaultWorker[]
  readonly spawned: FaultWorker[] = []

  constructor(
    private readonly harness: WorkerHarness,
    k: number,
  ) {
    this.workers = Array.from({ length: k }, (_, index) => this.make(index + 1))
  }

  private make(id: number): FaultWorker {
    return new FaultWorker(this.harness.spawn<FaultWorkerOut, FaultWorkerIn>(faultWorkerEntry), id)
  }

  spawn(): SessionWorker {
    const worker = this.make(100 + this.spawned.length)
    this.spawned.push(worker)
    return worker
  }

  async terminateAll(): Promise<void> {}
}

describe("4.8 key→worker map lifecycle", () => {
  test("insert on dispatch, running while executing, deleted on completion", async () => {
    const pool = new FakePool(1)
    const dispatcher = createDispatcher({ pool })
    const run = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()

    const reservation = dispatcher.registry.get(sid("ses_1"))
    expect(reservation?.state).toBe("running")
    expect(reservation?.workerID).toBe(pool.workers[0].id)

    pool.workers[0].finish(sid("ses_1"))
    await run
    expect(dispatcher.registry.has(sid("ses_1"))).toBe(false)
  })

  test("same-kind duplicate attaches/joins instead of running twice", async () => {
    const pool = new FakePool(1)
    const dispatcher = createDispatcher({ pool })
    const first = dispatcher.dispatch({ sessionID: sid("ses_1") })
    const joined = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()

    expect(pool.workers[0].runs).toHaveLength(1)
    expect(dispatcher.queuedCount).toBe(0)

    pool.workers[0].finish(sid("ses_1"))
    await Promise.all([first, joined])
  })

  test("cancel deletes a queued reservation", async () => {
    const pool = new FakePool(1)
    const dispatcher = createDispatcher({ pool })
    const running = dispatcher.dispatch({ sessionID: sid("ses_running") })
    const queued = dispatcher.dispatch({ sessionID: sid("ses_queued") })
    await flush()

    await dispatcher.cancel(sid("ses_queued"))
    await expect(queued).rejects.toBeInstanceOf(SessionCancelledError)
    expect(dispatcher.registry.has(sid("ses_queued"))).toBe(false)
    expect(dispatcher.queuedCount).toBe(0)

    pool.workers[0].finish(sid("ses_running"))
    await running
  })

  test("cancel of a running session deletes the reservation and replaces the worker", async () => {
    const pool = new FakePool(1)
    const scheduler = createTestScheduler()
    const dispatcher = createDispatcher({ pool, schedule: scheduler.schedule })
    const running = dispatcher.dispatch({ sessionID: sid("ses_1") })
    const settled = capture(running)
    await flush()

    await dispatcher.cancel(sid("ses_1"))
    await flush()
    expect(await settled).toBeInstanceOf(SessionCancelledError)
    expect(dispatcher.registry.has(sid("ses_1"))).toBe(false)
    expect(pool.workers[0].alive).toBe(false)
    expect(pool.spawned).toHaveLength(1)
    // the terminate-induced rejection must NOT schedule a spurious respawn
    expect(scheduler.pending()).toHaveLength(0)
  })

  test("terminate clears the map and rejects in-flight sessions", async () => {
    const pool = new FakePool(1)
    const dispatcher = createDispatcher({ pool })
    const running = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()
    expect(dispatcher.registry.size).toBe(1)

    await dispatcher.terminateAll()
    expect(dispatcher.registry.size).toBe(0)
    await expect(running).rejects.toBeInstanceOf(SessionCancelledError)
  })
})

describe("4.9 WorkerPool interface", () => {
  test("dispatches a session through real Bun workers behind the interface", async () => {
    await using pool = createBunWorkerPool({ k: 1 })
    await using dispatcher = createDispatcher({ pool })

    await dispatcher.dispatch({ sessionID: sid("ses_real") })

    expect(dispatcher.activeCount).toBe(0)
    expect(dispatcher.registry.size).toBe(0)
  })
})
