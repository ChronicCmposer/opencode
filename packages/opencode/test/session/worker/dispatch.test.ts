// Phase 4.3–4.9 — dispatch scheduler core (plan tasks 4.3–4.9).
//
// One active session per worker; K busy queues FIFO; the admission queue is
// bounded; on-demand headroom is reserved for depth-1 children; a crash rejects
// at-most-once and respawns; the key→worker map tracks the reservation
// lifecycle. The dispatcher is exercised against the `WorkerPool` interface via
// an in-memory fake; the production Bun pool lives in `hub.ts`.

import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import type { SessionID } from "@/session/schema"
import {
  createDispatcher,
  deriveHeadroom,
  HEADROOM_MIN,
  HeadroomFullError,
  SessionCancelledError,
  SessionCrashedError,
  WorkerCrashedError,
  type DispatchRequest,
  type Scheduler,
  type SessionRun,
  type SessionWorker,
  type WorkerKind,
  type WorkerPool,
} from "@/session/worker/dispatch"
import {
  createExecutorWorker,
  createSessionDispatch,
  SessionWorkerReplyTimeoutError,
  type ExecutorTransport,
  type ExecutorWorkerPool,
  type SessionExecutorInbound,
  type SessionScheduler,
} from "@/session/worker/execute"
import type { CancelFrame, CancelPort } from "@/session/worker/cancel"
import { SessionQueueFullError } from "@/session/worker/queue"
import { workerLogMessage, type WorkerLogger } from "@/session/worker/log"
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

  /** Kill the worker with an arbitrary error (a dead isolate, not a typed crash). */
  crashWith(sessionID: SessionID, error: unknown): void {
    this.alive = false
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
    expect(dispatcher.busySlotCount).toBe(2)
    expect(dispatcher.queuedCount).toBe(1)

    pool.workers[0].finish(sid("ses_1"))
    await first
    await flush()
    expect(pool.workers[0].runs.map((run) => run.sessionID)).toEqual([sid("ses_1"), sid("ses_3")])

    pool.workers[0].finish(sid("ses_3"))
    pool.workers[1].finish(sid("ses_2"))
    await Promise.all([second, third])
    expect(dispatcher.busySlotCount).toBe(0)
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
    await expect(dispatcher.dispatch({ sessionID: sid("ses_4") })).rejects.toThrow(SessionQueueFullError)

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

  test("scales additively for oversubscribed K = nproc × F", () => {
    // Option B oversubscription: the foreground pool K is now `nproc × F`
    // (F = 2), so K commonly lands at 8/16/32. Headroom tracks it at
    // `max(HEADROOM_MIN, ceil(K/2))` — 8 → 4, 16 → 8, 32 → 16 — and is
    // ADDITIVE and TRANSIENT: it is spawned on demand for depth ≥ 1 children
    // and idle-reaped, never folded into the foreground pool.
    expect(deriveHeadroom(8)).toBe(4)
    expect(deriveHeadroom(16)).toBe(8)
    expect(deriveHeadroom(32)).toBe(16)

    // Small K still floors at HEADROOM_MIN rather than dropping to 1.
    expect(deriveHeadroom(1)).toBe(HEADROOM_MIN)
    expect(deriveHeadroom(2)).toBe(HEADROOM_MIN)
    expect(deriveHeadroom(3)).toBe(HEADROOM_MIN)
  })

  test("headroom is additive: K foreground workers are not pre-provisioned with headroom", async () => {
    const pool = new FakePool(8)
    const dispatcher = createDispatcher({ pool })

    // K = 8 foreground workers, headroom bound = max(2, ceil(8/2)) = 4.
    expect(dispatcher.headroomCapacity).toBe(4)
    expect(dispatcher.headroomSlotCount).toBe(0)

    const parent = capture(dispatcher.dispatch({ sessionID: sid("ses_parent") }))
    const child = capture(dispatcher.dispatch({ sessionID: sid("ses_child"), depth: 1 }))
    await flush()

    // The depth≥1 child spawns headroom on demand; it is not part of K.
    expect(dispatcher.headroomSlotCount).toBe(1)
    expect(pool.spawned).toHaveLength(1)

    await dispatcher.terminateAll()
    await Promise.allSettled([parent, child])
  })

  test("foreground dispatch never borrows headroom; children spawn it on demand", async () => {
    const pool = new FakePool(1)
    const dispatcher = createDispatcher({ pool })

    const parent = dispatcher.dispatch({ sessionID: sid("ses_parent") })
    const sibling = dispatcher.dispatch({ sessionID: sid("ses_sibling") })
    await flush()

    // K=1 is busy: the sibling queues and no headroom worker is spawned.
    expect(dispatcher.queuedCount).toBe(1)
    expect(dispatcher.headroomSlotCount).toBe(0)
    expect(pool.spawned).toHaveLength(0)

    const child = dispatcher.dispatch({ sessionID: sid("ses_child"), depth: 1 })
    await flush()
    expect(dispatcher.headroomSlotCount).toBe(1)
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

    const children = [0, 1, 2].map((index) => dispatcher.dispatch({ sessionID: sid(`ses_child_${index}`), depth: 1 }))
    await flush()

    // headroom = 2 for K=1: two children run, the third queues.
    expect(dispatcher.headroomSlotCount).toBe(2)
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

  test("a depth≥1 dispatch runs on a spawned headroom worker with its run depth", async () => {
    const pool = new FakePool(1)
    const dispatcher = createDispatcher({ pool })

    const parent = dispatcher.dispatch({ sessionID: sid("ses_parent") })
    const child = dispatcher.dispatch({ sessionID: sid("ses_child"), depth: 1 })
    await flush()

    // Depth ≥ 1 selects the headroom kind: a child never takes the foreground slot.
    expect(dispatcher.headroomSlotCount).toBe(1)
    expect(pool.spawned).toHaveLength(1)
    expect(pool.spawned[0].runs).toEqual([{ sessionID: sid("ses_child"), depth: 1 }])

    pool.spawned[0].finish(sid("ses_child"))
    await child
    pool.workers[0].finish(sid("ses_parent"))
    await parent
  })

  test("an exhausted child/headroom queue rejects with HeadroomFullError", async () => {
    const pool = new FakePool(1)
    const dispatcher = createDispatcher({ pool, headroom: 0, childCapacity: 2 })
    const parent = dispatcher.dispatch({ sessionID: sid("ses_parent") })
    const childA = dispatcher.dispatch({ sessionID: sid("ses_child_a"), depth: 1 })
    const childB = dispatcher.dispatch({ sessionID: sid("ses_child_b"), depth: 1 })

    expect(dispatcher.queuedCount).toBe(2)
    // Headroom cannot grow and the child queue is at capacity: the child is refused
    // loud (decision line 60) rather than admitted past the physical bound.
    await expect(dispatcher.dispatch({ sessionID: sid("ses_child_c"), depth: 1 })).rejects.toThrow(HeadroomFullError)

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
    expect(dispatcher.headroomSlotCount).toBe(1)

    headroom.finish(sid("ses_child"))
    await child
    await flush()

    const reap = scheduler.pending().find((timer) => timer.delayMs === 60_000)
    expect(reap).toBeDefined()
    reap?.run()
    await flush()

    expect(dispatcher.headroomSlotCount).toBe(0)
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
    expect(dispatcher.busySlotCount).toBe(0)
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
    expect(dispatcher.headroomSlotCount).toBe(0)
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

describe("4.10 crash error identity", () => {
  test("WorkerCrashedError carries its own _tag and is still a SessionCrashedError", () => {
    const crash = new WorkerCrashedError({ message: "worker 1 crashed", workerID: 1 })
    expect(crash._tag).toBe("WorkerCrashedError")
    expect(crash).toBeInstanceOf(WorkerCrashedError)
    expect(crash).toBeInstanceOf(SessionCrashedError)
  })

  test("Effect.catchTag matches the WorkerCrashedError tag", async () => {
    const caught = await Effect.runPromise(
      Effect.fail(new WorkerCrashedError({ message: "boom", workerID: 1 })).pipe(
        Effect.catchTag("WorkerCrashedError", () => Effect.succeed("worker")),
        Effect.orElseSucceed(() => "missed"),
      ),
    )
    expect(caught).toBe("worker")
  })
})

describe("4.11 dispatch validation and waiters", () => {
  test("an invalid depth rejects the returned promise instead of throwing", async () => {
    const pool = new FakePool(1)
    const dispatcher = createDispatcher({ pool })
    let outcome: Promise<void> | undefined
    expect(() => {
      outcome = dispatcher.dispatch({ sessionID: sid("ses_bad"), depth: -1 })
    }).not.toThrow()
    await expect(outcome!).rejects.toThrow(/invalid dispatch depth/)
  })

  test("a shifted waiter that cannot enqueue fails the whole waiter list", async () => {
    const pool = new FakePool(1)
    const dispatcher = createDispatcher({ pool, headroom: 0, childCapacity: 1 })
    const parent = dispatcher.dispatch({ sessionID: sid("ses_parent") })
    const queuedChild = dispatcher.dispatch({ sessionID: sid("ses_queued"), depth: 1 })
    const waiterA = capture(dispatcher.dispatch({ sessionID: sid("ses_parent"), depth: 1 }))
    const waiterB = capture(dispatcher.dispatch({ sessionID: sid("ses_parent"), depth: 1 }))
    await flush()
    expect(dispatcher.queuedCount).toBe(1)

    pool.workers[0].finish(sid("ses_parent"))
    await parent
    await flush()

    expect(await waiterA).toBeInstanceOf(HeadroomFullError)
    expect(await waiterB).toBeInstanceOf(HeadroomFullError)
    await dispatcher.terminateAll()
    await Promise.allSettled([queuedChild])
  })

  test("different-kind waiters for one key are bounded", async () => {
    const pool = new FakePool(1)
    const dispatcher = createDispatcher({ pool, waiterCapacity: 2 })
    const root = capture(dispatcher.dispatch({ sessionID: sid("ses_1") }))
    const one = capture(dispatcher.dispatch({ sessionID: sid("ses_1"), depth: 1 }))
    const two = capture(dispatcher.dispatch({ sessionID: sid("ses_1"), depth: 2 }))
    await expect(dispatcher.dispatch({ sessionID: sid("ses_1"), depth: 3 })).rejects.toThrow(SessionQueueFullError)
    await dispatcher.terminateAll()
    await Promise.allSettled([root, one, two])
  })
})

describe("execution seam forwards depth and the startShell frame", () => {
  /** A `SessionScheduler` fake that records every dispatch request. */
  function recordingScheduler(): { scheduler: SessionScheduler; calls: DispatchRequest[] } {
    const calls: DispatchRequest[] = []
    const scheduler: SessionScheduler = {
      dispatch: async (request) => {
        calls.push(request)
      },
      cancel: async () => {},
      terminateAll: async () => {},
      registry: { get: () => undefined },
    }
    return { scheduler, calls }
  }

  /** An `ExecutorWorkerPool` fake whose `resultFor` always resolves a result. */
  function resultPool(): ExecutorWorkerPool {
    return {
      workers: [],
      spawn: () => {
        throw new Error("the pool must not be asked to spawn for a recorded dispatch")
      },
      terminateAll: async () => {},
      byID: () => undefined,
      resultFor: (sessionID) => ({ sessionID }),
    }
  }

  test("ensureRunning carries an explicit depth into the dispatch request", async () => {
    const { scheduler, calls } = recordingScheduler()
    const dispatch = createSessionDispatch({ dispatcher: scheduler, pool: resultPool() })

    await dispatch.ensureRunning(sid("ses_child"), 1)

    expect(calls).toEqual([{ sessionID: sid("ses_child"), depth: 1, mode: "run" }])
  })

  test("ensureRunning derives depth from depthOf when none is passed", async () => {
    const { scheduler, calls } = recordingScheduler()
    const dispatch = createSessionDispatch({
      dispatcher: scheduler,
      pool: resultPool(),
      depthOf: (sessionID) => (sessionID === "ses_child" ? 2 : 0),
    })

    await dispatch.ensureRunning(sid("ses_child"))

    expect(calls).toEqual([{ sessionID: sid("ses_child"), depth: 2, mode: "run" }])
  })

  test("startShell dispatches the startShell mode (not a plain run)", async () => {
    const { scheduler, calls } = recordingScheduler()
    const dispatch = createSessionDispatch({ dispatcher: scheduler, pool: resultPool() })

    await dispatch.startShell(sid("ses_shell"))

    expect(calls).toEqual([{ sessionID: sid("ses_shell"), depth: 0, mode: "startShell" }])
  })

  test("createExecutorWorker posts a startShell frame for a startShell-mode run", async () => {
    const posted: SessionExecutorInbound[] = []
    const transport: ExecutorTransport = {
      post: (message) => {
        posted.push(message)
      },
      waitFor: async (match) => {
        const frame = posted[posted.length - 1] as { readonly id: number; readonly sessionID?: SessionID }
        const reply = { type: "done" as const, id: frame.id, result: { sessionID: frame.sessionID ?? "ses" } }
        return match(reply) ? reply : reply
      },
      alive: true,
      terminate: async () => 0,
    }
    const worker = createExecutorWorker({ id: 1, transport, workKind: "session" })

    await worker.execute({ sessionID: sid("ses_child"), depth: 1, mode: "startShell" })
    await worker.execute({ sessionID: sid("ses_root"), depth: 0 })

    expect(posted[0]).toMatchObject({ type: "startShell", sessionID: "ses_child", kind: "session" })
    expect(posted[1]).toMatchObject({ type: "run", sessionID: "ses_root", kind: "session" })
  })
})

describe("crash error surfacing", () => {
  test("a worker that dies with a real error surfaces its message and cause", async () => {
    const pool = new FakePool(1)
    const scheduler = createTestScheduler()
    const dispatcher = createDispatcher({ pool, schedule: scheduler.schedule })

    const running = dispatcher.dispatch({ sessionID: sid("ses_crash_message") })
    const settled = capture(running)
    await flush()

    const cause = new Error("ModuleNotFound resolving './session-worker.ts' (entry point)")
    pool.workers[0].crashWith(sid("ses_crash_message"), cause)

    const error = await settled
    expect(error).toBeInstanceOf(WorkerCrashedError)
    expect((error as WorkerCrashedError).message).toContain("ModuleNotFound resolving './session-worker.ts'")
    expect((error as WorkerCrashedError).cause).toBe(cause)

    await dispatcher.terminateAll()
  })
})

/**
 * A worker with a cancel port whose run unwinds (resolves), dies with the
 * executor machine (rejects), or ignores the cancel entirely (`ignore`).
 * `acknowledge` controls whether the worker reports a `cancel-ack`.
 */
class PortedWorker implements SessionWorker {
  alive = true
  readonly runs: SessionRun[] = []
  readonly hub: CancelPort
  private readonly pending = new Map<SessionID, PendingRun>()
  private readonly ackHandlers = new Set<(frame: CancelFrame) => void>()

  constructor(
    readonly id: number,
    private readonly onCancel: "resolve" | "reject" | "ignore",
    private readonly acknowledge = true,
  ) {
    this.hub = {
      notify: (frame) => {
        if (frame.type !== "cancel") return
        if (this.acknowledge) {
          for (const handler of [...this.ackHandlers]) {
            handler({ type: "cancel-ack", sessionID: frame.sessionID, accepted: true })
          }
        }
        if (this.onCancel === "ignore") return
        const sessionID = frame.sessionID as SessionID
        const pending = this.pending.get(sessionID)
        if (pending === undefined) return
        this.pending.delete(sessionID)
        if (this.onCancel === "resolve") pending.resolve()
        else pending.reject(new Error("executor machine interrupted"))
      },
      onMessage: (handler) => {
        this.ackHandlers.add(handler)
        return () => void this.ackHandlers.delete(handler)
      },
    }
  }

  execute(run: SessionRun): Promise<void> {
    this.runs.push(run)
    return new Promise<void>((resolve, reject) => {
      this.pending.set(run.sessionID, { resolve, reject })
    })
  }

  async terminate(): Promise<void> {
    this.alive = false
    const pending = [...this.pending.values()]
    this.pending.clear()
    for (const run of pending) run.reject(new SessionCancelledError({ message: "terminated", sessionID: "" }))
  }
}

class PortedPool implements WorkerPool {
  readonly workers: PortedWorker[]
  readonly spawned: PortedWorker[] = []

  constructor(
    k: number,
    private readonly onCancel: "resolve" | "reject" | "ignore",
  ) {
    this.workers = Array.from({ length: k }, (_, index) => new PortedWorker(index + 1, onCancel))
  }

  spawn(): SessionWorker {
    const worker = new PortedWorker(100 + this.spawned.length, this.onCancel)
    this.spawned.push(worker)
    return worker
  }

  async terminateAll(): Promise<void> {
    await Promise.all(this.workers.map((worker) => worker.terminate()))
  }
}

describe("11.5 reaping seams + still-live terminate hardening", () => {
  test("the terminate path invokes the reap seam with the worker", async () => {
    const pool = new FakePool(1)
    const reaped: number[] = []
    const dispatcher = createDispatcher({
      pool,
      cancellation: { reap: (worker) => reaped.push(worker.id) },
    })

    const running = capture(dispatcher.dispatch({ sessionID: sid("ses_reap") }))
    await flush()
    await dispatcher.cancelWithGrace(sid("ses_reap"))

    expect(await running).toBeInstanceOf(SessionCancelledError)
    expect(reaped).toEqual([1])

    await dispatcher.terminateAll()
  })

  test("terminateAll invokes the reapAll seam exactly once", async () => {
    const pool = new FakePool(1)
    let reapAllCalls = 0
    const dispatcher = createDispatcher({
      pool,
      cancellation: {
        reapAll: () => {
          reapAllCalls += 1
        },
      },
    })

    const running = capture(dispatcher.dispatch({ sessionID: sid("ses_reap_all") }))
    await flush()
    await dispatcher.terminateAll()
    await running
    await dispatcher.terminateAll()

    expect(reapAllCalls).toBe(1)
  })

  test("a cooperative ack whose run actually unwound reuses the worker and does not reap", async () => {
    const pool = new PortedPool(1, "resolve")
    const reaped: number[] = []
    const dispatcher = createDispatcher({
      pool,
      cancellation: {
        portFor: (worker) => (worker as PortedWorker).hub,
        graceMs: 500,
        schedule: createTestScheduler().schedule,
        reap: (worker) => reaped.push(worker.id),
      },
    })

    const running = capture(dispatcher.dispatch({ sessionID: sid("ses_unwound") }))
    await flush()
    const outcome = await dispatcher.cancelWithGrace(sid("ses_unwound"))

    expect(outcome).toMatchObject({ mode: "cooperative", winner: "cancel", acknowledged: true })
    expect(pool.workers[0].alive).toBe(true)
    expect(reaped).toEqual([])
    expect(await running).toBeInstanceOf(SessionCancelledError)

    await dispatcher.terminateAll()
  })

  test("grace expiry terminates a still-running worker and reaps it exactly once", async () => {
    const pool = new PortedPool(1, "ignore")
    const scheduler = createTestScheduler()
    const reaped: number[] = []
    const dispatcher = createDispatcher({
      pool,
      cancellation: {
        portFor: (worker) => (worker as PortedWorker).hub,
        graceMs: 500,
        schedule: scheduler.schedule,
        reap: (worker) => reaped.push(worker.id),
      },
    })

    const running = capture(dispatcher.dispatch({ sessionID: sid("ses_grace") }))
    await flush()
    const pending = dispatcher.cancelWithGrace(sid("ses_grace"))
    await flush()

    // The run ignored the cancel: at grace expiry the dispatcher terminates the
    // isolate (reaping mirrored PGIDs once — the controller reaps, the idempotent
    // dispatcher terminate does not reap again).
    scheduler
      .pending()
      .find((timer) => timer.delayMs === 500)
      ?.run()
    const outcome = await pending

    expect(outcome).toMatchObject({ mode: "terminated", winner: "cancel", acknowledged: true })
    expect(pool.workers[0].alive).toBe(false)
    expect(reaped).toEqual([1])
    expect(await running).toBeInstanceOf(SessionCancelledError)

    await dispatcher.terminateAll()
  })
})

// ---------------------------------------------------------------------------
// dbwal-12-02 DISPATCH stream — session-worker observability logging.
//
// The dispatcher's lifecycle transitions emit one structured line each through
// the shared `WorkerLogger` seam. A recording logger captures `(message, fields)`
// so each event's message and payload can be pinned. `level` rides in the fields
// payload (the fixed seam has no level parameter — see the stream artifact).
// ---------------------------------------------------------------------------

interface RecordedLog {
  readonly message: string
  readonly fields: Record<string, unknown>
}

/** A `WorkerLogger` that records every line the dispatcher emits. */
function recordingLogger(): { log: WorkerLogger; lines: RecordedLog[] } {
  const lines: RecordedLog[] = []
  return {
    log: (message, fields) => {
      lines.push({ message, fields })
    },
    lines,
  }
}

describe("dbwal-12-02 dispatch observability logging", () => {
  test("C1 session admitted (INFO) carries worker, kind, depth and queue depth", async () => {
    const pool = new FakePool(1)
    const { log, lines } = recordingLogger()
    const dispatcher = createDispatcher({ pool, log })

    const run = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()

    const admitted = lines.find((line) => line.message === workerLogMessage("session admitted"))
    expect(admitted).toBeDefined()
    expect(admitted!.fields).toMatchObject({
      level: "INFO",
      sessionID: "ses_1",
      workerID: pool.workers[0].id,
      kind: "foreground",
      depth: 0,
    })
    expect(typeof admitted!.fields.queueDepth).toBe("number")

    pool.workers[0].finish(sid("ses_1"))
    await run
  })

  test("C2 session queued (INFO) carries the queue name, depth, capacity and size", async () => {
    const pool = new FakePool(1)
    const { log, lines } = recordingLogger()
    const dispatcher = createDispatcher({ pool, log, admissionCapacity: 3 })

    const first = dispatcher.dispatch({ sessionID: sid("ses_1") })
    const second = dispatcher.dispatch({ sessionID: sid("ses_2") })
    await flush()

    const queued = lines.filter((line) => line.message === workerLogMessage("session queued"))
    expect(queued).toHaveLength(1)
    expect(queued[0].fields).toMatchObject({
      level: "INFO",
      sessionID: "ses_2",
      queue: "admission",
      depth: 0,
      capacity: 3,
      size: 1,
    })

    pool.workers[0].finish(sid("ses_1"))
    await first
    await flush()
    pool.workers[0].finish(sid("ses_2"))
    await second
  })

  test("C3 queue full (WARN) is emitted where the admission queue rejects", async () => {
    const pool = new FakePool(1)
    const { log, lines } = recordingLogger()
    const dispatcher = createDispatcher({ pool, log, admissionCapacity: 1 })

    const first = dispatcher.dispatch({ sessionID: sid("ses_1") })
    const second = dispatcher.dispatch({ sessionID: sid("ses_2") })
    await flush()

    await expect(dispatcher.dispatch({ sessionID: sid("ses_3") })).rejects.toThrow(SessionQueueFullError)

    const full = lines.find((line) => line.message === workerLogMessage("queue full"))
    expect(full).toBeDefined()
    expect(full!.fields).toMatchObject({ level: "WARN", sessionID: "ses_3", queue: "admission", capacity: 1 })

    pool.workers[0].finish(sid("ses_1"))
    await first
    await flush()
    pool.workers[0].finish(sid("ses_2"))
    await second
  })

  test("C4 session completed (INFO) carries the worker and a non-negative duration", async () => {
    const pool = new FakePool(1)
    const { log, lines } = recordingLogger()
    const dispatcher = createDispatcher({ pool, log })

    const run = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()
    pool.workers[0].finish(sid("ses_1"))
    await run

    const done = lines.find((line) => line.message === workerLogMessage("session completed"))
    expect(done).toBeDefined()
    expect(done!.fields).toMatchObject({ level: "INFO", sessionID: "ses_1", workerID: pool.workers[0].id })
    expect(typeof done!.fields.durationMs).toBe("number")
    expect(done!.fields.durationMs as number).toBeGreaterThanOrEqual(0)
  })

  test("C5 waiter promoted (INFO) carries the remaining waiter depth", async () => {
    const pool = new FakePool(1)
    const { log, lines } = recordingLogger()
    const dispatcher = createDispatcher({ pool, log })

    const root = capture(dispatcher.dispatch({ sessionID: sid("ses_1") }))
    const waiter = capture(dispatcher.dispatch({ sessionID: sid("ses_1"), depth: 1 }))
    await flush()

    pool.workers[0].finish(sid("ses_1"))
    await root
    await flush()

    const promoted = lines.find((line) => line.message === workerLogMessage("waiter promoted"))
    expect(promoted).toBeDefined()
    expect(promoted!.fields).toMatchObject({ level: "INFO", sessionID: "ses_1" })
    expect(typeof promoted!.fields.waiterQueueDepth).toBe("number")

    await dispatcher.terminateAll()
    await Promise.allSettled([waiter])
  })

  test("B4 worker crashed (WARN) carries the crash decision", async () => {
    const pool = new FakePool(1)
    const scheduler = createTestScheduler()
    const { log, lines } = recordingLogger()
    const dispatcher = createDispatcher({ pool, log, schedule: scheduler.schedule })

    const crashed = capture(dispatcher.dispatch({ sessionID: sid("ses_1") }))
    await flush()
    pool.workers[0].crash(sid("ses_1"))
    await crashed

    const line = lines.find((entry) => entry.message === workerLogMessage("worker crashed"))
    expect(line).toBeDefined()
    expect(line!.fields).toMatchObject({
      level: "WARN",
      sessionID: "ses_1",
      attempts: 1,
      backoffMs: 1_000,
      degraded: false,
    })
    expect(typeof line!.fields.workerID).toBe("number")

    await dispatcher.terminateAll()
  })

  test("B5 slot degraded (ERROR) carries the degraded count", async () => {
    const pool = new FakePool(1)
    const scheduler = createTestScheduler()
    const { log, lines } = recordingLogger()
    const dispatcher = createDispatcher({ pool, log, schedule: scheduler.schedule })

    for (let attempt = 0; attempt < 5; attempt++) {
      const run = capture(dispatcher.dispatch({ sessionID: sid(`ses_${attempt}`) }))
      await flush()
      const worker = attempt === 0 ? pool.workers[0] : pool.spawned[attempt - 1]
      worker.crash(sid(`ses_${attempt}`))
      await run
      const pending = scheduler.pending()
      if (pending.length > 0) {
        pending[pending.length - 1].run()
        await flush()
      }
    }

    const line = lines.find((entry) => entry.message === workerLogMessage("slot degraded"))
    expect(line).toBeDefined()
    expect(line!.fields).toMatchObject({ level: "ERROR", degradedCount: 1 })
    expect(typeof line!.fields.workerID).toBe("number")

    await dispatcher.terminateAll()
  })

  test("B6 headroom reaped (INFO) carries the idle window", async () => {
    const pool = new FakePool(1)
    const scheduler = createTestScheduler()
    const { log, lines } = recordingLogger()
    const dispatcher = createDispatcher({ pool, log, schedule: scheduler.schedule, headroomIdleReapMs: 60_000 })

    const parent = dispatcher.dispatch({ sessionID: sid("ses_parent") })
    const child = dispatcher.dispatch({ sessionID: sid("ses_child"), depth: 1 })
    await flush()

    pool.spawned[0].finish(sid("ses_child"))
    await child
    await flush()

    scheduler
      .pending()
      .find((timer) => timer.delayMs === 60_000)
      ?.run()
    await flush()

    const line = lines.find((entry) => entry.message === workerLogMessage("headroom reaped"))
    expect(line).toBeDefined()
    expect(line!.fields).toMatchObject({ level: "INFO", idleMs: 60_000 })
    expect(typeof line!.fields.workerID).toBe("number")

    pool.workers[0].finish(sid("ses_parent"))
    await parent
  })
})

// ---------------------------------------------------------------------------
// dbwal-12-07 WIRING — the cancel controller is built at the production call
// site, so its D1 `cancel requested` line must carry the dispatcher's `log` and
// the owning worker id. Before the wiring, `controllerFor` built the controller
// without `log`/`workerID`, so the line never fired in production.
// ---------------------------------------------------------------------------

describe("dbwal-12-07 dispatch wiring — cancel controller emits D1", () => {
  test("cancelWithGrace emits 'cancel requested' with sessionID, workerID and graceMs", async () => {
    const pool = new PortedPool(1, "ignore")
    const scheduler = createTestScheduler()
    const { log, lines } = recordingLogger()
    const dispatcher = createDispatcher({
      pool,
      log,
      cancellation: {
        portFor: (worker) => (worker as PortedWorker).hub,
        graceMs: 750,
        schedule: scheduler.schedule,
      },
    })

    const running = capture(dispatcher.dispatch({ sessionID: sid("ses_d1") }))
    await flush()
    const pending = dispatcher.cancelWithGrace(sid("ses_d1"))
    await flush()

    const line = lines.find((entry) => entry.message === workerLogMessage("cancel requested"))
    expect(line).toBeDefined()
    expect(line!.fields).toEqual({ sessionID: "ses_d1", workerID: 1, graceMs: 750 })

    // The run ignores the cancel, so settle it via the grace window.
    scheduler
      .pending()
      .find((timer) => timer.delayMs === 750)
      ?.run()
    await pending
    expect(await running).toBeInstanceOf(SessionCancelledError)

    await dispatcher.terminateAll()
  })
})

// ---------------------------------------------------------------------------
// Reply-timeout hardening. A worker reply timeout must TERMINATE the isolate (so
// a wedged run stops issuing writer transactions) and retire/replace the slot
// under the same supervised backoff as a crash — instead of rejecting the caller
// while leaving the worker running.
// ---------------------------------------------------------------------------

describe("reply timeout terminates and retires the worker", () => {
  test("a reply timeout rejects with the timeout error, terminates and replaces the slot", async () => {
    const pool = new FakePool(1)
    const scheduler = createTestScheduler()
    const dispatcher = createDispatcher({ pool, schedule: scheduler.schedule })

    const run = capture(dispatcher.dispatch({ sessionID: sid("ses_timeout") }))
    await flush()

    const wedged = pool.workers[0]
    const timedOut = new SessionWorkerReplyTimeoutError(30_000)
    // The isolate wedges: its reply never arrives, so `execute` rejects with the timeout.
    wedged.fail(sid("ses_timeout"), timedOut)

    // (a) the caller rejects with the timeout error itself (not a cancellation).
    expect(await run).toBe(timedOut)
    // (b) the wedged isolate was terminated.
    expect(wedged.alive).toBe(false)
    // (c) a supervised replacement is scheduled with the initial backoff, not spawned yet.
    expect(scheduler.pending().map((timer) => timer.delayMs)).toEqual([1_000])
    expect(pool.spawned).toHaveLength(0)
    scheduler.pending()[0].run()
    await flush()
    expect(pool.spawned).toHaveLength(1)

    // (d) the reservation cleared and a later dispatch runs on the NEW worker.
    expect(dispatcher.workerForSession(sid("ses_timeout"))).toBeUndefined()
    const next = capture(dispatcher.dispatch({ sessionID: sid("ses_next") }))
    await flush()
    expect(pool.spawned[0].runs.map((record) => record.sessionID)).toEqual([sid("ses_next")])
    pool.spawned[0].finish(sid("ses_next"))
    await next
  })

  test("a timeout loop degrades the slot (K−1) instead of hot-looping", async () => {
    const pool = new FakePool(1)
    const scheduler = createTestScheduler()
    const degraded: number[] = []
    const dispatcher = createDispatcher({ pool, schedule: scheduler.schedule, onDegraded: (id) => degraded.push(id) })

    for (let attempt = 0; attempt < 5; attempt++) {
      const run = capture(dispatcher.dispatch({ sessionID: sid(`ses_timeout_${attempt}`) }))
      await flush()
      const worker = attempt === 0 ? pool.workers[0] : pool.spawned[attempt - 1]
      worker.fail(sid(`ses_timeout_${attempt}`), new SessionWorkerReplyTimeoutError(1_000))
      expect(await run).toBeInstanceOf(SessionWorkerReplyTimeoutError)
      const pending = scheduler.pending()
      if (pending.length > 0) {
        pending[pending.length - 1].run()
        await flush()
      }
    }

    expect(degraded).toHaveLength(1)
    expect(dispatcher.degradedCount).toBe(1)
    // capacity is reduced: no free headroom slot remains.
    expect(dispatcher.headroomSlotCount).toBe(0)
    await dispatcher.terminateAll()
  })

  test("a timeout that races a cancel settles once and does not double-spawn", async () => {
    const pool = new FakePool(1)
    const scheduler = createTestScheduler()
    const dispatcher = createDispatcher({ pool, schedule: scheduler.schedule })

    const run = capture(dispatcher.dispatch({ sessionID: sid("ses_race") }))
    await flush()

    const worker = pool.workers[0]
    // Deliver both in the same tick: the isolate rejects with a timeout while the
    // cancel is already retiring the slot. Exactly one retirement may happen.
    worker.fail(sid("ses_race"), new SessionWorkerReplyTimeoutError(30_000))
    const cancelled = dispatcher.cancel(sid("ses_race"))
    await cancelled
    await flush()

    expect(await run).toBeInstanceOf(SessionCancelledError)
    // only the cancel's single replacement is spawned; the timeout adds no second.
    expect(pool.spawned).toHaveLength(1)
    expect(scheduler.pending()).toHaveLength(0)

    await dispatcher.terminateAll()
  })
})
