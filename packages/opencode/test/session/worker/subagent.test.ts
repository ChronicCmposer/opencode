// Phase 9.1/9.5/9.6 — subagent dispatch, admission and parent-cancel cascade
// (plan tasks 9.1, 9.5, 9.6).
//
// 9.1: a `task` child session is dispatched to a worker at depth 1. 9.5: nesting
// is bounded by `subagent_depth` and headroom; depth>limit or headroom-full is
// rejected BEFORE the scheduler is touched (never queued). 9.6: the REAL registry
// is wired into the Phase 11 `CancellationConfig.childrenOf` seam so a parent
// cancel cascades children-first. The 9.1 real-isolate case drives the landed
// executor workers through a real dispatcher (no model APIs).

import { describe, expect, test } from "bun:test"
import type { SessionID } from "@/session/schema"
import {
  createDispatcher,
  SessionCancelledError,
  type CancellationConfig,
  type SessionRun,
  type SessionWorker,
  type WorkerKind,
  type WorkerPool,
} from "@/session/worker/dispatch"
import {
  createExecutorWorker,
  createExecutorWorkerPool,
  type ExecutorTransport,
  type SessionExecutorInbound,
  type SessionExecutorOutbound,
} from "@/session/worker/execute"
import { createJobRegistry } from "@/session/worker/job-registry"
import {
  assertSubagentAdmitted,
  dispatchSubagent,
  HeadroomFullError,
  SubagentDepthError,
  type SubagentScheduler,
} from "@/session/worker/subagent"
import { createWorkerHarness, type WorkerHarness } from "../../lib/worker-harness"

const sid = (value: string) => value as SessionID
const messageOf = (error: unknown): string => (error instanceof Error ? error.message : String(error))
const flush = () => new Promise<void>((resolve) => setTimeout(resolve, 0))
const capture = (promise: Promise<unknown>): Promise<unknown> =>
  promise.then(
    () => undefined,
    (error: unknown) => error,
  )

interface Recorded {
  readonly sessionID: string
  readonly depth: number | undefined
}

function recordingScheduler(): { scheduler: SubagentScheduler; calls: Recorded[] } {
  const calls: Recorded[] = []
  return {
    calls,
    scheduler: {
      dispatch: async (request) => {
        calls.push({ sessionID: request.sessionID, depth: request.depth })
      },
    },
  }
}

describe("9.5 nesting is bounded by subagent_depth and headroom", () => {
  test("assertSubagentAdmitted admits within the limit when headroom is free", () => {
    expect(() => assertSubagentAdmitted({ depth: 0, limit: 1, headroomFull: false })).not.toThrow()
    expect(() => assertSubagentAdmitted({ depth: 1, limit: 1, headroomFull: false })).not.toThrow()
  })

  test("depth beyond subagent_depth is rejected", () => {
    expect(() => assertSubagentAdmitted({ depth: 2, limit: 1, headroomFull: false })).toThrow(SubagentDepthError)
  })

  test("headroom-full is rejected", () => {
    expect(() => assertSubagentAdmitted({ depth: 1, limit: 2, headroomFull: true })).toThrow(HeadroomFullError)
  })

  test("depth>limit rejects before the scheduler is touched (never queues)", async () => {
    const registry = createJobRegistry()
    registry.start({ id: "ses_parent", type: "task", depth: 1 })
    const { scheduler, calls } = recordingScheduler()

    const error = await capture(
      dispatchSubagent(
        { registry, scheduler, depthLimit: 1, headroomFull: () => false },
        { parentID: sid("ses_parent"), childID: sid("ses_child"), type: "task" },
      ),
    )
    expect(error).toBeInstanceOf(SubagentDepthError)
    expect(calls).toHaveLength(0)
    expect(registry.get("ses_child")).toBeUndefined()
  })

  test("headroom-full rejects before the scheduler is touched (never queues)", async () => {
    const registry = createJobRegistry()
    const { scheduler, calls } = recordingScheduler()

    const error = await capture(
      dispatchSubagent(
        { registry, scheduler, depthLimit: 2, headroomFull: () => true },
        { parentID: sid("ses_parent"), childID: sid("ses_child"), type: "task" },
      ),
    )
    expect(error).toBeInstanceOf(HeadroomFullError)
    expect(calls).toHaveLength(0)
    expect(registry.get("ses_child")).toBeUndefined()
  })
})

describe("9.1 a task child session is dispatched to a worker", () => {
  test("dispatchSubagent records parent linkage and dispatches at depth 1", async () => {
    const registry = createJobRegistry()
    const { scheduler, calls } = recordingScheduler()

    await dispatchSubagent(
      { registry, scheduler, depthLimit: 1, headroomFull: () => false },
      { parentID: sid("ses_parent"), childID: sid("ses_child"), type: "task", title: "work" },
    )

    expect(calls).toEqual([{ sessionID: "ses_child", depth: 1 }])
    expect(registry.parentOf("ses_child")).toBe("ses_parent")
    expect(registry.get("ses_child")).toMatchObject({ depth: 1, parentID: "ses_parent", status: "running" })
  })

  test("a nested child derives depth from the parent job", async () => {
    const registry = createJobRegistry()
    const { scheduler, calls } = recordingScheduler()
    await dispatchSubagent(
      { registry, scheduler, depthLimit: 2, headroomFull: () => false },
      { parentID: sid("ses_root"), childID: sid("ses_child"), type: "task" },
    )
    await dispatchSubagent(
      { registry, scheduler, depthLimit: 2, headroomFull: () => false },
      { parentID: sid("ses_child"), childID: sid("ses_grandchild"), type: "task" },
    )
    expect(calls).toEqual([
      { sessionID: "ses_child", depth: 1 },
      { sessionID: "ses_grandchild", depth: 2 },
    ])
  })

  test("a scheduler failure marks the job failed", async () => {
    const registry = createJobRegistry()
    const scheduler: SubagentScheduler = { dispatch: () => Promise.reject(new Error("no slot")) }
    const error = await capture(
      dispatchSubagent(
        { registry, scheduler, depthLimit: 1, headroomFull: () => false },
        { parentID: sid("ses_parent"), childID: sid("ses_child"), type: "task" },
      ),
    )
    expect(messageOf(error)).toBe("no slot")
    expect(registry.get("ses_child")?.status).toBe("error")
  })
})

// ---- 9.1 real isolate: the child runs in a Bun worker ----------------------

const ENTRY = new URL("../../fixture/session-execute-worker.ts", import.meta.url)

function spawnTransport(harness: WorkerHarness): ExecutorTransport {
  const handle = harness.spawn<SessionExecutorOutbound, SessionExecutorInbound>(ENTRY)
  return {
    post: (message) => handle.post(message),
    waitFor: (match, options) => handle.waitFor(match, options),
    get alive() {
      return handle.alive
    },
    terminate: (code) => handle.terminate(code),
  }
}

describe("9.1 real isolate: dispatchSubagent runs the child on a headroom worker", () => {
  test("the child executes in a worker and headroom is consumed", async () => {
    const harness = createWorkerHarness({ cores: 4, timeoutMs: 10_000 })
    const pool = createExecutorWorkerPool({
      k: 1,
      spawn: (id) => createExecutorWorker({ id, transport: spawnTransport(harness), workKind: "slow" }),
    })
    const dispatcher = createDispatcher({ pool })
    const registry = createJobRegistry()
    try {
      await dispatchSubagent(
        {
          registry,
          scheduler: dispatcher,
          depthLimit: 1,
          headroomFull: () => dispatcher.headroomCount >= dispatcher.headroomCapacity,
        },
        { parentID: sid("ses_parent"), childID: sid("ses_child"), type: "task" },
      )

      expect(registry.get("ses_child")?.depth).toBe(1)
      expect(registry.childrenOf("ses_parent")).toEqual(["ses_child"])
      expect(dispatcher.headroomCount).toBe(1)
      expect(pool.resultFor(sid("ses_child"))?.sessionID).toBe("ses_child")
    } finally {
      await dispatcher.terminateAll().catch(() => undefined)
      await harness.terminateAll().catch(() => undefined)
    }
  })
})

// ---- 9.6 cascade through the REAL Phase 11 seam ----------------------------

interface PendingRun {
  readonly resolve: () => void
  readonly reject: (error: unknown) => void
}

class CooperativeWorker implements SessionWorker {
  alive = true
  readonly runs: SessionRun[] = []
  terminateCount = 0
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

  async terminate(): Promise<void> {
    this.alive = false
    this.terminateCount++
    const pending = [...this.pending.values()]
    this.pending.clear()
    for (const run of pending) run.reject(new SessionCancelledError({ message: "terminated", sessionID: "" }))
  }
}

class FakePool implements WorkerPool {
  readonly workers: CooperativeWorker[]
  readonly spawned: CooperativeWorker[] = []

  constructor(k: number) {
    this.workers = Array.from({ length: k }, (_, index) => new CooperativeWorker(index + 1))
  }

  spawn(_kind: WorkerKind): SessionWorker {
    const worker = new CooperativeWorker(100 + this.spawned.length)
    this.spawned.push(worker)
    return worker
  }

  async terminateAll(): Promise<void> {}
}

describe("9.6 the real registry wires the parent-cancel cascade", () => {
  test("cancelWithGrace cancels registry children before the parent", async () => {
    const registry = createJobRegistry()
    registry.linkChild("ses_parent", "ses_child_a")
    registry.linkChild("ses_parent", "ses_child_b")

    const pool = new FakePool(3)
    const order: string[] = []
    const cancellation: CancellationConfig = {
      portFor: () => undefined,
      graceMs: 500,
      childrenOf: (sessionID) => registry.childrenOf(sessionID).map((child) => sid(child)),
      onCancelled: (event) => order.push(event.sessionID),
    }
    const dispatcher = createDispatcher({ pool, cancellation })

    const parent = capture(dispatcher.dispatch({ sessionID: sid("ses_parent") }))
    const childA = capture(dispatcher.dispatch({ sessionID: sid("ses_child_a") }))
    const childB = capture(dispatcher.dispatch({ sessionID: sid("ses_child_b") }))
    await flush()

    await dispatcher.cancelWithGrace(sid("ses_parent"))

    expect(await parent).toBeInstanceOf(SessionCancelledError)
    expect(await childA).toBeInstanceOf(SessionCancelledError)
    expect(await childB).toBeInstanceOf(SessionCancelledError)
    expect(order).toEqual(["ses_child_a", "ses_child_b", "ses_parent"])
  })
})
