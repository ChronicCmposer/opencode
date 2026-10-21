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
import {
  createDispatcher,
  SessionCancelledError,
  type CancellationConfig,
} from "@/session/worker/dispatch"
import { createExecutorWorker, createExecutorWorkerPool } from "@/session/worker/execute"
import { createJobRegistry } from "@/session/worker/job-registry"
import {
  assertSubagentAdmitted,
  dispatchSubagent,
  HeadroomFullError,
  SubagentDepthError,
  type SubagentScheduler,
} from "@/session/worker/subagent"
import { createWorkerHarness } from "../../lib/worker-harness"
import { spawnTransport } from "../../lib/transport"
import { capture, flush, sid } from "../../lib/async"
import { createFakeWorkerPool } from "../../lib/fake-worker-pool"

const messageOf = (error: unknown): string => (error instanceof Error ? error.message : String(error))

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
    expect(() => assertSubagentAdmitted({ depth: 1, limit: 1, headroomFull: false })).not.toThrow()
    expect(() => assertSubagentAdmitted({ depth: 1, limit: 2, headroomFull: false })).not.toThrow()
  })

  test("depth below 1 or non-integer is rejected loud", () => {
    expect(() => assertSubagentAdmitted({ depth: 0, limit: 1, headroomFull: false })).toThrow(SubagentDepthError)
    expect(() => assertSubagentAdmitted({ depth: 1.5, limit: 2, headroomFull: false })).toThrow(SubagentDepthError)
  })

  test("a non-integer or negative limit is rejected loud", () => {
    expect(() => assertSubagentAdmitted({ depth: 1, limit: -1, headroomFull: false })).toThrow(SubagentDepthError)
    expect(() => assertSubagentAdmitted({ depth: 1, limit: 2.5, headroomFull: false })).toThrow(SubagentDepthError)
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
    expect(error).toEqual({ ok: false, error: expect.any(SubagentDepthError) })
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
    expect(error).toEqual({ ok: false, error: expect.any(HeadroomFullError) })
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

  test("dispatchSubagent returns the child's settled registry outcome", async () => {
    const registry = createJobRegistry()
    const scheduler: SubagentScheduler = {
      dispatch: async (request) => {
        registry.complete(request.sessionID, "child-output")
      },
    }
    const info = await dispatchSubagent(
      { registry, scheduler, depthLimit: 1, headroomFull: () => false },
      { parentID: sid("ses_parent"), childID: sid("ses_child"), type: "task" },
    )
    expect(info).toMatchObject({ id: "ses_child", status: "completed", output: "child-output" })
  })

  test("a scheduler failure marks the job failed", async () => {
    const registry = createJobRegistry()
    const scheduler: SubagentScheduler = { dispatch: () => Promise.reject(new Error("no slot")) }
    const settled = await capture(
      dispatchSubagent(
        { registry, scheduler, depthLimit: 1, headroomFull: () => false },
        { parentID: sid("ses_parent"), childID: sid("ses_child"), type: "task" },
      ),
    )
    expect(settled.ok).toBe(false)
    if (!settled.ok) expect(messageOf(settled.error)).toBe("no slot")
    expect(registry.get("ses_child")?.status).toBe("error")
  })
})

// ---- 9.1 real isolate: the child runs in a Bun worker ----------------------

describe("9.1 real isolate: dispatchSubagent runs the child on a headroom worker", () => {
  test("the child executes in a worker and headroom is consumed", async () => {
    await using harness = createWorkerHarness({ cores: 4, timeoutMs: 10_000 })
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
          headroomFull: () => dispatcher.headroomSlotCount >= dispatcher.headroomCapacity,
        },
        { parentID: sid("ses_parent"), childID: sid("ses_child"), type: "task" },
      )

      expect(registry.get("ses_child")?.depth).toBe(1)
      expect(registry.childrenOf("ses_parent")).toEqual(["ses_child"])
      expect(dispatcher.headroomSlotCount).toBe(1)
      expect(pool.resultFor(sid("ses_child"))?.sessionID).toBe("ses_child")
    } finally {
      // The harness is torn down by `await using`; only the dispatcher is explicit.
      await dispatcher.terminateAll().catch(() => undefined)
    }
  })
})

// ---- 9.6 cascade through the REAL Phase 11 seam ----------------------------

describe("9.6 the real registry wires the parent-cancel cascade", () => {
  test("cancelWithGrace cancels registry children before the parent", async () => {
    const registry = createJobRegistry()
    registry.linkChild("ses_parent", "ses_child_a")
    registry.linkChild("ses_parent", "ses_child_b")

    const pool = createFakeWorkerPool(3)
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

    expect(await parent).toEqual({ ok: false, error: expect.any(SessionCancelledError) })
    expect(await childA).toEqual({ ok: false, error: expect.any(SessionCancelledError) })
    expect(await childB).toEqual({ ok: false, error: expect.any(SessionCancelledError) })
    expect(order).toEqual(["ses_child_a", "ses_child_b", "ses_parent"])
  })
})
