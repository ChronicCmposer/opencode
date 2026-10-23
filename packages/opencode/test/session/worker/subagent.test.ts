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
  WorkerCrashedError,
  type CancellationConfig,
} from "@/session/worker/dispatch"
import { createExecutorWorker, createExecutorWorkerPool, createSessionDispatch } from "@/session/worker/execute"
import { createJobRegistry, type JobInfo, type JobRegistry } from "@/session/worker/job-registry"
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

  test("depth>limit returns a failed job before the scheduler is touched (never queues)", async () => {
    const registry = createJobRegistry()
    registry.start({ id: "ses_parent", type: "task", depth: 1 })
    const { scheduler, calls } = recordingScheduler()

    const info = await dispatchSubagent(
      { registry, scheduler, depthLimit: 1, headroomFull: () => false },
      { parentID: sid("ses_parent"), childID: sid("ses_child"), type: "task" },
    )
    expect(info).toMatchObject({ id: "ses_child", status: "error", depth: 2 })
    expect(info.error).toContain("exceeds the limit")
    expect(calls).toHaveLength(0)
    expect(registry.get("ses_child")).toBeUndefined()
  })

  test("headroom-full returns a failed job before the scheduler is touched (never queues)", async () => {
    const registry = createJobRegistry()
    const { scheduler, calls } = recordingScheduler()

    const info = await dispatchSubagent(
      { registry, scheduler, depthLimit: 2, headroomFull: () => true },
      { parentID: sid("ses_parent"), childID: sid("ses_child"), type: "task" },
    )
    expect(info).toMatchObject({ id: "ses_child", status: "error", depth: 1 })
    expect(info.error).toContain("headroom is full")
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
    // The dispatch resolved, so the bridge settled the child (output is empty
    // because this scheduler reports no worker result).
    expect(registry.get("ses_child")).toMatchObject({ depth: 1, parentID: "ses_parent", status: "completed" })
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

  test("a scheduler failure returns a failed job (not a throw)", async () => {
    const registry = createJobRegistry()
    const scheduler: SubagentScheduler = { dispatch: () => Promise.reject(new Error("no slot")) }
    const info = await dispatchSubagent(
      { registry, scheduler, depthLimit: 1, headroomFull: () => false },
      { parentID: sid("ses_parent"), childID: sid("ses_child"), type: "task" },
    )
    expect(info.status).toBe("error")
    expect(info.error).toBe("no slot")
    expect(registry.get("ses_child")?.status).toBe("error")
  })

  test("the child's final assistant text lands in JobInfo.output", async () => {
    const registry = createJobRegistry()
    const scheduler: SubagentScheduler = { dispatch: () => Promise.resolve() }
    const info = await dispatchSubagent(
      {
        registry,
        scheduler,
        depthLimit: 1,
        headroomFull: () => false,
        resultFor: (sessionID) => ({
          sessionID,
          value: {
            info: { role: "assistant" },
            parts: [
              { type: "text", text: "first" },
              { type: "text", text: "final answer" },
            ],
          },
        }),
      },
      { parentID: sid("ses_parent"), childID: sid("ses_child"), type: "task" },
    )
    expect(info).toMatchObject({ id: "ses_child", status: "completed", output: "final answer" })
    expect(registry.get("ses_child")?.output).toBe("final answer")
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

// ---- 33 end-to-end: a depth-1 child routes its settled result to the parent ---

/** One settled child routed to the parent worker, as the hub router would push it. */
interface Routed {
  readonly parentID: string
  readonly id: string
  readonly status: JobInfo["status"]
  readonly output: string | undefined
}

/**
 * A registry wired to capture every routed child result, with a depth-0 parent
 * already running — the minimum hub state a child's result routes against.
 */
function routingRegistry(): { registry: JobRegistry; routed: Routed[] } {
  const routed: Routed[] = []
  const registry = createJobRegistry({
    routeResult: (parentID, info) => routed.push({ parentID, id: info.id, status: info.status, output: info.output }),
  })
  registry.start({ id: "ses_parent", type: "task", depth: 0 })
  return { registry, routed }
}

/**
 * The child worker's job client, faked: it records the dispatch and then settles
 * the child's hub-registry job exactly as the worker would over its job port.
 */
function headroomWorker(registry: JobRegistry, output: string): { scheduler: SubagentScheduler; dispatched: Recorded[] } {
  const dispatched: Recorded[] = []
  const scheduler: SubagentScheduler = {
    dispatch: async (request) => {
      dispatched.push({ sessionID: request.sessionID, depth: request.depth })
      registry.complete(request.sessionID, output)
    },
  }
  return { scheduler, dispatched }
}

describe("33 subagent path end-to-end: a depth-1 child routes its settled result to the parent", () => {
  test("a depth-1 child runs on a headroom worker and its completed result routes to the parent", async () => {
    const { registry, routed } = routingRegistry()
    const { scheduler, dispatched } = headroomWorker(registry, "child-output")

    const info = await dispatchSubagent(
      { registry, scheduler, depthLimit: 1, headroomFull: () => false },
      { parentID: sid("ses_parent"), childID: sid("ses_child"), type: "task", title: "work" },
    )

    expect(dispatched).toEqual([{ sessionID: "ses_child", depth: 1 }])
    expect(info).toMatchObject({
      id: "ses_child",
      status: "completed",
      output: "child-output",
      depth: 1,
      parentID: "ses_parent",
    })
    expect(routed).toEqual([{ parentID: "ses_parent", id: "ses_child", status: "completed", output: "child-output" }])
    expect(registry.childrenOf("ses_parent")).toEqual(["ses_child"])
  })

  test("a cancelled child settles as cancelled and routes the cancellation to the parent", async () => {
    const { registry, routed } = routingRegistry()
    const scheduler: SubagentScheduler = {
      dispatch: () => Promise.reject(new SessionCancelledError({ message: "cancelled", sessionID: sid("ses_child") })),
    }

    const info = await dispatchSubagent(
      { registry, scheduler, depthLimit: 1, headroomFull: () => false },
      { parentID: sid("ses_parent"), childID: sid("ses_child"), type: "task" },
    )

    expect(info.status).toBe("cancelled")
    expect(registry.get("ses_child")?.status).toBe("cancelled")
    expect(routed).toEqual([{ parentID: "ses_parent", id: "ses_child", status: "cancelled", output: undefined }])
  })

  test("a crashed child settles as an error (not a cancellation) and routes the error", async () => {
    const { registry, routed } = routingRegistry()
    const scheduler: SubagentScheduler = {
      dispatch: () => Promise.reject(new WorkerCrashedError({ message: "worker 7 crashed", workerID: 7 })),
    }

    const info = await dispatchSubagent(
      { registry, scheduler, depthLimit: 1, headroomFull: () => false },
      { parentID: sid("ses_parent"), childID: sid("ses_child"), type: "task" },
    )

    expect(info.status).toBe("error")
    expect(registry.get("ses_child")?.status).toBe("error")
    expect(routed).toEqual([{ parentID: "ses_parent", id: "ses_child", status: "error", output: undefined }])
  })

  test("an over-deep child is rejected before it is registered or routed", async () => {
    const { registry, routed } = routingRegistry()
    const { scheduler, dispatched } = headroomWorker(registry, "unreachable")

    const info = await dispatchSubagent(
      { registry, scheduler, depthLimit: 0, headroomFull: () => false },
      { parentID: sid("ses_parent"), childID: sid("ses_child"), type: "task" },
    )

    expect(info).toMatchObject({ id: "ses_child", status: "error" })
    expect(dispatched).toHaveLength(0)
    expect(registry.get("ses_child")).toBeUndefined()
    expect(routed).toHaveLength(0)
  })
})

describe("33 real dispatcher: a depth-1 child consumes headroom and routing fires on settle", () => {
  test("the child runs on a spawned headroom worker and its result routes to the parent", async () => {
    const pool = createFakeWorkerPool(2)
    const routed: Array<{ parentID: string; id: string; status: JobInfo["status"] }> = []
    const registry = createJobRegistry({
      routeResult: (parentID, info) => routed.push({ parentID, id: info.id, status: info.status }),
    })
    registry.start({ id: "ses_parent", type: "task", depth: 0 })
    const dispatcher = createDispatcher({ pool })

    try {
      const settled = dispatchSubagent(
        {
          registry,
          scheduler: dispatcher,
          depthLimit: 1,
          headroomFull: () => dispatcher.headroomSlotCount >= dispatcher.headroomCapacity,
        },
        { parentID: sid("ses_parent"), childID: sid("ses_child"), type: "task" },
      )
      await flush()

      // The depth-1 child took a spawned headroom slot, not a foreground worker.
      expect(dispatcher.headroomSlotCount).toBe(1)
      expect(pool.spawned).toHaveLength(1)
      expect(pool.spawned[0].runs).toEqual([{ sessionID: sid("ses_child"), depth: 1 }])

      // The child worker settles its job over the job port, then its run ends.
      registry.complete("ses_child", "child-output")
      pool.spawned[0].finish(sid("ses_child"))

      const info = await settled
      expect(info).toMatchObject({ id: "ses_child", status: "completed", output: "child-output", depth: 1 })
      expect(routed).toEqual([{ parentID: "ses_parent", id: "ses_child", status: "completed" }])
    } finally {
      await dispatcher.terminateAll()
    }
  })
})

describe("33 production execution seam: depth plumbs through createSessionDispatch", () => {
  test("ensureRunning(child, 1) runs the child on headroom, not the freed foreground slot", async () => {
    await using harness = createWorkerHarness({ cores: 4, timeoutMs: 10_000 })
    const pool = createExecutorWorkerPool({
      k: 1,
      spawn: (id) => createExecutorWorker({ id, transport: spawnTransport(harness), workKind: "slow" }),
    })
    const dispatcher = createDispatcher({ pool })
    const dispatch = createSessionDispatch({ dispatcher, pool })
    try {
      // The root finishes first, leaving the foreground worker idle.
      const parent = await dispatch.ensureRunning(sid("ses_parent"))
      expect(parent.sessionID).toBe("ses_parent")
      expect(dispatcher.headroomSlotCount).toBe(0)

      // A depth-1 dispatch must still spawn headroom rather than borrow the idle
      // foreground slot — the exact bug when depth is hardcoded to 0.
      const child = await dispatch.ensureRunning(sid("ses_child"), 1)
      expect(child.sessionID).toBe("ses_child")
      expect(dispatcher.headroomSlotCount).toBe(1)
    } finally {
      await dispatcher.terminateAll().catch(() => undefined)
    }
  })

  test("startShell runs on a worker through the inbound startShell frame", async () => {
    await using harness = createWorkerHarness({ cores: 4, timeoutMs: 10_000 })
    const pool = createExecutorWorkerPool({
      k: 1,
      spawn: (id) => createExecutorWorker({ id, transport: spawnTransport(harness), workKind: "slow" }),
    })
    const dispatcher = createDispatcher({ pool })
    const dispatch = createSessionDispatch({ dispatcher, pool })
    try {
      const shell = await dispatch.startShell(sid("ses_shell"))
      expect(shell.sessionID).toBe("ses_shell")
    } finally {
      await dispatcher.terminateAll().catch(() => undefined)
    }
  })
})
