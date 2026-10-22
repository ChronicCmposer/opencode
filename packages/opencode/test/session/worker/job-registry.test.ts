// Phase 9.2/9.4/9.6 — hub-hosted cross-worker job registry (plan tasks 9.2, 9.4, 9.6).
//
// The hub is the single authority for background jobs. These tests exercise the
// in-memory registry directly: lifecycle (start/extend/complete/fail/cancel),
// cross-worker metadata (parentID/workerID/depth), the parent→children index,
// the terminal-result routing seam, and promotion/wait semantics. No threads.

import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { BackgroundJob } from "@/background/job"
import { createJobRegistry, type JobInfo, type JobRegistry, type JobScheduler } from "@/session/worker/job-registry"
import { createJobClient, serveJobRegistry, type JobFrame } from "@/session/worker/job-client"
import { createListenerPair } from "../../lib/port-pair"
import { createRecordingScheduler } from "../../lib/scheduler"

/**
 * Drive the worker's *production* `BackgroundJob` (`clientLayer`) against the hub
 * registry, then resolve once the job settles. This is the exact seam a `task`
 * subagent traverses in production: the core `StartInput` is reported to the hub
 * through the thin job client. Testing `registry.start` directly would bypass the
 * forwarding that makes parent→child linkage live, so it would not prove the fix.
 */
function productionWorker(registry: JobRegistry) {
  const pair = createListenerPair<JobFrame>()
  serveJobRegistry(registry, pair.hub)
  const client = createJobClient(pair.worker)
  const layer = BackgroundJob.clientLayer(client)

  const start = (input: BackgroundJob.StartInput): Promise<BackgroundJob.Info> =>
    Effect.runPromise(
      Effect.gen(function* () {
        const jobs = yield* BackgroundJob.Service
        const info = yield* jobs.start(input)
        yield* jobs.wait({ id: info.id, timeout: 2_000 })
        return info
      }).pipe(Effect.provide(layer)),
    )

  return {
    start,
    dispose: () => {
      client.dispose()
      pair.close()
    },
  }
}

describe("9.2 registry lifecycle", () => {
  test("start registers a running job with parent linkage and cross-worker metadata", () => {
    const registry = createJobRegistry()
    const info = registry.start({
      id: "ses_child",
      type: "task",
      title: "do work",
      parentID: "ses_parent",
      workerID: 3,
      depth: 1,
    })

    expect(info.status).toBe("running")
    expect(info.parentID).toBe("ses_parent")
    expect(info.workerID).toBe(3)
    expect(info.depth).toBe(1)
    expect(info.started_at).toBeGreaterThan(0)
    expect(registry.get("ses_child")).toEqual(info)
    expect(registry.size).toBe(1)
    expect(registry.childrenOf("ses_parent")).toEqual(["ses_child"])
    expect(registry.parentOf("ses_child")).toBe("ses_parent")
  })

  test("start is idempotent for a running id (joins the existing job)", () => {
    const registry = createJobRegistry()
    const first = registry.start({ id: "ses_job", type: "task" })
    const second = registry.start({ id: "ses_job", type: "other" })
    expect(second).toEqual(first)
    expect(registry.size).toBe(1)
  })

  test("wait resolves on completion; list is ordered by started_at", async () => {
    let clock = 0
    const registry = createJobRegistry({ now: () => (clock += 10) })
    registry.start({ id: "ses_a", type: "task" })
    const waiting = registry.wait({ id: "ses_a" })

    const completed = registry.complete("ses_a", "hello")
    expect(completed?.status).toBe("completed")
    expect(completed?.completed_at).toBeGreaterThan(0)

    const result = await waiting
    expect(result.timedOut).toBe(false)
    expect(result.info?.output).toBe("hello")

    registry.start({ id: "ses_b", type: "task" })
    expect(registry.list().map((job) => job.id)).toEqual(["ses_a", "ses_b"])
  })

  test("fail marks error with the message and resolves waiters", async () => {
    const registry = createJobRegistry()
    registry.start({ id: "ses_job", type: "task" })
    const waiting = registry.wait({ id: "ses_job" })

    registry.fail("ses_job", new Error("boom"))
    const result = await waiting
    expect(result.info).toMatchObject({ status: "error", error: "boom" })
  })

  test("wait times out against the injected scheduler", async () => {
    const scheduler = createRecordingScheduler<JobScheduler>()
    const registry = createJobRegistry({ schedule: scheduler.schedule })
    registry.start({ id: "ses_job", type: "task" })

    const unknown = await registry.wait({ id: "ses_missing" })
    expect(unknown).toEqual({ timedOut: false })

    let settled: { timedOut: boolean; info?: JobInfo } | undefined
    const pending = registry.wait({ id: "ses_job", timeout: 25 }).then((value) => {
      settled = value
      return value
    })

    expect(settled).toBeUndefined()
    expect(scheduler.pending()[0]?.delayMs).toBe(25)
    scheduler.pending()[0]?.run()

    const result = await pending
    expect(result.timedOut).toBe(true)
    expect(result.info?.status).toBe("running")
  })

  test("complete cancels a pending timeout", async () => {
    const scheduler = createRecordingScheduler<JobScheduler>()
    const registry = createJobRegistry({ schedule: scheduler.schedule })
    registry.start({ id: "ses_job", type: "task" })
    const pending = registry.wait({ id: "ses_job", timeout: 100 })
    registry.complete("ses_job", "done")
    expect((await pending).info?.output).toBe("done")
    expect(scheduler.timers[0]?.cancelled).toBe(true)
  })

  test("cancel resolves waiters as cancelled and is idempotent", async () => {
    const registry = createJobRegistry()
    registry.start({ id: "ses_job", type: "task" })
    const waiting = registry.wait({ id: "ses_job" })

    registry.cancel("ses_job")
    expect((await waiting).info?.status).toBe("cancelled")
    expect(registry.cancel("ses_job")?.status).toBe("cancelled")
  })

  test("extend joins a running job; complete only settles at pending zero", () => {
    const registry = createJobRegistry()
    registry.start({ id: "ses_job", type: "task" })
    expect(registry.extend("ses_job")).toBe(true)

    registry.complete("ses_job", "first", 0)
    expect(registry.get("ses_job")?.status).toBe("running")

    registry.complete("ses_job", "second", 1)
    expect(registry.get("ses_job")?.status).toBe("completed")
    expect(registry.get("ses_job")?.output).toBe("second")

    expect(registry.extend("ses_missing")).toBe(false)
  })

  test("promote flips metadata.background and wakes waitForPromotion", async () => {
    const registry = createJobRegistry()
    registry.start({ id: "ses_job", type: "task" })
    const promoted = registry.waitForPromotion("ses_job")

    const info = registry.promote("ses_job")
    expect(info?.metadata?.background).toBe(true)
    expect((await promoted).metadata?.background).toBe(true)
  })

  test("waitForPromotion resolves immediately when already background", async () => {
    const registry = createJobRegistry()
    registry.start({ id: "ses_job", type: "task", metadata: { background: true } })
    expect((await registry.waitForPromotion("ses_job")).metadata?.background).toBe(true)
  })
})

describe("9.4 terminal result routing by parentID", () => {
  test("a settled child with a parentID is routed to that parent", () => {
    const routed: Array<{ parentID: string; info: JobInfo }> = []
    const registry = createJobRegistry({ routeResult: (parentID, info) => routed.push({ parentID, info }) })

    registry.start({ id: "ses_child", type: "task", parentID: "ses_parent" })
    registry.complete("ses_child", "done")

    expect(routed).toHaveLength(1)
    expect(routed[0]?.parentID).toBe("ses_parent")
    expect(routed[0]?.info).toMatchObject({ id: "ses_child", status: "completed", output: "done" })
  })

  test("a root job (no parentID) is never routed", () => {
    const routed: JobInfo[] = []
    const registry = createJobRegistry({ routeResult: (_parentID, info) => routed.push(info) })
    registry.start({ id: "ses_root", type: "task" })
    registry.complete("ses_root", "done")
    expect(routed).toHaveLength(0)
  })

  test("an error result is routed too", () => {
    const routed: Array<{ parentID: string; status: string }> = []
    const registry = createJobRegistry({
      routeResult: (parentID, info) => routed.push({ parentID, status: info.status }),
    })
    registry.start({ id: "ses_child", type: "task", parentID: "ses_parent" })
    registry.fail("ses_child", "nope")
    expect(routed).toEqual([{ parentID: "ses_parent", status: "error" }])
  })
})

describe("9.4/9.6 parent→children index", () => {
  test("linkChild is idempotent and childrenOf is stable-ordered", () => {
    const registry = createJobRegistry()
    registry.linkChild("ses_parent", "ses_a")
    registry.linkChild("ses_parent", "ses_b")
    registry.linkChild("ses_parent", "ses_a")

    expect(registry.childrenOf("ses_parent")).toEqual(["ses_a", "ses_b"])
    expect(registry.childrenOf("ses_other")).toEqual([])
  })
})

describe("index hygiene on id reuse (9.4/9.6)", () => {
  test("reusing a settled id clears its stale parent link", () => {
    const registry = createJobRegistry()
    registry.start({ id: "ses_child", type: "task", parentID: "ses_parent_a" })
    registry.complete("ses_child", "done")
    registry.start({ id: "ses_child", type: "task", parentID: "ses_parent_b" })

    expect(registry.parentOf("ses_child")).toBe("ses_parent_b")
    expect(registry.childrenOf("ses_parent_a")).toEqual([])
    expect(registry.childrenOf("ses_parent_b")).toEqual(["ses_child"])
  })

  test("reusing a settled id without a parent drops the prior edge", () => {
    const registry = createJobRegistry()
    registry.start({ id: "ses_child", type: "task", parentID: "ses_parent" })
    registry.complete("ses_child", "done")
    registry.start({ id: "ses_child", type: "task" })

    expect(registry.parentOf("ses_child")).toBeUndefined()
    expect(registry.childrenOf("ses_parent")).toEqual([])
  })
})

describe("bounded retention (9.2)", () => {
  test("a settled job is evicted after the retention window and its indexes pruned", () => {
    const scheduler = createRecordingScheduler<JobScheduler>()
    const registry = createJobRegistry({ schedule: scheduler.schedule, retentionMs: 1_000 })
    registry.start({ id: "ses_child", type: "task", parentID: "ses_parent" })
    registry.complete("ses_child", "done")

    expect(registry.get("ses_child")?.status).toBe("completed")
    const eviction = scheduler.timers.find((timer) => timer.delayMs === 1_000)
    expect(eviction).toBeDefined()
    eviction!.run()

    expect(registry.get("ses_child")).toBeUndefined()
    expect(registry.parentOf("ses_child")).toBeUndefined()
    expect(registry.childrenOf("ses_parent")).toEqual([])
    expect(registry.size).toBe(0)
  })

  test("the retention cap evicts the oldest settled jobs first", () => {
    let clock = 0
    const registry = createJobRegistry({ maxJobs: 2, retentionMs: Infinity, now: () => (clock += 10) })
    for (const id of ["a", "b", "c"]) {
      registry.start({ id, type: "task" })
      registry.complete(id, "done")
    }

    expect(registry.size).toBe(2)
    expect(registry.get("a")).toBeUndefined()
    expect(registry.get("c")?.status).toBe("completed")
  })
})

describe("9.6 production start path threads parentID worker → hub", () => {
  test("(a) a job started without parentID has neither a parent nor children", async () => {
    const registry = createJobRegistry()
    const worker = productionWorker(registry)

    const info = await worker.start({ id: "ses_root", type: "task", run: Effect.succeed("done") })

    expect(info.id).toBe("ses_root")
    expect(registry.parentOf("ses_root")).toBeUndefined()
    expect(registry.childrenOf("ses_root")).toEqual([])
    worker.dispose()
  })

  test("(b) a production-shaped start with parentID links parent → child on the hub", async () => {
    const registry = createJobRegistry()
    const worker = productionWorker(registry)

    await worker.start({ id: "ses_parent", type: "task", run: Effect.succeed("parent") })
    await worker.start({ id: "ses_child", type: "task", parentID: "ses_parent", run: Effect.succeed("child") })

    expect(registry.get("ses_child")?.parentID).toBe("ses_parent")
    expect(registry.childrenOf("ses_parent")).toEqual(["ses_child"])
    expect(registry.parentOf("ses_child")).toBe("ses_parent")
    worker.dispose()
  })

  test("(c) routeResult fires only for a child that carries a parentID", async () => {
    const routed: Array<{ parentID: string; id: string }> = []
    const registry = createJobRegistry({ routeResult: (parentID, info) => routed.push({ parentID, id: info.id }) })
    const worker = productionWorker(registry)

    await worker.start({ id: "ses_root", type: "task", run: Effect.succeed("root") })
    await worker.start({ id: "ses_child", type: "task", parentID: "ses_parent", run: Effect.succeed("child") })

    expect(routed).toEqual([{ parentID: "ses_parent", id: "ses_child" }])
    worker.dispose()
  })
})
