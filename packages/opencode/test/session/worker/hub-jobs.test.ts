// Phase 9.2/9.4 + 14 — the hub hosts the cross-worker job registry (plan tasks
// 9.2/9.4; decision line 54) and the worker layer accepts the thin client.
//
// `createSessionHub` owns one `JobRegistry` for the whole hub. Each worker
// attaches its dedicated job port (`attachJobPort`), the hub serves the registry
// on it, and a settled child is routed to the worker hosting its `parentID`.
// These tests drive the hub over an in-memory executor pool and real
// `MessageChannel`s, so the wiring is exercised end-to-end without spawning
// isolates.

import { describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import type { SessionID } from "@/session/schema"
import { createSessionHub } from "@/session/worker/hub"
import { createJobClient, jobPortFromMessagePort } from "@/session/worker/job-client"
import { BackgroundJobClientService, ModelClient, buildSessionWorkerLayer } from "@/session/worker/layer"
import { dispatchSubagent as dispatchSubagentToHub } from "@/session/worker/subagent"
import {
  createExecutorWorkerPool,
  type ExecutorRun,
  type ExecutorSessionWorker,
  type ExecutorWorker,
  type ExecutorWorkerPool,
  type SessionExecutionResult,
} from "@/session/worker/execute"
import { flush, sid } from "../../lib/async"
import { workerSnapshot } from "../../lib/snapshot"

/** A worker whose run the test completes explicitly, so a session stays running. */
class ControlledWorker implements ExecutorWorker {
  alive = true
  private resolve: (() => void) | undefined
  private readonly results = new Map<SessionID, SessionExecutionResult>()

  constructor(readonly id: number) {}

  execute(run: ExecutorRun): Promise<void> {
    this.results.set(run.sessionID, { sessionID: run.sessionID, value: null })
    return new Promise<void>((resolve) => {
      this.resolve = resolve
    })
  }

  finish(): void {
    this.resolve?.()
    this.resolve = undefined
  }

  async cancel(): Promise<void> {}
  async assertNotBusy(): Promise<boolean> {
    return false
  }
  resultFor(sessionID: SessionID): SessionExecutionResult | undefined {
    return this.results.get(sessionID)
  }
  async terminate(): Promise<void> {
    this.alive = false
    this.finish()
  }
}

class ControlledPool implements ExecutorWorkerPool {
  readonly workers: ControlledWorker[]
  private nextSpawnId = 100

  constructor(k: number) {
    this.workers = Array.from({ length: k }, (_, index) => new ControlledWorker(index + 1))
  }

  spawn(): ExecutorSessionWorker {
    return new ControlledWorker(this.nextSpawnId++)
  }

  async terminateAll(): Promise<void> {
    await Promise.all(this.workers.map((worker) => worker.terminate()))
  }

  byID(id: number): ExecutorWorker | undefined {
    return this.workers.find((worker) => worker.id === id)
  }

  resultFor(sessionID: SessionID): SessionExecutionResult | undefined {
    return this.workers.find((worker) => worker.resultFor(sessionID) !== undefined)?.resultFor(sessionID)
  }
}

describe("hub-hosted job registry (9.2)", () => {
  test("the hub serves one registry to a worker's job port", async () => {
    const pool = new ControlledPool(1)
    const hub = createSessionHub({ pool })
    const channel = new MessageChannel()
    const client = createJobClient(jobPortFromMessagePort(channel.port2))
    const dispose = hub.attachJobPort(1, jobPortFromMessagePort(channel.port1))

    await client.start({ id: "ses_job", type: "task" })
    expect(hub.jobs.get("ses_job")?.status).toBe("running")

    await client.complete("ses_job", "done")
    expect(hub.jobs.get("ses_job")?.status).toBe("completed")
    expect(hub.jobs.get("ses_job")?.output).toBe("done")

    dispose()
    client.dispose()
    await hub.terminateAll()
  })
})

describe("child result routed to the parent worker (9.4)", () => {
  test("a settled child is pushed to the worker hosting its parentID", async () => {
    const pool = new ControlledPool(2)
    const hub = createSessionHub({ pool })

    const parentRun = hub.dispatcher.dispatch({ sessionID: sid("ses_parent"), depth: 0 })
    const parentSettled = parentRun.then(
      () => undefined,
      () => undefined,
    )
    await flush()
    expect(hub.dispatcher.registry.get(sid("ses_parent"))?.workerID).toBe(1)

    const parentChannel = new MessageChannel()
    const parentClient = createJobClient(jobPortFromMessagePort(parentChannel.port2))
    const disposeParent = hub.attachJobPort(1, jobPortFromMessagePort(parentChannel.port1))
    const childChannel = new MessageChannel()
    const childClient = createJobClient(jobPortFromMessagePort(childChannel.port2))
    const disposeChild = hub.attachJobPort(2, jobPortFromMessagePort(childChannel.port1))

    const received: string[] = []
    parentClient.onChildResult((info) => received.push(info.id))

    await childClient.start({ id: "ses_child", type: "task", parentID: "ses_parent" })
    await childClient.complete("ses_child", "done")
    await flush()

    expect(received).toEqual(["ses_child"])

    disposeParent()
    disposeChild()
    parentClient.dispose()
    childClient.dispose()
    pool.workers[0].finish()
    await parentSettled
    await hub.terminateAll()
  })
})

describe("worker layer injection (14)", () => {
  test("buildSessionWorkerLayer accepts the cross-worker client as the backgroundJob handle", async () => {
    const channel = new MessageChannel()
    const client = createJobClient(jobPortFromMessagePort(channel.port1))
    const built = buildSessionWorkerLayer({
      snapshot: workerSnapshot(),
      env: { values: {} },
      clients: {
        writer: { query: async () => [], publish: async () => ({ seq: 1, committed: true }) },
        egress: { emit: () => {} },
        broker: { call: async () => null },
        backgroundJob: client,
      },
      modelClient: Layer.succeed(ModelClient, { complete: () => Effect.succeed({ text: "ok" }) }),
    })

    const resolved = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* BackgroundJobClientService
      }).pipe(Effect.provide(built.layer)),
    )
    expect(resolved).toBe(client)
    expect(typeof resolved.wait).toBe("function")

    client.dispose()
  })
})

describe("hub decoupled worker lookup", () => {
  test("routeResult consults dispatcher.workerForSession when it is present", async () => {
    const pool = new ControlledPool(2)
    const hub = createSessionHub({ pool })
    // Simulate the dispatcher gaining `workerForSession` (consolidation request):
    // it must win over the registry fallback. No session is reserved, so the
    // registry alone would route nowhere.
    ;(hub.dispatcher as unknown as { workerForSession: (sessionID: SessionID) => number | undefined }).workerForSession =
      () => 2

    const channel = new MessageChannel()
    const client = createJobClient(jobPortFromMessagePort(channel.port2))
    const dispose = hub.attachJobPort(2, jobPortFromMessagePort(channel.port1))

    const received: string[] = []
    client.onChildResult((info) => received.push(info.id))

    await client.start({ id: "ses_child", type: "task", parentID: "ses_parent" })
    await client.complete("ses_child", "done")
    await flush()

    expect(received).toEqual(["ses_child"])

    dispose()
    client.dispose()
    await hub.terminateAll()
  })
})

// ---------------------------------------------------------------------------
// Solution 1 (B1) — bridging a dispatched child's worker result into the
// registry. The hub's `dispatchSubagent` RPC settles the child's job with the
// last assistant text the headroom worker produced, so `JobInfo.output` is
// populated for the parent that awaits it.
// ---------------------------------------------------------------------------

/** An in-memory worker that completes instantly and keeps a `WithParts` result. */
class FinalTextWorker implements ExecutorWorker {
  alive = true
  private readonly results = new Map<SessionID, SessionExecutionResult>()

  constructor(readonly id: number) {}

  async execute(run: ExecutorRun): Promise<void> {
    this.results.set(run.sessionID, {
      sessionID: run.sessionID,
      value: {
        info: { role: "assistant" },
        parts: [
          { type: "text", text: "thinking..." },
          { type: "text", text: "the answer" },
        ],
      },
    })
  }
  async cancel(): Promise<void> {}
  async assertNotBusy(): Promise<boolean> {
    return false
  }
  resultFor(sessionID: SessionID): SessionExecutionResult | undefined {
    return this.results.get(sessionID)
  }
  async terminate(): Promise<void> {
    this.alive = false
  }
}

describe("B1 final assistant text bridge (dispatchSubagent)", () => {
  test("a dispatched child settles with its last assistant text as JobInfo.output", async () => {
    const pool = createExecutorWorkerPool({ k: 1, spawn: (id) => new FinalTextWorker(id) })
    const hub = createSessionHub({ pool })
    try {
      const info = await dispatchSubagentToHub(
        {
          registry: hub.jobs,
          scheduler: hub.dispatcher,
          depthLimit: 1,
          headroomFull: () => false,
          resultFor: (sessionID) => pool.resultFor(sessionID),
        },
        { parentID: sid("ses_parent"), childID: sid("ses_child"), type: "task", depth: 1 },
      )

      expect(info).toMatchObject({ id: "ses_child", status: "completed", output: "the answer", depth: 1 })
      expect(hub.jobs.get("ses_child")?.output).toBe("the answer")
    } finally {
      await hub.terminateAll()
    }
  })
})
