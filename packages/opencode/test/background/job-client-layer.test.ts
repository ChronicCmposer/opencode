// Phase 14 — the production `BackgroundJob` routes through the hub-hosted
// registry (plan decision line 54).
//
// `BackgroundJob.clientLayer(client)` adapts the worker-side thin client to the
// core `BackgroundJob.Interface`: the run still executes in the worker, but
// every lifecycle transition is reported to the hub registry, so `wait` resolves
// across workers and a parent crash can cancel children. These tests drive the
// layer against an in-process registry served over `MessageChannel`-free port
// pairs (no isolates).

import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { BackgroundJob } from "@/background/job"
import {
  createJobClient,
  serveJobRegistry,
  type JobFrame,
  type JobPort,
} from "@/session/worker/job-client"
import { createJobRegistry } from "@/session/worker/job-registry"

/** An in-process hub↔worker port pair; `hub.post` reaches the worker client. */
function createPortPair(): { hub: JobPort; worker: JobPort } {
  const hubListeners = new Set<(frame: JobFrame) => void>()
  const workerListeners = new Set<(frame: JobFrame) => void>()
  const deliver = (listeners: Set<(frame: JobFrame) => void>, frame: JobFrame): void => {
    for (const listener of listeners) listener(frame)
  }
  return {
    hub: {
      post: (frame) => deliver(workerListeners, frame),
      onMessage: (handler) => {
        hubListeners.add(handler)
        return () => hubListeners.delete(handler)
      },
    },
    worker: {
      post: (frame) => deliver(hubListeners, frame),
      onMessage: (handler) => {
        workerListeners.add(handler)
        return () => workerListeners.delete(handler)
      },
    },
  }
}

describe("BackgroundJob.clientLayer routes through the hub registry", () => {
  test("runs the work locally and reports completion to the registry", async () => {
    const registry = createJobRegistry()
    const pair = createPortPair()
    serveJobRegistry(registry, pair.hub)
    const client = createJobClient(pair.worker)

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const jobs = yield* BackgroundJob.Service
        const info = yield* jobs.start({ id: "job_1", type: "test", run: Effect.succeed("done") })
        return yield* jobs.wait({ id: info.id })
      }).pipe(Effect.provide(BackgroundJob.clientLayer(client))),
    )

    expect(result.timedOut).toBe(false)
    expect(result.info?.status).toBe("completed")
    expect(result.info?.output).toBe("done")
    expect(registry.get("job_1")?.status).toBe("completed")
    client.dispose()
  })

  test("a wait on one worker resolves when another worker completes the job", async () => {
    const registry = createJobRegistry()
    const a = createPortPair()
    const b = createPortPair()
    serveJobRegistry(registry, a.hub)
    serveJobRegistry(registry, b.hub)
    const clientA = createJobClient(a.worker)
    const clientB = createJobClient(b.worker)

    await clientA.start({ id: "job_x", type: "test" })

    const waiting = Effect.runPromise(
      Effect.gen(function* () {
        const jobs = yield* BackgroundJob.Service
        return yield* jobs.wait({ id: "job_x" })
      }).pipe(Effect.provide(BackgroundJob.clientLayer(clientB))),
    )

    await clientA.complete("job_x", "cross-worker")
    const result = await waiting
    expect(result.info?.output).toBe("cross-worker")

    clientA.dispose()
    clientB.dispose()
  })

  test("a failed run is reported as an error job", async () => {
    const registry = createJobRegistry()
    const pair = createPortPair()
    serveJobRegistry(registry, pair.hub)
    const client = createJobClient(pair.worker)

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const jobs = yield* BackgroundJob.Service
        const info = yield* jobs.start({ id: "job_e", type: "test", run: Effect.fail(new Error("boom")) })
        return yield* jobs.wait({ id: info.id })
      }).pipe(Effect.provide(BackgroundJob.clientLayer(client))),
    )

    expect(result.info?.status).toBe("error")
    expect(result.info?.error).toBe("boom")
    client.dispose()
  })
})
