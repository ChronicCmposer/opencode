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
import { Deferred, Effect } from "effect"
import { BackgroundJob } from "@/background/job"
import {
  createJobClient,
  serveJobRegistry,
  type JobClient,
  type JobFrame,
} from "@/session/worker/job-client"
import { createJobRegistry } from "@/session/worker/job-registry"
import { createListenerPair } from "../lib/port-pair"

describe("BackgroundJob.clientLayer routes through the hub registry", () => {
  test("runs the work locally and reports completion to the registry", async () => {
    const registry = createJobRegistry()
    const pair = createListenerPair<JobFrame>()
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
    const a = createListenerPair<JobFrame>()
    const b = createListenerPair<JobFrame>()
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
    const pair = createListenerPair<JobFrame>()
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

  test("concurrent starts of the same id run the work exactly once", async () => {
    const registry = createJobRegistry()
    const pair = createListenerPair<JobFrame>()
    serveJobRegistry(registry, pair.hub)
    const client = createJobClient(pair.worker)
    let runs = 0

    const [a, b] = await Effect.runPromise(
      Effect.gen(function* () {
        const jobs = yield* BackgroundJob.Service
        const run = Effect.sync(() => {
          runs += 1
          return "done"
        })
        const started = yield* Effect.all(
          [jobs.start({ id: "job_once", type: "test", run }), jobs.start({ id: "job_once", type: "test", run })],
          { concurrency: "unbounded" },
        )
        yield* jobs.wait({ id: "job_once" })
        return started
      }).pipe(Effect.provide(BackgroundJob.clientLayer(client))),
    )

    expect(runs).toBe(1)
    expect(a.id).toBe("job_once")
    expect(b.id).toBe("job_once")
    client.dispose()
  })

  test("onPromote runs locally when the job is promoted", async () => {
    const registry = createJobRegistry()
    const pair = createListenerPair<JobFrame>()
    serveJobRegistry(registry, pair.hub)
    const client = createJobClient(pair.worker)

    const promoted = await Effect.runPromise(
      Effect.gen(function* () {
        const jobs = yield* BackgroundJob.Service
        const signal = yield* Deferred.make<void>()
        const info = yield* jobs.start({
          id: "job_promote",
          type: "test",
          onPromote: Deferred.succeed(signal, undefined).pipe(Effect.asVoid),
          run: Effect.never,
        })
        const after = yield* jobs.promote(info.id)
        // The callback cannot cross the job port; the layer must run it locally.
        yield* Deferred.await(signal).pipe(Effect.timeout("1 second"))
        return after
      }).pipe(Effect.provide(BackgroundJob.clientLayer(client))),
    )

    expect(promoted?.metadata?.background).toBe(true)
    client.dispose()
  })

  test("a failed completion report terminates the job instead of stranding it", async () => {
    const registry = createJobRegistry()
    const pair = createListenerPair<JobFrame>()
    serveJobRegistry(registry, pair.hub)
    const base = createJobClient(pair.worker)
    const client: JobClient = {
      ...base,
      complete: async () => {
        throw new Error("report failed")
      },
    }

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const jobs = yield* BackgroundJob.Service
        const info = yield* jobs.start({ id: "job_report_fail", type: "test", run: Effect.succeed("done") })
        return yield* jobs.wait({ id: info.id, timeout: 2000 })
      }).pipe(Effect.provide(BackgroundJob.clientLayer(client))),
    )

    expect(result.timedOut).toBe(false)
    expect(result.info?.status).toBe("cancelled")
    base.dispose()
  })
})
