import { describe, expect } from "bun:test"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Cause, Deferred, Effect, Exit } from "effect"
import { BackgroundJob } from "@/background/job"
import { BackgroundJob as CoreBackgroundJob } from "@opencode-ai/core/background-job"
import { testEffect } from "../lib/effect"

const it = testEffect(LayerNode.compile(BackgroundJob.node))

describe("background.job", () => {
  it.instance("tracks started jobs through completion", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const latch = yield* Deferred.make<void>()
      const job = yield* jobs.start({
        type: "test",
        title: "test job",
        run: Deferred.await(latch).pipe(Effect.as("done")),
      })

      expect(job.id.startsWith("job_")).toBe(true)
      expect(job.status).toBe("running")
      expect(job.title).toBe("test job")

      yield* Deferred.succeed(latch, undefined)
      const done = yield* jobs.wait({ id: job.id })

      expect(done.timedOut).toBe(false)
      expect(done.info?.status).toBe("completed")
      expect(done.info?.output).toBe("done")
      expect((yield* jobs.list()).map((item) => item.id)).toEqual([job.id])
    }),
  )

  it.instance("returns a running snapshot when wait times out", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const job = yield* jobs.start({
        type: "test",
        run: Effect.never,
      })

      const result = yield* jobs.wait({ id: job.id, timeout: 1 })

      expect(result.timedOut).toBe(true)
      expect(result.info?.status).toBe("running")
    }),
  )

  it.instance("deduplicates concurrent starts for a running id", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const started = yield* Deferred.make<void>()
      const id = "job_test"
      const [first, second] = yield* Effect.all(
        [
          jobs.start({
            id,
            type: "test",
            run: Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)),
          }),
          jobs.start({
            id,
            type: "test",
            run: Effect.fail(new Error("duplicate started")),
          }),
        ],
        { concurrency: "unbounded" },
      )

      yield* Deferred.await(started)

      expect(first.id).toBe(id)
      expect(second.id).toBe(id)
      expect(first.status).toBe("running")
      expect(second.status).toBe("running")
      expect((yield* jobs.list()).map((item) => item.id)).toEqual([id])

      yield* jobs.cancel(id)
    }),
  )

  it.instance("waits for extensions before completing a running job", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const first = yield* Deferred.make<void>()
      const second = yield* Deferred.make<void>()
      const job = yield* jobs.start({
        type: "test",
        run: Deferred.await(first).pipe(Effect.as("first")),
      })

      expect(yield* jobs.extend({ id: job.id, run: Deferred.await(second).pipe(Effect.as("second")) })).toBe(true)
      yield* Deferred.succeed(first, undefined)
      expect((yield* jobs.get(job.id))?.status).toBe("running")

      yield* Deferred.succeed(second, undefined)
      const done = yield* jobs.wait({ id: job.id })
      expect(done.info?.status).toBe("completed")
      expect(done.info?.output).toBe("second")
    }),
  )

  it.instance("runs extensions after earlier work completes", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const first = yield* Deferred.make<void>()
      const order: string[] = []
      const job = yield* jobs.start({
        type: "test",
        run: Effect.sync(() => order.push("start")).pipe(Effect.andThen(Deferred.await(first)), Effect.as("first")),
      })

      expect(
        yield* jobs.extend({
          id: job.id,
          run: Effect.sync(() => order.push("extend")).pipe(Effect.as("second")),
        }),
      ).toBe(true)
      yield* Effect.yieldNow
      expect(order).toEqual(["start"])

      yield* Deferred.succeed(first, undefined)
      expect((yield* jobs.wait({ id: job.id })).info?.output).toBe("second")
      expect(order).toEqual(["start", "extend"])
    }),
  )

  it.instance("rejects extensions after a job completes", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const job = yield* jobs.start({ type: "test", run: Effect.succeed("done") })
      yield* jobs.wait({ id: job.id })

      expect(yield* jobs.extend({ id: job.id, run: Effect.succeed("late") })).toBe(false)
      expect((yield* jobs.get(job.id))?.output).toBe("done")
    }),
  )

  it.instance("records failed jobs", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const job = yield* jobs.start({
        type: "test",
        run: Effect.fail(new Error("boom")),
      })

      const result = yield* jobs.wait({ id: job.id })

      expect(result.info?.status).toBe("error")
      expect(result.info?.error).toBe("boom")
    }),
  )

  it.instance("ignores stale settlements after restarting a failed job", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const fail = yield* Deferred.make<void>()
      const interrupted = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const id = "job_test"
      yield* jobs.start({
        id,
        type: "test",
        run: Deferred.await(fail).pipe(Effect.andThen(Effect.fail(new Error("boom")))),
      })
      yield* jobs.extend({
        id,
        run: Effect.never.pipe(
          Effect.ensuring(Deferred.succeed(interrupted, undefined).pipe(Effect.andThen(Deferred.await(release)))),
        ),
      })

      yield* Deferred.succeed(fail, undefined)
      expect((yield* jobs.wait({ id })).info?.status).toBe("error")
      yield* Deferred.await(interrupted)
      yield* jobs.start({ id, type: "test", run: Effect.never })

      yield* Deferred.succeed(release, undefined)
      yield* Effect.yieldNow
      expect((yield* jobs.get(id))?.status).toBe("running")
      yield* jobs.cancel(id)
    }),
  )

  it.instance("can cancel running jobs", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const interrupted = yield* Deferred.make<void>()
      const job = yield* jobs.start({
        type: "test",
        run: Effect.never.pipe(Effect.ensuring(Deferred.succeed(interrupted, undefined))),
      })
      yield* jobs.extend({
        id: job.id,
        run: Effect.never,
      })

      const cancelled = yield* jobs.cancel(job.id)

      expect(cancelled?.status).toBe("cancelled")
      yield* Deferred.await(interrupted).pipe(Effect.timeout("1 second"))
      expect((yield* jobs.get(job.id))?.status).toBe("cancelled")
    }),
  )

  it.instance("promotes running jobs without interrupting them", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const latch = yield* Deferred.make<void>()
      const promoted = yield* Deferred.make<void>()
      const job = yield* jobs.start({
        type: "test",
        metadata: { parentSessionId: "parent" },
        onPromote: Deferred.succeed(promoted, undefined).pipe(Effect.asVoid),
        run: Deferred.await(latch).pipe(Effect.as("done")),
      })

      const info = yield* jobs.promote(job.id)

      expect(info?.status).toBe("running")
      expect(info?.metadata?.background).toBe(true)
      yield* Deferred.await(promoted)
      expect((yield* jobs.get(job.id))?.status).toBe("running")

      yield* Deferred.succeed(latch, undefined)
      expect((yield* jobs.wait({ id: job.id })).info?.output).toBe("done")
    }),
  )

  it.instance("evicts the oldest terminal jobs at the retention bound", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const jobs = yield* CoreBackgroundJob.makeWithRetention({ maxTerminalJobs: 3 })
        for (let index = 0; index < 6; index++) {
          const job = yield* jobs.start({
            id: `job_${index}`,
            type: "test",
            run: Effect.succeed(`done-${index}`),
          })
          yield* jobs.wait({ id: job.id })
        }

        const list = yield* jobs.list()
        expect(list).toHaveLength(3)
        expect(list.map((job) => job.id)).toEqual(["job_3", "job_4", "job_5"])
      }),
    ),
  )

  it.instance("never evicts a running job to stay under the retention bound", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const jobs = yield* CoreBackgroundJob.makeWithRetention({ maxTerminalJobs: 2 })
        const running = yield* jobs.start({ id: "job_running", type: "test", run: Effect.never })
        for (let index = 0; index < 5; index++) {
          const job = yield* jobs.start({
            id: `job_terminal_${index}`,
            type: "test",
            run: Effect.succeed(`done-${index}`),
          })
          yield* jobs.wait({ id: job.id })
        }

        const list = yield* jobs.list()
        expect(list.map((job) => job.id)).toContain(running.id)
        expect(list.filter((job) => job.status !== "running")).toHaveLength(2)
        yield* jobs.cancel(running.id)
      }),
    ),
  )

  it.instance("retains no terminal jobs when the bound is zero", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const jobs = yield* CoreBackgroundJob.makeWithRetention({ maxTerminalJobs: 0 })
        for (let index = 0; index < 3; index++) {
          const job = yield* jobs.start({
            id: `job_zero_${index}`,
            type: "test",
            run: Effect.succeed(`done-${index}`),
          })
          yield* jobs.wait({ id: job.id })
        }

        expect(yield* jobs.list()).toEqual([])
        expect(yield* jobs.get("job_zero_2")).toBeUndefined()
      }),
    ),
  )

  it.instance("keeps every terminal job when the bound is Infinity", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const jobs = yield* CoreBackgroundJob.makeWithRetention({ maxTerminalJobs: Infinity })
        for (let index = 0; index < 3; index++) {
          const job = yield* jobs.start({
            id: `job_unbounded_${index}`,
            type: "test",
            run: Effect.succeed(`done-${index}`),
          })
          yield* jobs.wait({ id: job.id })
        }

        expect((yield* jobs.list()).map((job) => job.id)).toEqual([
          "job_unbounded_0",
          "job_unbounded_1",
          "job_unbounded_2",
        ])
      }),
    ),
  )

  it.instance("rejects a start when the id is already running and the caller asks to fail busy", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const started = yield* Deferred.make<void>()
      const id = "job_busy"
      yield* jobs.start({
        id,
        type: "test",
        run: Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)),
      })
      yield* Deferred.await(started)

      const exit = yield* jobs
        .start({ id, type: "test", rejectIfRunning: true, run: Effect.succeed("duplicate") })
        .pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) {
        expect(Cause.squash(exit.cause)).toBeInstanceOf(CoreBackgroundJob.JobAlreadyRunningError)
      }
      yield* jobs.cancel(id)
    }),
  )

  it.instance("returns immutable snapshots", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const job = yield* jobs.start({
        type: "test",
        metadata: { value: "initial" },
        run: Effect.succeed("done"),
      })

      expect(job.metadata?.value).toBe("initial")
      // Metadata is deep-frozen and cloned at the registry boundary, so an
      // in-place mutation of a returned snapshot is rejected (strict mode)
      // rather than silently corrupting the registry's copy.
      expect(() => {
        if (job.metadata) job.metadata.value = "changed"
      }).toThrow()

      const read = yield* jobs.get(job.id)
      expect(read?.metadata?.value).toBe("initial")
      expect(() => {
        if (read?.metadata) read.metadata.value = "changed"
      }).toThrow()
      expect((yield* jobs.get(job.id))?.metadata?.value).toBe("initial")
    }),
  )
})
