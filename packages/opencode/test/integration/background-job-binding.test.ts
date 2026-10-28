// Worker job mirrors are session-bound.
//
// A session worker may register, settle, and await promotion only for the jobs
// it owns. A foreign job id is invisible: it cannot be cancelled, settled, or
// observed through another worker's authority. A worker-supplied `parentID` is
// rejected unless it names the worker's own bound session.

import { describe, expect, test } from "bun:test"
import { Context, Effect, Exit, Layer, Scope } from "effect"
import { BackgroundJob as CoreBackgroundJob } from "@opencode-ai/core/background-job"
import { createWorkerJobMirror } from "@/session/worker/hub"
import { pollWithTimeout } from "../lib/effect"
import { sid } from "../lib/async"

async function buildRegistry() {
  const scope = Effect.runSync(Scope.make())
  const layer = Layer.effect(CoreBackgroundJob.Service, CoreBackgroundJob.makeWithRetention({ maxTerminalJobs: 8 }))
  const context = Effect.runSync(Layer.buildWithScope(layer, scope))
  const registry = Context.get(context, CoreBackgroundJob.Service)
  const run = <A, E>(effect: Effect.Effect<A, E>) => Effect.runPromise(Effect.provide(effect, context))
  return {
    registry,
    run,
    close: () => Effect.runPromise(Scope.close(scope, Exit.void)),
  }
}

function running(id: string, parentID?: string): CoreBackgroundJob.Info {
  return {
    id,
    type: "task",
    status: "running",
    started_at: 0,
    ...(parentID === undefined ? {} : { parentID }),
  }
}

function waitUntil(predicate: Effect.Effect<boolean | undefined>, message: string): Promise<boolean> {
  return Effect.runPromise(pollWithTimeout(predicate, message))
}

describe("worker job mirror session binding", () => {
  test("a bound session cannot settle or observe another session's job", async () => {
    const registry = await buildRegistry()
    const mirror = createWorkerJobMirror()
    const own = mirror.authorityFor(registry.run, sid("ses_own"))
    const foreign = mirror.authorityFor(registry.run, sid("ses_foreign"))

    try {
      await own.register(running("job_own", "ses_own"))
      await foreign.register(running("job_foreign", "ses_foreign"))

      // Settling a foreign id is a no-op: the peer's job keeps running.
      await own.settle({ id: "job_foreign", status: "cancelled" })
      expect((await registry.run(registry.registry.get("job_foreign")))?.status).toBe("running")

      // Neither a foreign nor an unknown id is observable.
      expect(await own.awaitPromotion("job_foreign")).toBe(false)
      expect(await own.awaitPromotion("job_missing")).toBe(false)

      // The worker can still observe and settle its own job.
      const promoted = own.awaitPromotion("job_own")
      await registry.run(registry.registry.promote("job_own"))
      expect(await promoted).toBe(true)

      await own.settle({ id: "job_own", status: "completed", output: "done" })
      expect((await registry.run(registry.registry.get("job_own")))?.status).toBe("completed")

      // The foreign job was never settled by the other worker.
      expect((await registry.run(registry.registry.get("job_foreign")))?.status).toBe("running")
    } finally {
      await registry.close()
    }
  })

  test("a foreign parentID is rejected and the bound session owns the mirror row", async () => {
    const registry = await buildRegistry()
    const mirror = createWorkerJobMirror()
    const own = mirror.authorityFor(registry.run, sid("ses_own"))

    try {
      await expect(own.register(running("job_spoofed", "ses_foreign"))).rejects.toThrow(
        /may not register job job_spoofed under ses_foreign/,
      )
      expect(await registry.run(registry.registry.get("job_spoofed"))).toBeUndefined()

      // A worker that omits the parent still mirrors under its bound session.
      await own.register(running("job_unparented"))
      expect((await registry.run(registry.registry.get("job_unparented")))?.parentID).toBe("ses_own")
    } finally {
      await registry.close()
    }
  })

  test("releasing a session prunes only that session's mirrored jobs", async () => {
    const registry = await buildRegistry()
    const mirror = createWorkerJobMirror()
    const own = mirror.authorityFor(registry.run, sid("ses_own"))
    const foreign = mirror.authorityFor(registry.run, sid("ses_foreign"))

    try {
      await own.register(running("job_own", "ses_own"))
      await foreign.register(running("job_foreign", "ses_foreign"))

      mirror.releaseSession("ses_foreign")
      await waitUntil(
        Effect.map(registry.registry.get("job_foreign"), (job) =>
          job === undefined || job.status !== "running" ? true : undefined,
        ),
        "releasing the foreign session left its mirrored job running",
      )
      expect((await registry.run(registry.registry.get("job_own")))?.status).toBe("running")

      mirror.releaseSession("ses_own")
      await waitUntil(
        Effect.map(registry.registry.get("job_own"), (job) =>
          job === undefined || job.status !== "running" ? true : undefined,
        ),
        "releasing the owning session left its mirrored job running",
      )
    } finally {
      await registry.close()
    }
  })
})
