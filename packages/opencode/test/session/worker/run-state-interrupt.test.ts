// A caller-side interruption (HTTP abort/TUI cancel) must reach the hub: the
// rejected promise alone cannot stop the worker's run. The hub is asked to
// cancel the session and the caller's interrupt finalizer still runs.

import { describe, expect } from "bun:test"
import { Effect, Fiber, Layer } from "effect"
import { BackgroundJob } from "@/background/job"
import { SessionRunState } from "@/session/run-state"
import { SessionStatus } from "@/session/status"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { createSessionHub, SessionHubService, type SessionHub } from "@/session/worker/hub"
import { createTestBarrier } from "../../lib/run-barrier-test"
import { createFakeExecutorPool } from "../../lib/fake-worker-pool"
import { withTmpdirInstance } from "../../fixture/fixture"
import { testEffect } from "../../lib/effect"
import { sid } from "../../lib/async"
import { withParts } from "../../lib/session-parts"

const it = testEffect(Layer.empty)
const flagsOn = RuntimeFlags.layer({ experimentalSessionWorkers: true })
const runStateLayer = SessionRunState.layer.pipe(
  Layer.provide(
    Layer.mergeAll(
      Layer.mock(BackgroundJob.Service, { list: () => Effect.succeed([]) }),
      Layer.mock(SessionStatus.Service, {
        set: () => Effect.void,
        get: () => Effect.succeed({ type: "idle" }),
      }),
    ),
  ),
)

describe("hub-hosted run interruption", () => {
  it.live("interrupting the caller cancels through the hub and runs the caller finalizer", () => {
    const sessionID = sid("ses_run_interrupt")
    const base = createSessionHub({ ...{ barrier: createTestBarrier() }, pool: createFakeExecutorPool(1, { value: withParts(sessionID) }) })
    let cancelCalls = 0
    let started!: () => void
    const startedPromise = new Promise<void>((resolve) => {
      started = resolve
    })
    const hub: SessionHub = {
      ...base,
      ensureRunningEffect: () =>
        // The worker run never settles; only the caller's interruption ends it.
        Effect.sync(started).pipe(Effect.andThen(Effect.never)),
      cancelWithReport: async (id) => {
        cancelCalls += 1
        return {
          sessionID: id,
          outcome: { sessionID: id, mode: "terminated", winner: "cancel", acknowledged: false, durationMs: 0 },
          descendants: [],
        }
      },
    }
    let interrupted = false

    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        const fiber = yield* state
          .ensureRunning(
            sessionID,
            Effect.sync(() => {
              interrupted = true
              return withParts(sessionID)
            }),
            Effect.succeed(withParts(sessionID)),
          )
          .pipe(Effect.forkChild)
        yield* Effect.promise(() => startedPromise)
        yield* Fiber.interrupt(fiber)

        expect(cancelCalls).toBe(1)
        expect(interrupted).toBe(true)
      }).pipe(
        Effect.provide(runStateLayer),
        Effect.provideService(SessionHubService, hub),
        Effect.provide(flagsOn),
      ),
    )
  })
})
