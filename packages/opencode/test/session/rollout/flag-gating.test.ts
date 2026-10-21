// Phase 14.1 — the session-worker pool is behind `RuntimeFlags.experimentalSessionWorkers`
// (`OPENCODE_EXPERIMENTAL_SESSION_WORKERS`), defaulting OFF.
//
// Two levels of proof:
//   * CONFIG — the flag resolves `false` from an empty config, `true` from the
//     dedicated env var, and inherits the `OPENCODE_EXPERIMENTAL` umbrella unless
//     the dedicated flag overrides it.
//   * GATING — with a hub (and its pool) present, the flag OFF leaves the pool
//     completely untouched and the in-hub runner executes the work; the flag ON
//     dispatches to the pool and the in-hub work effect never runs.
//
// The pool here is a spy so "untouched" is an observable fact, not an assumption.

import { describe, expect } from "bun:test"
import { ConfigProvider, Effect, Layer } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import type { SessionV1 } from "@opencode-ai/core/v1/session"
import { BackgroundJob } from "@/background/job"
import { RuntimeFlags } from "@/effect/runtime-flags"
import type { SessionID } from "@/session/schema"
import { SessionRunState } from "@/session/run-state"
import { SessionStatus } from "@/session/status"
import { createSessionHub, SessionHubService } from "@/session/worker/hub"
import type {
  ExecutorRun,
  ExecutorSessionWorker,
  ExecutorWorker,
  ExecutorWorkerPool,
  SessionExecutionResult,
} from "@/session/worker/execute"
import { withTmpdirInstance } from "../../fixture/fixture"
import { it } from "../../lib/effect"

const sid = (value: string) => value as SessionID

/** A minimal valid `SessionV1.WithParts` (the worker result payload). */
const withParts = (sessionID: SessionID): SessionV1.WithParts =>
  ({
    info: {
      id: "msg_phase14",
      sessionID,
      role: "user",
      time: { created: 0 },
      agent: "build",
      model: { providerID: "test", modelID: "test" },
    },
    parts: [],
  }) as unknown as SessionV1.WithParts

/** A pool that records every dispatch so "the pool ran" is directly observable. */
class SpyPool implements ExecutorWorkerPool {
  readonly dispatched: SessionID[] = []
  readonly workers: ExecutorSessionWorker[]
  private readonly all: ExecutorWorker[]
  private readonly results = new Map<SessionID, SessionExecutionResult>()
  private nextSpawnId = 100

  constructor(k: number, private readonly value: unknown) {
    this.all = Array.from({ length: k }, (_, index) => this.make(index + 1))
    this.workers = this.all
  }

  private make(id: number): ExecutorWorker {
    return {
      id,
      alive: true,
      execute: async (run: ExecutorRun) => {
        this.dispatched.push(run.sessionID)
        this.results.set(run.sessionID, { sessionID: run.sessionID, value: this.value })
      },
      cancel: async () => {},
      assertNotBusy: async () => false,
      resultFor: (sessionID) => this.results.get(sessionID),
      terminate: async () => {},
    }
  }

  spawn(): ExecutorSessionWorker {
    return this.make(this.nextSpawnId++)
  }
  async terminateAll(): Promise<void> {}
  byID(id: number): ExecutorWorker | undefined {
    return this.all.find((worker) => worker.id === id)
  }
  resultFor(sessionID: SessionID): SessionExecutionResult | undefined {
    return this.results.get(sessionID)
  }
}

const runStateLayer = SessionRunState.layer.pipe(
  Layer.provide(
    Layer.mergeAll(
      Layer.mock(BackgroundJob.Service, {}),
      Layer.mock(SessionStatus.Service, { set: () => Effect.void }),
    ),
  ),
)

const fromConfig = (input: Record<string, unknown>) =>
  AppNodeBuilder.build(RuntimeFlags.node).pipe(Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown(input))))

const readFlags = RuntimeFlags.Service.useSync((flags) => flags)

describe("14.1 RuntimeFlags.experimentalSessionWorkers defaults OFF", () => {
  it.effect("is false with an empty config", () =>
    Effect.gen(function* () {
      const flags = yield* readFlags.pipe(Effect.provide(fromConfig({})))
      expect(flags.experimentalSessionWorkers).toBe(false)
    }),
  )

  it.effect("is true via OPENCODE_EXPERIMENTAL_SESSION_WORKERS", () =>
    Effect.gen(function* () {
      const flags = yield* readFlags.pipe(Effect.provide(fromConfig({ OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "true" })))
      expect(flags.experimentalSessionWorkers).toBe(true)
    }),
  )

  it.effect("inherits the OPENCODE_EXPERIMENTAL umbrella", () =>
    Effect.gen(function* () {
      const flags = yield* readFlags.pipe(Effect.provide(fromConfig({ OPENCODE_EXPERIMENTAL: "true" })))
      expect(flags.experimentalSessionWorkers).toBe(true)
    }),
  )

  it.effect("the dedicated flag overrides the umbrella", () =>
    Effect.gen(function* () {
      const flags = yield* readFlags.pipe(
        Effect.provide(
          fromConfig({ OPENCODE_EXPERIMENTAL: "true", OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "false" }),
        ),
      )
      expect(flags.experimentalSessionWorkers).toBe(false)
    }),
  )
})

describe("14.1 the pool is gated by the flag (a present hub is ignored when OFF)", () => {
  it.live("flag OFF: the pool is never dispatched; the in-hub runner runs the work", () => {
    const pool = new SpyPool(1, withParts(sid("ses_off")))
    const hub = createSessionHub({ pool })
    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        let workRan = false
        const result = yield* state.ensureRunning(
          sid("ses_off"),
          Effect.succeed(withParts(sid("ses_off"))),
          Effect.sync(() => {
            workRan = true
            return withParts(sid("ses_off"))
          }),
        )

        expect(result).toEqual(withParts(sid("ses_off")))
        expect(workRan).toBe(true)
        expect(pool.dispatched).toEqual([])
        expect(hub.dispatcher.registry.size).toBe(0)
      }).pipe(
        Effect.provide(runStateLayer),
        Effect.provideService(SessionHubService, hub),
        // RuntimeFlags absent ⇒ the flag is OFF ⇒ the in-hub runner is used.
      ),
    )
  })

  it.live("flag ON: the pool is dispatched; the in-hub work effect never runs", () => {
    const pool = new SpyPool(1, withParts(sid("ses_on")))
    const hub = createSessionHub({ pool })
    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        let workRan = false
        const result = yield* state.ensureRunning(
          sid("ses_on"),
          Effect.succeed(withParts(sid("ses_on"))),
          Effect.sync(() => {
            workRan = true
            return withParts(sid("ses_on"))
          }),
        )

        expect(result).toEqual(withParts(sid("ses_on")))
        expect(workRan).toBe(false)
        expect(pool.dispatched).toEqual([sid("ses_on")])
      }).pipe(
        Effect.provide(runStateLayer),
        Effect.provideService(SessionHubService, hub),
        Effect.provide(RuntimeFlags.layer({ experimentalSessionWorkers: true })),
      ),
    )
  })
})
