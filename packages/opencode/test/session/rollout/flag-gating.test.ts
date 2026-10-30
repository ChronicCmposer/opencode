// The session-worker pool is behind `RuntimeFlags.experimentalSessionWorkers`
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
// This file is the canonical home of the delegation matrix; the
// `startShell`/`cancel` delegation wiring that is unique to the hub lives in
// `worker/hub-wiring.test.ts`.

import { describe, expect } from "bun:test"
import { ConfigProvider, Effect, Layer } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { BackgroundJob } from "@/background/job"
import { RuntimeFlags } from "@/effect/runtime-flags"
import type { SessionID } from "@/session/schema"
import { SessionRunState } from "@/session/run-state"
import { SessionStatus } from "@/session/status"
import { createSessionHub, SessionHubService } from "@/session/worker/hub"
import { createTestBarrier } from "../../lib/run-barrier-test"
import type {
  ExecutorRun,
  ExecutorSessionWorker,
  ExecutorWorker,
  ExecutorWorkerPool,
  SessionExecutionResult,
} from "@/session/worker/execute"
import { withTmpdirInstance } from "../../fixture/fixture"
import { it } from "../../lib/effect"
import { sid } from "../../lib/async"
import { withParts } from "../../lib/session-parts"

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
      Layer.mock(SessionStatus.Service, {
        set: () => Effect.void,
        get: () => Effect.succeed({ type: "idle" as const }),
      }),
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

/**
 * The canonical flag-on/flag-off delegation matrix. One row is one observable
 * outcome of `ensureRunning` when a hub (and its pool) is present:
 *   * `workRan` — did the in-hub work effect execute?
 *   * `dispatched` — which sessions the pool was asked to run.
 *   * `registrySize` — the hub's key→worker map after the call.
 * An absent `flags` row proves the flag-off default with no flag service at all.
 */
interface DelegationCase {
  readonly name: string
  readonly sessionID: SessionID
  readonly flags?: Partial<RuntimeFlags.Info>
  readonly workRan: boolean
  readonly dispatched: SessionID[]
  readonly registrySize: number
}

const delegationMatrix: readonly DelegationCase[] = [
  {
    name: "flag OFF: the pool is never dispatched; the in-hub runner runs the work",
    sessionID: sid("ses_off"),
    workRan: true,
    dispatched: [],
    registrySize: 0,
  },
  {
    name: "flag ON: the pool is dispatched; the in-hub work effect never runs",
    sessionID: sid("ses_on"),
    flags: { experimentalSessionWorkers: true },
    workRan: false,
    dispatched: [sid("ses_on")],
    registrySize: 0,
  },
]

describe("14.1 the pool is gated by the flag (a present hub is ignored when OFF)", () => {
  for (const testCase of delegationMatrix) {
    it.live(testCase.name, () => {
      const pool = new SpyPool(1, withParts(testCase.sessionID))
      const hub = createSessionHub({ ...{ barrier: createTestBarrier() }, pool })
      const run = Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        let workRan = false
        const result = yield* state.ensureRunning(
          testCase.sessionID,
          Effect.succeed(withParts(testCase.sessionID)),
          Effect.sync(() => {
            workRan = true
            return withParts(testCase.sessionID)
          }),
        )

        expect(result).toEqual(withParts(testCase.sessionID))
        expect(workRan).toBe(testCase.workRan)
        expect(pool.dispatched).toEqual(testCase.dispatched)
        expect(hub.dispatcher.registry.size).toBe(testCase.registrySize)
      }).pipe(Effect.provide(runStateLayer), Effect.provideService(SessionHubService, hub))

      // `flags` absent leaves RuntimeFlags unprovided ⇒ the flag defaults OFF ⇒
      // the in-hub runner is used; otherwise the explicit flag-on layer is added.
      return withTmpdirInstance()(
        testCase.flags === undefined ? run : run.pipe(Effect.provide(RuntimeFlags.layer(testCase.flags))),
      )
    })
  }
})
