// Phase 12 — flag-on wiring: `run-state.ts` delegates to the hub/executor, and the
// hub routes UI stop through the Phase 11 cooperative `cancelWithGrace` path.
//
// Flag off (or no `SessionHubService`) must remain the in-hub `RunnerTable` path;
// flag on delegates `ensureRunning`/`startShell`/`assertNotBusy`/`cancel` to the
// hub. The hub here runs over an in-memory executor pool so the wiring is tested
// deterministically without spawning isolates.

import { describe, expect } from "bun:test"
import { Effect, Layer } from "effect"
import type { SessionID } from "@/session/schema"
import type { SessionV1 } from "@opencode-ai/core/v1/session"
import { BackgroundJob } from "@/background/job"
import { SessionStatus } from "@/session/status"
import { SessionRunState } from "@/session/run-state"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { createSessionHub, SessionHubService, type SessionHub } from "@/session/worker/hub"
import type {
  ExecutorRun,
  ExecutorSessionWorker,
  ExecutorWorker,
  ExecutorWorkerPool,
  SessionExecutionResult,
} from "@/session/worker/execute"
import { withTmpdirInstance } from "../../fixture/fixture"
import { testEffect } from "../../lib/effect"

const it = testEffect(Layer.empty)
const sid = (value: string) => value as SessionID

/** A minimal valid `SessionV1.WithParts` (the worker result payload). */
const withParts = (sessionID: SessionID): SessionV1.WithParts =>
  ({
    info: {
      id: "msg_phase12",
      sessionID,
      role: "user",
      time: { created: 0 },
      agent: "build",
      model: { providerID: "test", modelID: "test" },
    },
    parts: [],
  }) as unknown as SessionV1.WithParts

class FakeWorker implements ExecutorWorker {
  busy = false
  constructor(
    readonly id: number,
    private readonly onExecute: (run: ExecutorRun) => void,
  ) {}
  get alive(): boolean {
    return true
  }
  async execute(run: ExecutorRun): Promise<void> {
    this.onExecute(run)
  }
  async cancel(): Promise<void> {}
  async assertNotBusy(): Promise<boolean> {
    return this.busy
  }
  resultFor(): SessionExecutionResult | undefined {
    return undefined
  }
  async terminate(): Promise<void> {}
}

class FakePool implements ExecutorWorkerPool {
  readonly workers: FakeWorker[]
  readonly spawned: FakeWorker[] = []
  private readonly results = new Map<SessionID, SessionExecutionResult>()
  private nextSpawnId = 100

  constructor(
    k: number,
    private readonly value: unknown,
  ) {
    this.workers = Array.from({ length: k }, (_, index) => this.make(index + 1))
  }

  private make(id: number): FakeWorker {
    return new FakeWorker(id, (run) => {
      this.results.set(run.sessionID, { sessionID: run.sessionID, value: this.value })
    })
  }

  spawn(): ExecutorSessionWorker {
    const worker = this.make(this.nextSpawnId++)
    this.spawned.push(worker)
    return worker
  }

  async terminateAll(): Promise<void> {}

  byID(id: number): ExecutorWorker | undefined {
    return [...this.workers, ...this.spawned].find((worker) => worker.id === id)
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
const flagsOn = RuntimeFlags.layer({ experimentalSessionWorkers: true })

describe("12 — flag-on run-state delegation", () => {
  it.live("ensureRunning delegates to the hub; the hub-authored work effect never runs", () => {
    const pool = new FakePool(1, withParts(sid("ses_1")))
    const hub = createSessionHub({ pool })
    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        let workRan = false
        const result = yield* state.ensureRunning(
          sid("ses_1"),
          Effect.succeed(withParts(sid("ses_1"))),
          Effect.sync(() => {
            workRan = true
            return withParts(sid("ses_1"))
          }),
        )
        expect(result).toEqual(withParts(sid("ses_1")))
        expect(workRan).toBe(false)
      }).pipe(
        Effect.provide(runStateLayer),
        Effect.provideService(SessionHubService, hub),
        Effect.provide(flagsOn),
      ),
    )
  })

  it.live("cancel delegates to the hub cancel path", () => {
    const pool = new FakePool(1, undefined)
    const base = createSessionHub({ pool })
    const cancelled: SessionID[] = []
    const hub: SessionHub = {
      ...base,
      cancel: async (sessionID) => {
        cancelled.push(sessionID)
        return base.cancel(sessionID)
      },
    }
    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        yield* state.cancel(sid("ses_none"))
        expect(cancelled).toEqual([sid("ses_none")])
      }).pipe(
        Effect.provide(runStateLayer),
        Effect.provideService(SessionHubService, hub),
        Effect.provide(flagsOn),
      ),
    )
  })

  it.live("flag off keeps the in-hub runner path (no hub service provided)", () => {
    const pool = new FakePool(1, withParts(sid("ses_1")))
    const hub = createSessionHub({ pool })
    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        let workRan = false
        const result = yield* state.ensureRunning(
          sid("ses_1"),
          Effect.succeed(withParts(sid("ses_1"))),
          Effect.sync(() => {
            workRan = true
            return withParts(sid("ses_1"))
          }),
        )
        expect(result).toEqual(withParts(sid("ses_1")))
        expect(workRan).toBe(true)
        expect(hub.dispatcher.registry.size).toBe(0)
      }).pipe(
        Effect.provide(runStateLayer),
        Effect.provideService(SessionHubService, hub),
        // RuntimeFlags absent → flag treated as off → in-hub runner used.
      ),
    )
  })
})
