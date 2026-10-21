// Phase 12 — flag-on hub wiring: `run-state.ts` forwards `startShell` and
// `cancel` to the hub, and the hub routes UI stop through the Phase 11
// cooperative `cancelWithGrace` path.
//
// The flag-on/flag-off `ensureRunning` delegation matrix lives in
// `rollout/flag-gating.test.ts` (finding #186). What is unique here is the
// control wiring: `startShell` opens the caller's `ready` latch (the hub cannot
// carry it), maps a hub busy rejection to `Session.BusyError`, and `cancel`
// reaches the hub. The hub runs over an in-memory executor pool so the wiring is
// tested deterministically without spawning isolates.

import { describe, expect } from "bun:test"
import { Effect, Latch, Layer } from "effect"
import type { SessionID } from "@/session/schema"
import { BackgroundJob } from "@/background/job"
import { Session } from "@/session/session"
import { SessionStatus } from "@/session/status"
import { SessionRunState } from "@/session/run-state"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { createSessionHub, SessionHubService, type SessionHub } from "@/session/worker/hub"
import {
  SessionBusyError,
  type ExecutorRun,
  type ExecutorSessionWorker,
  type ExecutorWorker,
  type ExecutorWorkerPool,
  type SessionExecutionResult,
} from "@/session/worker/execute"
import { withTmpdirInstance } from "../../fixture/fixture"
import { testEffect } from "../../lib/effect"
import { sid } from "../../lib/async"
import { withParts } from "../../lib/session-parts"

const it = testEffect(Layer.empty)

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

describe("12 — flag-on hub wiring (startShell / cancel delegation)", () => {
  it.live("startShell calls the hub's startShell and opens the caller's ready latch", () => {
    const pool = new FakePool(1, withParts(sid("ses_shell")))
    const base = createSessionHub({ pool })
    let shellCalls = 0
    const hub: SessionHub = {
      ...base,
      startShell: async (sessionID) => {
        shellCalls += 1
        return base.startShell(sessionID)
      },
    }
    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        const ready = yield* Latch.make()
        const result = yield* state.startShell(
          sid("ses_shell"),
          Effect.succeed(withParts(sid("ses_shell"))),
          Effect.succeed(withParts(sid("ses_shell"))),
          ready,
        )
        expect(result).toEqual(withParts(sid("ses_shell")))
        expect(shellCalls).toBe(1)
        // The hub cannot carry the caller's latch, so run-state must open it:
        // awaiting would suspend forever (and time the test out) otherwise.
        yield* ready.await
      }).pipe(
        Effect.provide(runStateLayer),
        Effect.provideService(SessionHubService, hub),
        Effect.provide(flagsOn),
      ),
    )
  })

  it.live("startShell maps a hub busy rejection to Session.BusyError", () => {
    const pool = new FakePool(1, undefined)
    const base = createSessionHub({ pool })
    const hub: SessionHub = {
      ...base,
      startShell: async (sessionID) => {
        throw new SessionBusyError({ sessionID })
      },
    }
    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        const error = yield* state
          .startShell(
            sid("ses_busy"),
            Effect.succeed(withParts(sid("ses_busy"))),
            Effect.succeed(withParts(sid("ses_busy"))),
          )
          .pipe(Effect.flip)
        expect(error).toBeInstanceOf(Session.BusyError)
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
})
