// p1-entry — the `SessionRunState.startShell` flag seam.
//
// Frozen contract: `startShell` takes a trailing optional `shell` payload. Flag
// ON (hub present + `experimentalSessionWorkers`) the shell rides the hub
// dispatch (`hub.startShell(sessionID, shell)`) and the caller's local `work`
// never runs (its closures cannot cross the isolate). Flag OFF the local `work`
// runs in-process. These tests pin the forwarding and the branch split that the
// end-to-end shell tests in `prompt.test.ts` do not observe directly.

import { describe, expect } from "bun:test"
import { Cause, Effect, Exit, Latch, Layer } from "effect"
import { BackgroundJob } from "@/background/job"
import type { SessionID } from "@/session/schema"
import { Session } from "@/session/session"
import { SessionRunState } from "@/session/run-state"
import type { DispatchOrigin } from "@/session/origin"
import { SessionStatus } from "@/session/status"
import { SessionCancelledError, WorkerCrashedError } from "@/session/worker/dispatch"
import type { ExecutorWorker, ExecutorWorkerPool } from "@/session/worker/execute"
import type { ShellInput } from "@/session/prompt"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { createSessionHub, SessionHubService, type SessionHub } from "@/session/worker/hub"
import { withTmpdirInstance } from "../fixture/fixture"
import { pollWithTimeout, testEffect } from "../lib/effect"
import { sid } from "../lib/async"
import { withParts } from "../lib/session-parts"
import { createFakeExecutorPool } from "../lib/fake-worker-pool"

const it = testEffect(Layer.empty)

const flagsOn = RuntimeFlags.layer({ experimentalSessionWorkers: true })

/** `SessionRunState.layer` with the two ambient services its branches touch. */
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

/** A hub that records the `startShell` arguments it receives, then delegates. */
const recordingShellHub = (sessionID: SessionID, forwarded: Array<{ sessionID: SessionID; shell?: ShellInput }>) => {
  const base = createSessionHub({ pool: createFakeExecutorPool(1, { value: withParts(sessionID) }) })
  return {
    ...base,
    startShell: async (id: SessionID, shell?: ShellInput) => {
      forwarded.push({ sessionID: id, shell })
      return base.startShell(id, shell)
    },
  } satisfies SessionHub
}

describe("SessionRunState.startShell — flag seam", () => {
  it.live("flag ON forwards the shell payload to the hub and skips the local work", () => {
    const sessionID = sid("ses_shell_on")
    const shell: ShellInput = { sessionID, agent: "build", command: "echo on" }
    const forwarded: Array<{ sessionID: SessionID; shell?: ShellInput }> = []
    const hub = recordingShellHub(sessionID, forwarded)
    let localWorkRan = false

    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        const ready = yield* Latch.make()
        const result = yield* state.startShell(
          sessionID,
          Effect.succeed(withParts(sessionID)),
          Effect.sync(() => {
            localWorkRan = true
            return withParts(sessionID)
          }),
          ready,
          shell,
        )

        expect(result).toEqual(withParts(sessionID))
        // The command rides the hub dispatch; the local work is never invoked.
        expect(forwarded).toEqual([{ sessionID, shell }])
        expect(localWorkRan).toBe(false)
        // The hub cannot carry the caller's latch, so run-state opens it.
        yield* ready.await
      }).pipe(
        Effect.provide(runStateLayer),
        Effect.provideService(SessionHubService, hub),
        Effect.provide(flagsOn),
      ),
    )
  })

  it.live("flag ON accepts an absent shell (optional trailing argument)", () => {
    const sessionID = sid("ses_shell_on_absent")
    const forwarded: Array<{ sessionID: SessionID; shell?: ShellInput }> = []
    const hub = recordingShellHub(sessionID, forwarded)

    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        const ready = yield* Latch.make()
        const result = yield* state.startShell(
          sessionID,
          Effect.succeed(withParts(sessionID)),
          Effect.succeed(withParts(sessionID)),
          ready,
        )

        expect(result).toEqual(withParts(sessionID))
        expect(forwarded).toEqual([{ sessionID, shell: undefined }])
      }).pipe(
        Effect.provide(runStateLayer),
        Effect.provideService(SessionHubService, hub),
        Effect.provide(flagsOn),
      ),
    )
  })

  it.live("flag OFF runs the local work in-process", () => {
    const sessionID = sid("ses_shell_off")
    const shell: ShellInput = { sessionID, agent: "build", command: "echo off" }
    let localWorkRan = false

    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        const ready = yield* Latch.make()
        const result = yield* state.startShell(
          sessionID,
          Effect.succeed(withParts(sessionID)),
          Effect.sync(() => {
            localWorkRan = true
            return withParts(sessionID)
          }),
          ready,
          shell,
        )

        // No hub is provided, so the in-process runner executes `work` itself.
        expect(result).toEqual(withParts(sessionID))
        expect(localWorkRan).toBe(true)
      }).pipe(Effect.provide(runStateLayer)),
    )
  })

  it.live("flag OFF ignores the shell payload when no hub hosts the run", () => {
    const sessionID = sid("ses_shell_off_absent")
    let localWorkRan = false

    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        const result = yield* state.startShell(
          sessionID,
          Effect.succeed(withParts(sessionID)),
          Effect.sync(() => {
            localWorkRan = true
            return withParts(sessionID)
          }),
        )

        expect(result).toEqual(withParts(sessionID))
        expect(localWorkRan).toBe(true)
      }).pipe(Effect.provide(runStateLayer)),
    )
  })
})

/** A hub that records the `ensureRunning` origin it receives, then delegates. */
const recordingEnsureHub = (
  sessionID: SessionID,
  forwarded: Array<{ sessionID: SessionID; origin?: DispatchOrigin }>,
) => {
  const base = createSessionHub({ pool: createFakeExecutorPool(1, { value: withParts(sessionID) }) })
  return {
    ...base,
    ensureRunning: async (id: SessionID, origin?: DispatchOrigin) => {
      forwarded.push({ sessionID: id, origin })
      return base.ensureRunning(id, origin)
    },
  } satisfies SessionHub
}

describe("SessionRunState.ensureRunning — dispatch origin seam", () => {
  it.live("flag ON forwards the origin to the hub (absent stays undefined = user)", () => {
    const sessionID = sid("ses_origin_on")
    const forwarded: Array<{ sessionID: SessionID; origin?: DispatchOrigin }> = []
    const hub = recordingEnsureHub(sessionID, forwarded)
    const work = Effect.succeed(withParts(sessionID))

    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        yield* state.ensureRunning(sessionID, work, work, "auto")
        yield* state.ensureRunning(sessionID, work, work)

        expect(forwarded).toEqual([
          { sessionID, origin: "auto" },
          { sessionID, origin: undefined },
        ])
      }).pipe(
        Effect.provide(runStateLayer),
        Effect.provideService(SessionHubService, hub),
        Effect.provide(flagsOn),
      ),
    )
  })
})

// ---------------------------------------------------------------------------
// Cancel/crash typing: a hub rejection is a typed failure (or typed cancel
// outcome), never an unhandled defect. `Effect.promise` used to turn every
// rejection into a die, erasing the cancel/crash distinction callers branch on.
// ---------------------------------------------------------------------------

describe("SessionRunState — hub rejections are typed", () => {
  const failingHub = (overrides: Partial<SessionHub>): SessionHub => {
    const sessionID = sid("ses_typed")
    const base = createSessionHub({ pool: createFakeExecutorPool(1, { value: withParts(sessionID) }) })
    return { ...base, ...overrides } satisfies SessionHub
  }

  it.live("ensureRunning surfaces a hub cancel as a typed failure, not a defect", () => {
    const sessionID = sid("ses_cancel_typed")
    const hub = failingHub({
      ensureRunning: async () => {
        throw new SessionCancelledError({ message: "cancelled", sessionID })
      },
    })

    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        const exit = yield* state
          .ensureRunning(sessionID, Effect.succeed(withParts(sessionID)), Effect.succeed(withParts(sessionID)))
          .pipe(Effect.exit)

        expect(Exit.isFailure(exit)).toBe(true)
        if (!Exit.isFailure(exit)) return
        expect(Cause.hasDies(exit.cause)).toBe(false)
        expect(Cause.squash(exit.cause)).toBeInstanceOf(SessionCancelledError)
      }).pipe(Effect.provide(runStateLayer), Effect.provideService(SessionHubService, hub), Effect.provide(flagsOn)),
    )
  })

  it.live("ensureRunning surfaces a hub crash as a typed failure, not a defect", () => {
    const sessionID = sid("ses_crash_typed")
    const hub = failingHub({
      ensureRunning: async () => {
        throw new WorkerCrashedError({ message: "worker crashed", workerID: 7, sessionID })
      },
    })

    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        const exit = yield* state
          .ensureRunning(sessionID, Effect.succeed(withParts(sessionID)), Effect.succeed(withParts(sessionID)))
          .pipe(Effect.exit)

        expect(Exit.isFailure(exit)).toBe(true)
        if (!Exit.isFailure(exit)) return
        expect(Cause.hasDies(exit.cause)).toBe(false)
        expect(Cause.squash(exit.cause)).toBeInstanceOf(WorkerCrashedError)
      }).pipe(Effect.provide(runStateLayer), Effect.provideService(SessionHubService, hub), Effect.provide(flagsOn)),
    )
  })

  it.live("startShell maps a hub crash to a typed failure instead of dying", () => {
    const sessionID = sid("ses_shell_crash_typed")
    const hub = failingHub({
      startShell: async () => {
        throw new WorkerCrashedError({ message: "worker crashed", workerID: 9, sessionID })
      },
    })

    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        const exit = yield* state
          .startShell(
            sessionID,
            Effect.succeed(withParts(sessionID)),
            Effect.succeed(withParts(sessionID)),
            undefined,
            { sessionID, agent: "build", command: "echo typed" },
          )
          .pipe(Effect.exit)

        expect(Exit.isFailure(exit)).toBe(true)
        if (!Exit.isFailure(exit)) return
        expect(Cause.hasDies(exit.cause)).toBe(false)
        expect(Cause.squash(exit.cause)).toBeInstanceOf(WorkerCrashedError)
      }).pipe(Effect.provide(runStateLayer), Effect.provideService(SessionHubService, hub), Effect.provide(flagsOn)),
    )
  })

  it.live("cancel returns a typed outcome when the hub fails to report", () => {
    const sessionID = sid("ses_cancel_unreported")
    const hub = failingHub({
      cancelWithReport: async () => {
        throw new WorkerCrashedError({ message: "worker crashed while cancelling", workerID: 3, sessionID })
      },
    })

    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        const outcome = yield* state.cancel(sessionID)

        expect(outcome).toMatchObject({
          sessionID,
          mode: "terminated",
          winner: "cancel",
          acknowledged: false,
        })
        // The fallback idle is written by the run-state service's own
        // SessionStatus dependency (mocked above); the typed outcome is the
        // contract here.
      }).pipe(Effect.provide(runStateLayer), Effect.provideService(SessionHubService, hub), Effect.provide(flagsOn)),
    )
  })

  it.live("revert gate: assertNotBusy fails closed when the hub probe rejects", () => {
    const sessionID = sid("ses_probe_failed")
    const hub = failingHub({
      assertNotBusy: async () => {
        throw new Error("worker did not answer the busy probe")
      },
    })

    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        // `SessionRevert.revert`/`unrevert` call this before mutating anything. A
        // probe failure is not proof of idleness, so it must surface as BusyError.
        const exit = yield* state.assertNotBusy(sessionID).pipe(Effect.exit)

        expect(Exit.isFailure(exit)).toBe(true)
        if (!Exit.isFailure(exit)) return
        expect(Cause.hasDies(exit.cause)).toBe(false)
        expect(Cause.squash(exit.cause)).toBeInstanceOf(Session.BusyError)
      }).pipe(Effect.provide(runStateLayer), Effect.provideService(SessionHubService, hub), Effect.provide(flagsOn)),
    )
  })

  it.live("assertNotBusy treats a proven-dead running worker as idle", () => {
    const sessionID = sid("ses_dead_worker_idle")
    // Blocking keeps the worker owned by an in-flight run, so the registry does
    // not short-circuit this as an already-idle entry.
    const pool = createFakeExecutorPool(1, { mode: "blocking", firstSpawnId: 1 })
    let dead: ExecutorWorker | undefined
    const guardedPool: ExecutorWorkerPool = {
      get workers() {
        return pool.workers
      },
      spawn: (id) => {
        const original = pool.spawn(id)
        // A dead isolate whose probe would throw if it were ever sent one. The
        // hub must short-circuit on `alive === false`; only an unproven probe
        // fails closed.
        dead = {
          id: original.id,
          alive: false,
          execute: original.execute.bind(original),
          cancel: original.cancel.bind(original),
          assertNotBusy: async () => {
            throw new Error("a dead worker must never be probed")
          },
          resultFor: original.resultFor.bind(original),
          terminate: original.terminate.bind(original),
          cancelPort: original.cancelPort,
        }
        return original
      },
      terminateAll: () => pool.terminateAll(),
      byID: (id) => (dead !== undefined && id === dead.id ? dead : pool.byID(id)),
      resultFor: (id) => pool.resultFor(id),
    }
    const hub = createSessionHub({ pool: guardedPool })

    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        void hub.ensureRunning(sessionID).catch(() => undefined)
        yield* pollWithTimeout(
          Effect.sync(() => (dead === undefined ? undefined : true)),
          "dispatch never spawned a worker",
        )
        yield* state.assertNotBusy(sessionID)
      }).pipe(
        Effect.provide(runStateLayer),
        Effect.provideService(SessionHubService, hub),
        Effect.provide(flagsOn),
        Effect.ensuring(Effect.promise(() => hub.terminateAll())),
      ),
    )
  })
})
