// The `SessionRunState.startShell` flag seam.
//
// Frozen contract: `startShell` takes a trailing optional `shell` payload. Flag
// ON (hub present + `experimentalSessionWorkers`) the shell rides the hub
// dispatch (`hub.startShell(sessionID, shell)`) and the caller's local `work`
// never runs (its closures cannot cross the isolate). Flag OFF the local `work`
// runs in-process. These tests pin the forwarding and the branch split that the
// end-to-end shell tests in `prompt.test.ts` do not observe directly.

import { describe, expect } from "bun:test"
import { Cause, Deferred, Effect, Exit, Fiber, Latch, Layer } from "effect"
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
import { createTestBarrier } from "../lib/run-barrier-test"
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

/** A `SessionStatus` layer that records every write, in order, for assertions. */
const recordingStatus = () => {
  const store = new Map<SessionID, SessionStatus.Info>()
  const log: string[] = []
  const calls: Array<{ sessionID: SessionID; status: SessionStatus.Info }> = []
  const layer = Layer.succeed(SessionStatus.Service, {
    get: (sessionID) => Effect.sync(() => store.get(sessionID) ?? { type: "idle" as const }),
    set: (sessionID, status) =>
      Effect.sync(() => {
        store.set(sessionID, status)
        log.push(`${String(sessionID)}:${status.type}`)
        calls.push({ sessionID, status })
      }),
    list: () => Effect.sync(() => new Map(store)),
  })
  return { store, log, calls, layer }
}

/** `SessionRunState.layer` with a recording `SessionStatus` in place of the idle mock. */
const runStateLayerWith = (status: Layer.Layer<SessionStatus.Service>) =>
  SessionRunState.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(BackgroundJob.Service, { list: () => Effect.succeed([]) }),
        status,
      ),
    ),
  )

/** A hub that records the `startShell` arguments it receives, then delegates. */
const recordingShellHub = (sessionID: SessionID, forwarded: Array<{ sessionID: SessionID; shell?: ShellInput }>) => {
  const base = createSessionHub({ ...{ barrier: createTestBarrier() }, pool: createFakeExecutorPool(1, { value: withParts(sessionID) }) })
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
  const base = createSessionHub({ ...{ barrier: createTestBarrier() }, pool: createFakeExecutorPool(1, { value: withParts(sessionID) }) })
  return {
    ...base,
    ensureRunningEffect: (id: SessionID, origin?: DispatchOrigin) => {
      forwarded.push({ sessionID: id, origin })
      return base.ensureRunningEffect(id, origin)
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
    const base = createSessionHub({ ...{ barrier: createTestBarrier() }, pool: createFakeExecutorPool(1, { value: withParts(sessionID) }) })
    return { ...base, ...overrides } satisfies SessionHub
  }

  it.live("ensureRunning honors onInterrupt when the hub reports a clean cancel", () => {
    const sessionID = sid("ses_cancel_typed")
    const interrupted = withParts(sessionID)
    const hub = failingHub({
      ensureRunningEffect: () => Effect.fail(new SessionCancelledError({ message: "cancelled", sessionID })),
    })

    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        // Flag-off parity: a clean cancel is the caller's interrupted result, not
        // a typed failure. The HTTP prompt handler therefore returns 200.
        const result = yield* state.ensureRunning(
          sessionID,
          Effect.succeed(interrupted),
          Effect.succeed(withParts(sessionID)),
        )

        expect(result).toEqual(interrupted)
      }).pipe(Effect.provide(runStateLayer), Effect.provideService(SessionHubService, hub), Effect.provide(flagsOn)),
    )
  })

  it.live("startShell honors onInterrupt when the hub reports a clean cancel", () => {
    const sessionID = sid("ses_shell_cancel_typed")
    const interrupted = withParts(sessionID)
    const hub = failingHub({
      startShell: async () => {
        throw new SessionCancelledError({ message: "cancelled", sessionID })
      },
    })

    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        const result = yield* state.startShell(
          sessionID,
          Effect.succeed(interrupted),
          Effect.succeed(withParts(sessionID)),
          undefined,
          { sessionID, agent: "build", command: "echo cancel" },
        )

        expect(result).toEqual(interrupted)
      }).pipe(Effect.provide(runStateLayer), Effect.provideService(SessionHubService, hub), Effect.provide(flagsOn)),
    )
  })

  it.live("a worker result holding a stored part that no longer decodes completes like in-process", () => {
    const sessionID = sid("ses_legacy_result")
    const message = withParts(sessionID)
    const legacy = {
      ...message,
      parts: [{ id: "prt_legacy", sessionID, messageID: message.info.id, type: "tool", tool: "compress", state: { status: "pending", input: {}, raw: "" } }],
    }
    const hub = failingHub({ ensureRunningEffect: () => Effect.succeed({ sessionID, value: legacy }) })

    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        const result = yield* state.ensureRunning(sessionID, Effect.succeed(message), Effect.succeed(message))
        expect(result).toEqual(legacy as never)
      }).pipe(Effect.provide(runStateLayer), Effect.provideService(SessionHubService, hub), Effect.provide(flagsOn)),
    )
  })

  it.live("a worker result that is not a message is a defect", () => {
    const sessionID = sid("ses_broken_result")
    const hub = failingHub({ ensureRunningEffect: () => Effect.succeed({ sessionID, value: "not a message" }) })

    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        const exit = yield* state
          .ensureRunning(sessionID, Effect.succeed(withParts(sessionID)), Effect.succeed(withParts(sessionID)))
          .pipe(Effect.exit)
        expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(true)
      }).pipe(Effect.provide(runStateLayer), Effect.provideService(SessionHubService, hub), Effect.provide(flagsOn)),
    )
  })

  it.live("ensureRunning surfaces a hub crash as a typed failure, not a defect", () => {
    const sessionID = sid("ses_crash_typed")
    const hub = failingHub({
      ensureRunningEffect: () => Effect.fail(new WorkerCrashedError({ message: "worker crashed", workerID: 7, sessionID })),
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
    const hub = createSessionHub({ ...{ barrier: createTestBarrier() }, pool: guardedPool })

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

describe("SessionRunState.ensureRunning — worker compatibility", () => {
  const hubWith = (sessionID: SessionID, reason: string | undefined, dispatched: SessionID[]) => {
    const base = createSessionHub({
      barrier: createTestBarrier(),
      pool: createFakeExecutorPool(1, { value: withParts(sessionID) }),
    })
    return {
      ...base,
      workerIncompatibility: () => Effect.succeed(reason),
      ensureRunningEffect: (id: SessionID, origin?: DispatchOrigin) => {
        dispatched.push(id)
        return base.ensureRunningEffect(id, origin)
      },
    } satisfies SessionHub
  }

  const run = (reason: string | undefined) => {
    const sessionID = sid(reason === undefined ? "ses_worker_ok" : "ses_worker_incompatible")
    const dispatched: SessionID[] = []
    const hub = hubWith(sessionID, reason, dispatched)
    let localRan = false
    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        const work = Effect.sync(() => {
          localRan = true
          return withParts(sessionID)
        })
        yield* state.ensureRunning(sessionID, work, work)
        return { localRan, dispatched }
      }).pipe(
        Effect.provide(runStateLayer),
        Effect.provideService(SessionHubService, hub),
        Effect.provide(flagsOn),
      ),
    )
  }

  it.live("a worker-incompatible model runs in-process with the flag on", () =>
    Effect.gen(function* () {
      const result = yield* run("provider uses OAuth")
      expect(result.localRan).toBe(true)
      expect(result.dispatched).toEqual([])
    }),
  )

  it.live("a worker-compatible model still dispatches to the hub", () =>
    Effect.gen(function* () {
      const result = yield* run(undefined)
      expect(result.localRan).toBe(false)
      expect(result.dispatched).toHaveLength(1)
    }),
  )
})

// ---------------------------------------------------------------------------
// Hub admission: busy is published before a cold worker boots, exactly once
// per in-flight run, and released with one idle when the boot fails. `awaitBusy`
// must observe that admission rather than race the worker's own busy.
// ---------------------------------------------------------------------------

describe("SessionRunState — hub admission busy/idle", () => {
  const hubWith = (sessionID: SessionID, overrides: Partial<SessionHub>): SessionHub => {
    const base = createSessionHub({
      barrier: createTestBarrier(),
      pool: createFakeExecutorPool(1, { value: withParts(sessionID) }),
    })
    return { ...base, ...overrides } satisfies SessionHub
  }

  it.live("admission busy is published before dispatch and one idle follows a boot failure", () => {
    const sessionID = sid("ses_cold_admit")
    const status = recordingStatus()
    let sawBusyAtDispatch = false
    const hub = hubWith(sessionID, {
      ensureRunningEffect: (id) =>
        Effect.sync(() => {
          sawBusyAtDispatch = status.store.get(id)?.type === "busy"
        }).pipe(
          Effect.andThen(
            Effect.fail(new WorkerCrashedError({ message: "boot failed", workerID: 1, sessionID })),
          ),
        ),
    })

    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        const exit = yield* state
          .ensureRunning(sessionID, Effect.succeed(withParts(sessionID)), Effect.succeed(withParts(sessionID)))
          .pipe(Effect.exit)

        expect(Exit.isFailure(exit)).toBe(true)
        expect(sawBusyAtDispatch).toBe(true)
        expect(status.log.filter((entry) => entry.endsWith(":busy"))).toHaveLength(1)
        expect(status.log.filter((entry) => entry.endsWith(":idle"))).toHaveLength(1)
      }).pipe(
        Effect.provide(runStateLayerWith(status.layer)),
        Effect.provideService(SessionHubService, hub),
        Effect.provide(flagsOn),
      ),
    )
  })

  it.live("a clean cancel during boot publishes exactly one busy and one idle", () => {
    const sessionID = sid("ses_cold_cancel")
    const status = recordingStatus()
    const hub = hubWith(sessionID, {
      ensureRunningEffect: () =>
        Effect.fail(new SessionCancelledError({ message: "cancelled during boot", sessionID })),
    })

    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        // A clean cancel during boot is the caller's interrupt result, not a
        // failure, and the admission busy must be released with exactly one idle.
        const result = yield* state.ensureRunning(
          sessionID,
          Effect.succeed(withParts(sessionID)),
          Effect.succeed(withParts(sessionID)),
        )

        expect(result).toEqual(withParts(sessionID))
        expect(status.calls).toEqual([
          { sessionID, status: { type: "busy" } },
          { sessionID, status: { type: "idle" } },
        ])
      }).pipe(
        Effect.provide(runStateLayerWith(status.layer)),
        Effect.provideService(SessionHubService, hub),
        Effect.provide(flagsOn),
      ),
    )
  })

  it.live("a second prompt during a cold start publishes no extra busy", () => {
    const sessionID = sid("ses_cold_join")
    const status = recordingStatus()
    const gate = Deferred.makeUnsafe<void>()
    let dispatches = 0
    const hub = hubWith(sessionID, {
      ensureRunningEffect: () =>
        Effect.sync(() => {
          dispatches += 1
        }).pipe(
          Effect.andThen(Deferred.await(gate)),
          Effect.andThen(Effect.succeed({ sessionID: String(sessionID), value: withParts(sessionID) })),
        ),
    })

    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        const work = Effect.succeed(withParts(sessionID))
        const first = yield* state.ensureRunning(sessionID, work, work).pipe(Effect.forkChild)
        yield* pollWithTimeout(
          Effect.sync(() => (dispatches >= 1 ? true : undefined)),
          "first dispatch never started",
        )
        const second = yield* state.ensureRunning(sessionID, work, work).pipe(Effect.forkChild)
        yield* pollWithTimeout(
          Effect.sync(() => (dispatches >= 2 ? true : undefined)),
          "second dispatch never started",
        )
        expect(status.log.filter((entry) => entry.endsWith(":busy"))).toHaveLength(1)
        yield* Deferred.succeed(gate, undefined)
        yield* Fiber.await(first)
        yield* Fiber.await(second)
      }).pipe(
        Effect.provide(runStateLayerWith(status.layer)),
        Effect.provideService(SessionHubService, hub),
        Effect.provide(flagsOn),
      ),
    )
  })

  it.live("awaitBusy on a routed session waits for admission", () => {
    const sessionID = sid("ses_await_busy")
    const status = recordingStatus()
    const gate = Deferred.makeUnsafe<void>()
    const hub = hubWith(sessionID, {
      ensureRunningEffect: () =>
        Deferred.await(gate).pipe(
          Effect.as({ sessionID: String(sessionID), value: withParts(sessionID) }),
        ),
    })

    return withTmpdirInstance()(
      Effect.gen(function* () {
        const state = yield* SessionRunState.Service
        const woke = yield* Deferred.make<void>()
        const waiter = yield* state
          .awaitBusy(sessionID)
          .pipe(Effect.andThen(Deferred.succeed(woke, undefined)), Effect.forkChild)
        yield* Effect.yieldNow
        expect(yield* Deferred.isDone(woke)).toBe(false)
        const work = Effect.succeed(withParts(sessionID))
        const run = yield* state.ensureRunning(sessionID, work, work).pipe(Effect.forkChild)
        yield* pollWithTimeout(
          Effect.map(Deferred.isDone(woke), (done) => (done ? true : undefined)),
          "awaitBusy never resolved after admission",
        )
        expect(status.log.filter((entry) => entry.endsWith(":busy"))).toHaveLength(1)
        yield* Deferred.succeed(gate, undefined)
        yield* Fiber.await(run)
        yield* Fiber.await(waiter)
      }).pipe(
        Effect.provide(runStateLayerWith(status.layer)),
        Effect.provideService(SessionHubService, hub),
        Effect.provide(flagsOn),
      ),
    )
  })
})
