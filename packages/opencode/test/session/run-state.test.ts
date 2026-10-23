// p1-entry — the `SessionRunState.startShell` flag seam.
//
// Frozen contract: `startShell` takes a trailing optional `shell` payload. Flag
// ON (hub present + `experimentalSessionWorkers`) the shell rides the hub
// dispatch (`hub.startShell(sessionID, shell)`) and the caller's local `work`
// never runs (its closures cannot cross the isolate). Flag OFF the local `work`
// runs in-process. These tests pin the forwarding and the branch split that the
// end-to-end shell tests in `prompt.test.ts` do not observe directly.

import { describe, expect } from "bun:test"
import { Effect, Latch, Layer } from "effect"
import { BackgroundJob } from "@/background/job"
import type { SessionID } from "@/session/schema"
import { SessionRunState } from "@/session/run-state"
import { SessionStatus } from "@/session/status"
import type { ShellInput } from "@/session/prompt"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { createSessionHub, SessionHubService, type SessionHub } from "@/session/worker/hub"
import { withTmpdirInstance } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
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
      Layer.mock(SessionStatus.Service, { set: () => Effect.void }),
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
