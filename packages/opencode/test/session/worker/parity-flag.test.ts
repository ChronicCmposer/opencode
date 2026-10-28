// Flag-ON ≡ flag-OFF parity for a deterministic session-worker scenario.
//
// `runScenario` (test/lib/parity-harness.ts) runs ONE scenario through the
// per-session worker E2E graph. The flag selects how the hub-owned authorities
// are reached:
//
//   * ON  — `SessionStatus` over the real hub proxy, exactly as a worker calls
//     the hub;
//   * OFF — the authority stays in the worker graph (the in-process parity
//     wiring).
//
// Both sides must observe the same lifecycle. Generated ids and wall-clock
// fields are normalized before comparing, so only behavioral divergence fails.

import { describe, expect, test } from "bun:test"
import { Deferred, Effect, Fiber } from "effect"
import { SessionID } from "@/session/schema"
import { SessionStatus } from "@/session/status"
import { normalizeTrace, runScenario, type Scenario } from "../../lib/parity-harness"
import { workerSnapshot } from "../../lib/snapshot"
import { tmpdir } from "../../fixture/fixture"

/** The scenario outcome, normalized before comparison. */
interface StatusTrace {
  readonly sessionID: string
  readonly statuses: ReadonlyArray<string>
}

/**
 * A status lifecycle both flag states can run without a provider call: set
 * busy, read it back, set idle, read it back. Reading back is what makes the
 * hub-proxy and in-process paths comparable — each side must observe its own
 * writes through its configured transport.
 */
const statusScenario = (directory: string): Scenario<StatusTrace, SessionStatus.Service> => ({
  name: "session-status-lifecycle",
  snapshot: workerSnapshot({ directory }),
  modelConfig: {
    providerID: "mock",
    modelID: "mock",
    apiId: "mock",
    baseUrl: "http://127.0.0.1:1",
    apiKey: "test",
  },
  run: () =>
    Effect.gen(function* () {
      const status = yield* SessionStatus.Service
      const sessionID = SessionID.descending()

      yield* status.set(sessionID, { type: "busy" })
      const busy = yield* status.get(sessionID)
      yield* status.set(sessionID, { type: "idle" })
      const idle = yield* status.get(sessionID)

      return { sessionID, statuses: [busy.type, idle.type] }
    }),
})

/**
 * A live run that is interrupted while busy. The run's finalizer settles the
 * session to idle, so the observable cancel outcome is "busy then idle" — the
 * same settlement a worker must perform when its run is cancelled. Reading the
 * final status back is what makes the hub-proxy and in-process paths comparable.
 *
 * The interruption is driven by a `Deferred` readiness signal, not a sleep, so
 * the cancel can never race the run's start.
 */
const cancelScenario = (directory: string): Scenario<StatusTrace, SessionStatus.Service> => ({
  name: "session-cancel-settles-idle",
  snapshot: workerSnapshot({ directory }),
  modelConfig: {
    providerID: "mock",
    modelID: "mock",
    apiId: "mock",
    baseUrl: "http://127.0.0.1:1",
    apiKey: "test",
  },
  run: () =>
    Effect.gen(function* () {
      const status = yield* SessionStatus.Service
      const sessionID = SessionID.descending()
      const started = yield* Deferred.make<void>()

      yield* status.set(sessionID, { type: "busy" })
      const running = yield* Effect.gen(function* () {
        yield* Deferred.succeed(started, undefined)
        return yield* Effect.never
      }).pipe(
        Effect.ensuring(status.set(sessionID, { type: "idle" })),
        Effect.exit,
        Effect.forkChild,
      )

      yield* Deferred.await(started)
      yield* Fiber.interrupt(running)
      const settled = yield* status.get(sessionID)

      return { sessionID, statuses: ["busy", settled.type] }
    }),
})

describe("session worker flag parity", () => {
  test("the worker side reports the normalized status lifecycle", async () => {
    await using tmp = await tmpdir()
    const trace = await runScenario(true, statusScenario(tmp.path))

    expect(trace.flagOn).toBe(true)
    expect(normalizeTrace(trace.value)).toEqual({
      sessionID: "ses_<id>",
      statuses: ["busy", "idle"],
    })
  }, 30_000)

  test("flag ON and flag OFF produce the same normalized trace", async () => {
    await using tmp = await tmpdir()
    const scenario = statusScenario(tmp.path)

    const off = await runScenario(false, scenario)
    const on = await runScenario(true, scenario)

    expect(off.flagOn).toBe(false)
    expect(on.flagOn).toBe(true)
    expect(normalizeTrace(on.value)).toEqual(normalizeTrace(off.value))
  }, 60_000)

  test("cancelling a live run settles status idle under the worker side", async () => {
    await using tmp = await tmpdir()
    const trace = await runScenario(true, cancelScenario(tmp.path))

    expect(trace.flagOn).toBe(true)
    expect(normalizeTrace(trace.value)).toEqual({
      sessionID: "ses_<id>",
      statuses: ["busy", "idle"],
    })
  }, 30_000)

  test("flag ON and flag OFF settle a cancelled run identically", async () => {
    await using tmp = await tmpdir()
    const scenario = cancelScenario(tmp.path)

    const off = await runScenario(false, scenario)
    const on = await runScenario(true, scenario)

    expect(off.flagOn).toBe(false)
    expect(on.flagOn).toBe(true)
    expect(normalizeTrace(on.value)).toEqual(normalizeTrace(off.value))
  }, 60_000)
})
