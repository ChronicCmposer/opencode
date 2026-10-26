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
import { Effect } from "effect"
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

  // Phase 11 gate: terminate-based cancel + PID reaping must look the same from
  // both flag states (cancel settles the run, status ends idle/cancelled, no
  // session is left busy). It is skipped, not failing, because `runScenario`
  // has no prompt/cancel driver yet: a real cancel needs the provider-driven
  // session loop in `test/lib/parity-session.ts`.
  // TODO(Phase 11): add a synchronized cancel scenario (promise-gated prompt,
  // abort, then await settlement) and assert equal normalized traces; replace
  // `.skip` with `test` once the driver exists. Do not add fixed sleeps.
  test.skip("phase 11 gate: cancel settles identically under both flag states", () => {})
})
