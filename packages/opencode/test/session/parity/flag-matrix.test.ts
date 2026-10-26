// Wave 6 flag and kill-switch matrix.
//
// The two-state resolver and the build-hash status projection exist today, so
// they are asserted directly. Production startup validation, typed schema
// mismatch, kill-switch wiring, and the status endpoint are still owned by other
// waves; each lagging assertion is a `todo` with its owner rather than a silent
// absence.

import { describe, expect, test } from "bun:test"
import { BusyRetriesExhaustedError } from "@opencode-ai/core/database/busy"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { BuildMismatch, buildHashStatus } from "@/session/worker/build-hash"
import {
  DECLARED_ONLY_HOOKS,
  assertConfiguredHooksSupported,
  createRolloutStatus,
  decideKillSwitchAdmission,
  isRolloutInfraFailure,
  parseRolloutState,
  parseRolloutStatus,
} from "../../lib/rollout-matrix"

describe("rollout flag is two-state only", () => {
  test("the canonical resolver returns exactly off or on", () => {
    expect(RuntimeFlags.resolveSessionWorkersFlag({})).toBe(false)
    expect(RuntimeFlags.resolveSessionWorkersFlag({ OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "off" })).toBe(false)
    expect(RuntimeFlags.resolveSessionWorkersFlag({ OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "on" })).toBe(true)
    expect(RuntimeFlags.resolveSessionWorkersFlag({ OPENCODE_EXPERIMENTAL: "on" })).toBe(true)
    expect(RuntimeFlags.resolveSessionWorkersFlag({ OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "auto" })).toBe(false)
    expect(parseRolloutState(RuntimeFlags.resolveSessionWorkersFlag({}) ? "on" : "off")).toBe("off")
  })
})

describe("status projection", () => {
  test("build identity and mode are carried together", () => {
    const build = buildHashStatus({ env: {}, definedHash: "deadbeef" })
    expect(build).toMatchObject({ buildHash: "deadbeef", source: "build", override: false })

    const status = parseRolloutStatus(
      createRolloutStatus({
        enabled: false,
        disabledReason: "operator kill switch",
        buildHash: build.buildHash,
        protocolSchemaVersion: build.protocolSchemaVersion,
        buildHashSource: build.source,
        override: build.override,
      }),
    )
    expect(status).toMatchObject({ mode: "off", disabledReason: "operator kill switch", buildHash: "deadbeef" })
  })

  test("a development override is visible in status", () => {
    const logs: string[] = []
    const build = buildHashStatus({
      env: { OPENCODE_BUILD_HASH_OVERRIDE: "dev-hash" },
      definedHash: "build-hash",
      log: (message) => void logs.push(message),
    })
    expect(build).toMatchObject({ buildHash: "dev-hash", source: "override", override: true })
    expect(logs).toHaveLength(1)
  })
})

describe("kill-switch contract", () => {
  test("new Sessions are blocked while existing Sessions drain", () => {
    const existing = new Set(["ses_existing"])
    expect(
      decideKillSwitchAdmission({ killSwitchEnabled: true, existingSessionIDs: existing, sessionID: "ses_new" }),
    ).toEqual({ admit: false, reason: "kill-switch-new-session" })
    expect(
      decideKillSwitchAdmission({
        killSwitchEnabled: true,
        existingSessionIDs: existing,
        sessionID: "ses_existing",
      }),
    ).toEqual({ admit: true, reason: "drain-existing" })
  })
})

describe("typed infrastructure failures", () => {
  test("build mismatch and busy exhaustion do not collapse to a generic error", () => {
    expect(isRolloutInfraFailure(new BuildMismatch({ expected: "a", actual: "b" }), "BuildMismatch")).toBe(true)
    expect(
      isRolloutInfraFailure(
        new BusyRetriesExhaustedError({ attempts: 5, lastCause: new Error("locked") }),
        "BusyRetriesExhaustedError",
      ),
    ).toBe(true)
  })
})

describe("startup validation contract", () => {
  test("configured unsupported hooks are fatal and declared-only hooks are allowed", () => {
    const supported = new Set(["chat.params"])
    expect(() =>
      assertConfiguredHooksSupported({ configured: ["chat.params", "tool.execute.after"], supported }),
    ).toThrow(/no worker route/)
    expect(() =>
      assertConfiguredHooksSupported({ configured: ["chat.params", ...DECLARED_ONLY_HOOKS], supported }),
    ).not.toThrow()
  })
})

describe("lagging production assertions", () => {
  test.todo("configured unsupported plugin hook fails hub startup — owner plugin (W4.1)")
  test.todo("SchemaMismatch is typed when a worker observes a different schema — owner writer-db (W1.6)")
  test.todo("kill switch blocks new hub dispatches and drains existing Sessions — owner layer-graph (W6.3)")
  test.todo("status endpoint exposes mode, disabled reason, and build hash — owner session-root (W6.3)")
})
