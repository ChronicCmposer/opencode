// Flag and kill-switch matrix.
//
// The two-state resolver and the build-hash status projection exist today, so
// they are asserted directly. Production startup validation, typed schema
// mismatch, kill-switch wiring, and the status endpoint are still owned by other
// waves; each lagging assertion is a `todo` with its owner rather than a silent
// absence.

import { describe, expect, test } from "bun:test"
import { BusyExhaustedError } from "@opencode-ai/core/database/busy"
import { RuntimeFlags } from "@/effect/runtime-flags"
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
  test("protocol version and mode are carried together", () => {
    const status = parseRolloutStatus(
      createRolloutStatus({
        enabled: false,
        disabledReason: "operator kill switch",
        protocolSchemaVersion: 7,
      }),
    )
    expect(status).toMatchObject({ mode: "off", disabledReason: "operator kill switch", protocolSchemaVersion: 7 })
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
  test("busy exhaustion does not collapse to a generic error", () => {
    expect(
      isRolloutInfraFailure(
        new BusyExhaustedError(5, new Error("locked")),
        "BusyExhausted",
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
  test.todo("configured unsupported plugin hook fails hub startup — owner plugin")
  test.todo("SchemaMismatch is typed when a worker observes a different schema — owner writer-db")
  test.todo("kill switch blocks new hub dispatches and drains existing Sessions — owner layer-graph")
  test.todo("status endpoint exposes mode, disabled reason, and build hash — owner session-root")
})
