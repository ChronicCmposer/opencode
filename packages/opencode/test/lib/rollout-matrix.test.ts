import { describe, expect, test } from "bun:test"
import { BusyExhaustedError } from "@opencode-ai/core/database/busy"
import {
  DECLARED_ONLY_HOOKS,
  UnsupportedConfiguredHookError,
  assertConfiguredHooksSupported,
  createRolloutStatus,
  decideKillSwitchAdmission,
  isRolloutInfraFailure,
  parseRolloutState,
  parseRolloutStatus,
  rolloutStateFromFlag,
} from "./rollout-matrix"

const build = { protocolSchemaVersion: 7 }

describe("two-state rollout", () => {
  test("only off and on parse; a third state fails loud", () => {
    expect(parseRolloutState("off")).toBe("off")
    expect(parseRolloutState("on")).toBe("on")
    expect(rolloutStateFromFlag(false)).toBe("off")
    expect(rolloutStateFromFlag(true)).toBe("on")
    expect(() => parseRolloutState("auto")).toThrow(/expected "off" or "on"/)
    expect(() => parseRolloutState(undefined)).toThrow(/invalid rollout state/)
  })

  test("the status projection carries mode, reason, and protocol schema version", () => {
    const off = createRolloutStatus({ enabled: false, disabledReason: "kill switch", ...build })
    expect(off).toMatchObject({ mode: "off", disabledReason: "kill switch", protocolSchemaVersion: 7 })

    const on = createRolloutStatus({ enabled: true, disabledReason: "ignored", ...build })
    expect(on).toMatchObject({ mode: "on", disabledReason: null })

    expect(() => createRolloutStatus({ enabled: false, ...build })).toThrow(/disabledReason/)
    expect(() => createRolloutStatus({ enabled: true, disabledReason: "why", ...build })).not.toThrow()
    expect(() =>
      parseRolloutStatus({ mode: "off", disabledReason: "x", protocolSchemaVersion: -1 }),
    ).toThrow(/protocolSchemaVersion/)
  })
})

describe("kill switch", () => {
  const existing = new Set(["ses_existing"])

  test("does nothing while disabled", () => {
    expect(
      decideKillSwitchAdmission({ killSwitchEnabled: false, existingSessionIDs: existing, sessionID: "ses_new" }),
    ).toEqual({ admit: true, reason: "allowed" })
  })

  test("blocks new Sessions and drains existing ones", () => {
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

describe("startup plugin validation", () => {
  const supported = new Set(["chat.params", "chat.headers", "tool.execute.before"])

  test("a configured unsupported hook is fatal and names every offender", () => {
    expect(() =>
      assertConfiguredHooksSupported({ configured: ["chat.params", "tool.execute.after"], supported }),
    ).toThrow(UnsupportedConfiguredHookError)
  })

  test("declared-only hooks are allowed to be configured", () => {
    expect(() =>
      assertConfiguredHooksSupported({
        configured: ["chat.params", ...DECLARED_ONLY_HOOKS],
        supported,
      }),
    ).not.toThrow()
  })

  test("supported hooks pass and duplicates are de-duplicated", () => {
    expect(() =>
      assertConfiguredHooksSupported({ configured: ["chat.params", "chat.params"], supported }),
    ).not.toThrow()
  })
})

describe("typed infrastructure failures", () => {
  test("busy exhaustion stays typed", () => {
    expect(
      isRolloutInfraFailure(
        new BusyExhaustedError(5, new Error("locked")),
        "BusyExhausted",
      ),
    ).toBe(true)
    expect(isRolloutInfraFailure(new Error("plain"), "BusyExhausted")).toBe(false)
  })

  test("schema mismatch is recognized by tag once the owner lands it", () => {
    expect(isRolloutInfraFailure({ _tag: "SchemaMismatch" }, "SchemaMismatch")).toBe(true)
  })
})
