import { beforeEach, describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { RuntimeFlags } from "../src/effect/runtime-flags"

type Env = Record<string, string | undefined>
type Source = "dedicated" | "umbrella" | "default"
type LogCall = { message: string; fields: RuntimeFlags.SessionWorkersFlagFields }

const SW = "OPENCODE_EXPERIMENTAL_SESSION_WORKERS"
const EXP = "OPENCODE_EXPERIMENTAL"

const recordingLogInfo = () => {
  const calls: LogCall[] = []
  const logInfo = (message: string, fields: RuntimeFlags.SessionWorkersFlagFields) => {
    calls.push({ message, fields })
    return Effect.void
  }
  return { calls, logInfo }
}

describe("describeSessionWorkersFlag", () => {
  const cases: Array<{ name: string; env: Env; enabled: boolean; source: Source }> = [
    { name: "nothing set", env: {}, enabled: false, source: "default" },
    { name: "dedicated true", env: { [SW]: "true" }, enabled: true, source: "dedicated" },
    { name: "dedicated yes", env: { [SW]: "yes" }, enabled: true, source: "dedicated" },
    { name: "dedicated on", env: { [SW]: "on" }, enabled: true, source: "dedicated" },
    { name: "dedicated 1", env: { [SW]: "1" }, enabled: true, source: "dedicated" },
    { name: "dedicated y", env: { [SW]: "y" }, enabled: true, source: "dedicated" },
    { name: "dedicated false", env: { [SW]: "false" }, enabled: false, source: "dedicated" },
    { name: "dedicated no", env: { [SW]: "no" }, enabled: false, source: "dedicated" },
    { name: "dedicated off", env: { [SW]: "off" }, enabled: false, source: "dedicated" },
    { name: "dedicated 0", env: { [SW]: "0" }, enabled: false, source: "dedicated" },
    { name: "dedicated n", env: { [SW]: "n" }, enabled: false, source: "dedicated" },
    { name: "dedicated case-sensitive TRUE", env: { [SW]: "TRUE" }, enabled: false, source: "default" },
    { name: "dedicated nonsense", env: { [SW]: "nonsense" }, enabled: false, source: "default" },
    { name: "umbrella true", env: { [EXP]: "true" }, enabled: true, source: "umbrella" },
    { name: "umbrella false", env: { [EXP]: "false" }, enabled: false, source: "umbrella" },
    { name: "umbrella nonsense", env: { [EXP]: "nonsense" }, enabled: false, source: "default" },
    { name: "dedicated true wins over umbrella false", env: { [SW]: "true", [EXP]: "false" }, enabled: true, source: "dedicated" },
    { name: "dedicated false wins over umbrella true", env: { [SW]: "false", [EXP]: "true" }, enabled: false, source: "dedicated" },
    { name: "dedicated invalid falls back to umbrella true", env: { [SW]: "nonsense", [EXP]: "true" }, enabled: true, source: "umbrella" },
    { name: "undefined values treated as absent", env: { [SW]: undefined, [EXP]: undefined }, enabled: false, source: "default" },
  ]

  for (const c of cases) {
    test(`${c.name} -> ${c.enabled}/${c.source}`, () => {
      const described = RuntimeFlags.describeSessionWorkersFlag(c.env)
      expect(described).toEqual({ enabled: c.enabled, source: c.source })
    })

    test(`${c.name} stays in lockstep with the canonical parser`, () => {
      const described = RuntimeFlags.describeSessionWorkersFlag(c.env)
      expect(described.enabled).toBe(RuntimeFlags.experimentalSessionWorkersFromEnv(c.env))
    })
  }
})

describe("sessionWorkersFlagFields", () => {
  test("preserves raw values verbatim and marks unset with <unset>", () => {
    expect(RuntimeFlags.sessionWorkersFlagFields({ [SW]: "yes" })).toEqual({
      [SW]: "yes",
      [EXP]: "<unset>",
      experimentalSessionWorkers: true,
      source: "dedicated",
    })
  })

  test("reports umbrella source when only the umbrella is set", () => {
    expect(RuntimeFlags.sessionWorkersFlagFields({ [EXP]: "true" })).toEqual({
      [SW]: "<unset>",
      [EXP]: "true",
      experimentalSessionWorkers: true,
      source: "umbrella",
    })
  })

  test("reports default source when nothing is set", () => {
    expect(RuntimeFlags.sessionWorkersFlagFields({})).toEqual({
      [SW]: "<unset>",
      [EXP]: "<unset>",
      experimentalSessionWorkers: false,
      source: "default",
    })
  })

  test("omitting runtimeValue leaves the two runtime fields absent", () => {
    const fields = RuntimeFlags.sessionWorkersFlagFields({ [SW]: "true" })

    expect("runtimeExperimentalSessionWorkers" in fields).toBe(false)
    expect("diverges" in fields).toBe(false)
    expect(fields).toEqual({
      [SW]: "true",
      [EXP]: "<unset>",
      experimentalSessionWorkers: true,
      source: "dedicated",
    })
  })
})

describe("sessionWorkersFlagFields runtime gate", () => {
  test("agree (both true) reports runtime true and diverges false", () => {
    const fields = RuntimeFlags.sessionWorkersFlagFields({ [SW]: "true" }, true)

    expect(fields.experimentalSessionWorkers).toBe(true)
    expect(fields.runtimeExperimentalSessionWorkers).toBe(true)
    expect(fields.diverges).toBe(false)
  })

  test("agree (both false) reports runtime false and diverges false", () => {
    const fields = RuntimeFlags.sessionWorkersFlagFields({ [SW]: "false" }, false)

    expect(fields.experimentalSessionWorkers).toBe(false)
    expect(fields.runtimeExperimentalSessionWorkers).toBe(false)
    expect(fields.diverges).toBe(false)
  })

  test("diverge (env false, runtime true) reports diverges true", () => {
    const fields = RuntimeFlags.sessionWorkersFlagFields({ [SW]: "false" }, true)

    expect(fields.experimentalSessionWorkers).toBe(false)
    expect(fields.runtimeExperimentalSessionWorkers).toBe(true)
    expect(fields.diverges).toBe(true)
  })
})

describe("logSessionWorkersFlag", () => {
  test("emits exactly the 'session workers flag' message with the derived fields", () => {
    const { calls, logInfo } = recordingLogInfo()

    Effect.runSync(RuntimeFlags.logSessionWorkersFlag(logInfo, { [SW]: "yes" }))

    expect(calls).toHaveLength(1)
    expect(calls[0].message).toBe("session workers flag")
    expect(calls[0].fields).toEqual(RuntimeFlags.sessionWorkersFlagFields({ [SW]: "yes" }))
  })

  test("threads the runtime gate value into the emitted fields", () => {
    const { calls, logInfo } = recordingLogInfo()

    Effect.runSync(RuntimeFlags.logSessionWorkersFlag(logInfo, { [SW]: "false" }, true))

    expect(calls).toHaveLength(1)
    expect(calls[0].fields.runtimeExperimentalSessionWorkers).toBe(true)
    expect(calls[0].fields.diverges).toBe(true)
  })
})

describe("logSessionWorkersFlagOnce", () => {
  beforeEach(() => {
    RuntimeFlags.resetSessionWorkersFlagLoggedForTest()
  })

  test("emits the line exactly once across repeated invocations", () => {
    const { calls, logInfo } = recordingLogInfo()

    Effect.runSync(RuntimeFlags.logSessionWorkersFlagOnce(logInfo, {}))
    Effect.runSync(RuntimeFlags.logSessionWorkersFlagOnce(logInfo, {}))

    expect(calls).toHaveLength(1)
    expect(calls[0].message).toBe("session workers flag")
  })

  test("threads the runtime gate value through the once-guard", () => {
    const { calls, logInfo } = recordingLogInfo()

    Effect.runSync(RuntimeFlags.logSessionWorkersFlagOnce(logInfo, {}, true))

    expect(calls).toHaveLength(1)
    expect(calls[0].fields.runtimeExperimentalSessionWorkers).toBe(true)
    expect(calls[0].fields.diverges).toBe(true)
  })
})
