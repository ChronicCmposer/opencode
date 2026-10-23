// Session-worker rollout flag: ON/OFF behavior and the parser↔Config lockstep.
//
// The F5 refactor removed the logging scaffolding that used to live in
// `src/effect/runtime-flags.ts` (`describeSessionWorkersFlag`,
// `sessionWorkersFlagFields`, `logSessionWorkersFlag*`, the once-guard). What
// remains is:
//   * `experimentalSessionWorkersFromEnv` — the canonical parser, and
//   * the `RuntimeFlags.experimentalSessionWorkers` Config field, which
//     `src/project/bootstrap.ts` emits exactly once via
//     `Effect.logInfo("session workers flag", { experimentalSessionWorkers })`.
//
// This file pins the parser's ON/OFF behavior and proves the Config field never
// disagrees with it, so the value bootstrap logs can't diverge from the graph
// decision. The old source-attribution / field-shape / once-guard assertions are
// gone with the scaffolding they targeted.

import { describe, expect, test } from "bun:test"
import { ConfigProvider, Effect, Layer } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { RuntimeFlags } from "../src/effect/runtime-flags"
import { it } from "./lib/effect"

const SW = "OPENCODE_EXPERIMENTAL_SESSION_WORKERS"
const EXP = "OPENCODE_EXPERIMENTAL"

type Env = Record<string, string | undefined>
type FlagCase = {
  readonly name: string
  readonly env: Env
  readonly enabled: boolean
  // An invalid `OPENCODE_EXPERIMENTAL` makes the whole RuntimeFlags service
  // unbuildable — unrelated strict booleans (`experimental`) die on it — so this
  // case can only exercise the parser, not the Config path.
  readonly parserOnly?: boolean
}

const cases: readonly FlagCase[] = [
  { name: "nothing set", env: {}, enabled: false },
  { name: "dedicated true", env: { [SW]: "true" }, enabled: true },
  { name: "dedicated yes", env: { [SW]: "yes" }, enabled: true },
  { name: "dedicated on", env: { [SW]: "on" }, enabled: true },
  { name: "dedicated 1", env: { [SW]: "1" }, enabled: true },
  { name: "dedicated y", env: { [SW]: "y" }, enabled: true },
  { name: "dedicated false", env: { [SW]: "false" }, enabled: false },
  { name: "dedicated no", env: { [SW]: "no" }, enabled: false },
  { name: "dedicated off", env: { [SW]: "off" }, enabled: false },
  { name: "dedicated 0", env: { [SW]: "0" }, enabled: false },
  { name: "dedicated n", env: { [SW]: "n" }, enabled: false },
  { name: "dedicated case-sensitive TRUE is not boolean", env: { [SW]: "TRUE" }, enabled: false },
  { name: "dedicated nonsense falls back to the default", env: { [SW]: "nonsense" }, enabled: false },
  { name: "umbrella true", env: { [EXP]: "true" }, enabled: true },
  { name: "umbrella false", env: { [EXP]: "false" }, enabled: false },
  { name: "umbrella nonsense is treated as absent", env: { [EXP]: "nonsense" }, enabled: false, parserOnly: true },
  { name: "dedicated true wins over umbrella false", env: { [SW]: "true", [EXP]: "false" }, enabled: true },
  { name: "dedicated false wins over umbrella true", env: { [SW]: "false", [EXP]: "true" }, enabled: false },
  { name: "dedicated nonsense falls back to umbrella true", env: { [SW]: "nonsense", [EXP]: "true" }, enabled: true },
  { name: "undefined values are treated as absent", env: { [SW]: undefined, [EXP]: undefined }, enabled: false },
]

const fromConfig = (input: Env) =>
  AppNodeBuilder.build(RuntimeFlags.node).pipe(Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown(input))))

const readFlag = RuntimeFlags.Service.useSync((flags) => flags.experimentalSessionWorkers)

describe("session-worker flag ON/OFF (parser and Config field in lockstep)", () => {
  for (const c of cases) {
    test(`${c.name} -> parser ${c.enabled ? "ON" : "OFF"}`, () => {
      expect(RuntimeFlags.experimentalSessionWorkersFromEnv(c.env)).toBe(c.enabled)
    })

    if (c.parserOnly) continue

    it.effect(`${c.name} -> Config field ${c.enabled ? "ON" : "OFF"}`, () =>
      Effect.gen(function* () {
        const resolved = yield* readFlag.pipe(Effect.provide(fromConfig(c.env)))

        expect(resolved).toBe(c.enabled)
        expect(resolved).toBe(RuntimeFlags.experimentalSessionWorkersFromEnv(c.env))
      }),
    )
  }
})
