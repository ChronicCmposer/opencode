import { describe, expect } from "bun:test"
import { Effect } from "effect"
import path from "path"
import { pathToFileURL } from "url"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Npm } from "@opencode-ai/core/npm"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { GlobalBus, type GlobalEvent } from "../../src/bus/global"
import { Auth } from "../../src/auth"
import { Account } from "../../src/account/account"
import { RuntimeFlags } from "../../src/effect/runtime-flags"
import { Plugin } from "../../src/plugin/index"
import { TestInstance } from "../fixture/fixture"
import { testEffect, pollWithTimeout } from "../lib/effect"
import { AccountTest } from "../fake/account"
import { AuthTest } from "../fake/auth"
import { NpmTest } from "../fake/npm"

const it = testEffect(
  AppNodeBuilder.build(LayerNode.group([Plugin.node, CrossSpawnSpawner.node]), [
    [Auth.node, AuthTest.empty],
    [Account.node, AccountTest.empty],
    [Npm.node, NpmTest.noop],
    [RuntimeFlags.node, RuntimeFlags.layer({ disableDefaultPlugins: true })],
  ]),
)

const systemHook = "experimental.chat.system.transform"

// The test harness sets `OPENCODE_DISABLE_PROJECT_CONFIG` and points `OPENCODE_CONFIG_DIR` at the
// ambient merged config. Point it at a throwaway dir so this test's plugin list is exactly the
// broken plugin below.
function withProcessEnv<A, E, R>(key: string, value: string | undefined, effect: Effect.Effect<A, E, R>) {
  return Effect.acquireUseRelease(
    Effect.sync(() => {
      const original = process.env[key]
      if (value === undefined) delete process.env[key]
      else process.env[key] = value
      return original
    }),
    () => effect,
    (original) =>
      Effect.sync(() => {
        if (original === undefined) delete process.env[key]
        else process.env[key] = original
      }),
  )
}

describe("plugin.load.error", () => {
  it.instance(
    "publishes a loud error for a failing plugin without failing trigger",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const configDir = path.join(test.directory, "configdir")
        const file = path.join(configDir, "broken.ts")
        yield* Effect.promise(() =>
          Bun.write(file, ["export default async () => {", "  throw new Error('plugin load boom')", "}", ""].join("\n")),
        )
        yield* Effect.promise(() =>
          Bun.write(
            path.join(configDir, "opencode.json"),
            JSON.stringify(
              {
                $schema: "https://opencode.ai/config.json",
                plugin: [pathToFileURL(file).href],
              },
              null,
              2,
            ),
          ),
        )

        yield* withProcessEnv(
          "OPENCODE_CONFIG_DIR",
          configDir,
          Effect.gen(function* () {
            const received: GlobalEvent[] = []
            const handler = (event: GlobalEvent) => received.push(event)
            GlobalBus.on("event", handler)
            yield* Effect.addFinalizer(() => Effect.sync(() => GlobalBus.off("event", handler)))

            const plugin = yield* Plugin.Service
            // Drive instance-state creation through `trigger`: it resolves once plugins are loaded.
            // (`init()` uses the same state but is blocked by an unrelated pre-existing hang.)
            yield* plugin.trigger(
              systemHook,
              { model: { providerID: ProviderV2.ID.anthropic, modelID: ModelV2.ID.make("claude-sonnet-4-6") } },
              { system: [] as string[] },
            )

            const hit = yield* pollWithTimeout(
              Effect.sync(() => received.find((event) => JSON.stringify(event.payload).includes("broken.ts"))),
              "no plugin load error event was published",
              "2 seconds",
            )
            expect(hit.payload.type).toBe("session.error")
            expect(JSON.stringify(hit.payload)).toContain("Failed to load plugin")
          }),
        )
      }),
    15_000,
  )
})
