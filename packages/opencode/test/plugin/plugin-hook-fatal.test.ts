// W4.1 / D19 — a configured plugin that declares an unsupported invoked hook is
// fatal at startup when session workers are enabled. It is never skipped and
// there is no in-process fallback.

import { describe, expect } from "bun:test"
import { Cause, Effect, Exit } from "effect"
import path from "path"
import { pathToFileURL } from "url"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Npm } from "@opencode-ai/core/npm"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import { Auth } from "../../src/auth"
import { Account } from "../../src/account/account"
import { RuntimeFlags } from "../../src/effect/runtime-flags"
import { Plugin } from "../../src/plugin/index"
import { UnsupportedPluginHookError } from "../../src/plugin/routes"
import { TestInstance } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { AccountTest } from "../fake/account"
import { AuthTest } from "../fake/auth"
import { NpmTest } from "../fake/npm"

const it = testEffect(
  AppNodeBuilder.build(LayerNode.group([Plugin.node, CrossSpawnSpawner.node]), [
    [Auth.node, AuthTest.empty],
    [Account.node, AccountTest.empty],
    [Npm.node, NpmTest.noop],
    [RuntimeFlags.node, RuntimeFlags.layer({ disableDefaultPlugins: true, experimentalSessionWorkers: true })],
  ]),
)

const systemHook = "experimental.chat.system.transform"

function withProject<A, E, R>(source: string, self: Effect.Effect<A, E, R>) {
  return Effect.gen(function* () {
    const test = yield* TestInstance
    const file = path.join(test.directory, "plugin.ts")
    yield* Effect.all(
      [
        Effect.promise(() => Bun.write(file, source)),
        Effect.promise(() =>
          Bun.write(
            path.join(test.directory, "opencode.json"),
            JSON.stringify(
              {
                $schema: "https://opencode.ai/config.json",
                plugin: [pathToFileURL(file).href],
              },
              null,
              2,
            ),
          ),
        ),
      ],
      { discard: true, concurrency: 2 },
    )
    return yield* self
  })
}

const triggerSystemTransform = Effect.fn("PluginHookFatalTest.triggerSystemTransform")(function* () {
  const plugin = yield* Plugin.Service
  return yield* Effect.exit(
    plugin.trigger(
      systemHook,
      {
        model: {
          providerID: ProviderV2.ID.anthropic,
          modelID: ModelV2.ID.make("claude-sonnet-4-6"),
        },
      },
      { system: [] as string[] },
    ),
  )
})

describe("configured plugin hook route", () => {
  it.instance(
    "fails startup on an unsupported invoked hook",
    () =>
      withProject(
        [
          "export default async () => ({",
          '  "chat.message": (_input, output) => {',
          '    output.parts ??= []',
          "  },",
          "})",
          "",
        ].join("\n"),
        Effect.gen(function* () {
          const exit = yield* triggerSystemTransform()
          expect(Exit.isFailure(exit)).toBe(true)
          if (!Exit.isFailure(exit)) throw new Error("expected startup to fail")
          expect(Cause.hasDies(exit.cause)).toBe(true)
          const defect = exit.cause.reasons.filter(Cause.isDieReason).at(0)?.defect
          expect(defect).toBeInstanceOf(UnsupportedPluginHookError)
          expect((defect as UnsupportedPluginHookError).hook).toBe("chat.message")
        }),
      ),
    15_000,
  )

  it.instance(
    "loads a configured plugin whose hooks are routed",
    () =>
      withProject(
        [
          "export default async () => ({",
          `  ${JSON.stringify(systemHook)}: (_input, output) => {`,
          '    output.system.push("routed")',
          "  },",
          "})",
          "",
        ].join("\n"),
        Effect.gen(function* () {
          const plugin = yield* Plugin.Service
          const system: string[] = []
          yield* plugin.trigger(
            systemHook,
            {
              model: {
                providerID: ProviderV2.ID.anthropic,
                modelID: ModelV2.ID.make("claude-sonnet-4-6"),
              },
            },
            { system },
          )
          expect(system).toEqual(["routed"])
        }),
      ),
    15_000,
  )
})
