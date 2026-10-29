// A configured plugin that merely *declares* a routed-but-
// unsupported hook must not fail startup; the hook fails typed only when the
// worker actually invokes it. A hook with no route at all stays a startup defect.

import { describe, expect, test } from "bun:test"
import { Cause, Effect, Exit, Layer } from "effect"
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
import { ModelClient, buildSessionWorkerLayer } from "../../src/session/worker/layer"
import { createWorkerPluginHost } from "../../src/session/worker/plugin-host"
import { TestInstance } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { workerSnapshot } from "../lib/snapshot"
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

const triggerSystemTransform = Effect.fn("PluginHookStartupTest.triggerSystemTransform")(function* () {
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

/** Build the reduced worker plugin layer and trigger a hook through it. */
async function triggerWorkerHook(name: string): Promise<Exit.Exit<unknown, unknown>> {
  const built = buildSessionWorkerLayer({
    snapshot: workerSnapshot(),
    env: { values: {} },
    clients: {
      writer: { query: async () => [], publish: async () => ({ seq: 1, committed: true }) },
      egress: { emit: () => {} },
      backgroundJob: { wait: async () => null, cancel: async () => {} },
    },
    modelClient: Layer.succeed(ModelClient, { complete: () => Effect.succeed({ text: "stub" }) }),
    // The hook is declared by a loaded plugin, so the worker must fail typed
    // rather than silently pass the trigger through.
    plugins: createWorkerPluginHost([{ name: "empty", hooks: {}, declared: [name] }]),
  })
  return Effect.runPromise(
    Effect.gen(function* () {
      const plugin = (yield* Plugin.Service) as Plugin.Interface
      return yield* plugin.trigger(name as never, {} as never, { env: {} })
    }).pipe(Effect.provide(built.layer), Effect.exit),
  )
}

describe("configured plugin hook route", () => {
  it.instance(
    "loads a configured plugin declaring an uninvoked unsupported hook",
    () =>
      withProject(
        [
          "export default async () => ({",
          '  "command.execute.before": (_input, output) => {',
          '    output.parts ??= []',
          "  },",
          "})",
          "",
        ].join("\n"),
        Effect.gen(function* () {
          // Startup must succeed: the plugin loads and a routed trigger works.
          const exit = yield* triggerSystemTransform()
          expect(Exit.isSuccess(exit)).toBe(true)
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

describe("worker invocation of an unsupported hook", () => {
  test("the worker dispatches a configured command hook instead of failing", async () => {
    const exit = await triggerWorkerHook("command.execute.before")
    if (Exit.isFailure(exit)) throw new Error(Cause.pretty(exit.cause))
  })
})
