import { describe, expect, test } from "bun:test"
import { Effect, Exit, Layer } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Config } from "@opencode-ai/core/config"
import { Global } from "@opencode-ai/core/global"
import { Location } from "@opencode-ai/core/location"
import { PluginV2 } from "@opencode-ai/core/plugin"
import { referenceOnlyNode } from "@opencode-ai/core/plugin/internal"
import { Reference } from "@opencode-ai/core/reference"
import { tempLocationLayer } from "./fixture/location"
import { testEffect } from "./lib/effect"

const referenceOnlyLayer = AppNodeBuilder.build(
  LayerNode.group([PluginV2.node, referenceOnlyNode]),
  [[Location.node, tempLocationLayer]],
)

const it = testEffect(referenceOnlyLayer)

describe("plugin reference-only boot", () => {
  it.effect("activates core/config-reference", () =>
    Effect.gen(function* () {
      const plugins = yield* PluginV2.Service
      yield* plugins.wait(PluginV2.ID.make("core/config-reference"))
    }),
  )

  test("fails layer construction when a plugin add fails instead of hanging", async () => {
    const failingAdd = Layer.succeed(
      PluginV2.Service,
      PluginV2.Service.of({
        add: () => Effect.die("plugin add failed"),
        remove: () => Effect.void,
        wait: () => Effect.void,
      }),
    )
    const layer = AppNodeBuilder.build(
      LayerNode.group([referenceOnlyNode]),
      [
        [Location.node, tempLocationLayer],
        [PluginV2.node, failingAdd],
        [Config.node, Layer.succeed(Config.Service, Config.Service.of({ entries: () => Effect.succeed([]) }))],
        [Global.node, Layer.succeed(Global.Service, Global.Service.of(Global.make()))],
        [
          Reference.node,
          Layer.succeed(
            Reference.Service,
            Reference.Service.of({
              transform: () => Effect.succeed({ dispose: Effect.void }),
              reload: () => Effect.void,
              list: () => Effect.succeed([]),
            }),
          ),
        ],
      ],
    )

    const exit = await Effect.runPromise(
      Effect.scoped(Layer.build(layer)).pipe(Effect.timeout("5 seconds"), Effect.exit),
    )

    expect(Exit.isFailure(exit)).toBe(true)
  })
})
