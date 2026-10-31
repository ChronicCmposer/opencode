import { afterEach, describe, expect } from "bun:test"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2 } from "@opencode-ai/core/event"
import { Effect, Layer } from "effect"
import path from "path"
import { pathToFileURL } from "url"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Config } from "@/config/config"
import { InstanceRef } from "@/effect/instance-ref"
import { EventV2Bridge } from "../../src/event-v2-bridge"
import { disposeAllInstances, provideInstance, testInstanceStoreLayer, tmpdirScoped } from "../fixture/fixture"
import { CountingLazyJson } from "../lib/counting-lazy-json"
import { testEffect } from "../lib/effect"
const { Plugin } = await import("../../src/plugin/index")
const { TestConfig } = await import("../fixture/config")
const { RuntimeFlags } = await import("../../src/effect/runtime-flags")

afterEach(async () => {
  await disposeAllInstances()
})

const it = testEffect(
  Layer.mergeAll(LayerNode.compile(LayerNode.group([CrossSpawnSpawner.node, FSUtil.node])), testInstanceStoreLayer),
)

const received: Array<{ name: string; properties: Record<string, unknown> }> = []
Object.assign(globalThis, { __pluginLazyEvents: received })

const recorder = (name: string, mutate: boolean) =>
  [
    "export default async () => ({",
    "  event: async ({ event }) => {",
    `    if (event.type !== "test.lazy") return`,
    `    globalThis.__pluginLazyEvents.push({ name: ${JSON.stringify(name)}, properties: event.properties })`,
    mutate ? `    event.properties.mutated = true` : "",
    "  },",
    "})",
    "",
  ].join("\n")

describe("plugin event hook with LazyJson data", () => {
  it.live("plugins share one parsed object and a mutation does not reach the raw bytes", () =>
    Effect.gen(function* () {
      received.length = 0
      const dir = yield* tmpdirScoped()
      const first = path.join(dir, "first.ts")
      const second = path.join(dir, "second.ts")
      yield* Effect.promise(() => Bun.write(first, recorder("first", true)))
      yield* Effect.promise(() => Bun.write(second, recorder("second", false)))
      const plugins = [pathToFileURL(first).href, pathToFileURL(second).href]

      const lazy = new CountingLazyJson({ hello: "world", n: [1, 2] })
      const rawBefore = lazy.text
      const graph = LayerNode.compile(LayerNode.group([Plugin.node, EventV2Bridge.node]), [
        [
          Config.node,
          TestConfig.layer({
            get: () =>
              Effect.succeed({
                plugin: plugins,
                plugin_origins: plugins.map((spec) => ({
                  spec,
                  source: path.join(dir, "opencode.json"),
                  scope: "local" as const,
                })),
              }),
            directories: () => Effect.succeed([dir]),
          }),
        ],
        [RuntimeFlags.node, RuntimeFlags.layer({ disableDefaultPlugins: true })],
      ])

      yield* Effect.gen(function* () {
        const plugin = yield* Plugin.Service
        yield* plugin.list()
        const events = yield* EventV2Bridge.Service
        const ctx = yield* InstanceRef
        yield* events.publishLive({
          id: EventV2.ID.create(),
          type: "test.lazy",
          data: lazy,
          location: EventV2Bridge.instanceLocation(ctx),
        })
      }).pipe(Effect.provide(graph), provideInstance(dir))

      expect(received.map((item) => item.name)).toEqual(["first", "second"])
      expect(received[0].properties).toBe(received[1].properties)
      expect(received[0].properties).toMatchObject({ hello: "world", n: [1, 2] })
      expect(lazy.reads).toBe(1)
      expect(lazy.text).toBe(rawBefore)
      expect(JSON.parse(lazy.text)).not.toHaveProperty("mutated")
    }),
  )
})
