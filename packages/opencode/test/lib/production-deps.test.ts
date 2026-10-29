import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { ModelsDev } from "@opencode-ai/core/models-dev"
import { Auth } from "@/auth"
import { Config } from "@/config/config"
import { EventV2Bridge } from "@/event-v2-bridge"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Permission } from "@/permission"
import { Provider } from "@/provider/provider"
import { Question } from "@/question"
import { SessionStatus } from "@/session/status"
import { RunBarrier } from "@/session/run-barrier"
import { productionServiceReplacements } from "./production-deps"

/** The ten service deps `productionSessionHubNode()` declares, in order. */
const DECLARED_DEP_NAMES = [
  "@opencode/Config",
  "@opencode/Auth",
  "@opencode/ModelsDev",
  "@opencode/Provider",
  "@opencode/RuntimeFlags",
  "@opencode/SessionStatus",
  "@opencode/Permission",
  "@opencode/Question",
  "@opencode/EventV2Bridge",
  "@opencode/RunBarrier",
] as const

/** The same ten nodes, so the replacement list can be exercised as a layer. */
const DECLARED_NODES = [
  Config.node,
  Auth.node,
  ModelsDev.node,
  Provider.node,
  RuntimeFlags.node,
  SessionStatus.node,
  Permission.node,
  Question.node,
  EventV2Bridge.node,
  RunBarrier.node,
]

/** Build a runtime graph over the declared deps, using the shared replacements. */
const buildReplacementsLayer = (replacements: LayerNode.Replacements) =>
  AppNodeBuilder.build(LayerNode.group(DECLARED_NODES), replacements)

describe("productionServiceReplacements", () => {
  test("covers every declared production-session-hub service dep", () => {
    const names = productionServiceReplacements().map(([node]) => node.name)
    expect(names).toEqual([...DECLARED_DEP_NAMES])
  })

  test("defaults are dependency-free fixtures (no filesystem/network)", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const config = yield* Config.Service
        const auth = yield* Auth.Service
        const models = yield* ModelsDev.Service
        const provider = yield* Provider.Service
        return {
          config: yield* config.get(),
          auth: yield* auth.get("test"),
          models: yield* models.get(),
          provider: yield* provider.defaultModel(),
        }
      }).pipe(Effect.provide(buildReplacementsLayer(productionServiceReplacements()))),
    )

    expect(result.config).toEqual({ model: "test/model" })
    expect(result.auth).toEqual(new Auth.Api({ type: "api", key: "sk-test" }))
    expect(result.models).toEqual({})
    expect(String(result.provider.providerID)).toBe("test")
    expect(String(result.provider.modelID)).toBe("model")
  })

  test("a caller override replaces the default method", async () => {
    const layer = buildReplacementsLayer(
      productionServiceReplacements({
        config: { get: () => Effect.succeed({ model: "override/model" }) },
      }),
    )

    const config = await Effect.runPromise(
      Effect.gen(function* () {
        const service = yield* Config.Service
        return yield* service.get()
      }).pipe(Effect.provide(layer)),
    )

    expect(config).toEqual({ model: "override/model" })
  })
})
