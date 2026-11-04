import { Effect, type Scope } from "effect"
import type { PluginContext } from "@opencode-ai/plugin/v2/effect"
import { PluginV2 } from "../plugin"
import type { Plugin } from "./internal"

export type PluginProvider<R> = (
  effect: Effect.Effect<void, never, R | Scope.Scope>,
) => Effect.Effect<void, never, Scope.Scope>

/** The single add/ID path shared by the full boot and the reference-only boot. */
export const registerPlugin = <R>(plugin: Plugin<R>, provide: PluginProvider<R>) =>
  Effect.gen(function* () {
    const service = yield* PluginV2.Service
    yield* service.add(
      PluginV2.ID.make(plugin.id),
      (context: PluginContext) => provide(plugin.effect(context)),
    )
  })
