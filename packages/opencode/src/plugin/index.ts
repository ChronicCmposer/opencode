import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import type { Hooks } from "@opencode-ai/plugin"
import { Config } from "@/config/config"
import { Effect, Layer, Context } from "effect"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { EventV2Bridge } from "@/event-v2-bridge"
import { InstallationChannel } from "@opencode-ai/core/installation/version"

// Hook names that follow the (input, output) => Promise<void> trigger pattern
export type TriggerName = {
  [K in keyof Hooks]-?: NonNullable<Hooks[K]> extends (input: any, output: any) => Promise<void> ? K : never
}[keyof Hooks]

export interface Interface {
  readonly trigger: <
    Name extends TriggerName,
    Input = Parameters<Required<Hooks>[Name]>[0],
    Output = Parameters<Required<Hooks>[Name]>[1],
  >(
    name: Name,
    input: Input,
    output: Output,
  ) => Effect.Effect<Output>
  readonly list: () => Effect.Effect<Hooks[]>
  readonly init: () => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/Plugin") {}

export function experimentalWebSocketsEnabled(input: { enabled: boolean; channel?: string }) {
  return input.enabled || ["local", "dev", "beta"].includes(input.channel ?? InstallationChannel)
}

// The plugin implementation drags in the server graph, built-in auth plugins, and
// npm plugin loading. Keep this entry light: the heavy layer is only imported when
// the node is actually built.
export const node = LayerNode.make({
  service: Service,
  layer: Layer.unwrap(Effect.promise(() => import("./effect-layer").then((m) => m.effectLayer))),
  deps: [EventV2Bridge.node, Config.node, RuntimeFlags.node],
})

export * as Plugin from "."
