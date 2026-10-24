import { Layer, ManagedRuntime } from "effect"
import { attach } from "./run-service"
import * as Observability from "@opencode-ai/core/observability"

import { Ripgrep } from "@opencode-ai/core/ripgrep"
import { memoMap } from "@opencode-ai/core/effect/memo-map"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { AppNodeBuilderV1 } from "./app-node-builder-v1"
import { appServiceNodes } from "./app-graph"

export const AppLayer = AppNodeBuilderV1.build(LayerNode.group(appServiceNodes())).pipe(
  Layer.provideMerge(AppNodeBuilderV1.build(Ripgrep.node)),
  Layer.provideMerge(Observability.layer),
)

const rt = ManagedRuntime.make(AppLayer, { memoMap })
type Runtime = Pick<typeof rt, "runSync" | "runPromise" | "runPromiseExit" | "runFork" | "runCallback" | "dispose">

/** Services provided by AppRuntime — i.e. what an Effect run via AppRuntime.runPromise can yield. */
export type AppServices = ManagedRuntime.ManagedRuntime.Services<typeof rt>
const wrap = (effect: Parameters<typeof rt.runSync>[0]) => attach(effect as never) as never

export const AppRuntime: Runtime = {
  runSync(effect) {
    return rt.runSync(wrap(effect))
  },
  runPromise(effect, options) {
    return rt.runPromise(wrap(effect), options)
  },
  runPromiseExit(effect, options) {
    return rt.runPromiseExit(wrap(effect), options)
  },
  runFork(effect) {
    return rt.runFork(wrap(effect))
  },
  runCallback(effect) {
    return rt.runCallback(wrap(effect))
  },
  dispose: () => rt.dispose(),
}
