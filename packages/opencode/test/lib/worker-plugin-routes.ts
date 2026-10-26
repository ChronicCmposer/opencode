// PENDING-plugin (W4.1) — test-side plugin route pass-through.
//
// `WORKER_HOOK_ROUTES` currently classifies `tool.definition` and
// `experimental.session.compacting` as unsupported, although the real
// `ToolRegistry`/compaction paths always trigger them. The fatal-admission
// contract is covered by `test/lib/rollout-matrix.test.ts` and
// `test/session/parity/flag-matrix.test.ts`; worker suites that exercise prompt
// turns need the six dispatched hooks to reach their supplied host while the
// un-routed triggers pass through unchanged.
//
// Remove this helper (and its uses) when `src/plugin/routes.ts` gains routes for
// the remaining invoked hooks.

import { Effect, Layer } from "effect"
import { Plugin } from "@/plugin"
import type { LayerNode } from "@opencode-ai/core/effect/layer-node"
import type { WorkerPluginHost } from "@/session/worker/plugin-host"
import { assertPluginToolDescriptorsUnique, type RemotePluginTools } from "@/session/worker/layer"
import { remotePluginToolHooks } from "@/session/worker/plugin-tool-wire"
import type { Hooks } from "@opencode-ai/plugin"

/** Accept every trigger the reduced graph dispatches; delegate the six hosted hooks. */
export function pluginPassThroughLayer(
  host?: WorkerPluginHost,
  pluginTools?: RemotePluginTools,
): Layer.Layer<Plugin.Service> {
  const trigger = ((name: string, input: unknown, output: unknown) => {
    if (host === undefined) return Effect.succeed(output)
    const run = (method: Exclude<keyof WorkerPluginHost, "names">) => {
      const fn = host[method] as (input: unknown, output: unknown) => Promise<unknown>
      return Effect.promise(() => fn(input, output))
    }
    switch (name) {
      case "chat.params":
        return run("chatParams")
      case "chat.headers":
        return run("chatHeaders")
      case "tool.execute.before":
        return run("toolBefore")
      case "tool.execute.after":
        return run("toolAfter")
      case "experimental.chat.system.transform":
        return run("systemTransform")
      case "experimental.chat.messages.transform":
        return run("messagesTransform")
      default:
        return Effect.succeed(output)
    }
  }) as Plugin.Interface["trigger"]

  const hooks = (): Hooks[] => {
    if (pluginTools === undefined) return []
    const descriptors = assertPluginToolDescriptorsUnique(pluginTools.descriptors)
    if (descriptors.length === 0) return []
    const callbacks = {
      ...(pluginTools.abort === undefined ? {} : { abort: pluginTools.abort }),
      ...(pluginTools.refreshFromHub === undefined ? {} : { refresh: pluginTools.refreshFromHub }),
    }
    return [remotePluginToolHooks(descriptors, pluginTools.executor, callbacks) as unknown as Hooks]
  }

  return Layer.succeed(Plugin.Service, {
    trigger,
    list: () => Effect.succeed(hooks()),
    init: () => Effect.void,
  })
}

/** The replacement tuple every worker-graph test helper can append. */
export function pluginPassThroughReplacement(
  host?: WorkerPluginHost,
  pluginTools?: RemotePluginTools,
): LayerNode.Replacement {
  return [Plugin.node, pluginPassThroughLayer(host, pluginTools)]
}
