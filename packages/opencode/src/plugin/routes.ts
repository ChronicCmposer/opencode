// D19 — the worker hook route table.
//
// The reduced worker graph calls `plugin.trigger(name, ...)`. Every hook the
// source actually invokes is classified here, and a configured plugin that
// declares a hook with no worker route is fatal at startup instead of silently
// passing through. `permission.ask` is declared-only: plugin declarations may
// carry it, but the worker graph never dispatches it as a trigger.
//
// `hub-proxy` covers a hook the hub owns: either the worker forwards the trigger
// over the control port, or the hook is invoked hub-side before a worker exists
// (`experimental.provider.small_model`).

import { Schema } from "effect"

/**
 * How a hook is served for a worker:
 * - `worker-hosted`: resolved on the hub and applied/run in the worker.
 * - `hub-proxy`: owned by the hub; forwarded from the worker or never dispatched
 *   by the worker at all.
 * - `unsupported`: the worker dispatches the trigger but has no route; declaring
 *   it is fatal rather than a silent skip.
 */
export type WorkerHookRoute = "worker-hosted" | "hub-proxy" | "unsupported"

/** The 14 hooks `plugin.trigger(...)` actually invokes in `src`. */
export const WORKER_HOOK_ROUTES = {
  "chat.params": "worker-hosted",
  "chat.headers": "worker-hosted",
  "tool.execute.before": "hub-proxy",
  "tool.execute.after": "hub-proxy",
  "experimental.chat.system.transform": "hub-proxy",
  "experimental.chat.messages.transform": "hub-proxy",
  "experimental.provider.small_model": "hub-proxy",
  // The worker's shell tool and tool registry dispatch these, but the real
  // plugin instance lives on the hub, so they are forwarded over the control
  // port (D19: never invoked in a worker, never silently skipped).
  "shell.env": "hub-proxy",
  "tool.definition": "hub-proxy",
  // The worker prompt path invokes this on every user message. The plugin
  // instance lives on the hub, so the trigger is forwarded over the control
  // port and its returned `{message, parts}` is reconciled in place.
  "chat.message": "hub-proxy",
  // Slash commands, compaction and text completion run in the worker; their
  // plugins live on the hub, so each is forwarded and its output applied back.
  "command.execute.before": "hub-proxy",
  "experimental.session.compacting": "hub-proxy",
  "experimental.compaction.autocontinue": "hub-proxy",
  "experimental.text.complete": "hub-proxy",
} as const satisfies Readonly<Record<string, WorkerHookRoute>>

export type WorkerHookName = keyof typeof WORKER_HOOK_ROUTES

/** Trigger-shaped hooks a plugin may declare that the worker never dispatches. */
export const DECLARED_ONLY_HOOKS = ["permission.ask"] as const

const declaredOnlyHooks: ReadonlySet<string> = new Set(DECLARED_ONLY_HOOKS)

/** `Hooks` members dispatched by a mechanism other than `plugin.trigger`. */
const nonTriggerHooks: ReadonlySet<string> = new Set(["auth", "config", "dispose", "event", "provider", "tool"])

/** Raised when a configured plugin declares a worker trigger marked unsupported. */
export class UnsupportedPluginHookError extends Schema.TaggedErrorClass<UnsupportedPluginHookError>()(
  "UnsupportedPluginHookError",
  {
    plugin: Schema.String,
    hook: Schema.String,
  },
) {
  override get message() {
    return `plugin "${this.plugin}" declares unsupported worker hook "${this.hook}"; there is no silent skip or fallback`
  }
}

/** Raised when a function-shaped hook has no route entry at all. */
export class UnclassifiedPluginHookError extends Schema.TaggedErrorClass<UnclassifiedPluginHookError>()(
  "UnclassifiedPluginHookError",
  {
    plugin: Schema.String,
    hook: Schema.String,
  },
) {
  override get message() {
    return `plugin "${this.plugin}" declares hook "${this.hook}" with no worker route; classify it in WORKER_HOOK_ROUTES`
  }
}

/** The route for `name`, or `undefined` when it is not an invoked hook. */
export function workerHookRoute(name: string): WorkerHookRoute | undefined {
  return Object.hasOwn(WORKER_HOOK_ROUTES, name) ? WORKER_HOOK_ROUTES[name as WorkerHookName] : undefined
}

export function isDeclaredOnlyHook(name: string): boolean {
  return declaredOnlyHooks.has(name)
}

/** Whether `name` is served by the hub and forwarded over the control port. */
export function isHubProxyHook(name: string): boolean {
  return workerHookRoute(name) === "hub-proxy"
}

/**
 * The first admission problem in one plugin's hooks, if any. Function-valued
 * hooks are triggers; object-valued members (`tool`, `auth`, `provider`) and the
 * non-trigger lifecycle hooks are skipped so only dispatchable hooks are judged.
 */
export function pluginHookAdmissionError(
  plugin: string,
  hooks: object,
): UnsupportedPluginHookError | UnclassifiedPluginHookError | undefined {
  for (const [name, value] of Object.entries(hooks)) {
    if (typeof value !== "function") continue
    if (nonTriggerHooks.has(name) || isDeclaredOnlyHook(name)) continue
    const route = workerHookRoute(name)
    if (route === undefined) return new UnclassifiedPluginHookError({ plugin, hook: name })
    if (route === "unsupported") return new UnsupportedPluginHookError({ plugin, hook: name })
  }
  return undefined
}

/** Fail loud on the first unsupported or unclassified hook. */
export function assertPluginHookAdmission(plugin: string, hooks: object): void {
  const error = pluginHookAdmissionError(plugin, hooks)
  if (error !== undefined) throw error
}

/**
 * Startup admission: a configured hook that the worker *can* dispatch but has no
 * route for (the `unsupported` route) must not fail startup. It is only a defect
 * when the worker actually invokes it *and a loaded plugin declared it*;
 * `runWorkerPluginHook` fails typed at that point (`layer.ts`), while an
 * undeclared hook is a true no-op. A hook with no route entry at all can never be
 * served by any invocation path, so it stays a startup configuration defect.
 */
export function pluginHookStartupError(
  plugin: string,
  hooks: object,
): UnsupportedPluginHookError | UnclassifiedPluginHookError | undefined {
  const error = pluginHookAdmissionError(plugin, hooks)
  return error instanceof UnsupportedPluginHookError ? undefined : error
}

/** Fail loud at startup only on a hook that no route can ever serve. */
export function assertPluginHookStartup(plugin: string, hooks: object): void {
  const error = pluginHookStartupError(plugin, hooks)
  if (error !== undefined) throw error
}

export * as PluginRoutes from "./routes"
