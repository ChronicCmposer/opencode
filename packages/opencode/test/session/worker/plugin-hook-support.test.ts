// The worker hook route table is derived from the hooks the source
// actually invokes. A new `plugin.trigger` is a red test until its route is
// recorded, and an unsupported or unclassified hook is never a silent skip.
//
// Admission has two boundaries, pinned separately: startup tolerates a
// configured `unsupported` hook (it is only a defect when dispatched) but still
// rejects a hook with no route at all; invocation always fails typed.

import { describe, expect, test } from "bun:test"
import { readdirSync, readFileSync } from "node:fs"
import path from "node:path"
import {
  DECLARED_ONLY_HOOKS,
  UnclassifiedPluginHookError,
  UnsupportedPluginHookError,
  WORKER_HOOK_ROUTES,
  assertPluginHookAdmission,
  assertPluginHookStartup,
  isDeclaredOnlyHook,
  pluginHookAdmissionError,
  pluginHookStartupError,
  workerHookRoute,
} from "@/plugin/routes"

const SRC_ROOT = path.resolve(import.meta.dir, "../../../src")
const TRIGGER = /\.trigger(?:<[^>]*>)?\(\s*"([^"]+)"/g

function sourceFiles(dir: string): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const full = path.join(dir, entry.name)
    if (entry.isDirectory()) return sourceFiles(full)
    return entry.isFile() && entry.name.endsWith(".ts") ? [full] : []
  })
}

/** Every hook name a `plugin.trigger(...)` invocation in `src` uses. */
function invokedHooks(): string[] {
  const names = new Set<string>()
  for (const file of sourceFiles(SRC_ROOT)) {
    for (const match of readFileSync(file, "utf8").matchAll(TRIGGER)) names.add(match[1]!)
  }
  return [...names].sort()
}

const invoked = invokedHooks()

describe("worker hook route table", () => {
  test("covers exactly the 14 actually invoked hooks", () => {
    expect(invoked).toEqual([
      "chat.headers",
      "chat.message",
      "chat.params",
      "command.execute.before",
      "experimental.chat.messages.transform",
      "experimental.chat.system.transform",
      "experimental.compaction.autocontinue",
      "experimental.provider.small_model",
      "experimental.session.compacting",
      "experimental.text.complete",
      "shell.env",
      "tool.definition",
      "tool.execute.after",
      "tool.execute.before",
    ])
    expect(Object.keys(WORKER_HOOK_ROUTES).sort()).toEqual(invoked)
  })

  test("every invoked hook has a route", () => {
    expect(invoked.filter((name) => workerHookRoute(name) === undefined)).toEqual([])
  })

  test("routes are concrete and the known gaps stay explicit", () => {
    expect(WORKER_HOOK_ROUTES["chat.params"]).toBe("worker-hosted")
    expect(WORKER_HOOK_ROUTES["chat.headers"]).toBe("worker-hosted")
    expect(WORKER_HOOK_ROUTES["tool.execute.before"]).toBe("hub-proxy")
    expect(WORKER_HOOK_ROUTES["tool.execute.after"]).toBe("hub-proxy")
    // D19: the shell tool and the tool registry invoke these in a worker, but the
    // plugin instance lives on the hub, so they are hub-routed, never local.
    expect(WORKER_HOOK_ROUTES["shell.env"]).toBe("hub-proxy")
    expect(WORKER_HOOK_ROUTES["tool.definition"]).toBe("hub-proxy")
    expect(WORKER_HOOK_ROUTES["experimental.chat.system.transform"]).toBe("hub-proxy")
    expect(WORKER_HOOK_ROUTES["experimental.chat.messages.transform"]).toBe("hub-proxy")
    // The worker prompt path invokes this on every message; the hub owns the
    // plugin instance, so the trigger is proxied rather than rejected.
    expect(WORKER_HOOK_ROUTES["chat.message"]).toBe("hub-proxy")
    // The hub resolves the small model before a worker exists; the worker never
    // dispatches this trigger.
    expect(WORKER_HOOK_ROUTES["experimental.provider.small_model"]).toBe("hub-proxy")

    for (const name of [
      "command.execute.before",
      "experimental.session.compacting",
      "experimental.compaction.autocontinue",
      "experimental.text.complete",
    ] as const) {
      expect(WORKER_HOOK_ROUTES[name]).toBe("unsupported")
    }
  })

  test("permission.ask is declared-only and outside the invoked table", () => {
    expect(DECLARED_ONLY_HOOKS).toEqual(["permission.ask"])
    expect(isDeclaredOnlyHook("permission.ask")).toBe(true)
    expect(workerHookRoute("permission.ask")).toBeUndefined()
    expect(pluginHookAdmissionError("declared", { "permission.ask": async () => {} })).toBeUndefined()
  })

  test("an unsupported invoked hook is a typed admission failure", () => {
    const error = pluginHookAdmissionError("configured", { "command.execute.before": async () => {} })
    expect(error).toBeInstanceOf(UnsupportedPluginHookError)
    expect(error?.hook).toBe("command.execute.before")
    expect(pluginHookAdmissionError("configured", { "chat.message": async () => {} })).toBeUndefined()
  })

  test("startup tolerates a configured unsupported hook", () => {
    const hooks = { "command.execute.before": async () => {} }
    expect(pluginHookStartupError("configured", hooks)).toBeUndefined()
    expect(() => assertPluginHookStartup("configured", hooks)).not.toThrow()
    expect(pluginHookStartupError("configured", { "chat.message": async () => {} })).toBeUndefined()
  })

  test("startup still rejects an unclassified hook", () => {
    const hooks = { "future.trigger": async () => {} }
    const error = pluginHookStartupError("dynamic", hooks)
    expect(error).toBeInstanceOf(UnclassifiedPluginHookError)
    expect(error?.hook).toBe("future.trigger")
    expect(() => assertPluginHookStartup("dynamic", hooks)).toThrow(UnclassifiedPluginHookError)
  })

  test("an unclassified function hook throws instead of falling through", () => {
    const error = pluginHookAdmissionError("dynamic", { "future.trigger": async () => {} })
    expect(error).toBeInstanceOf(UnclassifiedPluginHookError)
    expect(error?.hook).toBe("future.trigger")
    expect(() => assertPluginHookAdmission("dynamic", { "future.trigger": async () => {} })).toThrow(
      UnclassifiedPluginHookError,
    )
  })

  test("non-trigger hooks are not admission failures", () => {
    expect(
      pluginHookAdmissionError("plugin", {
        event: async () => {},
        config: async () => {},
        dispose: async () => {},
        tool: {},
        auth: {},
        provider: {},
      }),
    ).toBeUndefined()
  })
})
