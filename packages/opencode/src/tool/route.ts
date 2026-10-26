// The single tool-routing decision point.
//
// A session worker runs the session pipeline but owns no MCP/LSP child
// processes and no hub plugin tool definitions. Those tools execute on the hub.
// Every consumer asks this module which host a tool belongs to; no other module
// grows its own port/client/layer branch for that decision.

import { processRole } from "@/effect/process-role"

/** Where a tool's implementation runs. */
export type ToolHost = "worker" | "hub"

/**
 * The closed set of tool sources. A source is a property of how the tool was
 * registered, not of its id: MCP ids are arbitrary, so guessing from the id
 * would misroute a plugin that happens to be named after an MCP tool.
 */
export type ToolSource = "builtin" | "file" | "plugin" | "remote-plugin" | "mcp" | "lsp"

export interface ToolRef {
  readonly id: string
  readonly source: ToolSource
}

export interface ToolRoute extends ToolRef {
  readonly host: ToolHost
}

/** Sources whose implementation only the hub is allowed to run. */
const HUB_SOURCES: ReadonlySet<ToolSource> = new Set(["mcp", "lsp", "remote-plugin"])

/** The host that owns tools from `source`. */
export function hostForSource(source: ToolSource): ToolHost {
  return HUB_SOURCES.has(source) ? "hub" : "worker"
}

/**
 * The current isolate's tool host. Only a declared `session` role is a worker;
 * the hub (and any test harness that has not declared a role) is the hub, which
 * is the process that actually owns MCP/LSP.
 */
export function hostForRole(role: string | undefined = processRole()): ToolHost {
  return role === "session" ? "worker" : "hub"
}

/** The canonical route for one tool reference. */
export function routeTool(ref: ToolRef): ToolRoute {
  return { id: ref.id, source: ref.source, host: hostForSource(ref.source) }
}

/**
 * Whether `route` may execute in `host` (the single availability predicate).
 *
 * The hub is a superset: it owns MCP/LSP/remote tools *and* can run the
 * worker-local built-ins (v1 runs tools in-process). A worker may only run
 * worker-sourced tools, so a hub-sourced tool is never silently executed there
 * with an empty boundary implementation.
 */
export function runsOn(route: ToolRoute, host: ToolHost = hostForRole()): boolean {
  return host === "hub" || route.host === "worker"
}

/**
 * Source of a registry-provided tool. Registry built-ins are worker-local;
 * `lsp` is the one built-in backed by a hub-owned child process. Remote plugin
 * tools are registered as forwarding stubs, so they stay worker-local.
 */
export function sourceForRegistryTool(id: string): ToolSource {
  return id === "lsp" ? "lsp" : "builtin"
}

export * as ToolRoute from "./route"
