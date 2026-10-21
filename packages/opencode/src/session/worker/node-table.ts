// Phase 6.3 / 6.5 — session-role node table (plan decision lines 58, 152).
//
// The real `AppLayer` (`src/effect/app-runtime.ts`) composes ~47 nodes in one
// flat group. Session workers must run a *reduced* graph: they own the session
// pipeline, but database connections, OS children and process-global state stay
// on the hub/writer/broker. This module declares that split as data and derives
// pure validators, so the reduced layer can be checked mechanically instead of
// by inspection.
//
// The table is keyed by the node's service key (`LayerNode.Node.name`). Some
// nodes (e.g. `session-projector`) use an explicit `name`; both are covered.

import { LayerNode } from "@opencode-ai/core/effect/layer-node"
export type Role = "session" | "writer" | "broker" | "hub"

/**
 * Where a node's side effects must live.
 *
 * - `worker-local` — safe to instantiate inside a session worker.
 * - `broker-proxy` — a client that forwards to the MCP/LSP broker worker.
 * - `hub-proxy` — a client that forwards to a hub-resident authority.
 * - `excluded` — must never be instantiated in a session worker.
 */
export type Boundary = "worker-local" | "broker-proxy" | "hub-proxy" | "excluded"

export interface NodeEntry {
  /** Human name used by the plan/task (e.g. `Session`, `Database`). */
  readonly label: string
  /** `LayerNode.Node.name` / service key. */
  readonly key: string
  readonly roles: readonly Role[]
  readonly boundary: Boundary
  readonly note?: string
}

const entry = (
  label: string,
  key: string,
  roles: readonly Role[],
  boundary: Boundary,
  note?: string,
): NodeEntry => ({ label, key, roles, boundary, ...(note === undefined ? {} : { note }) })

/** The approved session-role allowlist (task list, plan line 58). */
export const SESSION_ALLOWLIST: readonly NodeEntry[] = [
  entry("Session", "@opencode/Session", ["session"], "worker-local"),
  entry("SessionPrompt", "@opencode/SessionPrompt", ["session"], "worker-local"),
  entry("SessionProcessor", "@opencode/SessionProcessor", ["session"], "worker-local"),
  entry("SessionRunState", "@opencode/SessionRunState", ["session"], "worker-local"),
  entry("LLM", "@opencode/LLM", ["session"], "worker-local"),
  entry("Plugin", "@opencode/Plugin", ["session"], "worker-local", "worker-hosted hooks only"),
  entry("ToolRegistry", "@opencode/ToolRegistry", ["session"], "worker-local"),
  entry("Config", "@opencode/Config", ["session"], "worker-local", "hub snapshot-backed"),
  entry("Auth", "@opencode/Auth", ["session"], "worker-local", "hub snapshot-backed"),
  entry("Provider", "@opencode/Provider", ["session"], "worker-local", "hub snapshot-backed"),
  entry("Agent", "@opencode/Agent", ["session"], "worker-local"),
  entry("Skill", "@opencode/Skill", ["session"], "worker-local"),
  entry("Discovery", "@opencode/SkillDiscovery", ["session"], "worker-local"),
  entry("Question", "@opencode/Question", ["session"], "hub-proxy", "hub-resident authority"),
  entry("Permission", "@opencode/Permission", ["session"], "hub-proxy", "hub-resident authority"),
  entry("Todo", "@opencode/SessionTodo", ["session"], "worker-local"),
  entry("Truncate", "@opencode/Truncate", ["session"], "worker-local"),
  entry("Format", "@opencode/Format", ["session"], "worker-local"),
  entry("RuntimeFlags", "@opencode/RuntimeFlags", ["session"], "worker-local"),
  entry("EventEgressClient", "@opencode/session-worker/EventEgressClient", ["session"], "worker-local"),
  entry("WriterClient", "@opencode/session-worker/WriterClient", ["session"], "worker-local"),
  entry("BrokerClient", "@opencode/session-worker/BrokerClient", ["session"], "broker-proxy"),
  entry("BackgroundJobClient", "@opencode/session-worker/BackgroundJobClient", ["session"], "hub-proxy"),
  entry("SessionPipeline", "@opencode/session-worker/SessionPipeline", ["session"], "worker-local"),
  entry("ModelClient", "@opencode/session-worker/ModelClient", ["session"], "worker-local"),
  entry("WorkerSnapshot", "@opencode/session-worker/Snapshot", ["session"], "worker-local"),
  entry("WorkerEnv", "@opencode/session-worker/Env", ["session"], "worker-local"),
  entry("Observability", "observability", ["session"], "worker-local"),
]

/** Nodes that must NEVER be instantiated inside a session worker (task exclusions). */
export const SESSION_EXCLUDED: readonly NodeEntry[] = [
  entry("Database", "@opencode/v2/storage/Database", ["hub", "writer"], "excluded", "writer owns the sole connection"),
  entry("SessionProjector", "session-projector", ["writer"], "excluded", "durable projectors run in the writer isolate"),
  entry("LSP", "@opencode/LSP", ["broker"], "excluded", "broker-proxy only"),
  entry("MCP", "@opencode/MCP", ["broker"], "excluded", "broker-proxy only"),
  entry("McpAuth", "@opencode/McpAuth", ["broker"], "excluded", "broker-proxy only"),
  entry("Workspace", "@opencode/Workspace", ["hub"], "excluded"),
  entry("Worktree", "@opencode/Worktree", ["hub"], "excluded"),
  entry("Vcs", "@opencode/Vcs", ["hub"], "excluded"),
  entry("Installation", "@opencode/Installation", ["hub"], "excluded"),
  entry("Npm", "@opencode/Npm", ["hub"], "excluded", "no npm install in workers"),
  entry("ModelsDev", "@opencode/ModelsDev", ["hub"], "excluded", "hub-only catalog refresh"),
]

/** Client nodes the session layer MUST contain (task 6.5: writer RPC client present). */
export const SESSION_CLIENT_KEYS: readonly string[] = [
  "@opencode/session-worker/WriterClient",
  "@opencode/session-worker/EventEgressClient",
  "@opencode/session-worker/BrokerClient",
  "@opencode/session-worker/BackgroundJobClient",
]

const ALL_ENTRIES: readonly NodeEntry[] = [...SESSION_ALLOWLIST, ...SESSION_EXCLUDED]

const EXCLUDED_KEYS: ReadonlySet<string> = new Set(SESSION_EXCLUDED.map((item) => item.key))
const CLIENT_KEY_SET: ReadonlySet<string> = new Set(SESSION_CLIENT_KEYS)

/** Look up a table entry by node name or label. */
export function lookupNode(name: string): NodeEntry | undefined {
  return ALL_ENTRIES.find((item) => item.key === name || item.label === name)
}

/** Walk a node graph and collect every reachable node name (groups excluded). */
export function collectNodeNames(root: LayerNode.Node<unknown, unknown, any>): ReadonlySet<string> {
  const names = new Set<string>()
  const seen = new Set<LayerNode.Node<unknown, unknown, any>>()
  const visit = (node: LayerNode.Node<unknown, unknown, any>): void => {
    if (seen.has(node)) return
    seen.add(node)
    if (node.kind !== "group") names.add(node.name)
    for (const dependency of node.dependencies) visit(dependency)
  }
  visit(root)
  return names
}

/** Find a node by name in a graph (depth-first, first match). */
export function findNode(
  root: LayerNode.Node<unknown, unknown, any>,
  name: string,
): LayerNode.Node<unknown, unknown, any> | undefined {
  const seen = new Set<LayerNode.Node<unknown, unknown, any>>()
  const search = (node: LayerNode.Node<unknown, unknown, any>): LayerNode.Node<unknown, unknown, any> | undefined => {
    if (node.name === name && node.kind !== "group") return node
    if (seen.has(node)) return undefined
    seen.add(node)
    for (const dependency of node.dependencies) {
      const found = search(dependency)
      if (found !== undefined) return found
    }
    return undefined
  }
  return search(root)
}

type AnyReplacement = LayerNode.Replacements[number][1]

function isNode(input: AnyReplacement): input is LayerNode.Node<unknown, unknown, any> {
  return "kind" in input && "dependencies" in input
}

function replacementMap(replacements: LayerNode.Replacements): ReadonlyMap<string, AnyReplacement> {
  const map = new Map<string, AnyReplacement>()
  for (const [source, replacement] of replacements) map.set(source.name, replacement)
  return map
}

export interface SessionLayerReport {
  /** Excluded nodes reachable without a replacement (must be empty). */
  readonly excluded: readonly string[]
  /** Required client nodes absent from the effective graph (must be empty). */
  readonly missingClients: readonly string[]
  /** Reachable nodes that are neither allowlisted nor excluded (informational). */
  readonly unknown: readonly string[]
  readonly ok: boolean
}

/**
 * Validate a session layer graph after boundary replacements are applied.
 *
 * A node is "present" if it is reachable and either not excluded or replaced by
 * a boundary layer. Replaced nodes contribute their replacement's dependencies
 * (when the replacement is itself a node), so a replacement cannot smuggle an
 * excluded node back in unnoticed.
 */
export function validateSessionLayer(
  root: LayerNode.Node<unknown, unknown, any>,
  replacements: LayerNode.Replacements = [],
): SessionLayerReport {
  const map = replacementMap(replacements)
  const seen = new Set<LayerNode.Node<unknown, unknown, any>>()
  const excluded: string[] = []
  const clients = new Set<string>()
  const unknown = new Set<string>()

  const visit = (node: LayerNode.Node<unknown, unknown, any>): void => {
    if (seen.has(node)) return
    seen.add(node)

    if (node.kind === "group") {
      for (const dependency of node.dependencies) visit(dependency)
      return
    }

    // A client node is "present" whether it is provided directly or via a
    // boundary replacement: the service is reachable either way.
    if (CLIENT_KEY_SET.has(node.name)) clients.add(node.name)

    const replacement = map.get(node.name)
    if (replacement !== undefined) {
      if (isNode(replacement)) for (const dependency of replacement.dependencies) visit(dependency)
      return
    }

    if (EXCLUDED_KEYS.has(node.name)) excluded.push(node.name)
    if (lookupNode(node.name) === undefined) unknown.add(node.name)

    for (const dependency of node.dependencies) visit(dependency)
  }
  visit(root)

  const missingClients = SESSION_CLIENT_KEYS.filter((key) => !clients.has(key))
  return {
    excluded: [...excluded].sort(),
    missingClients,
    unknown: [...unknown].sort(),
    ok: excluded.length === 0 && missingClients.length === 0,
  }
}

/** Raised when a session layer violates the node table (fail loud, task 6.5). */
export class SessionLayerViolationError extends Error {
  readonly report: SessionLayerReport
  constructor(report: SessionLayerReport) {
    const parts: string[] = []
    if (report.excluded.length > 0) parts.push(`excluded nodes present: ${report.excluded.join(", ")}`)
    if (report.missingClients.length > 0) parts.push(`missing client nodes: ${report.missingClients.join(", ")}`)
    super(`session layer violates the node table (${parts.join("; ")})`)
    this.name = "SessionLayerViolationError"
    this.report = report
  }
}

/** Assert a session layer graph satisfies the table; throw descriptively otherwise. */
export function assertSessionLayer(
  root: LayerNode.Node<unknown, unknown, any>,
  replacements: LayerNode.Replacements = [],
): void {
  const report = validateSessionLayer(root, replacements)
  if (!report.ok) throw new SessionLayerViolationError(report)
}

/** Render the table for progress reports. */
export function describeNodeTable(): string {
  const rows = ALL_ENTRIES.map(
    (item) => `${item.label.padEnd(20)} ${item.boundary.padEnd(12)} ${item.roles.join(",")}`,
  )
  return rows.join("\n")
}

export * as WorkerNodeTable from "./node-table"
