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
  entry(
    "SessionProjector",
    "session-projector",
    ["session"],
    "worker-local",
    "materializes session/message/part rows from durable events",
  ),
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
  // Real session-pipeline dependencies (task 29): the reduced worker layer must
  // be able to run the V1 `SessionPrompt` loop, not just the toy pipeline.
  entry("SessionStatus", "@opencode/SessionStatus", ["session"], "hub-proxy", "hub-resident authority"),
  entry("SessionCompaction", "@opencode/SessionCompaction", ["session"], "worker-local"),
  entry("SessionRevert", "@opencode/SessionRevert", ["session"], "worker-local"),
  entry("SessionSummary", "@opencode/SessionSummary", ["session"], "worker-local"),
  entry("Instruction", "@opencode/Instruction", ["session"], "worker-local"),
  entry("SystemPrompt", "@opencode/SystemPrompt", ["session"], "worker-local"),
  entry("Command", "@opencode/Command", ["session"], "worker-local"),
  entry("Image", "@opencode/Image", ["session"], "worker-local"),
  entry("EventV2Bridge", "@opencode/EventV2Bridge", ["session"], "worker-local", "worker-local egress mesh"),
  entry("Event", "@opencode/Event", ["session"], "worker-local"),
  entry("Snapshot", "@opencode/Snapshot", ["session"], "worker-local"),
  entry("LLMClient", "@opencode/LLMClient", ["session"], "worker-local", "platform HTTP client"),
  entry("RequestExecutor", "@opencode/LLM/RequestExecutor", ["session"], "worker-local"),
  entry("Env", "@opencode/Env", ["session"], "worker-local", "worker-isolate environment"),
  entry("FileSystem", "@opencode/FileSystem", ["session"], "worker-local", "worker-isolate filesystem"),
  entry("EventEgressClient", "@opencode/session-worker/EventEgressClient", ["session"], "worker-local"),
  entry(
    "WorkerEventBridge",
    "@opencode/session-worker/EventBridge",
    ["session"],
    "worker-local",
    "worker EventV2 → events-port frames",
  ),
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
  entry("LSP", "@opencode/LSP", ["broker"], "excluded", "broker-proxy only"),
  entry("MCP", "@opencode/MCP", ["broker"], "excluded", "broker-proxy only"),
  entry("McpAuth", "@opencode/McpAuth", ["broker"], "excluded", "broker-proxy only"),
  entry("Workspace", "@opencode/Workspace", ["hub"], "excluded"),
  entry("Worktree", "@opencode/Worktree", ["hub"], "excluded"),
  entry("Vcs", "@opencode/Vcs", ["hub"], "excluded"),
  entry("Installation", "@opencode/Installation", ["hub"], "excluded"),
  entry("Npm", "@opencode/Npm", ["hub"], "excluded", "no npm install in workers"),
  entry("ModelsDev", "@opencode/ModelsDev", ["hub"], "excluded", "hub-only catalog refresh"),
  // Residual nodes reachable from the real `SessionPrompt` graph (task 29). They
  // are hub/writer/broker/infra-owned, so they are classified `excluded` and
  // supplied by boundary replacements rather than left fail-closed `unknown`.
  entry("Storage", "@opencode/Storage", ["hub", "writer"], "excluded", "writer-owned storage"),
  entry("Git", "@opencode/Git", ["hub"], "excluded", "hub-owned VCS"),
  entry("BackgroundJob", "@opencode/BackgroundJob", ["hub"], "excluded", "hub-resident job registry"),
  entry("Global", "@opencode/Global", ["hub"], "excluded", "process-global state"),
  entry("AppProcess", "@opencode/AppProcess", ["hub"], "excluded", "OS child processes stay on the hub"),
  entry("RipgrepBinary", "@opencode/RipgrepBinary", ["broker"], "excluded", "broker-owned binary"),
  entry("v2/Ripgrep", "@opencode/v2/Ripgrep", ["broker"], "excluded", "broker-proxy only"),
  entry("McpBrowser", "@opencode/McpBrowser", ["broker"], "excluded", "broker-proxy only"),
  entry("LocationServiceMap", "@opencode/example/LocationServiceMap", ["hub"], "excluded", "hub-composed location map"),
]

/**
 * Client service keys. This is the single source of truth: `layer.ts` keys its
 * client service classes from these constants, so the table and the layer can
 * never drift apart.
 */
export const WRITER_CLIENT_KEY = "@opencode/session-worker/WriterClient"
export const EVENT_EGRESS_CLIENT_KEY = "@opencode/session-worker/EventEgressClient"
export const BROKER_CLIENT_KEY = "@opencode/session-worker/BrokerClient"
export const BACKGROUND_JOB_CLIENT_KEY = "@opencode/session-worker/BackgroundJobClient"

/**
 * The worker's EventV2 → events-port bridge node. Worker-local (it subscribes to
 * the in-isolate EventV2 and frames onto the egress channel), so it is allowlisted
 * but not a port-backed client.
 */
export const EVENT_BRIDGE_KEY = "@opencode/session-worker/EventBridge"

/** Client nodes the session layer MUST contain (task 6.5: writer RPC client present). */
export const SESSION_CLIENT_KEYS: readonly string[] = [
  WRITER_CLIENT_KEY,
  EVENT_EGRESS_CLIENT_KEY,
  BROKER_CLIENT_KEY,
  BACKGROUND_JOB_CLIENT_KEY,
]

/**
 * The real session-pipeline services a worker must run (task 29). Every key is
 * allowlisted, so a reduced layer built from this set passes `assertSessionLayer`.
 * `RUNNABLE_SESSION_NODES` (`layer.ts`) is derived from this list, so the keys
 * are the single source of truth and the table and the layer cannot drift.
 */
export const REQUIRED_SESSION_NODE_KEYS: readonly string[] = [
  "@opencode/Session",
  "@opencode/SessionPrompt",
  "@opencode/SessionProcessor",
  "@opencode/SessionRunState",
  "session-projector",
  "@opencode/SessionStatus",
  "@opencode/SessionCompaction",
  "@opencode/SessionRevert",
  "@opencode/SessionSummary",
  "@opencode/SessionTodo",
  "@opencode/LLM",
  "@opencode/Provider",
  "@opencode/Agent",
  "@opencode/Skill",
  "@opencode/SkillDiscovery",
  "@opencode/ToolRegistry",
  "@opencode/Plugin",
  "@opencode/Config",
  "@opencode/Auth",
  "@opencode/RuntimeFlags",
  "@opencode/Question",
  "@opencode/Permission",
  "@opencode/Truncate",
  "@opencode/Format",
  "@opencode/Instruction",
  "@opencode/SystemPrompt",
  "@opencode/Command",
  "@opencode/Image",
  "@opencode/EventV2Bridge",
  "@opencode/Event",
  "@opencode/Snapshot",
  "@opencode/LLMClient",
]

const ALL_ENTRIES: readonly NodeEntry[] = [...SESSION_ALLOWLIST, ...SESSION_EXCLUDED]

const EXCLUDED_KEYS: ReadonlySet<string> = new Set(SESSION_EXCLUDED.map((item) => item.key))
const CLIENT_KEY_SET: ReadonlySet<string> = new Set(SESSION_CLIENT_KEYS)

/** Key and label both resolve to the same entry. */
const LOOKUP: ReadonlyMap<string, NodeEntry> = new Map(
  ALL_ENTRIES.flatMap((item) => [
    [item.key, item],
    [item.label, item],
  ]),
)

/** The table governs application nodes; Effect runtime infrastructure is out of scope. */
const isApplicationNode = (name: string): boolean => name.startsWith("@opencode/")

/** Look up a table entry by node name or label. */
export function lookupNode(name: string): NodeEntry | undefined {
  return LOOKUP.get(name)
}

type AnyNode = LayerNode.AnyNode

/**
 * Single traversal shared by `collectNodeNames` and `findNode`. Results are
 * memoized per node, so a node reachable by several paths is visited once.
 */
function walkNodes<Result>(root: AnyNode, visit: (node: AnyNode, recur: (node: AnyNode) => Result) => Result): Result {
  // Boxed so a legitimately `undefined` result is still a cache hit.
  const cache = new Map<AnyNode, { readonly value: Result }>()
  const recur = (node: AnyNode): Result => {
    const cached = cache.get(node)
    if (cached !== undefined) return cached.value
    const value = visit(node, recur)
    cache.set(node, { value })
    return value
  }
  return recur(root)
}

/** Walk a node graph and collect every reachable node name (groups excluded). */
export function collectNodeNames(root: AnyNode): ReadonlySet<string> {
  const names = new Set<string>()
  walkNodes(root, (node, recur) => {
    if (node.kind !== "group") names.add(node.name)
    for (const dependency of node.dependencies) recur(dependency)
  })
  return names
}

/** Find a node by name in a graph (depth-first, first match). */
export function findNode(root: AnyNode, name: string): AnyNode | undefined {
  return walkNodes<AnyNode | undefined>(root, (node, recur) => {
    if (node.name === name && node.kind !== "group") return node
    for (const dependency of node.dependencies) {
      const found = recur(dependency)
      if (found !== undefined) return found
    }
    return undefined
  })
}

type AnyReplacement = LayerNode.Replacements[number][1]

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
  /** Reachable application nodes that are neither allowlisted nor excluded (must be empty). */
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
  root: AnyNode,
  replacements: LayerNode.Replacements = [],
): SessionLayerReport {
  const map = replacementMap(replacements)
  const seen = new Set<AnyNode>()
  const excluded: string[] = []
  const clients = new Set<string>()
  const unknown = new Set<string>()

  const visit = (node: AnyNode): void => {
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
      if (LayerNode.isNode(replacement)) for (const dependency of replacement.dependencies) visit(dependency)
      return
    }

    if (EXCLUDED_KEYS.has(node.name)) excluded.push(node.name)
    // Fail closed: a reachable application node the table does not classify is
    // a violation, so a new dependency cannot slip in unclassified.
    if (lookupNode(node.name) === undefined && isApplicationNode(node.name)) unknown.add(node.name)

    for (const dependency of node.dependencies) visit(dependency)
  }
  visit(root)

  const missingClients = SESSION_CLIENT_KEYS.filter((key) => !clients.has(key))
  return {
    excluded: [...excluded].sort(),
    missingClients,
    unknown: [...unknown].sort(),
    ok: excluded.length === 0 && missingClients.length === 0 && unknown.size === 0,
  }
}

/** Raised when a session layer violates the node table (fail loud, task 6.5). */
export class SessionLayerViolationError extends Error {
  readonly report: SessionLayerReport
  constructor(report: SessionLayerReport) {
    const parts: string[] = []
    if (report.excluded.length > 0) parts.push(`excluded nodes present: ${report.excluded.join(", ")}`)
    if (report.missingClients.length > 0) parts.push(`missing client nodes: ${report.missingClients.join(", ")}`)
    if (report.unknown.length > 0) parts.push(`unclassified nodes present: ${report.unknown.join(", ")}`)
    super(`session layer violates the node table (${parts.join("; ")})`)
    this.name = "SessionLayerViolationError"
    this.report = report
  }
}

/** Assert a session layer graph satisfies the table; throw descriptively otherwise. */
export function assertSessionLayer(root: AnyNode, replacements: LayerNode.Replacements = []): void {
  const report = validateSessionLayer(root, replacements)
  if (!report.ok) throw new SessionLayerViolationError(report)
}

export * as WorkerNodeTable from "./node-table"
