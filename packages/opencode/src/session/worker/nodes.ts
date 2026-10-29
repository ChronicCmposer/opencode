// Flag-gated graph membership for the session-worker pool.
//
// The production app graphs (`effect/app-runtime.ts` `AppLayer` and
// `server/routes/instance/httpapi/server.ts` `app`) list layer nodes statically. The
// session-hub node, however, builds its pool lazily inside its layer (spawning no
// isolate until the first dispatch), so it must be a member only when the rollout
// flag is on. `sessionWorkerNodes` is that decision, factored out so both call
// sites share one pure predicate and the flag-off graph stays byte-identical to
// today (an empty spread).
//
// It is intentionally decoupled from `hub.ts`: the caller supplies the hub node
// (e.g. `SessionHub.productionSessionHubNode()`), so this module has no
// dependency on the production pool factory. Nothing here acquires resources or
// spawns workers — it only decides graph membership.
//
// The hub must never enter a worker-hosted graph. A session worker's reduced
// LAYER graph already excludes the hub, but its MODULE graph can dynamically pull
// in the app runtime (`effect/app-runtime.ts`) and server (`server/server.ts`) via
// `session/prompt.ts → plugin/index.ts`. When plugins are configured that
// materialises a second `Server`/`AppRuntime` inside the worker, whose app graph
// spreads this helper — building a nested hub (and a nested worker pool) in the
// worker. `sessionWorkerNodes` therefore also gates on the process role: only the
// `hub` role contributes the node, so a worker-hosted graph (`session`) always
// gets `[]`. The role is explicit per-isolate state (`@/effect/process-role`),
// replacing the ambient `isMainThread` check. The role is FAIL CLOSED: an isolate
// that never declared one is not a hub, so the gate stays shut.
//
// Applied wiring:
//
// ```ts
// // packages/opencode/src/effect/app-runtime.ts — inside AppLayer's group, after
// // SessionRunState.node:
// import { SessionWorkerNodes } from "@/session/worker/nodes"
// import { SessionHub } from "@/session/worker/hub"
// ...
// LayerNode.group([
//   ...,
//   SessionRunState.node,
//   ...SessionWorkerNodes.sessionWorkerNodes({ hubNode: SessionHub.productionSessionHubNode() }),
//   ...,
// ])
// ```
//
// The identical spread belongs in `server.ts`'s `app` group immediately after
// `SessionRunState.node`.

import type { AnyNode } from "@opencode-ai/core/effect/layer-node"
import { processRole } from "@/effect/process-role"
import { experimentalSessionWorkersFromEnv } from "@/effect/runtime-flags"
import type { Role } from "./node-table"

export interface SessionWorkerNodesOptions<N extends AnyNode = AnyNode> {
  /** The hub node to contribute when session workers are enabled. */
  readonly hubNode: N
  /**
   * Explicit gate. Defaults to `sessionWorkersEnabled(env)`, which delegates to
   * the canonical `RuntimeFlags.experimentalSessionWorkersFromEnv`.
   */
  readonly enabled?: boolean
  /** Environment source for the default gate. Defaults to `process.env`. */
  readonly env?: Record<string, string | undefined>
  /**
   * The process role to gate on. Defaults to {@link processRole}: only the
   * `hub` role contributes the hub node, so a worker-hosted graph (e.g. the app
   * runtime pulled in by a session worker's module graph) always gets `[]`, and
   * an isolate that never declared a role stays closed too.
   */
  readonly role?: Role
}

/**
 * Whether the session-worker rollout flag is on for `env`. Delegates to the
 * canonical parser `experimentalSessionWorkersFromEnv` (runtime-flags.ts), the
 * same parser `RuntimeFlags.experimentalSessionWorkers` resolves through. The
 * dedicated `OPENCODE_EXPERIMENTAL_SESSION_WORKERS` var wins when it parses as a
 * boolean, otherwise the `OPENCODE_EXPERIMENTAL` umbrella applies, otherwise
 * `false`.
 *
 * The graph decision is made at module-eval time from `process.env`, while the
 * runtime gate resolves through the ambient ConfigProvider (whose default reads
 * `process.env`). Sharing the parser keeps them in lockstep on every value,
 * including malformed ones; the sources only diverge when a caller overrides the
 * ConfigProvider away from the environment.
 */
export function sessionWorkersEnabled(env: Record<string, string | undefined> = process.env): boolean {
  return experimentalSessionWorkersFromEnv(env)
}

/**
 * The session-hub node as a spreadable list: `[hubNode]` when session workers are
 * enabled for the `hub` process role, `[]` otherwise. Pure and side-effect free.
 */
export function sessionWorkerNodes<const N extends AnyNode>(
  options: SessionWorkerNodesOptions<N>,
): readonly N[] {
  const enabled = options.enabled ?? sessionWorkersEnabled(options.env)
  const role = options.role ?? processRole()
  return enabled && role === "hub" ? [options.hubNode] : []
}

export * as SessionWorkerNodes from "./nodes"
