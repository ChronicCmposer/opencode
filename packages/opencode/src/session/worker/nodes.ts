// TDD steps 34–35 — flag-gated graph membership for the session-worker pool.
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
// (e.g. `SessionHubModule.productionSessionHubNode()`), so this module has no
// dependency on the production pool factory. Nothing here acquires resources or
// spawns workers — it only decides graph membership.
//
// Applied wiring (Wave 3):
//
// ```ts
// // packages/opencode/src/effect/app-runtime.ts — inside AppLayer's group, after
// // SessionRunState.node:
// import { SessionWorkerNodes } from "@/session/worker/nodes"
// import { SessionHubModule } from "@/session/worker/hub"
// ...
// LayerNode.group([
//   ...,
//   SessionRunState.node,
//   ...SessionWorkerNodes.sessionWorkerNodes({ hubNode: SessionHubModule.productionSessionHubNode() }),
//   ...,
// ])
// ```
//
// The identical spread belongs in `server.ts`'s `app` group immediately after
// `SessionRunState.node`.

import type { AnyNode } from "@opencode-ai/core/effect/layer-node"

export interface SessionWorkerNodesOptions<N extends AnyNode = AnyNode> {
  /** The hub node to contribute when session workers are enabled. */
  readonly hubNode: N
  /**
   * Explicit gate. Defaults to `sessionWorkersEnabled(env)`, which mirrors
   * `RuntimeFlags.experimentalSessionWorkers`.
   */
  readonly enabled?: boolean
  /** Environment source for the default gate. Defaults to `process.env`. */
  readonly env?: Record<string, string | undefined>
}

/**
 * Whether the session-worker rollout flag is on for `env`, mirroring
 * `enabledByExperimental` (runtime-flags.ts:12-16): the dedicated
 * `OPENCODE_EXPERIMENTAL_SESSION_WORKERS` var wins when present, otherwise the
 * `OPENCODE_EXPERIMENTAL` umbrella applies.
 */
export function sessionWorkersEnabled(env: Record<string, string | undefined> = process.env): boolean {
  const dedicated = env.OPENCODE_EXPERIMENTAL_SESSION_WORKERS
  return dedicated !== undefined ? dedicated === "true" : env.OPENCODE_EXPERIMENTAL === "true"
}

/**
 * The session-hub node as a spreadable list: `[hubNode]` when session workers are
 * enabled, `[]` otherwise. Pure and side-effect free.
 */
export function sessionWorkerNodes<const N extends AnyNode>(
  options: SessionWorkerNodesOptions<N>,
): readonly N[] {
  const enabled = options.enabled ?? sessionWorkersEnabled(options.env)
  return enabled ? [options.hubNode] : []
}

export * as SessionWorkerNodes from "./nodes"
