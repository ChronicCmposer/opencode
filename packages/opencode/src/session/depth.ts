/**
 * Depth is carried explicitly as `metadata.depth` on a session, not inferred
 * from the parent chain. Implicit children (delegate/metadata spawns) stay at
 * depth 0 because they carry no depth metadata; only task children advance it.
 */
export function spawnDepth(metadata: unknown): number {
  if (typeof metadata !== "object" || metadata === null) return 0
  const depth = (metadata as Record<string, unknown>).depth
  if (typeof depth !== "number") return 0
  if (!Number.isInteger(depth) || depth < 0) return 0
  return depth
}

/** The depth a task child spawned from the given parent metadata should carry. */
export function nextDepth(metadata: unknown): number {
  return spawnDepth(metadata) + 1
}

/** A child is rejected only when it exceeds the limit; equal depth is allowed. */
export function exceedsSubagentDepth(childDepth: number, limit: number): boolean {
  return childDepth > limit
}

/** The user-facing message shown when a child would exceed `subagent_depth`. */
export function subagentDepthMessage(limit: number): string {
  return `Subagent depth limit reached (${limit}). Increase "subagent_depth" to allow nested subagents.`
}

export * as SessionDepth from "./depth"
