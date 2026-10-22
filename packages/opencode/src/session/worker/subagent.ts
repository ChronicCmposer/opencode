// Phase 9.1/9.5 — subagent dispatch and nesting admission
// (plan tasks 9.1/9.5; decision line 60).
//
// A `task` child session is a subagent job: it is registered on the hub
// registry (9.2) and dispatched to a worker at `depth ≥ 1`. Nesting is bounded
// by the resolved `subagent_depth` limit and by physical headroom. Both bounds
// are checked BEFORE the scheduler is touched, so an over-deep or headroom-full
// child is rejected loudly and NEVER queued (decision line 60).
//
// The module is a thin orchestration seam: the registry is the hub authority
// (`job-registry.ts`) and the scheduler is the hub's `Dispatcher`
// (`dispatch.ts`), so `task` never needs to know about isolates.

import { Schema } from "effect"
import type { SessionID } from "@/session/schema"
import { HeadroomFullError } from "./dispatch"
import type { JobInfo, JobRegistry } from "./job-registry"

/** Raised when a subagent would exceed the resolved `subagent_depth` limit. */
export class SubagentDepthError extends Schema.TaggedErrorClass<SubagentDepthError>()("SubagentDepthError", {
  message: Schema.String,
  depth: Schema.Number,
  limit: Schema.Number,
}) {}

/**
 * Raised when no headroom worker is available; the child is never queued. The
 * scheduler is the authority for physical headroom, so the error is declared
 * there and re-exported here for the admission seam's callers.
 */
export { HeadroomFullError }

export interface SubagentAdmission {
  /** The child's nesting depth (0 = foreground/root, ≥1 = subagent). */
  readonly depth: number
  /** Resolved `subagent_depth` limit. */
  readonly limit: number
  /** Whether the physical headroom pool is at capacity. */
  readonly headroomFull: boolean
}

/** Fail loud on an inadmissible child; a valid admission returns normally. */
export function assertSubagentAdmitted(admission: SubagentAdmission): void {
  // Boundary parse: a subagent is depth ≥ 1 and the limit is a non-negative
  // integer. Validate before any comparison so a malformed value fails loud.
  if (!Number.isInteger(admission.depth) || admission.depth < 1) {
    throw new SubagentDepthError({
      message: `subagent depth ${admission.depth} must be an integer ≥ 1`,
      depth: admission.depth,
      limit: admission.limit,
    })
  }
  if (!Number.isInteger(admission.limit) || admission.limit < 0) {
    throw new SubagentDepthError({
      message: `subagent depth limit ${admission.limit} must be a non-negative integer`,
      depth: admission.depth,
      limit: admission.limit,
    })
  }
  if (admission.depth > admission.limit) {
    throw new SubagentDepthError({
      message: `subagent depth ${admission.depth} exceeds the limit ${admission.limit}`,
      depth: admission.depth,
      limit: admission.limit,
    })
  }
  if (admission.headroomFull) {
    throw new HeadroomFullError({ message: "subagent headroom is full; refusing to queue the child" })
  }
}

/** The scheduling slice of the hub `Dispatcher` this seam needs. */
export interface SubagentScheduler {
  dispatch(request: { readonly sessionID: SessionID; readonly depth?: number }): Promise<void>
}

export interface SubagentDispatchOptions {
  readonly registry: JobRegistry
  readonly scheduler: SubagentScheduler
  /** Resolved `subagent_depth`; the child's depth must not exceed it. */
  readonly depthLimit: number
  /** Whether the headroom pool is full (e.g. `dispatcher.headroomSlotCount >= dispatcher.headroomCapacity`). */
  readonly headroomFull: () => boolean
}

export interface SubagentDispatchRequest {
  readonly parentID: SessionID
  readonly childID: SessionID
  readonly type: string
  readonly title?: string
  readonly metadata?: Record<string, unknown>
  readonly workerID?: number
  /** Explicit depth; derived from the parent job when omitted. */
  readonly depth?: number
}

/**
 * A dispatch rejection is a cancellation (not a failure) when it carries the
 * exact `SessionCancelledError` tag. `WorkerCrashedError` re-parents its
 * prototype to `SessionCancelledError`, so an `instanceof` test would misread a
 * crash as a cancel; the tag check keeps the two distinct.
 */
function isSessionCancellation(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    (error as { readonly _tag?: unknown })._tag === "SessionCancelledError"
  )
}

/**
 * Admit, register and dispatch one subagent child. Resolves with the child's
 * settled registry info once `scheduler.dispatch` resolves; a dispatch failure
 * settles the job and rethrows — `cancelled` for a cancellation, `error`
 * otherwise — so the parent's routed result matches the child's terminal state.
 */
export async function dispatchSubagent(
  options: SubagentDispatchOptions,
  request: SubagentDispatchRequest,
): Promise<JobInfo | undefined> {
  const parent = options.registry.get(request.parentID)
  const depth = request.depth ?? (parent?.depth ?? 0) + 1
  assertSubagentAdmitted({ depth, limit: options.depthLimit, headroomFull: options.headroomFull() })

  options.registry.start({
    id: request.childID,
    type: request.type,
    ...(request.title === undefined ? {} : { title: request.title }),
    ...(request.metadata === undefined ? {} : { metadata: request.metadata }),
    parentID: request.parentID,
    ...(request.workerID === undefined ? {} : { workerID: request.workerID }),
    depth,
  })

  try {
    await options.scheduler.dispatch({ sessionID: request.childID, depth })
  } catch (error) {
    if (isSessionCancellation(error)) options.registry.cancel(request.childID)
    else options.registry.fail(request.childID, error)
    throw error
  }
  return options.registry.get(request.childID)
}

export * as Subagent from "./subagent"
