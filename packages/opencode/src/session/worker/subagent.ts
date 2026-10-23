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

import { Context, Effect, Schema } from "effect"
import type { SessionID } from "@/session/schema"
import type { PromptInput } from "@/session/prompt"
import { HeadroomFullError } from "./dispatch"
import type { SessionExecutionResult } from "./execute"
import type { JobInfo, JobRegistry } from "./job-registry"
import { messageOf } from "./port"

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
  dispatch(request: {
    readonly sessionID: SessionID
    readonly depth?: number
    /** The sub-agent prompt the worker persists before looping (Solution 1). */
    readonly prompt?: PromptInput
  }): Promise<void>
}

export interface SubagentDispatchOptions {
  readonly registry: JobRegistry
  readonly scheduler: SubagentScheduler
  /** Resolved `subagent_depth`; the child's depth must not exceed it. */
  readonly depthLimit: number
  /** Whether the headroom pool is full (e.g. `dispatcher.headroomSlotCount >= dispatcher.headroomCapacity`). */
  readonly headroomFull: () => boolean
  /**
   * Bridge a settled child's worker result back to the registry so
   * `JobInfo.output` carries the child's last assistant text. Absent in
   * schedulers whose job client already reports the output (e.g. a fake, or the
   * child's own job port).
   */
  readonly resultFor?: (sessionID: SessionID) => SessionExecutionResult | undefined
  /**
   * Propagate the parent's per-session instance/context to the child BEFORE the
   * child worker materialises. A subagent child is dispatched by the raw
   * scheduler, so the hub's own `onDispatch` capture never runs for it; without
   * this the child boots under the hub's cwd (`Project.ID.global`) and its
   * per-worker hub-proxy handlers run with no `InstanceRef`. Invoked once per
   * admitted child, after registration and before `scheduler.dispatch`.
   */
  readonly onSubagentDispatch?: (childSessionID: SessionID, parentSessionID: SessionID) => void
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
  /** The sub-agent prompt the worker persists before looping (Solution 1). */
  readonly prompt?: PromptInput
}

/**
 * The narrow hub-proxy surface a worker's `SessionPrompt.ops.dispatchSubagent`
 * calls. It exists so the worker graph can expose the RPC without depending on
 * the (absent) `SessionHubService`; a transport failure is the only rejection,
 * a domain outcome returns as a status-bearing {@link JobInfo}.
 */
export interface SubagentDispatchProxy {
  readonly dispatchSubagent: (request: SubagentDispatchRequest) => Effect.Effect<JobInfo, Error>
}

/** Worker-side service key for the hub-proxy subagent-dispatch RPC. */
export class SubagentDispatchProxyService extends Context.Service<
  SubagentDispatchProxyService,
  SubagentDispatchProxy
>()("@opencode/session-worker/SubagentDispatch") {}

/**
 * A dispatch rejection is a cancellation (not a failure) when it carries the
 * exact `SessionCancelledError` tag. `WorkerCrashedError` is a distinct tagged
 * class, so an `instanceof`-style check could misread a crash as a cancel; the
 * exact tag check keeps the two distinct.
 */
function isSessionCancellation(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    (error as { readonly _tag?: unknown })._tag === "SessionCancelledError"
  )
}

/** A text part carries the assistant prose the parent renders as output. */
function isTextPart(value: unknown): value is { readonly text: string } {
  return (
    typeof value === "object" &&
    value !== null &&
    (value as { readonly type?: unknown }).type === "text" &&
    typeof (value as { readonly text?: unknown }).text === "string"
  )
}

/**
 * Extract the last assistant text from a settled worker result. The session
 * result's `value` is a `SessionV1.WithParts` (`{ parts }`) in the real path or a
 * pipeline result (`{ text }`) in the reduced path; both are parsed at this
 * boundary so the registry `output` is a plain string.
 */
function finalTextFromResult(result: SessionExecutionResult | undefined): string | undefined {
  const value = result?.value
  if (value === null || typeof value !== "object") return undefined
  const parts = (value as { readonly parts?: unknown }).parts
  if (Array.isArray(parts)) {
    for (let index = parts.length - 1; index >= 0; index--) {
      const part = parts[index]
      if (isTextPart(part)) return part.text
    }
  }
  const text = (value as { readonly text?: unknown }).text
  return typeof text === "string" ? text : undefined
}

/**
 * A status-bearing failed `JobInfo` for a child rejected BEFORE registration.
 * The admission seam never queues an over-deep or headroom-full child, so no
 * registry entry exists; the caller still receives a settled outcome.
 */
function rejectedSubagentJob(request: SubagentDispatchRequest, depth: number, error: unknown): JobInfo {
  const now = Date.now()
  return {
    id: request.childID,
    type: request.type,
    ...(request.title === undefined ? {} : { title: request.title }),
    ...(request.metadata === undefined ? {} : { metadata: request.metadata }),
    parentID: request.parentID,
    status: "error",
    started_at: now,
    completed_at: now,
    error: messageOf(error),
    depth,
  }
}

/**
 * Settle a registered child once its run resolves: the worker's own result is
 * the only place the last assistant text lives, so complete the registry job
 * with it. A child already settled (by its job port or the scheduler) is left
 * untouched.
 */
function settleChildFromResult(options: SubagentDispatchOptions, childID: SessionID): JobInfo | undefined {
  const current = options.registry.get(childID)
  if (current === undefined || current.status !== "running") return current
  const text = finalTextFromResult(options.resultFor?.(childID))
  return options.registry.complete(childID, text ?? "")
}

/**
 * Admit, register and dispatch one subagent child. Resolves with the child's
 * settled registry info once `scheduler.dispatch` resolves. Domain outcomes are
 * status-bearing and NEVER thrown (only a transport fault rejects): an over-deep
 * or headroom-full child returns a failed job without being registered, a
 * cancellation settles `cancelled`, any other dispatch failure settles `error`.
 */
export async function dispatchSubagent(
  options: SubagentDispatchOptions,
  request: SubagentDispatchRequest,
): Promise<JobInfo> {
  const parent = options.registry.get(request.parentID)
  const depth = request.depth ?? (parent?.depth ?? 0) + 1
  try {
    assertSubagentAdmitted({ depth, limit: options.depthLimit, headroomFull: options.headroomFull() })
  } catch (error) {
    return rejectedSubagentJob(request, depth, error)
  }

  const started = options.registry.start({
    id: request.childID,
    type: request.type,
    ...(request.title === undefined ? {} : { title: request.title }),
    ...(request.metadata === undefined ? {} : { metadata: request.metadata }),
    parentID: request.parentID,
    ...(request.workerID === undefined ? {} : { workerID: request.workerID }),
    depth,
  })

  // Inherit the parent's per-session instance/context before the child worker is
  // materialised by the scheduler, so `resolveInit(child)` and the child's
  // per-worker hub-proxy handlers see the parent's instance (never the hub cwd).
  options.onSubagentDispatch?.(request.childID, request.parentID)

  try {
    await options.scheduler.dispatch({
      sessionID: request.childID,
      depth,
      ...(request.prompt === undefined ? {} : { prompt: request.prompt }),
    })
  } catch (error) {
    const settled = isSessionCancellation(error)
      ? options.registry.cancel(request.childID)
      : options.registry.fail(request.childID, error)
    return settled ?? started
  }
  return settleChildFromResult(options, request.childID) ?? options.registry.get(request.childID) ?? started
}

export * as Subagent from "./subagent"
