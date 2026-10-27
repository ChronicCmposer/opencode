// Session-spawn transport for a worker isolate.
//
// A worker runs the parent session's `task` tool but must not run the child on
// its own isolate: each session owns one lazily-materialized worker. The worker
// therefore forwards child-session operations over the hub-proxy control port,
// and the hub runs them under the parent's captured runtime context. This module
// is the dependency-free vocabulary both sides share; it deliberately imports no
// runtime session code.

import { Context, Effect, Schema } from "effect"
import type { SessionV1 } from "@opencode-ai/core/v1/session"
import type { SessionID } from "@/session/schema"
import type { PromptInput } from "@/session/prompt"
import type { Session } from "@/session/session"
import type { CancelOutcome } from "./cancel"

/** Raised when a child would exceed the resolved `subagent_depth` limit. */
export class SubagentDepthError extends Schema.TaggedErrorClass<SubagentDepthError>()("SubagentDepthError", {
  message: Schema.String,
  depth: Schema.Number,
  limit: Schema.Number,
}) {}

/**
 * Raised by the hub when a worker asks for a session outside its own child scope.
 * A bound worker may only observe and control itself and its direct children; a
 * child worker owns its own descendants.
 */
export class SessionScopeError extends Schema.TaggedErrorClass<SessionScopeError>()("SessionScopeError", {
  message: Schema.String,
  sessionID: Schema.String,
  targetID: Schema.String,
}) {}

/** A worker's request to spawn (and prompt) a child session on the hub. */
export interface SessionSpawnRequest {
  readonly parentID: SessionID
  /** Present once the caller has created the child (e.g. to stamp permissions). */
  readonly childID?: SessionID
  readonly type: string
  readonly title?: string
  readonly metadata?: Record<string, unknown>
  /** The first user prompt the child worker persists and runs. */
  readonly prompt: PromptInput
}

export interface SessionSpawnResult {
  readonly childID: SessionID
  readonly result: SessionV1.WithParts
}

/**
 * A worker's request to create a child on the hub. `sessionID` is the worker's
 * bound session and is checked against the hub's registration, never trusted on
 * its own.
 */
export interface SessionCreateRequest {
  readonly sessionID: SessionID
  readonly parentID?: SessionID
  readonly title?: string
  readonly agent?: string
  readonly model?: Session.Info["model"]
  readonly metadata?: Session.Info["metadata"]
  readonly permission?: Session.Info["permission"]
  readonly workspaceID?: Session.Info["workspaceID"]
}

/** A worker's read of one session in its own subtree. */
export interface SessionGetRequest {
  readonly sessionID: SessionID
  readonly targetID: SessionID
}

/** A worker's listing of the children of a session in its own subtree. */
export interface SessionChildrenRequest {
  readonly sessionID: SessionID
  readonly parentID: SessionID
}

/** A worker's request to cancel a session (and its descendants) on the hub. */
export interface SessionCancelRequest {
  readonly sessionID: SessionID
  readonly targetID: SessionID
}

/**
 * The narrow hub-proxy surface a worker's session operations call. The worker
 * never receives the child's rows: create returns the new id/Info, read returns
 * a hub-rendered Info, children returns hub-rendered Infos, and cancel returns
 * the hub's terminal outcome. A transport failure is the only rejection; a
 * domain outcome arrives as the result.
 */
export interface SessionSpawnProxy {
  /** The session this isolate is bound to, when known. */
  readonly boundID?: SessionID
  readonly spawn: (request: SessionSpawnRequest) => Effect.Effect<SessionSpawnResult, Error>
  /**
   * Optional so lightweight test doubles that only exercise `spawn` remain
   * assignable. The production worker proxy provides every method.
   */
  readonly create?: (input: Omit<SessionCreateRequest, "sessionID">) => Effect.Effect<Session.Info, Error>
  readonly get?: (targetID: SessionID) => Effect.Effect<Session.Info | null, Error>
  readonly children?: (parentID: SessionID) => Effect.Effect<readonly Session.Info[], Error>
  readonly cancel?: (targetID: SessionID) => Effect.Effect<CancelOutcome | undefined, Error>
}

/** Worker-side service key for the hub-proxy session-scope RPCs. */
export class SessionSpawnProxyService extends Context.Service<SessionSpawnProxyService, SessionSpawnProxy>()(
  "@opencode/session-worker/SessionSpawn",
) {}

export * as Subagent from "./subagent"
