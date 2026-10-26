// Session-spawn transport for a worker isolate.
//
// A worker runs the parent session's `task` tool but must not run the child on
// its own isolate: each session owns one lazily-materialized worker. The worker
// therefore forwards `Session.spawn` over the hub-proxy control port, and the
// hub runs the real spawn under the parent's captured runtime context (which
// dispatches the child to its own worker). This module is the dependency-free
// vocabulary both sides share; it deliberately imports no runtime session code.

import { Context, Effect, Schema } from "effect"
import type { SessionV1 } from "@opencode-ai/core/v1/session"
import type { SessionID } from "@/session/schema"
import type { PromptInput } from "@/session/prompt"

/** Raised when a child would exceed the resolved `subagent_depth` limit. */
export class SubagentDepthError extends Schema.TaggedErrorClass<SubagentDepthError>()("SubagentDepthError", {
  message: Schema.String,
  depth: Schema.Number,
  limit: Schema.Number,
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
 * The narrow hub-proxy surface a worker's `Session.spawn` calls. A transport
 * failure is the only rejection; a domain outcome arrives as the spawn result.
 */
export interface SessionSpawnProxy {
  readonly spawn: (request: SessionSpawnRequest) => Effect.Effect<SessionSpawnResult, Error>
}

/** Worker-side service key for the hub-proxy `session.spawn` RPC. */
export class SessionSpawnProxyService extends Context.Service<
  SessionSpawnProxyService,
  SessionSpawnProxy
>()("@opencode/session-worker/SessionSpawn") {}

export * as Subagent from "./subagent"
