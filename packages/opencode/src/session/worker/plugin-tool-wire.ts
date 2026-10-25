// The plugin-tool wire: the clone-safe shapes that let a session worker expose
// plugin-provided tools while the tool's `execute` runs on the hub, where the
// plugins are loaded (decision 70).
//
// A plugin tool *definition* carries an `execute` function and therefore cannot
// cross the isolate. The hub ships only clone-safe `PluginToolDescriptor`s in the
// worker init; the worker rebuilds each as a `RemotePluginToolDefinition` whose
// `execute` forwards over the hub-proxy control port. This mirrors the
// `messagesTransform` hub-proxy pattern (`hub-proxy-wire.ts`), but per call.
//
// This module is deliberately dependency-light: both the worker (`layer.ts`) and
// the hub (`hub.ts`) import it, so it must not reach back into either.

import type { JSONSchema7 } from "@ai-sdk/provider"

/** The clone-safe projection of one plugin tool, shipped to the worker init. */
export interface PluginToolDescriptor {
  readonly id: string
  readonly description: string
  readonly jsonSchema: JSONSchema7
}

/**
 * A single mid-execute progress update a hub plugin tool reports through its
 * `ToolContext.metadata`. Only clone-safe fields are carried: the worker turns
 * them back into the local `Tool.Context.metadata` call.
 */
export interface PluginToolProgress {
  readonly title?: string
  readonly metadata?: unknown
}

/** Receives {@link PluginToolProgress} updates while a remote tool executes. */
export type PluginToolProgressSink = (progress: PluginToolProgress) => void

/**
 * Ask the hub to unwind one in-flight `pluginTool.execute`, identified by its
 * `(sessionID, callID)` frame. The worker builds this over the hub control port
 * (`hubProxyPluginToolAbort`); {@link remotePluginToolHooks} adapts it onto each
 * remote definition's {@link RemotePluginToolDefinition.abort}, which the
 * registry invokes when the local `toolCtx.abort` fires.
 */
export type PluginToolAbort = (input: { readonly sessionID: string; readonly callID: string }) => void | Promise<void>

/** One `pluginTool.execute` hub-proxy request: which tool, in which call frame. */
export interface PluginToolExecuteInput {
  readonly toolID: string
  readonly sessionID: string
  readonly messageID: string
  readonly callID: string
  readonly agent: string
  readonly args: unknown
}

/** The clone-safe tool result the hub returns across the control port. */
export interface PluginToolExecuteResult {
  readonly title: string
  readonly output: string
  readonly metadata: unknown
  readonly attachments?: unknown
}

/** The worker's hub-port client for `pluginTool.execute`, when one is wired. */
export interface PluginToolExecutor {
  execute(input: PluginToolExecuteInput, onProgress?: PluginToolProgressSink): Promise<PluginToolExecuteResult>
}

/** The call frame a worker-side remote tool forwards to its executor. */
export interface RemotePluginToolContext {
  readonly sessionID: string
  readonly messageID: string
  readonly callID?: string
  readonly agent: string
}

/**
 * The worker-side stand-in for a hub-owned plugin tool. Structurally compatible
 * with a `Hooks.tool` entry, but with no local `execute`: the call is forwarded
 * to the hub through a {@link PluginToolExecutor}.
 */
export interface RemotePluginToolDefinition {
  readonly description: string
  readonly jsonSchema: JSONSchema7
  readonly execute: (
    args: unknown,
    ctx: RemotePluginToolContext,
    onProgress?: PluginToolProgressSink,
  ) => Promise<PluginToolExecuteResult>
  /**
   * Cross the isolate to abort this tool's in-flight hub call. Optional so a
   * definition built without a hub control port (or an older caller) stays
   * valid; the registry reads it structurally and invokes it on `toolCtx.abort`.
   */
  readonly abort?: (ctx: RemotePluginToolContext) => void | Promise<void>
  /**
   * Re-fetch the hub's descriptor set after an `UnknownPluginToolError` (a
   * stale descriptor or a plugin reloaded mid-generation). Optional; the
   * registry calls it structurally, then invalidates its cached registration so
   * the next build re-registers the fresh descriptors.
   */
  readonly refresh?: () => void | Promise<void>
}

/**
 * Raised when a hub plugin tool returns a result that cannot cross the isolate.
 * Fail loud: a non-cloneable payload is a config defect, and naming the tool and
 * the offending field turns a cryptic structuredClone failure into an actionable
 * one. Hub-side code throws this; it surfaces to the worker as a tagged
 * `HubProxyRemoteError`.
 */
export class PluginToolNotSerializableError extends Error {
  readonly _tag = "PluginToolNotSerializableError"
  constructor(
    readonly toolID: string,
    readonly detail: string,
  ) {
    super(`plugin tool "${toolID}" returned a non-serializable ${detail}`)
    this.name = "PluginToolNotSerializableError"
  }
}

/**
 * Whether `value` is a worker-side remote definition rather than a real
 * `ToolDefinition`. A real plugin tool carries a Zod `args` raw shape and never a
 * `jsonSchema` key; {@link remotePluginToolHooks} always sets `jsonSchema`, so the
 * own-key presence distinguishes them without a fragile brand.
 */
export function isRemotePluginToolDefinition(value: unknown): value is RemotePluginToolDefinition {
  if (typeof value !== "object" || value === null) return false
  const candidate = value as Record<string, unknown>
  return (
    typeof candidate.description === "string" &&
    typeof candidate.execute === "function" &&
    Object.hasOwn(candidate, "jsonSchema")
  )
}

/**
 * The optional hub callbacks each rebuilt remote definition carries: the abort
 * bridge (adapted from a {@link PluginToolAbort}) and the lazy-refresh action.
 * Both are omitted from the built stub when absent, so a definition without a
 * hub control port never advertises a seam that would fail on call.
 */
export interface RemotePluginToolCallbacks {
  readonly abort?: PluginToolAbort
  readonly refresh?: () => void | Promise<void>
}

/**
 * Rebuild the `Hooks.tool` map a worker's reduced `Plugin.Service` lists — one
 * remote definition per descriptor. Each call forwards to `executor` with the
 * descriptor's id, the call frame, and the caller's progress sink; the hub owns
 * the real plugin instance and runs it. `callbacks` (when supplied) attach the
 * hub abort bridge and the lazy-refresh seam to every definition.
 */
export function remotePluginToolHooks(
  descriptors: readonly PluginToolDescriptor[],
  executor: PluginToolExecutor,
  callbacks: RemotePluginToolCallbacks = {},
): { readonly tool: Record<string, RemotePluginToolDefinition> } {
  const { abort, refresh } = callbacks
  const tool: Record<string, RemotePluginToolDefinition> = {}
  for (const descriptor of descriptors) {
    tool[descriptor.id] = {
      description: descriptor.description,
      jsonSchema: descriptor.jsonSchema,
      execute: (args, ctx, onProgress) =>
        executor.execute(
          {
            toolID: descriptor.id,
            sessionID: ctx.sessionID,
            messageID: ctx.messageID,
            callID: ctx.callID ?? "",
            agent: ctx.agent,
            args,
          },
          onProgress,
        ),
      ...(abort === undefined
        ? {}
        : { abort: (ctx: RemotePluginToolContext) => abort({ sessionID: ctx.sessionID, callID: ctx.callID ?? "" }) }),
      ...(refresh === undefined ? {} : { refresh }),
    }
  }
  return { tool }
}
