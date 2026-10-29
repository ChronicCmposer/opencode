// The plugin-tool wire: the clone-safe shapes that let a session worker expose plugin-provided tools while the tool's `execute` runs on the hub.

import type { JSONSchema7 } from "@ai-sdk/provider"
import { Schema } from "effect"

/** The clone-safe projection of one plugin tool, shipped to the worker init. */
export interface PluginToolDescriptor {
  readonly id: string
  readonly description: string
  readonly jsonSchema: JSONSchema7
}

/** A single mid-execute progress update a hub plugin tool reports through its `ToolContext.metadata`. */
export interface PluginToolProgress {
  readonly title?: string
  readonly metadata?: unknown
}

/** Receives {@link PluginToolProgress} updates while a remote tool executes. */
export type PluginToolProgressSink = (progress: PluginToolProgress) => void

/** Ask the hub to unwind one in-flight `pluginTool.execute`, identified by its `(sessionID, callID)` frame. */
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

/** The worker-side stand-in for a hub-owned plugin tool. */
export interface RemotePluginToolDefinition {
  readonly description: string
  readonly jsonSchema: JSONSchema7
  readonly execute: (
    args: unknown,
    ctx: RemotePluginToolContext,
    onProgress?: PluginToolProgressSink,
  ) => Promise<PluginToolExecuteResult>
  /** Cross the isolate to abort this tool's in-flight hub call. */
  readonly abort?: (ctx: RemotePluginToolContext) => void | Promise<void>
  /** Re-fetch the hub's descriptor set after an `UnknownPluginToolError` (a stale descriptor or a plugin reloaded mid-generation). */
  readonly refresh?: () => void | Promise<void>
}

/** Raised when a hub plugin tool returns a result that cannot cross the isolate. */
export class PluginToolNotSerializableError extends Schema.TaggedErrorClass<PluginToolNotSerializableError>(
  "PluginToolNotSerializableError",
)("PluginTool.NotSerializable", {
  toolID: Schema.String,
  detail: Schema.String,
}) {
  constructor(
    override readonly toolID: string,
    override readonly detail: string,
  ) {
    super({ toolID, detail })
    this.name = "PluginToolNotSerializableError"
  }
  override get message() {
    return `plugin tool "${this.toolID}" returned a non-serializable ${this.detail}`
  }
}

/** Whether `value` is a worker-side remote definition rather than a real `ToolDefinition`. */
export function isRemotePluginToolDefinition(value: unknown): value is RemotePluginToolDefinition {
  if (typeof value !== "object" || value === null) return false
  const candidate = value as Record<string, unknown>
  return (
    typeof candidate.description === "string" &&
    typeof candidate.execute === "function" &&
    Object.hasOwn(candidate, "jsonSchema")
  )
}

/** The optional hub callbacks each rebuilt remote definition carries: the abort bridge (adapted from a {@link PluginToolAbort}) and the lazy-refresh. */
export interface RemotePluginToolCallbacks {
  readonly abort?: PluginToolAbort
  readonly refresh?: () => void | Promise<void>
}

/** Rebuild the `Hooks.tool` map a worker's reduced `Plugin.Service` lists — one remote definition per descriptor. */
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
