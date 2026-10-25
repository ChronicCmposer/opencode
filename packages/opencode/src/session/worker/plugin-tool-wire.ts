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

/** The clone-safe projection of one plugin tool, shipped to the worker init. */
export interface PluginToolDescriptor {
  readonly id: string
  readonly description: string
  readonly jsonSchema: unknown
}

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
  execute(input: PluginToolExecuteInput): Promise<PluginToolExecuteResult>
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
  readonly jsonSchema: unknown
  readonly execute: (args: unknown, ctx: RemotePluginToolContext) => Promise<PluginToolExecuteResult>
}

/**
 * Whether `value` is a worker-side remote definition rather than a real
 * `ToolDefinition`. A real plugin tool carries a Zod `args` raw shape and never a
 * `jsonSchema` key; {@link remotePluginToolHooks} always sets `jsonSchema` (even
 * when it is `undefined`), so the own-key presence distinguishes them without a
 * fragile brand.
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
 * Rebuild the `Hooks.tool` map a worker's reduced `Plugin.Service` lists — one
 * remote definition per descriptor. Each call forwards to `executor` with the
 * descriptor's id and the call frame; the hub owns the real plugin instance and
 * runs it.
 */
export function remotePluginToolHooks(
  descriptors: readonly PluginToolDescriptor[],
  executor: PluginToolExecutor,
): { readonly tool: Record<string, RemotePluginToolDefinition> } {
  const tool: Record<string, RemotePluginToolDefinition> = {}
  for (const descriptor of descriptors) {
    tool[descriptor.id] = {
      description: descriptor.description,
      jsonSchema: descriptor.jsonSchema,
      execute: (args, ctx) =>
        executor.execute({
          toolID: descriptor.id,
          sessionID: ctx.sessionID,
          messageID: ctx.messageID,
          callID: ctx.callID ?? "",
          agent: ctx.agent,
          args,
        }),
    }
  }
  return { tool }
}
