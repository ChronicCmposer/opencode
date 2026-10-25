// Phase 6.3 — worker-hosted plugin hooks (plan decision line 70).
//
// Plugins split across the process boundary: the hub hosts adapters, global
// events and auth; the *worker* hosts the model-facing hooks (`chat.params`,
// `chat.headers`) and the tool hooks (`tool.execute.before/after`). A hook runs
// in the worker isolate so it can see worker-local state (provider context,
// tool args) without an RPC per call.
//
// Two further hooks, `experimental.chat.system.transform` and
// `experimental.chat.messages.transform`, are HUB PROXIES rather than
// hub-resolved effects: the worker runs any worker-local function hooks, then
// forwards the structuredClone-safe payload to a hub-resident handler (the
// injected `options.systemTransform` / `options.messagesTransform`) that owns
// plugin loading. They cross the isolate as data by RPC, not as a serialized
// effect, because the real plugin function lives on the hub.
//
// Contract mirrors `@opencode-ai/plugin` `Hooks`: each hook receives an `input`
// and a mutable `output`; hooks run in registration order and each observes the
// previous hook's mutations. A throwing hook aborts the chain (fail loud).

/**
 * The mutable `chat.params` output a hook rewrites. The sampling fields are
 * optional: an absent field means "the worker's own baseline", so a plugin need
 * only set what it changes and a worker that does not advertise a field cannot
 * claim one. `options` is always present (the merged provider options).
 */
export interface ChatParamsOutput {
  temperature?: number
  topP?: number
  topK?: number
  maxOutputTokens?: number
  options: Record<string, unknown>
}

export interface ChatHeadersOutput {
  headers: Record<string, string>
}

export interface ToolBeforeOutput {
  args: unknown
}

export interface ToolAfterOutput {
  title: string
  output: string
  metadata: unknown
}

export interface SystemTransformInput {
  readonly sessionID?: string
  readonly model?: unknown
}

/** Mutable `string[]` the `experimental.chat.system.transform` hook rewrites. */
export interface SystemTransformOutput {
  system: string[]
}

export interface MessagesTransformInput {
  readonly sessionID?: string
  readonly model?: unknown
}

/** Mutable message list the `experimental.chat.messages.transform` hook rewrites. */
export interface MessagesTransformOutput {
  messages: unknown[]
}

export interface WorkerPluginHooks {
  readonly "chat.params"?: (input: unknown, output: ChatParamsOutput) => void | Promise<void>
  readonly "chat.headers"?: (input: unknown, output: ChatHeadersOutput) => void | Promise<void>
  readonly "tool.execute.before"?: (input: unknown, output: ToolBeforeOutput) => void | Promise<void>
  readonly "tool.execute.after"?: (input: unknown, output: ToolAfterOutput) => void | Promise<void>
  readonly "experimental.chat.system.transform"?: (
    input: SystemTransformInput,
    output: SystemTransformOutput,
  ) => void | Promise<void>
  readonly "experimental.chat.messages.transform"?: (
    input: MessagesTransformInput,
    output: MessagesTransformOutput,
  ) => void | Promise<void>
}

/**
 * The model-facing params a plugin changed, resolved on the hub into plain data.
 * A field present here is applied over the worker's own baseline; `null` means
 * the plugin set the field to `undefined` (e.g. `maxOutputTokens: undefined`).
 */
export interface ResolvedChatParams {
  readonly temperature?: number | null
  readonly topP?: number | null
  readonly topK?: number | null
  readonly maxOutputTokens?: number | null
  readonly options?: Readonly<Record<string, unknown>>
}

/** The headers a plugin added/overrode, resolved on the hub into plain data. */
export interface ResolvedChatHeaders {
  readonly headers?: Readonly<Record<string, string>>
}

/**
 * A plugin's effect, pre-resolved on the hub and shipped as structuredClone-safe
 * data. The hub owns plugin loading and can run a hook against the worker's
 * serialized model; the resulting mutation crosses the isolate as data because a
 * hook *function* can never be structured-cloned.
 */
export interface ResolvedWorkerPluginEffect {
  readonly "chat.params"?: ResolvedChatParams
  readonly "chat.headers"?: ResolvedChatHeaders
}

export interface WorkerPlugin {
  readonly name: string
  readonly hooks: WorkerPluginHooks
  /**
   * A hub-resolved effect. Applied (in plugin order) after the plugin's own
   * function hooks; absent for the in-process function-hook path.
   */
  readonly resolved?: ResolvedWorkerPluginEffect
}

export interface WorkerPluginHost {
  readonly names: readonly string[]
  chatParams(input: unknown, output: ChatParamsOutput): Promise<ChatParamsOutput>
  chatHeaders(input: unknown, output: ChatHeadersOutput): Promise<ChatHeadersOutput>
  toolBefore(input: unknown, output: ToolBeforeOutput): Promise<ToolBeforeOutput>
  toolAfter(input: unknown, output: ToolAfterOutput): Promise<ToolAfterOutput>
  systemTransform(input: SystemTransformInput, output: SystemTransformOutput): Promise<SystemTransformOutput>
  messagesTransform(input: MessagesTransformInput, output: MessagesTransformOutput): Promise<MessagesTransformOutput>
}

/** Raised when two plugins share a name (fail loud). */
export class DuplicatePluginError extends Error {
  constructor(name: string) {
    super(`duplicate plugin: ${name}`)
    this.name = "DuplicatePluginError"
  }
}

/**
 * Run one hook across every plugin. The caller selects the hook, so the
 * function's input/output types are pinned by the selector with no cast.
 */
async function runHook<TInput, TOutput>(
  plugins: readonly WorkerPlugin[],
  select: (hooks: WorkerPluginHooks) => ((input: TInput, output: TOutput) => void | Promise<void>) | undefined,
  input: TInput,
  output: TOutput,
): Promise<TOutput> {
  for (const plugin of plugins) {
    const fn = select(plugin.hooks)
    if (fn === undefined) continue
    await fn(input, output)
  }
  return output
}

/**
 * The mutable sampling fields a `chat.params` plugin may set. Exported so the
 * hub-side resolver (`init.ts`) diffs exactly the same keys this host applies.
 */
export const CHAT_PARAM_KEYS = ["temperature", "topP", "topK", "maxOutputTokens"] as const
type ChatParamKey = (typeof CHAT_PARAM_KEYS)[number]

/**
 * Apply every plugin's hub-resolved `chat.params` diff over `output`, in plugin
 * order. A field absent from the resolved effect is left untouched; a `null`
 * field clears the value (the plugin set it to `undefined`).
 */
function applyResolvedParams(plugins: readonly WorkerPlugin[], output: ChatParamsOutput): ChatParamsOutput {
  const next: ChatParamsOutput = { ...output, options: { ...output.options } }
  for (const plugin of plugins) {
    const resolved = plugin.resolved?.["chat.params"]
    if (resolved === undefined) continue
    for (const key of CHAT_PARAM_KEYS) {
      const value = resolved[key]
      if (value === undefined) continue
      next[key] = value ?? undefined
    }
    if (resolved.options !== undefined) {
      next.options = { ...next.options, ...resolved.options }
    }
  }
  return next
}

/** Merge every plugin's hub-resolved `chat.headers` over `output`, in plugin order. */
function applyResolvedHeaders(plugins: readonly WorkerPlugin[], output: ChatHeadersOutput): ChatHeadersOutput {
  let headers = output.headers
  for (const plugin of plugins) {
    const resolved = plugin.resolved?.["chat.headers"]
    if (resolved?.headers === undefined) continue
    headers = { ...headers, ...resolved.headers }
  }
  return { ...output, headers }
}

export function createWorkerPluginHost(
  plugins: readonly WorkerPlugin[],
  options?: {
    readonly systemTransform?: (
      input: SystemTransformInput,
      output: SystemTransformOutput,
    ) => Promise<SystemTransformOutput>
    readonly messagesTransform?: (
      input: MessagesTransformInput,
      output: MessagesTransformOutput,
    ) => Promise<MessagesTransformOutput>
  },
): WorkerPluginHost {
  const names = plugins.map((plugin) => plugin.name)
  const seen = new Set<string>()
  for (const name of names) {
    if (seen.has(name)) throw new DuplicatePluginError(name)
    seen.add(name)
  }

  return {
    names,
    chatParams: async (input, output) =>
      applyResolvedParams(plugins, await runHook(plugins, (hooks) => hooks["chat.params"], input, output)),
    chatHeaders: async (input, output) =>
      applyResolvedHeaders(plugins, await runHook(plugins, (hooks) => hooks["chat.headers"], input, output)),
    toolBefore: (input, output) => runHook(plugins, (hooks) => hooks["tool.execute.before"], input, output),
    toolAfter: (input, output) => runHook(plugins, (hooks) => hooks["tool.execute.after"], input, output),
    // Plugin function hooks run first (registration order), then the injected
    // hub-resolved transform; absent both, the output passes through unchanged.
    systemTransform: async (input, output) => {
      const transformed = await runHook(
        plugins,
        (hooks) => hooks["experimental.chat.system.transform"],
        input,
        output,
      )
      if (options?.systemTransform === undefined) return transformed
      return options.systemTransform(input, transformed)
    },
    // Same composition as `systemTransform`: worker-local function hooks first,
    // then the injected hub-resolved transform; absent both, the output passes
    // through by identity.
    messagesTransform: async (input, output) => {
      const transformed = await runHook(
        plugins,
        (hooks) => hooks["experimental.chat.messages.transform"],
        input,
        output,
      )
      if (options?.messagesTransform === undefined) return transformed
      return options.messagesTransform(input, transformed)
    },
  }
}

export * as WorkerPluginHostModule from "./plugin-host"
