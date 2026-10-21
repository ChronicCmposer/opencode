// Phase 6.3 — worker-hosted plugin hooks (plan decision line 70).
//
// Plugins split across the process boundary: the hub hosts adapters, global
// events and auth; the *worker* hosts the model-facing hooks (`chat.params`,
// `chat.headers`) and the tool hooks (`tool.execute.before/after`). A hook runs
// in the worker isolate so it can see worker-local state (provider context,
// tool args) without an RPC per call.
//
// Contract mirrors `@opencode-ai/plugin` `Hooks`: each hook receives an `input`
// and a mutable `output`; hooks run in registration order and each observes the
// previous hook's mutations. A throwing hook aborts the chain (fail loud).

export interface ChatParamsOutput {
  temperature: number
  topP: number
  topK: number
  maxOutputTokens: number | undefined
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

export interface WorkerPluginHooks {
  readonly "chat.params"?: (input: unknown, output: ChatParamsOutput) => void | Promise<void>
  readonly "chat.headers"?: (input: unknown, output: ChatHeadersOutput) => void | Promise<void>
  readonly "tool.execute.before"?: (input: unknown, output: ToolBeforeOutput) => void | Promise<void>
  readonly "tool.execute.after"?: (input: unknown, output: ToolAfterOutput) => void | Promise<void>
}

export interface WorkerPlugin {
  readonly name: string
  readonly hooks: WorkerPluginHooks
}

export interface WorkerPluginHost {
  readonly names: readonly string[]
  chatParams(input: unknown, output: ChatParamsOutput): Promise<ChatParamsOutput>
  chatHeaders(input: unknown, output: ChatHeadersOutput): Promise<ChatHeadersOutput>
  toolBefore(input: unknown, output: ToolBeforeOutput): Promise<ToolBeforeOutput>
  toolAfter(input: unknown, output: ToolAfterOutput): Promise<ToolAfterOutput>
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
async function runHook<TOutput>(
  plugins: readonly WorkerPlugin[],
  select: (hooks: WorkerPluginHooks) => ((input: unknown, output: TOutput) => void | Promise<void>) | undefined,
  input: unknown,
  output: TOutput,
): Promise<TOutput> {
  for (const plugin of plugins) {
    const fn = select(plugin.hooks)
    if (fn === undefined) continue
    await fn(input, output)
  }
  return output
}

export function createWorkerPluginHost(plugins: readonly WorkerPlugin[]): WorkerPluginHost {
  const names = plugins.map((plugin) => plugin.name)
  const seen = new Set<string>()
  for (const name of names) {
    if (seen.has(name)) throw new DuplicatePluginError(name)
    seen.add(name)
  }

  return {
    names,
    chatParams: (input, output) => runHook(plugins, (hooks) => hooks["chat.params"], input, output),
    chatHeaders: (input, output) => runHook(plugins, (hooks) => hooks["chat.headers"], input, output),
    toolBefore: (input, output) => runHook(plugins, (hooks) => hooks["tool.execute.before"], input, output),
    toolAfter: (input, output) => runHook(plugins, (hooks) => hooks["tool.execute.after"], input, output),
  }
}

export * as WorkerPluginHostModule from "./plugin-host"
