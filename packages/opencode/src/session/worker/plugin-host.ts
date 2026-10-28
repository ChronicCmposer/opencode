// Worker-hosted plugin hooks.
//
// Plugins split across the process boundary: the hub hosts adapters, global
// events and auth; the *worker* hosts the model-facing hooks (`chat.params`,
// `chat.headers`) and the tool hooks (`tool.execute.before/after`). A hook runs
// in the worker isolate so it can see worker-local state (provider context,
// tool args) without an RPC per call.
//
// Two further hooks, `experimental.chat.system.transform` and
// `chat.message`, `experimental.chat.system.transform` and
// `experimental.chat.messages.transform`, are HUB PROXIES rather than
// hub-resolved effects: the worker runs any worker-local function hooks, then
// forwards the structuredClone-safe payload to a hub-resident handler (the
// injected `options.systemTransform` / `options.messagesTransform`) or the
// generic hub bridge that owns plugin loading. They cross the isolate as data by RPC, not as a serialized
// effect, because the real plugin function lives on the hub.
//
// Contract mirrors `@opencode-ai/plugin` `Hooks`: each hook receives an `input`
// and a mutable `output`; hooks run in registration order and each observes the
// previous hook's mutations. A throwing hook aborts the chain (fail loud).

export {
  DECLARED_ONLY_HOOKS,
  UnclassifiedPluginHookError,
  UnsupportedPluginHookError,
  WORKER_HOOK_ROUTES,
  assertPluginHookAdmission,
  isDeclaredOnlyHook,
  pluginHookAdmissionError,
  workerHookRoute,
  type WorkerHookName,
  type WorkerHookRoute,
} from "@/plugin/routes"

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

/** Mutable user message + parts the `chat.message` hook rewrites. */
export interface ChatMessageOutput {
  message: unknown
  parts: unknown[]
}

/** Mutable environment map the `shell.env` hook rewrites. */
export interface ShellEnvOutput {
  env: Record<string, string>
}

/** Mutable tool description/schema the `tool.definition` hook rewrites. */
export interface ToolDefinitionOutput {
  description: string
  parameters: unknown
  jsonSchema: unknown
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
  readonly "chat.message"?: (input: unknown, output: ChatMessageOutput) => void | Promise<void>
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
  readonly "shell.env"?: (input: unknown, output: ShellEnvOutput) => void | Promise<void>
  readonly "tool.definition"?: (input: unknown, output: ToolDefinitionOutput) => void | Promise<void>
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
   * A hub-resolved effect, used as the degraded-boot fallback when no hub bridge
   * is wired. With a bridge the real hook runs per request, so a model switch
   * cannot freeze `chat.params`/`chat.headers` at the init model.
   */
  readonly resolved?: ResolvedWorkerPluginEffect
  /**
   * The hub-proxy hooks this plugin declares. Their functions live on the hub
   * (they cannot cross the isolate), so the worker forwards each dispatch over
   * the control port instead of dropping them.
   */
  readonly proxied?: readonly string[]
  /**
   * Every trigger-shaped hook the source declared, including routed-but-
   * unsupported ones. The worker needs this to tell an undeclared unsupported
   * hook (a true no-op) from one the plugin actually implements (a typed
   * failure). Only clone-safe names cross the isolate.
   */
  readonly declared?: readonly string[]
}

export interface WorkerPluginHost {
  readonly names: readonly string[]
  chatParams(input: unknown, output: ChatParamsOutput): Promise<ChatParamsOutput>
  chatHeaders(input: unknown, output: ChatHeadersOutput): Promise<ChatHeadersOutput>
  toolBefore(input: unknown, output: ToolBeforeOutput): Promise<ToolBeforeOutput>
  toolAfter(input: unknown, output: ToolAfterOutput): Promise<ToolAfterOutput>
  chatMessage(input: unknown, output: ChatMessageOutput): Promise<ChatMessageOutput>
  systemTransform(input: SystemTransformInput, output: SystemTransformOutput): Promise<SystemTransformOutput>
  messagesTransform(input: MessagesTransformInput, output: MessagesTransformOutput): Promise<MessagesTransformOutput>
  shellEnv(input: unknown, output: ShellEnvOutput): Promise<ShellEnvOutput>
  toolDefinition(input: unknown, output: ToolDefinitionOutput): Promise<ToolDefinitionOutput>
}

/**
 * Forwards one hub-proxied hook to the hub, where the real plugin function
 * runs. The worker only ships the clone-safe `(name, input, output)` payload.
 */
export type WorkerPluginHubHook = (name: string, input: unknown, output: unknown) => Promise<unknown>

export interface WorkerPluginHostOptions {
  readonly systemTransform?: (
    input: SystemTransformInput,
    output: SystemTransformOutput,
  ) => Promise<SystemTransformOutput>
  readonly messagesTransform?: (
    input: MessagesTransformInput,
    output: MessagesTransformOutput,
  ) => Promise<MessagesTransformOutput>
  /** The single hub-proxy bridge for every routed-but-hub-owned hook. */
  readonly hubHook?: WorkerPluginHubHook
}

/** Raised when two plugins share a name (fail loud). */
export class DuplicatePluginError extends Error {
  constructor(name: string) {
    super(`duplicate plugin: ${name}`)
    this.name = "DuplicatePluginError"
  }
}

/**
 * Raised when a hook replaces a field with a value that cannot be applied to the
 * caller's reference (for example `parts` set to a string). Silently keeping the
 * old value would make the hook's replacement vanish, so the mismatch fails at
 * the hook boundary instead.
 */
export class NonContainerReplacementError extends Error {
  constructor(
    readonly field: string,
    readonly kind: string,
  ) {
    super(`plugin replaced "${field}" with a ${kind}; the field requires a matching container`)
    this.name = "NonContainerReplacementError"
  }
}

function kindOf(value: unknown): string {
  if (value === null) return "null"
  if (Array.isArray(value)) return "array"
  return typeof value
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

/**
 * Replace `target`'s contents with `source`'s without changing `target`'s
 * identity, so a caller holding the original object observes the mutation. Only
 * same-shaped containers are reconciled; a primitive/array/object mismatch
 * returns `false` and the caller must assign the replacement.
 */
function replaceContents(target: unknown, source: unknown): boolean {
  if (target === source) return true
  if (Array.isArray(target) && Array.isArray(source)) {
    target.splice(0, target.length, ...source)
    return true
  }
  if (!isRecord(target) || !isRecord(source)) return false
  for (const key of Object.keys(target)) {
    if (!Object.hasOwn(source, key)) delete target[key]
  }
  for (const [key, value] of Object.entries(source)) target[key] = value
  return true
}

/**
 * Copy `source`'s fields onto `target` in place (arrays are spliced). A
 * non-container replacement cannot be applied in place, so it fails loud rather
 * than leaving the caller holding the unchanged object.
 */
function applyInPlace<T extends object>(target: T, source: unknown, field = "output"): T {
  if (!replaceContents(target, source)) throw new NonContainerReplacementError(field, kindOf(source))
  return target
}

/**
 * Apply a `tool.execute.before` result in place. `output.args` is special: call
 * sites keep a reference to the args object they passed in, so a hook that
 * replaces `output.args` must have its new contents copied into that original
 * object rather than handed back as a fresh reference the caller never reads.
 */
function applyToolBefore(output: ToolBeforeOutput, next: ToolBeforeOutput): ToolBeforeOutput {
  if (next.args !== output.args && !replaceContents(output.args, next.args)) output.args = next.args
  for (const key of Object.keys(output)) {
    if (key !== "args" && !Object.hasOwn(next, key)) delete (output as unknown as Record<string, unknown>)[key]
  }
  for (const [key, value] of Object.entries(next)) {
    if (key !== "args") (output as unknown as Record<string, unknown>)[key] = value
  }
  return output
}

/** Apply a `chat.message` result in place, keeping the caller's message/parts identity. */
function applyChatMessage(output: ChatMessageOutput, next: ChatMessageOutput): ChatMessageOutput {
  if (next.message !== output.message && !replaceContents(output.message, next.message)) output.message = next.message
  // `parts` is typed as an array on the contract: a plugin that replaces it with
  // a non-array cannot preserve the caller's reference, so fail loud instead of
  // dropping the replacement. `output.parts` (not `next.parts`) is the shape
  // that decides whether an in-place splice is possible.
  if (next.parts !== output.parts && !replaceContents(output.parts, next.parts)) {
    throw new NonContainerReplacementError("parts", kindOf(next.parts))
  }
  return output
}

/**
 * The hooks each host's loaded plugins declared, keyed by host identity. A
 * `WorkerPluginHost` exposes methods for every route regardless of declaration,
 * so this side table is the only way to tell "declared but unsupported" from
 * "not declared at all" without widening the host interface.
 */
const declaredHooksByHost = new WeakMap<WorkerPluginHost, ReadonlySet<string>>()

/** Whether any loaded plugin behind `host` declared `hook`. */
export function workerPluginHostDeclaresHook(host: WorkerPluginHost, hook: string): boolean {
  return declaredHooksByHost.get(host)?.has(hook) ?? false
}

function declaredHooksOf(plugins: readonly WorkerPlugin[]): ReadonlySet<string> {
  const declared = new Set<string>()
  for (const plugin of plugins) {
    for (const [name, value] of Object.entries(plugin.hooks)) {
      if (typeof value === "function") declared.add(name)
    }
    for (const name of plugin.proxied ?? []) declared.add(name)
    for (const name of plugin.declared ?? []) declared.add(name)
  }
  return declared
}

export function createWorkerPluginHost(
  plugins: readonly WorkerPlugin[],
  options?: WorkerPluginHostOptions,
): WorkerPluginHost {
  const names = plugins.map((plugin) => plugin.name)
  const seen = new Set<string>()
  for (const name of names) {
    if (seen.has(name)) throw new DuplicatePluginError(name)
    seen.add(name)
  }

  const host: WorkerPluginHost = {
    names,
    // `chat.params` / `chat.headers` must resolve against the model of THIS
    // request: a session can switch models mid-run, so the init-resolved diff is
    // only a degraded-boot fallback. With a hub bridge the real hook runs per
    // request and its returned mutation is applied to the caller's object.
    chatParams: async (input, output) => {
      const local = await runHook(plugins, (hooks) => hooks["chat.params"], input, output)
      const forward = options?.hubHook
      if (forward === undefined) return applyResolvedParams(plugins, local)
      return applyInPlace(local, await forward("chat.params", input, local))
    },
    chatHeaders: async (input, output) => {
      const local = await runHook(plugins, (hooks) => hooks["chat.headers"], input, output)
      const forward = options?.hubHook
      if (forward === undefined) return applyResolvedHeaders(plugins, local)
      return applyInPlace(local, await forward("chat.headers", input, local))
    },
    toolBefore: async (input, output) =>
      applyToolBefore(
        output,
        await forwardHook(
          plugins,
          (hooks) => hooks["tool.execute.before"],
          "tool.execute.before",
          input,
          output,
          options,
        ),
      ),
    toolAfter: async (input, output) =>
      applyInPlace(
        output,
        await forwardHook(
          plugins,
          (hooks) => hooks["tool.execute.after"],
          "tool.execute.after",
          input,
          output,
          options,
        ),
      ),
    chatMessage: async (input, output) =>
      applyChatMessage(
        output,
        await forwardHook(plugins, (hooks) => hooks["chat.message"], "chat.message", input, output, options),
      ),
    // Plugin function hooks run first (registration order), then the injected
    // hub-resolved transform; absent both, the output passes through unchanged.
    systemTransform: async (input, output) => {
      const transformed = await runHook(plugins, (hooks) => hooks["experimental.chat.system.transform"], input, output)
      if (options?.systemTransform !== undefined) return options.systemTransform(input, transformed)
      return forwardHook(plugins, () => undefined, "experimental.chat.system.transform", input, transformed, options)
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
      if (options?.messagesTransform !== undefined) return options.messagesTransform(input, transformed)
      return forwardHook(plugins, () => undefined, "experimental.chat.messages.transform", input, transformed, options)
    },
    shellEnv: async (input, output) =>
      applyInPlace(
        output,
        await forwardHook(plugins, (hooks) => hooks["shell.env"], "shell.env", input, output, options),
      ),
    // `parameters` is an Effect Schema on the worker, which cannot cross the
    // isolate. The hook's model-facing effect is description + JSON schema, so
    // those (clone-safe) fields are forwarded; the worker's local `parameters`
    // stays authoritative. The real plugin still runs on the hub, never here.
    // The result is applied to the caller's object in place: the registry reads
    // `output` after the trigger and ignores the returned value.
    toolDefinition: async (input, output) => {
      const local = await runHook(plugins, (hooks) => hooks["tool.definition"], input, output)
      const forward = options?.hubHook
      if (forward === undefined) return local
      const result = (await forward("tool.definition", input, {
        description: local.description,
        jsonSchema: local.jsonSchema,
      })) as { readonly description?: unknown; readonly jsonSchema?: unknown }
      return applyInPlace(local, {
        ...local,
        description: typeof result.description === "string" ? result.description : local.description,
        jsonSchema: "jsonSchema" in result ? result.jsonSchema : local.jsonSchema,
      })
    },
  }
  declaredHooksByHost.set(host, declaredHooksOf(plugins))
  return host
}

/**
 * Run a worker-local function hook (if the in-process source carries one), then
 * forward the result to the hub bridge. Absent a bridge the local result passes
 * through by identity; the hub hook is never a silent no-op because the route
 * table only classifies a name as `hub-proxy` when the hub owns it.
 */
async function forwardHook<TInput, TOutput>(
  plugins: readonly WorkerPlugin[],
  select: (hooks: WorkerPluginHooks) => ((input: TInput, output: TOutput) => void | Promise<void>) | undefined,
  name: string,
  input: TInput,
  output: TOutput,
  options: WorkerPluginHostOptions | undefined,
): Promise<TOutput> {
  const local = await runHook(plugins, select, input, output)
  const forward = options?.hubHook
  if (forward === undefined) return local
  return (await forward(name, input, local)) as TOutput
}

export * as WorkerPluginHostModule from "./plugin-host"
