// Hub-proxy handler/authority map and per-worker bound authority helpers.
import { Context, Deferred, Effect, Option } from "effect"
import { z } from "zod"
import path from "node:path"
import type { Hooks, ToolContext, ToolDefinition } from "@opencode-ai/plugin"
import { BackgroundJob } from "@opencode-ai/core/background-job"
import type { Info, Status } from "@opencode-ai/core/background-job"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { InstanceRef } from "@/effect/instance-ref"
import { EffectBridge } from "@/effect/bridge"
import { SessionStatus } from "@/session/status"
import { Permission } from "@/permission"
import { Question } from "@/question"
import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import type { SessionID } from "@/session/schema"
import { nextDepth } from "@/session/depth"
import { SessionScopeError, type SessionCancelRequest, type SessionChildrenRequest, type SessionCreateRequest, type SessionGetRequest, type SessionSpawnRequest, type SessionSpawnResult } from "./subagent"
import { type RunBarrierPromotionOutcome } from "@/session/run-barrier"
import { type CancelOutcome } from "./cancel"
import { Plugin } from "@/plugin"
import { UnsupportedPluginHookError, workerHookRoute } from "@/plugin/routes"
import type { MessagesTransformInput, MessagesTransformOutput, SystemTransformInput, SystemTransformOutput } from "./plugin-host"
import type { HubProxyHandlers, HubProxyStreamHandler } from "./hub-proxy-wire"
import { serveHubProxy } from "./hub-proxy-wire"
import type { PluginToolDescriptor, PluginToolExecuteInput, PluginToolExecuteResult, PluginToolProgress, PluginToolProgressSink } from "./plugin-tool-wire"
import { PluginToolNotSerializableError } from "./plugin-tool-wire"
import { Session } from "@/session/session"
import { SessionPromptService } from "@/session/prompt-service"
import { Truncate } from "@/tool/truncate"
import { pluginToolJsonSchema } from "@/tool/registry"
import { MCP } from "@/mcp"
import { McpCatalog } from "@/mcp/catalog"
import { LspTool, Parameters as LspParameters } from "@/tool/lsp"
import DESCRIPTION_LSP from "@/tool/lsp.txt"
import { ToolJsonSchema } from "@/tool/json-schema"
import type { Tool } from "@/tool/tool"
import { LSP } from "@/lsp/lsp"
import { Agent } from "@/agent/agent"

// ---------------------------------------------------------------------------
// Production hub-proxy handlers.
// ---------------------------------------------------------------------------

/**
 * The hub-resident authorities a hub-proxy handler map dispatches to. The worker
 * multiplexes all three over one control port (`session-worker.ts`), so the
 * handler map is keyed by the exact bare methods the worker-side proxy layers
 * call (`layer.ts` `sessionStatusProxyLayer` / `permissionProxyLayer` /
 * `questionProxyLayer`).
 */
export interface HubProxyAuthorities {
  readonly sessionStatus: SessionStatus.Interface
  readonly permission: Permission.Interface
  readonly question: Question.Interface
  /**
   * Hub-owned revert clearing. `revert` is immutable session authority (D6):
   * a worker may not publish `revert: null` through its projection, so the
   * worker's revert/unrevert path calls this authority and the hub clears its
   * own row under the session context. Absent means the RPC fails loud.
   */
  readonly clearRevert?: (sessionID: string) => Promise<void>
  /**
   * Session-spawn authority. The production hub runs `Session.spawn` under the
   * parent's captured runtime context, so the child is dispatched to its own
   * per-session worker. Absent means the RPC fails loud.
   */
  readonly sessionSpawn?: (request: SessionSpawnRequest) => Promise<SessionSpawnResult>
  /**
   * Hub-side child-session scope. A bound worker's `Session.create`/`get`/
   * `children` route here so the create/projection/read happens on the hub, not
   * in the worker's single-session database. Each authority verifies the target
   * belongs to the worker's own subtree.
   */
  readonly sessionCreate?: (request: SessionCreateRequest) => Promise<Session.Info>
  readonly sessionGet?: (request: SessionGetRequest) => Promise<Session.Info | null>
  readonly sessionChildren?: (request: SessionChildrenRequest) => Promise<readonly Session.Info[]>
  readonly sessionCancel?: (request: SessionCancelRequest) => Promise<CancelOutcome | undefined>
  /**
   * Optional system-prompt transform authority. The worker cannot run
   * `experimental.chat.system.transform` itself (plugins are loaded on the hub),
   * so it calls the hub, which mutates and returns `output`. Absent means the
   * handler returns `output` unchanged (flag-off / no plugin).
   */
  readonly systemTransform?: (
    input: SystemTransformInput,
    output: SystemTransformOutput,
  ) => Promise<SystemTransformOutput>
  /**
   * Optional message-list transform authority. The worker cannot run
   * `experimental.chat.messages.transform` itself (plugins are loaded on the hub),
   * so it calls the hub, which mutates and returns `output`. Absent means the
   * handler returns `output` unchanged (flag-off / no plugin).
   */
  readonly messagesTransform?: (
    input: MessagesTransformInput,
    output: MessagesTransformOutput,
  ) => Promise<MessagesTransformOutput>
  /**
   * Optional hub-proxied plugin trigger. The worker never invokes a hook the hub
   * owns (`tool.execute.before/after`, `shell.env`, `tool.definition`, the
   * transforms, `chat.message`); it forwards `(name, input, output)` here, where
   * the real plugin instance runs under the worker's session context. Absent means
   * the output passes through unchanged (no configured plugin). The authority
   * rejects a hook whose route is `unsupported`, so a worker cannot smuggle an
   * unsupported trigger across the port; a worker-hosted hook may be forwarded
   * too, which is how `chat.params`/`chat.headers` resolve per request.
   */
  readonly pluginTrigger?: (name: string, input: unknown, output: unknown) => Promise<unknown>
  /**
   * Optional plugin-tool execute authority. The worker cannot run a plugin tool's
   * `execute` (the plugin instance lives on the hub), so it forwards the call
   * here, where the hub-owned definition runs and a clone-safe result returns.
   * `emit` is the streaming sink that `pluginTool.metadata` progress events are
   * pushed through; a non-streaming caller supplies a no-op. Unlike the transforms
   * there is no identity pass-through: absent means the handler rejects.
   */
  readonly pluginTool?: (
    input: PluginToolExecuteInput,
    emit: PluginToolProgressSink,
  ) => Promise<PluginToolExecuteResult>
  /**
   * Optional plugin-tool list authority: the clone-safe descriptor refresh. The
   * worker ships descriptors at init; a plugin that reloads mid-generation leaves
   * them stale, so the worker can re-fetch the current list here. Absent means the
   * handler rejects (an empty list would silently hide a mis-wiring).
   */
  readonly pluginToolList?: () => Promise<readonly PluginToolDescriptor[]>
  /**
   * Optional plugin-tool abort authority: unwind an in-flight plugin tool's
   * `ctx.abort` by `(sessionID, callID)`. Best-effort — absent means the handler
   * resolves as a no-op rather than failing a cancel path.
   */
  readonly pluginToolAbort?: (input: { readonly sessionID: string; readonly callID: string }) => void | Promise<void>
  /**
   * Optional worker-job mirror authority. A session worker owns its process-local
   * `BackgroundJob` registry, so a foreground job is invisible to the hub's
   * promotion endpoint unless the worker registers it here. The hub mirrors the
   * running job in its own registry, which makes `list`/`promote` work; promotion
   * resolves the worker's `awaitPromotion` long-poll so the worker applies it to
   * the local registry (and fires the task notification).
   */
  readonly backgroundJob?: WorkerBackgroundJobAuthority
}

/** One worker-reported terminal job state, settled into the hub mirror. */
export interface WorkerBackgroundJobSettle {
  readonly id: string
  readonly status: Exclude<Status, "running">
  readonly output?: string
  readonly error?: string
}

/** The worker-job mirror surface exposed over the hub-proxy control port. */
export interface WorkerBackgroundJobAuthority {
  register(info: Info): Promise<void>
  settle(input: WorkerBackgroundJobSettle): Promise<void>
  /** Resolves `true` on promotion, `false` once the job settles or disappears. */
  awaitPromotion(id: string): Promise<boolean>
}

/**
 * Executes a hub authority effect for a worker request. The hub is a single
 * global runtime while a worker belongs to the instance that dispatched it, so
 * the runner provides that instance's context (captured at dispatch).
 */
export type HubProxyRunner = <A, E>(effect: Effect.Effect<A, E>) => Promise<A>

/**
 * The prompt authority's shared service key. Resolving it from
 * `prompt-service.ts` lets the hub run the authoritative children-first cancel
 * without importing `prompt.ts` (hub → prompt → run-state → hub would cycle) and
 * without declaring a second `@opencode/SessionPrompt` key.
 */
const promptAuthority = SessionPromptService.Service

/** The descriptive rejection an absent plugin-tool authority returns (fail loud). */
function missingPluginToolAuthority(what: string): Error {
  return new Error(`the hub has no "pluginTool" authority wired; cannot ${what}`)
}

/**
 * Build the hub-side handler map for the worker hub-proxy wire. Every method is
 * namespaced by its authority (`question.ask` vs `permission.ask`), so a single
 * multiplexed control port can never misroute a call — no request-shape guessing.
 */
export function buildHubProxyHandlers(authorities: HubProxyAuthorities, run: HubProxyRunner): HubProxyHandlers {
  // `session.clearRevert` is served even before the shared `HubProxyMethod` union
  // is extended, so the hub half of the authority is testable in isolation; the
  // worker-side client gains the method with the projection lane's wiring.
  const handlers: HubProxyHandlers & {
    readonly "session.clearRevert": (request: unknown) => Promise<void>
  } = {
    "sessionStatus.get": (sessionID) => run(authorities.sessionStatus.get(sessionID as SessionID)),
    "sessionStatus.set": (sessionID, status) =>
      run(authorities.sessionStatus.set(sessionID as SessionID, status as SessionStatus.Info)),
    "sessionStatus.list": () => run(authorities.sessionStatus.list()).then((entries) => [...entries]),
    "permission.ask": (input) => run(authorities.permission.ask(input as Parameters<Permission.Interface["ask"]>[0])),
    "permission.reply": (input) =>
      run(authorities.permission.reply(input as Parameters<Permission.Interface["reply"]>[0])),
    "permission.list": () => run(authorities.permission.list()),
    "question.ask": (input) => run(authorities.question.ask(input as Parameters<Question.Interface["ask"]>[0])),
    "question.reply": (input) => run(authorities.question.reply(input as Parameters<Question.Interface["reply"]>[0])),
    "question.reject": (requestID) =>
      run(authorities.question.reject(requestID as Parameters<Question.Interface["reject"]>[0])),
    "question.list": () => run(authorities.question.list()),
    // Always served: a worker's `experimental.chat.system.transform` reaches the
    // hub, which runs the plugin hook and returns the mutated `{system}`. Absent
    // an authority the output passes through unchanged (never an unknown method).
    systemTransform: (input, output) =>
      authorities.systemTransform === undefined
        ? (output as SystemTransformOutput)
        : authorities.systemTransform(input as SystemTransformInput, output as SystemTransformOutput),
    // Always served: a worker's `experimental.chat.messages.transform` reaches the
    // hub, which runs the plugin hook and returns the mutated `{messages}`. Absent
    // an authority the output passes through unchanged (never an unknown method).
    messagesTransform: (input, output) =>
      authorities.messagesTransform === undefined
        ? (output as MessagesTransformOutput)
        : authorities.messagesTransform(input as MessagesTransformInput, output as MessagesTransformOutput),
    // Always served: a worker forwards every hub-proxied hook through this one
    // method. Absent an authority (no configured plugin) the output is unchanged.
    pluginTrigger: (name, input, output) =>
      authorities.pluginTrigger === undefined ? output : authorities.pluginTrigger(name as string, input, output),
    // Always served, but with no identity fallback: an execute has no result to
    // hand back, so an absent authority is a descriptive rejection — never an
    // "unknown hub-proxy method" (that would mask a missing wiring, not a typo).
    // This non-streaming entry supplies a no-op emit; `buildHubProxyStreamHandlers`
    // overrides it with the real sink when the port is served with streaming.
    "pluginTool.execute": (input) =>
      authorities.pluginTool === undefined
        ? Promise.reject(missingPluginToolAuthority("execute a plugin-provided tool"))
        : authorities.pluginTool(input as PluginToolExecuteInput, () => {}),
    // The descriptor refresh RPC; absent a list authority there is no useful empty
    // fallback, so a typo'd wiring fails loud rather than silently listing nothing.
    "pluginTool.list": () =>
      authorities.pluginToolList === undefined
        ? Promise.reject(missingPluginToolAuthority("list plugin-provided tools"))
        : authorities.pluginToolList(),
    // Best-effort: no authority means nothing is in flight to abort, not an error.
    // A cancel path must never turn an abort into a tool failure.
    "pluginTool.abort": (input) =>
      authorities.pluginToolAbort?.(input as { readonly sessionID: string; readonly callID: string }),
    // Worker-owned jobs are mirrored into the hub registry when the worker
    // reports them. Absent an authority (a plain test hub) registration and
    // settlement are no-ops, but promotion never fabricates a `true`.
    "backgroundJob.register": (info) =>
      authorities.backgroundJob?.register(info as Info) ?? Promise.resolve(),
    "backgroundJob.settle": (input) =>
      authorities.backgroundJob?.settle(input as WorkerBackgroundJobSettle) ?? Promise.resolve(),
    "backgroundJob.awaitPromotion": (id) =>
      authorities.backgroundJob?.awaitPromotion(id as string) ?? Promise.resolve(false),
    "session.spawn": (request) =>
      authorities.sessionSpawn === undefined
        ? Promise.reject(new Error('the hub has no "sessionSpawn" authority wired; cannot spawn a session'))
        : authorities.sessionSpawn(request as SessionSpawnRequest),
    "session.create": (request) =>
      authorities.sessionCreate === undefined
        ? Promise.reject(new Error('the hub has no "sessionCreate" authority wired; cannot create a child session'))
        : authorities.sessionCreate(request as SessionCreateRequest),
    "session.get": (request) =>
      authorities.sessionGet === undefined
        ? Promise.reject(new Error('the hub has no "sessionGet" authority wired; cannot read a child session'))
        : authorities.sessionGet(request as SessionGetRequest),
    "session.children": (request) =>
      authorities.sessionChildren === undefined
        ? Promise.reject(new Error('the hub has no "sessionChildren" authority wired; cannot list child sessions'))
        : authorities.sessionChildren(request as SessionChildrenRequest),
    "session.cancel": (request) =>
      authorities.sessionCancel === undefined
        ? Promise.reject(new Error('the hub has no "sessionCancel" authority wired; cannot cancel a child session'))
        : authorities.sessionCancel(request as SessionCancelRequest),
    // Revert clearing is hub authority: the worker forwards its own bound
    // session and the hub clears the row under the session context. Absent an
    // authority fails loud, never silently leaving a stale revert behind.
    "session.clearRevert": (request) => {
      if (authorities.clearRevert === undefined) {
        return Promise.reject(new Error('the hub has no "clearRevert" authority wired; cannot clear a session revert'))
      }
      return authorities.clearRevert((request as { readonly sessionID: string }).sessionID)
    },
  }
  return handlers
}

/**
 * The streaming half of the hub-proxy handler map: the methods whose handler may
 * push correlated events to the awaiting worker before it replies. A method here
 * wins over a same-named plain handler ({@link buildHubProxyHandlers}).
 *
 * Today only `pluginTool.execute` streams: the hub plugin's mid-execute
 * `ToolContext.metadata` updates are forwarded as `pluginTool.metadata` events,
 * so a long-running plugin tool reports progress live instead of only in its
 * final, authoritative result.
 */
export function buildHubProxyStreamHandlers(
  authorities: HubProxyAuthorities,
): Readonly<Record<string, HubProxyStreamHandler>> {
  return {
    "pluginTool.execute": (sink, args) => {
      const input = args[0] as PluginToolExecuteInput
      if (authorities.pluginTool === undefined) {
        return Promise.reject(missingPluginToolAuthority("execute a plugin-provided tool"))
      }
      return authorities.pluginTool(input, (progress: PluginToolProgress) =>
        // The event method the worker's `hubProxyPluginTool` onEvent filters for.
        sink.emit("pluginTool.metadata", progress),
      )
    },
  }
}

/**
 * Build the hub-resident `experimental.chat.messages.transform` authority for a
 * worker. The hub owns plugin loading (decision 70), so resolution is deferred
 * through `plugin`, letting the caller read `Plugin.Service` from the worker's
 * session context. When no plugin is reachable the worker's payload passes
 * through by identity and `run` is never invoked; otherwise the plugin hook runs
 * under `run` (which provides the emitting instance's context) and its resolved
 * value is returned.
 */
export function messagesTransformAuthority(options: {
  readonly plugin: () => Option.Option<Plugin.Interface>
  readonly run: HubProxyRunner
}): NonNullable<HubProxyAuthorities["messagesTransform"]> {
  return (input, output) => {
    const plugin = options.plugin()
    return Option.isNone(plugin)
      ? Promise.resolve(output)
      : options.run(plugin.value.trigger("experimental.chat.messages.transform", input, output))
  }
}

/**
 * Build the hub-resident authority for the worker's hub-proxied plugin hook
 * bridge. The worker ships only `(name, input, output)`; the hub owns the plugin
 * functions and runs the real trigger under the worker's session context. A name
 * whose route is `unsupported` is rejected with
 * {@link UnsupportedPluginHookError}: the bridge is not a generic plugin RPC and
 * a worker may never invoke an unsupported hook through it. Worker-hosted hooks
 * may also be forwarded, which is how per-request `chat.params`/`chat.headers`
 * avoid freezing at the init model.
 */
export function pluginTriggerAuthority(options: {
  readonly plugin: () => Option.Option<Plugin.Interface>
  readonly run: HubProxyRunner
}): NonNullable<HubProxyAuthorities["pluginTrigger"]> {
  return (name, input, output) => {
    const route = workerHookRoute(name)
    if (route === undefined || route === "unsupported") {
      return Promise.reject(new UnsupportedPluginHookError({ plugin: "worker", hook: name }))
    }
    const plugin = options.plugin()
    return Option.isNone(plugin)
      ? Promise.resolve(output)
      : options.run(plugin.value.trigger(name as never, input as never, output as never))
  }
}

/**
 * Raised when a worker asks the hub to run a plugin tool id that no loaded plugin
 * currently exposes — a stale descriptor, or a plugin reloaded mid-generation.
 * Fail loud: an empty success would silently hide the config/version skew.
 */
export class UnknownPluginToolError extends Error {
  readonly _tag = "UnknownPluginToolError"
  constructor(readonly toolID: string) {
    super(`unknown plugin tool "${toolID}": no loaded plugin exposes that tool id`)
    this.name = "UnknownPluginToolError"
  }
}

/**
 * Raised when the args a worker forwarded do not satisfy the plugin tool's own
 * decoder. The worker ships args verbatim (`Schema.Unknown`), so the hub validates
 * them at the boundary; a bad arg is a fail-loud caller error, not a tool result.
 */
export class InvalidPluginToolArgsError extends Error {
  readonly _tag = "InvalidPluginToolArgsError"
  constructor(
    readonly toolID: string,
    readonly detail: string,
  ) {
    super(`invalid args for plugin tool "${toolID}": ${detail}`)
    this.name = "InvalidPluginToolArgsError"
  }
}

/** A registered in-flight plugin-tool call: its abort signal and unregister fn. */
export interface PluginToolAbortHandle {
  readonly signal: AbortSignal
  readonly dispose: () => void
}

/**
 * The hub's in-flight plugin-tool abort registry. A plugin tool's `execute` runs
 * on the hub under a per-call `AbortController`; keying those controllers by
 * `(sessionID, callID)` lets the hub cancel path (and the worker's
 * `pluginTool.abort` RPC) unwind exactly the calls it means to, and never a
 * settled one.
 */
export interface PluginToolAbortRegistry {
  readonly registerAbort: (sessionID: string, callID: string) => PluginToolAbortHandle
  readonly abort: (sessionID: string, callID: string) => void
  /** Abort every in-flight call of a session (the hub cancel cascade). */
  readonly abortSession: (sessionID: string) => void
}

/** A fresh abort registry backed by a session → callID → controller map. */
export function createPluginToolAbortRegistry(): PluginToolAbortRegistry {
  const bySession = new Map<string, Map<string, AbortController>>()
  return {
    registerAbort: (sessionID, callID) => {
      const controller = new AbortController()
      const calls = bySession.get(sessionID) ?? new Map<string, AbortController>()
      bySession.set(sessionID, calls)
      calls.set(callID, controller)
      return {
        signal: controller.signal,
        // Unregister on EVERY exit; the entry is dropped only when it is still the
        // registered controller, so a later call reusing the callID is untouched.
        dispose: () => {
          const current = bySession.get(sessionID)
          if (current === undefined) return
          if (current.get(callID) === controller) current.delete(callID)
          if (current.size === 0) bySession.delete(sessionID)
        },
      }
    },
    abort: (sessionID, callID) => {
      bySession.get(sessionID)?.get(callID)?.abort()
    },
    abortSession: (sessionID) => {
      for (const controller of bySession.get(sessionID)?.values() ?? []) controller.abort()
    },
  }
}

/** The hub-resident services the plugin-tool authority reads from context. */
type PluginToolServices = Permission.Service | Truncate.Service | Agent.Service | Session.Service

/** The clone-safe LSP tool descriptor the hub ships to a worker. */
const LSP_TOOL_ID = "lsp"
interface HubToolDescriptor extends PluginToolDescriptor {
  readonly source: "plugin" | "mcp" | "lsp"
}

function lspToolDescriptor(): PluginToolDescriptor {
  return {
    id: LSP_TOOL_ID,
    description: DESCRIPTION_LSP,
    jsonSchema: ToolJsonSchema.fromSchema(LspParameters as never),
  }
}

function mcpToolJsonSchema(definition: { readonly inputSchema: unknown }): PluginToolDescriptor["jsonSchema"] {
  const schema = definition.inputSchema as { properties?: Record<string, unknown> } | undefined
  return {
    ...(schema ?? {}),
    type: "object" as const,
    properties: schema?.properties ?? {},
    additionalProperties: false as const,
  } as PluginToolDescriptor["jsonSchema"]
}

/**
 * Module-level projection of the hub's MCP catalog plus the LSP tool into the
 * same clone-safe descriptor shape plugin tools use. A worker registers these as
 * forwarding stubs; the hub authority (below) executes them with its own MCP/LSP
 * services under the worker's captured session context.
 */
export async function resolveContextHubToolDescriptors(
  context: Context.Context<never> | undefined,
): Promise<readonly HubToolDescriptor[]> {
  const descriptors: HubToolDescriptor[] = []
  for (const descriptor of await resolveContextPluginToolDescriptors(context)) {
    descriptors.push({ ...descriptor, source: "plugin" })
  }
  if (context === undefined) return descriptors
  const mcp = Context.getOption(context, MCP.Service)
  if (Option.isSome(mcp)) {
    const tools = await Effect.runPromise(Effect.provide(mcp.value.tools(), context))
    for (const [id, entry] of Object.entries(tools)) {
      descriptors.push({
        id,
        description: entry.def.description ?? "",
        jsonSchema: mcpToolJsonSchema(entry.def),
        source: "mcp",
      })
    }
  }
  const flags = Context.getOption(context, RuntimeFlags.Service)
  if (Option.isNone(flags) || flags.value.experimentalLspTool) {
    descriptors.push({ ...lspToolDescriptor(), source: "lsp" })
  }
  return descriptors
}

/** Convert a raw MCP call result into the clone-safe plugin-tool result shape. */
function mcpToolResult(result: unknown): { output: string; metadata: unknown; attachments?: unknown[] } {
  const content = (result as { content?: ReadonlyArray<Record<string, unknown>> } | undefined)?.content ?? []
  const textParts: string[] = []
  const attachments: unknown[] = []
  for (const item of content) {
    if (item.type === "text" && typeof item.text === "string") textParts.push(item.text)
    else if (item.type === "image" && typeof item.data === "string" && typeof item.mimeType === "string") {
      attachments.push({ type: "file", mime: item.mimeType, url: `data:${item.mimeType};base64,${item.data}` })
    }
  }
  return {
    output: textParts.join("\n\n"),
    metadata: (result as { metadata?: unknown } | undefined)?.metadata ?? {},
    ...(attachments.length === 0 ? {} : { attachments }),
  }
}

/** Every typed failure a plugin-tool execute can raise. */
type PluginToolFailure = UnknownPluginToolError | InvalidPluginToolArgsError | PluginToolNotSerializableError

/** The first plugin tool definition exposing `toolID`, or `undefined`. */
function findPluginToolDefinition(hooks: readonly Hooks[], toolID: string): ToolDefinition | undefined {
  for (const hook of hooks) {
    const definition = hook.tool?.[toolID]
    if (definition !== undefined) return definition
  }
  return undefined
}

/** Whether a plugin tool arg entry is a Zod type (the registry's own predicate). */
function isZodType(value: unknown): value is z.ZodType {
  return typeof value === "object" && value !== null && "_zod" in value
}

/**
 * Validate forwarded args against the plugin tool's own decoder before `execute`.
 * The registry only builds a decoder when every arg is a Zod type; a legacy
 * JSON-schema map is `Schema.Unknown` there, so the hub passes it through rather
 * than inventing stricter validation than the tool advertises.
 */
function validatePluginToolArgs(toolID: string, definition: ToolDefinition, args: unknown): void {
  const raw = definition.args ?? {}
  if (!Object.values(raw).every(isZodType)) return
  const parsed = z.object(raw).safeParse(args ?? {})
  if (!parsed.success) {
    throw new InvalidPluginToolArgsError(toolID, parsed.error.message)
  }
}

/**
 * Fail loud on a result that cannot cross the control port, naming the tool and
 * the offending field. Without this a leaked function would only surface as the
 * generic `serveHubProxy` clone guard's cryptic message.
 */
export function sanitizePluginToolResult(toolID: string, result: PluginToolExecuteResult): PluginToolExecuteResult {
  assertCloneable(toolID, "title", result.title)
  assertCloneable(toolID, "output", result.output)
  assertCloneable(toolID, "metadata", result.metadata)
  if (result.attachments !== undefined) assertCloneable(toolID, "attachments", result.attachments)
  return result
}

function assertCloneable(toolID: string, field: string, value: unknown): void {
  try {
    structuredClone(value)
  } catch {
    throw new PluginToolNotSerializableError(toolID, field)
  }
}

/**
 * Execute a hub-owned MCP or LSP tool a worker forwarded over the plugin-tool
 * channel. The worker owns no MCP/LSP child process, so this is the single hub
 * execution point (`session/tools.ts` only decides *which* host). An unknown id
 * reports `UnknownPluginToolError` so the worker's stale-descriptor refresh path
 * is unchanged.
 */
const executeHubTool = (
  input: PluginToolExecuteInput,
  emit: PluginToolProgressSink,
  registerAbort?: (sessionID: string, callID: string) => PluginToolAbortHandle,
): Effect.Effect<PluginToolExecuteResult, UnknownPluginToolError, PluginToolServices> =>
  Effect.gen(function* () {
    const permission = yield* Permission.Service
    const truncate = yield* Truncate.Service
    const agents = yield* Agent.Service
    const sessions = yield* Session.Service
    const instance = yield* InstanceRef
    const agentInfo = yield* agents.get(input.agent)
    const sessionInfo = yield* sessions.get(input.sessionID as SessionID).pipe(Effect.orDie)
    const bridge = yield* EffectBridge.make()
    // Register only once the tool is known to execute: an unknown id must never
    // leave an abort entry the cancel path could later reach. A hub-executed
    // MCP/LSP tool is abortable exactly like a plugin tool, so a cancel unwinds
    // its in-flight `abortSignal` instead of waiting for the provider timeout.
    const beginAbort = (): PluginToolAbortHandle =>
      registerAbort?.(input.sessionID, input.callID) ?? {
        signal: new AbortController().signal,
        dispose: () => {},
      }
    const ask = (request: {
      permission: string
      patterns: string[]
      always: string[]
      metadata: Record<string, unknown>
    }) =>
      bridge.promise(
        permission
          .ask({
            ...request,
            sessionID: input.sessionID as SessionID,
            tool: { messageID: input.messageID, callID: input.callID },
            ruleset: Permission.merge(agentInfo.permission, sessionInfo.permission ?? []),
          })
          .pipe(Effect.orDie),
      )

    if (input.toolID === LSP_TOOL_ID) {
      const lsp = yield* Effect.serviceOption(LSP.Service)
      if (Option.isNone(lsp)) return yield* Effect.fail(new UnknownPluginToolError(input.toolID))
      const abort = beginAbort()
      return yield* Effect.gen(function* () {
        const ctx: Tool.Context = {
          sessionID: input.sessionID as SessionID,
          messageID: input.messageID as never,
          agent: input.agent,
          abort: abort.signal,
          callID: input.callID,
          messages: [],
          metadata: (update) => Effect.sync(() => emit({ title: update.title, metadata: update.metadata })),
          ask: (request) => Effect.promise(() => Promise.resolve(ask(request as never))),
        }
        const info = yield* LspTool
        const tool = yield* info.init()
        const result = yield* tool.execute(input.args as never, ctx)
        return { title: result.title, output: result.output, metadata: result.metadata }
      }).pipe(Effect.ensuring(Effect.sync(() => abort.dispose())))
    }

    const mcp = yield* Effect.serviceOption(MCP.Service)
    if (Option.isNone(mcp)) return yield* Effect.fail(new UnknownPluginToolError(input.toolID))
    const entry = (yield* mcp.value.tools())[input.toolID]
    if (entry === undefined) return yield* Effect.fail(new UnknownPluginToolError(input.toolID))
    const abort = beginAbort()
    return yield* Effect.gen(function* () {
      const tool = McpCatalog.convertTool(entry.def, entry.client, entry.timeout)
      yield* Effect.promise(() => ask({ permission: input.toolID, patterns: ["*"], always: ["*"], metadata: {} }))
      const raw = yield* Effect.promise(() =>
        (tool.execute as (args: unknown, options: { abortSignal: AbortSignal }) => Promise<unknown>)(input.args, {
          abortSignal: abort.signal,
        }),
      )
      const converted = mcpToolResult(raw)
      const bounded = yield* Truncate.boundResult(
        truncate,
        { output: converted.output, metadata: converted.metadata as Record<string, unknown> },
        agentInfo,
      )
      return {
        title: "",
        output: bounded.output,
        metadata: bounded.metadata,
        ...(converted.attachments === undefined ? {} : { attachments: converted.attachments }),
      }
    }).pipe(Effect.ensuring(Effect.sync(() => abort.dispose())))
  }) as Effect.Effect<PluginToolExecuteResult, UnknownPluginToolError, PluginToolServices>

/**
 * Run one plugin tool on the hub: locate its definition, validate the forwarded
 * args, reconstruct the `ToolContext` the plugin expects, execute, then
 * normalize, bound (once) and sanitize the result exactly like the registry's
 * local `fromPlugin` path.
 */
function pluginToolExecute(
  plugin: () => Option.Option<Plugin.Interface>,
  input: PluginToolExecuteInput,
  emit: PluginToolProgressSink,
  registerAbort: ((sessionID: string, callID: string) => PluginToolAbortHandle) | undefined,
): Effect.Effect<PluginToolExecuteResult, PluginToolFailure, PluginToolServices> {
  return Effect.gen(function* () {
    const current = plugin()
    const definition = Option.isNone(current)
      ? undefined
      : findPluginToolDefinition(yield* current.value.list(), input.toolID)
    if (definition === undefined) {
      return yield* executeHubTool(input, emit, registerAbort).pipe(
        Effect.catchTag("UnknownPluginToolError", () => Effect.fail(new UnknownPluginToolError(input.toolID))),
      )
    }
    validatePluginToolArgs(input.toolID, definition, input.args)
    const bridge = yield* EffectBridge.make()
    const permission = yield* Permission.Service
    const truncate = yield* Truncate.Service
    const agents = yield* Agent.Service
    const sessions = yield* Session.Service
    const instance = yield* InstanceRef
    const agentInfo = yield* agents.get(input.agent)
    const sessionInfo = yield* sessions.get(input.sessionID as SessionID).pipe(Effect.orDie)
    // Register only once we are committed to executing: an unknown tool or bad
    // args must never leave an entry the cancel path could later abort.
    const abort = registerAbort?.(input.sessionID, input.callID) ?? {
      signal: new AbortController().signal,
      dispose: () => {},
    }
    const pluginCtx: ToolContext = {
      sessionID: input.sessionID,
      messageID: input.messageID,
      agent: input.agent,
      directory: instance?.directory ?? process.cwd(),
      worktree: instance?.worktree ?? process.cwd(),
      // Per-call so a hub cancel can unwind an in-flight plugin tool.
      abort: abort.signal,
      // Mid-execute progress is streamed to the worker as `pluginTool.metadata`.
      metadata: (update) => emit({ title: update.title, metadata: update.metadata }),
      // Bound to the HUB-RESIDENT permission service. Routing this back over the
      // control port would deadlock: we are already inside that port's request.
      ask: (request) =>
        bridge.promise(
          permission
            .ask({
              ...request,
              sessionID: input.sessionID as SessionID,
              tool: { messageID: input.messageID, callID: input.callID },
              ruleset: Permission.merge(agentInfo.permission, sessionInfo.permission ?? []),
            })
            .pipe(Effect.orDie),
        ),
    }
    return yield* Effect.gen(function* () {
      const result = yield* Effect.promise(() => definition.execute(input.args as never, pluginCtx))
      const output = typeof result === "string" ? result : result.output
      const metadata = typeof result === "string" ? {} : (result.metadata ?? {})
      const attachments = typeof result === "string" ? undefined : result.attachments
      const bounded = yield* Truncate.boundResult(truncate, { output, metadata }, agentInfo)
      return sanitizePluginToolResult(input.toolID, {
        title: typeof result === "string" ? "" : (result.title ?? ""),
        output: bounded.output,
        metadata: bounded.metadata,
        ...(attachments === undefined ? {} : { attachments }),
      })
    }).pipe(Effect.ensuring(Effect.sync(() => abort.dispose())))
  })
}

/**
 * The hub-resident plugin-tool execute authority. `emit` is optional so a
 * non-streaming caller (or an existing single-argument consumer) still works; the
 * server's streaming handler always supplies the real sink.
 */
export type PluginToolAuthority = (
  input: PluginToolExecuteInput,
  emit?: PluginToolProgressSink,
) => Promise<PluginToolExecuteResult>

/**
 * Build the hub-resident plugin-tool authority for a worker. The hub owns plugin
 * loading (decision 70), so resolution is deferred through `plugin`, letting the
 * caller read `Plugin.Service` (and the other hub authorities) from the worker's
 * captured session context. `registerAbort` wires the per-call controller into the
 * hub cancel path; absent, a throwaway controller is used (best-effort).
 */
export function pluginToolAuthority(options: {
  readonly plugin: () => Option.Option<Plugin.Interface>
  readonly run: HubProxyRunner
  readonly registerAbort?: (sessionID: string, callID: string) => PluginToolAbortHandle
}): PluginToolAuthority {
  return (input, emit = () => {}) =>
    // The effect needs the hub-resident services (Permission / Truncate / Agent /
    // Session). `run` provides the dispatching session's captured context, but
    // `HubProxyRunner` erases `R`, so the requirement is asserted at this single
    // boundary rather than threaded through the frozen signature.
    options.run(
      pluginToolExecute(options.plugin, input, emit, options.registerAbort) as unknown as Effect.Effect<
        PluginToolExecuteResult,
        UnknownPluginToolError
      >,
    )
}

/**
 * Resolve the clone-safe plugin-tool descriptors for a dispatch context. The real
 * plugin tools live on the hub (`Plugin.list()` → `Hooks.tool`); only their
 * clone-safe `{ id, description, jsonSchema }` projection crosses into the worker
 * init. Absent context/plugin resolves to no descriptors — a degraded boot simply
 * lists no plugin tools, never a failure.
 *
 * The `jsonSchema` is computed by the registry's exported `pluginToolJsonSchema`,
 * the single shared implementation the hub registry itself uses, so a shipped
 * descriptor can never drift from what `fromPlugin` would advertise. `hub.ts` may
 * import `@/tool/registry` here without a module cycle: the registry's only
 * `session/worker` dependency is the dependency-light `plugin-tool-wire` leaf.
 */
export async function resolveContextPluginToolDescriptors(
  context: Context.Context<never> | undefined,
): Promise<readonly PluginToolDescriptor[]> {
  if (context === undefined) return []
  const plugin = Context.getOption(context, Plugin.Service)
  if (Option.isNone(plugin)) return []
  // `Plugin.list` resolves the per-directory `InstanceState`, so run it under the
  // dispatching instance's captured context.
  const hooks = await Effect.runPromise(Effect.provide(plugin.value.list(), context))
  const descriptors: PluginToolDescriptor[] = []
  for (const hook of hooks) {
    for (const [id, definition] of Object.entries(hook.tool ?? {})) {
      descriptors.push({ id, description: definition.description, jsonSchema: pluginToolJsonSchema(definition) })
    }
  }
  return descriptors
}

/**
 * Caller metadata is data, never classification. Strip a `depth` a worker may
 * have forged so the only depth on the spawned/created child is the one the hub
 * derives from the persisted parent.
 */
function withoutSpawnDepth(metadata: Record<string, unknown> | undefined): Record<string, unknown> | undefined {
  if (metadata === undefined) return undefined
  const { depth: _depth, ...rest } = metadata
  return rest
}

/**
 * The hub's authoritative child spawn. The hub supplies its own resolved depth
 * limit, so `Session.spawn` rejects an over-depth child before creating one even
 * when the worker forwarded no limit.
 */
export const spawnChildOnHub = (
  sessions: Session.Interface,
  subagentDepth: number,
  request: SessionSpawnRequest,
): Effect.Effect<SessionSpawnResult> =>
  sessions.spawn({
    parentID: request.parentID,
    ...(request.childID === undefined ? {} : { childID: request.childID }),
    type: request.type,
    ...(request.title === undefined ? {} : { title: request.title }),
    ...(request.metadata === undefined ? {} : { metadata: withoutSpawnDepth(request.metadata) }),
    depthLimit: subagentDepth,
    prompt: request.prompt,
  })

/**
 * Resolve the worker's authoritative bound session. Only the hub registration
 * (the worker → session binding) can establish authority: an unregistered worker
 * cannot claim a session by naming it on the request. A request that names a
 * different session than the registration is a hard rejection too.
 */
function boundSessionScope(
  expected: SessionID | undefined,
  requested: SessionID | undefined,
  targetID: string,
): Effect.Effect<SessionID, SessionScopeError> {
  if (expected === undefined) {
    return Effect.fail(
      new SessionScopeError({
        message: "the hub has no bound session for this worker; refusing the request",
        sessionID: "",
        targetID,
      }),
    )
  }
  if (requested !== undefined && requested !== expected) {
    return Effect.fail(
      new SessionScopeError({
        message: `worker bound to ${expected} may not act as ${requested}`,
        sessionID: expected,
        targetID,
      }),
    )
  }
  return Effect.succeed(expected)
}

/**
 * Whether `target` is a DIRECT child of `bound`. A bound worker owns exactly one
 * session, so it may only name its own direct children when it asks the hub to
 * read or cancel a child. Self, grandchild, ancestor, sibling and foreign ids all
 * fail this check.
 */
function isDirectChildOf(sessions: Session.Interface, bound: SessionID, target: SessionID): Effect.Effect<boolean> {
  if (target === bound) return Effect.succeed(false)
  return Effect.gen(function* () {
    const info: Option.Option<Session.Info> = yield* Effect.option(sessions.get(target))
    return Option.isSome(info) && info.value.parentID === bound
  })
}

/** Create a child on the hub, only under the worker's own bound session. */
export function createSessionOnHub(
  sessions: Session.Interface,
  expected: SessionID | undefined,
  request: SessionCreateRequest,
): Effect.Effect<Session.Info, SessionScopeError> {
  return Effect.gen(function* () {
    const bound = yield* boundSessionScope(expected, request.sessionID, String(request.parentID ?? ""))
    if (request.parentID !== bound) {
      return yield* Effect.fail(
        new SessionScopeError({
          message: `worker bound to ${bound} may not create a child under ${String(request.parentID)}`,
          sessionID: bound,
          targetID: String(request.parentID ?? ""),
        }),
      )
    }
    const parent = yield* Effect.option(sessions.get(bound))
    if (Option.isNone(parent)) {
      return yield* Effect.fail(
        new SessionScopeError({
          message: `cannot create a child under unknown session ${bound}`,
          sessionID: bound,
          targetID: String(bound),
        }),
      )
    }
    // The child's depth belongs to the persisted parent, exactly as on spawn.
    // Caller metadata is never read for classification, so a worker cannot
    // create a depth-0 row (and claim foreground headroom) under a deep parent.
    const childDepth = nextDepth(parent.value.metadata)
    const { sessionID: _sessionID, ...input } = request
    return yield* sessions
      .create({ ...input, metadata: { ...withoutSpawnDepth(request.metadata), depth: childDepth } })
      .pipe(Effect.orDie)
  })
}

/** Read one session, only when it is a direct child of the bound session. */
export function getSessionOnHub(
  sessions: Session.Interface,
  expected: SessionID | undefined,
  request: SessionGetRequest,
): Effect.Effect<Session.Info | null, SessionScopeError> {
  return Effect.gen(function* () {
    const bound = yield* boundSessionScope(expected, request.sessionID, String(request.targetID))
    if (!(yield* isDirectChildOf(sessions, bound, request.targetID))) {
      return yield* Effect.fail(
        new SessionScopeError({
          message: `worker bound to ${bound} may not read session ${request.targetID}: not a direct child`,
          sessionID: bound,
          targetID: String(request.targetID),
        }),
      )
    }
    const info = yield* Effect.option(sessions.get(request.targetID))
    return Option.isNone(info) ? null : info.value
  })
}

/** List the bound session's own children; a child's children are out of scope. */
export function childrenOnHub(
  sessions: Session.Interface,
  expected: SessionID | undefined,
  request: SessionChildrenRequest,
): Effect.Effect<readonly Session.Info[], SessionScopeError> {
  return Effect.gen(function* () {
    const bound = yield* boundSessionScope(expected, request.sessionID, String(request.parentID))
    if (request.parentID !== bound) {
      return yield* Effect.fail(
        new SessionScopeError({
          message: `worker bound to ${bound} may not list children of ${request.parentID}`,
          sessionID: bound,
          targetID: String(request.parentID),
        }),
      )
    }
    return yield* sessions.children(request.parentID).pipe(Effect.orDie)
  })
}

/**
 * Cancel the bound session (its own run, cascading to its descendants on the
 * hub) or one of its direct children. A grandchild, ancestor, sibling or foreign
 * id is rejected: the child worker owns its own subtree.
 */
export function cancelSessionOnHub(
  sessions: Session.Interface,
  expected: SessionID | undefined,
  request: SessionCancelRequest,
  cancel: (sessionID: SessionID) => Effect.Effect<CancelOutcome | undefined>,
): Effect.Effect<CancelOutcome | undefined, SessionScopeError> {
  return Effect.gen(function* () {
    const bound = yield* boundSessionScope(expected, request.sessionID, String(request.targetID))
    if (request.targetID !== bound && !(yield* isDirectChildOf(sessions, bound, request.targetID))) {
      return yield* Effect.fail(
        new SessionScopeError({
          message: `worker bound to ${bound} may not cancel session ${request.targetID}: not a direct child`,
          sessionID: bound,
          targetID: String(request.targetID),
        }),
      )
    }
    return yield* cancel(request.targetID)
  })
}

/**
 * The dependencies a session-scope hub authority runs under. The runner carries
 * the dispatching worker's captured context, so `Session`/`SessionPrompt` are
 * resolved there rather than from the hub's ambient graph.
 */
export interface SessionScopeAuthorityDeps {
  readonly run: HubProxyRunner
  /** The hub-resolved `subagent_depth` cap enforced before a child is created. */
  readonly subagentDepth: number
  /** Record a verified child's authoritative depth before it can dispatch. */
  readonly recordDepth: (sessionID: SessionID, depth: number) => void
  /** Synchronous parent → children edge, recorded only for a verified child. */
  readonly registerChild: (parentID: SessionID, childID: SessionID) => void
}

function missingSessionService(): Error {
  return new Error("the hub has no Session service in the parent context")
}

function missingPromptService(): Error {
  return new Error("the hub has no SessionPrompt service in the parent context")
}

/** Run a session-scope check under the worker's captured context. */
function withSessionScope<A>(
  run: HubProxyRunner,
  effect: (sessions: Session.Interface) => Effect.Effect<A, SessionScopeError>,
): Promise<A> {
  return run(
    Effect.gen(function* () {
      const sessions = yield* Effect.serviceOption(Session.Service)
      if (Option.isNone(sessions)) return yield* Effect.die(missingSessionService())
      return yield* effect(sessions.value)
    }),
  )
}

/** Create a child on the hub, only under the worker's own bound session. */
export function createSessionFor(
  boundSessionID: SessionID | undefined,
  run: HubProxyRunner,
): NonNullable<HubProxyAuthorities["sessionCreate"]> {
  return (request) => withSessionScope(run, (sessions) => createSessionOnHub(sessions, boundSessionID, request))
}

/** Read one session, only within the worker's own subtree. */
export function getSessionFor(
  boundSessionID: SessionID | undefined,
  run: HubProxyRunner,
): NonNullable<HubProxyAuthorities["sessionGet"]> {
  return (request) => withSessionScope(run, (sessions) => getSessionOnHub(sessions, boundSessionID, request))
}

/** List a session's children, only within the worker's own subtree. */
export function sessionChildrenFor(
  boundSessionID: SessionID | undefined,
  run: HubProxyRunner,
): NonNullable<HubProxyAuthorities["sessionChildren"]> {
  return (request) => withSessionScope(run, (sessions) => childrenOnHub(sessions, boundSessionID, request))
}

/** Cancel a session and its descendants, only within the worker's subtree. */
export function cancelSessionFor(
  boundSessionID: SessionID | undefined,
  run: HubProxyRunner,
): NonNullable<HubProxyAuthorities["sessionCancel"]> {
  return (request) =>
    run(
      Effect.gen(function* () {
        const sessions = yield* Effect.serviceOption(Session.Service)
        if (Option.isNone(sessions)) return yield* Effect.die(missingSessionService())
        const prompt = yield* Effect.serviceOption(promptAuthority)
        if (Option.isNone(prompt)) return yield* Effect.die(missingPromptService())
        return yield* cancelSessionOnHub(sessions.value, boundSessionID, request, (sessionID) =>
          prompt.value.cancel(sessionID),
        )
      }),
    )
}

/**
 * The hub's authoritative child spawn. A worker must be bound, may only spawn
 * under that bound session, and a supplied `childID` must already be a direct
 * child of it. Depth comes from the persisted parent (`nextDepth`), never from
 * the request metadata a worker controls.
 */
export function sessionSpawnFor(
  boundSessionID: SessionID | undefined,
  deps: SessionScopeAuthorityDeps,
): NonNullable<HubProxyAuthorities["sessionSpawn"]> {
  return (request) =>
    deps.run(
      Effect.gen(function* () {
        const sessions = yield* Effect.serviceOption(Session.Service)
        if (Option.isNone(sessions)) return yield* Effect.die(missingSessionService())
        if (boundSessionID === undefined) {
          return yield* Effect.fail(
            new SessionScopeError({
              message: "the hub has no bound session for this worker; refusing to spawn",
              sessionID: "",
              targetID: String(request.parentID),
            }),
          )
        }
        if (request.parentID !== boundSessionID) {
          return yield* Effect.fail(
            new SessionScopeError({
              message: `worker bound to ${boundSessionID} may not spawn under ${request.parentID}`,
              sessionID: boundSessionID,
              targetID: String(request.parentID),
            }),
          )
        }
        const parent = yield* Effect.option(sessions.value.get(request.parentID))
        if (Option.isNone(parent)) {
          return yield* Effect.fail(
            new SessionScopeError({
              message: `cannot spawn under unknown session ${request.parentID}`,
              sessionID: boundSessionID,
              targetID: String(request.parentID),
            }),
          )
        }
        // The child's depth is a property of the persisted parent, not of the
        // worker-supplied metadata (which a caller could forge).
        const childDepth = nextDepth(parent.value.metadata)
        if (request.childID !== undefined) {
          if (request.childID === request.parentID) {
            return yield* Effect.fail(
              new SessionScopeError({
                message: `worker bound to ${boundSessionID} may not resume its own session as a child`,
                sessionID: boundSessionID,
                targetID: String(request.childID),
              }),
            )
          }
          const child = yield* Effect.option(sessions.value.get(request.childID))
          if (Option.isNone(child) || child.value.parentID !== request.parentID) {
            return yield* Effect.fail(
              new SessionScopeError({
                message: `worker bound to ${boundSessionID} may not resume ${request.childID}: it is not a direct child of ${request.parentID}`,
                sessionID: boundSessionID,
                targetID: String(request.childID),
              }),
            )
          }
          deps.recordDepth(request.childID, childDepth)
        }
        const spawned = yield* spawnChildOnHub(sessions.value, deps.subagentDepth, request)
        deps.recordDepth(spawned.childID, childDepth)
        // Record the edge synchronously, before the child's own worker can
        // start. A parent crash between here and the child dispatch then still
        // cascades (the child is either queued or admitted, both cancellable).
        deps.registerChild(boundSessionID, spawned.childID)
        return spawned
      }),
    )
}

/**
 * Bind every `sessionStatus` operation to the worker's own session. A foreign
 * read never reaches the hub service (it resolves idle), a foreign write is
 * ignored, and the list exposes only the bound entry.
 */
function bindSessionStatus(
  inner: SessionStatus.Interface,
  boundSessionID: SessionID | undefined,
): SessionStatus.Interface {
  const owns = (sessionID: SessionID): boolean => boundSessionID !== undefined && sessionID === boundSessionID
  return {
    get: (sessionID) => (owns(sessionID) ? inner.get(sessionID) : Effect.succeed({ type: "idle" as const })),
    list: () =>
      boundSessionID === undefined
        ? Effect.succeed(new Map<SessionID, SessionStatus.Info>())
        : inner
            .list()
            .pipe(
              Effect.map((statuses) => new Map([...statuses].filter(([sessionID]) => sessionID === boundSessionID))),
            ),
    set: (sessionID, status) => (owns(sessionID) ? inner.set(sessionID, status) : Effect.void),
  }
}

/** A foreign session target on a per-session cancel/plugin authority. */
function foreignSessionError(
  what: string,
  boundSessionID: SessionID | undefined,
  targetID: string,
): SessionScopeError | undefined {
  if (boundSessionID !== undefined && targetID === boundSessionID) return undefined
  return new SessionScopeError({
    message: `worker bound to ${boundSessionID ?? "no session"} may not ${what} for ${targetID}`,
    sessionID: boundSessionID ?? "",
    targetID,
  })
}

/**
 * Bind a per-session cancel to the worker's own session. The hub release path
 * cancels only the session whose authority map it builds; a worker asking for
 * another session must fail, not silently cancel (or leave) that session's
 * pending requests.
 */
function bindCancelSession<ID extends string>(
  cancel: (sessionID: ID) => Effect.Effect<void>,
  boundSessionID: SessionID | undefined,
  what: string,
): (sessionID: ID) => Effect.Effect<void> {
  return (sessionID) => {
    const foreign = foreignSessionError(what, boundSessionID, String(sessionID))
    return foreign === undefined ? cancel(sessionID) : Effect.die(foreign)
  }
}

/** Bind a plugin-tool execute to the worker's own session before it reaches the hub plugin. */
function bindPluginTool(
  execute: NonNullable<HubProxyAuthorities["pluginTool"]>,
  boundSessionID: SessionID | undefined,
): NonNullable<HubProxyAuthorities["pluginTool"]> {
  return (input, emit) => {
    const foreign = foreignSessionError("execute a plugin tool", boundSessionID, input.sessionID)
    return foreign === undefined ? execute(input, emit) : Promise.reject(foreign)
  }
}

/** Bind a plugin-tool abort to the worker's own session before it reaches the abort registry. */
function bindPluginToolAbort(
  abort: NonNullable<HubProxyAuthorities["pluginToolAbort"]>,
  boundSessionID: SessionID | undefined,
): NonNullable<HubProxyAuthorities["pluginToolAbort"]> {
  return (input) => {
    const foreign = foreignSessionError("abort a plugin tool", boundSessionID, input.sessionID)
    return foreign === undefined ? abort(input) : Promise.reject(foreign)
  }
}

/** Bind a revert clear to the worker's own session before it reaches `Session.clearRevert`. */
function bindSessionClearRevert(
  clear: NonNullable<HubProxyAuthorities["clearRevert"]>,
  boundSessionID: SessionID | undefined,
): NonNullable<HubProxyAuthorities["clearRevert"]> {
  return async (sessionID) => {
    const foreign = foreignSessionError("clear a session revert", boundSessionID, sessionID)
    if (foreign !== undefined) throw foreign
    await clear(sessionID)
  }
}

/** Bind permission asks/replies/lists to the worker's own session's requests. */
function bindPermission(inner: Permission.Interface, boundSessionID: SessionID | undefined): Permission.Interface {
  return {
    ask: (input) =>
      boundSessionID === undefined || input.sessionID !== boundSessionID
        ? Effect.fail(new PermissionV1.DeniedError({ ruleset: [] }))
        : inner.ask(input),
    reply: (input) =>
      Effect.gen(function* () {
        if (boundSessionID === undefined) {
          return yield* Effect.fail(new PermissionV1.NotFoundError({ requestID: input.requestID }))
        }
        const pending = yield* inner.list()
        const target = pending.find((item) => item.id === input.requestID)
        if (target === undefined || target.sessionID !== boundSessionID) {
          return yield* Effect.fail(new PermissionV1.NotFoundError({ requestID: input.requestID }))
        }
        return yield* inner.reply(input)
      }),
    list: () =>
      boundSessionID === undefined
        ? Effect.succeed<ReadonlyArray<PermissionV1.Request>>([])
        : inner.list().pipe(Effect.map((pending) => pending.filter((item) => item.sessionID === boundSessionID))),
    ...(inner.cancelSession === undefined
      ? {}
      : { cancelSession: bindCancelSession(inner.cancelSession, boundSessionID, "cancel permission requests") }),
  }
}

/** Bind question asks/replies/rejects/lists to the worker's own session's requests. */
function bindQuestion(inner: Question.Interface, boundSessionID: SessionID | undefined): Question.Interface {
  return {
    ask: (input) =>
      boundSessionID === undefined || input.sessionID !== boundSessionID
        ? Effect.fail(new Question.RejectedError())
        : inner.ask(input),
    reply: (input) =>
      Effect.gen(function* () {
        if (boundSessionID === undefined) {
          return yield* Effect.fail(new Question.NotFoundError({ requestID: input.requestID }))
        }
        const pending = yield* inner.list()
        const target = pending.find((item) => item.id === input.requestID)
        if (target === undefined || target.sessionID !== boundSessionID) {
          return yield* Effect.fail(new Question.NotFoundError({ requestID: input.requestID }))
        }
        return yield* inner.reply(input)
      }),
    reject: (requestID) =>
      Effect.gen(function* () {
        if (boundSessionID === undefined) {
          return yield* Effect.fail(new Question.NotFoundError({ requestID }))
        }
        const pending = yield* inner.list()
        const target = pending.find((item) => item.id === requestID)
        if (target === undefined || target.sessionID !== boundSessionID) {
          return yield* Effect.fail(new Question.NotFoundError({ requestID }))
        }
        return yield* inner.reject(requestID)
      }),
    list: () =>
      boundSessionID === undefined
        ? Effect.succeed<ReadonlyArray<Question.Request>>([])
        : inner.list().pipe(Effect.map((pending) => pending.filter((item) => item.sessionID === boundSessionID))),
    ...(inner.cancelSession === undefined
      ? {}
      : { cancelSession: bindCancelSession(inner.cancelSession, boundSessionID, "cancel question requests") }),
  }
}

/** The inputs the worker-facing authority map is built from. */
export interface HubProxyAuthorityDeps extends SessionScopeAuthorityDeps {
  readonly boundSessionID: SessionID | undefined
  readonly sessionStatus: SessionStatus.Interface
  readonly permission: Permission.Interface
  readonly question: Question.Interface
  readonly systemTransform?: NonNullable<HubProxyAuthorities["systemTransform"]>
  readonly messagesTransform?: NonNullable<HubProxyAuthorities["messagesTransform"]>
  readonly pluginTrigger?: NonNullable<HubProxyAuthorities["pluginTrigger"]>
  readonly pluginTool?: NonNullable<HubProxyAuthorities["pluginTool"]>
  readonly pluginToolList?: NonNullable<HubProxyAuthorities["pluginToolList"]>
  readonly pluginToolAbort?: NonNullable<HubProxyAuthorities["pluginToolAbort"]>
  readonly clearRevert?: NonNullable<HubProxyAuthorities["clearRevert"]>
  readonly backgroundJob?: NonNullable<HubProxyAuthorities["backgroundJob"]>
}

/**
 * The production authority map for one worker. Every worker-controlled id is
 * bound to the session the worker is registered to: status writes, permission
 * and question replies/lists, and every session-scope RPC.
 */
export function hubProxyAuthoritiesFor(deps: HubProxyAuthorityDeps): HubProxyAuthorities {
  return {
    sessionStatus: bindSessionStatus(deps.sessionStatus, deps.boundSessionID),
    permission: bindPermission(deps.permission, deps.boundSessionID),
    question: bindQuestion(deps.question, deps.boundSessionID),
    ...(deps.systemTransform === undefined ? {} : { systemTransform: deps.systemTransform }),
    ...(deps.messagesTransform === undefined ? {} : { messagesTransform: deps.messagesTransform }),
    ...(deps.pluginTrigger === undefined ? {} : { pluginTrigger: deps.pluginTrigger }),
    ...(deps.pluginTool === undefined
      ? {}
      : { pluginTool: bindPluginTool(deps.pluginTool, deps.boundSessionID) }),
    ...(deps.pluginToolList === undefined ? {} : { pluginToolList: deps.pluginToolList }),
    ...(deps.pluginToolAbort === undefined
      ? {}
      : { pluginToolAbort: bindPluginToolAbort(deps.pluginToolAbort, deps.boundSessionID) }),
    ...(deps.clearRevert === undefined
      ? {}
      : { clearRevert: bindSessionClearRevert(deps.clearRevert, deps.boundSessionID) }),
    // The mirror is session-bound, so an unbound worker must never receive one:
    // an authority without a bound session could otherwise act on any job id.
    ...(deps.backgroundJob === undefined || deps.boundSessionID === undefined
      ? {}
      : { backgroundJob: deps.backgroundJob }),
    sessionSpawn: sessionSpawnFor(deps.boundSessionID, deps),
    sessionCreate: createSessionFor(deps.boundSessionID, deps.run),
    sessionGet: getSessionFor(deps.boundSessionID, deps.run),
    sessionChildren: sessionChildrenFor(deps.boundSessionID, deps.run),
    sessionCancel: cancelSessionFor(deps.boundSessionID, deps.run),
  }
}

interface MirroredWorkerJob {
  /** The session that owns the worker; used to prune on release. */
  readonly sessionID: string
  readonly registry: BackgroundJob.Interface
  /** The session-context runner, so registry methods resolve their instance. */
  readonly run: HubProxyRunner
  readonly done: Deferred.Deferred<{ readonly output?: string; readonly error?: string }>
  readonly promotionWaiters: Set<(promoted: boolean) => void>
  promoted: boolean
  settled: boolean
}

/** Resolve every pending promotion long-poll with one outcome and clear them. */
function resolvePromotionWaiters(job: MirroredWorkerJob, promoted: boolean): void {
  for (const waiter of job.promotionWaiters) waiter(promoted)
  job.promotionWaiters.clear()
}

/**
 * The hub-half of worker-job promotion. Each worker reports its running jobs
 * here; the mirror lives in the hub's own `BackgroundJob` registry so the
 * existing `list`/`promote` endpoint sees foreground jobs too. Promotion
 * resolves the worker's long-poll, which is the only way to run the worker-local
 * `onPromote` (and therefore the task notification) across the isolate.
 *
 * The mirror is keyed by job id and pruned when the worker reports settlement or
 * its session is released, so a dead worker cannot leave a permanently-running
 * ghost entry. Each authority is bound to the worker's own session: a job id
 * owned by another session is invisible, so a worker can neither settle nor
 * observe (promote) a peer's job.
 */
export interface WorkerJobMirror {
  /**
   * Build the job authority for ONE worker. `boundSessionID` is the session the
   * worker is registered to; it owns every job the authority registers and is
   * the only session whose jobs the authority may settle or await.
   */
  authorityFor(run: HubProxyRunner, boundSessionID: SessionID): WorkerBackgroundJobAuthority
  releaseSession(sessionID: string): void
  /**
   * The promotion decision the hub observed for a session: `promoted` if any of
   * its jobs was promoted, `not-promoted` if it started a foreground job that
   * stayed foreground, `no-job` if it started none. This is the run barrier's
   * `promotion` report at run end.
   */
  promotionOutcome(sessionID: string): RunBarrierPromotionOutcome
}

export function createWorkerJobMirror(): WorkerJobMirror {
  const jobs = new Map<string, MirroredWorkerJob>()
  // Decision memory outlives the job row: a promoted job settles and is pruned
  // before the run barrier reads the decision, and a foreground job that settled
  // still observed "not-promoted" rather than "no job".
  const promoted = new Set<string>()
  const hadJob = new Set<string>()

  const settleMirror = async (job: MirroredWorkerJob, input: WorkerBackgroundJobSettle): Promise<void> => {
    job.settled = true
    jobs.delete(input.id)
    resolvePromotionWaiters(job, false)
    if (input.status === "cancelled") {
      await job.run(job.registry.cancel(input.id)).catch(() => undefined)
      return
    }
    const payload = input.status === "error" ? { error: input.error ?? "job failed" } : { output: input.output ?? "" }
    await job.run(Deferred.succeed(job.done, payload)).catch(() => undefined)
  }

  return {
    authorityFor: (run, boundSessionID) => ({
      register: async (info) => {
        if (info.status !== "running") return
        // A worker may only register jobs under its own session. A supplied
        // parentID is a claim, not authority: reject a foreign one so a worker
        // cannot reparent a job into another session's mirror.
        if (info.parentID !== undefined && info.parentID !== boundSessionID) {
          throw new Error(
            `worker bound to ${boundSessionID} may not register job ${info.id} under ${info.parentID}`,
          )
        }
        const existing = jobs.get(info.id)
        if (existing !== undefined) {
          // A job id already mirrored by another session is foreign state, never
          // something this worker may promote or overwrite.
          if (existing.sessionID !== boundSessionID) {
            throw new Error(`job ${info.id} is not owned by the worker's session ${boundSessionID}`)
          }
          // A resumed foreground job is promoted worker-side; propagate that
          // state so the hub mirror stops reporting it as foreground. Promoting
          // an already-background mirror is an observation, not a new promotion.
          if (info.metadata?.background === true && !existing.promoted) {
            await run(existing.registry.promote(info.id))
          }
          return
        }
        const registry = await run(Effect.serviceOption(BackgroundJob.Service))
        if (Option.isNone(registry)) return
        const job: MirroredWorkerJob = {
          sessionID: boundSessionID,
          registry: registry.value,
          run,
          done: Effect.runSync(Deferred.make<{ readonly output?: string; readonly error?: string }>()),
          promotionWaiters: new Set(),
          promoted: false,
          settled: false,
        }
        jobs.set(info.id, job)
        try {
          await run(
            registry.value.start({
              id: info.id,
              type: info.type,
              ...(info.title === undefined ? {} : { title: info.title }),
              ...(info.metadata === undefined ? {} : { metadata: info.metadata }),
              // The mirror owner is authoritative: force the hub-registry parent
              // to the bound session even when the worker omitted or spoofed it.
              parentID: boundSessionID,
              onPromote: Effect.sync(() => {
                job.promoted = true
                promoted.add(job.sessionID)
                resolvePromotionWaiters(job, true)
              }),
              run: Deferred.await(job.done).pipe(
                Effect.flatMap((payload) =>
                  payload.error === undefined
                    ? Effect.succeed(payload.output ?? "")
                    : Effect.fail(new Error(payload.error)),
                ),
              ),
            }),
          )
        } catch (error) {
          jobs.delete(info.id)
          resolvePromotionWaiters(job, false)
          throw error
        }
        hadJob.add(job.sessionID)
      },
      settle: async (input) => {
        const job = jobs.get(input.id)
        // Unknown and foreign ids are no-ops: a worker cannot cancel or settle
        // another session's mirrored job.
        if (job === undefined || job.sessionID !== boundSessionID) return
        await settleMirror(job, input)
      },
      awaitPromotion: (id) => {
        const job = jobs.get(id)
        // A foreign or unknown id is never observable, so a worker cannot learn
        // another session's promotion outcome (or hold its long-poll open).
        if (job === undefined || job.sessionID !== boundSessionID || job.settled) return Promise.resolve(false)
        if (job.promoted) return Promise.resolve(true)
        return new Promise((resolve) => job.promotionWaiters.add(resolve))
      },
    }),
    releaseSession: (sessionID) => {
      promoted.delete(sessionID)
      hadJob.delete(sessionID)
      for (const [id, job] of jobs) {
        if (job.sessionID !== sessionID) continue
        void settleMirror(job, { id, status: "cancelled" })
      }
    },
    promotionOutcome: (sessionID) => {
      if (promoted.has(sessionID)) return "promoted"
      return hadJob.has(sessionID) ? "not-promoted" : "no-job"
    },
  }
}
