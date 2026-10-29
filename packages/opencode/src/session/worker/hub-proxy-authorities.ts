import { Context, Deferred, Effect, Option, Schema } from "effect"
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


/** The hub-resident authorities a hub-proxy handler map dispatches to. */
export interface DiscardRevertedRequest {
  readonly sessionID: string
  readonly messageIDs: ReadonlyArray<string>
  readonly parts: ReadonlyArray<{ readonly messageID: string; readonly partID: string }>
}

export interface HubProxyAuthorities {
  readonly sessionStatus: SessionStatus.Interface
  readonly permission: Permission.Interface
  readonly question: Question.Interface
  /** Hub-owned revert discard. */
  readonly discardReverted?: (request: DiscardRevertedRequest) => Promise<void>
  /** Session-spawn authority. */
  readonly sessionSpawn?: (request: SessionSpawnRequest) => Promise<SessionSpawnResult>
  /** Hub-side child-session scope. */
  readonly sessionCreate?: (request: SessionCreateRequest) => Promise<Session.Info>
  readonly sessionGet?: (request: SessionGetRequest) => Promise<Session.Info | null>
  readonly sessionChildren?: (request: SessionChildrenRequest) => Promise<readonly Session.Info[]>
  readonly sessionCancel?: (request: SessionCancelRequest) => Promise<CancelOutcome | undefined>
  /** Optional system-prompt transform authority. */
  readonly systemTransform?: (
    input: SystemTransformInput,
    output: SystemTransformOutput,
  ) => Promise<SystemTransformOutput>
  /** Optional message-list transform authority. */
  readonly messagesTransform?: (
    input: MessagesTransformInput,
    output: MessagesTransformOutput,
  ) => Promise<MessagesTransformOutput>
  /** Optional hub-proxied plugin trigger. */
  readonly pluginTrigger?: (name: string, input: unknown, output: unknown) => Promise<unknown>
  /** Optional plugin-tool execute authority. */
  readonly pluginTool?: (
    input: PluginToolExecuteInput,
    emit: PluginToolProgressSink,
  ) => Promise<PluginToolExecuteResult>
  /** Optional plugin-tool list authority: the clone-safe descriptor refresh. */
  readonly pluginToolList?: () => Promise<readonly PluginToolDescriptor[]>
  /** Optional plugin-tool abort authority: unwind an in-flight plugin tool's `ctx.abort` by `(sessionID, callID)`. */
  readonly pluginToolAbort?: (input: { readonly sessionID: string; readonly callID: string }) => void | Promise<void>
  /** Optional worker-job mirror authority. */
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

/** Executes a hub authority effect for a worker request. */
export type HubProxyRunner = <A, E>(effect: Effect.Effect<A, E>) => Promise<A>

/** The prompt authority's shared service key. */
const promptAuthority = SessionPromptService.Service

/** The descriptive rejection an absent plugin-tool authority returns (fail loud). */
function missingPluginToolAuthority(what: string): Error {
  return new Error(`the hub has no "pluginTool" authority wired; cannot ${what}`)
}

/** Build the hub-side handler map for the worker hub-proxy wire. */
export function buildHubProxyHandlers(authorities: HubProxyAuthorities, run: HubProxyRunner): HubProxyHandlers {
  const handlers: HubProxyHandlers & {
    readonly "session.discardReverted": (request: unknown) => Promise<void>
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
    // Always served: a worker's `experimental.chat.system.transform` reaches the hub, which runs the plugin hook and returns the mutated `{system}`.
    systemTransform: (input, output) =>
      authorities.systemTransform === undefined
        ? (output as SystemTransformOutput)
        : authorities.systemTransform(input as SystemTransformInput, output as SystemTransformOutput),
    // Always served: a worker's `experimental.chat.messages.transform` reaches the hub, which runs the plugin hook and returns the mutated `{messages}`.
    messagesTransform: (input, output) =>
      authorities.messagesTransform === undefined
        ? (output as MessagesTransformOutput)
        : authorities.messagesTransform(input as MessagesTransformInput, output as MessagesTransformOutput),
    pluginTrigger: (name, input, output) =>
      authorities.pluginTrigger === undefined ? output : authorities.pluginTrigger(name as string, input, output),
    // Always served, but with no identity fallback: an execute has no result to hand back, so an absent authority is a descriptive rejection.
    "pluginTool.execute": (input) =>
      authorities.pluginTool === undefined
        ? Promise.reject(missingPluginToolAuthority("execute a plugin-provided tool"))
        : authorities.pluginTool(input as PluginToolExecuteInput, () => {}),
    // The descriptor refresh RPC; absent a list authority there is no useful empty fallback.
    "pluginTool.list": () =>
      authorities.pluginToolList === undefined
        ? Promise.reject(missingPluginToolAuthority("list plugin-provided tools"))
        : authorities.pluginToolList(),
    // Best-effort: no authority means nothing is in flight to abort, not an error.
    "pluginTool.abort": (input) =>
      authorities.pluginToolAbort?.(input as { readonly sessionID: string; readonly callID: string }),
    // Worker-owned jobs are mirrored into the hub registry when the worker reports them.
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
    // Absent an authority fails loud, never silently leaving a stale revert behind.
    "session.discardReverted": (request) => {
      if (authorities.discardReverted === undefined) {
        return Promise.reject(
          new Error('the hub has no "discardReverted" authority wired; cannot discard a session revert'),
        )
      }
      return authorities.discardReverted(request as DiscardRevertedRequest)
    },
  }
  return handlers
}

/** The streaming half of the hub-proxy handler map: the methods whose handler may push correlated events to the awaiting worker before it replies. */
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
        sink.emit("pluginTool.metadata", progress),
      )
    },
  }
}

/** Build the hub-resident `experimental.chat.messages.transform` authority for a worker. */
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

/** Build the hub-resident authority for the worker's hub-proxied plugin hook bridge. */
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

/** Raised when a worker asks the hub to run a plugin tool id that no loaded plugin currently exposes — a stale descriptor. */
export class UnknownPluginToolError extends Schema.TaggedErrorClass<UnknownPluginToolError>(
  "UnknownPluginToolError",
)("HubProxyAuthorities.UnknownPluginTool", {
  toolID: Schema.String,
}) {
  constructor(override readonly toolID: string) {
    super({ toolID })
    this.name = "UnknownPluginToolError"
  }

  override get message() {
    return `unknown plugin tool "${this.toolID}": no loaded plugin exposes that tool id`
  }
}

/** Raised when the args a worker forwarded do not satisfy the plugin tool's own decoder. */
export class InvalidPluginToolArgsError extends Schema.TaggedErrorClass<InvalidPluginToolArgsError>(
  "InvalidPluginToolArgsError",
)("PluginTool.InvalidArgs", {
  toolID: Schema.String,
  detail: Schema.String,
}) {
  constructor(
    override readonly toolID: string,
    override readonly detail: string,
  ) {
    super({ toolID, detail })
    this.name = "InvalidPluginToolArgsError"
  }
  override get message() {
    return `invalid args for plugin tool "${this.toolID}": ${this.detail}`
  }
}

/** A hub-executed tool or permission prompt rejected; `detail` carries the original message. */
export class PluginToolExecutionError extends Schema.TaggedErrorClass<PluginToolExecutionError>(
  "PluginToolExecutionError",
)("PluginTool.Execution", {
  toolID: Schema.String,
  detail: Schema.String,
  cause: Schema.Unknown,
}) {
  override get message() {
    return this.detail
  }
}

const executionError = (toolID: string) => (cause: unknown) =>
  new PluginToolExecutionError({ toolID, detail: cause instanceof Error ? cause.message : String(cause), cause })

/** A registered in-flight plugin-tool call: its abort signal and unregister fn. */
export interface PluginToolAbortHandle {
  readonly signal: AbortSignal
  readonly dispose: () => void
}

/** The hub's in-flight plugin-tool abort registry. */
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
        // Unregister on EVERY exit; the entry is dropped only when it is still the registered controller, so a later call reusing the callID is untouched.
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

/** Module-level projection of the hub's MCP catalog plus the LSP tool into the same clone-safe descriptor shape plugin tools use. */
export const resolveContextHubToolDescriptors = Effect.fn("HubProxyAuthorities.resolveContextHubToolDescriptors")(
  function* (context: Context.Context<never> | undefined) {
  const descriptors: HubToolDescriptor[] = []
  for (const descriptor of yield* resolveContextPluginToolDescriptors(context)) {
    descriptors.push({ ...descriptor, source: "plugin" })
  }
  if (context === undefined) return descriptors
  const mcp = Context.getOption(context, MCP.Service)
  if (Option.isSome(mcp)) {
    const tools = yield* Effect.provide(mcp.value.tools(), context)
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
  return descriptors as readonly HubToolDescriptor[]
})

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
type PluginToolFailure =
  | UnknownPluginToolError
  | InvalidPluginToolArgsError
  | PluginToolNotSerializableError
  | PluginToolExecutionError

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

/** Validate forwarded args against the plugin tool's own decoder before `execute`. */
const validatePluginToolArgs = Effect.fn("HubProxyAuthorities.validatePluginToolArgs")(function* (
  toolID: string,
  definition: ToolDefinition,
  args: unknown,
) {
  const raw = definition.args ?? {}
  if (!Object.values(raw).every(isZodType)) return
  const parsed = z.object(raw).safeParse(args ?? {})
  if (!parsed.success) return yield* new InvalidPluginToolArgsError(toolID, parsed.error.message)
})

/** Fail loud on a result that cannot cross the control port, naming the tool and the offending field. */
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

/** Execute a hub-owned MCP or LSP tool a worker forwarded over the plugin-tool channel. */
const executeHubTool = (
  input: PluginToolExecuteInput,
  emit: PluginToolProgressSink,
  registerAbort?: (sessionID: string, callID: string) => PluginToolAbortHandle,
): Effect.Effect<PluginToolExecuteResult, UnknownPluginToolError | PluginToolExecutionError, PluginToolServices> =>
  Effect.gen(function* () {
    const permission = yield* Permission.Service
    const truncate = yield* Truncate.Service
    const agents = yield* Agent.Service
    const sessions = yield* Session.Service
    const instance = yield* InstanceRef
    const agentInfo = yield* agents.get(input.agent)
    const sessionInfo = yield* sessions.get(input.sessionID as SessionID).pipe(Effect.orDie)
    // Register only once the tool is known to execute: an unknown id must never leave an abort entry the cancel path could later reach.
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
      permission
        .ask({
          ...request,
          sessionID: input.sessionID as SessionID,
          tool: { messageID: input.messageID, callID: input.callID },
          ruleset: Permission.merge(agentInfo.permission, sessionInfo.permission ?? []),
        })
        .pipe(Effect.orDie)

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
          ask: (request) => ask(request as never),
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
      yield* ask({ permission: input.toolID, patterns: ["*"], always: ["*"], metadata: {} })
      const raw = yield* Effect.tryPromise({
        try: () =>
          (tool.execute as (args: unknown, options: { abortSignal: AbortSignal }) => Promise<unknown>)(input.args, {
            abortSignal: abort.signal,
          }),
        catch: executionError(input.toolID),
      })
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
  }) as Effect.Effect<PluginToolExecuteResult, UnknownPluginToolError | PluginToolExecutionError, PluginToolServices>

/** Run one plugin tool on the hub: locate its definition, validate the forwarded args, reconstruct the `ToolContext` the plugin expects, execute. */
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
        Effect.catchTag("HubProxyAuthorities.UnknownPluginTool", () =>
          Effect.fail(new UnknownPluginToolError(input.toolID)),
        ),
      )
    }
    yield* validatePluginToolArgs(input.toolID, definition, input.args)
    const bridge = yield* EffectBridge.make()
    const permission = yield* Permission.Service
    const truncate = yield* Truncate.Service
    const agents = yield* Agent.Service
    const sessions = yield* Session.Service
    const instance = yield* InstanceRef
    const agentInfo = yield* agents.get(input.agent)
    const sessionInfo = yield* sessions.get(input.sessionID as SessionID).pipe(Effect.orDie)
    // Register only once we are committed to executing: an unknown tool or bad args must never leave an entry the cancel path could later abort.
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
      abort: abort.signal,
      metadata: (update) => emit({ title: update.title, metadata: update.metadata }),
      // Bound to the HUB-RESIDENT permission service.
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
      const result = yield* Effect.tryPromise({
        try: () => definition.execute(input.args as never, pluginCtx),
        catch: executionError(input.toolID),
      })
      const output = typeof result === "string" ? result : result.output
      const metadata = typeof result === "string" ? {} : (result.metadata ?? {})
      const attachments = typeof result === "string" ? undefined : result.attachments
      const bounded = yield* Truncate.boundResult(truncate, { output, metadata }, agentInfo)
      const shaped: PluginToolExecuteResult = {
        title: typeof result === "string" ? "" : (result.title ?? ""),
        output: bounded.output,
        metadata: bounded.metadata,
        ...(attachments === undefined ? {} : { attachments }),
      }
      return yield* Effect.try({
        try: () => sanitizePluginToolResult(input.toolID, shaped),
        catch: (cause) =>
          cause instanceof PluginToolNotSerializableError ? cause : executionError(input.toolID)(cause),
      })
    }).pipe(Effect.ensuring(Effect.sync(() => abort.dispose())))
  })
}

/** The hub-resident plugin-tool execute authority. */
export type PluginToolAuthority = (
  input: PluginToolExecuteInput,
  emit?: PluginToolProgressSink,
) => Promise<PluginToolExecuteResult>

/** Build the hub-resident plugin-tool authority for a worker. */
export function pluginToolAuthority(options: {
  readonly plugin: () => Option.Option<Plugin.Interface>
  readonly run: HubProxyRunner
  readonly registerAbort?: (sessionID: string, callID: string) => PluginToolAbortHandle
}): PluginToolAuthority {
  return (input, emit = () => {}) =>
    options.run(
      pluginToolExecute(options.plugin, input, emit, options.registerAbort).pipe(
        // The tool's own rejection (a permission denial, say) crosses the port unwrapped.
        Effect.catchTag("PluginTool.Execution", (error) => Effect.fail(error.cause)),
      ) as unknown as Effect.Effect<PluginToolExecuteResult, UnknownPluginToolError>,
    )
}

/** Resolve the clone-safe plugin-tool descriptors for a dispatch context. */
export const resolveContextPluginToolDescriptors = Effect.fn("HubProxyAuthorities.resolveContextPluginToolDescriptors")(
  function* (context: Context.Context<never> | undefined) {
  if (context === undefined) return [] as readonly PluginToolDescriptor[]
  const plugin = Context.getOption(context, Plugin.Service)
  if (Option.isNone(plugin)) return [] as readonly PluginToolDescriptor[]
  const hooks = yield* Effect.provide(plugin.value.list(), context)
  const descriptors: PluginToolDescriptor[] = []
  for (const hook of hooks) {
    for (const [id, definition] of Object.entries(hook.tool ?? {})) {
      descriptors.push({ id, description: definition.description, jsonSchema: pluginToolJsonSchema(definition) })
    }
  }
  return descriptors as readonly PluginToolDescriptor[]
})

/** Caller metadata is data, never classification. */
function withoutSpawnDepth(metadata: Record<string, unknown> | undefined): Record<string, unknown> | undefined {
  if (metadata === undefined) return undefined
  const { depth: _depth, ...rest } = metadata
  return rest
}

/** The hub's authoritative child spawn. */
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

/** Resolve the worker's authoritative bound session. */
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

/** Whether `target` is a DIRECT child of `bound`. */
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

/** Cancel the bound session (its own run, cascading to its descendants on the hub) or one of its direct children. */
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

/** The dependencies a session-scope hub authority runs under. */
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

/** The hub's authoritative child spawn. */
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
        // Record the edge synchronously, before the child's own worker can start.
        deps.registerChild(boundSessionID, spawned.childID)
        return spawned
      }),
    )
}

/** Bind every `sessionStatus` operation to the worker's own session. */
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

/** Bind a per-session cancel to the worker's own session. */
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

/** Bind a revert discard to the worker's own session before it reaches `Session.discardReverted`. */
function bindSessionDiscardReverted(
  discard: NonNullable<HubProxyAuthorities["discardReverted"]>,
  boundSessionID: SessionID | undefined,
): NonNullable<HubProxyAuthorities["discardReverted"]> {
  return async (request) => {
    const foreign = foreignSessionError("discard a session revert", boundSessionID, request.sessionID)
    if (foreign !== undefined) throw foreign
    await discard(request)
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
  readonly discardReverted?: NonNullable<HubProxyAuthorities["discardReverted"]>
  readonly backgroundJob?: NonNullable<HubProxyAuthorities["backgroundJob"]>
}

/** The production authority map for one worker. */
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
    ...(deps.discardReverted === undefined
      ? {}
      : { discardReverted: bindSessionDiscardReverted(deps.discardReverted, deps.boundSessionID) }),
    // The mirror is session-bound, so an unbound worker must never receive one: an authority without a bound session could otherwise act on any job id.
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

/** The hub-half of worker-job promotion. */
export interface WorkerJobMirror {
  /** Build the job authority for ONE worker. */
  authorityFor(run: HubProxyRunner, boundSessionID: SessionID): WorkerBackgroundJobAuthority
  releaseSession(sessionID: string): void
  /** The promotion decision the hub observed for a session: `promoted` if any of its jobs was promoted. */
  promotionOutcome(sessionID: string): RunBarrierPromotionOutcome
}

export function createWorkerJobMirror(): WorkerJobMirror {
  const jobs = new Map<string, MirroredWorkerJob>()
  // Decision memory outlives the job row: a promoted job settles and is pruned before the run barrier reads the decision.
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
        // A worker may only register jobs under its own session.
        if (info.parentID !== undefined && info.parentID !== boundSessionID) {
          throw new Error(
            `worker bound to ${boundSessionID} may not register job ${info.id} under ${info.parentID}`,
          )
        }
        const existing = jobs.get(info.id)
        if (existing !== undefined) {
          // A job id already mirrored by another session is foreign state, never something this worker may promote or overwrite.
          if (existing.sessionID !== boundSessionID) {
            throw new Error(`job ${info.id} is not owned by the worker's session ${boundSessionID}`)
          }
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
          done: Deferred.makeUnsafe<{ readonly output?: string; readonly error?: string }>(),
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
        // Unknown and foreign ids are no-ops: a worker cannot cancel or settle another session's mirrored job.
        if (job === undefined || job.sessionID !== boundSessionID) return
        await settleMirror(job, input)
      },
      awaitPromotion: (id) => {
        const job = jobs.get(id)
        // A foreign or unknown id is never observable, so a worker cannot learn another session's promotion outcome (or hold its long-poll open).
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
