import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { httpClient } from "@opencode-ai/core/effect/app-node-platform"
import { Ripgrep } from "@opencode-ai/core/ripgrep"
import { PlanExitTool } from "./plan"
import { Session } from "@/session/session"
import { QuestionTool } from "./question"
import { ShellTool } from "./shell"
import { EditTool } from "./edit"
import { GlobTool } from "./glob"
import { GrepTool } from "./grep"
import { ReadTool } from "./read"
import { TaskTool } from "./task"
import { Database } from "@opencode-ai/core/database/database"
import { TodoWriteTool } from "./todo"
import { WebFetchTool } from "./webfetch"
import { WriteTool } from "./write"
import { InvalidTool } from "./invalid"
import { SkillTool } from "./skill"
import * as Tool from "./tool"
import { Config } from "@/config/config"
import { type ToolContext as PluginToolContext, type ToolDefinition } from "@opencode-ai/plugin"
import type { JSONSchema7, JSONSchema7Definition } from "@ai-sdk/provider"
import { Schema } from "effect"
import z from "zod"
import { Plugin } from "../plugin"
import { Provider } from "@/provider/provider"

import { WebSearchTool } from "./websearch"
import { LspTool } from "./lsp"
import * as Truncate from "./truncate"
import { ApplyPatchTool } from "./apply_patch"
import { Glob } from "@opencode-ai/core/util/glob"
import path from "path"
import { pathToFileURL } from "url"
import { Effect, Layer, Context } from "effect"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Format } from "../format"
import { InstanceState } from "@/effect/instance-state"
import { EffectBridge } from "@/effect/bridge"
import { Question } from "../question"
import { Todo } from "../session/todo"
import { LSP } from "@/lsp/lsp"
import { Instruction } from "../session/instruction"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { EventV2Bridge } from "@/event-v2-bridge"
import { Agent } from "../agent/agent"
import { Skill } from "../skill"
import { Permission } from "@/permission"
import { BackgroundJob } from "@/background/job"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import { MCP } from "@/mcp"
import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { McpCatalog } from "@/mcp/catalog"
import { sourceForRegistryTool, type ToolSource } from "./route"
import {
  isRemotePluginToolDefinition,
  type PluginToolExecuteResult,
  type RemotePluginToolContext,
  type RemotePluginToolDefinition,
} from "@/session/worker/plugin-tool-wire"

export function webSearchEnabled(providerID: ProviderV2.ID, flags = { exa: false, parallel: false }) {
  return (
    providerID === ProviderV2.ID.opencode ||
    providerID === ProviderV2.ID.make("opencode-go") ||
    flags.exa ||
    flags.parallel
  )
}

type TaskDef = Tool.InferDef<typeof TaskTool>
type ReadDef = Tool.InferDef<typeof ReadTool>

type State = {
  custom: Tool.Def[]
  builtin: Tool.Def[]
  sources: Map<string, ToolSource>
  task: TaskDef
  read: ReadDef
}

/** A registry tool plus the source that its routing decision derives from. */
export type RegisteredTool = Tool.Def & { readonly source: ToolSource }

export interface Interface {
  readonly ids: () => Effect.Effect<string[]>
  readonly all: () => Effect.Effect<Tool.Def[]>
  readonly named: () => Effect.Effect<{ task: TaskDef; read: ReadDef }>
  readonly tools: (model: {
    providerID: ProviderV2.ID
    modelID: ModelV2.ID
    agent: Agent.Info
    permission?: PermissionV1.Ruleset
  }) => Effect.Effect<RegisteredTool[]>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/ToolRegistry") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const config = yield* Config.Service
    const plugin = yield* Plugin.Service
    const agents = yield* Agent.Service
    const truncate = yield* Truncate.Service
    const flags = yield* RuntimeFlags.Service
    const mcp = yield* MCP.Service

    const invalid = yield* InvalidTool
    const task = yield* TaskTool
    const read = yield* ReadTool
    const question = yield* QuestionTool
    const todo = yield* TodoWriteTool
    const lsptool = yield* LspTool
    const plan = yield* PlanExitTool
    const webfetch = yield* WebFetchTool
    const websearch = yield* WebSearchTool
    const shell = yield* ShellTool
    const globtool = yield* GlobTool
    const writetool = yield* WriteTool
    const edit = yield* EditTool
    const greptool = yield* GrepTool
    const patchtool = yield* ApplyPatchTool
    const skilltool = yield* SkillTool
    const agent = yield* Agent.Service
    const codeMode = flags.experimentalCodeMode ? yield* Effect.promise(() => import("./code-mode")) : undefined
    const codeModeTool = codeMode ? yield* codeMode.CodeModeTool : undefined

    const state = yield* InstanceState.make<State>(
      Effect.fn("ToolRegistry.state")(function* (ctx) {
        const custom: Tool.Def[] = []
        const sources = new Map<string, ToolSource>()

        function fromPlugin(id: string, def: ToolDefinition): Tool.Def {
          // Plugin tools still expose Zod args publicly; keep that compatibility
          // boxed at the registry boundary and give the LLM the original JSON Schema.
          // Normalize missing args to `{}` once — pre-1.14.49 the code was
          // `z.object(def.args)` and Zod silently tolerated undefined (#27451, #27630).
          const { zodParams, jsonSchema } = pluginToolSchema(def)
          const parameters = zodParams
            ? Schema.declare<unknown>((u): u is unknown => zodParams.safeParse(u).success)
            : Schema.Unknown
          return {
            id,
            parameters,
            jsonSchema,
            description: def.description,
            execute: (args, toolCtx) =>
              Effect.gen(function* () {
                // Bridge the host's Effect-based `ask` into a Promise-returning
                // function for the plugin to make sure context persists
                const bridge = yield* EffectBridge.make()
                const pluginCtx: PluginToolContext = {
                  ...toolCtx,
                  ask: (req) => bridge.promise(toolCtx.ask(req)),
                  directory: ctx.directory,
                  worktree: ctx.worktree,
                }
                const result = yield* Effect.promise(() =>
                  def.execute(args as Parameters<ToolDefinition["execute"]>[0], pluginCtx),
                )
                const output = typeof result === "string" ? result : result.output
                const metadata = typeof result === "string" ? {} : (result.metadata ?? {})
                const attachments = typeof result === "string" ? undefined : result.attachments
                const info = yield* agent.get(toolCtx.agent)
                const bounded = yield* Truncate.boundResult(truncate, { output, metadata }, info)
                return {
                  title: typeof result === "string" ? "" : (result.title ?? ""),
                  output: bounded.output,
                  attachments,
                  metadata: bounded.metadata,
                }
              }).pipe(
                Effect.withSpan("Tool.execute", {
                  attributes: {
                    "tool.name": id,
                    "session.id": toolCtx.sessionID,
                    "message.id": toolCtx.messageID,
                    ...(toolCtx.callID ? { "tool.call_id": toolCtx.callID } : {}),
                  },
                }),
              ),
          }
        }

        const dirs = yield* config.directories()
        const matches = dirs.flatMap((dir) =>
          Glob.scanSync("{tool,tools}/*.{js,ts}", { cwd: dir, absolute: true, dot: true, symlink: true }),
        )
        if (matches.length) yield* config.waitForDependencies()
        for (const match of matches) {
          const namespace = path.basename(match, path.extname(match))
          // `match` is an absolute filesystem path from `Glob.scanSync(..., { absolute: true })`.
          // Import it as `file://` so Node on Windows accepts the dynamic import.
          const mod = yield* Effect.promise(() => import(pathToFileURL(match).href))
          for (const [id, def] of Object.entries(mod)) {
            if (!isPluginTool(def)) continue
            const toolID = id === "default" ? namespace : `${namespace}_${id}`
            custom.push(fromPlugin(toolID, def))
            sources.set(toolID, "file")
          }
        }

        const remoteToolIds = new Set<string>()
        const plugins = yield* plugin.list()
        for (const p of plugins) {
          for (const [id, def] of Object.entries(p.tool ?? {})) {
            // A worker ships hub-executed plugin tools as clone-safe remote
            // definitions; `fromPlugin` would call a local `execute` and
            // re-truncate, so those take the proxy-stub path instead.
            if (isRemotePluginToolDefinition(def)) {
              remoteToolIds.add(id)
              custom.push(fromRemotePluginTool(id, def, () => InstanceState.invalidate(state)))
              sources.set(id, "remote-plugin")
              continue
            }
            custom.push(fromPlugin(id, def))
            sources.set(id, "plugin")
          }
        }

        yield* config.get()
        const questionEnabled = ["app", "cli", "desktop"].includes(flags.client) || flags.enableQuestionTool

        const tool = yield* Effect.all({
          invalid: Tool.init(invalid),
          shell: Tool.init(shell),
          read: Tool.init(read),
          glob: Tool.init(globtool),
          grep: Tool.init(greptool),
          edit: Tool.init(edit),
          write: Tool.init(writetool),
          task: Tool.init(task),
          fetch: Tool.init(webfetch),
          todo: Tool.init(todo),
          search: Tool.init(websearch),
          skill: Tool.init(skilltool),
          patch: Tool.init(patchtool),
          question: Tool.init(question),
          lsp: Tool.init(lsptool),
          plan: Tool.init(plan),
          ...(codeModeTool ? { execute: Tool.init(codeModeTool) } : {}),
        })

        const builtin: Tool.Def[] = [
          tool.invalid,
          ...(questionEnabled ? [tool.question] : []),
          tool.shell,
          tool.read,
          tool.glob,
          tool.grep,
          tool.edit,
          tool.write,
          tool.task,
          tool.fetch,
          tool.todo,
          tool.search,
          tool.skill,
          tool.patch,
          ...(tool.execute ? [tool.execute] : []),
          ...(flags.experimentalLspTool ? [tool.lsp] : []),
          ...(flags.experimentalPlanMode && flags.client === "cli" ? [tool.plan] : []),
        ]

        assertRemoteToolIdsAreUnique(remoteToolIds, custom, builtin)
        for (const def of builtin) {
          if (!sources.has(def.id)) sources.set(def.id, sourceForRegistryTool(def.id))
        }

        return {
          custom,
          builtin,
          sources,
          task: tool.task,
          read: tool.read,
        }
      }),
    )

    const all: Interface["all"] = Effect.fn("ToolRegistry.all")(function* () {
      const s = yield* InstanceState.get(state)
      return [...s.builtin, ...s.custom] as Tool.Def[]
    })

    const ids: Interface["ids"] = Effect.fn("ToolRegistry.ids")(function* () {
      return (yield* all()).map((tool) => tool.id)
    })

    const describeTask = Effect.fn("ToolRegistry.describeTask")(function* (agent: Agent.Info) {
      const items = (yield* agents.list()).filter((item) => item.mode !== "primary")
      const filtered = items.filter(
        (item) => Permission.evaluate("task", item.name, agent.permission).action !== "deny",
      )
      const list = filtered.toSorted((a, b) => a.name.localeCompare(b.name))
      const description = list
        .map(
          (item) =>
            `- ${item.name}: ${item.description ?? "This subagent should only be called manually by the user."}`,
        )
        .join("\n")
      return ["Available agent types and the tools they have access to:", description].join("\n")
    })

    const describeCodeMode = Effect.fn("ToolRegistry.describeCodeMode")(function* (input: {
      agent: Agent.Info
      permission?: PermissionV1.Ruleset
    }) {
      if (!codeMode) return
      const ruleset = Permission.merge(input.agent.permission, input.permission ?? [])
      const tools = Permission.visibleTools(yield* mcp.tools(), ruleset)
      if (Object.keys(tools).length === 0) return
      return codeMode.describeCatalog(tools, Object.keys(yield* mcp.clients()).map(McpCatalog.sanitize))
    })

    const tools: Interface["tools"] = Effect.fn("ToolRegistry.tools")(function* (input) {
      const filtered = (yield* all()).filter((tool) => {
        if (tool.id === WebSearchTool.id) {
          return webSearchEnabled(input.providerID, { exa: flags.enableExa, parallel: flags.enableParallel })
        }

        const usePatch =
          input.modelID.includes("gpt-") && !input.modelID.includes("oss") && !input.modelID.includes("gpt-4")
        if (tool.id === ApplyPatchTool.id) return usePatch
        if (tool.id === EditTool.id || tool.id === WriteTool.id) return !usePatch

        return true
      })

      const codeModeDescription = filtered.some((tool) => tool.id === "execute")
        ? yield* describeCodeMode(input)
        : undefined
      const visible = filtered.filter((tool) => tool.id !== "execute" || codeModeDescription)

      return yield* Effect.forEach(
        visible,
        Effect.fnUntraced(function* (tool: Tool.Def) {
          const output = {
            description: tool.description,
            parameters: tool.parameters,
            jsonSchema: tool.jsonSchema,
          }
          yield* plugin.trigger("tool.definition", { toolID: tool.id }, output)
          const jsonSchema =
            output.parameters === tool.parameters || output.jsonSchema !== tool.jsonSchema
              ? output.jsonSchema
              : undefined
          return {
            id: tool.id,
            source: (yield* InstanceState.get(state)).sources.get(tool.id) ?? sourceForRegistryTool(tool.id),
            description: [
              output.description,
              tool.id === TaskTool.id ? yield* describeTask(input.agent) : undefined,
              tool.id === "execute" ? codeModeDescription : undefined,
            ]
              .filter(Boolean)
              .join("\n"),
            parameters: output.parameters,
            jsonSchema,
            execute: tool.execute,
            formatValidationError: tool.formatValidationError,
          }
        }),
        { concurrency: "unbounded" },
      )
    })

    const named: Interface["named"] = Effect.fn("ToolRegistry.named")(function* () {
      const s = yield* InstanceState.get(state)
      return { task: s.task, read: s.read }
    })

    return Service.of({ ids, all, named, tools })
  }),
)

function isZodType(value: unknown): value is z.ZodType {
  return typeof value === "object" && value !== null && "_zod" in value
}

function isPluginTool(value: unknown): value is ToolDefinition {
  return typeof value === "object" && value !== null && "args" in value && "description" in value && "execute" in value
}

/**
 * The Zod decoder (when every arg is a Zod type) and the LLM-facing JSON Schema
 * for a plugin tool's public args. Shared by {@link fromPlugin} (hub
 * registration) and {@link pluginToolJsonSchema} (worker descriptor extraction),
 * so the two paths can never compute a different schema.
 */
function pluginToolSchema(def: ToolDefinition): { zodParams: z.ZodType | undefined; jsonSchema: JSONSchema7 } {
  const args = def.args ?? {}
  const entries = Object.entries(args)
  const zodParams = entries.every((entry) => isZodType(entry[1])) ? z.object(args) : undefined
  return { zodParams, jsonSchema: zodParams ? zodJsonSchema(zodParams) : legacyJsonSchema(entries) }
}

/**
 * The LLM-facing JSON Schema for a plugin tool. Exported so the hub ships a
 * worker the exact `PluginToolDescriptor.jsonSchema` the hub registry would use,
 * and the worker's remote-stub registration passes it through verbatim.
 */
export function pluginToolJsonSchema(def: ToolDefinition): JSONSchema7 {
  return pluginToolSchema(def).jsonSchema
}

/**
 * The worker-side sink a hub-executed plugin tool reports live progress through.
 * Structurally mirrors the wire's `PluginToolProgressSink` but is declared here
 * (a local type, not an import) so the stub forwards progress without the
 * registry taking a hard dependency on each wire revision;
 * {@link fromRemotePluginTool} runs the reported effect on the tool part through
 * an `EffectBridge`.
 */
type RemoteProgressSink = (progress: { readonly title?: string; readonly metadata?: unknown }) => void | Promise<void>

/** The optional hub abort bridge a remote definition may expose. */
type RemoteAbortBridge = (ctx: RemotePluginToolContext) => void | Promise<void>

/** The progress-aware `RemotePluginToolDefinition.execute` shape (wire v2). */
type RemoteExecute = (
  args: unknown,
  ctx: RemotePluginToolContext,
  onProgress?: RemoteProgressSink,
) => Promise<PluginToolExecuteResult>

/** Read the optional abort bridge off a remote definition without importing it. */
function readRemoteAbortBridge(def: RemotePluginToolDefinition): RemoteAbortBridge | undefined {
  const abort = (def as { readonly abort?: unknown }).abort
  return typeof abort === "function" ? (abort as RemoteAbortBridge) : undefined
}

/** Read the optional lazy-refresh seam off a remote definition. */
function readRemoteRefresh(def: RemotePluginToolDefinition): (() => void | Promise<void>) | undefined {
  const refresh = (def as { readonly refresh?: unknown }).refresh
  return typeof refresh === "function" ? (refresh as () => void | Promise<void>) : undefined
}

/**
 * Whether a remote failure is the hub's `UnknownPluginToolError` — the stale
 * descriptor signal that triggers a refresh. The failure crosses the control
 * port as a `HubProxyRemoteError` carrying the remote `_tag`, so match on the tag
 * structurally rather than importing the hub-only class (which lives behind the
 * worker boundary and would couple this module to it).
 */
function isUnknownPluginToolError(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    (error as { readonly tag?: unknown }).tag === "HubProxyAuthorities.UnknownPluginTool"
  )
}

/**
 * Remote plugin tools own the LLM-facing tool id. A collision — with a built-in,
 * a `{tool,tools}/*` file tool, or another plugin — would make calls resolve to
 * whichever definition wins, so it fails loud at registry build instead of
 * silently shadowing. Non-remote duplicate ids keep the previous tolerance.
 *
 * `layer.ts` owns the descriptor-duplicate half of this policy
 * (`assertPluginToolDescriptorsUnique`, which cannot know the built-ins); the
 * registry owns the reserved/built-in half because it is the only place that
 * knows them. The wording matches `PluginToolIDCollisionError`'s "reserved" case.
 */
function assertRemoteToolIdsAreUnique(
  remoteIds: ReadonlySet<string>,
  custom: readonly Tool.Def[],
  builtin: readonly Tool.Def[],
): void {
  if (remoteIds.size === 0) return
  const occurrences = new Map<string, number>()
  for (const def of [...custom, ...builtin]) occurrences.set(def.id, (occurrences.get(def.id) ?? 0) + 1)
  for (const id of remoteIds) {
    if ((occurrences.get(id) ?? 0) > 1) {
      throw new Error(`plugin tool id "${id}" collides with a locally registered tool`)
    }
  }
}

/**
 * Register a hub-executed plugin tool as a worker-side stub. The hub ships only
 * the clone-safe descriptor (`plugin-tool-wire.ts`); the plugin's real `execute`
 * closes over hub state and cannot cross the isolate, so this stub forwards the
 * call over the hub control port through the supplied executor.
 *
 * Args pass through verbatim (`Schema.Unknown`): the hub validates them against
 * the same `jsonSchema` and already bounds the result with
 * `Truncate.boundResult`, so this path must NOT re-truncate. A remote failure
 * surfaces as a defect, exactly like `fromPlugin`'s rejected plugin promise.
 *
 * Live progress is forwarded to `toolCtx.metadata` through an `EffectBridge`,
 * and `toolCtx.abort` is subscribed so cancellation reaches the hub's per-call
 * abort bridge (best-effort; the listener is removed when the call settles).
 *
 * When the hub reports `UnknownPluginToolError` the shipped descriptor is stale:
 * `refresh` re-fetches the hub's set and `invalidateRemoteTools` drops the cached
 * registration, so the next build re-registers fresh descriptors. The current
 * call still fails (it cannot be retried transparently).
 */
function fromRemotePluginTool(
  id: string,
  def: RemotePluginToolDefinition,
  invalidateRemoteTools: () => Effect.Effect<void>,
): Tool.Def {
  return {
    id,
    description: def.description,
    parameters: Schema.Unknown,
    jsonSchema: def.jsonSchema as JSONSchema7,
    execute: (args, toolCtx) =>
      Effect.gen(function* () {
        const bridge = yield* EffectBridge.make()
        const remoteCtx: RemotePluginToolContext = {
          sessionID: toolCtx.sessionID,
          messageID: toolCtx.messageID,
          callID: toolCtx.callID,
          agent: toolCtx.agent,
        }
        const onProgress: RemoteProgressSink = (progress) =>
          bridge.promise(
            toolCtx.metadata({
              title: progress.title,
              metadata: progress.metadata as Tool.ExecuteResult["metadata"],
            }),
          )
        const abort = readRemoteAbortBridge(def)
        const onAbort = () => {
          if (!abort) return
          try {
            void Promise.resolve(abort(remoteCtx)).catch(() => {})
          } catch {
            // Best-effort: an abort-bridge failure must not tear down the call.
          }
        }
        // Subscribe before the RPC so an abort during execute reaches the hub; an
        // already-aborted signal (the call started cancelled) is honoured at once.
        if (toolCtx.abort.aborted) onAbort()
        else toolCtx.abort.addEventListener("abort", onAbort)

        return yield* Effect.tryPromise({
          try: () => (def.execute as RemoteExecute)(args, remoteCtx, onProgress),
          catch: (error) => error,
        }).pipe(
          Effect.ensuring(Effect.sync(() => toolCtx.abort.removeEventListener("abort", onAbort))),
          Effect.map(
            (result): Tool.ExecuteResult => ({
              title: result.title,
              output: result.output,
              metadata: (result.metadata ?? {}) as Tool.ExecuteResult["metadata"],
              attachments: result.attachments as Tool.ExecuteResult["attachments"],
            }),
          ),
        )
      }).pipe(
        Effect.catch((error) => {
          const refresh = readRemoteRefresh(def)
          if (refresh === undefined || !isUnknownPluginToolError(error)) return Effect.fail(error)
          return Effect.gen(function* () {
            yield* Effect.promise(() => Promise.resolve(refresh()))
            yield* invalidateRemoteTools()
            return yield* Effect.fail(error)
          })
        }),
        Effect.orDie,
      ),
  }
}

function isJsonSchemaDefinition(value: unknown): value is JSONSchema7Definition {
  return typeof value === "boolean" || (typeof value === "object" && value !== null && !Array.isArray(value))
}

function legacyJsonSchema(entries: [string, unknown][]): JSONSchema7 {
  const properties = Object.fromEntries(
    entries.filter((entry): entry is [string, JSONSchema7Definition] => isJsonSchemaDefinition(entry[1])),
  )
  return {
    type: "object",
    properties,
    required: Object.keys(properties),
  }
}

function zodJsonSchema(schema: z.ZodType): JSONSchema7 {
  const result = normalizeZodJsonSchema(z.toJSONSchema(schema, { io: "input", metadata: zodMetadataRegistry(schema) }))
  if (!isJsonSchemaObject(result)) throw new Error("plugin tool Zod schema produced a non-object JSON Schema")
  const { $defs, ...rest } = result
  return (
    $defs && isJsonSchemaObject($defs) ? { ...rest, definitions: $defs as JSONSchema7["definitions"] } : rest
  ) as JSONSchema7
}

function zodMetadataRegistry(schema: z.ZodType) {
  const registry = z.registry<Record<string, unknown>>()
  const seen = new WeakSet<object>()
  const collect = (value: unknown) => {
    if (typeof value !== "object" || value === null) return
    if (seen.has(value)) return
    seen.add(value)

    if (isZodType(value)) {
      const metadata = typeof value.meta === "function" ? value.meta() : undefined
      const description = typeof value.description === "string" ? value.description : undefined
      const merged = {
        ...(metadata && typeof metadata === "object" ? metadata : {}),
        ...(description ? { description } : {}),
      }
      if (Object.keys(merged).length) registry.add(value, merged)
      collect(value._zod.def)
      return
    }

    for (const item of Object.values(value)) collect(item)
  }
  collect(schema)
  return registry
}

function normalizeZodJsonSchema(value: unknown): unknown {
  if (Array.isArray(value)) return value.map((item) => normalizeZodJsonSchema(item))
  if (typeof value !== "object" || value === null) return value
  return Object.fromEntries(
    Object.entries(value)
      .filter((entry) =>
        (entry[0] === "exclusiveMaximum" || entry[0] === "exclusiveMinimum") && typeof entry[1] === "boolean"
          ? false
          : true,
      )
      .map(([key, item]) => [key, normalizeZodJsonSchema(item)]),
  )
}

function isJsonSchemaObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

export const node = LayerNode.make({
  service: Service,
  layer,
  deps: [
    Config.node,
    Plugin.node,
    Question.node,
    Todo.node,
    Agent.node,
    Skill.node,
    Session.node,
    BackgroundJob.node,
    Provider.node,
    LSP.node,
    Instruction.node,
    FSUtil.node,
    EventV2Bridge.node,
    httpClient,
    CrossSpawnSpawner.node,
    Format.node,
    Truncate.node,
    RuntimeFlags.node,
    MCP.node,
    Database.node,
    Ripgrep.node,
  ],
})

export * as ToolRegistry from "./registry"
