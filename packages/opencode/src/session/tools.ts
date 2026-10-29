import { Agent } from "@/agent/agent"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Provider } from "@/provider/provider"
import { ProviderTransform } from "@/provider/transform"
import { MCP } from "@/mcp"
import { McpCatalog } from "@/mcp/catalog"
import { Permission } from "@/permission"
import { Tool } from "@/tool/tool"
import { ToolJsonSchema } from "@/tool/json-schema"
import { ToolRegistry } from "@/tool/registry"
import { Truncate } from "@/tool/truncate"

import { Plugin } from "@/plugin"
import type { TaskPromptOps } from "@/tool/task"
import { type Tool as AITool, tool, jsonSchema, type ToolExecutionOptions, asSchema } from "ai"
import { Cause, Effect, Exit, Option } from "effect"
import {
  ToolCallLedger,
  ToolCallNotStartedError,
  ToolCallNotStartedServiceError,
  fromHandle,
  type LedgerExclusive,
  type ToolCallLedgerHandle,
} from "./ledger/tool-call"
import type { LifecycleReporting } from "./ledger/lifecycle"
import { Faults } from "@/testing/faults"
import { WorkerDb } from "./worker/db"
import { Database } from "@opencode-ai/core/database/database"
import { Session } from "./session"
import { SessionProcessor } from "./processor"
import { PartID } from "./schema"
import { EffectBridge } from "@/effect/bridge"
import { ModelV2 } from "@opencode-ai/core/model"
import { isRecord } from "@/util/record"
import {
  formatMcpResourceBytes,
  MAX_MCP_RESOURCE_BLOB_BYTES,
  mcpResourceBase64Size,
  SUPPORTED_MCP_RESOURCE_ATTACHMENT_MIMES,
} from "@/util/mcp-resource"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { ToolRoute } from "@/tool/route"

const MCP_RESOURCE_TOOLS = {
  list: "list_mcp_resources",
  listTemplates: "list_mcp_resource_templates",
  read: "read_mcp_resource",
} as const
const NEVER_ABORTED_SIGNAL = new AbortController().signal

export const resolve = Effect.fn("SessionTools.resolve")(function* (input: {
  agent: Agent.Info
  model: Provider.Model
  session: Session.Info
  processor: Pick<SessionProcessor.Handle, "message" | "updateToolCall" | "completeToolCall"> & {
    /**
     * Optional because a caller may supply a reduced processor (tests, degraded
     * boot). When present, a successful call is settled at the durable result
     * commit instead of at the tool effect's exit.
     */
    readonly registerToolSettlement?: SessionProcessor.Handle["registerToolSettlement"]
  }
  bypassAgentCheck: boolean
  messages: SessionV1.WithParts[]
  promptOps: TaskPromptOps
  /**
   * A synchronous ledger handle for direct/test callers. Production passes no
   * handle: the per-worker `ToolCallLedger` service is resolved from the graph,
   * and a hub process (no worker service) records nothing.
   */
  ledger?: ToolCallLedgerHandle
  /** The hub-observed run truth; mirrors in-flight calls for auto-resume when supplied. */
  lifecycle?: Pick<LifecycleReporting, "toolStarted" | "toolFinished">
}) {
  const tools: Record<string, AITool> = {}
  const run = yield* EffectBridge.make()
  const plugin = yield* Plugin.Service
  const permission = yield* Permission.Service
  const registry = yield* ToolRegistry.Service
  const mcp = yield* MCP.Service
  const truncate = yield* Truncate.Service
  const flags = yield* RuntimeFlags.Service
  // The one routing decision for this process: worker-local tools versus
  // hub-executed MCP/LSP. Every registration below asks this predicate, never
  // the presence of a client or a layer node.
  const toolHost = ToolRoute.hostForRole()
  const available = (ref: ToolRoute.ToolRef) => ToolRoute.runsOn(ToolRoute.routeTool(ref), toolHost)

  // The ledger row is the durable truth a crash is recovered from. Production
  // consumes the worker's `ToolCallLedger` service; a direct caller may still
  // supply a synchronous handle (tests). A hub process has no service, so it
  // records nothing.
  const ledger = yield* Effect.gen(function* () {
    if (input.ledger !== undefined) return fromHandle(input.ledger)
    const service = yield* Effect.serviceOption(ToolCallLedger.Service)
    if (Option.isSome(service) && service.value.available) return service.value
    // A reduced caller may provide the worker database without the ledger
    // service. Build the same service over it; absent a real worker DB (the
    // fail-loud boundary) the tool path keeps its previous no-ledger behavior.
    const workerDb = yield* Effect.serviceOption(WorkerDb)
    if (Option.isNone(workerDb) || typeof workerDb.value.filename !== "string") return undefined
    const repository = workerDb.value.repository
    if (typeof repository?.insert !== "function" || typeof repository?.select !== "function") return undefined
    const database = yield* Effect.serviceOption(Database.Service)
    const exclusive: LedgerExclusive | undefined = Option.isSome(database)
      ? <A>(operation: () => A) =>
          database.value.db
            .transaction(() => Effect.sync(operation), { behavior: "immediate" })
            .pipe(Effect.orDie)
      : undefined
    return yield* ToolCallLedger.makeFromWorker(workerDb.value, exclusive === undefined ? {} : { exclusive })
  })

  /**
   * Write the durable ledger finish. `commit` joins the owning result
   * transaction (a plain UPDATE); `settle` acquires the connection's exclusive
   * owner first. Settlement is bookkeeping: a ledger fault must never replace
   * the tool's own result or failure, so it is logged and swallowed.
   */
  const finishLedger = (
    ledgerKey: string,
    outcome: "ok" | "error",
    phase: "commit" | "settle",
  ): Effect.Effect<void> => {
    if (ledger === undefined) return Effect.void
    const finish = { callID: ledgerKey, sessionID: input.session.id, outcome }
    const write = phase === "commit" ? ledger.finish(finish) : ledger.finishStandalone(finish)
    return write.pipe(
      Effect.asVoid,
      Effect.catchCause((cause) => {
        const error = Cause.squash(cause)
        // A duplicate finish (abort result, then the processor's event) must not
        // turn a successful tool into a failure.
        if (error instanceof ToolCallNotStartedError || error instanceof ToolCallNotStartedServiceError) {
          return Effect.void
        }
        return Effect.logError("tool ledger finish failed", { callID: ledgerKey, sessionID: input.session.id, error })
      }),
    )
  }

  /**
   * Settle the durable row and the hub-observed in-flight entry exactly once.
   *
   * `commit` runs inside the result transaction and writes only the ledger.
   * `afterCommit` runs once that transaction has committed and reports the
   * finish to the hub, so the hub is never awaited while the writer is held.
   * `settle` is the standalone abort/cleanup path: no transaction exists, so it
   * writes the ledger under the connection semaphore and then reports.
   *
   * A terminal outcome settles exactly once. Cleanup's `abandon` is not an
   * outcome: the ledger row stays `started` (indeterminate) so a successful
   * result that lands after the run's settlement deadline can still settle `ok`
   * instead of being recorded as a false error.
   */
  const createSettlement = (callID: string, ledgerKey: string): SessionProcessor.ToolSettlement => {
    let settled: "ok" | "error" | undefined
    let hubFinished = false
    const finishHub = (outcome: "ok" | "error"): Effect.Effect<void> =>
      Effect.suspend(() => {
        if (hubFinished) return Effect.void
        hubFinished = true
        const finished = input.lifecycle?.toolFinished(input.session.id, callID)
        if (finished === undefined) return Effect.void
        return Effect.promise(() => finished).pipe(
          Effect.catchCause((cause) =>
            Effect.logError("tool lifecycle finish failed", { callID, sessionID: input.session.id, outcome, cause }),
          ),
        )
      })
    return (outcome, phase) => {
      if (phase === "abandon") return Effect.void
      if (phase === "commit") {
        if (settled !== undefined) return Effect.void
        // Left unmarked until `afterCommit`: a rolled-back transaction must
        // still be settled by cleanup.
        return finishLedger(ledgerKey, outcome, "commit")
      }
      if (settled !== undefined) return Effect.void
      settled = outcome
      if (phase === "afterCommit") return finishHub(outcome)
      return Effect.gen(function* () {
        // A late terminal outcome after abandonment is still the truth: the
        // ledger update succeeds because the row was never finished.
        yield* finishLedger(ledgerKey, outcome, "settle")
        yield* finishHub(outcome)
      })
    }
  }

  /**
   * Record `started` before the effect and settle it on every exit path. A
   * successful call is **not** settled here: its result part has not committed
   * yet, and a crash between this point and that commit must leave the row
   * `started` (indeterminate) instead of `finished` with no result. The
   * processor finishes it inside the result transaction. Typed failures,
   * defects and interruptions settle `error` immediately so a cancelled tool
   * releases the barrier's `tools` phase.
   *
   * `start` is always standalone: it waits for the connection's exclusive owner
   * (the Effect transaction semaphore), so a parallel tool's row can never be
   * written inside another tool's result transaction and erased by its rollback.
   */
  const withToolLedger = <A, E, R>(tool: string, callID: string, effect: Effect.Effect<A, E, R>) => {
    if (ledger === undefined && input.lifecycle === undefined) return effect
    // Only settle a call this wrapper actually recorded: a duplicate `start`
    // throws before the row exists and must not finish someone else's call.
    let recorded = false
    // The provider may reuse a call id; the durable ledger keys each call by
    // its owning assistant message so the second call never collides with (or
    // settles) the first. Lifecycle and the processor keep the provider id.
    const ledgerKey = `${input.processor.message.id}:${callID}`
    const settlement = createSettlement(callID, ledgerKey)
    const registered = input.processor.registerToolSettlement !== undefined
    return Effect.gen(function* () {
      recorded = false
      if (ledger !== undefined) {
        yield* ledger.startStandalone({ callID: ledgerKey, sessionID: input.session.id, tool, sideEffecting: true })
      }
      recorded = true
      // Wait for the hub to observe the start BEFORE the effect runs: a crash
      // between the two must leave an indeterminate call recovery refuses to
      // replay. The durable ledger row is written first for the same reason.
      const started = input.lifecycle?.toolStarted(input.session.id, callID, true)
      if (started !== undefined) yield* Effect.promise(() => started)
      // Register before the effect, not after it: a tool that observes an
      // aborted signal commits its result inside the effect, and that commit
      // must be able to drain the settlement. On success the registration is
      // left for the durable result commit; every other exit settles error.
      if (registered) input.processor.registerToolSettlement!(callID, settlement)
      Faults.crashWorker("tool:effect")
      const value = yield* effect
      // The side effect has happened but its durable result has not: a crash
      // here must leave the row `started`, so recovery reports indeterminate and
      // never replays the effect. Settlement is deferred past this point.
      Faults.crashWorker("tool:result")
      return value
    }).pipe(
      Effect.onExit((exit) => {
        if (!recorded) return Effect.void
        if (!Exit.isSuccess(exit)) return settlement("error", "settle")
        // Success settles only with its durable result. Without a processor hook
        // (reduced caller) fall back to settling here rather than leaking a
        // `started` row.
        if (registered) return Effect.void
        return settlement("ok", "settle")
      }),
    )
  }

  const context = (args: Record<string, unknown>, options: ToolExecutionOptions): Tool.Context => ({
    sessionID: input.session.id,
    abort: options.abortSignal ?? NEVER_ABORTED_SIGNAL,
    messageID: input.processor.message.id,
    callID: options.toolCallId,
    extra: { model: input.model, bypassAgentCheck: input.bypassAgentCheck, promptOps: input.promptOps },
    agent: input.agent.name,
    messages: input.messages,
    metadata: (val) =>
      input.processor.updateToolCall(options.toolCallId, (match) => {
        if (!["running", "pending"].includes(match.state.status)) return match
        return {
          ...match,
          state: {
            title: val.title,
            metadata: val.metadata,
            status: "running",
            input: args,
            time: match.state.status === "running" ? match.state.time : { start: Date.now() },
          },
        }
      }),
    ask: (req) =>
      permission
        .ask({
          ...req,
          sessionID: input.session.id,
          tool: { messageID: input.processor.message.id, callID: options.toolCallId },
          ruleset: Permission.merge(input.agent.permission, input.session.permission ?? []),
        })
        .pipe(Effect.orDie),
  })

  const registerRegistryTools = Effect.fnUntraced(function* () {
    for (const item of yield* registry.tools({
      modelID: ModelV2.ID.make(input.model.api.id),
      providerID: input.model.providerID,
      agent: input.agent,
      permission: input.session.permission,
    })) {
      if (!available({ id: item.id, source: item.source ?? ToolRoute.sourceForRegistryTool(item.id) })) continue
      const schema = ProviderTransform.schema(input.model, ToolJsonSchema.fromTool(item))
      tools[item.id] = tool({
        description: item.description,
        inputSchema: jsonSchema(schema),
        execute(args, options) {
          return run.promise(
            withToolLedger(
              item.id,
              options.toolCallId,
              Effect.gen(function* () {
                const ctx = context(args, options)
                yield* plugin.trigger(
                  "tool.execute.before",
                  { tool: item.id, sessionID: ctx.sessionID, callID: ctx.callID },
                  { args },
                )
                const result = yield* item.execute(args, ctx)
                const output = {
                  ...result,
                  attachments: result.attachments?.map((attachment) => ({
                    ...attachment,
                    id: PartID.ascending(),
                    sessionID: ctx.sessionID,
                    messageID: input.processor.message.id,
                  })),
                }
                yield* plugin.trigger(
                  "tool.execute.after",
                  { tool: item.id, sessionID: ctx.sessionID, callID: ctx.callID, args },
                  output,
                )
                if (options.abortSignal?.aborted) {
                  yield* input.processor.completeToolCall(options.toolCallId, output)
                }
                return output
              }),
            ),
          )
        },
      })
    }
  })

  yield* registerRegistryTools()

  type McpListable = { readonly client: string; readonly name: string }

  /**
   * Shared builder for the two "list" MCP tools. They differ only in the
   * resource accessor, the sort field, the result key and their labels; the
   * permission/plugin/truncation contract is identical.
   */
  const createMcpResourceListTool = <Item extends McpListable>(spec: {
    readonly tool: string
    readonly noun: string
    readonly description: string
    readonly title: string
    readonly resultKey: "resources" | "resourceTemplates"
    readonly sortKey: (item: Item) => string
    readonly fetch: (server?: string) => Effect.Effect<Record<string, Item>>
  }) =>
    tool({
      description: spec.description,
      inputSchema: jsonSchema(
        ProviderTransform.schema(input.model, {
          type: "object",
          properties: {
            server: {
              type: "string",
              description: `Optional MCP server name. When omitted, lists ${spec.noun} from every connected server.`,
            },
          },
          additionalProperties: false,
        }),
      ),
      execute(args, opts) {
        return run.promise(
          withToolLedger(
            spec.tool,
            opts.toolCallId,
            Effect.gen(function* () {
              const parsed = parseListMcpResourcesArgs(args)
              const ctx = context(toRecord(args), opts)
              const clients = yield* mcp.clients()
              const resourceServers = Object.entries(clients)
                .filter((entry) => !!entry[1].getServerCapabilities()?.resources)
                .map((entry) => entry[0])
                .sort((a, b) => a.localeCompare(b))
              if (parsed.server && !resourceServers.includes(parsed.server)) {
                throw new Error(
                  resourceServers.length === 0
                    ? `MCP server "${parsed.server}" does not support resources`
                    : `MCP server "${parsed.server}" does not support resources. Available resource servers: ${resourceServers.join(", ")}`,
                )
              }
              const permissionPatterns = parsed.server
                ? [`mcp:${parsed.server}:*`]
                : resourceServers.map((server) => `mcp:${server}:*`)
              yield* plugin.trigger(
                "tool.execute.before",
                { tool: spec.tool, sessionID: ctx.sessionID, callID: opts.toolCallId },
                { args },
              )
              yield* ctx.ask({
                permission: "read",
                metadata: parsed.server ? { server: parsed.server } : {},
                patterns: permissionPatterns,
                always: permissionPatterns,
              })

              const items = Object.values(yield* spec.fetch(parsed.server))
              const filtered = items
                .filter((item) => !parsed.server || item.client === parsed.server)
                .toSorted((a, b) => spec.sortKey(a).localeCompare(spec.sortKey(b)))
              const content = JSON.stringify({ [spec.resultKey]: filtered.map(formatMcpResourceEntry) }, null, 2)
              const bounded = yield* Truncate.boundResult(
                truncate,
                {
                  output: content,
                  metadata: {
                    count: filtered.length,
                    servers: resourceServers,
                    ...(parsed.server ? { server: parsed.server } : {}),
                  },
                },
                input.agent,
              )
              const output = {
                title: parsed.server ? `${spec.title}: ${parsed.server}` : spec.title,
                metadata: bounded.metadata,
                output: bounded.output,
              }
              yield* plugin.trigger(
                "tool.execute.after",
                { tool: spec.tool, sessionID: ctx.sessionID, callID: opts.toolCallId, args },
                output,
              )
              if (opts.abortSignal?.aborted) {
                yield* input.processor.completeToolCall(opts.toolCallId, output)
              }
              return output
            }),
          ),
        )
      },
    })

  const createMcpResourceReadTool = () =>
    tool({
      description:
        "Read a specific resource from an MCP server using the server name and resource URI. The URI is an MCP identifier and does not need to be a file URL.",
      inputSchema: jsonSchema(
        ProviderTransform.schema(input.model, {
          type: "object",
          properties: {
            server: {
              type: "string",
              description: "MCP server name exactly as returned by list_mcp_resources.",
            },
            uri: {
              type: "string",
              description: "Resource URI to read. Use the exact URI string returned by list_mcp_resources.",
            },
          },
          required: ["server", "uri"],
          additionalProperties: false,
        }),
      ),
      execute(args, opts) {
        return run.promise(
          withToolLedger(
            MCP_RESOURCE_TOOLS.read,
            opts.toolCallId,
            Effect.gen(function* () {
              const parsed = parseReadMcpResourceArgs(args)
            const ctx = context(toRecord(args), opts)
            const clients = yield* mcp.clients()
            const client = clients[parsed.server]
            if (!client) {
              throw new Error(`MCP server "${parsed.server}" is not connected`)
            }
            if (!client.getServerCapabilities()?.resources) {
              throw new Error(`MCP server "${parsed.server}" does not support resources`)
            }
            yield* plugin.trigger(
              "tool.execute.before",
              { tool: MCP_RESOURCE_TOOLS.read, sessionID: ctx.sessionID, callID: opts.toolCallId },
              { args },
            )
            yield* ctx.ask({
              permission: "read",
              metadata: { server: parsed.server, uri: parsed.uri },
              patterns: [`mcp:${parsed.server}:${parsed.uri}`],
              always: [`mcp:${parsed.server}:*`],
            })

            const content = yield* mcp.readResource(parsed.server, parsed.uri)
            if (!content) throw new Error(`Failed to read MCP resource: ${parsed.server}/${parsed.uri}`)

            const formatted = formatMcpResourceContent(parsed.server, parsed.uri, content)
            const bounded = yield* Truncate.boundResult(
              truncate,
              {
                output: formatted.text,
                metadata: {
                  server: parsed.server,
                  uri: parsed.uri,
                  contents: formatted.contents,
                  attachments: formatted.attachments.length,
                },
              },
              input.agent,
            )
            const output = {
              title: `MCP resource: ${parsed.uri}`,
              metadata: bounded.metadata,
              output: bounded.output,
              attachments: formatted.attachments.map((attachment) => ({
                ...attachment,
                id: PartID.ascending(),
                sessionID: ctx.sessionID,
                messageID: input.processor.message.id,
              })),
            }
            yield* plugin.trigger(
              "tool.execute.after",
              { tool: MCP_RESOURCE_TOOLS.read, sessionID: ctx.sessionID, callID: opts.toolCallId, args },
              output,
            )
            if (opts.abortSignal?.aborted) {
              yield* input.processor.completeToolCall(opts.toolCallId, output)
            }
            return output
            }),
          ),
        )
      },
    })

  const hasMcpResourceServer =
    available({ id: MCP_RESOURCE_TOOLS.list, source: "mcp" }) &&
    Object.values(yield* mcp.clients()).some((client) => !!client.getServerCapabilities()?.resources)
  if (hasMcpResourceServer) {
    tools[MCP_RESOURCE_TOOLS.list] = createMcpResourceListTool({
      tool: MCP_RESOURCE_TOOLS.list,
      noun: "resources",
      description:
        "Lists resources provided by connected MCP servers. Resources provide context such as files, database schemas, or application-specific information.",
      title: "MCP resources",
      resultKey: "resources",
      fetch: (server) => mcp.resources(server),
      sortKey: (item) => `${item.client}\u0000${item.name}\u0000${item.uri}`,
    })

    tools[MCP_RESOURCE_TOOLS.listTemplates] = createMcpResourceListTool({
      tool: MCP_RESOURCE_TOOLS.listTemplates,
      noun: "resource templates",
      description:
        "Lists resource templates provided by connected MCP servers. Resource templates are parameterized resources that can be read after filling in their URI template.",
      title: "MCP resource templates",
      resultKey: "resourceTemplates",
      fetch: (server) => mcp.resourceTemplates(server),
      sortKey: (item) => `${item.client}\u0000${item.name}\u0000${item.uriTemplate}`,
    })

    tools[MCP_RESOURCE_TOOLS.read] = createMcpResourceReadTool()
  }

  const registerMcpTools = Effect.fnUntraced(function* () {
    if (flags.experimentalCodeMode) return
    if (!available({ id: "mcp", source: "mcp" })) return
    for (const [key, entry] of Object.entries(yield* mcp.tools())) {
      const item = McpCatalog.convertTool(entry.def, entry.client, entry.timeout)
      const execute = item.execute
      if (!execute) continue

      const schema = yield* Effect.promise(() => Promise.resolve(asSchema(item.inputSchema).jsonSchema))
      const transformed = ProviderTransform.schema(input.model, { ...schema, properties: schema.properties ?? {} })
      item.inputSchema = jsonSchema(transformed)
      item.execute = (args, opts) =>
        run.promise(
          withToolLedger(
            key,
            opts.toolCallId,
            Effect.gen(function* () {
              const ctx = context(args, opts)
            yield* plugin.trigger(
              "tool.execute.before",
              { tool: key, sessionID: ctx.sessionID, callID: opts.toolCallId },
              { args },
            )
            const result: Awaited<ReturnType<NonNullable<typeof execute>>> = yield* Effect.gen(function* () {
              yield* ctx.ask({ permission: key, metadata: {}, patterns: ["*"], always: ["*"] })
              return yield* Effect.promise(() => execute(args, opts))
            }).pipe(
              Effect.withSpan("Tool.execute", {
                attributes: {
                  "tool.name": key,
                  "tool.call_id": opts.toolCallId,
                  "session.id": ctx.sessionID,
                  "message.id": input.processor.message.id,
                },
              }),
            )
            yield* plugin.trigger(
              "tool.execute.after",
              { tool: key, sessionID: ctx.sessionID, callID: opts.toolCallId, args },
              result,
            )

            const textParts: string[] = []
            const attachments: Omit<SessionV1.FilePart, "id" | "sessionID" | "messageID">[] = []
            for (const contentItem of result.content) {
              if (contentItem.type === "text") textParts.push(contentItem.text)
              else if (contentItem.type === "image") {
                attachments.push({
                  type: "file",
                  mime: contentItem.mimeType,
                  url: `data:${contentItem.mimeType};base64,${contentItem.data}`,
                })
              } else if (contentItem.type === "resource") {
                const { resource } = contentItem
                if (resource.text) textParts.push(resource.text)
                if (resource.blob) {
                  const mime = resource.mimeType ?? "application/octet-stream"
                  const size = mcpResourceBase64Size(resource.blob)
                  if (!SUPPORTED_MCP_RESOURCE_ATTACHMENT_MIMES.has(mime)) {
                    textParts.push(
                      `[Binary MCP resource omitted: ${resource.uri} (${mime}, ${formatMcpResourceBytes(size)}) is not a supported attachment type]`,
                    )
                    continue
                  }
                  if (size > MAX_MCP_RESOURCE_BLOB_BYTES) {
                    textParts.push(
                      `[Binary MCP resource omitted: ${resource.uri} (${mime}, ${formatMcpResourceBytes(size)}) exceeds ${formatMcpResourceBytes(MAX_MCP_RESOURCE_BLOB_BYTES)}]`,
                    )
                    continue
                  }
                  attachments.push({
                    type: "file",
                    mime,
                    url: `data:${mime};base64,${resource.blob}`,
                    filename: resource.uri,
                  })
                }
              }
            }

            const bounded = yield* Truncate.boundResult(
              truncate,
              { output: textParts.join("\n\n"), metadata: result.metadata },
              input.agent,
            )

            const output = {
              title: "",
              metadata: bounded.metadata,
              output: bounded.output,
              attachments: attachments.map((attachment) => ({
                ...attachment,
                id: PartID.ascending(),
                sessionID: ctx.sessionID,
                messageID: input.processor.message.id,
              })),
              content: result.content,
            }
            if (opts.abortSignal?.aborted) {
              yield* input.processor.completeToolCall(opts.toolCallId, output)
            }
            return output
            }),
          ),
        )
      tools[key] = item
    }
  })

  yield* registerMcpTools()

  return tools
})

function toRecord(value: unknown) {
  if (isRecord(value)) return value
  return {}
}

function parseListMcpResourcesArgs(value: unknown) {
  const args = toRecord(value)
  return { server: optionalString(args, "server") }
}

function parseReadMcpResourceArgs(value: unknown) {
  const args = toRecord(value)
  return { server: requiredString(args, "server"), uri: requiredString(args, "uri") }
}

function optionalString(args: Record<string, unknown>, key: string) {
  const value = args[key]
  if (value === undefined || value === null || value === "") return undefined
  if (typeof value !== "string") throw new Error(`${key} must be a string`)
  return value
}

function requiredString(args: Record<string, unknown>, key: string) {
  const value = optionalString(args, key)
  if (value) return value
  throw new Error(`${key} is required`)
}

function formatMcpResourceEntry(item: { client: string }) {
  const result = Object.fromEntries(Object.entries(item).filter((entry) => entry[0] !== "client"))
  return { ...result, server: item.client }
}

function formatMcpResourceContent(server: string, uri: string, content: { contents: unknown }) {
  const items = (Array.isArray(content.contents) ? content.contents : [content.contents]).filter(isRecord)
  const text: string[] = []
  const attachments: Omit<SessionV1.FilePart, "id" | "sessionID" | "messageID">[] = []

  for (const item of items) {
    const itemUri = typeof item.uri === "string" ? item.uri : uri
    const mime = typeof item.mimeType === "string" ? item.mimeType : "application/octet-stream"
    if (typeof item.text === "string") {
      text.push(`Resource: ${itemUri}\nMIME: ${mime}\n${item.text}`)
      continue
    }
    if (typeof item.blob === "string") {
      const size = mcpResourceBase64Size(item.blob)
      if (!SUPPORTED_MCP_RESOURCE_ATTACHMENT_MIMES.has(mime)) {
        text.push(
          `[Binary MCP resource omitted: ${itemUri} (${mime}, ${formatMcpResourceBytes(size)}) is not a supported attachment type]`,
        )
        continue
      }
      if (size > MAX_MCP_RESOURCE_BLOB_BYTES) {
        text.push(
          `[Binary MCP resource omitted: ${itemUri} (${mime}, ${formatMcpResourceBytes(size)}) exceeds ${formatMcpResourceBytes(MAX_MCP_RESOURCE_BLOB_BYTES)}]`,
        )
        continue
      }
      text.push(`[Binary MCP resource attached: ${itemUri} (${mime})]`)
      attachments.push({
        type: "file",
        mime,
        url: `data:${mime};base64,${item.blob}`,
        filename: itemUri,
      })
      continue
    }
    text.push(`[MCP resource content without text or blob: ${itemUri}]`)
  }

  return {
    contents: items.length,
    attachments,
    text: text.join("\n\n") || `MCP resource ${uri} from ${server} returned no contents.`,
  }
}

export * as SessionTools from "./tools"
