// Reduced session-worker AppLayer.

import { Context, Effect, Layer, Option, Schema, Stream } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { AppNodeBuilderV1 } from "@/effect/app-node-builder-v1"
import type { ConfigV1 } from "@opencode-ai/core/v1/config/config"
import { emptyConsoleState } from "@opencode-ai/core/v1/config/console-state"
import { ModelsDev } from "@opencode-ai/core/models-dev"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { LLMEvent } from "@opencode-ai/llm"
import { LLMClient, type LLMClientService, type LLMClientShape } from "@opencode-ai/llm/route"
import { llmClient } from "@opencode-ai/core/effect/app-node-platform"
import type { LanguageModelV3 } from "@ai-sdk/provider"
import { createAnthropic } from "@ai-sdk/anthropic"
import { createGoogleGenerativeAI } from "@ai-sdk/google"
import { createOpenAI } from "@ai-sdk/openai"
import { createOpenAICompatible } from "@ai-sdk/openai-compatible"

import type { SessionID } from "@/session/schema"
import { SessionStatus } from "@/session/status"
import { EventV2Bridge } from "@/event-v2-bridge"
import { EventV2 } from "@opencode-ai/core/event"
import { DurableLog } from "@opencode-ai/core/durable-log"
import { Latest } from "@/event-manifest"

import { Session } from "@/session/session"
import { SessionPrompt } from "@/session/prompt"
import { SessionProcessor } from "@/session/processor"
import { SessionRunState } from "@/session/run-state"
import { LLM } from "@/session/llm"
import { Plugin } from "@/plugin"
import { ToolRegistry } from "@/tool/registry"
import { Config } from "@/config/config"
import { Auth } from "@/auth"
import { Provider } from "@/provider/provider"
import { Agent } from "@/agent/agent"
import { Skill } from "@/skill"
import { Discovery } from "@/skill/discovery"
import { Question } from "@/question"
import { Permission } from "@/permission"
import { Todo } from "@/session/todo"
import { Truncate } from "@/tool/truncate"
import { Format } from "@/format"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { SessionCompaction } from "@/session/compaction"
import { SessionRevert } from "@/session/revert"
import { SessionSummary } from "@/session/summary"
import { Instruction } from "@/session/instruction"
import { SystemPrompt } from "@/session/system"
import { Command } from "@/command"
import { Image } from "@/image/image"
import { Snapshot } from "@/snapshot"
import { SessionProjector, sessionRow } from "@opencode-ai/core/session/projector"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { SessionEvent } from "@opencode-ai/core/session/event"
import { MessageTable, PartTable, SessionTable } from "@opencode-ai/core/session/sql"
import { eq } from "drizzle-orm"
import { Database } from "@opencode-ai/core/database/database"
import { MCP } from "@/mcp"
import { LSP } from "@/lsp/lsp"
import { McpAuth } from "@/mcp/auth"
import { McpBrowser } from "@/mcp/browser"
import { Npm } from "@opencode-ai/core/npm"
import { Account } from "@/account/account"
import { AccountRepo } from "@/account/repo"
import { Storage } from "@/storage/storage"
import { Git } from "@/git"
import { BackgroundJob } from "@/background/job"
import { Global } from "@opencode-ai/core/global"
import { RipgrepBinary } from "@opencode-ai/core/ripgrep/binary"
import { LocationServiceMap } from "@opencode-ai/core/location-service-map"
import { make } from "@opencode-ai/core/background-job"
import { buildLocationServiceMap } from "@opencode-ai/core/location-services"

import {
  EVENT_BRIDGE_KEY,
  EVENT_EGRESS_CLIENT_KEY,
  REQUIRED_SESSION_NODE_KEYS,
  WRITER_CLIENT_KEY,
  assertSessionLayer,
} from "./node-table"
import { createWorkerEventBridge, DURABLE_WAKEUP_METADATA_KEY } from "./event-bridge"
import type { WorkerDurableSubscription } from "./egress-client"
import type { DurableCommit, MeshWorkerClient } from "@/session/egress/mesh"
import { createDeltaSubscription, type SnapshotReader } from "@/session/egress/delta"
import type { EventV2Event } from "./frame"
import { stringHeaders } from "./headers"
import {
  HubProxyRemoteError,
  type HubProxyClient,
  type HubProxyHandlers,
  type HubProxyMethod,
  type HubProxyStreamHandler,
} from "./hub-proxy-wire"
import {
  UnclassifiedPluginHookError,
  UnsupportedPluginHookError,
  workerHookRoute,
} from "@/plugin/routes"
import {
  remotePluginToolHooks,
  type PluginToolAbort,
  type PluginToolDescriptor,
  type PluginToolExecutor,
} from "./plugin-tool-wire"
import type { Hooks } from "@opencode-ai/plugin"
import {
  workerPluginHostDeclaresHook,
  type ChatHeadersOutput,
  type ChatMessageOutput,
  type ChatParamsOutput,
  type MessagesTransformInput,
  type MessagesTransformOutput,
  type ShellEnvOutput,
  type SystemTransformInput,
  type SystemTransformOutput,
  type ToolAfterOutput,
  type ToolBeforeOutput,
  type ToolDefinitionOutput,
  type CommandBeforeOutput,
  type SessionCompactingOutput,
  type CompactionAutocontinueOutput,
  type TextCompleteOutput,
  workerPluginHostDeclarers,
  type WorkerPluginHost,
} from "./plugin-host"
import { WorkerDb, databaseLayer, type WorkerDatabase } from "./db"
import { ToolCallLedger } from "@/session/ledger/tool-call"
import { changedHubOwnedSessionColumns, HUB_OWNED_METADATA, SESSION_IMMUTABLE_COLUMNS } from "./repository/policy"
import { PidReporter, type PidReporterInterface } from "./pid-registry"
import {
  SessionScopeError,
  SessionSpawnProxyService,
  type SessionSpawnProxy,
  type SessionSpawnRequest,
  type SessionSpawnResult,
} from "./subagent"
import type { CancelOutcome } from "./cancel"

export { PidReporter } from "./pid-registry"
export type { PidReport, PidReportEntry, PidReporterInterface } from "./pid-registry"


/** The worker-side writer seam. */
export interface WriterClientHandle {
  readonly query: (method: string, sql: string, params?: readonly unknown[]) => Promise<unknown>
  readonly publish: (event: { id: string; type: string; aggregateID: string; data: unknown }) => Promise<{
    readonly seq: number | null
    readonly committed: boolean
  }>
}

export interface EventEgressClientHandle {
  /** Forward one EventV2 event VERBATIM; the egress transport frames it. */
  readonly emit: (event: EventV2Event) => void
  /** Forward a durable boundary event with its aggregate `(aggregateID, seq)`. */
  readonly emitDurable?: (event: EventV2Event, meta: { readonly aggregateID: string; readonly seq: number }) => void
  /** The mesh control-plane client, present exactly when the worker was given a control port. */
  readonly mesh?: MeshWorkerClient | undefined
  readonly subscribeDurable?: (subscription: WorkerDurableSubscription) => () => void
}

export interface BackgroundJobClientHandle {
  readonly wait: (id: string, timeoutMs?: number) => Promise<unknown>
  readonly cancel: (id: string) => Promise<void>
}

const workerTags = LayerNode.tags({ worker: [] })

/** A client node is a service class, its unbound graph node, and the replacement layer that installs a handle. */
function defineClientNode<Shape>(key: string) {
  class Service extends Context.Service<Service, Shape>()(key) {}
  return {
    Service,
    node: LayerNode.unbound(Service, workerTags.values.worker),
    layer: (handle: Shape) => Layer.succeed(Service, handle),
  }
}

const writerClient = defineClientNode<WriterClientHandle>(WRITER_CLIENT_KEY)
const eventEgressClient = defineClientNode<EventEgressClientHandle>(EVENT_EGRESS_CLIENT_KEY)

export const WriterClientService = writerClient.Service
export const EventEgressClientService = eventEgressClient.Service

export const WriterClientNode = writerClient.node
export const EventEgressClientNode = eventEgressClient.node

/** The per-worker database node. */
export const WorkerDbNode = LayerNode.unbound(WorkerDb, workerTags.values.worker)


/** The live worker-event subscription; the service key is the node-table allowlist key. */
export interface WorkerEventBridgeService {
  readonly dispose: Effect.Effect<void>
}

export class WorkerEventBridge extends Context.Service<WorkerEventBridge, WorkerEventBridgeService>()(
  EVENT_BRIDGE_KEY,
) {}


/** The single durable wakeup pattern a worker subscribes to. */
export const WORKER_DURABLE_WAKE_PATTERN = "*"

/** Parse a durable commit into this build's definition: the aggregate it belongs to and the definition version. */
function durablePositionOf(
  commit: DurableCommit,
): { readonly aggregateID: string; readonly version: number } | undefined {
  const durable = Latest.get(commit.event.type)?.durable
  if (durable === undefined) return undefined
  const properties = commit.event.properties
  const aggregateID =
    properties !== null && typeof properties === "object"
      ? (properties as Readonly<Record<string, unknown>>)[durable.aggregate]
      : undefined
  if (typeof aggregateID !== "string") {
    throw new Error(`durable commit "${commit.event.type}" is missing string aggregate field ${durable.aggregate}`)
  }
  return { aggregateID, version: durable.version }
}

/** Wake one aggregate's `durable()` subscribers so they re-read SQLite. */
function wakeAggregate(
  events: Pick<EventV2.Interface, "wakeDurable">,
  aggregateID: string,
): void {
  const wakeDurable = events.wakeDurable
  if (wakeDurable === undefined) return
  Effect.runSync(wakeDurable(aggregateID))
}

/** Bridge one hub-delivered durable commit into the worker's in-isolate EventV2. */
export function republishDurableCommit(
  events: Pick<EventV2.Interface, "publishLive" | "wakeDurable">,
  commit: DurableCommit,
): void {
  const position = durablePositionOf(commit)
  Effect.runSync(
    events.publishLive({
      id: EventV2.ID.make(commit.event.id),
      type: commit.event.type,
      data: commit.event.properties,
      ...(position === undefined
        ? {}
        : {
            durable: {
              aggregateID: commit.meta.aggregateID,
              seq: commit.meta.seq,
              version: position.version,
            },
          }),
      metadata: { [DURABLE_WAKEUP_METADATA_KEY]: true },
    }),
  )
  // The durable write already happened in the hub writer; the in-isolate `durable()` subscribers only need waking so their aggregate stream re-reads and.
  if (position === undefined) return
  wakeAggregate(events, position.aggregateID)
}

/** Wire the worker's durable wakeups: subscribe to the hub mesh through the egress handle's control plane and republish each delivered commit into the. */
/** The EventV2 seams a durable wakeup needs; `durable` enables gap detection. */
type WakeupEvents = Pick<EventV2.Interface, "publishLive" | "wakeDurable"> &
  Partial<Pick<EventV2.Interface, "durable">>

/** Read a database snapshot for one aggregate through the production durable stream. */
function durableSnapshotOf(events: WakeupEvents): SnapshotReader | undefined {
  const durable = events.durable
  if (durable === undefined) return undefined
  return async (aggregateID, seq, after = -1) => {
    // -1 is the only "before the first event" cursor; 0 is a real event.
    if (seq < 0 || after >= seq) return []
    const payloads = await Effect.runPromise(
      durable({ aggregateID, after }).pipe(
        Stream.takeUntil((payload) => (payload.durable?.seq ?? -1) >= seq),
        Stream.runCollect,
      ),
    )
    return Array.from(payloads).map((payload) => ({
      id: payload.id,
      type: payload.type,
      properties: payload.data,
    }))
  }
}

/** Wire the worker's durable wakeups. */
export function wireDurableWakeups(
  events: WakeupEvents,
  egress: Pick<EventEgressClientHandle, "subscribeDurable" | "mesh">,
): () => void {
  const subscribe = egress.subscribeDurable
  if (subscribe === undefined || egress.mesh === undefined) return () => {}
  const seen = new Set<string>()
  const snapshot = durableSnapshotOf(events)
  const trackers = new Map<string, { receiveDurable: (commit: DurableCommit) => void }>()

  const trackerFor = (aggregateID: string): { receiveDurable: (commit: DurableCommit) => void } | undefined => {
    if (snapshot === undefined) return undefined
    const existing = trackers.get(aggregateID)
    if (existing !== undefined) return existing
    const subscription = createDeltaSubscription({
      aggregateID,
      snapshot,
      onDelta: () => {},
      // The commit already crossed live in `onDurable`; this subscription exists only to detect a gap and heal it from the snapshot.
      onDurable: () => {},
      onResync: () => wakeAggregate(events, aggregateID),
    })
    const tracker = {
      receiveDurable: (commit: DurableCommit) =>
        subscription.receiveDurable({
          aggregateID: commit.meta.aggregateID,
          seq: commit.meta.seq,
          event: commit.event,
        }),
    }
    trackers.set(aggregateID, tracker)
    return tracker
  }

  const unsubscribe = subscribe({
    pattern: WORKER_DURABLE_WAKE_PATTERN,
    onDurable: (commit) => {
      const aggregateID = commit.meta.aggregateID
      seen.add(aggregateID)
      republishDurableCommit(events, commit)
      trackerFor(aggregateID)?.receiveDurable(commit)
    },
    // A resync means the hub dropped a delivery (overflow or receiver gap).
    onResync: (_pattern, position) => {
      if (position !== undefined) {
        wakeAggregate(events, position.aggregateID)
        return
      }
      for (const aggregateID of seen) wakeAggregate(events, aggregateID)
    },
  })
  return () => {
    trackers.clear()
    unsubscribe()
  }
}

/** The egress handle IS the events-port sink: it accepts the verbatim `{ id, type, properties }` event and its transport builds the `eventFrame`. */
const eventBridgeLayer = Layer.effect(
  WorkerEventBridge,
  Effect.gen(function* () {
    const events = yield* EventV2Bridge.Service
    const egress = yield* EventEgressClientService
    const bridge = yield* createWorkerEventBridge(events, egress)
    // The hub→worker durable wakeup mesh: the egress control client owns the subscription; each delivered commit is republished into this isolate's EventV2.
    const unsubscribeDurable = wireDurableWakeups(events, egress)
    // Both subscriptions live exactly as long as the layer scope; disposing the scope (worker shutdown) unsubscribes.
    yield* Effect.addFinalizer(() => Effect.sync(() => unsubscribeDurable()).pipe(Effect.andThen(bridge.dispose)))
    return WorkerEventBridge.of({ dispose: bridge.dispose })
  }),
)

export const WorkerEventBridgeNode = LayerNode.make({
  service: WorkerEventBridge,
  layer: eventBridgeLayer,
  deps: [EventV2Bridge.node, EventEgressClientNode],
})


export { createHubProxyClient, HubProxyRemoteError, serveHubProxy } from "./hub-proxy-wire"
export type {
  HubProxyClient,
  HubProxyFrame,
  HubProxyHandlers,
  HubProxyMethod,
  HubProxyReply,
  HubProxyRequest,
} from "./hub-proxy-wire"

/** Rebuild a hub authority's typed domain error from its wire form. */
function restoreHubProxyError(error: unknown): Error {
  if (!(error instanceof HubProxyRemoteError)) return error instanceof Error ? error : new Error(String(error))
  switch (error.tag) {
    case "PermissionRejectedError":
      return new PermissionV1.RejectedError()
    case "PermissionCorrectedError":
      return new PermissionV1.CorrectedError({
        feedback: String((error.data as { feedback?: unknown } | undefined)?.feedback ?? ""),
      })
    case "PermissionDeniedError":
      return new PermissionV1.DeniedError({
        ruleset: (error.data as { ruleset?: unknown } | undefined)?.ruleset ?? [],
      })
    case "QuestionRejectedError":
      return new Question.RejectedError()
    case "SessionScopeError":
      return new SessionScopeError({
        message: error.message,
        sessionID: String((error.data as { sessionID?: unknown } | undefined)?.sessionID ?? ""),
        targetID: String((error.data as { targetID?: unknown } | undefined)?.targetID ?? ""),
      })
    default:
      return error
  }
}

/** Await a hub-proxy call inside Effect; a remote failure is a typed error. */
function hubProxyCall<A>(
  client: HubProxyClient,
  method: HubProxyMethod,
  args: readonly unknown[],
): Effect.Effect<A, Error> {
  if (client.callEffect !== undefined) {
    return (client.callEffect(method, args) as Effect.Effect<A, unknown>).pipe(Effect.mapError(restoreHubProxyError))
  }
  return Effect.tryPromise({
    try: () => client.call(method, args) as Promise<A>,
    catch: restoreHubProxyError,
  })
}

const sessionStatusClient = defineClientNode<SessionStatus.Interface>(SessionStatus.Service.key)
const permissionClient = defineClientNode<Permission.Interface>(Permission.Service.key)
const questionClient = defineClientNode<Question.Interface>(Question.Service.key)

export const SessionStatusProxyNode = sessionStatusClient.node
export const PermissionProxyNode = permissionClient.node
export const QuestionProxyNode = questionClient.node

/** `SessionStatus` proxy over a hub port; methods satisfy the real interface. */
export function sessionStatusProxyLayer(client: HubProxyClient): Layer.Layer<SessionStatus.Service> {
  const service = {
    get: (sessionID: SessionID) => hubProxyCall<SessionStatus.Info>(client, "sessionStatus.get", [sessionID]),
    list: () =>
      hubProxyCall<ReadonlyArray<readonly [SessionID, SessionStatus.Info]>>(client, "sessionStatus.list", []).pipe(
        Effect.map((entries) => new Map(entries)),
      ),
    set: (sessionID: SessionID, status: SessionStatus.Info) =>
      hubProxyCall<void>(client, "sessionStatus.set", [sessionID, status]),
  } as unknown as SessionStatus.Interface
  return Layer.succeed(SessionStatus.Service, service)
}

/** `Permission` proxy over a hub port; methods satisfy the real interface. */
export function permissionProxyLayer(client: HubProxyClient): Layer.Layer<Permission.Service> {
  const service = {
    ask: (input: PermissionV1.AskInput) => hubProxyCall<void>(client, "permission.ask", [input]),
    reply: (input: PermissionV1.ReplyInput) => hubProxyCall<void>(client, "permission.reply", [input]),
    list: () => hubProxyCall<ReadonlyArray<PermissionV1.Request>>(client, "permission.list", []),
  } as unknown as Permission.Interface
  return Layer.succeed(Permission.Service, service)
}

/** `Question` proxy over a hub port; methods satisfy the real interface. */
export function questionProxyLayer(client: HubProxyClient): Layer.Layer<Question.Service> {
  const service = {
    ask: (input: { sessionID: SessionID; questions: ReadonlyArray<Question.Info>; tool?: Question.Tool }) =>
      hubProxyCall<ReadonlyArray<Question.Answer>>(client, "question.ask", [input]),
    reply: (input: {
      requestID: Parameters<Question.Interface["reject"]>[0]
      answers: ReadonlyArray<Question.Answer>
    }) => hubProxyCall<void>(client, "question.reply", [input]),
    reject: (requestID: Parameters<Question.Interface["reject"]>[0]) =>
      hubProxyCall<void>(client, "question.reject", [requestID]),
    list: () => hubProxyCall<ReadonlyArray<Question.Request>>(client, "question.list", []),
  } as unknown as Question.Interface
  return Layer.succeed(Question.Service, service)
}

export interface HubProxyClients {
  readonly sessionStatus: HubProxyClient
  readonly permission: HubProxyClient
  readonly question: HubProxyClient
  /** Optional dedicated subagent-dispatch client. */
  readonly subagent?: HubProxyClient
}

/** Worker-side `SessionSpawnProxy` over the hub-proxy control port. */
export function sessionSpawnProxyLayer(
  client: HubProxyClient,
  boundSessionID?: SessionID,
): Layer.Layer<SessionSpawnProxyService> {
  const scope: Pick<SessionSpawnProxy, "create" | "get" | "children" | "cancel"> =
    boundSessionID === undefined
      ? {}
      : {
          create: (input) =>
            hubProxyCall<Session.Info>(client, "session.create", [{ ...input, sessionID: boundSessionID }]),
          get: (targetID) =>
            hubProxyCall<Session.Info | null>(client, "session.get", [{ sessionID: boundSessionID, targetID }]),
          children: (parentID) =>
            hubProxyCall<readonly Session.Info[]>(client, "session.children", [
              { sessionID: boundSessionID, parentID },
            ]),
          cancel: (targetID) =>
            hubProxyCall<CancelOutcome | undefined>(client, "session.cancel", [
              { sessionID: boundSessionID, targetID },
            ]),
        }
  return Layer.succeed(SessionSpawnProxyService, {
    ...(boundSessionID === undefined ? {} : { boundID: boundSessionID }),
    spawn: (request: SessionSpawnRequest) => hubProxyCall<SessionSpawnResult>(client, "session.spawn", [request]),
    ...scope,
  })
}

/** The client the session-spawn proxy multiplexes over. */
export function sessionSpawnClient(clients: HubProxyClients): HubProxyClient {
  return clients.subagent ?? clients.sessionStatus
}

/** Worker-side revert-discard authority. */
export function clearRevertProxyLayer(client: HubProxyClient): Layer.Layer<SessionRevert.ClearRevertAuthority> {
  return Layer.succeed(SessionRevert.ClearRevertAuthority, {
    discard: (input) => hubProxyCall<void>(client, "session.discardReverted", [input]).pipe(Effect.orDie),
  })
}

/** The hub-proxy authorities as REPLACEMENTS, never as appended root nodes: the authority services are already part of the declared graph. */
export function hubProxyReplacements(clients: HubProxyClients): LayerNode.Replacements {
  return [
    [SessionStatusProxyNode, sessionStatusProxyLayer(clients.sessionStatus)],
    [PermissionProxyNode, permissionProxyLayer(clients.permission)],
    [QuestionProxyNode, questionProxyLayer(clients.question)],
  ]
}

/** The hub proxies not already composed by {@link sharedBoundaryReplacements}. */
function hubAuthorityReplacements(clients: HubProxyClients): LayerNode.Replacements {
  return hubProxyReplacements(clients).filter(([node]) => node.name !== Permission.node.name)
}


export interface WorkerSnapshot {
  /** Monotonic snapshot epoch; a bump invalidates stale-tolerant caches. */
  readonly epoch: number
  readonly directory: string
  /** The project id the worker's ambient `InstanceRef` carries. */
  readonly projectId?: string
  /** The project VCS (`"git"`) when the hub's project is version-controlled. */
  readonly projectVcs?: "git"
  /** The project worktree; defaults to `directory` in the worker. */
  readonly worktree?: string
  /** The hub's `config.directories()`, shipped verbatim so worker-side discovery (skills/agents/commands) scans the same roots. */
  readonly directories?: readonly string[]
  /** The hub's resolved ripgrep executable path. */
  readonly ripgrepPath?: string
  readonly config: ConfigV1.Info
  readonly auth: Readonly<Record<string, unknown>>
  readonly modelsDev: Readonly<Record<string, ModelsDev.Provider>>
  readonly flags: RuntimeFlags.Info
}

/** An immutable environment overlay delivered to a worker (never `process.env`). */
export interface WorkerEnv {
  readonly values: Readonly<Record<string, string>>
}

export class WorkerSnapshotService extends Context.Service<WorkerSnapshotService, WorkerSnapshot>()(
  "@opencode/session-worker/Snapshot",
) {}
export class WorkerEnvService extends Context.Service<WorkerEnvService, WorkerEnv>()("@opencode/session-worker/Env") {}

export const WorkerSnapshotNode = LayerNode.unbound(WorkerSnapshotService, workerTags.values.worker)
export const WorkerEnvNode = LayerNode.unbound(WorkerEnvService, workerTags.values.worker)


export interface PipelineMessage {
  readonly role: string
  readonly content: string
}

export interface PipelineInput {
  readonly sessionID: string
  readonly messages: readonly PipelineMessage[]
}

export interface ModelCompletion {
  readonly text: string
  readonly usage?: { readonly input: number; readonly output: number }
}

export interface ModelClientService {
  readonly complete: (input: PipelineInput) => Effect.Effect<ModelCompletion, Error>
}

export class ModelClient extends Context.Service<ModelClient, ModelClientService>()(
  "@opencode/session-worker/ModelClient",
) {}

export const ModelClientNode = LayerNode.unbound(ModelClient, workerTags.values.worker)

export interface PipelineResult {
  readonly text: string
  readonly usage?: { readonly input: number; readonly output: number }
  readonly publishedSeq: number | null
  readonly emitted: number
}

export interface SessionPipelineService {
  readonly run: (input: PipelineInput) => Effect.Effect<PipelineResult, Error>
}

export class SessionPipeline extends Context.Service<SessionPipeline, SessionPipelineService>()(
  "@opencode/session-worker/SessionPipeline",
) {}

const pipelineLayer = Layer.effect(
  SessionPipeline,
  Effect.gen(function* () {
    const model = yield* ModelClient
    const writer = yield* WriterClientService
    const egress = yield* EventEgressClientService
    const snapshot = yield* WorkerSnapshotService

    const run = (input: PipelineInput): Effect.Effect<PipelineResult, Error> =>
      Effect.gen(function* () {
        const completion = yield* model.complete(input)
        const published = yield* Effect.tryPromise({
          try: () =>
            writer.publish({
              id: `${input.sessionID}-assistant`,
              type: "message.assistant",
              aggregateID: input.sessionID,
              data: { text: completion.text },
            }),
          catch: (error) => (error instanceof Error ? error : new Error(String(error))),
        })
        // Only a durable commit is egressed; a non-committed (idempotent replay or non-durable) publish emits nothing.
        const emitted = published.committed ? 1 : 0
        if (published.committed) {
          const boundary: EventV2Event = {
            id: `${input.sessionID}-text`,
            type: "session.text",
            properties: { text: completion.text, epoch: snapshot.epoch },
          }
          // The committed `session.text` is a durable boundary (5.5), so prefer the transport's durable path when the handle offers one: it carries the commit's.
          if (published.seq === null) {
            return yield* Effect.fail(
              new Error(`committed publish for aggregate ${input.sessionID} returned no sequence`),
            )
          }
          if (egress.emitDurable !== undefined)
            egress.emitDurable(boundary, { aggregateID: input.sessionID, seq: published.seq })
          else egress.emit(boundary)
        }
        return {
          text: completion.text,
          ...(completion.usage === undefined ? {} : { usage: completion.usage }),
          publishedSeq: published.seq,
          emitted,
        }
      })

    return SessionPipeline.of({ run })
  }),
)

export const SessionPipelineNode = LayerNode.make({
  service: SessionPipeline,
  layer: pipelineLayer,
  deps: [ModelClientNode, WriterClientNode, EventEgressClientNode, WorkerSnapshotNode],
})


/** Raised when worker code reaches a boundary owned by the hub / writer. */
export class SessionLayerBoundaryError extends Schema.TaggedErrorClass<SessionLayerBoundaryError>(
  "SessionLayerBoundaryError",
)("WorkerLayer.Boundary", {
  boundary: Schema.String,
  member: Schema.String,
}) {
  constructor(
    override readonly boundary: string,
    override readonly member: string,
  ) {
    super({ boundary, member })
    this.name = "SessionLayerBoundaryError"
  }
  override get message() {
    return `session worker boundary "${this.boundary}" cannot use "${this.member}": ${this.boundary} is hub/writer-owned`
  }
}

const boundaryViolation = (boundary: string, member: string): Effect.Effect<never> =>
  Effect.die(new SessionLayerBoundaryError(boundary, member))

/** Build a hub/writer-owned boundary service from the table of reads a worker legitimately performs. */
function failLoudBoundary<I, S extends object>(
  boundary: string,
  service: Context.Key<I, S>,
  reads: Partial<S>,
): Layer.Layer<I> {
  const table = new Proxy(reads, {
    get(target, member) {
      if (typeof member === "symbol" || member in target) return Reflect.get(target, member)
      return () => boundaryViolation(boundary, member)
    },
  })
  return Layer.succeed(service, table as S)
}

/** Immutable snapshot-backed Config: workers never read files or write config. */
export function snapshotConfigLayer(snapshot: WorkerSnapshot): Layer.Layer<Config.Service> {
  return failLoudBoundary("Config", Config.Service, {
    get: () => Effect.succeed(snapshot.config),
    getGlobal: () => Effect.succeed(snapshot.config),
    getConsoleState: () => Effect.succeed(emptyConsoleState),
    directories: () => Effect.succeed([...(snapshot.directories ?? [snapshot.directory])]),
    waitForDependencies: () => Effect.void,
  })
}

/** Immutable snapshot-backed Auth: credentials never touch `process.env`. */
export function snapshotAuthLayer(snapshot: WorkerSnapshot): Layer.Layer<Auth.Service> {
  return failLoudBoundary("Auth", Auth.Service, {
    get: (providerID: string) => Effect.succeed(snapshot.auth[providerID] as Auth.Info | undefined),
    all: () => Effect.succeed(snapshot.auth as Record<string, Auth.Info>),
  })
}

/** Immutable snapshot-backed ModelsDev: catalog refresh is hub-only. */
export function snapshotModelsDevLayer(snapshot: WorkerSnapshot): Layer.Layer<ModelsDev.Service> {
  return failLoudBoundary("ModelsDev", ModelsDev.Service, {
    get: () => Effect.succeed(snapshot.modelsDev as Record<string, ModelsDev.Provider>),
  })
}

/** Immutable snapshot-backed RuntimeFlags. */
export function snapshotRuntimeFlagsLayer(snapshot: WorkerSnapshot): Layer.Layer<RuntimeFlags.Service> {
  return Layer.succeed(RuntimeFlags.Service, snapshot.flags)
}

/** Database is worker-owned. */
export function databaseBoundaryLayer(): Layer.Layer<Database.Service> {
  const database: Database.Interface = {
    db: new Proxy({} as Database.Interface["db"], {
      get: (_target, member) => {
        if (member === "$client") return undefined
        throw new SessionLayerBoundaryError("Database", String(member))
      },
    }),
  }
  return Layer.succeed(Database.Service, database)
}

/** No per-worker connection was supplied: every access to it fails loud. */
export function workerDbBoundaryLayer(): Layer.Layer<WorkerDb> {
  return Layer.succeed(WorkerDb, failLoudService<WorkerDatabase>("WorkerDb"))
}

// --------------------------------------------------------------------------- Functional boundary layers for the hub-owned authorities.

/** The MCP boundary: a worker owns no servers, so every catalog read is empty. */
export function mcpBoundaryLayer(): Layer.Layer<MCP.Service> {
  return failLoudBoundary("MCP", MCP.Service, {
    status: () => Effect.succeed({}),
    clients: () => Effect.succeed({}),
    instructions: () => Effect.succeed([]),
    tools: () => Effect.succeed({}),
    prompts: () => Effect.succeed({}),
    resources: () => Effect.succeed({}),
    resourceTemplates: () => Effect.succeed({}),
    getPrompt: () => Effect.succeed(undefined),
    readResource: () => Effect.succeed(undefined),
    hasStoredTokens: () => Effect.succeed(false),
    getAuthStatus: () => Effect.succeed("not_authenticated"),
    supportsOAuth: () => Effect.succeed(false),
  })
}

/** The LSP boundary: a worker owns no language servers, so every read is empty. */
export function lspBoundaryLayer(): Layer.Layer<LSP.Service> {
  return failLoudBoundary("LSP", LSP.Service, {
    init: () => Effect.void,
    status: () => Effect.succeed([]),
    hasClients: () => Effect.succeed(false),
    touchFile: () => Effect.void,
    diagnostics: () => Effect.succeed({}),
    hover: () => Effect.succeed(undefined),
    definition: () => Effect.succeed([]),
    references: () => Effect.succeed([]),
    implementation: () => Effect.succeed([]),
    documentSymbol: () => Effect.succeed([]),
    workspaceSymbol: () => Effect.succeed([]),
    prepareCallHierarchy: () => Effect.succeed([]),
    incomingCalls: () => Effect.succeed([]),
    outgoingCalls: () => Effect.succeed([]),
  })
}

/** The MCP auth boundary: no stored tokens; persistence is hub-owned. */
export function mcpAuthBoundaryLayer(): Layer.Layer<McpAuth.Service> {
  return failLoudBoundary("McpAuth", McpAuth.Service, {
    all: () => Effect.succeed({}),
    get: () => Effect.succeed(undefined),
    getForUrl: () => Effect.succeed(undefined),
    getOAuthState: () => Effect.succeed(undefined),
  })
}

/** The Npm boundary: a worker never installs packages; resolution is hub-only. */
export function npmBoundaryLayer(): Layer.Layer<Npm.Service> {
  return failLoudBoundary("Npm", Npm.Service, {
    which: () => Effect.succeed(undefined),
  })
}

/** The Account boundary: read-only; a worker has no account authority. */
export function accountBoundaryLayer(): Layer.Layer<Account.Service> {
  return failLoudBoundary("Account", Account.Service, {
    active: () => Effect.succeed(Option.none()),
    activeOrg: () => Effect.succeed(Option.none()),
    list: () => Effect.succeed([]),
    orgsByAccount: () => Effect.succeed([]),
    orgs: () => Effect.succeed([]),
    config: () => Effect.succeed(Option.none()),
    token: () => Effect.succeed(Option.none()),
  })
}

/** The AccountRepo boundary: read-only; persistence is hub-owned. */
export function accountRepoBoundaryLayer(): Layer.Layer<AccountRepo.Service> {
  return failLoudBoundary("AccountRepo", AccountRepo.Service, {
    active: () => Effect.succeed(Option.none()),
    list: () => Effect.succeed([]),
    getRow: () => Effect.succeed(Option.none()),
  })
}

/** The Storage boundary: a worker owns no durable store; only `list` is empty. */
export function storageBoundaryLayer(): Layer.Layer<Storage.Service> {
  return failLoudBoundary("Storage", Storage.Service, {
    list: () => Effect.succeed([]),
  })
}

/** The Git boundary: a worker owns no checkout; every operation is process-backed. */
export function gitBoundaryLayer(): Layer.Layer<Git.Service> {
  return failLoudBoundary("Git", Git.Service, {})
}

/** The ripgrep binary a worker runs. */
export function snapshotRipgrepBinaryLayer(snapshot: WorkerSnapshot): Layer.Layer<RipgrepBinary.Service> {
  return Layer.succeed(RipgrepBinary.Service, { filepath: Effect.succeed(snapshot.ripgrepPath ?? "rg") })
}

/** The MCP browser boundary: a worker never opens a browser. */
export function mcpBrowserBoundaryLayer(): Layer.Layer<McpBrowser.Service> {
  return failLoudBoundary("McpBrowser", McpBrowser.Service, {})
}

/** The `Permission` boundary for a worker with no hub control plane (degraded boot). */
export function permissionBoundaryLayer(): Layer.Layer<Permission.Service> {
  return failLoudBoundary("Permission", Permission.Service, {
    list: () => Effect.succeed([]),
  })
}

/** The worker `Database` over its own WAL connection and table allowlist. */
export function workerDatabaseLayer(
  database: WorkerDatabase,
): Layer.Layer<Database.Service | EventV2.WorkerScopeService | DurableLog.Service> {
  return databaseLayer(database)
}

/** The process-local `BackgroundJob` registry. */
export function localBackgroundJobLayer(): Layer.Layer<BackgroundJob.Service> {
  return Layer.effect(BackgroundJob.Service, make)
}

/** The snapshot-backed and hub-authority fragments BOTH the full worker graph and the reduced in-worker `LLM` sub-graph compose. */
export interface SharedBoundaryOptions {
  readonly hubProxies?: HubProxyClients
  readonly plugins?: WorkerPluginHost
  readonly pluginTools?: RemotePluginTools
}

export function sharedBoundaryReplacements(
  snapshot: WorkerSnapshot,
  options: SharedBoundaryOptions = {},
): LayerNode.Replacements {
  return [
    [Auth.node, snapshotAuthLayer(snapshot)],
    [Config.node, snapshotConfigLayer(snapshot)],
    [RuntimeFlags.node, snapshotRuntimeFlagsLayer(snapshot)],
    workerPluginReplacement(options.plugins, options.pluginTools),
    [
      Permission.node,
      options.hubProxies === undefined
        ? permissionBoundaryLayer()
        : permissionProxyLayer(options.hubProxies.permission),
    ],
  ]
}

const sameProjectionValue = (left: unknown, right: unknown): boolean =>
  JSON.stringify(left ?? null) === JSON.stringify(right ?? null)

/** Payload metadata key carrying `Session.patch`'s changed-column delta. */
const SESSION_PATCH_METADATA_KEY = "sessionPatch"

type MutableSessionInfo = { -readonly [K in keyof SessionV1.SessionInfo]: SessionV1.SessionInfo[K] }

/** The columns a `session.updated` publisher declared it actually changed. */
function emittedSessionColumns(event: SessionProjector.ProjectedEvent): ReadonlySet<string> | undefined {
  const value = event.metadata?.[SESSION_PATCH_METADATA_KEY]
  if (!Array.isArray(value)) return undefined
  return new Set(value.filter((column): column is string => typeof column === "string"))
}

/** Keep the projector's full-image write safe for a delta patch: every hub-owned column the patch did not emit is restored from the authoritative row. */
function restoreUnemittedSessionColumns(
  info: MutableSessionInfo,
  authoritative: SessionV1.SessionInfo,
  emitted: ReadonlySet<string>,
): void {
  const keep = (column: string) => !emitted.has(column)
  if (keep("id")) info.id = authoritative.id
  if (keep("project_id")) info.projectID = authoritative.projectID
  if (keep("workspace_id")) info.workspaceID = authoritative.workspaceID
  if (keep("parent_id")) info.parentID = authoritative.parentID
  if (keep("slug")) info.slug = authoritative.slug
  if (keep("directory")) info.directory = authoritative.directory
  if (keep("path")) info.path = authoritative.path
  if (keep("version")) info.version = authoritative.version
  if (keep("permission")) info.permission = authoritative.permission
  if (keep("metadata")) info.metadata = authoritative.metadata
  if (keep("cost")) info.cost = authoritative.cost
  if (
    ["tokens_input", "tokens_output", "tokens_reasoning", "tokens_cache_read", "tokens_cache_write"].every(keep)
  ) {
    info.tokens = authoritative.tokens
  }
  if (keep("share_url")) info.share = authoritative.share
  if (keep("revert")) info.revert = authoritative.revert
  if (keep("time_created")) info.time = { ...info.time, created: authoritative.time.created }
}

function rejectProjection(
  event: SessionProjector.ProjectedEvent,
  sessionID: string,
  reason: string,
): Effect.Effect<never> {
  return Effect.die(new SessionProjector.ProjectionViolation({ type: event.type, sessionID, reason }))
}

function parseSessionJsonColumn(value: unknown): unknown {
  if (typeof value !== "string") return value
  try {
    return JSON.parse(value)
  } catch {
    return undefined
  }
}

/** The authoritative session row read synchronously on the worker's connection. */
function readSessionProjectionRow(
  database: WorkerDatabase,
  sessionID: string,
): (typeof SessionTable.$inferSelect) | undefined {
  const raw = database.connection.all<Record<string, unknown>>("SELECT * FROM session WHERE id = ?", [sessionID])[0]
  if (raw === undefined) return undefined
  return {
    ...raw,
    summary_diffs: parseSessionJsonColumn(raw.summary_diffs),
    metadata: parseSessionJsonColumn(raw.metadata),
    revert: parseSessionJsonColumn(raw.revert),
    permission: parseSessionJsonColumn(raw.permission),
    model: parseSessionJsonColumn(raw.model),
  } as unknown as typeof SessionTable.$inferSelect
}

/** The projection authority of a bound worker. */
function workerProjectionPolicy(database?: WorkerDatabase): SessionProjector.ProjectionPolicy {
  return {
    authorize: (event, db) => {
      if (event.type === SessionV1.Event.Updated.type && database !== undefined) {
        const emitted = emittedSessionColumns(event)
        if (emitted !== undefined) {
          const row = readSessionProjectionRow(database, event.data.sessionID)
          if (row !== undefined) {
            restoreUnemittedSessionColumns(
              event.data.info as MutableSessionInfo,
              Session.fromRow(row),
              emitted,
            )
          }
        }
      }
      return Effect.gen(function* () {
        if (event.type === SessionV1.Event.Created.type || event.type === SessionV1.Event.Deleted.type) {
          return yield* rejectProjection(event, event.data.sessionID, "the session root is hub authority")
        }

        // Relocation is a control-plane operation: it rewrites the immutable directory/workspace identity and must not originate in a worker.
        if (event.type === SessionEvent.Moved.type) {
          return yield* rejectProjection(event, event.data.sessionID, "session location is hub authority")
        }

        // Revert state is hub authority in every shape, not only the `Session.Info` image: the dedicated revert branches write the same hub-owned `revert`.
        if (
          event.type === SessionEvent.RevertEvent.Staged.type ||
          event.type === SessionEvent.RevertEvent.Cleared.type ||
          event.type === SessionEvent.RevertEvent.Committed.type
        ) {
          return yield* rejectProjection(event, event.data.sessionID, "session revert is hub authority")
        }

        if (event.type === SessionV1.Event.Updated.type) {
          const info = event.data.info
          if (info.id !== event.data.sessionID) {
            return yield* rejectProjection(
              event,
              event.data.sessionID,
              "info.id does not match the durable aggregate",
            )
          }
          const current = yield* db
            .select()
            .from(SessionTable)
            .where(eq(SessionTable.id, event.data.sessionID))
            .get()
            .pipe(Effect.orDie)
          // A missing row makes the projector update a no-op; it can never mint the root, because Created is rejected above.
          if (current === undefined) return
          // A delta publisher names exactly the columns it changed.
          const emitted = emittedSessionColumns(event)
          const isEmitted = (column: string) => emitted === undefined || emitted.has(column)
          const next: Record<string, unknown> = {
            id: info.id,
            project_id: info.projectID,
            workspace_id: info.workspaceID ?? null,
            slug: info.slug,
            directory: info.directory,
          }
          if (info.parentID !== undefined) next.parent_id = info.parentID
          if (info.permission !== undefined) next.permission = info.permission
          const before: Record<string, unknown> = {
            id: current.id,
            project_id: current.project_id,
            workspace_id: current.workspace_id ?? null,
            parent_id: current.parent_id ?? null,
            slug: current.slug,
            directory: current.directory,
            permission: current.permission ?? null,
          }
          for (const column of SESSION_IMMUTABLE_COLUMNS) {
            if (!isEmitted(column)) continue
            if (!(column in next)) continue
            if (!sameProjectionValue(next[column], before[column])) {
              return yield* rejectProjection(
                event,
                event.data.sessionID,
                `immutable session column ${column} changed`,
              )
            }
          }
          if (isEmitted("metadata") && info.metadata !== undefined) {
            for (const key of HUB_OWNED_METADATA.session ?? []) {
              if (!sameProjectionValue(info.metadata[key], current.metadata?.[key])) {
                return yield* rejectProjection(event, event.data.sessionID, `hub-owned metadata ${key} changed`)
              }
            }
          }
          // The projector writes the whole `sessionRow(info)` image (undefined fields are omitted by `SET`).
          const currentRow = current as Record<string, unknown>
          const image = sessionRow(info) as Record<string, unknown>
          const emittedImage =
            emitted === undefined
              ? image
              : Object.fromEntries([...emitted].map((column) => [column, image[column]]))
          const changed = changedHubOwnedSessionColumns(emittedImage, (column, value) =>
            sameProjectionValue(value, currentRow[column]),
          )
          if (changed.length > 0) {
            return yield* rejectProjection(
              event,
              event.data.sessionID,
              `hub-owned session column ${changed[0]} changed`,
            )
          }
          return
        }

        if (event.type === SessionV1.Event.MessageUpdated.type) {
          const info = event.data.info
          if (info.sessionID !== event.data.sessionID) {
            return yield* rejectProjection(
              event,
              event.data.sessionID,
              "info.sessionID does not match the durable aggregate",
            )
          }
          const existing = yield* db
            .select({ sessionID: MessageTable.session_id })
            .from(MessageTable)
            .where(eq(MessageTable.id, info.id))
            .get()
            .pipe(Effect.orDie)
          if (existing !== undefined && existing.sessionID !== event.data.sessionID) {
            return yield* rejectProjection(event, event.data.sessionID, "message id belongs to another session")
          }
          return
        }

        if (event.type === SessionV1.Event.PartUpdated.type) {
          const part = event.data.part
          if (part.sessionID !== event.data.sessionID) {
            return yield* rejectProjection(
              event,
              event.data.sessionID,
              "part.sessionID does not match the durable aggregate",
            )
          }
          const message = yield* db
            .select({ sessionID: MessageTable.session_id })
            .from(MessageTable)
            .where(eq(MessageTable.id, part.messageID))
            .get()
            .pipe(Effect.orDie)
          if (message === undefined || message.sessionID !== event.data.sessionID) {
            return yield* rejectProjection(
              event,
              event.data.sessionID,
              "part.messageID does not belong to the durable aggregate",
            )
          }
          const existing = yield* db
            .select({ sessionID: PartTable.session_id, messageID: PartTable.message_id })
            .from(PartTable)
            .where(eq(PartTable.id, part.id))
            .get()
            .pipe(Effect.orDie)
          if (
            existing !== undefined &&
            (existing.sessionID !== event.data.sessionID || existing.messageID !== part.messageID)
          ) {
            return yield* rejectProjection(event, event.data.sessionID, "part id is bound to another message")
          }
        }
      })
    },
  }
}

/** Boundary replacements every session layer needs. */
export function boundaryReplacements(
  snapshot: WorkerSnapshot,
  database?: WorkerDatabase,
  backgroundJob?: Layer.Layer<BackgroundJob.Service>,
  boundaries: SharedBoundaryOptions = {},
): LayerNode.Replacements {
  return [
    ...sharedBoundaryReplacements(snapshot, boundaries),
    [ModelsDev.node, snapshotModelsDevLayer(snapshot)],
    [Database.node, database === undefined ? databaseBoundaryLayer() : workerDatabaseLayer(database)],
    // `SessionProjector` deliberately has NO boundary replacement: it runs IN the worker.
    [MCP.node, mcpBoundaryLayer()],
    [LSP.node, lspBoundaryLayer()],
    [McpAuth.node, mcpAuthBoundaryLayer()],
    [Npm.node, npmBoundaryLayer()],
    [Account.node, accountBoundaryLayer()],
    [AccountRepo.node, accountRepoBoundaryLayer()],
    [Storage.node, storageBoundaryLayer()],
    [Git.node, gitBoundaryLayer()],
    [BackgroundJob.node, backgroundJob ?? localBackgroundJobLayer()],
    // `Global` is a plain record of paths (no I/O); the real value is safe and lets any read of `Global.Path`/`Service` work rather than return undefined.
    [Global.node, Layer.succeed(Global.Service, Global.make())],
    // `AppProcess` and `Ripgrep` run IN the worker: the worker already has a real `CrossSpawnSpawner` (via `ToolRegistry`).
    [RipgrepBinary.node, snapshotRipgrepBinaryLayer(snapshot)],
    [McpBrowser.node, mcpBrowserBoundaryLayer()],
    // The hub composes the location service map; the worker never resolves a location for a V1 session.
    [LocationServiceMap.node, buildLocationServiceMap()],
  ]
}


/** The serializable provider/model bootstrap a worker receives (see `init.ts`). */
export interface WorkerModelModalities {
  readonly text: boolean
  readonly audio: boolean
  readonly image: boolean
  readonly video: boolean
  readonly pdf: boolean
}

/** The real model capabilities the hub serialized into the worker descriptor. */
export interface WorkerModelCapabilities {
  readonly temperature: boolean
  readonly reasoning: boolean
  readonly attachment?: boolean
  readonly toolcall?: boolean
  readonly interleaved?: boolean | { readonly field: string }
  readonly input?: WorkerModelModalities
  readonly output?: WorkerModelModalities
}

/** The real model token limits the hub serialized into the worker descriptor. */
export interface WorkerModelLimit {
  readonly context: number
  readonly output: number
}

/** The reduced provider bootstrap a worker consumes. */
export interface WorkerModelConfig {
  readonly providerID: string
  readonly modelID: string
  readonly apiId: string
  readonly npm?: string
  readonly baseUrl: string
  readonly apiKey: string
  readonly headers?: Readonly<Record<string, string>>
  readonly capabilities?: WorkerModelCapabilities
  readonly limit?: WorkerModelLimit
  readonly options?: Readonly<Record<string, unknown>>
  readonly variants?: Readonly<Record<string, Readonly<Record<string, unknown>>>>
  /** Provider-level options (`provider.options`), spread into the SDK + transforms. */
  readonly providerOptions?: Readonly<Record<string, unknown>>
}

const allModalities = { text: true, audio: false, image: false, video: false, pdf: false }

/** The provider npm, defaulting to the OpenAI-compatible client as before. */
const workerNpm = (config: WorkerModelConfig): string => config.npm ?? "@ai-sdk/openai-compatible"

function reducedProviderInfo(config: WorkerModelConfig, model: Provider.Model): Provider.Info {
  return {
    id: ProviderV2.ID.make(config.providerID),
    name: config.providerID,
    source: "config",
    env: [],
    key: config.apiKey,
    options: { ...(config.providerOptions ?? {}) },
    models: { [config.modelID]: model },
  }
}

/** Raised when a session asks the worker for a provider/model other than the one the hub serialized into its bootstrap. */
export class WorkerModelMismatchError extends Schema.TaggedErrorClass<WorkerModelMismatchError>()(
  "WorkerModelMismatchError",
  {
    requestedProviderID: Schema.String,
    requestedModelID: Schema.String,
    serializedProviderID: Schema.String,
    serializedModelID: Schema.String,
  },
) {
  override get message() {
    return `Model not found: session worker was bootstrapped for ${this.serializedProviderID}/${this.serializedModelID}, but the session requested ${this.requestedProviderID}/${this.requestedModelID}. A worker can only run its serialized model.`
  }

  static isInstance(input: unknown): input is WorkerModelMismatchError {
    return input instanceof WorkerModelMismatchError
  }
}

function reducedProviderModel(config: WorkerModelConfig): Provider.Model {
  const capabilities = config.capabilities
  return {
    id: ModelV2.ID.make(config.modelID),
    providerID: ProviderV2.ID.make(config.providerID),
    api: { id: config.apiId, url: config.baseUrl, npm: workerNpm(config) },
    name: config.modelID,
    capabilities: {
      // The descriptor's real capabilities; absent means "not advertised".
      temperature: capabilities?.temperature ?? false,
      reasoning: capabilities?.reasoning ?? false,
      attachment: capabilities?.attachment ?? false,
      toolcall: capabilities?.toolcall ?? true,
      interleaved: capabilities?.interleaved ?? false,
      input: capabilities?.input ?? allModalities,
      output: capabilities?.output ?? allModalities,
    },
    cost: { input: 0, output: 0, cache: { read: 0, write: 0 } },
    limit: { context: config.limit?.context ?? 128_000, output: config.limit?.output ?? 0 },
    status: "active",
    options: { ...(config.options ?? {}) },
    headers: { ...(config.headers ?? {}) },
    ...(config.variants === undefined ? {} : { variants: { ...config.variants } }),
    release_date: "",
  }
}

interface WorkerLanguageSdk {
  languageModel(modelID: string): LanguageModelV3
}

/** An abort controller that fires after `ms`, mirroring `provider.ts`'s timeout. */
function timeoutController(ms: number) {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(new Error(`request timed out after ${ms}ms`)), ms)
  return { signal: controller.signal, clear: () => clearTimeout(timer) }
}

/** Abort a streaming response when no SSE frame arrives within `ms`. */
function wrapSseIdle(response: Response, ms: number, controller: AbortController): Response {
  if (!(ms > 0) || response.body === null || !response.headers.get("content-type")?.includes("text/event-stream")) {
    return response
  }
  const reader = response.body.getReader()
  const body = new ReadableStream<Uint8Array>({
    async pull(stream) {
      const part = await new Promise<Awaited<ReturnType<typeof reader.read>>>((resolve, reject) => {
        const timer = setTimeout(() => {
          const error = new Error(`SSE read timed out after ${ms}ms`)
          controller.abort(error)
          void reader.cancel(error).catch(() => {})
          reject(error)
        }, ms)
        reader.read().then(
          (value) => {
            clearTimeout(timer)
            resolve(value)
          },
          (error) => {
            clearTimeout(timer)
            reject(error)
          },
        )
      })
      if (part.done) {
        stream.close()
        return
      }
      stream.enqueue(part.value)
    },
    cancel(reason) {
      controller.abort(reason)
      return reader.cancel(reason)
    },
  })
  return new Response(body, {
    headers: new Headers(response.headers),
    status: response.status,
    statusText: response.statusText,
  })
}

/** The worker transport fetch: applies the serialized `headerTimeout` / `timeout` / `chunkTimeout` provider options the same way the hub's `resolveSDK`. */
/** Bun's `fetch` accepts a `timeout` extension `RequestInit` does not declare. */
interface BunRequestInit extends RequestInit {
  /** `false` disables Bun's own request timeout. */
  readonly timeout?: number | false
}

function workerTimeoutFetch(options: Readonly<Record<string, unknown>>): typeof fetch {
  const chunkTimeout = (options["chunkTimeout"] ?? 300_000) as number
  const headerTimeout = (options["headerTimeout"] ?? 300_000) as number | false
  const timeout = options["timeout"] as number | false | undefined
  return (async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const opts: RequestInit = init ?? {}
    const headerTimeoutMs = headerTimeout === false ? undefined : headerTimeout
    const chunkController = typeof chunkTimeout === "number" && chunkTimeout > 0 ? new AbortController() : undefined
    const headerController = typeof headerTimeoutMs === "number" ? timeoutController(headerTimeoutMs) : undefined
    const signals: AbortSignal[] = []
    if (opts.signal) signals.push(opts.signal)
    if (chunkController) signals.push(chunkController.signal)
    if (headerController) signals.push(headerController.signal)
    if (timeout !== undefined && timeout !== null && timeout !== false) signals.push(AbortSignal.timeout(timeout))
    const signal = signals.length === 0 ? undefined : signals.length === 1 ? signals[0] : AbortSignal.any(signals)
    const request: BunRequestInit = {
      ...opts,
      ...(signal === undefined ? {} : { signal }),
      timeout: false,
    }
    const response = await fetch(input, request).finally(() => headerController?.clear())
    if (chunkController === undefined) return response
    return wrapSseIdle(response, chunkTimeout, chunkController)
  }) as typeof fetch
}

export class WorkerSdkUnsupportedError extends Schema.TaggedErrorClass<WorkerSdkUnsupportedError>()(
  "WorkerSdkUnsupportedError",
  { npm: Schema.String },
) {
  override get message() {
    return `session workers cannot build the ${this.npm} SDK`
  }
}

/** Build the reduced provider's SDK from the serialized `npm`, so a worker makes the provider-shaped request the hub resolved instead of always an. */
function createWorkerLanguageSdk(config: WorkerModelConfig): WorkerLanguageSdk {
  const npm = workerNpm(config)
  const providerOptions = config.providerOptions ?? {}
  const options: Record<string, any> = {
    ...providerOptions,
    name: config.providerID,
    baseURL: config.baseUrl,
    apiKey: config.apiKey,
    headers: { ...stringHeaders(providerOptions["headers"]), ...(config.headers ?? {}) },
    fetch: workerTimeoutFetch(providerOptions),
    ...(npm.includes("@ai-sdk/openai-compatible") && providerOptions["includeUsage"] !== false
      ? { includeUsage: true }
      : {}),
  }
  switch (npm) {
    case "@ai-sdk/anthropic":
      return createAnthropic(options) as unknown as WorkerLanguageSdk
    case "@ai-sdk/google":
      return createGoogleGenerativeAI(options) as unknown as WorkerLanguageSdk
    case "@ai-sdk/openai":
      return createOpenAI(options) as unknown as WorkerLanguageSdk
    case "@ai-sdk/openai-compatible":
      return createOpenAICompatible(
        options as Parameters<typeof createOpenAICompatible>[0],
      ) as unknown as WorkerLanguageSdk
    default:
      // Dispatch routes sessions with other packages in-process, so reaching here is a programming error.
      throw new WorkerSdkUnsupportedError({ npm })
  }
}

/** The reduced in-worker `Provider`: a client over the serialized `{providerID, modelID, apiId, npm, baseUrl. */
export function reducedModelProviderLayer(config: WorkerModelConfig): Layer.Layer<Provider.Service> {
  const sdk = createWorkerLanguageSdk(config)
  const model = reducedProviderModel(config)
  const info = reducedProviderInfo(config, model)
  // The reduced provider serves exactly one model; any other request is a programming error surfaced loudly rather than silently mis-served.
  const mismatch = (requestedProvider: ProviderV2.ID, requestedModel: ModelV2.ID) =>
    new WorkerModelMismatchError({
      requestedProviderID: requestedProvider,
      requestedModelID: requestedModel,
      serializedProviderID: config.providerID,
      serializedModelID: config.modelID,
    })
  const service = {
    list: () => Effect.succeed({ [info.id]: info }),
    getProvider: () => Effect.succeed(info),
    getModel: (requestedProvider: ProviderV2.ID, requestedModel: ModelV2.ID) =>
      requestedProvider === config.providerID && requestedModel === config.modelID
        ? Effect.succeed(model)
        : Effect.fail(mismatch(requestedProvider, requestedModel)),
    getLanguage: (requested: Provider.Model) =>
      requested.providerID === config.providerID && requested.api.id === config.apiId
        ? Effect.succeed(sdk.languageModel(config.apiId))
        : Effect.fail(mismatch(requested.providerID, requested.id)),
    closest: () => Effect.succeed(undefined),
    getSmallModel: () => Effect.succeed(undefined),
    defaultModel: () => Effect.succeed({ providerID: info.id, modelID: model.id }),
  }
  return Layer.succeed(Provider.Service, service as unknown as Provider.Interface)
}

/** The boundary `Plugin.trigger`, erased to its runtime shape so the no-op and host-backed layers share one signature. */
type PluginTrigger = Plugin.Interface["trigger"]

/** The hooks the reduced worker graph can actually dispatch in this isolate. */
const WORKER_DISPATCHED_HOOKS: ReadonlySet<string> = new Set([
  "chat.params",
  "chat.headers",
  "chat.message",
  "tool.execute.before",
  "tool.execute.after",
  "shell.env",
  "tool.definition",
  "experimental.chat.system.transform",
  "experimental.chat.messages.transform",
  "command.execute.before",
  "experimental.session.compacting",
  "experimental.compaction.autocontinue",
  "experimental.text.complete",
])

/** No plugin host means no configured hook exists, so the trigger is a true no-op. */
const noopPluginTrigger = ((_name: string, _input: unknown, output: unknown) => Effect.succeed(output)) as PluginTrigger

/** Dispatch one `Plugin.trigger` to the matching worker-hosted hook. */
function runWorkerPluginHook(host: WorkerPluginHost, name: string, input: unknown, output: unknown): Promise<unknown> {
  const route = workerHookRoute(name)
  const plugin = workerPluginHostDeclarers(host, name).join(", ") || "worker"
  if (route === undefined) throw new UnclassifiedPluginHookError({ plugin, hook: name })
  if (route === "unsupported") {
    if (!workerPluginHostDeclaresHook(host, name)) return Promise.resolve(output)
    throw new UnsupportedPluginHookError({ plugin, hook: name })
  }
  if (!WORKER_DISPATCHED_HOOKS.has(name)) {
    throw new UnsupportedPluginHookError({ plugin, hook: name })
  }
  switch (name) {
    case "chat.params":
      return host.chatParams(input, output as ChatParamsOutput)
    case "chat.headers":
      return host.chatHeaders(input, output as ChatHeadersOutput)
    case "chat.message":
      return host.chatMessage(input, output as ChatMessageOutput)
    case "tool.execute.before":
      return host.toolBefore(input, output as ToolBeforeOutput)
    case "tool.execute.after":
      return host.toolAfter(input, output as ToolAfterOutput)
    case "shell.env":
      return host.shellEnv(input, output as ShellEnvOutput)
    case "tool.definition":
      return host.toolDefinition(input, output as ToolDefinitionOutput)
    case "experimental.chat.system.transform":
      return host.systemTransform(input as SystemTransformInput, output as SystemTransformOutput)
    case "experimental.chat.messages.transform":
      return host.messagesTransform(input as MessagesTransformInput, output as MessagesTransformOutput)
    case "command.execute.before":
      return host.commandBefore(input, output as CommandBeforeOutput)
    case "experimental.session.compacting":
      return host.sessionCompacting(input, output as SessionCompactingOutput)
    case "experimental.compaction.autocontinue":
      return host.compactionAutocontinue(input, output as CompactionAutocontinueOutput)
    case "experimental.text.complete":
      return host.textComplete(input, output as TextCompleteOutput)
    default:
      throw new UnsupportedPluginHookError({ plugin, hook: name })
  }
}

/** Hub-executed plugin tools as the worker sees them: the clone-safe descriptors plus the executor that proxies a call back over the hub control port. */
export interface RemotePluginTools {
  readonly descriptors: readonly PluginToolDescriptor[]
  readonly executor: PluginToolExecutor
  /** The hub abort bridge, forwarded onto every rebuilt remote definition so the registry's `toolCtx.abort` subscription reaches the hub's in-flight call. */
  readonly abort?: PluginToolAbort
  /** Re-fetch the hub's descriptors and swap them into this handle — the lazy self-heal action attached to each remote definition as its `refresh` seam. */
  readonly refreshFromHub?: () => Promise<void>
}

/** Raised when a remote plugin-tool id is ambiguous: it is declared twice, or it collides with a local/builtin tool the registry already owns. */
export class PluginToolIDCollisionError extends Schema.TaggedErrorClass<PluginToolIDCollisionError>(
  "PluginToolIDCollisionError",
)("PluginTool.IDCollision", {
  id: Schema.String,
  kind: Schema.Literals(["duplicate", "reserved"]),
}) {
  constructor(
    override readonly id: string,
    override readonly kind: "duplicate" | "reserved",
  ) {
    super({ id, kind })
    this.name = "PluginToolIDCollisionError"
  }
  override get message() {
    return this.kind === "duplicate"
      ? `plugin tool id "${this.id}" is declared by more than one remote descriptor`
      : `plugin tool id "${this.id}" collides with a locally registered tool`
  }
}

/** The collision policy for remote plugin-tool descriptors, applied before registration. */
export function assertPluginToolDescriptorsUnique(
  descriptors: readonly PluginToolDescriptor[],
  reservedIDs: Iterable<string> = [],
): readonly PluginToolDescriptor[] {
  const reserved = new Set(reservedIDs)
  const seen = new Set<string>()
  for (const descriptor of descriptors) {
    if (seen.has(descriptor.id)) throw new PluginToolIDCollisionError(descriptor.id, "duplicate")
    if (reserved.has(descriptor.id)) throw new PluginToolIDCollisionError(descriptor.id, "reserved")
    seen.add(descriptor.id)
  }
  return descriptors
}

export interface RefreshablePluginTools extends RemotePluginTools {
  /** Replace the descriptor set; the next `list()` reflects it. Fails loud on a collision. */
  refresh(descriptors: readonly PluginToolDescriptor[]): void
}

/** Build the live {@link RefreshablePluginTools} handle. */
export function refreshablePluginTools(
  initialDescriptors: readonly PluginToolDescriptor[],
  executor: PluginToolExecutor,
  options: {
    readonly abort?: PluginToolAbort
    readonly list?: () => Promise<readonly PluginToolDescriptor[]>
  } = {},
): RefreshablePluginTools {
  let current = assertPluginToolDescriptorsUnique(initialDescriptors)
  const refresh = (descriptors: readonly PluginToolDescriptor[]): void => {
    current = assertPluginToolDescriptorsUnique(descriptors)
  }
  const { abort, list } = options
  return {
    get descriptors() {
      return current
    },
    executor,
    refresh,
    ...(abort === undefined ? {} : { abort }),
    ...(list === undefined ? {} : { refreshFromHub: async () => refresh(await list()) }),
  }
}

function workerPluginLayer(trigger: PluginTrigger, pluginTools?: RemotePluginTools): Layer.Layer<Plugin.Service> {
  const plugin = {
    trigger,
    list: () => Effect.succeed(remotePluginHooks(pluginTools)),
    init: () => Effect.void,
  } as unknown as Plugin.Interface
  return Layer.succeed(Plugin.Service, plugin)
}

/** The synthetic `Hooks` a worker lists for its hub-executed plugin tools. */
function remotePluginHooks(pluginTools?: RemotePluginTools): Hooks[] {
  if (pluginTools === undefined) return []
  const descriptors = assertPluginToolDescriptorsUnique(pluginTools.descriptors)
  if (descriptors.length === 0) return []
  const callbacks = {
    ...(pluginTools.abort === undefined ? {} : { abort: pluginTools.abort }),
    ...(pluginTools.refreshFromHub === undefined ? {} : { refresh: pluginTools.refreshFromHub }),
  }
  return [remotePluginToolHooks(descriptors, pluginTools.executor, callbacks) as unknown as Hooks]
}

/** The worker-safe `Plugin.node` replacement: the worker-hosted hooks when a {@link WorkerPluginHost} is supplied, otherwise a no-op pass-through. */
function workerPluginReplacement(plugins?: WorkerPluginHost, pluginTools?: RemotePluginTools): LayerNode.Replacement {
  const trigger: PluginTrigger =
    plugins === undefined
      ? noopPluginTrigger
      : (((name: string, input: unknown, output: unknown) =>
          Effect.tryPromise({
            try: () => runWorkerPluginHook(plugins, name, input, output),
            catch: (cause) => cause,
          }).pipe(Effect.orDie)) as PluginTrigger)
  return [Plugin.node, workerPluginLayer(trigger, pluginTools)]
}

/** Build the minimal `LLM.StreamInput` for a worker pipeline call. */
function streamInputFor(model: Provider.Model, input: PipelineInput): LLM.StreamInput {
  return {
    user: { id: "worker", role: "user", model: { variant: undefined } } as unknown as LLM.StreamInput["user"],
    sessionID: input.sessionID,
    model,
    agent: { name: "worker", mode: "primary" } as unknown as LLM.StreamInput["agent"],
    system: [],
    messages: input.messages.map((message) => ({ role: message.role, content: message.content })),
    tools: {},
  } as unknown as LLM.StreamInput
}

export function llmModelClientLayer(config: WorkerModelConfig): Layer.Layer<ModelClient, never, LLM.Service> {
  const model = reducedProviderModel(config)
  return Layer.effect(
    ModelClient,
    Effect.gen(function* () {
      const llm = yield* LLM.Service
      const complete = (input: PipelineInput): Effect.Effect<ModelCompletion, Error> =>
        Stream.runCollect(llm.stream(streamInputFor(model, input))).pipe(
          Effect.mapError((error) => (error instanceof Error ? error : new Error(String(error)))),
          Effect.map((events) => ({
            text: [...events]
              .filter(LLMEvent.is.textDelta)
              .map((event) => event.text)
              .join(""),
          })),
        )
      return ModelClient.of({ complete })
    }),
  )
}

/** A fail-loud service for a worker boundary the *standalone* reduced LLM sub-graph must never call. */
function failLoudService<T extends object>(boundary: string): T {
  return new Proxy(
    {},
    {
      get(_target, member) {
        if (typeof member === "symbol") return undefined
        return () => boundaryViolation(boundary, member)
      },
    },
  ) as T
}

/** The `EventV2Bridge` boundary for the standalone reduced LLM sub-graph: the isolate publishes through its own event port in the full graph. */
function eventV2BridgeBoundaryLayer(): Layer.Layer<EventV2Bridge.Service> {
  return Layer.succeed(EventV2Bridge.Service, failLoudService<EventV2.Interface>("EventV2Bridge"))
}

/** The `LLMClient` boundary for the standalone reduced LLM sub-graph: the full worker graph builds the real platform client. */
function llmClientBoundaryLayer(): Layer.Layer<LLMClientService> {
  return Layer.succeed(LLMClient.Service, failLoudService<LLMClientShape>("LLMClient"))
}

/** The boundary replacements for the standalone reduced LLM sub-graph. */
export function llmBoundaryReplacements(
  snapshot: WorkerSnapshot,
  config: WorkerModelConfig,
  hubProxies?: HubProxyClients,
  plugins?: WorkerPluginHost,
  pluginTools?: RemotePluginTools,
): LayerNode.Replacements {
  return [
    ...sharedBoundaryReplacements(snapshot, { hubProxies, plugins, pluginTools }),
    [Provider.node, reducedModelProviderLayer(config)],
    [EventV2Bridge.node, eventV2BridgeBoundaryLayer()],
    [llmClient, llmClientBoundaryLayer()],
  ]
}

/** The worker-local `LLM` service: the real `LLM` node composed over the reduced provider. */
export function reducedLlmLayer(
  snapshot: WorkerSnapshot,
  config: WorkerModelConfig,
  hubProxies?: HubProxyClients,
  plugins?: WorkerPluginHost,
  pluginTools?: RemotePluginTools,
): Layer.Layer<LLM.Service> {
  return AppNodeBuilderV1.build(LLM.node, llmBoundaryReplacements(snapshot, config, hubProxies, plugins, pluginTools))
}


/** The ONE ordered registration list for the real session nodes. */
const SESSION_NODE_LIST = [
  Session.node,
  SessionPrompt.node,
  SessionProcessor.node,
  SessionRunState.node,
  SessionProjector.node,
  SessionStatus.node,
  SessionCompaction.node,
  SessionRevert.node,
  SessionSummary.node,
  Todo.node,
  LLM.node,
  Provider.node,
  Agent.node,
  Skill.node,
  Discovery.node,
  ToolRegistry.node,
  Plugin.node,
  Config.node,
  Auth.node,
  RuntimeFlags.node,
  Question.node,
  Permission.node,
  Truncate.node,
  Format.node,
  Instruction.node,
  SystemPrompt.node,
  Command.node,
  Image.node,
  EventV2Bridge.node,
  EventV2.node,
  Snapshot.node,
  llmClient,
] as const

/** The key→node registry, derived from the single registration list above. */
export const SESSION_NODE_REGISTRY: ReadonlyMap<string, LayerNode.AnyNode> = new Map(
  SESSION_NODE_LIST.map((node) => [node.name, node]),
)

/** Required-key parity, checked when this module initialises. */
function assertRequiredSessionNodeParity(): void {
  const registered = [...SESSION_NODE_REGISTRY.keys()]
  const missing = REQUIRED_SESSION_NODE_KEYS.filter((key) => !SESSION_NODE_REGISTRY.has(key))
  const extra = registered.filter((key) => !REQUIRED_SESSION_NODE_KEYS.includes(key))
  const duplicate =
    registered.length === SESSION_NODE_LIST.length
      ? []
      : SESSION_NODE_LIST.map((node) => node.name).filter((name, index, all) => all.indexOf(name) !== index)
  if (missing.length === 0 && extra.length === 0 && duplicate.length === 0) return
  throw new Error(
    "session-node registration drifted from REQUIRED_SESSION_NODE_KEYS" +
      (missing.length === 0 ? "" : `: missing [${missing.join(", ")}]`) +
      (extra.length === 0 ? "" : `: unrequired [${extra.join(", ")}]`) +
      (duplicate.length === 0 ? "" : `: duplicate [${[...new Set(duplicate)].join(", ")}]`),
  )
}

assertRequiredSessionNodeParity()

/** Resolve a required session-node key; a missing registration fails loud. */
function requiredSessionNode(key: string): LayerNode.AnyNode {
  const node = SESSION_NODE_REGISTRY.get(key)
  if (node === undefined) throw new Error(`no session node is registered for required key "${key}"`)
  return node
}

/** The port-backed client nodes every session graph contains. */
const SESSION_CLIENT_NODES = [WriterClientNode, EventEgressClientNode] as const

/** Worker-local infrastructure shared by the runnable and pipeline graphs. */
const WORKER_INFRA_NODES = [WorkerEventBridgeNode, WorkerSnapshotNode, WorkerEnvNode, WorkerDbNode] as const

/** The real nodes every session graph must provide, in declaration order. */
const REQUIRED_SESSION_NODES = REQUIRED_SESSION_NODE_KEYS.map(requiredSessionNode)

/** The declared session-role allowlist group (real nodes + client/egress infra). */
export const SESSION_NODES = LayerNode.group([
  ...REQUIRED_SESSION_NODES,
  WorkerEventBridgeNode,
  ...SESSION_CLIENT_NODES,
])

/** The declared *runnable* graph: every required session-node service plus the worker-infrastructure nodes (pipeline seam, clients, snapshot, env). */
export const RUNNABLE_SESSION_NODES = LayerNode.group([
  ...REQUIRED_SESSION_NODES,
  SessionPipelineNode,
  ...WORKER_INFRA_NODES,
  ...SESSION_CLIENT_NODES,
])

/** The reduced pipeline subset: the one-shot `SessionPipeline` seam, runtime flags and the worker infrastructure. */
export const PIPELINE_SESSION_NODES = LayerNode.group([
  SessionPipelineNode,
  RuntimeFlags.node,
  ...SESSION_CLIENT_NODES,
  WorkerSnapshotNode,
  WorkerEnvNode,
])

export interface SessionLayerOptions {
  /** Node group; defaults to the full runnable session graph (`RUNNABLE_SESSION_NODES`). */
  readonly nodes?: LayerNode.AnyNode
  readonly snapshot: WorkerSnapshot
  readonly env: WorkerEnv
  readonly clients: {
    readonly writer: WriterClientHandle
    readonly egress: EventEgressClientHandle
    readonly backgroundJob: BackgroundJobClientHandle
    /** The worker's own WAL connection; replaces the legacy writer-proxied DB. */
    readonly database?: WorkerDatabase
  }
  /** Model seam; required because the layer never talks to a real provider by default. */
  readonly modelClient: Layer.Layer<ModelClient>
  /** Hub-resident authority proxies. */
  readonly hubProxies?: HubProxyClients
  /** The one session this isolate is bound to. */
  readonly boundSessionID?: SessionID
  /** Functional `BackgroundJob` replacement; the default is the process-local registry (per-session workers own their background jobs). */
  readonly backgroundJob?: Layer.Layer<BackgroundJob.Service>
  /** Worker→hub PID reporter. */
  readonly pidReporter?: PidReporterInterface
  /** The worker-hosted plugin hooks. */
  readonly plugins?: WorkerPluginHost
  /** Hub-executed plugin tools: clone-safe descriptors plus the hub-proxy executor. */
  readonly pluginTools?: RemotePluginTools
  /** Non-graph worker logging layer (see `worker-logging.ts`). */
  readonly logging?: Layer.Layer<never>
  /** Extra replacements, e.g. the real LLM-backed `ModelClient` in production. */
  readonly replacements?: LayerNode.Replacements
}

export interface SessionWorkerLayer {
  readonly root: LayerNode.AnyNode
  readonly replacements: LayerNode.Replacements
  readonly layer: Layer.Layer<unknown, unknown>
}

/** Compose the reduced session layer, assert it against the node table, and return the compiled layer. */
export function buildSessionWorkerLayer(options: SessionLayerOptions): SessionWorkerLayer {
  const base = options.nodes ?? RUNNABLE_SESSION_NODES
  // Hub-proxy authorities are REPLACEMENTS over nodes the base graph already contains (SessionStatus/Permission/Question).
  const replacements: LayerNode.Replacements = [
    [WriterClientNode, writerClient.layer(options.clients.writer)],
    [EventEgressClientNode, eventEgressClient.layer(options.clients.egress)],
    [WorkerSnapshotNode, Layer.succeed(WorkerSnapshotService, options.snapshot)],
    [WorkerEnvNode, Layer.succeed(WorkerEnvService, options.env)],
    [ModelClientNode, options.modelClient],
    [
      WorkerDbNode,
      options.clients.database === undefined
        ? workerDbBoundaryLayer()
        : Layer.succeed(WorkerDb, options.clients.database),
    ],
    ...boundaryReplacements(options.snapshot, options.clients.database, options.backgroundJob, {
      ...(options.hubProxies === undefined ? {} : { hubProxies: options.hubProxies }),
      ...(options.plugins === undefined ? {} : { plugins: options.plugins }),
      ...(options.pluginTools === undefined ? {} : { pluginTools: options.pluginTools }),
    }),
    ...(options.hubProxies === undefined ? [] : hubAuthorityReplacements(options.hubProxies)),
    ...(options.replacements ?? []),
  ]

  assertSessionLayer(base, replacements)
  const built = AppNodeBuilderV1.build(base, replacements)
  const withPid =
    options.pidReporter === undefined ? built : Layer.merge(built, Layer.succeed(PidReporter, options.pidReporter))
  const layer =
    options.hubProxies === undefined
      ? withPid
      : Layer.merge(withPid, sessionSpawnProxyLayer(sessionSpawnClient(options.hubProxies), options.boundSessionID))
  // The clear-revert authority is a non-graph seam: `SessionRevert` reads it optionally.
  const withClearRevert =
    options.hubProxies === undefined
      ? layer
      : Layer.merge(layer, clearRevertProxyLayer(options.hubProxies.sessionStatus))
  // `logging` is not a graph node either: it contributes only context references (the file sink + minimum level).
  const withLogging = options.logging === undefined ? withClearRevert : Layer.merge(withClearRevert, options.logging)
  // The strict projection authority is a non-graph seam too.
  const withProjectionPolicy =
    options.boundSessionID === undefined && options.clients.database?.sessionID === undefined
      ? withLogging
      : withLogging.pipe(Layer.provide(Layer.succeed(SessionProjector.Policy, workerProjectionPolicy(options.clients.database))))
  const withLedger = ToolCallLedger.workerLayer.pipe(Layer.provideMerge(withProjectionPolicy))
  return { root: base, replacements, layer: withLedger }
}

export * as SessionWorkerLayer from "./layer"
