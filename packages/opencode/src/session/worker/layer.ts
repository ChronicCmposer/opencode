// Phase 6.1 / 6.2 / 6.5 — reduced session-worker AppLayer.
//
// The hub composes ~47 nodes (`src/effect/app-runtime.ts`). A session worker
// must compose a *reduced* graph: the session pipeline and its model-facing
// services live in the worker, while database connections, OS children and
// process-global state stay on the hub/writer/broker (see `node-table.ts`).
//
// This module builds that reduced layer from:
//   * the allowlisted real nodes (default group `SESSION_NODES`);
//   * four client nodes (`WriterClient` / `EventEgressClient` / `BrokerClient` /
//     `BackgroundJobClient`) that proxy to the writer, egress and hub authorities;
//   * an immutable per-worker `WorkerSnapshot` + `WorkerEnv` (plan decision 67/68);
//   * boundary replacement layers for the excluded nodes (Database, MCP/LSP, Npm,
//     ModelsDev, …), which production supplies from Phases 7/10. `SessionProjector`
//     is NOT replaced: it runs in-worker so durable events materialize the
//     session/message/part rows the loop reads back.
//
// `buildSessionWorkerLayer` composes the graph and then asserts it against the
// node table, so a regression that reintroduces an excluded node such as
// `Database` into the session graph fails at build time, not at run time.

import { Context, Effect, Layer, Option, Schema, Stream } from "effect"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
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
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { Database } from "@opencode-ai/core/database/database"
import { MCP } from "@/mcp"
import { LSP } from "@/lsp/lsp"
import { McpAuth } from "@/mcp/auth"
import { McpBrowser } from "@/mcp/browser"
import { Npm } from "@opencode-ai/core/npm"
import { Account } from "@/account/account"
import { AccountRepo } from "@/account/repo"
// Residual hub/writer/broker-owned boundaries (node-table `SESSION_EXCLUDED`).
import { Storage } from "@/storage/storage"
import { Git } from "@/git"
import { BackgroundJob } from "@/background/job"
import { Global } from "@opencode-ai/core/global"
import { RipgrepBinary } from "@opencode-ai/core/ripgrep/binary"
import { LocationServiceMap } from "@opencode-ai/core/location-service-map"
import { BackgroundJob as CoreBackgroundJob } from "@opencode-ai/core/background-job"
import { buildLocationServiceMap } from "@opencode-ai/core/location-services"

import {
  BACKGROUND_JOB_CLIENT_KEY,
  BROKER_CLIENT_KEY,
  EVENT_BRIDGE_KEY,
  EVENT_EGRESS_CLIENT_KEY,
  REQUIRED_SESSION_NODE_KEYS,
  WRITER_CLIENT_KEY,
  assertSessionLayer,
} from "./node-table"
import { createWorkerEventBridge, DURABLE_WAKEUP_METADATA_KEY } from "./event-bridge"
import type { WorkerDurableSubscription } from "./egress-client"
import type { DurableCommit, MeshWorkerClient } from "@/session/egress/mesh"
import type { EventV2Event } from "./frame"
import { stringHeaders } from "./headers"
import { HubProxyRemoteError, type HubProxyClient, type HubProxyMethod } from "./hub-proxy-wire"
import type { JobClient } from "./job-client"
import type {
  ChatHeadersOutput,
  ChatParamsOutput,
  MessagesTransformInput,
  MessagesTransformOutput,
  SystemTransformInput,
  SystemTransformOutput,
  ToolAfterOutput,
  ToolBeforeOutput,
  WorkerPluginHost,
} from "./plugin-host"
import { writerSqlClientLayer, type WriterSqlHandle } from "./writer-sql-client"
import { PidReporter, type PidReporterInterface } from "./pid-registry"
import type { JobInfo } from "./job-registry"
import { SubagentDispatchProxyService } from "./subagent"

export { PidReporter } from "./pid-registry"
export type { PidReport, PidReportEntry, PidReporterInterface } from "./pid-registry"

// ---------------------------------------------------------------------------
// Client nodes — the worker-side handles to the writer / egress / hub.
// ---------------------------------------------------------------------------

/**
 * The transaction primitives the worker writer exposes. They mirror
 * `WorkerWriterClient` in `writer-client.ts`: a pinned transaction is identified
 * by an opaque handle, statements run against it, then it commits or rolls back.
 *
 * A production writer (`createWorkerWriterClient`) always provides all four. They
 * are optional here so reduced test/degraded handles remain structurally
 * assignable; {@link writerDatabaseLayer} turns a missing primitive into a loud
 * failure rather than a silent `undefined`.
 */
export interface WriterTransactionHandle {
  /** Pin a transaction and return its opaque handle (e.g. `"tx-1"`). */
  begin(): Promise<string>
  /** Run a statement against the pinned transaction; rejects on a typed failure. */
  txQuery(handle: string, method: string, sql: string, params?: readonly unknown[]): Promise<unknown>
  /** Commit the pinned transaction; the handle is spent. */
  commit(handle: string): Promise<void>
  /** Roll back the pinned transaction, discarding its writes. */
  rollback(handle: string): Promise<void>
}

export interface WriterClientHandle extends Partial<WriterTransactionHandle> {
  readonly query: (method: string, sql: string, params?: readonly unknown[]) => Promise<unknown>
  readonly publish: (event: { id: string; type: string; aggregateID: string; data: unknown }) => Promise<{
    readonly seq: number | null
    readonly committed: boolean
  }>
}

export interface EventEgressClientHandle {
  /** Forward one EventV2 event VERBATIM; the egress transport frames it. */
  readonly emit: (event: EventV2Event) => void
  /** Forward a durable boundary event; defaults to the transport's ordinal. */
  readonly emitDurable?: (event: EventV2Event) => void
  /**
   * The mesh control-plane client, present exactly when the worker was given a
   * control port. It is the single source of truth for "can this worker receive
   * cross-isolate durable wakeups": a handle with no mesh (`mesh === undefined`)
   * has no transport to carry them, so {@link wireDurableWakeups} leaves the
   * wakeup bridge unwired instead of reaching {@link subscribeDurable}. A
   * DIRECT `subscribeDurable` call on a control-plane-less client still fails
   * loud — that is a genuine caller misconfiguration, not this legitimate
   * hub-less topology.
   */
  readonly mesh?: MeshWorkerClient | undefined
  /**
   * Register a durable wakeup subscription with the hub mesh (spec 5.9). The
   * production egress client owns the control port and provides this; a reduced
   * handle with no mesh (degraded boot / test stub) omits it, so the wakeup
   * bridge is simply not wired.
   */
  readonly subscribeDurable?: (subscription: WorkerDurableSubscription) => () => void
}

export interface BrokerClientHandle {
  readonly call: (directory: string, server: string, method: string, params?: unknown) => Promise<unknown>
}

export interface BackgroundJobClientHandle {
  readonly wait: (id: string, timeoutMs?: number) => Promise<unknown>
  readonly cancel: (id: string) => Promise<void>
}

const workerTags = LayerNode.tags({ worker: [] })

/**
 * A client node is a service class, its unbound graph node, and the replacement
 * layer that installs a handle. The service key comes from `node-table.ts`, so
 * the table and the layer can never drift apart.
 */
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
const brokerClient = defineClientNode<BrokerClientHandle>(BROKER_CLIENT_KEY)
const backgroundJobClient = defineClientNode<BackgroundJobClientHandle>(BACKGROUND_JOB_CLIENT_KEY)

export const WriterClientService = writerClient.Service
export const EventEgressClientService = eventEgressClient.Service
export const BrokerClientService = brokerClient.Service
export const BackgroundJobClientService = backgroundJobClient.Service

export const WriterClientNode = writerClient.node
export const EventEgressClientNode = eventEgressClient.node
export const BrokerClientNode = brokerClient.node
export const BackgroundJobClientNode = backgroundJobClient.node

// ---------------------------------------------------------------------------
// Worker EventV2 bridge (spec Q6/Q13/Q15).
// ---------------------------------------------------------------------------

/** The live worker-event subscription; the service key is the node-table allowlist key. */
export interface WorkerEventBridgeService {
  readonly dispose: Effect.Effect<void>
}

export class WorkerEventBridge extends Context.Service<WorkerEventBridge, WorkerEventBridgeService>()(
  EVENT_BRIDGE_KEY,
) {}

// ---------------------------------------------------------------------------
// Worker durable wakeups (spec 5.9): hub mesh commit → in-isolate EventV2.
// ---------------------------------------------------------------------------

/**
 * The single durable wakeup pattern a worker subscribes to. The hub routes every
 * matching durable commit over the mesh control plane; the worker's own EventV2
 * performs the per-aggregate filtering, so the worker asks for the whole durable
 * stream (`"*"` is the hub matcher's match-everything pattern).
 */
export const WORKER_DURABLE_WAKE_PATTERN = "*"

/**
 * The aggregate a durable commit belongs to, or `undefined` when the commit's
 * type names no durable definition this build knows. The mesh frame carries the
 * event verbatim (`{ id, type, properties }`) with no aggregate of its own, so it
 * is parsed from the payload exactly as core's durable commit does: the
 * definition's `durable.aggregate` field names the payload property. An unknown
 * type is skipped rather than fatal — the commit is still republished live-only —
 * so a synthetic or version-skewed durable event never breaks the control port.
 * A KNOWN durable type whose payload omits the aggregate field is malformed and
 * fails loud.
 */
function durableAggregateOf(commit: DurableCommit): string | undefined {
  const aggregate = Latest.get(commit.event.type)?.durable?.aggregate
  if (aggregate === undefined) return undefined
  const properties = commit.event.properties
  const aggregateID =
    properties !== null && typeof properties === "object"
      ? (properties as Readonly<Record<string, unknown>>)[aggregate]
      : undefined
  if (typeof aggregateID !== "string") {
    throw new Error(`durable commit "${commit.event.type}" is missing string aggregate field ${aggregate}`)
  }
  return aggregateID
}

/**
 * Bridge one hub-delivered durable commit into the worker's in-isolate EventV2.
 * The commit already durably committed in the hub writer, so it is republished
 * LIVE-ONLY — never re-committed — crossing verbatim as `{ id, type, properties }`
 * (`properties` is the EventV2 `data` reference). Its committed aggregate is then
 * woken so the isolate's `durable()` subscribers re-read and observe it.
 * Synchronous because the mesh delivery runs on the synchronous control-port
 * handler.
 *
 * The republished payload is stamped with {@link DURABLE_WAKEUP_METADATA_KEY} so
 * the worker's own event bridge ({@link createWorkerEventBridge}) does not
 * re-forward it: the mesh delivers a worker its OWN durable commit back too, and
 * re-forwarding would make the hub publish that event a second time.
 */
export function republishDurableCommit(
  events: Pick<EventV2.Interface, "publishLive" | "wakeDurable">,
  commit: DurableCommit,
): void {
  Effect.runSync(
    events.publishLive({
      id: EventV2.ID.make(commit.event.id),
      type: commit.event.type,
      data: commit.event.properties,
      metadata: { [DURABLE_WAKEUP_METADATA_KEY]: true },
    }),
  )
  // The durable write already happened in the hub writer; the in-isolate
  // `durable()` subscribers only need waking so their aggregate stream re-reads
  // and observes it. A reduced handle without the seam, or a commit whose type
  // this build does not know, skips the wake.
  const wakeDurable = events.wakeDurable
  if (wakeDurable === undefined) return
  const aggregateID = durableAggregateOf(commit)
  if (aggregateID === undefined) return
  Effect.runSync(wakeDurable(aggregateID))
}

/**
 * Wire the worker's durable wakeups: subscribe to the hub mesh through the egress
 * handle's control plane and republish each delivered commit into the isolate's
 * EventV2. Returns a disposer.
 *
 * A handle with no subscription surface, or with no mesh control plane (the
 * hub-less / degraded topology, e.g. the in-process parity harness), cannot be
 * woken durably: the bridge is simply not wired and the returned disposer is a
 * no-op. This is a legitimate topology, never an error — only a DIRECT
 * `subscribeDurable` call without a control port fails loud.
 */
export function wireDurableWakeups(
  events: Pick<EventV2.Interface, "publishLive" | "wakeDurable">,
  egress: Pick<EventEgressClientHandle, "subscribeDurable" | "mesh">,
): () => void {
  const subscribe = egress.subscribeDurable
  if (subscribe === undefined || egress.mesh === undefined) return () => {}
  return subscribe({
    pattern: WORKER_DURABLE_WAKE_PATTERN,
    onDurable: (commit) => republishDurableCommit(events, commit),
    // A resync means the hub dropped a delivery under backpressure; the worker's
    // next durable read catches it up, so there is nothing to replay here.
    onResync: () => {},
  })
}

/**
 * The egress handle IS the events-port sink: it accepts the verbatim
 * `{ id, type, properties }` event and its transport builds the `eventFrame`.
 */
const eventBridgeLayer = Layer.effect(
  WorkerEventBridge,
  Effect.gen(function* () {
    const events = yield* EventV2Bridge.Service
    const egress = yield* EventEgressClientService
    const bridge = yield* createWorkerEventBridge(events, egress)
    // The hub→worker durable wakeup mesh: the egress control client owns the
    // subscription; each delivered commit is republished into this isolate's
    // EventV2. A handle without a mesh yields a no-op.
    const unsubscribeDurable = wireDurableWakeups(events, egress)
    // Both subscriptions live exactly as long as the layer scope; disposing the
    // scope (worker shutdown) unsubscribes.
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => unsubscribeDurable()).pipe(Effect.andThen(bridge.dispose)),
    )
    return WorkerEventBridge.of({ dispose: bridge.dispose })
  }),
)

export const WorkerEventBridgeNode = LayerNode.make({
  service: WorkerEventBridge,
  layer: eventBridgeLayer,
  deps: [EventV2Bridge.node, EventEgressClientNode],
})

// ---------------------------------------------------------------------------
// Hub-proxy nodes (plan decision 64): `SessionStatus` / `Permission` /
// `Question` are hub-resident authorities. A worker reaches them with a
// blocking RPC over its control port; the proxy services satisfy the *real*
// hub service interfaces, so a session graph that depends on them composes
// unchanged. This is the generic hub-proxy wire the three nodes share.
// ---------------------------------------------------------------------------

// The wire itself lives in `hub-proxy-wire.ts` so neither the worker nor the hub
// imports the other. Re-exported here so existing consumers keep the same path.
export {
  createHubProxyClient,
  HubProxyRemoteError,
  serveHubProxy,
} from "./hub-proxy-wire"
export type {
  HubProxyClient,
  HubProxyFrame,
  HubProxyHandlers,
  HubProxyMethod,
  HubProxyReply,
  HubProxyRequest,
} from "./hub-proxy-wire"

/**
 * Rebuild a hub authority's typed domain error from its wire form. The wire
 * flattens a thrown error to a message + `_tag`; without this, a remote
 * `Permission` / `Question` rejection reaches the processor as a generic
 * `HubProxyRemoteError`, so its `instanceof PermissionV1.RejectedError` abort
 * check misses and the run continues instead of stopping. Rebuilding the
 * original class keeps the worker's abort behaviour identical to flag-off.
 */
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
    default:
      return error
  }
}

/** Await a hub-proxy call inside Effect; a remote failure is a typed error. */
function hubProxyCall<A>(client: HubProxyClient, method: HubProxyMethod, args: readonly unknown[]): Effect.Effect<A, Error> {
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
  /**
   * Optional dedicated subagent-dispatch client. Production multiplexes all
   * four over one port, so an absent value falls back to `sessionStatus`
   * (identical client). Present so a test can route dispatch independently.
   */
  readonly subagent?: HubProxyClient
}

/**
 * Worker-side `SubagentDispatchProxy` over the hub-proxy control port. The
 * `SessionPrompt` layer reads it optionally, so `ops.dispatchSubagent` is wired
 * exactly when a worker reaches a hub control plane.
 */
export function subagentDispatchProxyLayer(client: HubProxyClient): Layer.Layer<SubagentDispatchProxyService> {
  return Layer.succeed(SubagentDispatchProxyService, {
    dispatchSubagent: (request) => hubProxyCall<JobInfo>(client, "dispatchSubagent", [request]),
  })
}

/** The client the subagent-dispatch proxy multiplexes over. */
export function subagentDispatchClient(clients: HubProxyClients): HubProxyClient {
  return clients.subagent ?? clients.sessionStatus
}

/** Replacement layers for the three hub-resident authorities. */
export function hubProxyReplacements(clients: HubProxyClients): LayerNode.Replacements {
  return [
    [SessionStatusProxyNode, sessionStatusProxyLayer(clients.sessionStatus)],
    [PermissionProxyNode, permissionProxyLayer(clients.permission)],
    [QuestionProxyNode, questionProxyLayer(clients.question)],
  ]
}

/** The three hub-proxy nodes as a spreadable group. */
export const HUB_PROXY_NODES = LayerNode.group([SessionStatusProxyNode, PermissionProxyNode, QuestionProxyNode])

// ---------------------------------------------------------------------------
// Immutable per-worker snapshot + environment overlay (plan decisions 67/68).
// ---------------------------------------------------------------------------

export interface WorkerSnapshot {
  /** Monotonic snapshot epoch; a bump invalidates stale-tolerant caches. */
  readonly epoch: number
  readonly directory: string
  /**
   * The project id the worker's ambient `InstanceRef` carries. Shipped from the
   * hub's instance context so the isolate can rebuild a valid `Project.Info` —
   * `Snapshot.state`, `Session.create` and the event bridge all read `project.id`
   * and previously crashed on the `project: undefined as never` placeholder.
   */
  readonly projectId?: string
  /** The project VCS (`"git"`) when the hub's project is version-controlled. */
  readonly projectVcs?: "git"
  /** The project worktree; defaults to `directory` in the worker. */
  readonly worktree?: string
  /**
   * The hub's `config.directories()`, shipped verbatim so worker-side discovery
   * (skills/agents/commands) scans the same roots; absent ⇒ fall back to
   * `[directory]`.
   */
  readonly directories?: readonly string[]
  /**
   * The hub's resolved ripgrep executable path. Shipped so the worker runs the
   * same `rg` the hub uses instead of locating/downloading its own; absent ⇒ the
   * worker falls back to a PATH-resolved `rg`.
   */
  readonly ripgrepPath?: string
  readonly config: ConfigV1.Info
  readonly auth: Readonly<Record<string, unknown>>
  readonly modelsDev: Readonly<Record<string, ModelsDev.Provider>>
  readonly flags: RuntimeFlags.Info
  readonly workerCount: number
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

// ---------------------------------------------------------------------------
// Pipeline seam — the model call the worker runs (real LLM or a mock).
// ---------------------------------------------------------------------------

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
        // A rejected publish is a typed error, not an unhandled defect.
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
        // Only a durable commit is egressed; a non-committed (idempotent replay
        // or non-durable) publish emits nothing.
        const emitted = published.committed ? 1 : 0
        if (published.committed) {
          const boundary: EventV2Event = {
            id: `${input.sessionID}-text`,
            type: "session.text",
            properties: { text: completion.text, epoch: snapshot.epoch },
          }
          // The committed `session.text` is a durable boundary (5.5), so prefer
          // the transport's durable path when the handle offers one: it carries
          // a writer ordinal and bypasses delta coalescing. A handle without it
          // (the reduced test stubs / degraded boot) falls back to `emit`.
          if (egress.emitDurable !== undefined) egress.emitDurable(boundary)
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

// ---------------------------------------------------------------------------
// Boundary replacement layers (production wiring is Phases 7/10).
// ---------------------------------------------------------------------------

/** Raised when worker code reaches a boundary owned by the hub / writer / broker. */
export class SessionLayerBoundaryError extends Error {
  constructor(
    readonly boundary: string,
    readonly member: string,
  ) {
    super(`session worker boundary "${boundary}" cannot use "${member}": ${boundary} is hub/writer/broker-owned`)
    this.name = "SessionLayerBoundaryError"
  }
}

const boundaryViolation = (boundary: string, member: string): Effect.Effect<never> =>
  Effect.die(new SessionLayerBoundaryError(boundary, member))

/**
 * Build a hub/writer/broker-owned boundary service from the table of reads a
 * worker legitimately performs. Every method NOT listed fails loud with a
 * {@link SessionLayerBoundaryError} the moment it is called, so a method added
 * to the interface later is denied by default instead of silently returning
 * `undefined` or a `Layer.mock` no-op.
 *
 * The service is a `Proxy` because the method set is only known from the
 * interface type (erased at runtime): an unlisted property resolves to a
 * fail-loud function, so the table can never omit a denial.
 */
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

/**
 * Immutable snapshot-backed RuntimeFlags.
 *
 * A worker must NEVER re-derive flags from its own `process.env` / ConfigProvider:
 * the hub already resolved them once (including any per-layer override) and froze
 * the result into {@link WorkerSnapshot.flags}. Serving that value verbatim keeps
 * the isolate in lockstep with the hub — the previous gap let the reduced LLM
 * sub-graph see the hub flags while the full session loop silently fell back to
 * the process environment, so a flag set only on the hub (e.g.
 * `experimentalBackgroundSubagents`) never reached the session graph.
 */
export function snapshotRuntimeFlagsLayer(snapshot: WorkerSnapshot): Layer.Layer<RuntimeFlags.Service> {
  return Layer.succeed(RuntimeFlags.Service, snapshot.flags)
}

/** Database is writer-owned: any access is a loud violation, never a silent `{}`. */
export function databaseBoundaryLayer(): Layer.Layer<Database.Service> {
  const database: Database.Interface = {
    get db(): never {
      throw new SessionLayerBoundaryError("Database", "db")
    },
  }
  return Layer.succeed(Database.Service, database)
}

// ---------------------------------------------------------------------------
// Functional boundary layers for the hub / broker-owned authorities.
//
// A worker legitimately owns no MCP/LSP servers, no OS process, no git checkout
// and no storage. Each layer below therefore serves the flag-off EMPTY value for
// the reads the session path performs (an MCP catalog with no servers, an LSP
// with no clients) and fails loud with a {@link SessionLayerBoundaryError} for a
// mutation a worker may not perform. The previous `Layer.mock(Service, {})`
// provided neither: an unlisted read died with `Unimplemented method "<name>"`,
// aborting the prompt loop the moment it read the MCP catalog.
// ---------------------------------------------------------------------------

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

/**
 * The ripgrep binary a worker runs.
 *
 * `Ripgrep` runs in-worker (the worker has the real `AppProcess` over its
 * `CrossSpawnSpawner`), but it must never download or locate the binary itself:
 * the hub resolved the real path and froze it into the snapshot. Serve that path
 * verbatim, falling back to a PATH-resolved `rg` when the snapshot omits it.
 */
export function snapshotRipgrepBinaryLayer(snapshot: WorkerSnapshot): Layer.Layer<RipgrepBinary.Service> {
  return Layer.succeed(RipgrepBinary.Service, { filepath: Effect.succeed(snapshot.ripgrepPath ?? "rg") })
}

/** The MCP browser boundary: a worker never opens a browser. */
export function mcpBrowserBoundaryLayer(): Layer.Layer<McpBrowser.Service> {
  return failLoudBoundary("McpBrowser", McpBrowser.Service, {})
}

/**
 * The `Permission` boundary for a worker with no hub control plane (degraded
 * boot). `Permission` is a hub-resident authority, so a worker that cannot reach
 * the hub must fail loud when a session asks for authorization — never a silent
 * `Layer.mock` no-op that would let a denied tool run. `list` is a neutral read
 * and stays empty; `ask`/`reply` die with a descriptive
 * {@link SessionLayerBoundaryError}.
 */
export function permissionBoundaryLayer(): Layer.Layer<Permission.Service> {
  return failLoudBoundary("Permission", Permission.Service, {
    list: () => Effect.succeed([]),
  })
}

/** A writer primitive the reduced/degraded handle did not implement. */
const missingWriterPrimitive = (method: keyof WriterTransactionHandle) => (): Promise<never> =>
  Promise.reject(new SessionLayerBoundaryError("WriterClient", method))

/**
 * Parse the worker writer handle into the statement surface the writer-backed
 * `SqlClient` drives. A production writer supplies every primitive; a partial
 * handle yields a method that fails loud when the Database actually needs it.
 */
function writerSqlHandle(writer: WriterClientHandle): WriterSqlHandle {
  return {
    query: writer.query,
    begin: writer.begin ?? missingWriterPrimitive("begin"),
    txQuery: writer.txQuery ?? missingWriterPrimitive("txQuery"),
    commit: writer.commit ?? missingWriterPrimitive("commit"),
    rollback: writer.rollback ?? missingWriterPrimitive("rollback"),
  }
}

/**
 * The worker `Database`, backed by the hub-owned writer (plan decision 24).
 *
 * Workers hold no SQLite connection: `writerSqlClientLayer` turns the writer's
 * `query` / transaction primitives into a real Effect `SqlClient`, and the same
 * `EffectDrizzleSqlite.makeWithDefaults()` construction as the flag-OFF database
 * builds the Drizzle `Database.Interface`. `select` / `insert` / `update` /
 * `delete` / `transaction` therefore return Effects and run unchanged inside the
 * worker. The result is structurally a `Database.Interface` — no cast.
 */
export function writerDatabaseLayer(writer: WriterClientHandle): Layer.Layer<Database.Service> {
  const makeDatabase = EffectDrizzleSqlite.makeWithDefaults()
  return Layer.effect(
    Database.Service,
    Effect.map(makeDatabase, (db): Database.Interface => ({ db })),
  ).pipe(Layer.provide(writerSqlClientLayer(writerSqlHandle(writer))))
}

/**
 * The process-local `BackgroundJob` registry. Used when no hub-backed client is
 * supplied (tests / degraded boot); production overrides it with
 * {@link backgroundJobClientLayer} over the transferred job port so every
 * lifecycle transition is reported to the hub registry (plan decision 54).
 */
export function localBackgroundJobLayer(): Layer.Layer<BackgroundJob.Service> {
  return Layer.effect(BackgroundJob.Service, CoreBackgroundJob.make)
}

/**
 * The hub-backed `BackgroundJob` service over a full {@link JobClient}: the
 * worker owns the run locally, the hub registry owns cross-worker coordination
 * (plan decision 54). This is the production replacement for the excluded
 * `BackgroundJob` node.
 */
export function backgroundJobClientLayer(client: JobClient): Layer.Layer<BackgroundJob.Service> {
  return BackgroundJob.clientLayer(client)
}

/**
 * Boundary replacements every session layer needs. `Database` is replaced so the
 * worker never opens SQLite (a real writer-proxy when a `writer` client is
 * supplied — plan decision 24); MCP/LSP are reached through the broker client
 * instead. `SessionProjector` is intentionally NOT replaced: it runs in-worker,
 * registering `EventV2.project` subscribers so durable session events materialize
 * the `session`/`message`/`part` rows, inside the writer-pinned transaction.
 *
 * Every other hub/writer/broker-owned node reachable from the real
 * `SessionPrompt` graph is replaced by a FUNCTIONAL boundary layer, not
 * `Layer.empty`: the graph must boot (a dependency on an absent service is a
 * build failure), and a worker that reaches a hub-owned authority fails loud at
 * call time. `BackgroundJob` uses the hub client when supplied; otherwise the
 * process-local registry.
 */
export function boundaryReplacements(
  snapshot: WorkerSnapshot,
  writer?: WriterClientHandle,
  backgroundJob?: Layer.Layer<BackgroundJob.Service>,
): LayerNode.Replacements {
  return [
    [Config.node, snapshotConfigLayer(snapshot)],
    [Auth.node, snapshotAuthLayer(snapshot)],
    [ModelsDev.node, snapshotModelsDevLayer(snapshot)],
    // The hub resolved every runtime flag (including any per-layer override) and
    // froze it into the snapshot; the worker serves that value rather than
    // re-deriving from its own `process.env`. Without this replacement the full
    // session graph built the real RuntimeFlags node and re-read the environment,
    // so a hub-only flag override never crossed the isolate boundary.
    [RuntimeFlags.node, snapshotRuntimeFlagsLayer(snapshot)],
    [Database.node, writer === undefined ? databaseBoundaryLayer() : writerDatabaseLayer(writer)],
    // `SessionProjector` deliberately has NO boundary replacement: it runs IN the
    // worker. `Session.updateMessage`/`updatePart` publish durable events, and the
    // projector materializes the `session`/`message`/`part` rows the loop reads
    // back. Its `EventV2.project` subscribers run inside `EventV2`'s durable-commit
    // `db.transaction`, which the writer-backed Database routes through the writer
    // tx primitives — so projection is atomic with the event insert, exactly as
    // flag-off. (The node-table classifies it `worker-local`.)
    // Hub-owned / broker-owned authorities the reduced graph yields. Each is a
    // FUNCTIONAL boundary: the graph BOOTS (an `Layer.empty` provides nothing, so
    // `SessionPrompt`'s `yield* MCP.Service`/`LSP.Service` would fail at build)
    // AND a session that reads a neutral catalog (MCP `clients`/`instructions`,
    // LSP `diagnostics`) gets the flag-off empty value instead of aborting. A
    // genuinely unsupported mutation (an MCP `add`, a git `run`) still fails loud
    // with a `SessionLayerBoundaryError`.
    [MCP.node, mcpBoundaryLayer()],
    [LSP.node, lspBoundaryLayer()],
    [McpAuth.node, mcpAuthBoundaryLayer()],
    [Npm.node, npmBoundaryLayer()],
    [Account.node, accountBoundaryLayer()],
    [AccountRepo.node, accountRepoBoundaryLayer()],
    // Residual hub/writer/broker/infra boundaries (see `SESSION_EXCLUDED`).
    [Storage.node, storageBoundaryLayer()],
    [Git.node, gitBoundaryLayer()],
    [BackgroundJob.node, backgroundJob ?? localBackgroundJobLayer()],
    // `Global` is a plain record of paths (no I/O); the real value is safe and
    // lets any read of `Global.Path`/`Service` work rather than return undefined.
    [Global.node, Layer.succeed(Global.Service, Global.make())],
    // `AppProcess` and `Ripgrep` run IN the worker: the worker already has a real
    // `CrossSpawnSpawner` (via `ToolRegistry`), so its ripgrep search path is
    // functional, not a fail-loud boundary. Only the binary path is frozen — the
    // worker must never locate or download `rg` on its own.
    [RipgrepBinary.node, snapshotRipgrepBinaryLayer(snapshot)],
    [McpBrowser.node, mcpBrowserBoundaryLayer()],
    // The hub composes the location service map; the worker never resolves a
    // location for a V1 session, so a lazily-built empty map satisfies the graph
    // without pulling the V2 location graph into the isolate.
    [LocationServiceMap.node, buildLocationServiceMap()],
  ]
}

// ---------------------------------------------------------------------------
// In-worker LLM (plan decision 20): the stream stays in the isolate. A worker
// builds a *reduced* `Provider` from its serializable model snapshot and runs
// the real `LLM` node over it. Nothing proxies the `Response`/`ReadableStream`.
// ---------------------------------------------------------------------------

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

/**
 * The reduced provider bootstrap a worker consumes. Identity and transport come
 * from the hub; `capabilities` / `limit` / `options` / `variants` /
 * `providerOptions` are the REAL serialized model descriptor, so the worker's
 * outgoing request params equal flag-off (no synthesized temperature capability,
 * no 4096 output cap, no dropped reasoning/variant/option surface).
 *
 * The descriptor fields are optional so a legacy/minimal bootstrap still boots;
 * an absent field is treated as "not advertised" rather than re-invented.
 */
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

/**
 * Raised when a session asks the worker for a provider/model other than the one
 * the hub serialized into its bootstrap. A worker owns exactly one model, so the
 * mismatch is diagnosed at the provider boundary — naming both models — before
 * any request is made, instead of surfacing as a generic
 * `ProviderModelNotFoundError` die mid-run.
 */
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
      // The descriptor's real capabilities; absent means "not advertised", so we
      // never synthesize `temperature: true` (which forced `transform.temperature`
      // to 1.0 for glm/minimax/kimi/gemini) or a dropped reasoning surface.
      temperature: capabilities?.temperature ?? false,
      reasoning: capabilities?.reasoning ?? false,
      attachment: capabilities?.attachment ?? false,
      toolcall: capabilities?.toolcall ?? true,
      interleaved: capabilities?.interleaved ?? false,
      input: capabilities?.input ?? allModalities,
      output: capabilities?.output ?? allModalities,
    },
    cost: { input: 0, output: 0, cache: { read: 0, write: 0 } },
    // The descriptor's real limits. An absent output limit means "unbounded"
    // (`maxOutputTokens` then falls back to the global max) rather than the
    // synthesized 4096 cap that truncated worker turns.
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

/**
 * The worker transport fetch: applies the serialized `headerTimeout` / `timeout`
 * / `chunkTimeout` provider options the same way the hub's `resolveSDK` installs
 * `timeoutFetch` (`provider.ts:1852`), so a worker request is bounded like
 * flag-off instead of using the unbounded global `fetch`.
 */
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

/**
 * Build the reduced provider's SDK from the serialized `npm`, so a worker makes
 * the provider-shaped request the hub resolved instead of always an
 * OpenAI-compatible body. Unknown packages keep the OpenAI-compatible default
 * (the previous behavior), so a provider that is OpenAI-compatible but has no
 * bundled SDK still works.
 *
 * The serialized `providerOptions` are spread into the SDK the same way the
 * hub's `resolveSDK` does (`provider.ts:1791-1865`): the base URL / API key /
 * headers override them, `includeUsage` is set for OpenAI-compatible providers,
 * and a timeout-aware `fetch` bounds the request.
 */
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
    default:
      return createOpenAICompatible(
        options as Parameters<typeof createOpenAICompatible>[0],
      ) as unknown as WorkerLanguageSdk
  }
}

/**
 * The reduced in-worker `Provider`: a client over the serialized
 * `{providerID, modelID, apiId, npm, baseUrl, apiKey}` bootstrap plus the real
 * serialized model descriptor. `getLanguage` returns a real AI-SDK language
 * model, so the worker makes the HTTP call itself.
 */
export function reducedModelProviderLayer(config: WorkerModelConfig): Layer.Layer<Provider.Service> {
  const sdk = createWorkerLanguageSdk(config)
  const model = reducedProviderModel(config)
  const info = reducedProviderInfo(config, model)
  // The reduced provider serves exactly one model; any other request is a
  // programming error surfaced loudly rather than silently mis-served.
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
  // The reduced provider is structurally a `Provider.Interface`, but its
  // single-model mismatch is a more specific typed error than the interface's
  // `ModelNotFoundError`. The cast keeps that diagnosis intact; the worker's
  // prompt path surfaces it verbatim instead of a generic not-found die.
  return Layer.succeed(Provider.Service, service as unknown as Provider.Interface)
}

/**
 * The boundary `Plugin.trigger`, erased to its runtime shape so the no-op and
 * host-backed layers share one signature.
 */
type PluginTrigger = Plugin.Interface["trigger"]

/** No-op pass-through trigger: the hook leaves the output untouched. */
const noopPluginTrigger = ((_name: string, _input: unknown, output: unknown) =>
  Effect.succeed(output)) as PluginTrigger

/**
 * Dispatch one `Plugin.trigger` to the matching worker-hosted hook.
 */
function runWorkerPluginHook(host: WorkerPluginHost, name: string, input: unknown, output: unknown): Promise<unknown> {
  switch (name) {
    case "chat.params":
      return host.chatParams(input, output as ChatParamsOutput)
    case "chat.headers":
      return host.chatHeaders(input, output as ChatHeadersOutput)
    case "tool.execute.before":
      return host.toolBefore(input, output as ToolBeforeOutput)
    case "tool.execute.after":
      return host.toolAfter(input, output as ToolAfterOutput)
    case "experimental.chat.system.transform":
      return host.systemTransform(input as SystemTransformInput, output as SystemTransformOutput)
    case "experimental.chat.messages.transform":
      return host.messagesTransform(input as MessagesTransformInput, output as MessagesTransformOutput)
    default:
      return Promise.resolve(output)
  }
}

/**
 * The boundary `Plugin` for a worker, parameterised by its `trigger` (decision
 * 70): the hub owns plugin *loading*, but the model-facing hooks run in the
 * isolate. A no-op trigger yields the pass-through; a host-backed trigger runs
 * `chat.params` / `chat.headers`, the tool hooks, and the system/messages
 * transforms in-worker, so plugin-set sampling/headers and pruning match
 * flag-off. One layer covers both cases.
 */
function workerPluginLayer(trigger: PluginTrigger): Layer.Layer<Plugin.Service> {
  const plugin = {
    trigger,
    list: () => Effect.succeed([]),
    init: () => Effect.void,
  } as unknown as Plugin.Interface
  return Layer.succeed(Plugin.Service, plugin)
}

/**
 * The worker-safe `Plugin.node` replacement: the worker-hosted hooks when a
 * {@link WorkerPluginHost} is supplied, otherwise a no-op pass-through. This is
 * the single source for the replacement, used by both the full worker graph
 * ({@link buildSessionWorkerLayer}) and the reduced `LLM` sub-graph
 * ({@link reducedLlmLayer}), so neither ever builds the global `Plugin` layer.
 */
function workerPluginReplacement(plugins?: WorkerPluginHost): LayerNode.Replacement {
  const trigger: PluginTrigger =
    plugins === undefined
      ? noopPluginTrigger
      : (((name: string, input: unknown, output: unknown) =>
          Effect.promise(() => runWorkerPluginHook(plugins, name, input, output))) as PluginTrigger)
  return [Plugin.node, workerPluginLayer(trigger)]
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

/**
 * An `LLM.stream`-backed `ModelClient`: concatenates the streamed `text-delta`
 * events of one provider turn (design decision 20 — the stream is consumed
 * in-worker, never proxied).
 */
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

/**
 * A fail-loud service for a worker boundary the *standalone* reduced LLM
 * sub-graph must never call. Every member access dies with a descriptive
 * {@link SessionLayerBoundaryError} naming the boundary and member, so a
 * mis-wired worker surfaces a precise diagnosis instead of a generic
 * `UnimplementedError`. The full worker graph supplies the real nodes; this
 * sub-graph only needs a satisfiable service so `LLM` compiles.
 */
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

/**
 * The `EventV2Bridge` boundary for the standalone reduced LLM sub-graph: the
 * isolate publishes through its own event port in the full graph, so this
 * sub-graph never reaches the hub bridge and fails loud if it ever does.
 */
function eventV2BridgeBoundaryLayer(): Layer.Layer<EventV2Bridge.Service> {
  return Layer.succeed(EventV2Bridge.Service, failLoudService<EventV2.Interface>("EventV2Bridge"))
}

/**
 * The `LLMClient` boundary for the standalone reduced LLM sub-graph: the full
 * worker graph builds the real platform client, so this sub-graph never issues a
 * request and fails loud if it ever does.
 */
function llmClientBoundaryLayer(): Layer.Layer<LLMClientService> {
  return Layer.succeed(LLMClient.Service, failLoudService<LLMClientShape>("LLMClient"))
}

/**
 * The boundary replacements for the standalone reduced LLM sub-graph.
 * `Permission` is a hub-resident authority: route it over the hub-proxy control
 * port when the worker has one, otherwise fail loud. `EventV2Bridge`/`LLMClient`
 * are hub-owned; this sub-graph only needs satisfiable services so `LLM`
 * compiles and never reaches them. `Plugin` is a worker-safe replacement here
 * (host when supplied, no-op otherwise); the full worker graph additionally
 * replaces `Plugin.node` at the top level ({@link buildSessionWorkerLayer}).
 *
 * Exported so a test can assert the per-node boundary wiring without resolving
 * the sub-graph's (single, `LLM`) output service.
 */
export function llmBoundaryReplacements(
  snapshot: WorkerSnapshot,
  config: WorkerModelConfig,
  hubProxies?: HubProxyClients,
  plugins?: WorkerPluginHost,
): LayerNode.Replacements {
  return [
    [Auth.node, snapshotAuthLayer(snapshot)],
    [Config.node, snapshotConfigLayer(snapshot)],
    [Provider.node, reducedModelProviderLayer(config)],
    workerPluginReplacement(plugins),
    // `Permission` is a hub-resident authority: route it over the hub-proxy
    // control port when the worker has one, otherwise fail loud. Never a silent
    // mock, which would let a denied tool run in a degraded isolate.
    [Permission.node, hubProxies === undefined ? permissionBoundaryLayer() : permissionProxyLayer(hubProxies.permission)],
    [EventV2Bridge.node, eventV2BridgeBoundaryLayer()],
    [llmClient, llmClientBoundaryLayer()],
    [RuntimeFlags.node, snapshotRuntimeFlagsLayer(snapshot)],
  ]
}

/**
 * The worker-local `LLM` service: the real `LLM` node composed over the reduced
 * provider, the snapshot Config/Auth/ModelsDev and the hub-proxy authorities.
 * This keeps the provider stream inside the isolate (design decision 20).
 */
export function reducedLlmLayer(
  snapshot: WorkerSnapshot,
  config: WorkerModelConfig,
  hubProxies?: HubProxyClients,
  plugins?: WorkerPluginHost,
): Layer.Layer<LLM.Service> {
  return AppNodeBuilderV1.build(LLM.node, llmBoundaryReplacements(snapshot, config, hubProxies, plugins))
}

// ---------------------------------------------------------------------------
// Groups + builder
// ---------------------------------------------------------------------------

/**
 * The real session nodes a worker must contain, keyed by service key. The
 * groups below are derived from `REQUIRED_SESSION_NODE_KEYS` (node-table.ts)
 * through this map, so the layer and the node table cannot drift apart.
 */
const SESSION_NODE_BY_KEY: ReadonlyMap<string, LayerNode.AnyNode> = new Map<string, LayerNode.AnyNode>([
  [Session.node.name, Session.node],
  [SessionPrompt.node.name, SessionPrompt.node],
  [SessionProcessor.node.name, SessionProcessor.node],
  [SessionRunState.node.name, SessionRunState.node],
  [SessionProjector.node.name, SessionProjector.node],
  [SessionStatus.node.name, SessionStatus.node],
  [SessionCompaction.node.name, SessionCompaction.node],
  [SessionRevert.node.name, SessionRevert.node],
  [SessionSummary.node.name, SessionSummary.node],
  [Todo.node.name, Todo.node],
  [LLM.node.name, LLM.node],
  [Provider.node.name, Provider.node],
  [Agent.node.name, Agent.node],
  [Skill.node.name, Skill.node],
  [Discovery.node.name, Discovery.node],
  [ToolRegistry.node.name, ToolRegistry.node],
  [Plugin.node.name, Plugin.node],
  [Config.node.name, Config.node],
  [Auth.node.name, Auth.node],
  [RuntimeFlags.node.name, RuntimeFlags.node],
  [Question.node.name, Question.node],
  [Permission.node.name, Permission.node],
  [Truncate.node.name, Truncate.node],
  [Format.node.name, Format.node],
  [Instruction.node.name, Instruction.node],
  [SystemPrompt.node.name, SystemPrompt.node],
  [Command.node.name, Command.node],
  [Image.node.name, Image.node],
  [EventV2Bridge.node.name, EventV2Bridge.node],
  [EventV2.node.name, EventV2.node],
  [Snapshot.node.name, Snapshot.node],
  [llmClient.name, llmClient],
])

/** Resolve a required session-node key; a missing registration fails loud. */
function requiredSessionNode(key: string): LayerNode.AnyNode {
  const node = SESSION_NODE_BY_KEY.get(key)
  if (node === undefined) throw new Error(`no session node is registered for required key "${key}"`)
  return node
}

/** The declared session-role allowlist group (real nodes). */
export const SESSION_NODES = LayerNode.group([
  ...REQUIRED_SESSION_NODE_KEYS.map(requiredSessionNode),
  WorkerEventBridgeNode,
  WriterClientNode,
  EventEgressClientNode,
  BrokerClientNode,
  BackgroundJobClientNode,
])

/**
 * The declared *runnable* graph: every required session-node service plus the
 * worker-infrastructure nodes (pipeline seam, clients, snapshot, env). A worker
 * is "able to satisfy" the real `SessionPrompt`/`SessionProcessor` loop when
 * `validateSessionLayer(RUNNABLE_SESSION_NODES, boundaryReplacements(...))` is
 * clean and fails closed on any unclassified reachable node.
 */
export const RUNNABLE_SESSION_NODES = LayerNode.group([
  ...REQUIRED_SESSION_NODE_KEYS.map(requiredSessionNode),
  SessionPipelineNode,
  WorkerEventBridgeNode,
  WriterClientNode,
  EventEgressClientNode,
  BrokerClientNode,
  BackgroundJobClientNode,
  WorkerSnapshotNode,
  WorkerEnvNode,
])

/**
 * The reduced pipeline subset: the one-shot `SessionPipeline` seam, runtime
 * flags and the four client nodes. `ModelClient` is the injectable model seam,
 * so a test can point it at the mock provider. Retained as an explicit,
 * lightweight graph for pipeline-only tests; the production default is the full
 * runnable session graph (`RUNNABLE_SESSION_NODES`), which now boots against the
 * functional boundary layers.
 */
export const PIPELINE_SESSION_NODES = LayerNode.group([
  SessionPipelineNode,
  RuntimeFlags.node,
  WriterClientNode,
  EventEgressClientNode,
  BrokerClientNode,
  BackgroundJobClientNode,
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
    readonly broker: BrokerClientHandle
    readonly backgroundJob: BackgroundJobClientHandle
  }
  /** Model seam; required because the layer never talks to a real provider by default. */
  readonly modelClient: Layer.Layer<ModelClient>
  /**
   * Hub-resident authority proxies (plan decision 64). When supplied, the
   * `SessionStatus` / `Permission` / `Question` hub-proxy nodes join the graph
   * and their services are provided over the ports.
   */
  readonly hubProxies?: HubProxyClients
  /**
   * Functional `BackgroundJob` replacement. Production supplies the hub-backed
   * client layer ({@link backgroundJobClientLayer}); the default is the
   * process-local registry.
   */
  readonly backgroundJob?: Layer.Layer<BackgroundJob.Service>
  /**
   * Worker→hub PID reporter. Production supplies the control-port-backed
   * reporter so a detached shell child's process group reaches the hub mirror.
   * Absent on degraded/hub-only paths, where the shell tool is a no-op.
   */
  readonly pidReporter?: PidReporterInterface
  /**
   * The worker-hosted plugin hooks. The session layer owns the `Plugin.node`
   * replacement (the hub loads plugins, decision 70), so the model-facing hooks
   * run in this isolate. Absent, the graph uses a no-op pass-through; the global
   * `Plugin` layer is never built in the worker.
   */
  readonly plugins?: WorkerPluginHost
  /**
   * Non-graph worker logging layer (see `worker-logging.ts`). Merged beside the
   * non-graph seams so the worker's `Effect.log*` reaches the main sink; absent
   * keeps the graph byte-for-byte unchanged.
   */
  readonly logging?: Layer.Layer<never>
  /** Extra replacements, e.g. the real LLM-backed `ModelClient` in production. */
  readonly replacements?: LayerNode.Replacements
}

export interface SessionWorkerLayer {
  readonly root: LayerNode.AnyNode
  readonly replacements: LayerNode.Replacements
  readonly layer: Layer.Layer<unknown, unknown>
}

/**
 * Compose the reduced session layer, assert it against the node table, and
 * return the compiled layer. Fails loud (`SessionLayerViolationError`) if an
 * excluded node leaked in or a required client node is missing.
 */
export function buildSessionWorkerLayer(options: SessionLayerOptions): SessionWorkerLayer {
  const base = options.nodes ?? RUNNABLE_SESSION_NODES
  const root = options.hubProxies === undefined ? base : LayerNode.group([base, HUB_PROXY_NODES])
  const replacements: LayerNode.Replacements = [
    [WriterClientNode, writerClient.layer(options.clients.writer)],
    [EventEgressClientNode, eventEgressClient.layer(options.clients.egress)],
    [BrokerClientNode, brokerClient.layer(options.clients.broker)],
    [BackgroundJobClientNode, backgroundJobClient.layer(options.clients.backgroundJob)],
    [WorkerSnapshotNode, Layer.succeed(WorkerSnapshotService, options.snapshot)],
    [WorkerEnvNode, Layer.succeed(WorkerEnvService, options.env)],
    [ModelClientNode, options.modelClient],
    // The worker graph owns `Plugin`: the hub loads plugins (decision 70), but
    // the model-facing hooks run here. Replace the real node for the whole graph
    // so it is never built in the isolate.
    workerPluginReplacement(options.plugins),
    ...boundaryReplacements(options.snapshot, options.clients.writer, options.backgroundJob),
    ...(options.hubProxies === undefined ? [] : hubProxyReplacements(options.hubProxies)),
    ...(options.replacements ?? []),
  ]

  assertSessionLayer(root, replacements)
  const built = AppNodeBuilderV1.build(root, replacements)
  // `PidReporter` is not a graph node: the shell tool reads it optionally at
  // execution time, so merging it keeps degraded graphs (no reporter) building
  // unchanged while the production graph sees the control-port reporter.
  const withPid =
    options.pidReporter === undefined
      ? built
      : Layer.merge(built, Layer.succeed(PidReporter, options.pidReporter))
  // `SubagentDispatchProxy` is likewise not a graph node: `SessionPrompt.ops`
  // reads it optionally, so a hub-reaching worker gets `dispatchSubagent` while
  // a degraded/hub-less graph builds unchanged (and runs sub-agents inline).
  const layer =
    options.hubProxies === undefined
      ? withPid
      : Layer.merge(withPid, subagentDispatchProxyLayer(subagentDispatchClient(options.hubProxies)))
  // `logging` is not a graph node either: it contributes only context references
  // (the file sink + minimum level), so merging it leaves the graph and its
  // node-table assertion untouched. Absent keeps the layer unchanged.
  const withLogging = options.logging === undefined ? layer : Layer.merge(layer, options.logging)
  return { root, replacements, layer: withLogging }
}

export * as SessionWorkerLayer from "./layer"
