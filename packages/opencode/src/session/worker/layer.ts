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
import { LLMClient } from "@opencode-ai/llm/route"
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
import { AppProcess } from "@opencode-ai/core/process"
import { Ripgrep } from "@opencode-ai/core/ripgrep"
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
import { createWorkerEventBridge } from "./event-bridge"
import type { EventV2Event } from "./frame"
import { messageOf, portFromMessagePort } from "./port"
import type { JobClient } from "./job-client"
import type {
  ChatHeadersOutput,
  ChatParamsOutput,
  ToolAfterOutput,
  ToolBeforeOutput,
  WorkerPluginHost,
} from "./plugin-host"
import { writerSqlClientLayer, type WriterSqlHandle } from "./writer-sql-client"
import { PidReporter, type PidReporterInterface } from "./pid-registry"
import type { JobInfo } from "./job-registry"
import { SubagentDispatchProxyService, type SubagentDispatchRequest } from "./subagent"

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
    // The subscription lives exactly as long as the layer scope; disposing the
    // scope (worker shutdown) unsubscribes.
    yield* Effect.addFinalizer(() => bridge.dispose)
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

export type HubProxyRequest = {
  readonly kind: "request"
  readonly id: number
  readonly method: string
  readonly args: readonly unknown[]
}

export type HubProxyReply =
  | { readonly kind: "reply"; readonly id: number; readonly ok: true; readonly value: unknown }
  | { readonly kind: "reply"; readonly id: number; readonly ok: false; readonly message: string }

export type HubProxyFrame = HubProxyRequest | HubProxyReply

/** A typed failure returned by a hub authority across the port. */
export class HubProxyRemoteError extends Error {
  constructor(message: string) {
    super(message)
    this.name = "HubProxyRemoteError"
  }
}

export interface HubProxyClient {
  readonly call: (method: string, args: readonly unknown[]) => Promise<unknown>
  /**
   * The typed subagent-dispatch RPC (Solution 1). Optional so lightweight test
   * doubles (`{ call, dispose }`) remain assignable; the multiplexed client
   * always installs it.
   */
  readonly dispatchSubagent?: (request: SubagentDispatchRequest) => Promise<JobInfo>
  dispose(): void
}

interface HubPending {
  readonly resolve: (frame: HubProxyReply) => void
  readonly reject: (error: unknown) => void
}

/**
 * Worker side: a thin request/reply client over a hub-proxy `MessagePort`.
 * Requests carry a numeric `id` correlated by the client; replies echo it. Only
 * replies are addressed to the client, so request frames are ignored.
 */
export function createHubProxyClient(port: MessagePort): HubProxyClient {
  const channel = portFromMessagePort<HubProxyFrame>(port)
  let nextId = 1
  let disposed = false
  const pending = new Map<number, HubPending>()

  const unsubscribe = channel.onMessage((frame) => {
    if (frame.kind !== "reply") return
    const waiter = pending.get(frame.id)
    if (waiter === undefined) return
    pending.delete(frame.id)
    waiter.resolve(frame)
  })

  const call = (method: string, args: readonly unknown[]): Promise<unknown> => {
    if (disposed) return Promise.reject(new HubProxyRemoteError("hub proxy client disposed"))
    const id = nextId++
    return new Promise<unknown>((resolve, reject) => {
      pending.set(id, {
        resolve: (frame) => (frame.ok ? resolve(frame.value) : reject(new HubProxyRemoteError(frame.message))),
        reject,
      })
      try {
        channel.post({ kind: "request", id, method, args })
      } catch (error) {
        pending.delete(id)
        reject(error)
      }
    })
  }

  return {
    call,
    dispatchSubagent: (request) => call("dispatchSubagent", [request]) as Promise<JobInfo>,
    dispose() {
      if (disposed) return
      disposed = true
      unsubscribe()
      for (const waiter of pending.values()) waiter.reject(new HubProxyRemoteError("hub proxy client disposed"))
      pending.clear()
    },
  }
}

export type HubProxyHandlers = Readonly<Record<string, (...args: readonly unknown[]) => unknown>>

/** Hub side: serve hub-proxy requests against `handlers`; returns a disposer. */
export function serveHubProxy(handlers: HubProxyHandlers, port: MessagePort): () => void {
  const channel = portFromMessagePort<HubProxyFrame>(port)
  return channel.onMessage((frame) => {
    if (frame.kind !== "request") return
    const reply = (payload: HubProxyReply): void => {
      try {
        channel.post(payload)
      } catch {
        // Drop the reply: the worker is gone.
      }
    }
    const handler = handlers[frame.method]
    if (handler === undefined) {
      reply({ kind: "reply", id: frame.id, ok: false, message: `unknown hub-proxy method "${frame.method}"` })
      return
    }
    Promise.resolve()
      .then(() => handler(...frame.args))
      .then(
        (value) => reply({ kind: "reply", id: frame.id, ok: true, value: value ?? null }),
        (error) => reply({ kind: "reply", id: frame.id, ok: false, message: messageOf(error) }),
      )
  })
}

/** Await a hub-proxy call inside Effect; a remote failure is a typed error. */
function hubProxyCall<A>(client: HubProxyClient, method: string, args: readonly unknown[]): Effect.Effect<A, Error> {
  return Effect.tryPromise({
    try: () => client.call(method, args) as Promise<A>,
    catch: (error) => (error instanceof Error ? error : new Error(String(error))),
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
    get: (sessionID: SessionID) => hubProxyCall<SessionStatus.Info>(client, "get", [sessionID]),
    list: () =>
      hubProxyCall<ReadonlyArray<readonly [SessionID, SessionStatus.Info]>>(client, "list", []).pipe(
        Effect.map((entries) => new Map(entries)),
      ),
    set: (sessionID: SessionID, status: SessionStatus.Info) => hubProxyCall<void>(client, "set", [sessionID, status]),
  } as unknown as SessionStatus.Interface
  return Layer.succeed(SessionStatus.Service, service)
}

/** `Permission` proxy over a hub port; methods satisfy the real interface. */
export function permissionProxyLayer(client: HubProxyClient): Layer.Layer<Permission.Service> {
  const service = {
    ask: (input: PermissionV1.AskInput) => hubProxyCall<void>(client, "ask", [input]),
    reply: (input: PermissionV1.ReplyInput) => hubProxyCall<void>(client, "reply", [input]),
    list: () => hubProxyCall<ReadonlyArray<PermissionV1.Request>>(client, "list", []),
  } as unknown as Permission.Interface
  return Layer.succeed(Permission.Service, service)
}

/** `Question` proxy over a hub port; methods satisfy the real interface. */
export function questionProxyLayer(client: HubProxyClient): Layer.Layer<Question.Service> {
  const service = {
    ask: (input: { sessionID: SessionID; questions: ReadonlyArray<Question.Info>; tool?: Question.Tool }) =>
      hubProxyCall<ReadonlyArray<Question.Answer>>(client, "ask", [input]),
    reply: (input: {
      requestID: Parameters<Question.Interface["reject"]>[0]
      answers: ReadonlyArray<Question.Answer>
    }) => hubProxyCall<void>(client, "reply", [input]),
    reject: (requestID: Parameters<Question.Interface["reject"]>[0]) =>
      hubProxyCall<void>(client, "reject", [requestID]),
    list: () => hubProxyCall<ReadonlyArray<Question.Request>>(client, "list", []),
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
          egress.emit({
            id: `${input.sessionID}-text`,
            type: "session.text",
            properties: { text: completion.text, epoch: snapshot.epoch },
          })
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

/** Immutable snapshot-backed Config: workers never read files or write config. */
export function snapshotConfigLayer(snapshot: WorkerSnapshot): Layer.Layer<Config.Service> {
  const config: Config.Interface = {
    get: () => Effect.succeed(snapshot.config),
    getGlobal: () => Effect.succeed(snapshot.config),
    getConsoleState: () => Effect.succeed(emptyConsoleState),
    update: () => boundaryViolation("Config", "update"),
    updateGlobal: () => boundaryViolation("Config", "updateGlobal"),
    invalidate: () => boundaryViolation("Config", "invalidate"),
    directories: () => Effect.succeed([snapshot.directory]),
    waitForDependencies: () => Effect.void,
  }
  return Layer.succeed(Config.Service, config)
}

/** Immutable snapshot-backed Auth: credentials never touch `process.env`. */
export function snapshotAuthLayer(snapshot: WorkerSnapshot): Layer.Layer<Auth.Service> {
  const auth: Auth.Interface = {
    get: (providerID: string) => Effect.succeed(snapshot.auth[providerID] as Auth.Info | undefined),
    all: () => Effect.succeed(snapshot.auth as Record<string, Auth.Info>),
    set: () => boundaryViolation("Auth", "set"),
    remove: () => boundaryViolation("Auth", "remove"),
  }
  return Layer.succeed(Auth.Service, auth)
}

/** Immutable snapshot-backed ModelsDev: catalog refresh is hub-only. */
export function snapshotModelsDevLayer(snapshot: WorkerSnapshot): Layer.Layer<ModelsDev.Service> {
  const modelsDev: ModelsDev.Interface = {
    get: () => Effect.succeed(snapshot.modelsDev as Record<string, ModelsDev.Provider>),
    refresh: () => boundaryViolation("ModelsDev", "refresh"),
  }
  return Layer.succeed(ModelsDev.Service, modelsDev)
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
  const mcp: MCP.Interface = {
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
    add: () => boundaryViolation("MCP", "add"),
    connect: () => boundaryViolation("MCP", "connect"),
    disconnect: () => boundaryViolation("MCP", "disconnect"),
    startAuth: () => boundaryViolation("MCP", "startAuth"),
    authenticate: () => boundaryViolation("MCP", "authenticate"),
    finishAuth: () => boundaryViolation("MCP", "finishAuth"),
    removeAuth: () => boundaryViolation("MCP", "removeAuth"),
  }
  return Layer.succeed(MCP.Service, mcp)
}

/** The LSP boundary: a worker owns no language servers, so every read is empty. */
export function lspBoundaryLayer(): Layer.Layer<LSP.Service> {
  const lsp: LSP.Interface = {
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
  }
  return Layer.succeed(LSP.Service, lsp)
}

/** The MCP auth boundary: no stored tokens; persistence is hub-owned. */
export function mcpAuthBoundaryLayer(): Layer.Layer<McpAuth.Service> {
  const auth: McpAuth.Interface = {
    all: () => Effect.succeed({}),
    get: () => Effect.succeed(undefined),
    getForUrl: () => Effect.succeed(undefined),
    getOAuthState: () => Effect.succeed(undefined),
    set: () => boundaryViolation("McpAuth", "set"),
    remove: () => boundaryViolation("McpAuth", "remove"),
    updateTokens: () => boundaryViolation("McpAuth", "updateTokens"),
    updateClientInfo: () => boundaryViolation("McpAuth", "updateClientInfo"),
    updateCodeVerifier: () => boundaryViolation("McpAuth", "updateCodeVerifier"),
    clearCodeVerifier: () => boundaryViolation("McpAuth", "clearCodeVerifier"),
    updateOAuthState: () => boundaryViolation("McpAuth", "updateOAuthState"),
    clearOAuthState: () => boundaryViolation("McpAuth", "clearOAuthState"),
  }
  return Layer.succeed(McpAuth.Service, auth)
}

/** The Npm boundary: a worker never installs packages; resolution is hub-only. */
export function npmBoundaryLayer(): Layer.Layer<Npm.Service> {
  const npm: Npm.Interface = {
    which: () => Effect.succeed(undefined),
    add: () => boundaryViolation("Npm", "add"),
    install: () => boundaryViolation("Npm", "install"),
  }
  return Layer.succeed(Npm.Service, npm)
}

/** The Account boundary: read-only; a worker has no account authority. */
export function accountBoundaryLayer(): Layer.Layer<Account.Service> {
  const account: Account.Interface = {
    active: () => Effect.succeed(Option.none()),
    activeOrg: () => Effect.succeed(Option.none()),
    list: () => Effect.succeed([]),
    orgsByAccount: () => Effect.succeed([]),
    orgs: () => Effect.succeed([]),
    config: () => Effect.succeed(Option.none()),
    token: () => Effect.succeed(Option.none()),
    remove: () => boundaryViolation("Account", "remove"),
    use: () => boundaryViolation("Account", "use"),
    login: () => boundaryViolation("Account", "login"),
    poll: () => boundaryViolation("Account", "poll"),
  }
  return Layer.succeed(Account.Service, account)
}

/** The AccountRepo boundary: read-only; persistence is hub-owned. */
export function accountRepoBoundaryLayer(): Layer.Layer<AccountRepo.Service> {
  const repo: AccountRepo.Interface = {
    active: () => Effect.succeed(Option.none()),
    list: () => Effect.succeed([]),
    getRow: () => Effect.succeed(Option.none()),
    remove: () => boundaryViolation("AccountRepo", "remove"),
    use: () => boundaryViolation("AccountRepo", "use"),
    persistToken: () => boundaryViolation("AccountRepo", "persistToken"),
    persistAccount: () => boundaryViolation("AccountRepo", "persistAccount"),
  }
  return Layer.succeed(AccountRepo.Service, repo)
}

/** The Storage boundary: a worker owns no durable store; only `list` is empty. */
export function storageBoundaryLayer(): Layer.Layer<Storage.Service> {
  const storage: Storage.Interface = {
    list: () => Effect.succeed([]),
    read: () => boundaryViolation("Storage", "read"),
    update: () => boundaryViolation("Storage", "update"),
    write: () => boundaryViolation("Storage", "write"),
    remove: () => boundaryViolation("Storage", "remove"),
  }
  return Layer.succeed(Storage.Service, storage)
}

/** The Git boundary: a worker owns no checkout; every operation is process-backed. */
export function gitBoundaryLayer(): Layer.Layer<Git.Service> {
  const git: Git.Interface = {
    run: () => boundaryViolation("Git", "run"),
    branch: () => boundaryViolation("Git", "branch"),
    prefix: () => boundaryViolation("Git", "prefix"),
    defaultBranch: () => boundaryViolation("Git", "defaultBranch"),
    hasHead: () => boundaryViolation("Git", "hasHead"),
    mergeBase: () => boundaryViolation("Git", "mergeBase"),
    show: () => boundaryViolation("Git", "show"),
    status: () => boundaryViolation("Git", "status"),
    diff: () => boundaryViolation("Git", "diff"),
    stats: () => boundaryViolation("Git", "stats"),
    patch: () => boundaryViolation("Git", "patch"),
    patchAll: () => boundaryViolation("Git", "patchAll"),
    patchUntracked: () => boundaryViolation("Git", "patchUntracked"),
    statUntracked: () => boundaryViolation("Git", "statUntracked"),
    applyPatch: () => boundaryViolation("Git", "applyPatch"),
  }
  return Layer.succeed(Git.Service, git)
}

/**
 * The AppProcess boundary: a worker owns no OS child-process authority, so
 * every operation fails loud. `spawn`/`run` are Effects; the two stream forms
 * use `Stream.die` to fail in their own stream channel.
 */
export function appProcessBoundaryLayer(): Layer.Layer<AppProcess.Service> {
  const unsupportedStream = (member: string): Stream.Stream<never, never> =>
    Stream.die(new SessionLayerBoundaryError("AppProcess", member))
  const appProcess: AppProcess.Interface = {
    spawn: () => boundaryViolation("AppProcess", "spawn"),
    exitCode: () => boundaryViolation("AppProcess", "exitCode"),
    streamString: () => unsupportedStream("streamString"),
    streamLines: () => unsupportedStream("streamLines"),
    lines: () => boundaryViolation("AppProcess", "lines"),
    string: () => boundaryViolation("AppProcess", "string"),
    run: () => boundaryViolation("AppProcess", "run"),
    runStream: () => unsupportedStream("runStream"),
  }
  return Layer.succeed(AppProcess.Service, appProcess)
}

/** The ripgrep binary boundary: a worker never downloads or locates the binary. */
export function ripgrepBinaryBoundaryLayer(): Layer.Layer<RipgrepBinary.Service> {
  return Layer.succeed(RipgrepBinary.Service, {
    filepath: boundaryViolation("RipgrepBinary", "filepath"),
  })
}

/** The ripgrep boundary: a worker owns no search process; every search fails loud. */
export function ripgrepBoundaryLayer(): Layer.Layer<Ripgrep.Service> {
  return Layer.succeed(Ripgrep.Service, {
    find: () => boundaryViolation("Ripgrep", "find"),
    glob: () => boundaryViolation("Ripgrep", "glob"),
    grep: () => boundaryViolation("Ripgrep", "grep"),
  })
}

/** The MCP browser boundary: a worker never opens a browser. */
export function mcpBrowserBoundaryLayer(): Layer.Layer<McpBrowser.Service> {
  return Layer.succeed(McpBrowser.Service, {
    open: () => boundaryViolation("McpBrowser", "open"),
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
    [AppProcess.node, appProcessBoundaryLayer()],
    [RipgrepBinary.node, ripgrepBinaryBoundaryLayer()],
    [Ripgrep.node, ripgrepBoundaryLayer()],
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

/** Keep only string-valued header entries from a serialized provider option. */
function stringHeaders(value: unknown): Record<string, string> {
  if (value === null || typeof value !== "object") return {}
  const headers: Record<string, string> = {}
  for (const [key, header] of Object.entries(value as Record<string, unknown>)) {
    if (typeof header === "string") headers[key] = header
  }
  return headers
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
function workerTimeoutFetch(options: Record<string, any>): typeof fetch {
  const chunkTimeout = options["chunkTimeout"] ?? 300_000
  const headerTimeout = options["headerTimeout"] ?? 300_000
  const timeout = options["timeout"]
  return (async (input: any, init?: any) => {
    const opts = init ?? {}
    const headerTimeoutMs = headerTimeout === false ? undefined : headerTimeout
    const chunkController = typeof chunkTimeout === "number" && chunkTimeout > 0 ? new AbortController() : undefined
    const headerController = typeof headerTimeoutMs === "number" ? timeoutController(headerTimeoutMs) : undefined
    const signals: AbortSignal[] = []
    if (opts.signal) signals.push(opts.signal)
    if (chunkController) signals.push(chunkController.signal)
    if (headerController) signals.push(headerController.signal)
    if (timeout !== undefined && timeout !== null && timeout !== false) signals.push(AbortSignal.timeout(timeout))
    const signal = signals.length === 0 ? undefined : signals.length === 1 ? signals[0] : AbortSignal.any(signals)
    const response = await fetch(input, {
      ...opts,
      ...(signal === undefined ? {} : { signal }),
      // @ts-ignore Bun-specific: disable Bun's own request timeout.
      timeout: false,
    }).finally(() => headerController?.clear())
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
 * The boundary `Plugin` for a worker: the hub owns plugin *loading* (decision
 * 70), but the model-facing hooks run in the isolate. Without a host the worker
 * uses this no-op pass-through; when a worker-hosted {@link WorkerPluginHost} is
 * supplied, {@link workerPluginLayer} runs its `chat.params` / `chat.headers`
 * (and tool) hooks so plugin-set sampling/headers match flag-off.
 */
function noopPluginLayer(): Layer.Layer<Plugin.Service> {
  const plugin = {
    trigger: (_name: string, _input: unknown, output: unknown) => Effect.succeed(output),
    list: () => Effect.succeed([]),
    init: () => Effect.void,
  } as unknown as Plugin.Interface
  return Layer.succeed(Plugin.Service, plugin)
}

/** Dispatch one `Plugin.trigger` to the matching worker-hosted hook. */
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
    default:
      return Promise.resolve(output)
  }
}

/**
 * The worker `Plugin` over a worker-hosted hook host: `chat.params` /
 * `chat.headers` (and the tool hooks) run in the isolate, so plugin-set
 * sampling/headers are applied to the worker's outgoing request exactly as
 * flag-off. Unknown triggers pass through unchanged.
 */
function workerPluginLayer(host: WorkerPluginHost): Layer.Layer<Plugin.Service> {
  const plugin = {
    trigger: (name: string, input: unknown, output: unknown) =>
      Effect.promise(() => runWorkerPluginHook(host, name, input, output)),
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
  return [Plugin.node, plugins === undefined ? noopPluginLayer() : workerPluginLayer(plugins)]
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
 * The worker-local `LLM` service: the real `LLM` node composed over the reduced
 * provider, the snapshot Config/Auth/ModelsDev and the hub-proxy authorities.
 * This keeps the provider stream inside the isolate (design decision 20).
 *
 * `EventV2Bridge` / `LLMClient` are boundary services on the hub; the worker only
 * needs them at LLM build time (`Layer.mock` provides the service without their
 * hub-side behaviour, which the session-worker does not exercise for a single
 * reduced turn). `Plugin` is a worker-safe replacement here (host when supplied,
 * no-op otherwise) because this sub-graph is compiled standalone by callers such
 * as the reduced pipeline; the full worker graph additionally replaces
 * `Plugin.node` at the top level ({@link buildSessionWorkerLayer}).
 */
export function reducedLlmLayer(
  snapshot: WorkerSnapshot,
  config: WorkerModelConfig,
  hubProxies?: HubProxyClients,
  plugins?: WorkerPluginHost,
): Layer.Layer<LLM.Service> {
  const replacements: LayerNode.Replacements = [
    [Auth.node, snapshotAuthLayer(snapshot)],
    [Config.node, snapshotConfigLayer(snapshot)],
    [Provider.node, reducedModelProviderLayer(config)],
    workerPluginReplacement(plugins),
    [
      Permission.node,
      hubProxies === undefined ? Layer.mock(Permission.Service, {}) : permissionProxyLayer(hubProxies.permission),
    ],
    [EventV2Bridge.node, Layer.mock(EventV2Bridge.Service, {})],
    [llmClient, Layer.mock(LLMClient.Service, {})],
    [RuntimeFlags.node, Layer.succeed(RuntimeFlags.Service, snapshot.flags)],
  ]
  return AppNodeBuilderV1.build(LLM.node, replacements)
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
  readonly nodes?: LayerNode.Node<unknown, unknown, any>
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
  /** Extra replacements, e.g. the real LLM-backed `ModelClient` in production. */
  readonly replacements?: LayerNode.Replacements
}

export interface SessionWorkerLayer {
  readonly root: LayerNode.Node<unknown, unknown, any>
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
  return { root, replacements, layer }
}

/** Build a `ManagedRuntime`-style runner for the reduced layer. */
export function createSessionWorkerRuntime(options: SessionLayerOptions): Layer.Layer<unknown, unknown> {
  return buildSessionWorkerLayer(options).layer
}

export * as SessionWorkerLayer from "./layer"
