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
//   * boundary replacement layers for the excluded nodes (Database, projector,
//     MCP/LSP, Npm, ModelsDev, …), which production supplies from Phases 7/10.
//
// `buildSessionWorkerLayer` composes the graph and then asserts it against the
// node table, so a regression that reintroduces `Database`/`SessionProjector`
// into the session graph fails at build time, not at run time.

import { Context, Effect, Layer, Stream } from "effect"
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

// ---------------------------------------------------------------------------
// Client nodes — the worker-side handles to the writer / egress / hub.
// ---------------------------------------------------------------------------

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

  return {
    call(method, args) {
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
    },
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
    reply: (input: { requestID: Parameters<Question.Interface["reject"]>[0]; answers: ReadonlyArray<Question.Answer> }) =>
      hubProxyCall<void>(client, "reply", [input]),
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
export class WorkerEnvService extends Context.Service<WorkerEnvService, WorkerEnv>()(
  "@opencode/session-worker/Env",
) {}

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

/**
 * The worker-side Database surface. Workers hold no SQLite connection: every
 * statement is forwarded to the hub-owned writer over the writer client port
 * (plan decision 24). The writer speaks `query(method, sql, params)` with
 * `method` one of `all` / `values` / `run` / `exec`.
 */
export interface WorkerDatabaseHandle {
  readonly run: (sql: string, params?: readonly unknown[]) => Promise<unknown>
  readonly all: (sql: string, params?: readonly unknown[]) => Promise<unknown>
  readonly get: (sql: string, params?: readonly unknown[]) => Promise<unknown>
  readonly values: (sql: string, params?: readonly unknown[]) => Promise<unknown>
  readonly exec: (sql: string, params?: readonly unknown[]) => Promise<unknown>
}

/**
 * A `Database` replacement that routes reads and writes through the writer
 * client instead of throwing. `Database.db` is typed as the drizzle shape in
 * the app graph; here it carries the reduced {@link WorkerDatabaseHandle}
 * surface (the SQL RPC the writer protocol exposes).
 */
export function writerDatabaseLayer(writer: WriterClientHandle): Layer.Layer<Database.Service> {
  const statement = (method: string) => (sql: string, params?: readonly unknown[]) =>
    writer.query(method, sql, params)
  const db: WorkerDatabaseHandle = {
    run: statement("run"),
    all: statement("all"),
    get: statement("values"),
    values: statement("values"),
    exec: statement("exec"),
  }
  return Layer.succeed(Database.Service, { db } as unknown as Database.Interface)
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
 * Boundary replacements every session layer needs. `Database` and
 * `SessionProjector` are replaced so the worker never opens SQLite nor runs
 * durable projectors; MCP/LSP are reached through the broker client instead.
 * When a `writer` client is supplied, `Database` is a real writer-proxy (plan
 * decision 24); otherwise it stays the loud boundary stub.
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
    // Effect-only node: `session-projector` provides no service, so an empty
    // layer is the correct (and complete) replacement.
    [SessionProjector.node, Layer.empty],
    // Hub-owned / broker-owned authorities the reduced graph yields. A mock
    // provides the service shape so the graph BOOTS (an `Layer.empty` provides
    // nothing, so `SessionPrompt`'s `yield* MCP.Service`/`LSP.Service` would
    // fail at build); a worker that reaches one fails loud at call time.
    [MCP.node, Layer.mock(MCP.Service, {})],
    [LSP.node, Layer.mock(LSP.Service, {})],
    [McpAuth.node, Layer.mock(McpAuth.Service, {})],
    [Npm.node, Layer.mock(Npm.Service, {})],
    [Account.node, Layer.mock(Account.Service, {})],
    [AccountRepo.node, Layer.mock(AccountRepo.Service, {})],
    // Residual hub/writer/broker/infra boundaries (see `SESSION_EXCLUDED`).
    [Storage.node, Layer.mock(Storage.Service, {})],
    [Git.node, Layer.mock(Git.Service, {})],
    [BackgroundJob.node, backgroundJob ?? localBackgroundJobLayer()],
    // `Global` is a plain record of paths (no I/O); the real value is safe and
    // lets any read of `Global.Path`/`Service` work rather than return undefined.
    [Global.node, Layer.succeed(Global.Service, Global.make())],
    [AppProcess.node, Layer.mock(AppProcess.Service, {})],
    [RipgrepBinary.node, Layer.mock(RipgrepBinary.Service, {})],
    [Ripgrep.node, Layer.mock(Ripgrep.Service, {})],
    [McpBrowser.node, Layer.mock(McpBrowser.Service, {})],
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
export interface WorkerModelConfig {
  readonly providerID: string
  readonly modelID: string
  readonly apiId: string
  readonly npm: string
  readonly baseUrl: string
  readonly apiKey: string
  readonly headers: Readonly<Record<string, string>>
}

const allModalities = { text: true, audio: false, image: false, video: false, pdf: false }

function reducedProviderInfo(config: WorkerModelConfig, model: Provider.Model): Provider.Info {
  return {
    id: ProviderV2.ID.make(config.providerID),
    name: config.providerID,
    source: "config",
    env: [],
    key: config.apiKey,
    options: {},
    models: { [config.modelID]: model },
  }
}

function reducedProviderModel(config: WorkerModelConfig): Provider.Model {
  return {
    id: ModelV2.ID.make(config.modelID),
    providerID: ProviderV2.ID.make(config.providerID),
    api: { id: config.apiId, url: config.baseUrl, npm: config.npm },
    name: config.modelID,
    capabilities: {
      temperature: true,
      reasoning: false,
      attachment: false,
      toolcall: true,
      input: allModalities,
      output: allModalities,
      interleaved: false,
    },
    cost: { input: 0, output: 0, cache: { read: 0, write: 0 } },
    limit: { context: 128_000, output: 4096 },
    status: "active",
    options: {},
    headers: { ...config.headers },
    release_date: "",
  }
}

/**
 * The reduced in-worker `Provider`: an OpenAI-compatible client over the
 * serialized `{baseUrl, apiKey, model}` bootstrap. `getLanguage` returns a real
 * AI-SDK language model, so the worker makes the HTTP call itself.
 */
export function reducedModelProviderLayer(config: WorkerModelConfig): Layer.Layer<Provider.Service> {
  const sdk = createOpenAICompatible({
    name: config.providerID,
    baseURL: config.baseUrl,
    apiKey: config.apiKey,
    headers: { ...config.headers },
  })
  const model = reducedProviderModel(config)
  const info = reducedProviderInfo(config, model)
  const providerID = ProviderV2.ID.make(config.providerID)
  const modelID = ModelV2.ID.make(config.modelID)
  // The reduced provider serves exactly one model; any other request is a
  // programming error surfaced loudly rather than silently mis-served.
  const notFound = (requestedID: ModelV2.ID) => new Provider.ModelNotFoundError({ providerID, modelID: requestedID })
  const service: Provider.Interface = {
    list: () => Effect.succeed({ [info.id]: info }),
    getProvider: () => Effect.succeed(info),
    getModel: (requestedProvider, requestedModel) =>
      requestedProvider === config.providerID && requestedModel === config.modelID
        ? Effect.succeed(model)
        : Effect.fail(notFound(requestedModel)),
    getLanguage: (requested) =>
      requested.providerID === config.providerID && requested.api.id === config.apiId
        ? Effect.succeed(sdk.chatModel(config.apiId) as unknown as LanguageModelV3)
        : Effect.fail(notFound(requested.id)),
    closest: () => Effect.succeed(undefined),
    getSmallModel: () => Effect.succeed(undefined),
    defaultModel: () => Effect.succeed({ providerID: info.id, modelID: model.id }),
  }
  return Layer.succeed(Provider.Service, service)
}

/**
 * The boundary `Plugin` for a worker: the hub owns plugin adapters (decision
 * 70). A worker turn needs only the no-op trigger pass-through, so the reduced
 * LLM runs without the hub's plugin registry.
 */
function noopPluginLayer(): Layer.Layer<Plugin.Service> {
  const plugin = {
    trigger: (_name: string, _input: unknown, output: unknown) => Effect.succeed(output),
    list: () => Effect.succeed([]),
    init: () => Effect.void,
  } as unknown as Plugin.Interface
  return Layer.succeed(Plugin.Service, plugin)
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
 * `Plugin` / `EventV2Bridge` / `LLMClient` are boundary services on the hub;
 * the worker only needs them at LLM build time (`Layer.mock` provides the
 * service without their hub-side behaviour, which the session-worker does not
 * exercise for a single reduced turn).
 */
export function reducedLlmLayer(
  snapshot: WorkerSnapshot,
  config: WorkerModelConfig,
  hubProxies?: HubProxyClients,
): Layer.Layer<LLM.Service> {
  const replacements: LayerNode.Replacements = [
    [Auth.node, snapshotAuthLayer(snapshot)],
    [Config.node, snapshotConfigLayer(snapshot)],
    [Provider.node, reducedModelProviderLayer(config)],
    [Plugin.node, noopPluginLayer()],
    [Permission.node, hubProxies === undefined ? Layer.mock(Permission.Service, {}) : permissionProxyLayer(hubProxies.permission)],
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
    ...boundaryReplacements(options.snapshot, options.clients.writer, options.backgroundJob),
    ...(options.hubProxies === undefined ? [] : hubProxyReplacements(options.hubProxies)),
    ...(options.replacements ?? []),
  ]

  assertSessionLayer(root, replacements)
  const layer = AppNodeBuilderV1.build(root, replacements)
  return { root, replacements, layer }
}

/** Build a `ManagedRuntime`-style runner for the reduced layer. */
export function createSessionWorkerRuntime(options: SessionLayerOptions): Layer.Layer<unknown, unknown> {
  return buildSessionWorkerLayer(options).layer
}

export * as SessionWorkerLayer from "./layer"
