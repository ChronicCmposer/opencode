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

import { Context, Effect, Layer } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { AppNodeBuilderV1 } from "@/effect/app-node-builder-v1"
import type { ConfigV1 } from "@opencode-ai/core/v1/config/config"
import { emptyConsoleState } from "@opencode-ai/core/v1/config/console-state"
import { ModelsDev } from "@opencode-ai/core/models-dev"

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
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { Database } from "@opencode-ai/core/database/database"
import { MCP } from "@/mcp"
import { LSP } from "@/lsp/lsp"
import { McpAuth } from "@/mcp/auth"
import { Npm } from "@opencode-ai/core/npm"
import { Account } from "@/account/account"
import { AccountRepo } from "@/account/repo"

import {
  BACKGROUND_JOB_CLIENT_KEY,
  BROKER_CLIENT_KEY,
  EVENT_EGRESS_CLIENT_KEY,
  WRITER_CLIENT_KEY,
  assertSessionLayer,
} from "./node-table"

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
  readonly emit: (event: { id: string; type: string; data: unknown }) => void
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
            data: { text: completion.text, epoch: snapshot.epoch },
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
 * Boundary replacements every session layer needs. `Database` and
 * `SessionProjector` are replaced so the worker never opens SQLite nor runs
 * durable projectors; MCP/LSP are reached through the broker client instead.
 * The real writer-backed Database replacement lands with Phase 7.
 */
export function boundaryReplacements(snapshot: WorkerSnapshot): LayerNode.Replacements {
  return [
    [Config.node, snapshotConfigLayer(snapshot)],
    [Auth.node, snapshotAuthLayer(snapshot)],
    [ModelsDev.node, snapshotModelsDevLayer(snapshot)],
    [Database.node, databaseBoundaryLayer()],
    [SessionProjector.node, Layer.empty],
    [MCP.node, Layer.empty],
    [LSP.node, Layer.empty],
    // Hub-owned / broker-owned boundaries that are not wired into a worker yet.
    // `Layer.empty` is sufficient for node-table validation; the real client
    // replacements arrive with Phases 7 (writer) and 10 (broker).
    [McpAuth.node, Layer.empty],
    [Npm.node, Layer.empty],
    [Account.node, Layer.empty],
    [AccountRepo.node, Layer.empty],
  ]
}

// ---------------------------------------------------------------------------
// Groups + builder
// ---------------------------------------------------------------------------

/** The declared session-role allowlist group (real nodes). */
export const SESSION_NODES = LayerNode.group([
  Session.node,
  SessionPrompt.node,
  SessionProcessor.node,
  SessionRunState.node,
  LLM.node,
  Plugin.node,
  ToolRegistry.node,
  Config.node,
  Auth.node,
  Provider.node,
  Agent.node,
  Skill.node,
  Discovery.node,
  Question.node,
  Permission.node,
  Todo.node,
  Truncate.node,
  Format.node,
  RuntimeFlags.node,
  WriterClientNode,
  EventEgressClientNode,
  BrokerClientNode,
  BackgroundJobClientNode,
])

/**
 * The subset that is runnable today without the Phase 7/10 client backends:
 * the pipeline, runtime flags and the four client nodes. `ModelClient` is the
 * injectable model seam, so the boot test can point it at the mock provider.
 */
export const RUNNABLE_SESSION_NODES = LayerNode.group([
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
  /** Node group; defaults to the runnable subset (`RUNNABLE_SESSION_NODES`). */
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
  const root = options.nodes ?? RUNNABLE_SESSION_NODES
  const replacements: LayerNode.Replacements = [
    [WriterClientNode, writerClient.layer(options.clients.writer)],
    [EventEgressClientNode, eventEgressClient.layer(options.clients.egress)],
    [BrokerClientNode, brokerClient.layer(options.clients.broker)],
    [BackgroundJobClientNode, backgroundJobClient.layer(options.clients.backgroundJob)],
    [WorkerSnapshotNode, Layer.succeed(WorkerSnapshotService, options.snapshot)],
    [WorkerEnvNode, Layer.succeed(WorkerEnvService, options.env)],
    [ModelClientNode, options.modelClient],
    ...boundaryReplacements(options.snapshot),
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
