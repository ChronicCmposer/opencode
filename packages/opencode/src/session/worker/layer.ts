// Reduced session-worker AppLayer.
//
// The hub composes ~47 nodes (`src/effect/app-runtime.ts`). A session worker
// must compose a *reduced* graph: the session pipeline, its model-facing
// services and one WAL connection per worker live in the worker, while
// migration, schema state, OS children and process-global state stay on the hub
// (see `node-table.ts`).
//
// This module builds that reduced layer from:
//   * the allowlisted real nodes (default group `SESSION_NODES`);
//   * two client nodes (`WriterClient` / `EventEgressClient`) that reach the
//     worker's WAL-backed writer seam and the egress authority;
//   * an immutable per-worker `WorkerSnapshot` + `WorkerEnv`;
//   * boundary replacement layers for the excluded nodes (Database, MCP/LSP, Npm,
//     ModelsDev, …). `Database` is replaced by the per-worker WAL connection
//     (`WorkerDbNode`); `SessionProjector` is NOT replaced: it runs in-worker so
//     durable events materialize the session/message/part rows the loop reads
//     back.
//
// `buildSessionWorkerLayer` composes the graph and then asserts it against the
// node table, so a regression that reintroduces an excluded node such as
// `Database` into the session graph fails at build time, not at run time.

import { Context, Effect, Layer, Option, Ref, Schema, Stream } from "effect"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import type { Acquirer, Connection } from "effect/unstable/sql/SqlConnection"
import { classifySqliteError, SqlError } from "effect/unstable/sql/SqlError"
import * as Statement from "effect/unstable/sql/Statement"
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
// Residual hub/writer-owned boundaries (node-table `SESSION_EXCLUDED`).
import { Storage } from "@/storage/storage"
import { Git } from "@/git"
import { BackgroundJob } from "@/background/job"
import { Global } from "@opencode-ai/core/global"
import { RipgrepBinary } from "@opencode-ai/core/ripgrep/binary"
import { LocationServiceMap } from "@opencode-ai/core/location-service-map"
import { BackgroundJob as CoreBackgroundJob } from "@opencode-ai/core/background-job"
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
  type WorkerPluginHost,
} from "./plugin-host"
import { WorkerDb, databaseLayer, type WorkerDatabase } from "./db"
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

// ---------------------------------------------------------------------------
// Client nodes — the worker-side handles to the writer / egress / hub.
// ---------------------------------------------------------------------------

/**
 * The transaction primitives the legacy writer handle exposes: a pinned
 * transaction is identified by an opaque handle, statements run against it, then
 * it commits or rolls back.
 *
 * The production per-worker path no longer uses these; they remain optional so
 * reduced test/degraded handles stay structurally assignable, and
 * {@link writerDatabaseLayer} turns a missing primitive into a loud failure
 * rather than a silent `undefined`.
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
  /** Forward a durable boundary event with its aggregate `(aggregateID, seq)`. */
  readonly emitDurable?: (event: EventV2Event, meta: { readonly aggregateID: string; readonly seq: number }) => void
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

export const WriterClientService = writerClient.Service
export const EventEgressClientService = eventEgressClient.Service

export const WriterClientNode = writerClient.node
export const EventEgressClientNode = eventEgressClient.node

/**
 * The per-worker database node. A worker graph contains exactly one, backed by
 * that worker's WAL connection; it replaces the writer-proxied `Database`.
 */
export const WorkerDbNode = LayerNode.unbound(WorkerDb, workerTags.values.worker)

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
 * Parse a durable commit into this build's definition: the aggregate it belongs
 * to and the definition version, or `undefined` when the commit's type names no
 * durable definition this build knows. The mesh frame carries the event verbatim
 * (`{ id, type, properties }`) with no aggregate of its own, so it is parsed from
 * the payload exactly as core's durable commit does: the definition's
 * `durable.aggregate` field names the payload property. An unknown type is
 * skipped rather than fatal — the commit is still republished live-only — so a
 * synthetic or version-skewed durable event never breaks the control port. A
 * KNOWN durable type whose payload omits the aggregate field is malformed and
 * fails loud.
 */
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

/**
 * Bridge one hub-delivered durable commit into the worker's in-isolate EventV2.
 * The commit already durably committed in the hub writer, so it is republished
 * LIVE-ONLY — never re-committed — crossing verbatim as `{ id, type, properties }`
 * (`properties` is the EventV2 `data` reference). Its committed aggregate is then
 * woken so the isolate's `durable()` subscribers re-read and observe it.
 * Synchronous because the mesh delivery runs on the synchronous control-port
 * handler.
 *
 * The republished payload keeps the committed position (`durable.aggregateID` /
 * `durable.seq`) the mesh delivered, stamped with the definition version from
 * this build's manifest, so the isolate's durable subscribers and the sync
 * envelope see the original position. A commit whose type this build does not
 * know has no version to stamp and crosses live-only.
 *
 * The payload is stamped with {@link DURABLE_WAKEUP_METADATA_KEY} so the
 * worker's own event bridge ({@link createWorkerEventBridge}) does not
 * re-forward it: the mesh delivers a worker its OWN durable commit back too, and
 * re-forwarding would make the hub publish that event a second time.
 */
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
  // The durable write already happened in the hub writer; the in-isolate
  // `durable()` subscribers only need waking so their aggregate stream re-reads
  // and observes it. A reduced handle without the seam, or a commit whose type
  // this build does not know, skips the wake.
  if (position === undefined) return
  wakeAggregate(events, position.aggregateID)
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
/** The EventV2 seams a durable wakeup needs; `durable` enables gap detection. */
type WakeupEvents = Pick<EventV2.Interface, "publishLive" | "wakeDurable"> &
  Partial<Pick<EventV2.Interface, "durable">>

/**
 * Read a database snapshot for one aggregate through the production durable
 * stream. `takeUntil` is what makes the read terminate: the snapshot cursor is a
 * commit that already exists in SQLite, so the scan stops at it instead of
 * waiting on the live tail.
 *
 * `after` is the last commit this isolate already observed. Reading only
 * `(after, seq]` is what keeps a real gap from re-scanning the whole aggregate
 * log; an empty range is answered without touching the stream at all.
 */
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

/**
 * Wire the worker's durable wakeups. A handle with no subscription surface, or
 * with no mesh control plane (the hub-less / degraded topology, e.g. the
 * in-process parity harness), cannot be woken durably: the bridge is simply not
 * wired and the returned disposer is a no-op. This is a legitimate topology,
 * never an error — only a DIRECT `subscribeDurable` call without a control port
 * fails loud.
 *
 * When the event service exposes `durable()`, each aggregate's delivered
 * sequence is ALSO tracked with the delta-resync machinery: a mesh gap (a commit
 * that never arrived, not just an overflow resync) produces a typed `Resync` and
 * reads the database snapshot at the last confirmed position. The wake then makes
 * the isolate's own `durable()` subscribers re-read the same log, so the session
 * heals without closing the control plane.
 */
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
      // The commit already crossed live in `onDurable`; this subscription exists
      // only to detect a gap and heal it from the snapshot.
      onDurable: () => {},
      // The wake heals the isolate's own `durable()` subscribers; the snapshot
      // read above is the DB observation that proves the position is readable.
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
    // A resync means the hub dropped a delivery (overflow or receiver gap). With
    // an exact position, wake that aggregate so its `durable()` stream re-reads
    // SQLite; without one, wake everything this worker has observed.
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
    yield* Effect.addFinalizer(() => Effect.sync(() => unsubscribeDurable()).pipe(Effect.andThen(bridge.dispose)))
    return WorkerEventBridge.of({ dispose: bridge.dispose })
  }),
)

export const WorkerEventBridgeNode = LayerNode.make({
  service: WorkerEventBridge,
  layer: eventBridgeLayer,
  deps: [EventV2Bridge.node, EventEgressClientNode],
})

// ---------------------------------------------------------------------------
// Hub-proxy nodes: `SessionStatus` / `Permission` /
// `Question` are hub-resident authorities. A worker reaches them with a
// blocking RPC over its control port; the proxy services satisfy the *real*
// hub service interfaces, so a session graph that depends on them composes
// unchanged. This is the generic hub-proxy wire the three nodes share.
// ---------------------------------------------------------------------------

// The wire itself lives in `hub-proxy-wire.ts` so neither the worker nor the hub
// imports the other. Re-exported here so existing consumers keep the same path.
export { createHubProxyClient, HubProxyRemoteError, serveHubProxy } from "./hub-proxy-wire"
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
 * Worker-side `SessionSpawnProxy` over the hub-proxy control port. `Session.spawn`
 * reads it optionally, so a worker forwards the spawn to the hub instead of
 * running the child on its own isolate.
 */
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

/**
 * Worker-side clear-revert authority. `revert` is hub-owned session state, so a
 * bound worker asks the hub to clear it over the control port instead of
 * publishing a worker-authored `session.updated` with `revert: null` (which the
 * projection policy refuses as an actual change to a hub column).
 */
export function clearRevertProxyLayer(client: HubProxyClient): Layer.Layer<SessionRevert.ClearRevertAuthority> {
  return Layer.succeed(SessionRevert.ClearRevertAuthority, {
    clear: (sessionID: SessionID) =>
      hubProxyCall<void>(client, "session.clearRevert", [{ sessionID }]).pipe(Effect.orDie),
  })
}

/**
 * The hub-proxy authorities as REPLACEMENTS, never as appended root nodes: the
 * authority services are already part of the declared graph, so replacing them by
 * name keeps exactly one node per service key and makes the proxy implementation
 * (not a duplicate root) the one that is built.
 */
export function hubProxyReplacements(clients: HubProxyClients): LayerNode.Replacements {
  return [
    [SessionStatusProxyNode, sessionStatusProxyLayer(clients.sessionStatus)],
    [PermissionProxyNode, permissionProxyLayer(clients.permission)],
    [QuestionProxyNode, questionProxyLayer(clients.question)],
  ]
}

/**
 * The hub proxies not already composed by {@link sharedBoundaryReplacements}.
 * `Permission` belongs to the shared snapshot/permission fragment both graphs
 * install, so only the status/question authorities remain here.
 */
function hubAuthorityReplacements(clients: HubProxyClients): LayerNode.Replacements {
  return hubProxyReplacements(clients).filter(([node]) => node.name !== Permission.node.name)
}

// ---------------------------------------------------------------------------
// Immutable per-worker snapshot + environment overlay.
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
          // the commit's aggregate `(aggregateID, seq)` and bypasses delta
          // coalescing. A committed publish always has a sequence; a null one is
          // a broken writer contract, never a silently non-durable event.
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

// ---------------------------------------------------------------------------
// Boundary replacement layers (production wiring is Phases 7/10).
// ---------------------------------------------------------------------------

/** Raised when worker code reaches a boundary owned by the hub / writer. */
export class SessionLayerBoundaryError extends Error {
  constructor(
    readonly boundary: string,
    readonly member: string,
  ) {
    super(`session worker boundary "${boundary}" cannot use "${member}": ${boundary} is hub/writer-owned`)
    this.name = "SessionLayerBoundaryError"
  }
}

const boundaryViolation = (boundary: string, member: string): Effect.Effect<never> =>
  Effect.die(new SessionLayerBoundaryError(boundary, member))

/**
 * Build a hub/writer-owned boundary service from the table of reads a
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

/** No per-worker connection was supplied: every access to it fails loud. */
export function workerDbBoundaryLayer(): Layer.Layer<WorkerDb> {
  return Layer.succeed(WorkerDb, failLoudService<WorkerDatabase>("WorkerDb"))
}

// ---------------------------------------------------------------------------
// Functional boundary layers for the hub-owned authorities.
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

type WriterSqlHandle = Required<WriterTransactionHandle> & { readonly query: WriterClientHandle["query"] }

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

type QueryMethod = "all" | "values" | "run" | "exec"

const writerSqlError =
  (operation: string) =>
  (cause: unknown): SqlError =>
    new SqlError({ reason: classifySqliteError(cause, { message: "writer statement failed", operation }) })

const transactionNotOpen = (operation: string) =>
  writerSqlError(operation)(new Error("statement executed outside an open writer transaction"))

function asRows(value: unknown): ReadonlyArray<Record<string, unknown>> {
  if (!Array.isArray(value)) throw new Error("writer returned a non-array for a row query")
  for (const row of value) {
    if (typeof row !== "object" || row === null || Array.isArray(row)) {
      throw new Error("writer returned a non-object row")
    }
  }
  return value as ReadonlyArray<Record<string, unknown>>
}

function asValues(value: unknown): ReadonlyArray<ReadonlyArray<unknown>> {
  if (!Array.isArray(value)) throw new Error("writer returned a non-array for a value query")
  for (const row of value) {
    if (!Array.isArray(row)) throw new Error("writer returned a non-array row for a value query")
  }
  return value as ReadonlyArray<ReadonlyArray<unknown>>
}

const writerQuery = <A>(
  writer: WriterSqlHandle,
  method: QueryMethod,
  sql: string,
  params: ReadonlyArray<unknown>,
  parse: (value: unknown) => A,
): Effect.Effect<A, SqlError> =>
  Effect.tryPromise({
    try: async () => parse(await writer.query(method, sql, params)),
    catch: writerSqlError("execute"),
  })

const writerTxQuery = <A>(
  writer: WriterSqlHandle,
  handle: string,
  method: QueryMethod,
  sql: string,
  params: ReadonlyArray<unknown>,
  parse: (value: unknown) => A,
): Effect.Effect<A, SqlError> =>
  Effect.tryPromise({
    try: async () => parse(await writer.txQuery(handle, method, sql, params)),
    catch: writerSqlError("execute"),
  })

const makeStatelessWriterConnection = (writer: WriterSqlHandle): Connection => ({
  execute: (sql, params, transformRows) => {
    const rows = writerQuery(writer, "all", sql, params, asRows)
    return transformRows ? Effect.map(rows, transformRows) : rows
  },
  executeRaw: (sql, params) => writerQuery(writer, "all", sql, params, asRows),
  executeValues: (sql, params) => writerQuery(writer, "values", sql, params, asValues),
  executeUnprepared: (sql, params, transformRows) => {
    const rows = writerQuery(writer, "all", sql, params, asRows)
    return transformRows ? Effect.map(rows, transformRows) : rows
  },
  executeStream: () => Stream.die("writer SqlClient does not support streaming"),
})

const makeTransactionWriterConnection = (writer: WriterSqlHandle, handle: Ref.Ref<string | undefined>): Connection => {
  const requireHandle = Effect.flatMap(Ref.get(handle), (tx) =>
    tx === undefined ? Effect.fail(transactionNotOpen("execute")) : Effect.succeed(tx),
  )
  const txRows = (method: QueryMethod, sql: string, params: ReadonlyArray<unknown>) =>
    Effect.flatMap(requireHandle, (tx) => writerTxQuery(writer, tx, method, sql, params, asRows))
  const txValues = (sql: string, params: ReadonlyArray<unknown>) =>
    Effect.flatMap(requireHandle, (tx) => writerTxQuery(writer, tx, "values", sql, params, asValues))

  return {
    execute: (sql, params, transformRows) => {
      const rows = txRows("all", sql, params)
      return transformRows ? Effect.map(rows, transformRows) : rows
    },
    executeRaw: (sql, params) => txRows("all", sql, params),
    executeValues: (sql, params) => txValues(sql, params),
    executeUnprepared: (sql, params, transformRows) =>
      Effect.gen(function* () {
        const tx = yield* Ref.get(handle)
        const statement = sql.trim().toLowerCase()
        if (statement.startsWith("begin")) {
          if (tx !== undefined) {
            return yield* Effect.fail(writerSqlError("begin")(new Error("writer transaction is already open")))
          }
          const opened = yield* Effect.tryPromise({ try: () => writer.begin(), catch: writerSqlError("begin") })
          yield* Ref.set(handle, opened)
          return []
        }
        if (statement.startsWith("commit")) {
          if (tx === undefined) return yield* Effect.fail(transactionNotOpen("commit"))
          yield* Effect.tryPromise({ try: () => writer.commit(tx), catch: writerSqlError("commit") })
          yield* Ref.set(handle, undefined)
          return []
        }
        if (statement.startsWith("rollback") && !statement.startsWith("rollback to savepoint")) {
          if (tx === undefined) return yield* Effect.fail(transactionNotOpen("rollback"))
          yield* Effect.tryPromise({ try: () => writer.rollback(tx), catch: writerSqlError("rollback") })
          yield* Ref.set(handle, undefined)
          return []
        }
        const rows = yield* txRows("all", sql, params)
        return transformRows === undefined ? rows : transformRows(rows)
      }),
    executeStream: () => Stream.die("writer SqlClient does not support streaming"),
  }
}

const acquireWriterTransaction = (writer: WriterSqlHandle): Acquirer =>
  Effect.gen(function* () {
    const handle = yield* Ref.make<string | undefined>(undefined)
    yield* Effect.addFinalizer(() =>
      Ref.get(handle).pipe(
        Effect.flatMap((tx) =>
          tx === undefined
            ? Effect.void
            : Effect.tryPromise({ try: () => writer.rollback(tx), catch: () => undefined }).pipe(Effect.ignore),
        ),
      ),
    )
    return makeTransactionWriterConnection(writer, handle)
  })

function writerSqlClientLayer(writer: WriterSqlHandle): Layer.Layer<SqlClient.SqlClient> {
  const client = SqlClient.make({
    acquirer: Effect.succeed(makeStatelessWriterConnection(writer)),
    compiler: Statement.makeCompilerSqlite(),
    transactionAcquirer: acquireWriterTransaction(writer),
    spanAttributes: [["db.system.name", "sqlite"]],
  })
  return Layer.effect(SqlClient.SqlClient, client).pipe(Layer.provide(Reactivity.layer))
}

/**
 * The worker `Database`, backed by the hub-owned writer (legacy path until the
 * worker opens its own connection). The production path is
 * {@link workerDatabaseLayer}, which installs the per-worker WAL connection.
 */
export function writerDatabaseLayer(writer: WriterClientHandle): Layer.Layer<Database.Service> {
  const makeDatabase = EffectDrizzleSqlite.makeWithDefaults()
  return Layer.effect(
    Database.Service,
    Effect.map(makeDatabase, (db): Database.Interface => ({ db })),
  ).pipe(Layer.provide(writerSqlClientLayer(writerSqlHandle(writer))))
}

/** The worker `Database` over its own WAL connection and table allowlist. */
export function workerDatabaseLayer(database: WorkerDatabase): Layer.Layer<Database.Service> {
  return databaseLayer(database)
}

/** The Database source a worker was given: its own connection or the legacy writer. */
export type WorkerDatabaseSource = WriterClientHandle | WorkerDatabase

/** Distinguish the per-worker connection from the legacy writer handle at the seam. */
function isWorkerDatabase(source: WorkerDatabaseSource): source is WorkerDatabase {
  return "identity" in source && "connection" in source && "filename" in source
}

/** Build the `Database` replacement for whichever source the worker was given. */
export function databaseSourceLayer(source: WorkerDatabaseSource): Layer.Layer<Database.Service> {
  return isWorkerDatabase(source) ? workerDatabaseLayer(source) : writerDatabaseLayer(source)
}

/**
 * The process-local `BackgroundJob` registry. Per-session workers run their own
 * background jobs; cascade cancellation reaches their child sessions through
 * `Session.children`, so no hub-side job registry is needed.
 */
export function localBackgroundJobLayer(): Layer.Layer<BackgroundJob.Service> {
  return Layer.effect(BackgroundJob.Service, CoreBackgroundJob.make)
}

/**
 * The snapshot-backed and hub-authority fragments BOTH the full worker graph and
 * the reduced in-worker `LLM` sub-graph compose. Single-sourcing them means a
 * change to `Auth`/`Config`/`RuntimeFlags`/`Plugin`/`Permission` wiring can never
 * land in one view only.
 *
 * `Permission` is a hub-resident authority: route it over the hub-proxy control
 * port when the worker has one, otherwise fail loud (never a silent mock, which
 * would let a denied tool run in a degraded isolate).
 */
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
    // The hub resolved every runtime flag (including any per-layer override) and
    // froze it into the snapshot; the worker serves that value rather than
    // re-deriving from its own `process.env`.
    [RuntimeFlags.node, snapshotRuntimeFlagsLayer(snapshot)],
    // The worker owns `Plugin`: the hub loads plugins (decision 70), but the
    // model-facing hooks run here. Replacing the real node for the whole graph
    // means the global `Plugin` layer is never built in the isolate.
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

/**
 * The columns a `session.updated` publisher declared it actually changed.
 * `undefined` means an older/undeltaed publisher, which keeps the conservative
 * full-image comparison.
 */
function emittedSessionColumns(event: SessionProjector.ProjectedEvent): ReadonlySet<string> | undefined {
  const value = event.metadata?.[SESSION_PATCH_METADATA_KEY]
  if (!Array.isArray(value)) return undefined
  return new Set(value.filter((column): column is string => typeof column === "string"))
}

/**
 * Keep the projector's full-image write safe for a delta patch: every
 * hub-owned column the patch did not emit is restored from the authoritative
 * row, so a stale worker snapshot can never resurrect billing/share/revert or
 * identity state that the hub advanced concurrently.
 */
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

/**
 * The authoritative session row read synchronously on the worker's connection.
 * `authorize` is called before `SessionProjector` builds its write effect, so
 * the delta normalization below must read the row at call time to be seen by
 * that effect. The worker connection is inside the durable-commit transaction,
 * so this read observes every committed hub write.
 */
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

/**
 * The projection authority of a bound worker. The durable aggregate fence only
 * checks `data.sessionID`; this policy also pins every nested identity to it,
 * rejects worker-authored session-root lifecycle, and refuses immutable session
 * columns plus hub-owned `metadata.depth`. It runs before any projector write,
 * inside the same durable-commit transaction.
 *
 * A delta patch (`metadata.sessionPatch`) is normalized synchronously at call
 * time: every hub-owned column it did not emit is replaced with the
 * authoritative row before `SessionProjector` builds its full-image write.
 */
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

        // Relocation is a control-plane operation: it rewrites the immutable
        // directory/workspace identity and must not originate in a worker.
        if (event.type === SessionEvent.Moved.type) {
          return yield* rejectProjection(event, event.data.sessionID, "session location is hub authority")
        }

        // Revert state is hub authority in every shape, not only the
        // `Session.Info` image: the dedicated revert branches write the same
        // hub-owned `revert` column without consulting the column whitelist.
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
          // A missing row makes the projector update a no-op; it can never mint
          // the root, because Created is rejected above.
          if (current === undefined) return
          // A delta publisher names exactly the columns it changed. Judge (and
          // write) only those, so a stale full image cannot be rejected for, or
          // overwrite, hub-owned columns it merely echoed.
          const emitted = emittedSessionColumns(event)
          const isEmitted = (column: string) => emitted === undefined || emitted.has(column)
          // Mirror the projector's write semantics: an absent optional field is
          // omitted from the UPDATE (preserved), while `workspaceID` is written
          // as null when absent.
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
          // The projector writes the whole `sessionRow(info)` image (undefined
          // fields are omitted by `SET`). Compare the columns this event emitted
          // with the row the policy already loaded and allow only worker-owned
          // columns to change; a repeated hub value is a no-op, a changed one is
          // authority.
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

/**
 * Boundary replacements every session layer needs. `Database` is replaced by
 * the per-worker WAL connection when one is supplied (otherwise the legacy
 * writer proxy, or a fail-loud boundary). MCP/LSP are hub-owned and stay
 * excluded from the worker graph. `SessionProjector` is intentionally NOT replaced: it runs in-worker,
 * registering `EventV2.project` subscribers so durable session events materialize
 * the `session`/`message`/`part` rows, inside the writer-pinned transaction.
 *
 * Every other hub/writer-owned node reachable from the real
 * `SessionPrompt` graph is replaced by a FUNCTIONAL boundary layer, not
 * `Layer.empty`: the graph must boot (a dependency on an absent service is a
 * build failure), and a worker that reaches a hub-owned authority fails loud at
 * call time. `BackgroundJob` uses the hub client when supplied; otherwise the
 * process-local registry.
 */
export function boundaryReplacements(
  snapshot: WorkerSnapshot,
  database?: WorkerDatabaseSource,
  backgroundJob?: Layer.Layer<BackgroundJob.Service>,
  boundaries: SharedBoundaryOptions = {},
): LayerNode.Replacements {
  return [
    ...sharedBoundaryReplacements(snapshot, boundaries),
    [ModelsDev.node, snapshotModelsDevLayer(snapshot)],
    [Database.node, database === undefined ? databaseBoundaryLayer() : databaseSourceLayer(database)],
    // `SessionProjector` deliberately has NO boundary replacement: it runs IN the
    // worker. `Session.updateMessage`/`updatePart` publish durable events, and the
    // projector materializes the `session`/`message`/`part` rows the loop reads
    // back. Its `EventV2.project` subscribers run inside `EventV2`'s durable-commit
    // `db.transaction`, which the writer-backed Database routes through the writer
    // tx primitives — so projection is atomic with the event insert, exactly as
    // flag-off. (The node-table classifies it `worker-local`.)
    // Hub-owned authorities the reduced graph yields. Each is a
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
    // Residual hub/writer/infra boundaries (see `SESSION_EXCLUDED`).
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
// In-worker LLM: the stream stays in the isolate. A worker
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
])

/**
 * No plugin host means no configured hook exists, so the trigger is a true
 * no-op. An unsupported hook is only a failure when a loaded plugin declared it.
 */
const noopPluginTrigger = ((_name: string, _input: unknown, output: unknown) => Effect.succeed(output)) as PluginTrigger

/**
 * Dispatch one `Plugin.trigger` to the matching worker-hosted hook.
 *
 * A routed-but-unsupported hook is a true no-op when no loaded plugin declared
 * it: the worker invokes `experimental.text.complete` on every text-end, so a
 * host existing (because `init.plugins` is an array) must not fail an
 * unimplemented hook. A hook a plugin *did* declare stays a typed failure at
 * invocation (D19) rather than being silently skipped.
 */
function runWorkerPluginHook(host: WorkerPluginHost, name: string, input: unknown, output: unknown): Promise<unknown> {
  const route = workerHookRoute(name)
  if (route === undefined) throw new UnclassifiedPluginHookError({ plugin: "worker", hook: name })
  if (route === "unsupported") {
    if (!workerPluginHostDeclaresHook(host, name)) return Promise.resolve(output)
    throw new UnsupportedPluginHookError({ plugin: "worker", hook: name })
  }
  if (!WORKER_DISPATCHED_HOOKS.has(name)) {
    throw new UnsupportedPluginHookError({ plugin: "worker", hook: name })
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
    default:
      throw new UnsupportedPluginHookError({ plugin: "worker", hook: name })
  }
}

/**
 * Hub-executed plugin tools as the worker sees them: the clone-safe descriptors
 * plus the executor that proxies a call back over the hub control port. Bundled
 * so a descriptor without an executor (an unexecutable stub) is unrepresentable.
 */
export interface RemotePluginTools {
  readonly descriptors: readonly PluginToolDescriptor[]
  readonly executor: PluginToolExecutor
  /**
   * The hub abort bridge, forwarded onto every rebuilt remote definition so the
   * registry's `toolCtx.abort` subscription reaches the hub's in-flight call.
   * Optional: a handle without a hub control port omits it.
   */
  readonly abort?: PluginToolAbort
  /**
   * Re-fetch the hub's descriptors and swap them into this handle — the lazy
   * self-heal action attached to each remote definition as its `refresh` seam.
   * Optional: present only when the handle can reach the hub's list authority.
   */
  readonly refreshFromHub?: () => Promise<void>
}

/**
 * Raised when a remote plugin-tool id is ambiguous: it is declared twice, or it
 * collides with a local/builtin tool the registry already owns. Plugin tool ids
 * are bare plugin keys (`registry.ts:199`), so a collision would otherwise
 * silently shadow a tool; fail loud instead.
 */
export class PluginToolIDCollisionError extends Error {
  constructor(
    readonly id: string,
    readonly kind: "duplicate" | "reserved",
  ) {
    super(
      kind === "duplicate"
        ? `plugin tool id "${id}" is declared by more than one remote descriptor`
        : `plugin tool id "${id}" collides with a locally registered tool`,
    )
    this.name = "PluginToolIDCollisionError"
  }
}

/**
 * The collision policy for remote plugin-tool descriptors, applied before
 * registration. Fails loud when the same id is declared twice, or when an id is
 * already reserved by a local/builtin tool (the `reservedIDs` the registry
 * knows). Returns the descriptors unchanged so a caller can chain into
 * registration; the registry passes its builtin ids here, and
 * {@link refreshablePluginTools} applies the same policy at its boundary.
 */
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

/**
 * A {@link RemotePluginTools} whose descriptor set can be replaced in place
 * after a hub-side plugin reload (design Q4). `descriptors` is a live getter, so
 * the worker's `Plugin.Service.list()` reads whatever set is current at call
 * time; a caller that captured the layer before a refresh still sees the new
 * set. The registry (L2) owns re-registration; this seam only keeps the source
 * live.
 */
export interface RefreshablePluginTools extends RemotePluginTools {
  /** Replace the descriptor set; the next `list()` reflects it. Fails loud on a collision. */
  refresh(descriptors: readonly PluginToolDescriptor[]): void
}

/**
 * Build the live {@link RefreshablePluginTools} handle. The returned value IS
 * the `RemotePluginTools` to pass as `SessionLayerOptions.pluginTools`; call
 * `refresh` after a plugin reload to replace the descriptors. Both the initial
 * set and every refresh apply {@link assertPluginToolDescriptorsUnique}, so an
 * ambiguous remote id is rejected at the boundary rather than registered.
 *
 * `abort` and `list` (when supplied) are the hub seams: `abort` becomes each
 * remote definition's abort bridge, and `list` backs `refreshFromHub`, the lazy
 * re-fetch the registry triggers after an `UnknownPluginToolError`.
 */
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

/**
 * The boundary `Plugin` for a worker, parameterised by its `trigger` (decision
 * 70): the hub owns plugin *loading*, but the model-facing hooks run in the
 * isolate. A no-op trigger yields the pass-through; a host-backed trigger runs
 * `chat.params` / `chat.headers`, the tool hooks, and the system/messages
 * transforms in-worker, so plugin-set sampling/headers and pruning match
 * flag-off. One layer covers both cases.
 */
function workerPluginLayer(trigger: PluginTrigger, pluginTools?: RemotePluginTools): Layer.Layer<Plugin.Service> {
  const plugin = {
    trigger,
    list: () => Effect.succeed(remotePluginHooks(pluginTools)),
    init: () => Effect.void,
  } as unknown as Plugin.Interface
  return Layer.succeed(Plugin.Service, plugin)
}

/**
 * The synthetic `Hooks` a worker lists for its hub-executed plugin tools.
 * `remotePluginToolHooks` builds one remote definition per descriptor, so the
 * real `ToolRegistry` registers a proxy stub. Absent the remote tools (degraded
 * boot, or no plugin tools), the worker lists nothing. The descriptor set is
 * re-validated on every read so a colliding id fails loud before registration,
 * whether the caller used {@link refreshablePluginTools} or a plain literal.
 */
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

/**
 * The worker-safe `Plugin.node` replacement: the worker-hosted hooks when a
 * {@link WorkerPluginHost} is supplied, otherwise a no-op pass-through. This is
 * the single source for the replacement, used by both the full worker graph
 * ({@link buildSessionWorkerLayer}) and the reduced `LLM` sub-graph
 * ({@link reducedLlmLayer}), so neither ever builds the global `Plugin` layer.
 */
function workerPluginReplacement(plugins?: WorkerPluginHost, pluginTools?: RemotePluginTools): LayerNode.Replacement {
  const trigger: PluginTrigger =
    plugins === undefined
      ? noopPluginTrigger
      : (((name: string, input: unknown, output: unknown) =>
          Effect.promise(() => runWorkerPluginHook(plugins, name, input, output))) as PluginTrigger)
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
 * `pluginTools`, when supplied, reaches this sub-graph's `Plugin` replacement
 * too, so a tool-consuming reduced node sees the hub-executed remote tools
 * exactly as the full graph does.
 *
 * Exported so a test can assert the per-node boundary wiring without resolving
 * the sub-graph's (single, `LLM`) output service.
 */
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
  pluginTools?: RemotePluginTools,
): Layer.Layer<LLM.Service> {
  return AppNodeBuilderV1.build(LLM.node, llmBoundaryReplacements(snapshot, config, hubProxies, plugins, pluginTools))
}

// ---------------------------------------------------------------------------
// Groups + builder
// ---------------------------------------------------------------------------

/**
 * The ONE ordered registration list for the real session nodes. The groups and
 * the key→node registry are derived from it, so there is a single place to add
 * a node; `REQUIRED_SESSION_NODE_KEYS` (node-table.ts) is the independent
 * declaration of what must be present, checked at module init below.
 */
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

/**
 * Required-key parity, checked when this module initialises. A node added to the
 * list but not to `REQUIRED_SESSION_NODE_KEYS` (or vice versa) is a table/layer
 * drift that would otherwise surface only when a worker graph fails to build.
 */
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

/**
 * The declared *runnable* graph: every required session-node service plus the
 * worker-infrastructure nodes (pipeline seam, clients, snapshot, env). A worker
 * is "able to satisfy" the real `SessionPrompt`/`SessionProcessor` loop when
 * `validateSessionLayer(RUNNABLE_SESSION_NODES, boundaryReplacements(...))` is
 * clean and fails closed on any unclassified reachable node.
 */
export const RUNNABLE_SESSION_NODES = LayerNode.group([
  ...REQUIRED_SESSION_NODES,
  SessionPipelineNode,
  ...WORKER_INFRA_NODES,
  ...SESSION_CLIENT_NODES,
])

/**
 * The reduced pipeline subset: the one-shot `SessionPipeline` seam, runtime
 * flags and the worker infrastructure. `ModelClient` is the injectable model
 * seam, so a test can point it at the mock provider. Retained as an explicit,
 * lightweight graph for pipeline-only tests; the production default is the full
 * runnable session graph (`RUNNABLE_SESSION_NODES`), which now boots against the
 * functional boundary layers.
 */
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
  /**
   * Hub-resident authority proxies. When supplied, the
   * `SessionStatus` / `Permission` / `Question` hub-proxy nodes join the graph
   * and their services are provided over the ports.
   */
  readonly hubProxies?: HubProxyClients
  /**
   * The one session this isolate is bound to. The hub-proxy session scope uses
   * it to authorize child create/read/children/cancel so a worker can never
   * reach another session's rows. Absent on degraded graphs that run inline.
   */
  readonly boundSessionID?: SessionID
  /**
   * Functional `BackgroundJob` replacement; the default is the process-local
   * registry (per-session workers own their background jobs).
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
   * Hub-executed plugin tools: clone-safe descriptors plus the hub-proxy
   * executor. The worker's `Plugin.Service.list()` then exposes one remote stub
   * per descriptor, which `ToolRegistry` registers and whose execution proxies
   * to the hub. Absent (degraded boot, no hub client, or no plugin tools) the
   * worker lists no plugin tools. Supply {@link refreshablePluginTools} to let a
   * hub-side plugin reload replace the set after boot.
   */
  readonly pluginTools?: RemotePluginTools
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
  // Hub-proxy authorities are REPLACEMENTS over nodes the base graph already
  // contains (SessionStatus/Permission/Question); appending a second proxy group
  // would duplicate those service keys in the root.
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
    // The shared fragment owns `Plugin` and `Permission`; the full graph adds
    // the remaining hub authorities (SessionStatus/Question) as replacements.
    ...boundaryReplacements(options.snapshot, options.clients.database ?? options.clients.writer, options.backgroundJob, {
      ...(options.hubProxies === undefined ? {} : { hubProxies: options.hubProxies }),
      ...(options.plugins === undefined ? {} : { plugins: options.plugins }),
      ...(options.pluginTools === undefined ? {} : { pluginTools: options.pluginTools }),
    }),
    ...(options.hubProxies === undefined ? [] : hubAuthorityReplacements(options.hubProxies)),
    ...(options.replacements ?? []),
  ]

  assertSessionLayer(base, replacements)
  const built = AppNodeBuilderV1.build(base, replacements)
  // `PidReporter` is not a graph node: the shell tool reads it optionally at
  // execution time, so merging it keeps degraded graphs (no reporter) building
  // unchanged while the production graph sees the control-port reporter.
  const withPid =
    options.pidReporter === undefined ? built : Layer.merge(built, Layer.succeed(PidReporter, options.pidReporter))
  // `SessionSpawnProxy` is likewise not a graph node: `Session.spawn`
  // reads it optionally, so a hub-reaching worker forwards `session.spawn` while
  // a degraded/hub-less graph builds unchanged (and runs sub-agents inline).
  const layer =
    options.hubProxies === undefined
      ? withPid
      : Layer.merge(withPid, sessionSpawnProxyLayer(sessionSpawnClient(options.hubProxies), options.boundSessionID))
  // The clear-revert authority is a non-graph seam: `SessionRevert` reads it
  // optionally, so a hub-only graph keeps `Session.clearRevert` while a worker
  // routes the hub-owned write over its control port.
  const withClearRevert =
    options.hubProxies === undefined
      ? layer
      : Layer.merge(layer, clearRevertProxyLayer(options.hubProxies.sessionStatus))
  // `logging` is not a graph node either: it contributes only context references
  // (the file sink + minimum level), so merging it leaves the graph and its
  // node-table assertion untouched. Absent keeps the layer unchanged.
  const withLogging = options.logging === undefined ? withClearRevert : Layer.merge(withClearRevert, options.logging)
  // The strict projection authority is a non-graph seam too. Only a BOUND worker
  // (one with a durable session aggregate) may publish session events; an
  // unbound/degraded graph keeps the hub's projection behavior unchanged.
  const withProjectionPolicy =
    options.boundSessionID === undefined && options.clients.database?.sessionID === undefined
      ? withLogging
      : withLogging.pipe(Layer.provide(Layer.succeed(SessionProjector.Policy, workerProjectionPolicy(options.clients.database))))
  return { root: base, replacements, layer: withProjectionPolicy }
}

export * as SessionWorkerLayer from "./layer"
