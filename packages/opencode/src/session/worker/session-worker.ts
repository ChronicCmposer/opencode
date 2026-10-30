// Production session-worker entry.
//
// This is the isolate the hub's `createBunExecutorWorkerPool` spawns. It builds
// the reduced session layer (`buildSessionWorkerLayer`), wires the
// the egress client and its own WAL database, installs a process-local
// background-job registry, and installs the executor with the real session work
// kind.
//
// Initialisation arrives through `workerData` (structured-cloned, never a
// closure): the immutable `WorkerSnapshot`, the `WorkerEnv` overlay, and the
// model bootstrap. The function-valued client handles are rebuilt in-worker over
// transferred `MessagePort`s (they cannot cross an isolate boundary).
//
// Protocol: the `execute.ts` frame protocol over the default worker channel.
// The worker additionally transfers its egress ports and its
// hub-proxy control port to the hub. (The old dedicated job control link was
// removed with the per-session worker refactor; background jobs stay local.)
//
// Thread naming (Linux-only, fail-soft): the isolate names its OS thread so
// `ps -T`/`top -H`/htop label the slot instead of the process (htop needs
// "Show custom thread names" enabled). Because the hub's main thread is `och`,
// the process `comm` no longer matches `pgrep opencode`/`ps -C opencode`; use
// `pgrep -f 'oc serve'` instead. Names are clipped to the kernel's 15-byte
// `comm` limit (see `@/util/thread-name`).

import path from "node:path"
import { threadId, workerData } from "node:worker_threads"
import { Cause, Context, Deferred, Effect, Exit, Fiber, Layer, Option, Schema, Scope } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { isRole, setProcessRole } from "@/effect/process-role"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { AppProcess } from "@opencode-ai/core/process"
import { BackgroundJob } from "@/background/job"
import { SessionPrompt } from "@/session/prompt"
import { SessionRunState } from "@/session/run-state"
import { SessionID } from "@/session/schema"
import { Snapshot } from "@/snapshot"
import { InstanceRef } from "@/effect/instance-ref"
import { EffectBridge } from "@/effect/bridge"
import { WorkerRuntime } from "./runtime-edge"
import { setThreadName } from "@/util/thread-name"
import type { InstanceContext } from "@/project/instance-context"
import { Provider } from "@/provider/provider"
import { DurableLog } from "@opencode-ai/core/durable-log"
import { EventV2 } from "@opencode-ai/core/event"
import { Database } from "@opencode-ai/core/database/database"
import { Durable } from "@opencode-ai/schema/durable-event-manifest"
import { openWriterConnection } from "@/session/writer/connection"
import { createWorkerEgressClient, type WorkerEgressClient } from "./egress-client"
import { openWorkerDatabase, type WorkerBus, type WorkerConnection, type WorkerDatabase } from "./db"
import { createProxyReporting, installReporting } from "@/session/ledger/lifecycle"
import { workerLogMessage } from "./worker-logging"
import { workerLoggingLayer } from "./worker-logging"
import { messageOf } from "./port"
import { createThreadNamer, type ThreadNamer } from "./thread-naming"
import {
  UnknownWorkKindError,
  installSessionExecutor,
  type SessionExecutionResult,
  type SessionRunControl,
  type SessionWork,
  type SessionWorkFactory,
} from "./execute"
import {
  createWorkerPluginHost,
  type MessagesTransformInput,
  type MessagesTransformOutput,
  type SystemTransformInput,
  type SystemTransformOutput,
  type WorkerPlugin,
  type WorkerPluginHubHook,
} from "./plugin-host"
import {
  SessionPipeline,
  PIPELINE_SESSION_NODES,
  localBackgroundJobLayer,
  buildSessionWorkerLayer,
  createHubProxyClient,
  llmModelClientLayer,
  reducedLlmLayer,
  reducedModelProviderLayer,
  snapshotProxyLayer,
  refreshablePluginTools,
  restoreHubProxyError,
  type HubProxyClient,
  type HubProxyClients,
  type PipelineMessage,
  type WorkerEnv,
  type WorkerModelConfig,
  type WorkerSnapshot,
  type WriterClientHandle,
} from "./layer"
import { makeInstanceContext, workerSnapshotBoundaryLayer } from "./snapshot"
import { workerAppProcessNode } from "./app-process-report"
import { PidReporter, type PidReport, type PidReporterInterface } from "./pid-registry"
import type { Role } from "./node-table"
import type {
  PluginToolDescriptor,
  PluginToolExecuteResult,
  PluginToolExecutor,
  PluginToolProgress,
} from "./plugin-tool-wire"

/** The real model capabilities, serialized for the isolate boundary. */
export interface SerializedModelCapabilities {
  readonly temperature: boolean
  readonly reasoning: boolean
  readonly interleaved?: boolean
  readonly toolcall?: boolean
  readonly attachment?: boolean
  readonly [k: string]: unknown
}

/** The real context/output window, serialized for the isolate boundary. */
export interface SerializedModelLimit {
  readonly context: number
  readonly output: number
}

/** The real model pricing, serialized for the isolate boundary. */
export interface SerializedModelCost {
  readonly input: number
  readonly output: number
  readonly cache: { readonly read: number; readonly write: number }
  readonly tiers?: ReadonlyArray<{
    readonly input: number
    readonly output: number
    readonly cache: { readonly read: number; readonly write: number }
    readonly tier: { readonly type: "context"; readonly size: number }
  }>
  readonly experimentalOver200K?: {
    readonly input: number
    readonly output: number
    readonly cache: { readonly read: number; readonly write: number }
  }
}

/**
 * The plain-data model bootstrap the hub ships to a worker. It carries the
 * REAL descriptor — capabilities, limits, options, variants and provider
 * options — so the reduced provider reproduces flag-OFF sampling instead of
 * inventing defaults. Every field is structured-clone safe.
 */
export interface SessionWorkerModel {
  readonly providerID: string
  readonly modelID: string
  readonly apiId: string
  readonly npm?: string
  readonly baseUrl: string
  readonly apiKey: string
  readonly headers?: Readonly<Record<string, string>>
  readonly capabilities: SerializedModelCapabilities
  readonly limit: SerializedModelLimit
  readonly options?: Readonly<Record<string, unknown>>
  readonly variants?: Readonly<Record<string, unknown>>
  readonly providerOptions?: Readonly<Record<string, unknown>>
  readonly cost?: SerializedModelCost
}

/**
 * Capabilities the hub advertises for a worker generation. `sessionWorkerPorts`
 * is `true` once the hub serves the worker's egress ports; the `false`/absent
 * member means the entry uses an in-worker fallback so a session cannot hang on
 * an unserved port. Modelled as a union so the served state is an explicit
 * `true`, never a truthy accident.
 */
export type SessionWorkerCapabilities = { readonly sessionWorkerPorts: true } | { readonly sessionWorkerPorts?: false }

export interface SessionWorkerInit {
  readonly snapshot: WorkerSnapshot
  readonly env: WorkerEnv
  readonly model: SessionWorkerModel
  /**
   * The process role this isolate adopts at boot (`setProcessRole(role)`). The
   * hub ships `"session"`; the role gates graph membership so a worker-hosted
   * graph never contributes the hub node. Required — a payload without a valid
   * role fails loud at the isolate boundary.
   */
  readonly role: Role
  readonly capabilities?: SessionWorkerCapabilities
  /**
   * The worker's hub pool id, when the hub ships it. `pid-report` frames carry
   * an owner id; the hub may override it with the authoritative port
   * association, but a matching id keeps the mirror keyed correctly out of the
   * box. Defaults to the isolate's `threadId` when absent.
   */
  readonly workerID?: number
  /**
   * The session this isolate is bound to at boot, when the hub knows it. Drives
   * the thread-naming seam (`bind`) before the first run arrives.
   */
  readonly sessionID?: string
  /**
   * Worker-hosted plugin hooks: the hub owns plugin
   * loading but the model-facing `chat.params`/`chat.headers` hooks run in the
   * isolate. Supplied when the hub can hand the loaded hooks across (in-process
   * tests today); absent means the reduced LLM uses its no-op pass-through.
   */
  readonly plugins?: readonly WorkerPlugin[]
  /**
   * Clone-safe descriptors for hub-executed plugin tools. The plugin instance
   * lives on the hub, so a plugin tool's `execute` cannot cross the isolate; the
   * hub ships only `{ id, description, jsonSchema }`, and the worker's reduced
   * `Plugin.Service.list()` exposes one proxy stub per descriptor. Absent (or
   * paired with no hub client) means the worker lists no plugin tools.
   */
  readonly pluginTools?: readonly PluginToolDescriptor[]
}

interface WorkerScope {
  postMessage(message: unknown, transfer?: readonly Transferable[]): void
  onmessage: ((event: { data: unknown }) => void) | null
  /** Present on a real Bun worker global; absent on minimal test doubles. */
  addEventListener?: (type: "message", listener: (event: { data: unknown }) => void) => void
  removeEventListener?: (type: "message", listener: (event: { data: unknown }) => void) => void
}

/**
 * Parse the structured-cloned `workerData` at the isolate boundary. A missing or
 * malformed payload must fail loud with a descriptive error, not surface later
 * as an opaque `undefined` property access.
 */
export function parseSessionWorkerInit(data: unknown): SessionWorkerInit {
  if (typeof data !== "object" || data === null) {
    throw new Error(`session worker requires a SessionWorkerInit workerData, received ${typeof data}`)
  }
  const candidate = data as Partial<SessionWorkerInit>

  const snapshot = candidate.snapshot
  if (typeof snapshot !== "object" || snapshot === null) {
    throw new Error("session worker workerData.snapshot is required")
  }
  if (typeof snapshot.flags !== "object" || snapshot.flags === null) {
    throw new Error("session worker workerData.snapshot.flags is required")
  }

  const env = candidate.env
  if (typeof env !== "object" || env === null) {
    throw new Error("session worker workerData.env is required")
  }
  if (typeof env.values !== "object" || env.values === null) {
    throw new Error("session worker workerData.env.values is required")
  }

  const model = candidate.model
  if (typeof model !== "object" || model === null) {
    throw new Error("session worker workerData.model is required")
  }
  for (const field of ["providerID", "modelID", "apiId", "baseUrl", "apiKey"] as const) {
    if (typeof model[field] !== "string") {
      throw new Error(`session worker workerData.model.${field} is required`)
    }
  }
  if (typeof model.capabilities !== "object" || model.capabilities === null) {
    throw new Error("session worker workerData.model.capabilities is required")
  }
  if (typeof model.limit !== "object" || model.limit === null) {
    throw new Error("session worker workerData.model.limit is required")
  }

  const role = candidate.role
  if (!isRole(role)) {
    throw new Error(`session worker workerData.role must be a valid process role, received ${JSON.stringify(role)}`)
  }

  const workerID = candidate.workerID
  if (workerID !== undefined && (typeof workerID !== "number" || !Number.isInteger(workerID) || workerID < 0)) {
    throw new Error(`session worker workerData.workerID must be a non-negative integer, received ${workerID}`)
  }

  const sessionID = candidate.sessionID
  if (sessionID !== undefined && typeof sessionID !== "string") {
    throw new Error(`session worker workerData.sessionID must be a string, received ${typeof sessionID}`)
  }

  const plugins = candidate.plugins
  if (plugins !== undefined && !Array.isArray(plugins)) {
    throw new Error("session worker workerData.plugins must be an array when present")
  }

  const pluginTools = candidate.pluginTools
  if (pluginTools !== undefined && !Array.isArray(pluginTools)) {
    throw new Error("session worker workerData.pluginTools must be an array when present")
  }

  // Build the typed init field-by-field from the validated values, so the
  // boundary result is trusted without a broad `as` cast. Flags are parsed into
  // a trusted `RuntimeFlags.Info`: the worker never re-reads its own
  // `process.env`, so the snapshot is the single source of truth.
  return {
    snapshot: { ...snapshot, flags: RuntimeFlags.parseInfo(snapshot.flags) },
    env,
    model,
    role,
    ...(candidate.capabilities === undefined ? {} : { capabilities: candidate.capabilities }),
    ...(workerID === undefined ? {} : { workerID }),
    ...(sessionID === undefined ? {} : { sessionID }),
    ...(plugins === undefined ? {} : { plugins }),
    ...(pluginTools === undefined ? {} : { pluginTools }),
  }
}

// ---------------------------------------------------------------------------
// Client aggregation.
// ---------------------------------------------------------------------------

/**
 * The worker-local writer over its own SQLite WAL connection (D5). It replaces
 * the deleted writer RPC: it publishes durable events, reads rows, and disposes
 * the generation lease/handle. Transaction control belongs to the worker's
 * `WorkerDatabase`, never to this seam.
 */
export interface WorkerWriterClient extends WriterClientHandle {
  dispose(): void
}

type WriterQueryConnection = Pick<WorkerConnection, "all" | "values" | "run" | "exec">

/** One third of the 30s worker lease TTL: two missed beats still leave headroom. */
const WORKER_LEASE_HEARTBEAT_MS = 10_000

function writerQuery(
  connection: WriterQueryConnection,
  method: string,
  sql: string,
  params?: readonly unknown[],
): unknown {
  switch (method) {
    case "all":
      return connection.all(sql, params)
    case "values":
      return connection.values(sql, params)
    case "run":
      return connection.run(sql, params)
    case "exec":
      connection.exec(sql)
      return { changes: 0 }
    default:
      throw new Error(`unsupported worker writer query method "${method}" (expected all|values|run|exec)`)
  }
}

/**
 * Expose the `WriterClientHandle` seam over one per-worker WAL connection. The
 * production boot passes the connection owned by `db.ts`, so the graph writer
 * and the worker `Database` share exactly one handle; the fallback opens the
 * retained WAL helper for standalone callers (tests, legacy boot).
 */
export function createWorkerDatabaseWriter(
  options: {
    workerID?: number
    filename?: string
    /**
     * Transaction-capable scoped handle. Production passes
     * `WorkerDatabase.connection` (repository-backed, never a raw main handle);
     * standalone tests may pass a trusted unbound connection. Ad-hoc raw SQL is
     * read-only: the typed repository/DurableLog owns every write.
     */
    connection?: WorkerConnection
    /** Read-only query connection for raw writer SQL; defaults to `connection`. */
    queryConnection?: WorkerConnection
    /**
     * Narrow DurableLog/EventV2 capability owned by the worker DB. Production passes
     * this instead of a raw connection; the returned handle has no `connection`
     * or transaction surface.
     */
    bus?: WorkerBus
    /** The session this generation owns; claimed (or reclaimed) at generation start. */
    sessionID?: string
    /** D16 content-addressed blob directory; defaults to a sibling of the DB. */
    blobDir?: string
    /**
     * Lease heartbeat cadence. Defaults to one third of the 30s worker lease TTL
     * so an idle generation never self-fences between writes. `0` disables it.
     */
    heartbeatIntervalMs?: number
    /** Per-database EventV2 binding; production passes `WorkerDatabase.eventBinding`. */
    eventBinding?: EventV2.WorkerScope
  } = {},
): WorkerWriterClient {
  // D16 blobs live next to the database this writer actually opens: an explicit
  // `filename` or custom connection wins over the process-global database path,
  // so a custom-file worker never publishes (or unlinks) the global DB's blobs.
  const databaseFilename = options.filename ?? options.connection?.filename ?? Database.path()
  const ownedConnection =
    options.connection ?? (options.bus === undefined ? openWriterConnection(databaseFilename) : undefined)
  // Transaction control (BEGIN/COMMIT/ROLLBACK) belongs to the worker's own
  // `WorkerDatabase`; the writer seam only reads and publishes. Raw writes are
  // repository/DurableLog-only.
  const readConnection =
    options.queryConnection ?? options.connection ?? ownedConnection ?? openWriterConnection(databaseFilename)
  const ownerID = DurableLog.workerOwnerID(options.workerID ?? threadId)
  if (options.eventBinding !== undefined) WorkerRuntime.runSync(options.eventBinding.setLeaseOwner(ownerID))
  // A session worker's DurableLog is scoped to its one session aggregate: publishing,
  // claiming, reading, or releasing another session's log is refused.
  const blobDir = options.blobDir ?? path.join(path.dirname(databaseFilename), "event-blobs")
  const bus =
    options.bus !== undefined
      ? options.bus.create({
          ownerID,
          leaseTimeoutMs: 30_000,
          ...(options.sessionID === undefined ? {} : { aggregateID: options.sessionID }),
          // Let the WorkerBus default to the actual database file unless the
          // caller named a directory explicitly.
          ...(options.blobDir === undefined ? {} : { blobDir: options.blobDir }),
        })
      : DurableLog.create({
          connection: ownedConnection!,
          ownerID,
          leaseTimeoutMs: 30_000,
          ...(options.sessionID === undefined ? {} : { aggregateID: options.sessionID }),
          blobDir,
        })
  // Claim only the worker-generation lease. The v1 sync owner
  // (`event_sequence.owner_id`) belongs to sync replication and is never
  // rewritten here. A lease held by a live generation is refused; a dead
  // generation is reclaimed only at the observed sequence inside one
  // `BEGIN IMMEDIATE`, so a raced commit fences the reclaim instead of losing it.
  const refusedLeaseAggregates = new Set<string>()
  const claimWorkerLease = (aggregateID: string): void => {
    // Refuse a non-worker schema before `ownerOf`: on a pre-`worker_owner_id`
    // journal `ownerOf` reports v1 sync state, which is not a reclaimable worker
    // lease. Surface the same typed refusal the DurableLog uses instead of silently
    // treating the sync owner as a predecessor generation.
    if (!bus.workerLease) {
      throw new DurableLog.WorkerLeaseUnsupportedError({
        type: "event_sequence",
        message: `Aggregate ${aggregateID} requires a worker lease but this schema has no worker_owner_id column`,
      })
    }
    const owner = bus.ownerOf(aggregateID)
    if (owner === undefined) {
      bus.claim(aggregateID)
      return
    }
    if (owner === ownerID) return
    bus.reclaim({
      aggregateID,
      expectedOwnerID: owner,
      expectedSeq: bus.latest(aggregateID),
      nextOwnerID: ownerID,
      onRefused: (refusal) => {
        if (refusedLeaseAggregates.has(refusal.aggregateID)) return
        refusedLeaseAggregates.add(refusal.aggregateID)
        console.warn(workerLogMessage("lease reclaim refused"), {
          aggregateID: refusal.aggregateID,
          ownerID: refusal.ownerID,
          expiresAt: refusal.expiresAt,
          reason: "the current lease is still live and its owner is not provably dead",
        })
      },
    })
  }
  if (options.sessionID !== undefined) claimWorkerLease(options.sessionID)

  // Keep the generation's lease live while it is idle. The DurableLog and EventV2 share
  // the same `event_sequence` lease columns, so renewing through the DurableLog also
  // renews the durable-event lease. A stale owner is ignored here: the next write
  // fences it through the normal owner check.
  const heartbeatIntervalMs = options.heartbeatIntervalMs ?? WORKER_LEASE_HEARTBEAT_MS
  const heartbeatSessionID = options.sessionID
  const heartbeat =
    heartbeatSessionID !== undefined && heartbeatIntervalMs > 0
      ? setInterval(() => {
          try {
            bus.heartbeat(heartbeatSessionID)
          } catch {
            // A superseded generation is fenced by its next write, not here.
          }
        }, heartbeatIntervalMs)
      : undefined
  heartbeat?.unref?.()

  return {
    query: async (method, sql, params) => writerQuery(readConnection, method, sql, params),
    publish: async (event) => {
      const definition =
        Durable.get(event.type) ?? [...Durable.values()].find((candidate) => candidate.type === event.type)
      if (definition === undefined) throw new Error(`unknown durable event type "${event.type}"`)
      // Non-blocking busy backoff: the async DurableLog path yields between retries so
      // cancellation and streaming progress keep running while SQLite is busy.
      const published = await bus.publishAsync([{ definition, data: event.data as never, id: event.id as never }])
      const last = published[published.length - 1]
      return { seq: last?.durable?.seq ?? null, committed: true }
    },
    dispose: () => {
      if (heartbeat !== undefined) clearInterval(heartbeat)
      if (options.sessionID !== undefined) bus.release(options.sessionID)
      if (options.eventBinding !== undefined) WorkerRuntime.runSync(options.eventBinding.clear())
      ownedConnection?.close()
    },
  }
}

/** The transferred ports a worker rebuilds its function-valued clients over. */
export interface WorkerClientPorts {
  readonly egress?: MessagePort | undefined
  /** Optional dedicated egress control (mesh) port. */
  readonly egressControl?: MessagePort | undefined
}

/** Raised when a required worker client port was not supplied. */
export class MissingWorkerPortError extends Schema.TaggedErrorClass<MissingWorkerPortError>("MissingWorkerPortError")(
  "WorkerSession.MissingPort",
  { field: Schema.String },
) {
  constructor(override readonly field: string) {
    super({ field })
    this.name = "MissingWorkerPortError"
  }
  override get message() {
    return `session worker is missing the "${this.field}" MessagePort`
  }
}

function requirePort(port: MessagePort | undefined, field: string): MessagePort {
  if (port === undefined) throw new MissingWorkerPortError(field)
  return port
}

/**
 * A `MessagePort` is structurally a `Transferable`; this narrows the double cast
 * to one place so every `postMessage` transfer list reads intent, not `unknown`.
 */
const transferable = (port: MessagePort): Transferable => port as unknown as Transferable

/**
 * The client handles the reduced session layer installs. The writer publishes
 * durable events, reads rows, and disposes its generation lease/handle; the
 * worker's own `WorkerDatabase` owns transaction control. The
 * background-job handle is process-local (the hub no longer serves a job port).
 */
export interface WorkerClients {
  readonly writer: WorkerWriterClient
  /** The full egress client, so the production control-plane mesh is reachable. */
  readonly egress: WorkerEgressClient
}

/**
 * Wire the egress port and expose the writer over the worker's one WAL
 * connection. A missing egress port fails loud rather than degrading to a mock
 * that silently drops events; the writer is never a port because D5 has no
 * writer RPC.
 *
 * `workerID`, when supplied, is threaded onto every client so their lifecycle
 * log lines carry the worker's owner id. It is omitted (never fabricated) when
 * the caller does not know it.
 *
 * `bridge`, when supplied, is threaded into the egress client so the peer
 * lifecycle lines actually fire in the isolate. Absent keeps logging inert.
 */
export function buildWorkerClients(
  ports: WorkerClientPorts,
  options: {
    workerID?: number
    bridge?: EffectBridge.Shape
    database?: WorkerDatabase
    sessionID?: string
  } = {},
): WorkerClients {
  const egress = requirePort(ports.egress, "egress")
  const workerID = options.workerID
  const withBridge = options.bridge === undefined ? {} : { bridge: options.bridge }
  return {
    writer: createWorkerDatabaseWriter({
      ...(workerID === undefined ? {} : { workerID }),
      ...(options.database === undefined
        ? {}
        : {
            bus: options.database.bus,
            connection: options.database.connection,
            queryConnection: options.database.strictConnection,
            filename: options.database.filename,
            eventBinding: options.database.eventBinding,
          }),
      ...(options.sessionID === undefined ? {} : { sessionID: options.sessionID }),
    }),
    egress: createWorkerEgressClient(egress, {
      ...(ports.egressControl === undefined ? {} : { controlPort: ports.egressControl }),
      ...(workerID === undefined ? {} : { workerID }),
      ...withBridge,
    }),
  }
}

// ---------------------------------------------------------------------------
// Session-worker bootstrap handle.
// ---------------------------------------------------------------------------

/** The handle {@link bootstrapSessionWorker} returns: teardown plus live seams. */
export interface SessionWorkerBootstrap {
  /**
   * Tear down the bootstrapped worker state. Resolves once the executor and
   * background-job scopes have settled and the SQLite handle/WAL is closed.
   */
  dispose(): Promise<void>
}

// ---------------------------------------------------------------------------
// Worker→hub PID reporting.
// ---------------------------------------------------------------------------

/**
 * The owner id a worker reports: the hub-assigned pool slot id when the hub
 * shipped one, else the isolate's own `threadId`. The hub still treats its
 * port association as authoritative; this only keeps the frame's mirror keyed
 * correctly out of the box. Falling back to `threadId` is a genuine-last resort
 * (a pool that resolves an init without a slot id), never a silent default.
 */
export function reportedWorkerID(init: Pick<SessionWorkerInit, "workerID">, threadId: number): number {
  return init.workerID ?? threadId
}

/**
 * The production {@link PidReporterInterface}: posts a frozen `pid-report`
 * frame over the worker's control/hub-proxy port. `workerID` is the worker's
 * owner id (the hub may override it with the authoritative port association).
 *
 * A closed port makes `postMessage` throw, so a report is never silently
 * dropped; the shell run fails loud instead. Before rethrowing, a WARN
 * `pid report failed` line (with `workerID` and `reason`) is emitted so the
 * dropped report is observable.
 */
export interface ControlPidReporterOptions {
  /** Optional re-entry bridge for the failure line; absent keeps logging inert. */
  readonly bridge?: EffectBridge.Shape
}

/**
 * Builds the worker's control-port PID reporter. Production uses
 * {@link createControlPidReporter} with the worker's forwarded `log`; the factory
 * seam lets a test hand the reporter a throwing port so the
 * `pid report failed` path (and the forwarded `log`) is observable without a
 * real closed port.
 */
export type ControlPidReporterFactory = (
  port: MessagePort,
  workerID: number,
  bridge: EffectBridge.Shape,
) => PidReporterInterface

export function createControlPidReporter(
  port: MessagePort,
  workerID: number,
  options: ControlPidReporterOptions = {},
): PidReporterInterface {
  const bridge = options.bridge
  return {
    report(entry) {
      const frame: PidReport = {
        type: "pid-report",
        workerID,
        entries: [{ pid: entry.pid, pgid: entry.pgid, label: entry.label, workerID }],
      }
      try {
        port.postMessage(frame)
      } catch (error) {
        // The port is closed. Emit the WARN before rethrowing so the report
        // is never silently dropped AND the reason is observable.
        void bridge?.fork(
          Effect.logWarning(workerLogMessage("pid report failed"), { workerID, reason: messageOf(error) }),
        )
        throw error
      }
    },
  }
}

// ---------------------------------------------------------------------------
// Model bootstrap.
// ---------------------------------------------------------------------------

/** The descriptor fields the reduced provider consumes (see `layer.ts`). */
function modelConfig(model: SessionWorkerModel): WorkerModelConfig {
  return {
    providerID: model.providerID,
    modelID: model.modelID,
    apiId: model.apiId,
    npm: model.npm ?? "@ai-sdk/openai-compatible",
    baseUrl: model.baseUrl,
    apiKey: model.apiKey,
    headers: model.headers ?? {},
    // The wire shapes (`SerializedModelCapabilities` / `SerializedModelCost` / a
    // loose variants record) are the frozen isolate contract; `WorkerModelConfig`
    // is the reduced provider's stricter projection. Narrowing here is the single
    // boundary.
    capabilities: model.capabilities as WorkerModelConfig["capabilities"],
    limit: model.limit,
    ...(model.options === undefined ? {} : { options: model.options }),
    ...(model.variants === undefined ? {} : { variants: model.variants as WorkerModelConfig["variants"] }),
    ...(model.providerOptions === undefined ? {} : { providerOptions: model.providerOptions }),
    ...(model.cost === undefined ? {} : { cost: model.cost as WorkerModelConfig["cost"] }),
  }
}

// ---------------------------------------------------------------------------
// Session work and work-kind registry.
// ---------------------------------------------------------------------------

const rowToMessage = (row: unknown): PipelineMessage | undefined => {
  if (row === null || typeof row !== "object") return undefined
  const data = (row as { data?: unknown }).data
  const parsed = typeof data === "string" ? safeJson(data) : data
  if (parsed === null || typeof parsed !== "object") return undefined
  const record = parsed as { role?: unknown; parts?: unknown; content?: unknown }
  const role = typeof record.role === "string" ? record.role : "user"
  if (typeof record.content === "string") return { role, content: record.content }
  if (Array.isArray(record.parts)) {
    const text = record.parts
      .map((part) => (part !== null && typeof part === "object" ? (part as { text?: unknown }).text : undefined))
      .filter((value): value is string => typeof value === "string")
      .join("")
    return { role, content: text }
  }
  return undefined
}

function safeJson(value: string): unknown {
  try {
    return JSON.parse(value)
  } catch {
    return undefined
  }
}

/** Read a session's persisted messages through the writer client (no local DB). */
export function writerReadMessages(
  writer: WriterClientHandle,
): (sessionID: string) => Effect.Effect<readonly PipelineMessage[]> {
  return (sessionID) =>
    Effect.tryPromise({
      try: () =>
        writer.query("all", "SELECT data FROM message WHERE session_id = ? ORDER BY time_created, id", [sessionID]),
      catch: (error) => (error instanceof Error ? error : new Error(String(error))),
    }).pipe(
      Effect.map((rows) =>
        Array.isArray(rows) ? rows.map(rowToMessage).filter((m): m is PipelineMessage => m !== undefined) : [],
      ),
      // A read failure must not abort the run: the reduced pipeline still
      // produces a turn with the messages it could read.
      Effect.orElseSucceed(() => [] as readonly PipelineMessage[]),
    )
}

/** The executor control plus an Effect signal that the run table has marked the session busy. */
export interface WorkerRunControl extends SessionRunControl {
  readonly awaitBusy: (sessionID: SessionID) => Effect.Effect<void>
}

export interface SessionWorkOptions {
  /**
   * The worker-lifetime graph context. When present, every run provides these
   * already-built services instead of rebuilding (and disposing) the layer, so
   * the per-worker DB connection survives across turns.
   */
  readonly context?: Context.Context<unknown>
  /**
   * The compiled reduced session layer the work runs against. Used by callers
   * that do not prebuild a worker-lifetime context; the layer is built and
   * disposed per run.
   */
  readonly layer?: Layer.Layer<unknown, unknown>
  /**
   * Worker-lifetime scope for `layer`. When supplied (production), the layer is
   * built once on this scope and its context is reused by every run; closing the
   * scope disposes the graph, including the worker DB connection. Omitted (test
   * half), the layer is built and disposed per run.
   */
  readonly scope?: Scope.Scope
  /** The writer client, for reading persisted messages in-worker. */
  readonly writer: WriterClientHandle
  readonly threadId: number
  /** Message source seam; defaults to {@link writerReadMessages}. */
  readonly readMessages?: (sessionID: string) => Effect.Effect<readonly PipelineMessage[]>
  /**
   * Run the real `SessionPrompt` loop. Defaults to `true`; the entry passes
   * `false` in the degraded no-hub-port mode, where the worker has no writer
   * database or ambient `InstanceRef` and can only run the reduced pipeline.
   */
  readonly prompt?: boolean
  /**
   * The ambient `InstanceRef` the worker's `InstanceState`-backed services
   * resolve. Derived from the worker snapshot's directory; omitted only in
   * in-process tests that supply their own context.
   */
  readonly instance?: InstanceContext
  /**
   * Register the worker-local run control. When the graph provides
   * `SessionRunState`, `run` resolves it and hands over a `SessionRunControl`
   * whose `cancel`/`busy` reach the real `SessionPrompt.loop` runner (table B),
   * so an executor cancel actually stops the loop (and `assertNotBusy` stops
   * lying). Pipeline-only graphs have no `SessionRunState`, so nothing registers.
   */
  readonly registerControl?: (control: WorkerRunControl) => void
  /**
   * Consume a cancel that arrived before the first graph build registered a
   * control. A pre-run cancel must not be lost: the run interrupts at its start
   * (before building) or immediately after the build, so a cancelled first turn
   * never executes its prompt.
   */
  readonly consumePreRunCancel?: (sessionID: SessionID) => boolean
}

/**
 * The real `SessionWork`. When the layer provides `SessionPrompt` and the
 * caller allows it, the worker runs the actual opencode session loop —
 * `SessionPrompt.loop` — which drives `SessionProcessor` over the worker-local
 * `LLM` and persists through the writer boundary. Otherwise it falls back to the
 * one-shot `SessionPipeline` reduction (pipeline-only graphs and the degraded
 * no-hub-port boot).
 *
 * A carried `prompt` routes through the full
 * `SessionPrompt.prompt` path — persist the user message for `sessionID`, then
 * loop — instead of the bare `loop` that expects the row to already exist.
 */
export function createSessionWork(options: SessionWorkOptions): SessionWork {
  if (options.context === undefined && options.layer === undefined) {
    throw new Error("createSessionWork requires a worker-lifetime context or a per-run layer")
  }
  // Built on first use so `bootstrapSessionWorker` stays synchronous: the async
  // graph build happens inside the first run, on the worker-lifetime scope.
  let builtContext: Context.Context<unknown> | undefined
  const resolveContext = (runScope: Scope.Scope): Effect.Effect<Context.Context<unknown>> =>
    Effect.suspend(() => {
      if (options.context !== undefined) return Effect.succeed(options.context)
      if (builtContext !== undefined) return Effect.succeed(builtContext)
      const layer = options.layer as Layer.Layer<unknown, unknown>
      const build =
        options.scope === undefined ? Layer.buildWithScope(layer, runScope) : Layer.buildWithScope(layer, options.scope)
      return Effect.map(build.pipe(Effect.orDie), (context) => {
        if (options.scope !== undefined) builtContext = context
        return context
      })
    })
  return (sessionID, carriedPrompt, carriedShell) => {
    const body = Effect.gen(function* () {
      if (options.registerControl !== undefined) {
        const runState = yield* Effect.serviceOption(SessionRunState.Service)
        if (Option.isSome(runState)) {
          // `SessionRunState` methods resolve their per-directory state through
          // `InstanceRef`; a control call is made outside the run's ambient
          // context, so re-provide the worker instance the run was built with.
          const withInstance = <A, E>(effect: Effect.Effect<A, E>): Effect.Effect<A, E> =>
            options.instance === undefined ? effect : Effect.provideService(effect, InstanceRef, options.instance)
          options.registerControl({
            cancel: (id) => WorkerRuntime.runPromise(withInstance(runState.value.cancel(id)).pipe(Effect.asVoid)),
            busy: (id) =>
              WorkerRuntime.runPromise(
                withInstance(runState.value.assertNotBusy(id)).pipe(
                  Effect.match({ onFailure: () => true, onSuccess: () => false }),
                ),
              ),
            awaitBusy: (id) => withInstance(runState.value.awaitBusy(id)),
          })
        }
      }
      if (options.prompt !== false) {
        const prompt = yield* Effect.serviceOption(SessionPrompt.Service)
        if (Option.isSome(prompt)) {
          // A carried shell replays the caller's full `SessionPrompt.shell`
          // path (persist + shell + settle) in-worker; a carried prompt persists
          // its user message (`createUserMessage`) before looping. The run's
          // `sessionID` is authoritative either way, so the row always lands on
          // the session this run was admitted for.
          const result =
            carriedShell !== undefined
              ? yield* prompt.value.shell({ ...carriedShell, sessionID })
              : carriedPrompt === undefined
                ? yield* prompt.value.loop(new SessionPrompt.LoopInput({ sessionID }))
                : yield* prompt.value.prompt({ ...carriedPrompt, sessionID })
          return { sessionID, value: result } satisfies SessionExecutionResult
        }
      }
      const readMessages = options.readMessages ?? writerReadMessages(options.writer)
      const messages = yield* readMessages(sessionID)
      const pipeline = yield* SessionPipeline
      const result = yield* pipeline.run({ sessionID, messages })
      return { sessionID, value: { ...result, threadId: options.threadId } } satisfies SessionExecutionResult
    })
    const run = Effect.scopedWith((runScope) =>
      Effect.gen(function* () {
        // A cancel that landed before this run started wins the whole build:
        // never construct the graph for work that is already cancelled.
        if (options.consumePreRunCancel?.(sessionID) === true) return yield* Effect.interrupt
        const context = yield* resolveContext(runScope)
        // A cancel that landed during the build must be honoured before the
        // prompt runs, even though the control could not receive it yet.
        if (options.consumePreRunCancel?.(sessionID) === true) return yield* Effect.interrupt
        return yield* body.pipe(
          // A failed run is logged once, through the worker logger (the run's
          // context provides the logging sink), before the cause is re-raised.
          // Interrupts (a cooperative cancel) are not failures and are not logged.
          Effect.tapCauseIf(
            (cause) => Cause.hasFails(cause) || Cause.hasDies(cause),
            (cause) => Effect.logError("session worker run failed", { sessionID, cause: Cause.pretty(cause) }),
          ),
          Effect.provide(context),
        )
      }),
    ).pipe(Effect.orDie)
    // The layer build (and any fiber it forks) must see the worker's ambient
    // instance context, so `InstanceState`-backed services resolve a directory.
    const provided = options.instance === undefined ? run : Effect.provideService(run, InstanceRef, options.instance)
    // One annotation at the per-session run boundary: every Effect log the run
    // emits is forwarded with the session id in its user-visible prefix.
    return provided.pipe(Effect.annotateLogs({ sessionID }))
  }
}

/**
 * Whether `sessionID` still owns a running process-local background job. A
 * per-session worker only starts jobs for its own session, but an unparented
 * job still owns the isolate, so both count. Terminal jobs (`completed`,
 * `error`, `cancelled`) are retained by the registry and must not keep the
 * worker pinned.
 */
export function hasRunningBackgroundJob(jobs: readonly BackgroundJob.Info[], sessionID: string): boolean {
  return jobs.some((job) => job.status === "running" && (job.parentID === undefined || job.parentID === sessionID))
}

/**
 * The worker-side `hasActiveWork` probe: consult the process-local
 * `BackgroundJob.Service.list()` (not the run/waiters state) so a background
 * job keeps the isolate alive across an idle-reap window without making a new
 * prompt fail `assertNotBusy`.
 */
export function createBackgroundWorkProbe(jobs: BackgroundJob.Interface): (sessionID: SessionID) => Promise<boolean> {
  return (sessionID) =>
    WorkerRuntime.runPromise(jobs.list().pipe(Effect.map((running) => hasRunningBackgroundJob(running, sessionID))))
}

/**
 * Worker-side mirror of the process-local job registry over the hub control
 * port. Foreground jobs run in the worker isolate, so the hub's promotion
 * endpoint cannot see them; each running job is registered in a hub-side mirror
 * (visible to `list`/`promote`) and the worker long-polls for promotion.
 *
 * Promotion must apply to the LOCAL registry: that is where `onPromote` (the
 * task notification and metadata rewrite) lives. The hub resolves the long-poll
 * when `promote` runs, and this wrapper then calls the local `promote`. Settlement
 * is reported back so the hub mirror is removed instead of leaking a ghost
 * running job.
 */
export function createHubSyncedBackgroundJobs(
  local: BackgroundJob.Interface,
  client: HubProxyClient,
): BackgroundJob.Interface {
  const mirrored = new Set<string>()

  const reportSettlement = async (id: string): Promise<void> => {
    const waited = await WorkerRuntime.runPromise(local.wait({ id }))
    const info = waited.info
    if (info === undefined || info.status === "running") return
    await client.call("backgroundJob.settle", [
      {
        id,
        status: info.status,
        ...(info.output === undefined ? {} : { output: info.output }),
        ...(info.error === undefined ? {} : { error: info.error }),
      },
    ])
  }

  const mirror = async (info: BackgroundJob.Info): Promise<void> => {
    if (info.status !== "running") return
    const first = !mirrored.has(info.id)
    if (first) mirrored.add(info.id)
    try {
      // A re-register carries updated metadata (a resumed foreground job is
      // promoted worker-side), so the hub mirror reports the current state.
      await client.call("backgroundJob.register", [info])
    } catch {
      if (first) mirrored.delete(info.id)
      return
    }
    if (!first) return
    // Both watchers settle independently: promotion applies locally, while
    // settlement removes the hub mirror. Neither may throw into the job start.
    void reportSettlement(info.id).catch(() => undefined)
    void client
      .call("backgroundJob.awaitPromotion", [info.id])
      .then((promoted) => (promoted === true ? WorkerRuntime.runPromise(local.promote(info.id)) : undefined))
      .catch(() => undefined)
      .finally(() => mirrored.delete(info.id))
  }

  const sync = (info: BackgroundJob.Info): void => {
    void mirror(info).catch(() => undefined)
  }

  return {
    list: () => local.list(),
    get: (id) => local.get(id),
    start: (input) => local.start(input).pipe(Effect.tap((info) => Effect.sync(() => sync(info)))),
    extend: (input) =>
      local.extend(input).pipe(
        Effect.flatMap((extended) => {
          if (!extended) return Effect.succeed(false)
          return local.get(input.id).pipe(
            Effect.tap((info) =>
              Effect.sync(() => {
                if (info !== undefined) sync(info)
              }),
            ),
            Effect.as(true),
          )
        }),
      ),
    wait: (input) => local.wait(input),
    waitForPromotion: (id) => local.waitForPromotion(id),
    promote: (id) =>
      local.promote(id).pipe(
        Effect.tap((info) =>
          Effect.sync(() => {
            if (info !== undefined) sync(info)
          }),
        ),
      ),
    cancel: (id) => local.cancel(id),
  }
}

/** An explicit work-kind registry; an unknown kind is a typed error. */
export type WorkFactories = Readonly<Record<string, SessionWork>>

export function createWorkRegistry(factories: WorkFactories): SessionWorkFactory {
  return (kind) => {
    const work = factories[kind]
    if (work === undefined) {
      throw new UnknownWorkKindError({ kind, message: `session worker has no work factory for kind "${kind}"` })
    }
    return work
  }
}

/** The production registry: the protocol literal `"session"` runs session work. */
export function productionWorkRegistry(session: SessionWork): SessionWorkFactory {
  return createWorkRegistry({ session })
}

// ---------------------------------------------------------------------------
// Entry bootstrap.
// ---------------------------------------------------------------------------

/**
 * The client handles used when the hub does not (yet) serve the worker's
 * egress ports: in-process adapters so a reduced turn can
 * still complete. Durable writes and egress are the hub's authority, so the
 * fallback reports a non-committed publish and emits nothing.
 */
function directClients(): WorkerClients {
  return {
    writer: {
      query: async () => [],
      publish: async () => ({ seq: null, committed: false }),
      dispose: () => {},
    },
    // Degraded boot: the hub serves no egress port, so events are dropped and
    // there is no control-plane mesh. Still a full `WorkerEgressClient` so the
    // aggregate handle type stays honest.
    egress: {
      emit: () => {},
      emitDurable: () => {},
      end: () => {},
      buffered: 0,
      ended: false,
      mesh: undefined,
      dispose: () => {},
    },
  }
}

/**
 * The worker-appropriate graph replacements: the reduced provider (the isolate
 * serves exactly one serialized model) and the fail-soft `Snapshot` boundary (the
 * isolate cannot run `git`). Kept as one seam so the production entry and its
 * tests wire the identical replacements.
 *
 * When the worker owns a control-port `reporter`, `AppProcess` is replaced with
 * the reporting wrapper ({@link workerAppProcessNode}): the worker runs real
 * child processes (`Ripgrep` searches, formatters), and a mid-flight child would
 * otherwise be orphaned on terminate (no finalizers run). The replacement is a
 * NODE, so the graph provides its `CrossSpawnSpawner` dependency. It is applied
 * after `boundaryReplacements`, so it wins over any default.
 */
export function workerGraphReplacements(
  config: WorkerModelConfig,
  reporter?: PidReporterInterface,
  hubClient?: HubProxyClient,
): LayerNode.Replacements {
  // The reporting `AppProcess` replacement is a NODE (not a plain layer), so the
  // graph provides its `CrossSpawnSpawner` dependency; typed as `Replacements`
  // here so the tuple survives the spread.
  const appProcess: LayerNode.Replacements =
    reporter === undefined ? [] : [[AppProcess.node, workerAppProcessNode(reporter)]]
  return [
    [Provider.node, reducedModelProviderLayer(config)],
    [Snapshot.node, hubClient === undefined ? workerSnapshotBoundaryLayer() : snapshotProxyLayer(hubClient)],
    ...appProcess,
  ]
}

/**
 * The worker-side logger. The isolate runs no long-lived Effect fiber (each frame
 * is executed with the shared runtime boundary), so a bridge is created synchronously from
 * the worker's own instance context. `bridge.fork` then runs each `Effect.logInfo`
 * on the default Effect runtime, so the peer lifecycle lines reach the worker's
 * output instead of being dropped by the no-op default.
 */
export function workerLogBridge(snapshot: WorkerSnapshot, logging?: Layer.Layer<never>): EffectBridge.Shape {
  const bridge = WorkerRuntime.runSync(
    EffectBridge.make().pipe(Effect.provideService(InstanceRef, workerInstanceContext(snapshot))),
  )
  if (logging === undefined) return bridge
  // The file sink opens asynchronously, so the synchronous boundary cannot build it into
  // the bridge context. Provide the logging layer per emitted line instead — the
  // bridge logger only emits a handful of lifecycle lines.
  return {
    ...bridge,
    fork: <A, E, R>(effect: Effect.Effect<A, E, R>): Fiber.Fiber<A, E> =>
      WorkerRuntime.runFork(effect.pipe(Effect.provide(logging)) as Effect.Effect<A, E, never>),
  }
}

/**
 * The hub-proxied `experimental.chat.system.transform` bridge: forwards the
 * worker's system array to the hub authority over the control port, where the
 * loaded plugins run and return the mutated `{ system }`. Every hub authority
 * multiplexes one port, so the caller hands the shared client.
 *
 * Returns `undefined` when the worker has no hub client (degraded boot), so the
 * plugin host omits the transform and the system array passes through unchanged.
 */
export function hubProxySystemTransform(
  hubProxy: HubProxyClient | undefined,
): ((input: SystemTransformInput, output: SystemTransformOutput) => Promise<SystemTransformOutput>) | undefined {
  if (hubProxy === undefined) return undefined
  return (input, output) => hubProxy.call("systemTransform", [input, output]) as Promise<SystemTransformOutput>
}

/**
 * The hub-proxied `experimental.chat.messages.transform` bridge: forwards the
 * worker's message array to the hub authority over the control port, where the
 * loaded plugins run and return the mutated `{ messages }`. Every hub authority
 * multiplexes one port, so the caller hands the shared client.
 *
 * Returns `undefined` when the worker has no hub client (degraded boot), so the
 * plugin host omits the transform and the message array passes through unchanged.
 */
export function hubProxyMessagesTransform(
  hubProxy: HubProxyClient | undefined,
): ((input: MessagesTransformInput, output: MessagesTransformOutput) => Promise<MessagesTransformOutput>) | undefined {
  if (hubProxy === undefined) return undefined
  return (input, output) => hubProxy.call("messagesTransform", [input, output]) as Promise<MessagesTransformOutput>
}

/**
 * The hub-proxied plugin-hook bridge: forwards every routed-but-hub-owned hook
 * (`tool.execute.before/after`, `shell.env`, `tool.definition`, the experimental
 * transforms) to the hub authority over the control port, where the real plugin
 * instance runs. The worker ships only `(name, input, output)`; the reply is the
 * mutated output. A hook whose route is not `hub-proxy` is rejected hub-side, so
 * this cannot become a generic plugin RPC.
 *
 * Returns `undefined` when the worker has no hub client (degraded boot), so the
 * plugin host forwards no hook and each output passes through unchanged.
 */
export function hubProxyPluginTrigger(hubProxy: HubProxyClient | undefined): WorkerPluginHubHook | undefined {
  if (hubProxy === undefined) return undefined
  return (name, input, output) => hubProxy.call("pluginTrigger", [name, input, output])
}

/**
 * The hub-proxied plugin-tool executor: forwards a worker tool call to the hub
 * authority over the same multiplexed control port, where the plugin's real
 * `execute` runs and returns a clone-safe result. Every hub authority
 * multiplexes one port, so the caller hands the shared client.
 *
 * Returns `undefined` when the worker has no hub client (degraded boot), so the
 * caller wires no plugin tools and the worker's `Plugin.Service.list()` stays
 * empty.
 */
export function hubProxyPluginTool(hubProxy: HubProxyClient | undefined): PluginToolExecutor | undefined {
  if (hubProxy === undefined) return undefined
  return {
    execute: (input, onProgress) =>
      hubProxy
        .call(
          "pluginTool.execute",
          [input],
          // The hub streams mid-execute `ToolContext.metadata` updates back as
          // `pluginTool.metadata` event frames; every other event method is for a
          // different concern and is dropped. The sink is optional, so a caller
          // that does not surface progress pays nothing.
          (method, payload) => {
            if (method === "pluginTool.metadata") onProgress?.(payload as PluginToolProgress)
          },
        )
        .catch((error: unknown) => {
          throw restoreHubProxyError(error)
        }) as Promise<PluginToolExecuteResult>,
  }
}

/**
 * The hub-proxied plugin-tool abort: forwards a `(sessionID, callID)` pair over
 * the same multiplexed control port so the hub can unwind an in-flight plugin
 * tool. Returns `undefined` on degraded boot, so a caller wires no abort rather
 * than a stub that would fail on call.
 */
export function hubProxyPluginToolAbort(
  hubProxy: HubProxyClient | undefined,
): ((input: { readonly sessionID: string; readonly callID: string }) => Promise<void>) | undefined {
  if (hubProxy === undefined) return undefined
  return (input) => hubProxy.call("pluginTool.abort", [input]) as Promise<void>
}

/**
 * The hub-proxied plugin-tool descriptor list: the lazy-refresh seam. Descriptors
 * are shipped once at worker-init, so a plugin that reloads mid-generation leaves
 * them stale; a registry that hits `UnknownPluginToolError` can call this to
 * re-fetch the current descriptors over the same multiplexed control port.
 * Returns `undefined` on degraded boot, so the caller keeps its current list.
 */
export function hubProxyPluginToolList(
  hubProxy: HubProxyClient | undefined,
): (() => Promise<readonly PluginToolDescriptor[]>) | undefined {
  if (hubProxy === undefined) return undefined
  return () => hubProxy.call("pluginTool.list", []) as Promise<readonly PluginToolDescriptor[]>
}

/** Options for {@link bootstrapSessionWorker}; every field is additive. */
export interface BootstrapSessionWorkerOptions {
  /**
   * Override the worker-side log bridge. Production omits it (a bridge is
   * derived from the snapshot); a test may inject a recorder to observe the
   * lines the client construction emits.
   */
  readonly bridge?: EffectBridge.Shape
  /**
   * Worker logging layer (see `worker-logging.ts`). Production omits it: the
   * worker derives a file-sink layer scoped to its slot id so its `Effect.log*`
   * lines reach the main `opencode.log`. A test may inject a recorder.
   */
  readonly logging?: Layer.Layer<never>
  /**
   * Test seam: build the control-port PID reporter. Defaults to
   * `createControlPidReporter(port, workerID, { bridge })`, so the worker's
   * forwarded bridge reaches the reporter's `pid report failed` line.
   */
  readonly pidReporterFactory?: ControlPidReporterFactory
  /**
   * Test seam: install (and capture the disposer of) the executor. Defaults to
   * {@link installSessionExecutor}; bootstrap disposal always disposes it so the
   * executor's owned scope finalizers run when the worker tears down.
   */
  readonly installExecutor?: typeof installSessionExecutor
  /** Test seam: the thread-naming lifecycle; defaults to `setThreadName`. */
  readonly threadNamer?: ThreadNamer
  /**
   * Test seam: observe the hub-proxy control client bootstrap installs. Lets a
   * test prove `dispose()` rejects its in-flight calls.
   */
  readonly observeHubProxy?: (client: HubProxyClient) => void
  /**
   * Test seam: observe the worker's own writer handle. Lets a shutdown test
   * probe the same connection the orderly close owns.
   */
  readonly observeWriter?: (writer: WorkerWriterClient) => void
  /**
   * Test seam: observe the process-local job registry the worker graph receives.
   * A full-port boot wraps it with the hub mirror, so a test can drive the same
   * registry the task tool uses end to end.
   */
  readonly observeBackgroundJobs?: (jobs: BackgroundJob.Interface) => void
}

/**
 * Whether the hub serves this worker's egress ports, i.e.
 * whether the full prompt path boots. A discriminated switch over the
 * capabilities union keeps the two boot modes explicit at the decision site.
 */
function servesWorkerPorts(capabilities: SessionWorkerCapabilities | undefined): boolean {
  switch (capabilities?.sessionWorkerPorts) {
    case true:
      return true
    case false:
    case undefined:
      return false
  }
}

/**
 * Build the reduced layer and install the executor. Split from module scope so
 * tests can import this module (`buildWorkerClients`, the registry) without a
 * worker context; the module only bootstraps when `workerData` is present.
 *
 * The egress (plus its dedicated control plane) ports are
 * transferred only when the hub advertises `capabilities.sessionWorkerPorts`,
 * because a port nobody serves would make a session hang; the production hub
 * serves all of them. There is no dedicated job control link (background jobs
 * are process-local), and the hub-proxy control port is transferred separately.
 *
 * Returns the child-result registration handshake so a caller can subscribe to
 * settled subagents the hub routes to this worker (and tear the subscription
 * down); production installs the subscription regardless.
 */
export function bootstrapSessionWorker(
  init: SessionWorkerInit,
  scope: WorkerScope,
  options: BootstrapSessionWorkerOptions = {},
): SessionWorkerBootstrap {
  const logging = options.logging ?? workerLoggingLayer({ id: `worker-${init.workerID ?? threadId}` })
  const bridge = options.bridge ?? workerLogBridge(init.snapshot, logging)
  // The control-port PID reporter's log is the worker's forwarded bridge (the
  // same seam the client lifecycles use). A test may swap the factory to observe
  // the failure.
  const createPidReporter =
    options.pidReporterFactory ??
    ((port: MessagePort, workerID: number, forwarded: EffectBridge.Shape) =>
      createControlPidReporter(port, workerID, { bridge: forwarded }))
  // Thread-naming lifecycle seam (N slice): bind at boot when the hub shipped
  // the session, then toggle running/idle per run and clear on dispose.
  const namer = options.threadNamer ?? createThreadNamer({ setName: setThreadName })
  // One boot write: bind the session the hub shipped (or the worker slot when
  // the hub could not name one). `bind` is the only lifecycle entry point.
  if (init.sessionID !== undefined) namer.bind(init.sessionID)
  else if (init.workerID !== undefined) namer.bind(String(init.workerID))
  let clients: WorkerClients | undefined
  let database: WorkerDatabase | undefined
  let hubProxyClient: HubProxyClient | undefined
  let hubProxies: HubProxyClients | undefined
  let pidReporter: PidReporterInterface | undefined
  // Partial-bootstrap state the shared teardown also owns. Declared before the
  // mid-bootstrap `try` so a throw can close whatever was already built instead
  // of relying on isolate teardown.
  let backgroundJobScope: Scope.Scope | undefined
  let workerScope: Scope.Scope | undefined
  let installed: ReturnType<typeof installSessionExecutor> | undefined
  // Orderly teardown, shared by the hub's `shutdown` frame and `dispose()`.
  // It is defined and installed BEFORE the DB opens: a shutdown that arrives
  // while the worker is still booting must not be lost, otherwise the isolate's
  // native WAL handle outlives `Worker.terminate()` and keeps the write lock
  // until the process exits. It must run while the realm still exists so the
  // handle closes and the DurableLog releases its lease.
  let resourcesClosed = false
  let shutdownRequested = false
  /** Run one best-effort disposal: a broken handle must never skip the rest. */
  const disposeSafely = (dispose: (() => void) | undefined): void => {
    try {
      dispose?.()
    } catch {
      // The resource may already be unusable under a forced terminate; the
      // remaining resources (and the native handle close) still run.
    }
  }
  const closeResources = (): void => {
    if (resourcesClosed) return
    resourcesClosed = true
    try {
      // Dispose the outbound clients first: pending hub-proxy calls reject instead
      // of hanging past termination, and the egress listeners are removed before
      // the graph/connection teardown. Each disposal is independently guarded
      // so a throw cannot skip the others or the native WAL close below.
      disposeSafely(() => hubProxyClient?.dispose())
      disposeSafely(() => clients?.egress.dispose())
      // The writer owns the generation lease and the same connection as the DB;
      // its dispose releases the lease and closes the handle (idempotently).
      disposeSafely(() => clients?.writer.dispose())
    } finally {
      // Always attempt the native handle close: it owns the WAL write lock, and a
      // half-open transaction is rolled back by SQLite's close.
      const db = database
      if (db !== undefined) disposeSafely(() => WorkerRuntime.runSync(db.close))
    }
  }
  /**
   * The one teardown for both the returned handle and a failed mid-bootstrap.
   * Every step is idempotent or best-effort, so it is safe on a partially built
   * worker and safe after `closeResources` already ran. The executor's owned
   * scope is torn down BEFORE the SQLite handle closes, so an in-flight run can
   * never observe a closed database; only then do the clients and handle close.
   */
  const disposeBootstrap = async (): Promise<void> => {
    installReporting(undefined)
    namer.onDispose()
    // Close the executor's owned scope so its runner-table fibers/finalizers
    // settle with the worker instead of leaking past bootstrap disposal.
    const executor = installed
    if (executor !== undefined) await executor.dispose().catch(() => undefined)
    // The registry scope outlives every run, so close it explicitly with the
    // worker bootstrap; a still-running job is interrupted with the isolate.
    const jobs = backgroundJobScope
    if (jobs !== undefined) await WorkerRuntime.runPromise(Scope.close(jobs, Exit.void)).catch(() => undefined)
    // Close durable resources before the graph scope; close is idempotent.
    closeResources()
    // Close the worker-lifetime graph scope (DB/egress/bridge finalizers).
    const graph = workerScope
    if (graph !== undefined) await WorkerRuntime.runPromise(Scope.close(graph, Exit.void)).catch(() => undefined)
  }
  /**
   * Synchronous best-effort teardown for the mid-bootstrap failure path. The
   * executor never started a run there, so closing the handle immediately cannot
   * race in-flight work; the executor scope still gets its disposal started.
   */
  const disposeBootstrapSync = (): void => {
    installReporting(undefined)
    namer.onDispose()
    const executor = installed
    if (executor !== undefined) void executor.dispose().catch(() => undefined)
    const jobs = backgroundJobScope
    if (jobs !== undefined) void WorkerRuntime.runPromise(Scope.close(jobs, Exit.void)).catch(() => undefined)
    closeResources()
    const graph = workerScope
    if (graph !== undefined) void WorkerRuntime.runPromise(Scope.close(graph, Exit.void)).catch(() => undefined)
  }
  // A raw worker message (not an executor frame), handled here rather than in
  // `execute.ts`; the executor's unknown-frame path is harmless.
  const onShutdownFrame = (event: { data: unknown }): void => {
    const frame = event.data as { readonly type?: unknown; readonly id?: unknown } | undefined
    if (frame?.type !== "shutdown") return
    shutdownRequested = true
    try {
      // A shutdown frame is a hard-close request: close the native handle while
      // the realm still runs so the WAL lock is released before the thread dies.
      closeResources()
    } finally {
      // The hub's bounded ack wait may already have elapsed and terminated the
      // thread; attempt the ack unconditionally so a closed WAL is observed as
      // orderly rather than as a timeout whenever the realm still runs.
      scope.postMessage({ type: "shutdown-ack", id: typeof frame.id === "number" ? frame.id : 0 })
    }
  }
  scope.addEventListener?.("message", onShutdownFrame)
  // A shutdown delivered synchronously during registration wins the whole boot:
  // never open a DB (or install an executor) for an isolate already being torn
  // down.
  if (shutdownRequested) return { dispose: async () => {} }

  try {
    // The real `SessionPrompt` loop needs the worker's own WAL connection and an
    // ambient `InstanceRef`; only the full hub-port wiring provides them. The
    // degraded (no-port) boot runs the reduced pipeline.
    const usePrompt = servesWorkerPorts(init.capabilities)
    if (usePrompt) {
      // One connection per worker generation: the schema gate is read-only and a
      // mismatch refuses boot before any run starts (D5/D7).
      database = WorkerRuntime.runSync(
        openWorkerDatabase({
          filename: Database.path(),
          ...(init.sessionID === undefined ? {} : { sessionID: init.sessionID }),
        }),
      )
      const egressChannel = new MessageChannel()
      // A dedicated control plane, so durable wakeups and resync never queue
      // behind the delta stream; without it the egress mesh is dead in production.
      const egressControlChannel = new MessageChannel()
      scope.postMessage(
        {
          type: "ports",
          egress: egressChannel.port2,
          egressControl: egressControlChannel.port2,
        },
        [egressChannel.port2, egressControlChannel.port2].map(transferable),
      )
      clients = buildWorkerClients(
        {
          egress: egressChannel.port1,
          egressControl: egressControlChannel.port1,
        },
        {
          workerID: reportedWorkerID(init, threadId),
          bridge,
          database,
          ...(init.sessionID === undefined ? {} : { sessionID: init.sessionID }),
        },
      )
      // A test may pin a write transaction on the worker's own connection before
      // delivering the shutdown frame, proving the orderly close releases the WAL.
      options.observeWriter?.(clients.writer)
      // The hub serves the `SessionStatus` / `Permission` / `Question`
      // authorities over one control port; a single multiplexing client backs all
      // three proxies (each call is correlated by the client's own request id).
      const hubProxyChannel = new MessageChannel()
      scope.postMessage({ type: "hub-proxy", port: hubProxyChannel.port2 }, [transferable(hubProxyChannel.port2)])
      const client = createHubProxyClient(hubProxyChannel.port1)
      hubProxyClient = client
      options.observeHubProxy?.(client)
      hubProxies = { sessionStatus: client, permission: client, question: client }
      // The real prompt runs in this isolate; report the durable lifecycle facts
      // (commit, tool start/finish, phase) to the hub tracker over the same
      // control port before the effects they guard.
      installReporting(createProxyReporting((method, args) => client.call(method as never, args)))
      // The shell tool's detached children are reaped by the hub, so the same
      // control port carries their `pid-report` frames (the hub distinguishes the
      // two frame shapes). Only the full-port boot owns children.
      pidReporter = createPidReporter(hubProxyChannel.port1, reportedWorkerID(init, threadId), bridge)
    } else {
      clients = directClients()
    }

    const config = modelConfig(init.model)
    // The worker runs the model-facing plugin hooks in its own isolate; build the
    // host from the shipped hooks so their `chat.params`/`chat.headers` mutations
    // reach the reduced LLM exactly as flag-off. Both the system and messages
    // transforms are hub proxies (the hub owns plugin loading and returns the
    // mutated arrays): with no hub client (degraded boot) each is omitted and its
    // array passes through unchanged.
    const systemTransform = hubProxySystemTransform(hubProxies?.sessionStatus)
    const messagesTransform = hubProxyMessagesTransform(hubProxies?.sessionStatus)
    const hubHook = hubProxyPluginTrigger(hubProxies?.sessionStatus)
    // The executor, abort and list seams all ride the SAME multiplexed control
    // client (`hubProxies.sessionStatus`): the hub distinguishes the methods, so no
    // second port is opened. The abort/list seams are surfaced on the bootstrap
    // handle for the registry/cancel owners to consume.
    const pluginToolExecutor = hubProxyPluginTool(hubProxies?.sessionStatus)
    const pluginToolAbort = hubProxyPluginToolAbort(hubProxies?.sessionStatus)
    const pluginToolList = hubProxyPluginToolList(hubProxies?.sessionStatus)
    const plugins =
      init.plugins === undefined
        ? undefined
        : createWorkerPluginHost(init.plugins, {
            ...(systemTransform === undefined ? {} : { systemTransform }),
            ...(messagesTransform === undefined ? {} : { messagesTransform }),
            ...(hubHook === undefined ? {} : { hubHook }),
          })
    // Hub-executed plugin tools need BOTH the clone-safe descriptors the hub
    // shipped and the hub-port executor. A descriptor without an executor (or the
    // reverse) cannot execute, so the option is omitted entirely: degraded boot
    // lists no plugin tools rather than a stub that would fail on call.
    // A live handle, not a frozen literal: `refresh` swaps the descriptor set in
    // place after a hub-side plugin reload, and the worker's `Plugin.Service.list()`
    // reads the current set on every call (design Q4).
    const pluginTools =
      pluginToolExecutor === undefined || init.pluginTools === undefined || init.pluginTools.length === 0
        ? undefined
        : refreshablePluginTools(init.pluginTools, pluginToolExecutor, {
            // The abort and list seams ride the same control client as the
            // executor; attaching them here makes each rebuilt remote definition
            // carry a working abort bridge and lazy-refresh action.
            ...(pluginToolAbort === undefined ? {} : { abort: pluginToolAbort }),
            ...(pluginToolList === undefined ? {} : { list: pluginToolList }),
          })
    // Pass the real hub-proxy clients so the reduced LLM sub-graph routes
    // `Permission` over the hub control port (never a silent mock); absent
    // (degraded boot) the sub-graph uses a fail-loud boundary. The same live
    // `pluginTools` handle reaches the reduced sub-graph's `Plugin` replacement, so
    // a future tool-consuming reduced node sees the refreshed set too.
    const modelClient = llmModelClientLayer(config).pipe(
      Layer.provide(reducedLlmLayer(init.snapshot, config, hubProxies, plugins, pluginTools)),
    )

    // One process-local `BackgroundJob` registry per worker, built on a scope that
    // lives for the isolate. `createSessionWork` provides the graph layer per run,
    // so a `Layer.effect` registry would be rebuilt (and its jobs interrupted) at
    // every turn boundary; a single `Layer.succeed` value keeps a background job
    // alive after the turn that started it, which the idle-reap probe observes.
    backgroundJobScope = Scope.makeUnsafe()
    const backgroundJobContext = WorkerRuntime.runSync(Layer.buildWithScope(localBackgroundJobLayer(), backgroundJobScope))
    const backgroundJobs = Context.get(backgroundJobContext, BackgroundJob.Service)
    // A full-port worker mirrors its jobs over the control port so the hub's
    // promotion endpoint can see foreground jobs; a degraded boot stays local.
    const syncedBackgroundJobs =
      hubProxyClient === undefined ? backgroundJobs : createHubSyncedBackgroundJobs(backgroundJobs, hubProxyClient)
    options.observeBackgroundJobs?.(syncedBackgroundJobs)
    const backgroundJobLayer = Layer.succeed(BackgroundJob.Service, syncedBackgroundJobs)

    const runtime = buildSessionWorkerLayer({
      // The full runnable graph needs the writer-backed Database and the serialized
      // model catalog the hub ships; the degraded boot runs the reduced pipeline.
      // The runnable graph also carries the worker EventV2 bridge, which rides the
      // transferred `egress` port and is disposed with the layer scope, so it is
      // built exactly when the events/egress port is available (`usePrompt`).
      ...(usePrompt ? {} : { nodes: PIPELINE_SESSION_NODES }),
      snapshot: init.snapshot,
      env: init.env,
      // PENDING hub-a: `node-table.ts`/`layer.ts` still declare an unused
      // `BackgroundJobClientNode`; supply its inert handle here until that removal
      // lands (the session worker owns its jobs through the process-local service).
      clients: {
        ...clients,
        ...(database === undefined ? {} : { database }),
        backgroundJob: { wait: async () => ({ timedOut: true }), cancel: async () => {} },
      },
      modelClient,
      backgroundJob: backgroundJobLayer,
      // The worker serves exactly one serialized model, so the reduced provider is
      // the graph's provider (the real loop's `LLM` and `SessionPrompt` use it), and
      // snapshot calls run on the hub over hub-proxy. With
      // a control-port reporter, `AppProcess` is replaced by the reporting wrapper
      // so a mid-flight `rg` child's PGID reaches the hub mirror before terminate.
      replacements: workerGraphReplacements(config, pidReporter, hubProxies?.sessionStatus),
      // The worker-hosted plugin host reaches the graph's `LLM`/`Plugin` node too,
      // not just the `ModelClient` sealed below, so plugin-set sampling/headers
      // apply identically in both. Absent keeps the graph's no-op pass-through.
      ...(plugins === undefined ? {} : { plugins }),
      ...(pluginTools === undefined ? {} : { pluginTools }),
      ...(hubProxies === undefined ? {} : { hubProxies }),
      // The worker's one session: every hub-proxied child operation is authorized
      // against it, so this isolate can never read or write another session's rows.
      ...(init.sessionID === undefined ? {} : { boundSessionID: SessionID.make(init.sessionID) }),
      ...(pidReporter === undefined ? {} : { pidReporter }),
      // The worker's Effect logs reach the main `opencode.log` sink. Merged as a
      // non-graph seam, so the node table and its assertion are untouched.
      logging,
    })

    // Build the graph exactly once on a worker-lifetime scope. Every turn provides
    // the cached context (the per-worker DB included) and only its own run scope;
    // the old per-run `Effect.provide(layer)` disposed the DB after the first turn
    // The scope is closed by `dispose()`.
    workerScope = Scope.makeUnsafe()

    // The worker-local run control is registered once the first `run` resolves the
    // worker's `SessionRunState`; the executor proxy reads it live.
    let control: WorkerRunControl | undefined
    // A cancel can arrive while the first run is still building its graph, before
    // any control exists. Latch it here and let the run consume it at start (or
    // after the build). The latch is scoped to a run that is actually in flight:
    // an idle cancel must never interrupt the NEXT run (pipeline-only graphs
    // never register a control at all).
    const preRunCancels = new Set<SessionID>()
    const activeRuns = new Map<SessionID, Deferred.Deferred<void>>()
    const sessionWork = createSessionWork({
      layer: runtime.layer,
      scope: workerScope,
      writer: clients.writer,
      threadId,
      prompt: usePrompt,
      instance: workerInstanceContext(init.snapshot),
      registerControl: (registered) => {
        control = registered
      },
      consumePreRunCancel: (sessionID) => preRunCancels.delete(sessionID),
    })
    const work = productionWorkRegistry((sessionID, prompt, shell) => {
      const ended = Deferred.makeUnsafe<void>()
      activeRuns.set(sessionID, ended)
      return sessionWork(sessionID, prompt, shell).pipe(
        // A run's pre-run latch dies with the run: a cancel delivered after its
        // last consume point can never outlive it into the next turn.
        Effect.ensuring(
          Effect.sync(() => {
            activeRuns.delete(sessionID)
            preRunCancels.delete(sessionID)
            Deferred.doneUnsafe(ended, Effect.void)
          }),
        ),
      )
    })
    const slot = init.workerID
    const installExecutor = options.installExecutor ?? installSessionExecutor
    installed = installExecutor(scope, {
      work,
      threadId,
      // The control is registered when the first `run` resolves the worker's
      // `SessionRunState`; the proxy reads it live so a cancel after registration
      // reaches the real loop (table B). Before that there is nothing to cancel.
      control: {
        cancel: (sessionID) => {
          const registered = control
          if (registered !== undefined) {
            const ended = activeRuns.get(sessionID)
            if (ended === undefined) return registered.cancel(sessionID)
            return WorkerRuntime.runPromise(cancelOnceBusy(registered, sessionID, ended))
          }
          // No graph/control yet: latch the cancel only for a run that is
          // building. With no run in flight there is nothing to cancel, so the
          // latch must not carry into the next run.
          if (activeRuns.has(sessionID)) preRunCancels.add(sessionID)
          return Promise.resolve()
        },
        busy: (sessionID) => control?.busy(sessionID) ?? Promise.resolve(false),
      },
      // A running process-local background job keeps the isolate alive across an
      // idle-reap window. Kept separate from `busy`: a background job must
      // never block the next prompt.
      activeWork: createBackgroundWorkProbe(backgroundJobs),
      // Dynamic OS thread name (Linux-only, fail-soft): `ocs<slot>` at rest and
      // `ocs<slot>:<last6>` while a run is live. `onIdle` fires on every terminal
      // (settle, cooperative cancel, interrupt), so the name always resets. Both
      // callbacks are fail-soft in `execute.ts`, so naming never unwinds a run.
      // Skipped when the hub shipped no slot id.
      ...(slot === undefined
        ? {}
        : {
            onRunStart: (sessionID) => namer.bind(sessionID, true),
            onIdle: () => Effect.sync(() => namer.onIdle()),
          }),
    })

    return {
      dispose() {
        return disposeBootstrap()
      },
    }
  } catch (error) {
    // A failed mid-bootstrap closes locally (SQLite handle/WAL included)
    // before the error surfaces, so the isolate is never the only closer.
    disposeBootstrapSync()
    throw error
  }
}

/**
 * Deliver a cancel to a run that is admitted but not yet busy in the run table
 * (its prompt is still being persisted). Resolves once the cancel is delivered
 * or the run ends first.
 */
export const cancelOnceBusy = (
  control: WorkerRunControl,
  sessionID: SessionID,
  ended: Deferred.Deferred<void>,
): Effect.Effect<void> =>
  Effect.gen(function* () {
    const busy = yield* Effect.raceFirst(
      control.awaitBusy(sessionID).pipe(Effect.as(true)),
      Deferred.await(ended).pipe(Effect.as(false)),
    )
    if (busy) yield* Effect.tryPromise(() => control.cancel(sessionID)).pipe(Effect.orDie)
  })

/**
 * The worker's ambient `InstanceRef`, rebuilt from the hub-shipped snapshot. The
 * isolate owns no project loader, so the snapshot is the single source of truth:
 * the directory is the working directory, the worktree defaults to it, and the
 * project identity comes from the hub's real instance context. The old
 * `project: undefined as never` placeholder made every `ctx.project.id` reader
 * (`Snapshot.state`, `Session.create`, the event bridge) throw on boot.
 *
 * The project id falls back to the directory only when the hub dispatched with
 * no ambient project (a hub-side gap, see `hub.ts` `fallbackInstanceContext`),
 * so the worker still boots instead of crashing on a missing id.
 */
export function workerInstanceContext(snapshot: WorkerSnapshot): InstanceContext {
  return makeInstanceContext({
    directory: snapshot.directory,
    ...(snapshot.worktree === undefined ? {} : { worktree: snapshot.worktree }),
    projectID: snapshot.projectId ?? snapshot.directory,
    ...(snapshot.projectVcs === undefined ? {} : { projectVcs: snapshot.projectVcs }),
  })
}

if (workerData !== null && workerData !== undefined) {
  const init = parseSessionWorkerInit(workerData)
  // Adopt the shipped role BEFORE any heavy bootstrap, so a dynamic app-runtime
  // import during bootstrap sees `session` and never contributes a nested hub.
  setProcessRole(init.role)
  // Thread naming is owned by `bootstrapSessionWorker` (`namer.bind`), which
  // writes the boot name exactly once; this entry only picks the role.
  bootstrapSessionWorker(init, globalThis as unknown as WorkerScope)
}
