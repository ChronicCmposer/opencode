// The pure hub.
//
// The hub is the composition of three landed layers:
//   * the per-session `Dispatcher` — one worker per session, reused across turns
//     and idle-reaped, with the active key→worker map;
//   * the executor pool — workers whose run-state machine lives inside
//     the isolate (`execute.ts`);
//   * the cooperative cancel path — `cancelWithGrace`.
//
// It owns NO session execution: every session runs in a worker isolate, the hub
// only schedules and forwards control. `SessionHubService` is the Effect service
// `run-state.ts` delegates to when `experimentalSessionWorkers` is on; the
// pool-spawning layer node (`makeSessionHubNode`) provides it and tears the pool
// down with the layer scope.

import { Context, Deferred, Effect, Fiber, Layer, Option } from "effect"
import { z } from "zod"
import os from "node:os"
import path from "node:path"
import type { Hooks, ToolContext as PluginToolContext, ToolDefinition } from "@opencode-ai/plugin"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { ModelsDev } from "@opencode-ai/core/models-dev"
import { BackgroundJob } from "@opencode-ai/core/background-job"
import type { Info as BackgroundJobInfo, Status as BackgroundJobStatus } from "@opencode-ai/core/background-job"
import { Database } from "@opencode-ai/core/database/database"
import { Auth } from "@/auth"
import { Config } from "@/config/config"
import { Provider } from "@/provider/provider"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { InstanceRef } from "@/effect/instance-ref"
import { EffectBridge } from "@/effect/bridge"
import { SessionStatus } from "@/session/status"
import { Permission } from "@/permission"
import { Question } from "@/question"
import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import type { InstanceContext } from "@/project/instance-context"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Project } from "@opencode-ai/schema/project"
import { createEgressReceiver } from "@/session/egress/receiver"
import { attachMeshWorker, createEgressMesh, type DurableCommit, type EgressMesh } from "@/session/egress/mesh"
import { durableAggregateMetaOf, type EventV2Event, type EventsAckPayload } from "./frame"
import { encodeFrame, resyncWithPositionFrame, type DurableResyncPayload } from "@/session/egress/frame"
import { createLiveEventPublisher, instanceLocation, EventV2Bridge, type LiveEventPublisher } from "@/event-v2-bridge"
import type { EventV2 } from "@opencode-ai/core/event"
import { StateCache } from "@opencode-ai/core/bus"
import type { SessionID } from "@/session/schema"
import { createDbExecutor, type DbExecutor } from "./db-executor"
import { nextDepth, spawnDepth } from "@/session/depth"
import type { DispatchOrigin } from "@/session/origin"
import { defined } from "@/util/object"
import { setThreadName } from "@/util/thread-name"
import {
  createDispatcher,
  type CancellationConfig,
  type CancelledMode,
  type Dispatcher,
  type DispatcherOptions,
  type SessionWorker,
} from "./dispatch"
import {
  SessionScopeError,
  type SessionCancelRequest,
  type SessionChildrenRequest,
  type SessionCreateRequest,
  type SessionGetRequest,
  type SessionSpawnRequest,
  type SessionSpawnResult,
} from "./subagent"
import { createLifecycleTracker, type LifecycleTracker } from "@/session/ledger/lifecycle"
import { decideAutoResume, type AutoResumeDecision, type IndeterminateCall } from "@/session/ledger/resume"
import {
  RUN_BARRIER_TIMEOUT_MS,
  RunBarrierTimeout,
  createRunBarrier,
  type RunBarrier,
  type RunBarrierPhases,
} from "@/session/run-barrier"
import {
  createExecutorCancelPort,
  createExecutorWorker,
  createExecutorWorkerPool,
  createSessionDispatch,
  SessionWorkerReplyTimeoutError,
  type ExecutorTransport,
  type ExecutorWorker,
  type ExecutorWorkerPool,
  type SessionDispatch,
  type SessionExecutionResult,
  type SessionExecutorInbound,
  type SessionExecutorOutbound,
  type WorkerSpawnReason,
} from "./execute"
import {
  BOOT_CANCELLED,
  createBootLifecycle,
  resolveCancelGraceMs,
  type BootCancelResult,
  type BootLifecycle,
  type BootTelemetry,
  type CancelOutcome,
} from "./cancel"
import { createSpawnGate, type SpawnGateHandle, type SpawnPermit } from "./spawn-gate"
import { SESSION_WORKER_IDLE_TIMEOUT_ENV, WORKER_COUNT_ENV } from "./pool"
import { createEpochSequence, makeInstanceContext, type EpochSequence } from "./snapshot"
import {
  buildSessionWorkerInit,
  mergeEnv,
  resolveWorkerPlugins,
  type BuildSessionWorkerInitOptions,
  type WorkerModelSelection,
  type WorkerPluginSource,
} from "./init"
import {
  createPidRegistry,
  reapSupport,
  registerReported,
  type PidRegistry,
  type PidReport,
  type PidReportEntry,
} from "./pid-registry"
import { Plugin } from "@/plugin"
import { UnsupportedPluginHookError, workerHookRoute } from "@/plugin/routes"
import type {
  MessagesTransformInput,
  MessagesTransformOutput,
  SystemTransformInput,
  SystemTransformOutput,
  WorkerPluginHooks,
} from "./plugin-host"
import type { HubProxyHandlers, HubProxyStreamHandler } from "./hub-proxy-wire"
import { serveHubProxy } from "./hub-proxy-wire"
import { BuildMismatch, verifyReadyBuild } from "./build-hash"
import type {
  PluginToolDescriptor,
  PluginToolExecuteInput,
  PluginToolExecuteResult,
  PluginToolProgress,
  PluginToolProgressSink,
} from "./plugin-tool-wire"
import { PluginToolNotSerializableError } from "./plugin-tool-wire"
import { errorTagOf, portFromMessagePort } from "./port"
import type { SessionWorkerInit } from "./session-worker"
import { noopWorkerLogger, workerLogMessage, workerLoggerFromBridge, type WorkerLogger } from "./log"
import { sessionThreadName } from "@/session/worker/thread-name"
import { Session } from "@/session/session"
import type { SessionPrompt } from "@/session/prompt"
import { Truncate } from "@/tool/truncate"
import { pluginToolJsonSchema } from "@/tool/registry"
import { MCP } from "@/mcp"
import { McpCatalog } from "@/mcp/catalog"
import { LspTool, Parameters as LspParameters } from "@/tool/lsp"
import DESCRIPTION_LSP from "@/tool/lsp.txt"
import { ToolJsonSchema } from "@/tool/json-schema"
import type { Tool as ToolDef } from "@/tool/tool"
import { LSP } from "@/lsp/lsp"
import { Agent } from "@/agent/agent"
import type { ShellInput } from "@/session/prompt"
import { isWorkerCrashPhase, recordAsync } from "@/session/ledger/crash"
import { ensureProductionSchema } from "@/session/db/schema"

/**
 * Advisory hub cache. Entries are keyed by `(aggregateID, seq)` and fenced by the
 * cache's state version; it is never authoritative. The durable database and log
 * remain the only truth (D14).
 */
export type HubTruthCache = StateCache<unknown>

/** The invalidator a durable event source calls after a commit. */
export interface HubCacheInvalidation {
  invalidate(aggregateID: string): void
}

/** The five crash phases a worker reports (D8). */
export const WORKER_CRASH_PHASES = ["boot", "materialize", "stream", "tool", "commit"] as const
export type WorkerCrashPhase = (typeof WORKER_CRASH_PHASES)[number]

const WORKER_CRASH_PHASE_SET: ReadonlySet<string> = new Set(WORKER_CRASH_PHASES)

/**
 * The D8 crash payload a worker or dispatcher supplies to the hub. It is the
 * persistence contract, not a hub-invented summary: every field comes from the
 * crashing generation.
 */
export interface WorkerCrashed {
  readonly sessionID: string
  readonly workerID: number
  readonly phase: WorkerCrashPhase
  readonly lastCommittedMessageID: string | null
  readonly inFlightCallIDs: readonly string[]
  readonly retryable: false
  readonly reason: string
}

/** Parse an untrusted crash payload at the boundary. */
export function isWorkerCrashed(value: unknown): value is WorkerCrashed {
  if (typeof value !== "object" || value === null) return false
  const payload = value as Partial<WorkerCrashed>
  return (
    typeof payload.sessionID === "string" &&
    typeof payload.workerID === "number" &&
    Number.isInteger(payload.workerID) &&
    payload.workerID >= 0 &&
    typeof payload.phase === "string" &&
    WORKER_CRASH_PHASE_SET.has(payload.phase) &&
    (typeof payload.lastCommittedMessageID === "string" || payload.lastCommittedMessageID === null) &&
    Array.isArray(payload.inFlightCallIDs) &&
    payload.inFlightCallIDs.every((id) => typeof id === "string") &&
    payload.retryable === false &&
    typeof payload.reason === "string"
  )
}

/**
 * Read the typed `WorkerCrashed` payload a crash error carries. The shape may be
 * attached as `workerCrashed`, `crash`, or on the cause; no payload means the W3
 * contract is absent, so the hub must not fabricate one.
 */
export function workerCrashedFrom(error: unknown): WorkerCrashed | undefined {
  if (typeof error !== "object" || error === null) return undefined
  const candidate = error as {
    readonly workerCrashed?: unknown
    readonly crash?: unknown
    readonly payload?: unknown
    readonly cause?: unknown
  }
  if (isWorkerCrashed(candidate.workerCrashed)) return candidate.workerCrashed
  if (isWorkerCrashed(candidate.crash)) return candidate.crash
  const cause = workerCrashedFrom(candidate.cause)
  if (cause !== undefined) return cause
  // The dispatcher exposes the synthesized D8 payload as a getter.
  return isWorkerCrashed(candidate.payload) ? candidate.payload : undefined
}

/** A descendant cancelled by the subagent cascade, and whether it needs a fallback idle. */
export interface CancelledDescendant {
  readonly sessionID: SessionID
  /**
   * `true` when the descendant's run did NOT settle its own worker — it was
   * queued, never admitted, or grace-terminated — so no worker published its
   * terminal idle and the caller must write the fallback idle. `false` when the
   * run settled cooperatively or completed, so its worker published the idle.
   */
  readonly needsFallbackIdle: boolean
}

/**
 * The result of cancelling a session and cascading to its subagent children.
 * The dispatcher discards a descendant's own outcome, so the hub reconstructs
 * the terminal modes it observed and reports which sessions still need an idle.
 */
export interface CancelReport {
  /** The requested session's outcome; `undefined` when it was not admitted. */
  readonly outcome: CancelOutcome | undefined
  /** Every descendant the cascade reached, plus any linked but unsettled child. */
  readonly descendants: ReadonlyArray<CancelledDescendant>
}

/** The hub surface `run-state.ts` consumes when the rollout flag is on. */
export interface SessionHub {
  readonly dispatcher: Dispatcher
  /**
   * The hub-observed run lifecycle truth (last committed message, in-flight tool
   * calls, barrier phases). The tool/commit paths feed it; the crash path reads
   * it synchronously and auto-resume applies its policy.
   */
  readonly lifecycle: LifecycleTracker
  /** The per-session run barrier the next run waits on. */
  readonly barrier: RunBarrier
  /**
   * The advisory hub cache. Never authoritative: the durable database and log
   * remain the only truth (D14). It is empty on construction (a restart starts
   * cold) and cleared on shutdown.
   */
  readonly cache?: HubTruthCache
  /**
   * The session a worker is currently bound to (the dispatcher's key→worker map,
   * inverted). A worker isolate serves exactly one session at a time, so this is
   * the resolver the hub-proxy and live-event paths use to reach the worker's own
   * instance context instead of latching the first dispatched one.
   */
  sessionOfWorker(workerID: number): SessionID | undefined
  /**
   * Serve the worker's transferred egress ports against the hub's
   * authorities. Returns a disposer; idempotent per worker. No-op for a port the
   * hub has no backend for. The writer port is never served (D5).
   */
  attachWorkerPorts(workerID: number, ports: WorkerTransferredPorts): () => void
  /**
   * Serve the three hub-proxy authorities (`SessionStatus` / `Permission` /
   * `Question`) over a worker's control port. Returns a disposer; idempotent per
   * worker; no-op when the hub has no hub-proxy handlers.
   */
  attachHubProxyPort(workerID: number, port: MessagePort): () => void
  /** Dispatch `sessionID` to a worker and resolve with its result (admission-aware). */
  ensureRunning(sessionID: SessionID, origin?: DispatchOrigin): Promise<SessionExecutionResult>
  /**
   * Dispatch a shell turn. `shell` is the serialized `ShellInput` the worker
   * replays through `SessionPrompt.shell`; absent keeps the plain startShell path.
   */
  startShell(sessionID: SessionID, shell?: ShellInput): Promise<SessionExecutionResult>
  assertNotBusy(sessionID: SessionID): Promise<boolean>
  /** The UI-stop path: cooperative checkpoint, then terminate after the grace window. */
  cancel(sessionID: SessionID): Promise<CancelOutcome | undefined>
  /**
   * Cancel a session and its subagent cascade, returning the requested session's
   * outcome AND every cascaded descendant with whether its worker published the
   * terminal idle. `run-state.ts` consumes this to write the fallback idle
   * exactly once per cancelled session (a cooperative settle publishes its own;
   * a queued or grace-terminated one does not).
   */
  cancelWithReport(sessionID: SessionID): Promise<CancelReport>
  cancelAll(): Promise<void>
  terminateAll(): Promise<void>
}

/**
 * The worker data ports transferred as one `{type:"ports", …}` frame
 * (session-worker.ts). The writer port is intentionally absent: the hub serves no
 * SQLite RPC (D5).
 */
export interface WorkerTransferredPorts {
  readonly egress?: MessagePort
  readonly egressControl?: MessagePort
}

/** The data ports present in a transfer, in a stable order (for diagnostics). */
const TRANSFERRED_PORT_NAMES = ["egress", "egressControl"] as const
function transferredPortNames(ports: WorkerTransferredPorts): string[] {
  return TRANSFERRED_PORT_NAMES.filter((name) => ports[name] !== undefined)
}

/**
 * Whether `frame` is a well-formed {@link PidReport} (fail-closed parse). A pid
 * report is a plain data frame carried on the worker control (hub-proxy) port,
 * recognised by its `type` rather than the hub-proxy `kind` discriminator.
 */
export function isPidReport(frame: unknown): frame is PidReport {
  if (typeof frame !== "object" || frame === null) return false
  const candidate = frame as Partial<PidReport>
  if (candidate.type !== "pid-report") return false
  if (typeof candidate.workerID !== "number") return false
  if (!Array.isArray(candidate.entries)) return false
  return candidate.entries.every(
    (entry) =>
      typeof entry === "object" &&
      entry !== null &&
      typeof (entry as PidReportEntry).pid === "number" &&
      typeof (entry as PidReportEntry).pgid === "number" &&
      typeof (entry as PidReportEntry).label === "string" &&
      typeof (entry as PidReportEntry).workerID === "number",
  )
}

/**
 * The hub-resident authorities a worker's ports are served against. `egress`
 * defaults to a draining receiver (the SSE route wires a real sink later);
 * `hubProxy` is served over the worker control port when present. There is
 * deliberately no writer RPC (D5) and no broker authority (D20).
 */
export interface WorkerPortBackends {
  /** Serve a worker's egress port; defaults to a draining receiver. */
  readonly egress?: (workerID: number, port: MessagePort, control?: MessagePort) => () => void
  /** Handlers for the three hub-proxy authorities served over the control port. */
  readonly hubProxy?: HubProxyHandlers
  /**
   * Per-worker hub-proxy handlers. A worker is bound to exactly ONE session, so
   * the production backend builds each control port's handler map under that
   * worker's session context (never a first-dispatch latch). Takes precedence
   * over {@link hubProxy} when both are present.
   */
  readonly hubProxyFor?: (workerID: number) => HubProxyHandlers | undefined
  /**
   * Per-worker streaming hub-proxy handlers: the methods that may push correlated
   * events (e.g. `pluginTool.execute`'s metadata) to the awaiting worker before
   * replying. Served alongside {@link hubProxyFor}; takes precedence over
   * {@link hubProxyStreaming}.
   */
  readonly hubProxyStreamingFor?: (workerID: number) => Readonly<Record<string, HubProxyStreamHandler>> | undefined
  /** Streaming hub-proxy handlers shared by every worker (fallback for {@link hubProxyStreamingFor}). */
  readonly hubProxyStreaming?: Readonly<Record<string, HubProxyStreamHandler>>
  /**
   * Tear down the backends' own resources. Called once by
   * `SessionHub.terminateAll`, only when the backends were actually built.
   */
  readonly dispose?: () => void | Promise<void>
}

/**
 * The hub's port authorities, either ready to use or built on demand. A source
 * function is invoked at most once, on the first worker port attach (i.e. on the
 * first dispatch), so building a production node does no work at graph-build
 * time.
 */
export type WorkerPortBackendsSource = WorkerPortBackends | (() => WorkerPortBackends)

export class SessionHubService extends Context.Service<SessionHubService, SessionHub>()("@opencode/SessionHub") {}

/**
 * Cancellation wiring, either pre-built or derived from the materialised pool
 * (the production case needs the pool to look up a worker's `cancelPort`).
 */
export type CancellationSource = CancellationConfig | ((pool: ExecutorWorkerPool) => CancellationConfig)

export interface SessionHubOptions {
  /** The executor pool; lazy pools spawn no isolate until the first dispatch. */
  readonly pool: ExecutorWorkerPool
  /**
   * Hub-resident authorities for the worker egress ports and the
   * hub-proxy control port. When present, the hub serves transferred worker
   * ports; the production init advertises `capabilities.sessionWorkerPorts`.
   */
  readonly ports?: WorkerPortBackendsSource
  /**
   * Invoked synchronously at the start of every `ensureRunning`/`startShell`,
   * before any async work, with the session being dispatched. Used by the
   * production hub to capture the caller's ambient `InstanceRef` and runtime
   * context (the `attach` pattern), and to remember which session's persisted
   * model the worker init should resolve.
   */
  readonly onDispatch?: (sessionID: SessionID) => void
  /**
   * Resolve a session's subagent depth (`metadata.depth`) for the dispatch path.
   * Depth 0 is a foreground root; a positive depth takes bounded child headroom
   * so a parent awaiting a `task` child never deadlocks the running-worker cap.
   * Values are clamped to a safe non-negative integer; absent/malformed is 0.
   */
  readonly depthOf?: (sessionID: SessionID) => number | undefined
  readonly cancellation?: CancellationSource
  /** Hub-side cleanup when a session's worker is retired (reap/cancel/crash). */
  readonly onSessionReleased?: (sessionID: SessionID) => void
  /**
   * Idle-reap window forwarded to the dispatcher. Absent uses the dispatcher's
   * env/default resolution; tests inject a short window.
   */
  readonly reapMs?: number
  /** Scheduler seam for the dispatcher's idle-reap timer (tests). */
  readonly schedule?: DispatcherOptions["schedule"]
  /**
   * Per-session durable-write barrier (plan 2.6 / R4#16): the dispatcher holds a
   * completed run's slot until this resolves, so the next run can never overtake
   * the prior run's durable egress. Absent means a released slot is immediate.
   */
  readonly writeBarrier?: DispatcherOptions["writeBarrier"]
  /**
   * Live-only event publisher. When present, the hub republishes worker events
   * from their port and publishes `session.cancelled` after a cancel.
   */
  readonly events?: LiveEventPublisher
  /**
   * Resolve the live publisher for a specific session, so `session.cancelled`
   * carries THAT session's location instead of a first-dispatch one. Falls back
   * to {@link events} when absent.
   */
  readonly eventPublisherFor?: (sessionID: SessionID) => LiveEventPublisher | undefined
  /**
   * Invoked at the start of every cancel, before the grace window, for the session
   * and each descendant. The production layer aborts in-flight hub-executed plugin
   * tools here; a plain hub without it simply skips that best-effort unwind.
   */
  readonly onCancelRequested?: (sessionID: SessionID) => void
  /**
   * Receive a worker's `{type:"pid-report"}` frame (plan 11.5) from its control
   * port. `workerID` is the authoritative owner (the port association), not the
   * worker's self-reported id. The production hub mirrors each entry into its
   * `PidRegistry`; absent means pid reports are ignored (a pure hub, no reaper).
   */
  readonly onPidReport?: (workerID: number, report: PidReport) => void
  /**
   * Off-loop fallback for hub-side SQL. The executor owns no ordering, cache,
   * migration, or schema policy; callers own every query (D5).
   */
  readonly db?: DbExecutor
  /** Advisory cache, never authoritative. Defaults to a fresh, empty cache. */
  readonly cache?: HubTruthCache
  /**
   * Persist/surface the typed D8 payload a crashed generation reports. When the
   * crash error carries no payload the hub logs the missing contract and does not
   * fabricate one.
   */
  readonly onWorkerCrashed?: (payload: WorkerCrashed) => void | Promise<void>
  /**
   * The run lifecycle truth shared with the tool/commit paths. Absent creates a
   * fresh bounded tracker owned by this hub.
   */
  readonly lifecycle?: LifecycleTracker
  /** The run barrier. Absent creates the production 5 s barrier. */
  readonly barrier?: RunBarrier
  /** Barrier phases for a session; defaults to the lifecycle tracker's phases. */
  readonly barrierPhases?: (sessionID: SessionID) => RunBarrierPhases
  /**
   * Re-dispatch after an allowed auto-resume. The production layer re-enters
   * `ensureRunning`; absent means the caller still observes the decision but
   * owns the replay.
   */
  readonly resumeRun?: (
    decision: Extract<AutoResumeDecision, { resume: true }>,
    payload: WorkerCrashed,
  ) => void | Promise<void>
  /** Optional structured logger seam; absent defaults to a no-op (flag-off behavior). */
  readonly log?: WorkerLogger
}

/**
 * Serve a worker control port for `{type:"pid-report"}` frames, forwarding each
 * well-formed frame to `onReport`. `portFromMessagePort` registers an
 * `addEventListener` listener, so this coexists with the hub-proxy request
 * server on the same port. Returns a disposer.
 */
export function servePidReportPort(onReport: (frame: PidReport) => void, port: MessagePort): () => void {
  const channel = portFromMessagePort<unknown>(port)
  return channel.onMessage((frame) => {
    if (isPidReport(frame)) onReport(frame)
  })
}

/** Default egress serving: drain the worker's frames (the SSE route wires a real sink later). */
export function drainEgressPort(port: MessagePort): () => void {
  const receiver = createEgressReceiver({ port, onFrame: () => {} })
  return () => receiver.close()
}

// ---------------------------------------------------------------------------
// Hub events sink (spec Q15/Q16/Q22): worker EventV2 frames → hub event surface.
// ---------------------------------------------------------------------------

/**
 * The canonical `session.cancelled` event type, sourced from the schema definition
 * the hub publishes after a cancel resolves.
 */
export const SESSION_CANCELLED_TYPE = SessionV1.Event.Cancelled.type

export interface HubEventSinkOptions {
  /** Live-only publisher the sink republishes each event through. */
  readonly publisher: LiveEventPublisher
  /** Advisory cache invalidated when a durable event reaches the hub. */
  readonly cache?: HubCacheInvalidation
  /** Resolve a durable event's aggregate id; defaults to its `sessionID`. */
  readonly aggregateOf?: (event: EventV2Event) => string | undefined
}

/** The aggregate an event belongs to, when its payload identifies one. */
export function aggregateIDOfEvent(event: EventV2Event): string | undefined {
  if (typeof event.properties !== "object" || event.properties === null) return undefined
  const properties = event.properties as Record<string, unknown>
  for (const field of ["sessionID", "aggregateID", "aggregate_id"]) {
    const value = properties[field]
    if (typeof value === "string" && value.length > 0) return value
  }
  return undefined
}

/**
 * A worker's events-port sink. Each event is republished live-only — never
 * re-committed, because a durable worker event already committed in its isolate.
 * `end`/`resync`/`ack` only update flow-control state: the sink owns no buffering,
 * so the overflow/resync policy lives with the receiver that owns backpressure.
 */
export interface HubEventSink {
  /**
   * Republish one worker event live-only. `durable` marks an event whose commit
   * already happened in the worker, so the hub cache must drop the aggregate.
   * `aggregateID`/`seq` are the frame's committed identity; when present they are
   * authoritative for cache invalidation (the payload is not re-parsed to guess).
   */
  event(
    event: EventV2Event,
    options?: {
      readonly durable?: boolean
      readonly aggregateID?: string
      readonly seq?: number
    },
  ): void
  /** The worker signalled end-of-stream. */
  end(): void
  /** A resync: a delta was dropped or a durable sequence gapped; durable events continue. */
  resync(payload?: DurableResyncPayload): void
  /** A delivery acknowledgement. */
  ack(payload?: EventsAckPayload): void
  /** Whether the worker signalled end-of-stream. */
  readonly ended: boolean
  /** Number of resyncs surfaced (received and overflow). */
  readonly resyncs: number
  /** Number of `ack` frames observed. */
  readonly acks: number
}

export function createHubEventSink(options: HubEventSinkOptions): HubEventSink {
  const { publisher, cache } = options
  const aggregateOf = options.aggregateOf ?? aggregateIDOfEvent
  let ended = false
  let resyncs = 0
  let acks = 0
  return {
    event(event, meta) {
      // The event crosses verbatim as `{ id, type, properties }`; transport-only
      // identity stays on the frame and is never fabricated onto the event.
      publisher.publish({ id: event.id, type: event.type, data: event.properties })
      if (meta?.durable !== true) return
      // Prefer the committed frame identity: the payload's `sessionID` can name a
      // different aggregate than the one the commit actually advanced.
      const aggregateID = meta.aggregateID ?? aggregateOf(event)
      if (aggregateID !== undefined) cache?.invalidate(aggregateID)
    },
    end() {
      ended = true
    },
    resync() {
      resyncs += 1
    },
    ack() {
      acks += 1
    },
    get ended() {
      return ended
    },
    get resyncs() {
      return resyncs
    },
    get acks() {
      return acks
    },
  }
}

/**
 * Serve a worker's events port with the shared egress receiver (length-prefixed
 * `WorkerFrameEnvelope` frames, backpressure-aware), routing every decoded frame
 * to `sink`. A malformed frame fails loud: the receiver closes the port.
 *
 * A durable event identified by its aggregate `(aggregateID, seq)` is additionally
 * handed to `onDurable` — the hub's cross-isolate mesh commit path, so another
 * worker's `events.durable()` subscribers wake. The live republish is unchanged:
 * the mesh is an additive wakeup channel, never a re-commit. A per-aggregate
 * sequence gap is surfaced as a resync (and sent back to the worker) instead of
 * failing the port, so the stream heals from the worker's DB snapshot.
 */
export function serveEventsPort(
  port: MessagePort,
  sink: HubEventSink,
  onError?: (error: Error) => void,
  onDurable?: (commit: DurableCommit) => void,
): () => void {
  const receiver = createEgressReceiver({
    port,
    onFrame: (frame) => {
      if (frame.kind !== "event") return
      // The canonical durability test (present only when BOTH `aggregateID` and
      // `seq` are set), shared with the receiver and fan-out so the three never
      // drift on exactly which frames are durable.
      const meta = durableAggregateMetaOf(frame)
      sink.event(frame.payload.event, {
        durable: meta !== undefined,
        ...(meta === undefined ? {} : { aggregateID: meta.aggregateID, seq: meta.seq }),
      })
      if (meta === undefined || onDurable === undefined) return
      onDurable({ meta, event: frame.payload.event })
    },
    // A worker that skipped a durable sequence is healed by its OWN database
    // snapshot, not by closing the port: surface the resync on the sink and ask
    // the worker (back over the same bidirectional events port) to re-read the
    // aggregate at the exact observed position.
    onDurableGap: (got) => {
      sink.resync({ aggregateID: got.aggregateID, seq: got.seq })
      try {
        port.postMessage(encodeFrame(resyncWithPositionFrame({ aggregateID: got.aggregateID, seq: got.seq })))
      } catch (error) {
        // The peer may already be gone; the sink counter still records the gap.
        onError?.(error instanceof Error ? error : new Error(String(error)))
      }
    },
    onEnd: () => sink.end(),
    onResync: (payload) => sink.resync(payload),
    onAck: (payload) => sink.ack(payload),
    ...(onError === undefined ? {} : { onError }),
  })
  return () => receiver.close()
}

// ---------------------------------------------------------------------------
// Production hub-proxy handlers.
// ---------------------------------------------------------------------------

/**
 * The hub-resident authorities a hub-proxy handler map dispatches to. The worker
 * multiplexes all three over one control port (`session-worker.ts`), so the
 * handler map is keyed by the exact bare methods the worker-side proxy layers
 * call (`layer.ts` `sessionStatusProxyLayer` / `permissionProxyLayer` /
 * `questionProxyLayer`).
 */
export interface HubProxyAuthorities {
  readonly sessionStatus: SessionStatus.Interface
  readonly permission: Permission.Interface
  readonly question: Question.Interface
  /**
   * Session-spawn authority. The production hub runs `Session.spawn` under the
   * parent's captured runtime context, so the child is dispatched to its own
   * per-session worker. Absent means the RPC fails loud.
   */
  readonly sessionSpawn?: (request: SessionSpawnRequest) => Promise<SessionSpawnResult>
  /**
   * Hub-side child-session scope. A bound worker's `Session.create`/`get`/
   * `children` route here so the create/projection/read happens on the hub, not
   * in the worker's single-session database. Each authority verifies the target
   * belongs to the worker's own subtree.
   */
  readonly sessionCreate?: (request: SessionCreateRequest) => Promise<Session.Info>
  readonly sessionGet?: (request: SessionGetRequest) => Promise<Session.Info | null>
  readonly sessionChildren?: (request: SessionChildrenRequest) => Promise<readonly Session.Info[]>
  readonly sessionCancel?: (request: SessionCancelRequest) => Promise<CancelOutcome | undefined>
  /**
   * Optional system-prompt transform authority. The worker cannot run
   * `experimental.chat.system.transform` itself (plugins are loaded on the hub),
   * so it calls the hub, which mutates and returns `output`. Absent means the
   * handler returns `output` unchanged (flag-off / no plugin).
   */
  readonly systemTransform?: (
    input: SystemTransformInput,
    output: SystemTransformOutput,
  ) => Promise<SystemTransformOutput>
  /**
   * Optional message-list transform authority. The worker cannot run
   * `experimental.chat.messages.transform` itself (plugins are loaded on the hub),
   * so it calls the hub, which mutates and returns `output`. Absent means the
   * handler returns `output` unchanged (flag-off / no plugin).
   */
  readonly messagesTransform?: (
    input: MessagesTransformInput,
    output: MessagesTransformOutput,
  ) => Promise<MessagesTransformOutput>
  /**
   * Optional hub-proxied plugin trigger. The worker never invokes a hook the hub
   * owns (`tool.execute.before/after`, `shell.env`, `tool.definition`, the
   * transforms, `chat.message`); it forwards `(name, input, output)` here, where
   * the real plugin instance runs under the worker's session context. Absent means
   * the output passes through unchanged (no configured plugin). The authority
   * rejects a hook whose route is `unsupported`, so a worker cannot smuggle an
   * unsupported trigger across the port; a worker-hosted hook may be forwarded
   * too, which is how `chat.params`/`chat.headers` resolve per request.
   */
  readonly pluginTrigger?: (name: string, input: unknown, output: unknown) => Promise<unknown>
  /**
   * Optional plugin-tool execute authority. The worker cannot run a plugin tool's
   * `execute` (the plugin instance lives on the hub), so it forwards the call
   * here, where the hub-owned definition runs and a clone-safe result returns.
   * `emit` is the streaming sink that `pluginTool.metadata` progress events are
   * pushed through; a non-streaming caller supplies a no-op. Unlike the transforms
   * there is no identity pass-through: absent means the handler rejects.
   */
  readonly pluginTool?: (
    input: PluginToolExecuteInput,
    emit: PluginToolProgressSink,
  ) => Promise<PluginToolExecuteResult>
  /**
   * Optional plugin-tool list authority: the clone-safe descriptor refresh. The
   * worker ships descriptors at init; a plugin that reloads mid-generation leaves
   * them stale, so the worker can re-fetch the current list here. Absent means the
   * handler rejects (an empty list would silently hide a mis-wiring).
   */
  readonly pluginToolList?: () => Promise<readonly PluginToolDescriptor[]>
  /**
   * Optional plugin-tool abort authority: unwind an in-flight plugin tool's
   * `ctx.abort` by `(sessionID, callID)`. Best-effort — absent means the handler
   * resolves as a no-op rather than failing a cancel path.
   */
  readonly pluginToolAbort?: (input: { readonly sessionID: string; readonly callID: string }) => void | Promise<void>
  /**
   * Optional worker-job mirror authority. A session worker owns its process-local
   * `BackgroundJob` registry, so a foreground job is invisible to the hub's
   * promotion endpoint unless the worker registers it here. The hub mirrors the
   * running job in its own registry, which makes `list`/`promote` work; promotion
   * resolves the worker's `awaitPromotion` long-poll so the worker applies it to
   * the local registry (and fires the task notification).
   */
  readonly backgroundJob?: WorkerBackgroundJobAuthority
}

/** One worker-reported terminal job state, settled into the hub mirror. */
export interface WorkerBackgroundJobSettle {
  readonly id: string
  readonly status: Exclude<BackgroundJobStatus, "running">
  readonly output?: string
  readonly error?: string
}

/** The worker-job mirror surface exposed over the hub-proxy control port. */
export interface WorkerBackgroundJobAuthority {
  register(info: BackgroundJobInfo): Promise<void>
  settle(input: WorkerBackgroundJobSettle): Promise<void>
  /** Resolves `true` on promotion, `false` once the job settles or disappears. */
  awaitPromotion(id: string): Promise<boolean>
}

/**
 * Executes a hub authority effect for a worker request. The hub is a single
 * global runtime while a worker belongs to the instance that dispatched it, so
 * the runner provides that instance's context (captured at dispatch).
 */
export type HubProxyRunner = <A, E>(effect: Effect.Effect<A, E>) => Promise<A>

/**
 * The prompt authority's string-keyed service. Resolving it by key lets the hub
 * run the authoritative children-first cancel without importing `prompt.ts`
 * (hub → prompt → run-state → hub would cycle).
 */
const SessionPromptService = Context.Service<SessionPrompt.Interface>("@opencode/SessionPrompt")

/** The descriptive rejection an absent plugin-tool authority returns (fail loud). */
function missingPluginToolAuthority(what: string): Error {
  return new Error(`the hub has no "pluginTool" authority wired; cannot ${what}`)
}

/**
 * Build the hub-side handler map for the worker hub-proxy wire. Every method is
 * namespaced by its authority (`question.ask` vs `permission.ask`), so a single
 * multiplexed control port can never misroute a call — no request-shape guessing.
 */
export function buildHubProxyHandlers(authorities: HubProxyAuthorities, run: HubProxyRunner): HubProxyHandlers {
  return {
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
    // Always served: a worker's `experimental.chat.system.transform` reaches the
    // hub, which runs the plugin hook and returns the mutated `{system}`. Absent
    // an authority the output passes through unchanged (never an unknown method).
    systemTransform: (input, output) =>
      authorities.systemTransform === undefined
        ? (output as SystemTransformOutput)
        : authorities.systemTransform(input as SystemTransformInput, output as SystemTransformOutput),
    // Always served: a worker's `experimental.chat.messages.transform` reaches the
    // hub, which runs the plugin hook and returns the mutated `{messages}`. Absent
    // an authority the output passes through unchanged (never an unknown method).
    messagesTransform: (input, output) =>
      authorities.messagesTransform === undefined
        ? (output as MessagesTransformOutput)
        : authorities.messagesTransform(input as MessagesTransformInput, output as MessagesTransformOutput),
    // Always served: a worker forwards every hub-proxied hook through this one
    // method. Absent an authority (no configured plugin) the output is unchanged.
    pluginTrigger: (name, input, output) =>
      authorities.pluginTrigger === undefined ? output : authorities.pluginTrigger(name as string, input, output),
    // Always served, but with no identity fallback: an execute has no result to
    // hand back, so an absent authority is a descriptive rejection — never an
    // "unknown hub-proxy method" (that would mask a missing wiring, not a typo).
    // This non-streaming entry supplies a no-op emit; `buildHubProxyStreamHandlers`
    // overrides it with the real sink when the port is served with streaming.
    "pluginTool.execute": (input) =>
      authorities.pluginTool === undefined
        ? Promise.reject(missingPluginToolAuthority("execute a plugin-provided tool"))
        : authorities.pluginTool(input as PluginToolExecuteInput, () => {}),
    // The descriptor refresh RPC; absent a list authority there is no useful empty
    // fallback, so a typo'd wiring fails loud rather than silently listing nothing.
    "pluginTool.list": () =>
      authorities.pluginToolList === undefined
        ? Promise.reject(missingPluginToolAuthority("list plugin-provided tools"))
        : authorities.pluginToolList(),
    // Best-effort: no authority means nothing is in flight to abort, not an error.
    // A cancel path must never turn an abort into a tool failure.
    "pluginTool.abort": (input) =>
      authorities.pluginToolAbort?.(input as { readonly sessionID: string; readonly callID: string }),
    // Worker-owned jobs are mirrored into the hub registry when the worker
    // reports them. Absent an authority (a plain test hub) registration and
    // settlement are no-ops, but promotion never fabricates a `true`.
    "backgroundJob.register": (info) =>
      authorities.backgroundJob?.register(info as BackgroundJobInfo) ?? Promise.resolve(),
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
  }
}

/**
 * The streaming half of the hub-proxy handler map: the methods whose handler may
 * push correlated events to the awaiting worker before it replies. A method here
 * wins over a same-named plain handler ({@link buildHubProxyHandlers}).
 *
 * Today only `pluginTool.execute` streams: the hub plugin's mid-execute
 * `ToolContext.metadata` updates are forwarded as `pluginTool.metadata` events,
 * so a long-running plugin tool reports progress live instead of only in its
 * final, authoritative result.
 */
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
        // The event method the worker's `hubProxyPluginTool` onEvent filters for.
        sink.emit("pluginTool.metadata", progress),
      )
    },
  }
}

/**
 * Build the hub-resident `experimental.chat.messages.transform` authority for a
 * worker. The hub owns plugin loading (decision 70), so resolution is deferred
 * through `plugin`, letting the caller read `Plugin.Service` from the worker's
 * session context. When no plugin is reachable the worker's payload passes
 * through by identity and `run` is never invoked; otherwise the plugin hook runs
 * under `run` (which provides the emitting instance's context) and its resolved
 * value is returned.
 */
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

/**
 * Build the hub-resident authority for the worker's hub-proxied plugin hook
 * bridge. The worker ships only `(name, input, output)`; the hub owns the plugin
 * functions and runs the real trigger under the worker's session context. A name
 * whose route is `unsupported` is rejected with
 * {@link UnsupportedPluginHookError}: the bridge is not a generic plugin RPC and
 * a worker may never invoke an unsupported hook through it. Worker-hosted hooks
 * may also be forwarded, which is how per-request `chat.params`/`chat.headers`
 * avoid freezing at the init model.
 */
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

/**
 * Raised when a worker asks the hub to run a plugin tool id that no loaded plugin
 * currently exposes — a stale descriptor, or a plugin reloaded mid-generation.
 * Fail loud: an empty success would silently hide the config/version skew.
 */
export class UnknownPluginToolError extends Error {
  readonly _tag = "UnknownPluginToolError"
  constructor(readonly toolID: string) {
    super(`unknown plugin tool "${toolID}": no loaded plugin exposes that tool id`)
    this.name = "UnknownPluginToolError"
  }
}

/**
 * Raised when the args a worker forwarded do not satisfy the plugin tool's own
 * decoder. The worker ships args verbatim (`Schema.Unknown`), so the hub validates
 * them at the boundary; a bad arg is a fail-loud caller error, not a tool result.
 */
export class InvalidPluginToolArgsError extends Error {
  readonly _tag = "InvalidPluginToolArgsError"
  constructor(
    readonly toolID: string,
    readonly detail: string,
  ) {
    super(`invalid args for plugin tool "${toolID}": ${detail}`)
    this.name = "InvalidPluginToolArgsError"
  }
}

/** A registered in-flight plugin-tool call: its abort signal and unregister fn. */
export interface PluginToolAbortHandle {
  readonly signal: AbortSignal
  readonly dispose: () => void
}

/**
 * The hub's in-flight plugin-tool abort registry. A plugin tool's `execute` runs
 * on the hub under a per-call `AbortController`; keying those controllers by
 * `(sessionID, callID)` lets the hub cancel path (and the worker's
 * `pluginTool.abort` RPC) unwind exactly the calls it means to, and never a
 * settled one.
 */
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
        // Unregister on EVERY exit; the entry is dropped only when it is still the
        // registered controller, so a later call reusing the callID is untouched.
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

/**
 * Module-level projection of the hub's MCP catalog plus the LSP tool into the
 * same clone-safe descriptor shape plugin tools use. A worker registers these as
 * forwarding stubs; the hub authority (below) executes them with its own MCP/LSP
 * services under the worker's captured session context.
 */
export async function resolveContextHubToolDescriptors(
  context: Context.Context<never> | undefined,
): Promise<readonly HubToolDescriptor[]> {
  const descriptors: HubToolDescriptor[] = []
  for (const descriptor of await resolveContextPluginToolDescriptors(context)) {
    descriptors.push({ ...descriptor, source: "plugin" })
  }
  if (context === undefined) return descriptors
  const mcp = Context.getOption(context, MCP.Service)
  if (Option.isSome(mcp)) {
    const tools = await Effect.runPromise(Effect.provide(mcp.value.tools(), context))
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
  return descriptors
}

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
type PluginToolFailure = UnknownPluginToolError | InvalidPluginToolArgsError | PluginToolNotSerializableError

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

/**
 * Validate forwarded args against the plugin tool's own decoder before `execute`.
 * The registry only builds a decoder when every arg is a Zod type; a legacy
 * JSON-schema map is `Schema.Unknown` there, so the hub passes it through rather
 * than inventing stricter validation than the tool advertises.
 */
function validatePluginToolArgs(toolID: string, definition: ToolDefinition, args: unknown): void {
  const raw = definition.args ?? {}
  if (!Object.values(raw).every(isZodType)) return
  const parsed = z.object(raw).safeParse(args ?? {})
  if (!parsed.success) {
    throw new InvalidPluginToolArgsError(toolID, parsed.error.message)
  }
}

/**
 * Fail loud on a result that cannot cross the control port, naming the tool and
 * the offending field. Without this a leaked function would only surface as the
 * generic `serveHubProxy` clone guard's cryptic message.
 */
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

/**
 * Execute a hub-owned MCP or LSP tool a worker forwarded over the plugin-tool
 * channel. The worker owns no MCP/LSP child process, so this is the single hub
 * execution point (`session/tools.ts` only decides *which* host). An unknown id
 * reports `UnknownPluginToolError` so the worker's stale-descriptor refresh path
 * is unchanged.
 */
const executeHubTool = (
  input: PluginToolExecuteInput,
  emit: PluginToolProgressSink,
): Effect.Effect<PluginToolExecuteResult, UnknownPluginToolError, PluginToolServices> =>
  Effect.gen(function* () {
    const permission = yield* Permission.Service
    const truncate = yield* Truncate.Service
    const agents = yield* Agent.Service
    const sessions = yield* Session.Service
    const instance = yield* InstanceRef
    const agentInfo = yield* agents.get(input.agent)
    const sessionInfo = yield* sessions.get(input.sessionID as SessionID).pipe(Effect.orDie)
    const bridge = yield* EffectBridge.make()
    const abort = new AbortController()
    const ask = (request: {
      permission: string
      patterns: string[]
      always: string[]
      metadata: Record<string, unknown>
    }) =>
      bridge.promise(
        permission
          .ask({
            ...request,
            sessionID: input.sessionID as SessionID,
            tool: { messageID: input.messageID, callID: input.callID },
            ruleset: Permission.merge(agentInfo.permission, sessionInfo.permission ?? []),
          })
          .pipe(Effect.orDie),
      )

    if (input.toolID === LSP_TOOL_ID) {
      const lsp = yield* Effect.serviceOption(LSP.Service)
      if (Option.isNone(lsp)) return yield* Effect.fail(new UnknownPluginToolError(input.toolID))
      const ctx: ToolDef.Context = {
        sessionID: input.sessionID as SessionID,
        messageID: input.messageID as never,
        agent: input.agent,
        abort: abort.signal,
        callID: input.callID,
        messages: [],
        metadata: (update) => Effect.sync(() => emit({ title: update.title, metadata: update.metadata })),
        ask: (request) => Effect.promise(() => Promise.resolve(ask(request as never))),
      }
      const info = yield* LspTool
      const tool = yield* info.init()
      const result = yield* tool.execute(input.args as never, ctx)
      return { title: result.title, output: result.output, metadata: result.metadata }
    }

    const mcp = yield* Effect.serviceOption(MCP.Service)
    if (Option.isNone(mcp)) return yield* Effect.fail(new UnknownPluginToolError(input.toolID))
    const entry = (yield* mcp.value.tools())[input.toolID]
    if (entry === undefined) return yield* Effect.fail(new UnknownPluginToolError(input.toolID))
    const tool = McpCatalog.convertTool(entry.def, entry.client, entry.timeout)
    yield* Effect.promise(() => ask({ permission: input.toolID, patterns: ["*"], always: ["*"], metadata: {} }))
    const raw = yield* Effect.promise(() =>
      (tool.execute as (args: unknown, options: { abortSignal: AbortSignal }) => Promise<unknown>)(input.args, {
        abortSignal: abort.signal,
      }),
    )
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
  }) as Effect.Effect<PluginToolExecuteResult, UnknownPluginToolError, PluginToolServices>

/**
 * Run one plugin tool on the hub: locate its definition, validate the forwarded
 * args, reconstruct the `PluginToolContext` the plugin expects, execute, then
 * normalize, bound (once) and sanitize the result exactly like the registry's
 * local `fromPlugin` path.
 */
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
      return yield* executeHubTool(input, emit).pipe(
        Effect.catchTag("UnknownPluginToolError", () => Effect.fail(new UnknownPluginToolError(input.toolID))),
      )
    }
    validatePluginToolArgs(input.toolID, definition, input.args)
    const bridge = yield* EffectBridge.make()
    const permission = yield* Permission.Service
    const truncate = yield* Truncate.Service
    const agents = yield* Agent.Service
    const sessions = yield* Session.Service
    const instance = yield* InstanceRef
    const agentInfo = yield* agents.get(input.agent)
    const sessionInfo = yield* sessions.get(input.sessionID as SessionID).pipe(Effect.orDie)
    // Register only once we are committed to executing: an unknown tool or bad
    // args must never leave an entry the cancel path could later abort.
    const abort = registerAbort?.(input.sessionID, input.callID) ?? {
      signal: new AbortController().signal,
      dispose: () => {},
    }
    const pluginCtx: PluginToolContext = {
      sessionID: input.sessionID,
      messageID: input.messageID,
      agent: input.agent,
      directory: instance?.directory ?? process.cwd(),
      worktree: instance?.worktree ?? process.cwd(),
      // Per-call so a hub cancel can unwind an in-flight plugin tool.
      abort: abort.signal,
      // Mid-execute progress is streamed to the worker as `pluginTool.metadata`.
      metadata: (update) => emit({ title: update.title, metadata: update.metadata }),
      // Bound to the HUB-RESIDENT permission service. Routing this back over the
      // control port would deadlock: we are already inside that port's request.
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
      const result = yield* Effect.promise(() => definition.execute(input.args as never, pluginCtx))
      const output = typeof result === "string" ? result : result.output
      const metadata = typeof result === "string" ? {} : (result.metadata ?? {})
      const attachments = typeof result === "string" ? undefined : result.attachments
      const bounded = yield* Truncate.boundResult(truncate, { output, metadata }, agentInfo)
      return sanitizePluginToolResult(input.toolID, {
        title: typeof result === "string" ? "" : (result.title ?? ""),
        output: bounded.output,
        metadata: bounded.metadata,
        ...(attachments === undefined ? {} : { attachments }),
      })
    }).pipe(Effect.ensuring(Effect.sync(() => abort.dispose())))
  })
}

/**
 * The hub-resident plugin-tool execute authority. `emit` is optional so a
 * non-streaming caller (or an existing single-argument consumer) still works; the
 * server's streaming handler always supplies the real sink.
 */
export type PluginToolAuthority = (
  input: PluginToolExecuteInput,
  emit?: PluginToolProgressSink,
) => Promise<PluginToolExecuteResult>

/**
 * Build the hub-resident plugin-tool authority for a worker. The hub owns plugin
 * loading (decision 70), so resolution is deferred through `plugin`, letting the
 * caller read `Plugin.Service` (and the other hub authorities) from the worker's
 * captured session context. `registerAbort` wires the per-call controller into the
 * hub cancel path; absent, a throwaway controller is used (best-effort).
 */
export function pluginToolAuthority(options: {
  readonly plugin: () => Option.Option<Plugin.Interface>
  readonly run: HubProxyRunner
  readonly registerAbort?: (sessionID: string, callID: string) => PluginToolAbortHandle
}): PluginToolAuthority {
  return (input, emit = () => {}) =>
    // The effect needs the hub-resident services (Permission / Truncate / Agent /
    // Session). `run` provides the dispatching session's captured context, but
    // `HubProxyRunner` erases `R`, so the requirement is asserted at this single
    // boundary rather than threaded through the frozen signature.
    options.run(
      pluginToolExecute(options.plugin, input, emit, options.registerAbort) as unknown as Effect.Effect<
        PluginToolExecuteResult,
        UnknownPluginToolError
      >,
    )
}

/**
 * The per-worker port servers: the transferred egress and hub-proxy
 * control ports. Each attach is idempotent per worker (a re-attach disposes the
 * previous server first), and `disposeAll` tears every server down. The backend
 * source and handler map are thunks because both materialise lazily, after the
 * pool and dispatcher exist.
 */
interface WorkerPortRegistryOptions {
  readonly log: WorkerLogger
  readonly backends: () => WorkerPortBackends | undefined
  /** The handler map for one worker's control port (per-session context). */
  readonly hubProxyHandlersFor: (workerID: number) => HubProxyHandlers | undefined
  /** The streaming half of one worker's control port (e.g. `pluginTool.metadata`). */
  readonly hubProxyStreamingFor: (workerID: number) => Readonly<Record<string, HubProxyStreamHandler>> | undefined
  readonly onPidReport?: (workerID: number, report: PidReport) => void
}

interface WorkerPortRegistry {
  readonly attachWorkerPorts: (workerID: number, ports: WorkerTransferredPorts) => () => void
  readonly attachHubProxyPort: (workerID: number, port: MessagePort) => () => void
  /**
   * Dispose one retired worker's port servers (egress and hub-proxy).
   * Idempotent; a never-attached worker is a no-op.
   */
  readonly disposeWorker: (workerID: number) => void
  readonly disposeAll: () => void
}

function createWorkerPortRegistry(options: WorkerPortRegistryOptions): WorkerPortRegistry {
  const { log } = options
  const workerPortDisposers = new Map<number, () => void>()
  const hubProxyDisposers = new Map<number, () => void>()

  // Serve a worker's transferred egress ports against the hub authorities. A
  // port the hub has no backend for is left unserved (the worker only transfers
  // ports the hub advertised in `capabilities.sessionWorkerPorts`).
  const attachWorkerPorts = (workerID: number, ports: WorkerTransferredPorts): (() => void) => {
    log(workerLogMessage("ports connected"), { level: "INFO", workerID, ports: transferredPortNames(ports) })
    workerPortDisposers.get(workerID)?.()
    const disposers: (() => void)[] = []
    const backends = options.backends()
    if (backends !== undefined) {
      if (ports.egress !== undefined) {
        disposers.push(
          backends.egress !== undefined
            ? backends.egress(workerID, ports.egress, ports.egressControl)
            : drainEgressPort(ports.egress),
        )
      }
    }
    const dispose = () => {
      for (const disposer of disposers) disposer()
      workerPortDisposers.delete(workerID)
    }
    workerPortDisposers.set(workerID, dispose)
    return dispose
  }

  // Serve the hub-proxy authorities over a worker's control port, plus any
  // `{type:"pid-report"}` frames the worker sends on the same port (plan 11.5).
  const attachHubProxyPort = (workerID: number, port: MessagePort): (() => void) => {
    log(workerLogMessage("ports connected"), { level: "INFO", workerID, ports: ["hub-proxy"] })
    hubProxyDisposers.get(workerID)?.()
    const disposers: (() => void)[] = []
    const handlers = options.hubProxyHandlersFor(workerID)
    if (handlers !== undefined) disposers.push(serveHubProxy(handlers, port, options.hubProxyStreamingFor(workerID)))
    const onPidReport = options.onPidReport
    if (onPidReport !== undefined) {
      // The port association is the authoritative owner: the worker only knows
      // its own thread id, not the hub's slot id.
      disposers.push(servePidReportPort((report) => onPidReport(workerID, report), port))
    }
    if (disposers.length === 0) return () => {}
    const dispose = () => {
      for (const disposer of disposers) disposer()
      hubProxyDisposers.delete(workerID)
    }
    hubProxyDisposers.set(workerID, dispose)
    return dispose
  }

  return {
    attachWorkerPorts,
    attachHubProxyPort,
    disposeWorker: (workerID) => {
      workerPortDisposers.get(workerID)?.()
      hubProxyDisposers.get(workerID)?.()
    },
    disposeAll: () => {
      for (const dispose of [...workerPortDisposers.values()]) dispose()
      for (const dispose of [...hubProxyDisposers.values()]) dispose()
    },
  }
}

/** The cancel reporter's collaborators: the dispatcher (lazy) and the child graph. */
interface CancelReporterOptions {
  readonly dispatcher: () => Dispatcher
  readonly childrenOf: (sessionID: SessionID) => ReadonlyArray<SessionID>
  readonly events?: LiveEventPublisher
  /** Per-session publisher, so a cancel carries the cancelled session's location. */
  readonly eventPublisherFor?: (sessionID: SessionID) => LiveEventPublisher | undefined
  /**
   * Invoked at the START of a cancel, before the grace window begins, for the
   * session and each descendant. The production layer aborts the session's
   * in-flight plugin tools here, so a hub-executed tool unwinds immediately
   * instead of only after the grace window has elapsed.
   */
  readonly onCancelRequested?: (sessionID: SessionID) => void
  readonly log: WorkerLogger
}

interface CancelReporter {
  /** Record a dispatcher-reported terminal mode so the report can reconstruct it. */
  readonly recordCancelled: (sessionID: string, mode: CancelledMode) => void
  readonly cancelWithReport: (sessionID: SessionID) => Promise<CancelReport>
}

/**
 * Reconstruct the terminal settlement of a cancelled session and its subagent
 * cascade. The dispatcher cascades children-first but discards a descendant's own
 * outcome, so this records each mode it reports and derives which descendants
 * still need a fallback idle (a cooperative settle publishes its own; a queued or
 * grace-terminated one does not).
 */
function createCancelReporter(options: CancelReporterOptions): CancelReporter {
  const cancelledModes = new Map<string, CancelledMode>()

  /**
   * Every descendant of `sessionID` (children-first, depth-first), as the
   * dispatcher's own cascade would visit them. A linked child that was never
   * admitted is included, so it still receives a fallback idle.
   */
  const descendantsOf = (sessionID: SessionID): SessionID[] => {
    const descendants: SessionID[] = []
    const visit = (id: SessionID): void => {
      for (const child of options.childrenOf(id)) {
        visit(child)
        descendants.push(child)
      }
    }
    visit(sessionID)
    return descendants
  }

  /** A cancel mode that implies the worker already published its terminal idle. */
  const workerPublishedIdle = (mode: CancelledMode | undefined): boolean =>
    mode === "cooperative" || mode === "completed"

  const cancelWithReport = async (sessionID: SessionID): Promise<CancelReport> => {
    const descendants = descendantsOf(sessionID)
    // Drop a prior request's recorded modes so its outcome is never reused.
    cancelledModes.delete(sessionID)
    for (const descendant of descendants) cancelledModes.delete(descendant)

    // Signal BEFORE the grace window: an in-flight plugin tool must see its
    // `ctx.abort` fire at the moment of the cancel, not only once the grace has
    // elapsed. The cascade mirrors the dispatcher's children-first order.
    options.onCancelRequested?.(sessionID)
    for (const descendant of descendants) options.onCancelRequested?.(descendant)

    // The dispatcher cascades to the descendants itself (children-first); the
    // recorded modes below reconstruct each descendant's settlement.
    const outcome = await options.dispatcher().cancelWithGrace(sessionID)

    // Report the terminal outcome to every SSE client. `SessionV1.Event.Cancelled`
    // is non-durable, and the live-only publisher refuses a durable definition, so
    // this can never re-commit the session state already written by the worker.
    //
    // Publish only when a cancel actually won. A `completion` outcome means the
    // run finished on its own during the grace window; telling the client it was
    // cancelled would be a lie (and would race the run's own idle/completion).
    // The hub deliberately emits no `session.status: idle` here — `run-state.ts`
    // owns the canonical cancel-idle transition.
    if (outcome !== undefined && outcome.winner === "cancel") {
      const publisher = options.eventPublisherFor?.(sessionID) ?? options.events
      publisher?.publishDefinition(SessionV1.Event.Cancelled, {
        sessionID: outcome.sessionID,
        // The durable event schema predates the typed boot outcome; a boot
        // cancel is a terminate of the partial isolate from its perspective.
        mode: outcome.mode === "boot" ? "terminated" : outcome.mode,
        durationMs: outcome.durationMs,
        acknowledged: outcome.acknowledged,
      })
    }
    const reportDescendants = descendants.map((child) => ({
      sessionID: child,
      needsFallbackIdle: !workerPublishedIdle(cancelledModes.get(child)),
    }))
    options.log(workerLogMessage("cancel cascade"), {
      level: "INFO",
      sessionID,
      descendants: reportDescendants.length,
      needsFallbackIdle: reportDescendants.filter((descendant) => descendant.needsFallbackIdle).length,
    })
    return { outcome, descendants: reportDescendants }
  }

  return {
    recordCancelled: (sessionID, mode) => {
      cancelledModes.set(sessionID, mode)
    },
    cancelWithReport,
  }
}

/**
 * Mark one retired generation's lease reclaimable, scoped to its exact
 * `worker_owner_id`. A successor's live lease is never touched, so a retirement
 * that lands after a reclaim cannot strand the new generation.
 */
export async function releaseRetiredWorkerLease(
  db: DbExecutor,
  sessionID: SessionID,
  retiredOwnerID: string,
): Promise<boolean> {
  if (typeof retiredOwnerID !== "string" || retiredOwnerID.length === 0) return false
  const result = await db.run(
    "UPDATE event_sequence SET worker_expires_at = 0 WHERE aggregate_id = ? AND worker_owner_id = ? AND worker_expires_at IS NOT NULL",
    [sessionID, retiredOwnerID],
  )
  return result.changes > 0
}

/** Compose the dispatcher + executor pool + cooperative cancel into one hub. */
/**
 * Upper bound on a dispatch depth carried from `metadata.depth`. Nesting beyond
 * this is already refused by the subagent limit; the clamp only keeps a
 * malformed/forged value from growing the cap's per-depth budget map without
 * bound.
 */
const MAX_DISPATCH_DEPTH = 64

/** Parse a dispatch depth at the boundary: finite, non-negative, bounded. */
export function boundedDispatchDepth(depth: number | undefined): number {
  if (depth === undefined || !Number.isFinite(depth)) return 0
  return Math.min(Math.max(0, Math.trunc(depth)), MAX_DISPATCH_DEPTH)
}

export function createSessionHub(options: SessionHubOptions): SessionHub {
  const log = options.log ?? noopWorkerLogger
  const cache = options.cache ?? new StateCache<unknown>()
  const db = options.db
  // Persist every typed crash through the hub's off-loop executor unless the
  // caller supplied its own sink; a hub with no executor has no sink. The
  // payload returned to the caller is the same value (no sync hub SQL here).
  const onWorkerCrashed =
    options.onWorkerCrashed ??
    (db === undefined ? undefined : (payload: WorkerCrashed) => recordAsync(db, payload).then(() => undefined))

  const surfaceWorkerCrash = async (error: unknown, sessionID: SessionID): Promise<void> => {
    const payload = workerCrashedFrom(error)
    if (payload === undefined) {
      if (errorTagOf(error) === "WorkerCrashedError") {
        log(workerLogMessage("worker crash payload missing"), {
          level: "ERROR",
          sessionID,
          expected: {
            sessionID,
            workerID: "number",
            phase: WORKER_CRASH_PHASES.join(" | "),
            lastCommittedMessageID: "string | null",
            inFlightCallIDs: "readonly string[]",
            retryable: false,
            reason: "string",
          },
        })
      }
      return
    }
    log(workerLogMessage("worker crashed"), {
      level: "ERROR",
      sessionID: payload.sessionID,
      workerID: payload.workerID,
      phase: payload.phase,
      retryable: payload.retryable,
      reason: payload.reason,
    })
    if (onWorkerCrashed === undefined) return
    try {
      await onWorkerCrashed(payload)
    } catch (sinkError) {
      // Persistence failure must never replace the crash the caller observes.
      log(workerLogMessage("worker crash persistence failed"), {
        level: "ERROR",
        sessionID: payload.sessionID,
        error: sinkError instanceof Error ? sinkError.message : String(sinkError),
      })
    }
  }
  // The port authorities are built on the first attach (never at construction):
  // a value is used as-is; a source function is invoked once and memoised.
  const portSource = options.ports
  let resolvedBackends: WorkerPortBackends | undefined
  let backendsResolved = false
  const portBackends = (): WorkerPortBackends | undefined => {
    if (portSource === undefined) return undefined
    if (typeof portSource !== "function") return portSource
    if (!backendsResolved) {
      resolvedBackends = portSource()
      backendsResolved = true
    }
    return resolvedBackends
  }
  // The backends to dispose at teardown, or `undefined` when none exist: a value
  // source is disposed directly; a function source is disposed only if it was
  // actually built (a lazy source that never attached owns no resources).
  const disposables = (): WorkerPortBackends | undefined => {
    if (typeof portSource !== "function") return portSource
    return backendsResolved ? resolvedBackends : undefined
  }
  // Assigned once the pool exists; read lazily by the routing closures below.
  let dispatcher: Dispatcher
  // Assigned once the registry exists (below); read lazily by the attach thunks.
  let ports: WorkerPortRegistry

  ports = createWorkerPortRegistry({
    log,
    backends: portBackends,
    hubProxyHandlersFor,
    hubProxyStreamingFor,
    onPidReport: options.onPidReport,
  })

  /**
   * The served hub-proxy handler map for ONE worker: the caller's authorities,
   * built per attach so a worker reads its OWN session context. A static
   * `hubProxy` remains the fallback for a plain test hub.
   */
  function hubProxyHandlersFor(workerID: number): HubProxyHandlers | undefined {
    const backends = portBackends()
    const handlers = backends?.hubProxyFor?.(workerID) ?? backends?.hubProxy
    if (handlers === undefined) return undefined
    // The hub owns the lifecycle truth. Serving these on every control port (not
    // only the production authority map) means a worker's real prompt/tool path
    // reports commit/tool/phase into the same tracker the crash path reads.
    return {
      ...handlers,
      "lifecycle.commit": (sessionID, messageID) => {
        lifecycle.commit(String(sessionID), String(messageID))
        lifecycle.phase(String(sessionID), "commit")
      },
      "lifecycle.phase": (sessionID, phase) => {
        if (isWorkerCrashPhase(phase)) lifecycle.phase(String(sessionID), phase)
      },
      "lifecycle.toolStarted": (sessionID, callID, sideEffecting) => {
        lifecycle.toolStarted(String(sessionID), String(callID), sideEffecting !== false)
        lifecycle.phase(String(sessionID), "tool")
      },
      "lifecycle.toolFinished": (sessionID, callID) => {
        lifecycle.toolFinished(String(sessionID), String(callID))
      },
    }
  }

  /**
   * The streaming half of a worker's control port. It cannot be mixed into
   * `handlers` (a method is served by one or the other), so it rides the separate
   * `streaming` map `serveHubProxy` accepts. A source backend may specialize by
   * worker; a static `hubProxyStreaming` is the fallback.
   */
  function hubProxyStreamingFor(workerID: number): Readonly<Record<string, HubProxyStreamHandler>> | undefined {
    const backends = portBackends()
    return backends?.hubProxyStreamingFor?.(workerID) ?? backends?.hubProxyStreaming
  }

  const pool = options.pool
  const resolvedCancellation =
    typeof options.cancellation === "function" ? options.cancellation(pool) : options.cancellation
  // Every terminal cancel mode the dispatcher reports, keyed by session id. The
  // dispatcher cascades children-first but discards a descendant's own outcome,
  // so the reporter records the modes and reconstructs which descendants still
  // need a fallback idle (see `cancelWithReport`).
  // The child edge is synchronous by contract: the dispatcher reads it on the
  // crash/timeout path, where a `Session.children` query would block. The
  // production layer supplies its spawn-observed index; a caller may override
  // it (tests, or a caller-driven tree walk) through `cancellation.childrenOf`.
  const childrenOf = resolvedCancellation?.childrenOf ?? (() => [])
  const cancelReporter = createCancelReporter({
    dispatcher: () => dispatcher,
    childrenOf,
    events: options.events,
    ...defined({ eventPublisherFor: options.eventPublisherFor, onCancelRequested: options.onCancelRequested }),
    log,
  })
  const baseOnCancelled = resolvedCancellation?.onCancelled
  const cancellation: CancellationConfig | undefined =
    resolvedCancellation === undefined
      ? undefined
      : {
          ...resolvedCancellation,
          childrenOf,
          onCancelled: (event) => {
            cancelReporter.recordCancelled(event.sessionID, event.mode)
            baseOnCancelled?.(event)
          },
        }
  const lifecycle = options.lifecycle ?? createLifecycleTracker()
  const barrier = options.barrier ?? createRunBarrier()
  const barrierPhases = options.barrierPhases ?? ((sessionID: SessionID) => lifecycle.phases(sessionID))

  /**
   * The dispatcher holds a completed run's slot until the four commit-side
   * phases settle. The run loop ending proves commits/projectors/promotion
   * landed; tools still in flight are what the barrier actually waits for.
   */
  const runBarrier = async (sessionID: SessionID): Promise<void> => {
    await options.writeBarrier?.(sessionID)
    lifecycle.settlePhase(sessionID, "commits")
    lifecycle.settlePhase(sessionID, "projectors")
    lifecycle.settlePhase(sessionID, "promotion")
    if (lifecycle.indeterminate(sessionID).length === 0) lifecycle.settlePhase(sessionID, "tools")
    await barrier.wait(sessionID, barrierPhases(sessionID))
  }

  /**
   * Sessions whose owning generation was retired (crash, hard terminate, idle
   * reap). Its in-flight tool calls can never settle, so the next admitted run
   * releases them instead of letting them time out its barrier. Only a lost
   * generation grants that release: a live run's calls always hold the barrier.
   */
  const lostGenerations = new Set<SessionID>()

  /**
   * A retired generation can never run `dispose()` (crash, hard terminate), so
   * its worker lease would otherwise stay live until the 30 s TTL and the
   * immediate successor's `Bus.reclaim` would fail with `Bus.LeaseConflict`.
   * Retirement is the authoritative "this generation is dead" signal, so mark
   * its lease expired here. The update is scoped to the retired generation's
   * exact `worker_owner_id`; a successor that already claimed the aggregate must
   * never have its live lease zeroed.
   */
  const retiredLeaseReleases = new Map<SessionID, Promise<void>>()

  const queueRetiredLeaseRelease = (sessionID: SessionID, task: () => Promise<void>): Promise<void> => {
    const previous = retiredLeaseReleases.get(sessionID) ?? Promise.resolve()
    const next = previous.then(task).catch((error: unknown) => {
      log(workerLogMessage("retired lease release failed"), {
        level: "WARN",
        sessionID,
        error: error instanceof Error ? error.message : String(error),
      })
    })
    retiredLeaseReleases.set(sessionID, next)
    void next.finally(() => {
      if (retiredLeaseReleases.get(sessionID) === next) retiredLeaseReleases.delete(sessionID)
    })
    return next
  }

  /**
   * Release the retired generation's lease. The `workerID` prefix is its stable
   * generation identity (`worker-<id>-<pid>-<uuid>`); the exact owner is read
   * from the row and passed to the scoped update so a successor's lease is
   * untouched even if it raced the retirement.
   */
  const releaseRetiredLease = (sessionID: SessionID, workerID?: number): Promise<void> => {
    if (db === undefined || workerID === undefined) return Promise.resolve()
    const generationPrefix = `worker-${workerID}-`
    return queueRetiredLeaseRelease(sessionID, async () => {
      const rows = await db.query("SELECT worker_owner_id FROM event_sequence WHERE aggregate_id = ?", [sessionID])
      const owner = rows[0]?.worker_owner_id
      if (typeof owner !== "string" || !owner.startsWith(generationPrefix)) return
      await releaseRetiredWorkerLease(db, sessionID, owner)
    })
  }

  /** Successor admission waits for the retired generation's release to land. */
  const awaitRetiredLeaseRelease = (sessionID: SessionID): Promise<void> =>
    retiredLeaseReleases.get(sessionID) ?? Promise.resolve()

  /**
   * A timed-out barrier refuses the next run while its worker is still alive
   * (the run may still settle). Once that worker has been retired — a crash or a
   * hard terminate — no fiber can ever settle the phases, so the stale barrier is
   * abandoned instead of wedging the Session forever with `RunBarrierTimeout`.
   */
  const admitBarrier = (sessionID: SessionID): void => {
    const state = barrier.state(sessionID)
    if (state === "timed-out") {
      if (dispatcher.registry.worker(sessionID) === undefined) {
        lostGenerations.add(sessionID)
        barrier.abandon(sessionID)
        return
      }
      throw new RunBarrierTimeout(sessionID, RUN_BARRIER_TIMEOUT_MS)
    }
    if (state === "cancelled" || state === "failed") barrier.reset(sessionID)
  }

  dispatcher = createDispatcher({
    pool,
    crashState: (sessionID) => lifecycle.crashState(sessionID),
    recordCrash: (payload) => lifecycle.observeCrash(payload),
    resumeDecision: (payload) => {
      // A compiled build mismatch is permanent for this entry: respawning the
      // same bundle would crash-loop. Refuse auto-resume instead of retrying.
      if (payload.reason.includes("session worker build mismatch")) {
        return { resume: false, sessionID: payload.sessionID, phase: payload.phase, reason: "phase" }
      }
      const decide = (indeterminate: readonly IndeterminateCall[]) =>
        decideAutoResume({
          sessionID: payload.sessionID,
          phase: payload.phase,
          attempts: lifecycle.attempts(payload.sessionID),
          cancelled: lifecycle.cancelled(payload.sessionID),
          lastCommittedMessageID: payload.lastCommittedMessageID,
          indeterminate,
        })
      // The durable `tool_call` rows are the crash truth. The tracker is only a
      // mirror: a worker can die between its durable write and the report, and a
      // finished row whose report was lost must not falsely block resume.
      if (db === undefined) return decide(lifecycle.indeterminate(payload.sessionID))
      return db
        .query(
          "SELECT call_id, side_effecting FROM tool_call WHERE session_id = ? AND state = 'started' ORDER BY time_started, call_id",
          [payload.sessionID],
        )
        .then((rows) =>
          decide(
            rows.map((row) => ({
              callID: String(row.call_id),
              sideEffecting: Number(row.side_effecting) === 1,
            })),
          ),
        )
        .catch(() => decide(lifecycle.indeterminate(payload.sessionID)))
    },
    onAutoResume: (decision, payload) => {
      lifecycle.recordAttempt(decision.sessionID)
      if (options.resumeRun !== undefined) {
        void Promise.resolve(options.resumeRun(decision, payload)).catch(() => undefined)
        return
      }
      void dispatch
        .ensureRunning(decision.sessionID as SessionID)
        .then(() => lifecycle.resetAttempts(decision.sessionID))
        .catch(() => undefined)
    },
    writeBarrier: runBarrier,
    onRunStart: (sessionID) => {
      // A lost generation's calls can never settle. Release them into the OLD
      // phases first (so the fresh `tools` phase below is not pre-resolved and
      // still gates calls started during this run), then start the generation.
      if (lostGenerations.delete(sessionID)) {
        for (const call of lifecycle.indeterminate(sessionID)) lifecycle.toolFinished(sessionID, call.callID)
      }
      lifecycle.beginRun(sessionID)
    },
    onSessionReleased: (sessionID, workerID) => {
      // This generation can never settle another phase: the next run must not
      // inherit its in-flight calls.
      lostGenerations.add(sessionID)
      // The retired generation's lease is dead even when its isolate never ran
      // `dispose()` (crash/terminate). Mark it reclaimable before the successor
      // boots; the next admission awaits this release (see `ensureRunning`).
      void releaseRetiredLease(sessionID, workerID)
      // Retirement (reap/cancel/crash) disposes this worker's hub-side port
      // servers immediately, rather than leaving them alive until shutdown.
      if (workerID !== undefined) ports.disposeWorker(workerID)
      options.onSessionReleased?.(sessionID)
    },
    ...defined({
      cancellation,
      reapMs: options.reapMs,
      schedule: options.schedule,
      log,
    }),
  })
  // Depth is carried explicitly on the run (`metadata.depth` -> Session.spawn).
  // The production hub resolves it per session; a plain hub without the seam is a
  // foreground root (0). Clamp so malformed metadata cannot grow the cap's depth
  // map without bound or claim an absurd level.
  const dispatch: SessionDispatch = createSessionDispatch({ dispatcher, pool })
  const onDispatch = options.onDispatch
  const resolveDepth = (sessionID: SessionID): number =>
    options.depthOf === undefined ? 0 : boundedDispatchDepth(options.depthOf(sessionID))

  return {
    dispatcher,
    cache,
    // The inverted key→worker map: a worker's session, or undefined when idle.
    sessionOfWorker: (workerID) => dispatcher.registry.sessionsOn(workerID)[0],
    attachWorkerPorts: ports.attachWorkerPorts,
    attachHubProxyPort: ports.attachHubProxyPort,
    lifecycle,
    barrier,
    ensureRunning: async (sessionID, origin) => {
      // Capture the caller's ambient context synchronously; awaiting first would
      // resume outside this fiber and lose it.
      onDispatch?.(sessionID)
      admitBarrier(sessionID)
      // A retired generation's release must land before its successor boots and
      // claims the aggregate; otherwise the successor can be admitted against a
      // still-live lease.
      await awaitRetiredLeaseRelease(sessionID)
      try {
        const result = await dispatch.ensureRunning(sessionID, resolveDepth(sessionID), origin)
        lifecycle.resetAttempts(sessionID)
        return result
      } catch (error) {
        await surfaceWorkerCrash(error, sessionID)
        throw error
      }
    },
    startShell: async (sessionID, shell) => {
      onDispatch?.(sessionID)
      admitBarrier(sessionID)
      await awaitRetiredLeaseRelease(sessionID)
      try {
        const result = await dispatch.startShell(sessionID, shell, resolveDepth(sessionID))
        lifecycle.resetAttempts(sessionID)
        return result
      } catch (error) {
        await surfaceWorkerCrash(error, sessionID)
        throw error
      }
    },
    assertNotBusy: async (sessionID) => {
      const workerID = dispatcher.registry.entry(sessionID)?.workerID
      const worker = workerID === undefined ? undefined : pool.byID(workerID)
      // A proven-dead isolate is idle even while its run is still registered:
      // the registry reservation cannot outlive the worker it describes.
      if (worker !== undefined && !worker.alive) return false
      // An admitted run is busy even before its worker materializes: a run queued
      // in `cap.acquire` has no isolate to probe, so the registry reservation is
      // the only truth. Reporting idle there would let a second prompt mutate a
      // session whose run is still waiting for a slot.
      if (dispatcher.registry.get(sessionID) !== undefined) return true
      // Only a proven-dead isolate is idle. A live worker that cannot answer the
      // probe (timeout, broken control channel) must fail closed: the caller
      // must not treat an unproven channel as permission to mutate the session.
      if (worker === undefined) return false
      return worker.assertNotBusy(sessionID)
    },
    cancel: async (sessionID) => {
      lifecycle.cancel(sessionID)
      barrier.cancel(sessionID)
      return (await cancelReporter.cancelWithReport(sessionID)).outcome
    },
    cancelWithReport: async (sessionID) => {
      lifecycle.cancel(sessionID)
      barrier.cancel(sessionID)
      return cancelReporter.cancelWithReport(sessionID)
    },
    cancelAll: async () => {
      for (const entry of dispatcher.registry.entries()) {
        lifecycle.cancel(entry.sessionID)
        barrier.cancel(entry.sessionID)
      }
      return dispatch.cancelAll()
    },
    async terminateAll() {
      ports.disposeAll()
      await dispatcher.terminateAll()
      cache.clear()
      // Only tear down backends we actually built (a source never attached to is
      // never constructed, so no isolate is spawned just to dispose it).
      const backends = disposables()
      await backends?.dispose?.()
      await db?.close().catch(() => undefined)
    },
  }
}

// ---------------------------------------------------------------------------
// Bun executor transport + pool (production adapters over `execute.ts` frames).
// ---------------------------------------------------------------------------

export interface BunExecutorTransportOptions {
  readonly entry: string | URL
  readonly env?: Record<string, string | undefined>
  readonly workerData?: unknown
  /** The SQLite file this isolate uses, for the live-worker/schema gate. */
  readonly databasePath?: string
  /**
   * OS thread name (`WorkerOptions.name`) applied at spawn, so the isolate is
   * born with the right Linux `comm` before it self-names. Absent leaves Bun's
   * default (`Worker`). Fail-soft: a name never affects spawning.
   */
  readonly name?: string
  readonly timeoutMs?: number
  /** Maximum buffered outbound frames kept for late `waitFor` matches. */
  readonly bufferCapacity?: number
  /** Called with the worker's transferred egress ports. */
  readonly onWorkerPorts?: (ports: WorkerTransferredPorts) => void
  /** Called with the worker's transferred hub-proxy control port. */
  readonly onHubProxyPort?: (port: MessagePort) => void
  /** Optional structured logger seam; absent defaults to a no-op (flag-off behavior). */
  readonly log?: WorkerLogger
  /** Boot-cancellation telemetry: a cancel win or a death during abort. */
  readonly onBootTelemetry?: (event: BootTelemetry) => void
  /**
   * Boot-stage phase reporting: `boot` before the isolate is constructed,
   * `materialize` when its validated `ready` handshake acknowledges the real
   * graph. The session worker then owns stream/tool/commit phases.
   */
  readonly onPhase?: (phase: "boot" | "materialize") => void
  /** Hub slot id for diagnostics (a boot/import failure is reported under it). */
  readonly workerID?: number
  /**
   * The one global spawn permit. When supplied, the isolate is constructed only
   * after `acquire()` resolves and the permit is released only on the worker's
   * validated `ready` handshake frame (port transfers arrive earlier). Absent
   * keeps the legacy eager construction (direct transport tests and
   * non-production callers).
   */
  readonly spawnGate?: SpawnGateHandle
  /**
   * Deadline for the boot-ready handshake after the worker is constructed. On
   * expiry the spawn fails alone (the permit is released and the in-flight wait
   * rejects); later sessions may spawn. Defaults to the transport `timeoutMs`.
   */
  readonly bootTimeoutMs?: number
}

const DEFAULT_TRANSPORT_TIMEOUT_MS = 30_000

/** Default deadline for waiting on the one global spawn permit. */
const DEFAULT_SPAWN_GATE_TIMEOUT_MS = 30_000

/** Cap on buffered frames, so a transport nobody waits on cannot grow unbounded. */
export const DEFAULT_TRANSPORT_BUFFER_CAPACITY = 1024

/**
 * How long an isolate may take to acknowledge the orderly-shutdown frame before
 * the hub terminates it anyway. Long enough for a `SQLite.close()` + lease
 * release; short enough that a wedged worker cannot stall retirement.
 */
const SHUTDOWN_ACK_TIMEOUT_MS = 1_000

/**
 * Ask the isolate to close its native WAL handle and release its bus lease
 * before the hub calls `Worker.terminate()`. Terminating without this leaves the
 * bun:sqlite handle (and its WAL lock) alive until process exit. Bounded: a
 * wedged or half-booted worker is still terminated after the timeout.
 */
async function requestWorkerShutdown(worker: Worker): Promise<void> {
  // A fake/partial worker without event plumbing can never deliver the ack.
  // Post the close frame (recording fakes assert on it) and return without the
  // bounded wait, so every test retire does not stall for the full budget.
  if (typeof worker.addEventListener !== "function") {
    try {
      worker.postMessage({ type: "shutdown", id: 0 })
    } catch {
      // The isolate is already gone; there is nothing left to close.
    }
    return
  }
  await new Promise<void>((resolve) => {
    let settled = false
    let timer: ReturnType<typeof setTimeout>
    const finish = (): void => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      worker.removeEventListener?.("message", onMessage)
      resolve()
    }
    const onMessage = (event: MessageEvent): void => {
      const frame = event.data as { readonly type?: unknown } | undefined
      if (frame?.type === "shutdown-ack") finish()
    }
    timer = setTimeout(finish, SHUTDOWN_ACK_TIMEOUT_MS)
    timer.unref?.()
    worker.addEventListener?.("message", onMessage)
    try {
      worker.postMessage({ type: "shutdown", id: 0 })
    } catch {
      finish()
    }
  })
}

/** A validated boot/port frame; `invalid` fails the spawn loud. */
type BootFrame =
  | { readonly kind: "ready" }
  | { readonly kind: "worker-ports"; readonly ports: WorkerTransferredPorts }
  | { readonly kind: "hub-proxy"; readonly port: MessagePort }
  /**
   * `error` preserves a typed failure (e.g. `BuildMismatch`) for the caller.
   * `booted` marks a `ready` frame that the worker actually emitted: the isolate
   * completed its DB/WAL boot, so its teardown must be orderly even though the
   * hub rejected it. A malformed frame before `ready` is not booted.
   */
  | { readonly kind: "invalid"; readonly reason: string; readonly error?: Error; readonly booted?: boolean }

const asMessagePort = (value: unknown): MessagePort | undefined =>
  typeof MessagePort !== "undefined" && value instanceof MessagePort ? value : undefined

const TRANSFERRED_PORT_FIELDS = ["egress", "egressControl"] as const

/**
 * Parse a worker boot/port frame at the boundary. A frame with a boot/port
 * `type` is never silently cast or ignored: a port field that is not a real
 * `MessagePort` (or a `ready` without a numeric thread id) is `invalid`, so the
 * transport fails the spawn loud instead of hanging on a half-booted worker.
 * Any other shape is an executor frame (`undefined`).
 */
function parseBootFrame(data: unknown): BootFrame | undefined {
  if (typeof data !== "object" || data === null) return undefined
  const frame = data as {
    readonly type?: unknown
    readonly threadId?: unknown
    readonly port?: unknown
    readonly egress?: unknown
    readonly egressControl?: unknown
  }
  if (frame.type === "ready") {
    // Any `ready` frame proves the isolate opened its DB/WAL: even a rejected
    // one must close through the orderly shutdown path, never terminate-only.
    if (typeof frame.threadId !== "number") {
      return { kind: "invalid", reason: "boot-ready frame without a numeric threadId", booted: true }
    }
    try {
      verifyReadyBuild(frame)
    } catch (error) {
      if (error instanceof BuildMismatch) return { kind: "invalid", reason: error.message, error, booted: true }
      return { kind: "invalid", reason: "boot-ready frame failed build-hash verification", booted: true }
    }
    return { kind: "ready" }
  }
  if (frame.type === "ports") {
    const ports: { -readonly [K in keyof WorkerTransferredPorts]: MessagePort | undefined } = {
      egress: undefined,
      egressControl: undefined,
    }
    for (const name of TRANSFERRED_PORT_FIELDS) {
      const value = frame[name]
      if (value === undefined) continue
      const port = asMessagePort(value)
      if (port === undefined) {
        return { kind: "invalid", reason: `ports frame field "${name}" is not a MessagePort` }
      }
      ports[name] = port
    }
    return { kind: "worker-ports", ports }
  }
  if (frame.type === "hub-proxy") {
    const port = asMessagePort(frame.port)
    return port === undefined
      ? { kind: "invalid", reason: "hub-proxy frame without a MessagePort" }
      : { kind: "hub-proxy", port }
  }
  return undefined
}

interface PendingWaiter {
  readonly predicate: (message: SessionExecutorOutbound) => boolean
  readonly resolve: (message: SessionExecutorOutbound) => void
  readonly reject: (error: Error) => void
  /** Absent when the wait has no deadline (`timeoutMs <= 0`). */
  readonly timer: ReturnType<typeof setTimeout> | undefined
}

/**
 * Process-wide live executor isolates, keyed by the database file their env
 * points at. The schema gate must observe actual spawns, not just the
 * single-flight permit: a worker releases the permit when it becomes boot-ready
 * but stays alive (and writes) for the rest of its generation. `spawnGate.held`
 * therefore cannot answer "is any worker live?". Every hub in one process shares
 * this registry; a terminated or failed isolate removes itself.
 *
 * The schema gate is per database file, so a worker only blocks a migration of
 * the database it actually uses: unrelated hubs in the same process (tests, a
 * second project) must not refuse each other.
 */
const liveExecutorWorkers = new Map<object, string | undefined>()

/** How many session-worker isolates live on `databasePath` (all files when omitted). */
export function liveSessionWorkerCount(databasePath?: string): number {
  if (databasePath === undefined) return liveExecutorWorkers.size
  const target = path.resolve(databasePath)
  let count = 0
  for (const workerPath of liveExecutorWorkers.values()) {
    if (workerPath !== undefined && path.resolve(workerPath) === target) count += 1
  }
  return count
}

/**
 * A `MessagePort`-free `ExecutorTransport` over a real Bun worker: buffers every
 * outbound frame so a `waitFor` may match one that already arrived, and fails
 * loud on timeout or isolate error.
 */
export function createBunExecutorTransport(options: BunExecutorTransportOptions): ExecutorTransport {
  const log = options.log ?? noopWorkerLogger
  const timeoutMs = options.timeoutMs ?? DEFAULT_TRANSPORT_TIMEOUT_MS
  const bufferCapacity = options.bufferCapacity ?? DEFAULT_TRANSPORT_BUFFER_CAPACITY
  const gate = options.spawnGate
  const bootTimeoutMs = options.bootTimeoutMs ?? timeoutMs
  const messages: SessionExecutorOutbound[] = []
  const waiters = new Set<PendingWaiter>()
  let alive = true
  let worker: Worker | undefined
  let booting: Promise<void> | undefined
  let bootTimer: ReturnType<typeof setTimeout> | undefined
  let bootLifecycle: BootLifecycle<Worker> | undefined
  // A boot that failed on its own (protocol error, deadline, isolate death) never
  // completed the handshake, so its teardown is terminate-only: a wedged isolate
  // cannot ack the orderly-shutdown frame. An explicit boot cancel stays orderly.
  let bootFailed = false
  // A cancel that arrives while the boot is still waiting for the global permit.
  // The cancel resolves immediately; the flag tells the pending acquisition to
  // release its eventual permit without constructing the isolate.
  let waitingCancelled = false
  let waitingCancel:
    | { readonly promise: Promise<BootCancelResult>; readonly resolve: (result: BootCancelResult) => void }
    | undefined
  // Posts that arrived before the gate granted a permit; flushed in order once
  // the isolate exists so a caller's `post`/`waitFor` pair is never reordered.
  const queued: SessionExecutorInbound[] = []
  // The error a boot/termination raised. A worker can die before a caller's
  // `waitFor` registers (an isolate that throws at import is that fast), so the
  // terminal error is remembered and replayed to a late waiter instead of
  // leaving it to time out.
  let terminal: Error | undefined
  const failWaiters = (error: Error): void => {
    terminal = error
    for (const waiter of waiters) {
      if (waiter.timer !== undefined) clearTimeout(waiter.timer)
      waiters.delete(waiter)
      waiter.reject(error)
    }
  }

  const constructWorker = (): Worker => {
    const created = new Worker(options.entry, {
      env: mergeEnv(options.env),
      // `smol` keeps each isolate's heap small; a per-session worker does not
      // need a full-size JS heap, and the cap only bounds concurrently running
      // workers, not the idle isolates a long session list can retain.
      smol: true,
      ...defined({ workerData: options.workerData, name: options.name }),
    })
    liveExecutorWorkers.set(created, options.databasePath ?? options.env?.OPENCODE_DB)
    return created
  }

  const clearBootTimer = (): void => {
    if (bootTimer !== undefined) clearTimeout(bootTimer)
    bootTimer = undefined
  }

  /**
   * Cancel a boot that is still waiting for the global permit. The caller gets
   * the typed cancel outcome NOW: the permit may be held by another worker for
   * its whole boot, and a cancel must not wait for it. The pending acquisition
   * later observes `waitingCancelled`, releases its permit, and never constructs.
   */
  const cancelWhileWaiting = (): Promise<BootCancelResult> => {
    waitingCancelled = true
    if (waitingCancel === undefined) {
      let resolve!: (result: BootCancelResult) => void
      const promise = new Promise<BootCancelResult>((settle) => {
        resolve = settle
      })
      waitingCancel = { promise, resolve }
      resolve(BOOT_CANCELLED)
    }
    return waitingCancel.promise
  }

  /**
   * Cancel the active boot interval. A boot that already started (the run was
   * handed the worker) yields to normal cancellation, so this resolves `undefined`.
   */
  const cancelBoot = (): Promise<BootCancelResult | undefined> => {
    if (bootLifecycle !== undefined) return bootLifecycle.cancel()
    if (booting !== undefined) return cancelWhileWaiting()
    return Promise.resolve(undefined)
  }

  /** Remove and return the first buffered frame matching `predicate`, if any. */
  const takeBuffered = (
    predicate: (message: SessionExecutorOutbound) => boolean,
  ): SessionExecutorOutbound | undefined => {
    const index = messages.findIndex(predicate)
    if (index === -1) return undefined
    return messages.splice(index, 1)[0]
  }

  /**
   * Tear down the isolate that owns the active boot interval. A booting/ready
   * lifecycle terminates the partial isolate itself (releasing the permit once);
   * a started worker is terminated directly.
   */
  const teardownBooted = (created: Worker): void => {
    liveExecutorWorkers.delete(created)
    if (bootLifecycle !== undefined && (bootLifecycle.state === "booting" || bootLifecycle.state === "ready")) {
      void bootLifecycle.cancel().catch(() => undefined)
      return
    }
    void created.terminate()
  }

  const connect = (created: Worker): void => {
    /**
     * Fail one spawn loud on a malformed boot/port frame, then tear the isolate
     * down. A typed cause (a `BuildMismatch` from a stale bundle) is preserved so
     * the dispatcher can refuse to retry it instead of crash-looping.
     * `booted` distinguishes a rejected `ready` (the isolate opened its DB/WAL,
     * so it needs the orderly close) from a frame before ready (never usable;
     * terminate-only).
     */
    const failProtocol = (reason: string, cause?: Error, booted = false): void => {
      const error = cause ?? new Error(`session worker protocol error: ${reason}`)
      bootFailed = !booted
      alive = false
      created.onmessage = null
      created.onerror = null
      log(workerLogMessage("worker boot failed"), {
        level: "ERROR",
        workerID: options.workerID,
        message: error.message,
      })
      failWaiters(error)
      teardownBooted(created)
    }

    created.onmessage = (event: MessageEvent) => {
      // Production sends `ports`/`hub-proxy` before the executor announces
      // `ready`, so only the validated ready handshake releases the permit.
      // Releasing there (not on construction or the first frame) is what
      // serializes the expensive cold boot; the boot timer stays armed until then.
      const bootFrame = parseBootFrame(event.data)
      if (bootFrame?.kind === "invalid") {
        failProtocol(bootFrame.reason, bootFrame.error, bootFrame.booted === true)
        return
      }
      if (bootFrame?.kind === "worker-ports") {
        options.onWorkerPorts?.(bootFrame.ports)
        return
      }
      if (bootFrame?.kind === "hub-proxy") {
        options.onHubProxyPort?.(bootFrame.port)
        return
      }
      if (bootFrame?.kind === "ready") {
        options.onPhase?.("materialize")
        // The permit is released as boot-ready only through the lifecycle's
        // `onRun`; a cancelled boot refuses the start and drops the frame.
        if (bootLifecycle !== undefined && !bootLifecycle.start(created)) return
      }
      const message = event.data as SessionExecutorOutbound
      // Offer the frame to a waiting caller BEFORE buffering, so a matching reply
      // can never be evicted by the capacity cap. A frame is consumed by exactly
      // one waiter; only a frame no waiter wants is buffered (and the buffer only
      // ever drops oldest frames that no waiter has claimed).
      for (const waiter of [...waiters]) {
        if (!waiter.predicate(message)) continue
        if (waiter.timer !== undefined) clearTimeout(waiter.timer)
        waiters.delete(waiter)
        waiter.resolve(message)
        return
      }
      messages.push(message)
      if (messages.length > bufferCapacity) messages.shift()
    }

    created.onerror = (event: ErrorEvent) => {
      bootFailed = true
      alive = false
      // Detach so a terminating worker cannot re-enter this handler, then fail
      // every waiter and tear the isolate down.
      created.onmessage = null
      created.onerror = null
      // A boot/import failure often reports an empty `message`; keep the
      // originating error's text and chain it as `cause` so the caller is not
      // left with a generic "worker crashed".
      const cause = event.error
      const detail =
        cause instanceof Error && cause.message !== "" && cause.message !== event.message ? `: ${cause.message}` : ""
      const message = `session worker error: ${event.message}${detail}`
      // A boot/import failure is the loudest signal a worker could not start;
      // report it before the waiters fail so the operator sees why.
      log(workerLogMessage("worker boot failed"), {
        level: "ERROR",
        workerID: options.workerID,
        message,
      })
      failWaiters(new Error(message, cause === undefined ? undefined : { cause }))
      teardownBooted(created)
    }

    // A worker that exits on its own (a hard crash, `process.exit`, an OOM kill)
    // reports `close`, not `error`. Without this a dead isolate would only be
    // noticed when the run reply times out, losing the crash phase truth.
    created.addEventListener?.("close", (event: Event) => {
      if (!alive) return
      bootFailed = true
      alive = false
      created.onmessage = null
      created.onerror = null
      const code = (event as Event & { readonly code?: number }).code
      failWaiters(new Error(`session worker exited with code ${code ?? "unknown"}`))
      teardownBooted(created)
    })
  }

  /**
   * Own one boot interval: the acquired permit is held until the validated
   * `ready` frame starts the run, or released exactly once when the boot is
   * cancelled/failed. The lifecycle terminates a partially materialized isolate
   * and reports the typed boot outcome to the cancel caller.
   */
  const startBoot = (acquired: SpawnPermit): void => {
    let created: Worker | undefined
    const lifecycle = createBootLifecycle<Worker>({
      permit: acquired,
      materialize: async (signal) => {
        if (signal.aborted) throw new Error("session worker boot aborted")
        // Arm the deadline before construction so a synchronous `new Worker`
        // throw still releases the global permit (the rejection path below).
        bootTimer = setTimeout(() => {
          bootTimer = undefined
          const error = new Error(`session worker boot timeout after ${bootTimeoutMs}ms`)
          bootFailed = true
          alive = false
          queued.length = 0
          failWaiters(error)
          void lifecycle.cancel().catch(() => undefined)
        }, bootTimeoutMs)
        bootTimer.unref?.()
        options.onPhase?.("boot")
        created = constructWorker()
        worker = created
        connect(created)
        for (const message of queued.splice(0)) created.postMessage(message)
        return created
      },
      terminate: async (partial) => {
        worker = undefined
        if (partial === undefined) return
        liveExecutorWorkers.delete(partial)
        // A failed boot has no acknowledged generation to drain, and the isolate
        // that missed its deadline or sent a malformed frame cannot answer the
        // shutdown frame; waiting the full ack budget would stall the released
        // permit for a worker that never became usable. Terminate it directly.
        if (bootFailed) {
          await partial.terminate()
          return
        }
        // A cancelled boot is still an orderly close: let the isolate release its
        // native WAL handle/lease through the shutdown frame before the thread is
        // destroyed. Terminate-only would strand the lock; `requestWorkerShutdown`
        // is the shared close primitive (owned by the cap/close section).
        await requestWorkerShutdown(partial)
        await partial.terminate()
      },
      onRun: () => {
        // The validated ready handshake is the one boot-ready permit release.
        clearBootTimer()
        acquired.release("ready")
      },
      onTelemetry: options.onBootTelemetry,
    })
    bootLifecycle = lifecycle
    void lifecycle.ready.then(
      (result) => {
        if (result === BOOT_CANCELLED) {
          // A cancelled boot is terminal for this transport: the run it was
          // materializing must settle instead of waiting out its deadline.
          alive = false
          queued.length = 0
          failWaiters(new Error("session worker boot cancelled"))
        }
      },
      (error: unknown) => {
        const failure = error instanceof Error ? error : new Error(String(error))
        clearBootTimer()
        alive = false
        queued.length = 0
        failWaiters(failure)
      },
    )
  }

  /**
   * Acquire the permit, construct the isolate and flush queued posts. Only used
   * when a spawn gate is configured; without one the worker is built eagerly.
   */
  const boot = (): void => {
    if (booting !== undefined || worker !== undefined || gate === undefined || !alive) return
    booting = (async () => {
      const acquired = await gate.acquire()
      // The cancel already resolved to its caller; release the permit the
      // acquisition granted and never construct the isolate. A terminate that
      // landed while waiting takes the same path without a cancel outcome.
      if (!alive || waitingCancelled) {
        const cancelled = waitingCancelled
        acquired.release("failed", cancelled ? undefined : new Error("session worker terminated before boot"))
        alive = false
        queued.length = 0
        failWaiters(new Error(cancelled ? "session worker boot cancelled" : "session worker terminated before boot"))
        if (cancelled) options.onBootTelemetry?.({ type: "boot-cancelled", phase: "boot" })
        if (waitingCancel !== undefined) {
          const waiting = waitingCancel
          waitingCancel = undefined
          waiting.resolve(BOOT_CANCELLED)
        }
        return
      }
      startBoot(acquired)
    })().catch((error) => {
      const failure = error instanceof Error ? error : new Error(String(error))
      // A cancelled waiter must settle even when the permit acquisition failed.
      if (waitingCancel !== undefined) {
        const waiting = waitingCancel
        waitingCancel = undefined
        waiting.resolve(BOOT_CANCELLED)
      }
      if (!alive) return
      alive = false
      queued.length = 0
      failWaiters(failure)
    })
  }

  if (gate === undefined) {
    worker = constructWorker()
    connect(worker)
  }

  return {
    post(message: SessionExecutorInbound) {
      if (!alive) throw new Error("cannot post to a terminated session worker")
      if (worker !== undefined) {
        worker.postMessage(message)
        return
      }
      queued.push(message)
      boot()
    },
    waitFor(predicate, waitOptions) {
      const buffered = takeBuffered(predicate)
      if (buffered !== undefined) return Promise.resolve(buffered)
      // A buffered frame still wins (it arrived before the failure); otherwise an
      // already-dead isolate fails the wait immediately rather than after the
      // full deadline.
      if (terminal !== undefined) return Promise.reject(terminal)
      // A transport observed before its first `post` still boots eagerly, so the
      // gate never depends on the caller's post/wait ordering.
      boot()
      const waitMs = waitOptions?.timeoutMs ?? timeoutMs
      return new Promise<SessionExecutorOutbound>((resolve, reject) => {
        // `timeoutMs <= 0` means "no deadline": a run/startShell reply is a whole
        // LLM turn that can outlast any fixed budget, so it never gets a timer.
        // The configured deadline applies only to the short control frames.
        const timer =
          waitMs > 0
            ? setTimeout(() => {
                waiters.delete(waiter)
                reject(new SessionWorkerReplyTimeoutError(waitMs))
              }, waitMs)
            : undefined
        const waiter: PendingWaiter = { predicate, resolve, reject, timer }
        waiters.add(waiter)
        // A caller that abandons the wait (the liveness probe on a resolved run)
        // aborts, so the waiter does not linger until its own deadline.
        waitOptions?.signal?.addEventListener(
          "abort",
          () => {
            if (!waiters.delete(waiter)) return
            if (timer !== undefined) clearTimeout(timer)
            reject(new Error("session worker wait aborted"))
          },
          { once: true },
        )
      })
    },
    get alive() {
      return alive
    },
    cancelBoot,
    async terminate(code = 0): Promise<number> {
      if (!alive) return code
      alive = false
      queued.length = 0
      // Fail every in-flight run before the bounded shutdown wait, so callers
      // observe the rejection immediately while the isolate closes its handle.
      failWaiters(new Error("session worker terminated"))
      // A booting/ready interval still owns the permit: cancelling it terminates
      // the partial isolate and releases the permit exactly once. A boot still
      // waiting for the permit resolves its waiter so the next acquisition frees.
      if (bootLifecycle !== undefined && (bootLifecycle.state === "booting" || bootLifecycle.state === "ready")) {
        await bootLifecycle.cancel().catch(() => undefined)
        return code
      }
      if (bootLifecycle === undefined && booting !== undefined) {
        void cancelWhileWaiting()
        return code
      }
      const current = worker
      worker = undefined
      if (current === undefined) return code
      liveExecutorWorkers.delete(current)
      // Close the isolate's SQLite handle/lease while its realm still runs, then
      // destroy the thread. `terminate()` alone would strand the WAL lock, so
      // this applies to an in-flight run too: a forced cancel mid-tool must not
      // keep the write lock until process exit. The wait is bounded, so a wedged
      // worker is still terminated.
      await requestWorkerShutdown(current)
      const result = await current.terminate()
      return typeof result === "number" ? result : code
    },
  }
}

export interface BunExecutorWorkerPoolOptions {
  readonly entry: string | URL
  readonly env?: Record<string, string | undefined>
  readonly workerData?: unknown
  /** The SQLite file this isolate uses, for the live-worker/schema gate. */
  readonly databasePath?: string
  readonly timeoutMs?: number
  /** Run-reply liveness budget; absent uses the default-on budget, `0` disables it. */
  readonly runIdleTimeoutMs?: number
  readonly workKind?: string
  /**
   * Per-session durable-egress barrier (plan 2.6 / R4#16): `execute` holds the
   * run until this resolves, so a result is never recorded before the run's
   * durable egress write is acked. Absent makes the result immediate.
   */
  readonly egressBarrier?: (sessionID: SessionID) => Promise<void> | undefined
  /** Receive each worker's transferred egress ports. */
  readonly onWorkerPorts?: (workerID: number, ports: WorkerTransferredPorts) => void
  /** Receive each worker's transferred hub-proxy control port (plan 24). */
  readonly onHubProxyPort?: (workerID: number, port: MessagePort) => void
  /** The session the isolate is bound to, when known at spawn (observability B1). */
  readonly sessionID?: SessionID
  /** Why this isolate is spawning; defaults to `"initial"` (observability B1). */
  readonly reason?: WorkerSpawnReason
  /** Optional structured logger seam; absent defaults to a no-op (flag-off behavior). */
  readonly log?: WorkerLogger
  /** Boot-cancellation telemetry: a cancel win or a death during abort. */
  readonly onBootTelemetry?: (event: BootTelemetry) => void
  /** Boot-stage phase reporting for the isolate. */
  readonly onPhase?: (phase: "boot" | "materialize") => void
  /** One global spawn permit; forwarded to the transport so cold spawns serialize. */
  readonly spawnGate?: SpawnGateHandle
  /** Boot-ready deadline; see {@link BunExecutorTransportOptions.bootTimeoutMs}. */
  readonly bootTimeoutMs?: number
}

/**
 * Create one eager Bun executor worker speaking the frame protocol. When a
 * spawn gate is supplied, the isolate is constructed only after the global
 * permit is granted and the permit is released on the worker's `ready` frame.
 */
export function spawnBunExecutorWorker(id: number, options: BunExecutorWorkerPoolOptions): ExecutorWorker {
  const onWorkerPorts = options.onWorkerPorts
  const onHubProxyPort = options.onHubProxyPort
  // The options doc promises a default-on budget even for a direct spawn. The
  // pool factories resolve once and pass a concrete value, so this is a no-op
  // for them (and never double-warns).
  const runIdleTimeoutMs = resolveRunIdleTimeoutMs(options)
  logRunIdleTimeoutWarning(options.log ?? noopWorkerLogger, options)
  const transport = createBunExecutorTransport({
    entry: options.entry,
    ...defined({
      env: options.env,
      databasePath: options.databasePath,
      workerData: options.workerData,
      // Born named for its bound session (or the legacy slot id when unknown)
      // so the isolate is observable from creation, not only once it self-names.
      // TODO(N-slice): route through thread-naming.ts once its seam lands.
      name: sessionThreadName(options.sessionID ?? String(id)),
      timeoutMs: options.timeoutMs,
      log: options.log,
      workerID: id,
      spawnGate: options.spawnGate,
      bootTimeoutMs: options.bootTimeoutMs,
      onBootTelemetry: options.onBootTelemetry,
      ...(options.onPhase === undefined ? {} : { onPhase: options.onPhase }),
      onWorkerPorts:
        onWorkerPorts === undefined ? undefined : (ports: WorkerTransferredPorts) => onWorkerPorts(id, ports),
      onHubProxyPort: onHubProxyPort === undefined ? undefined : (port: MessagePort) => onHubProxyPort(id, port),
    }),
  })
  return createExecutorWorker({
    id,
    transport,
    ...defined({
      workKind: options.workKind,
      timeoutMs: options.timeoutMs,
      runIdleTimeoutMs,
      sessionID: options.sessionID,
      reason: options.reason,
      egressBarrier: options.egressBarrier,
      // Forward the resolved logger so the B1 `worker spawning` line fires in
      // production, where this helper (not a test's direct `createExecutorWorker`)
      // builds the executor.
      log: options.log,
    }),
  })
}

/** Production `ExecutorWorkerPool`: spawns frame-protocol workers on demand. */
export function createBunExecutorWorkerPool(options: BunExecutorWorkerPoolOptions): ExecutorWorkerPool {
  // Resolve the default-on watchdog HERE: this factory is the documented entry
  // point, so an absent option must still arm the budget (an explicit `0`
  // disables it) rather than silently reaching the executor as `undefined`.
  const runIdleTimeoutMs = resolveRunIdleTimeoutMs(options)
  if (options.log !== undefined) logRunIdleTimeoutWarning(options.log, options)
  return createExecutorWorkerPool({
    spawn: (id) => spawnBunExecutorWorker(id, { ...options, runIdleTimeoutMs }),
    ...(options.log === undefined ? {} : { log: options.log }),
  })
}

// ---------------------------------------------------------------------------
// Lazy pool (step 12): no isolate is spawned until a worker is first used.
// ---------------------------------------------------------------------------

/** Minimum reserved child-headroom slots per nesting depth (D23). */
const WORKER_CAP_MIN_HEADROOM = 2

/**
 * A FIFO concurrency cap for running workers. At most `max` foreground runs
 * (depth 0) execute at once; the rest wait in arrival order. A parent that
 * awaits a foreground `task` child keeps its own slot, and the depth-1 child
 * runs in bounded headroom instead of deadlocking on the same budget. Each
 * nesting depth gets its own headroom, so a depth-N child can always start once
 * its ancestors are blocked.
 *
 * This is scheduling state only — it never rejects work and never owns
 * admission (the dispatcher does).
 */
export interface WorkerCap {
  /** Acquire one slot for a run at `depth` (0/absent = foreground). */
  acquire(depth?: number): Promise<() => void>
  readonly running: number
  readonly queued: number
}

export function createWorkerCap(max: number): WorkerCap {
  if (!Number.isInteger(max) || max < 1) {
    throw new Error(`worker cap must be a positive integer, received ${String(max)}`)
  }
  // Additive, transient headroom per nesting depth: bounded so child fan-out
  // cannot oversubscribe the machine, but never part of the foreground `max`.
  const headroom = Math.max(WORKER_CAP_MIN_HEADROOM, Math.ceil(max / 2))
  let foreground = 0
  const foregroundWaiters: Array<() => void> = []
  interface Headroom {
    running: number
    readonly waiters: Array<() => void>
  }
  const depths = new Map<number, Headroom>()
  const headroomAt = (depth: number): Headroom => {
    const existing = depths.get(depth)
    if (existing !== undefined) return existing
    const created: Headroom = { running: 0, waiters: [] }
    depths.set(depth, created)
    return created
  }

  /** A release frees exactly one slot, and only its own budget (idempotent). */
  const releaseOf = (budget: "foreground" | Headroom): (() => void) => {
    let released = false
    return () => {
      if (released) return
      released = true
      if (budget === "foreground") {
        foreground -= 1
        foregroundWaiters.shift()?.()
        return
      }
      budget.running -= 1
      budget.waiters.shift()?.()
    }
  }

  const acquire = (depth = 0): Promise<() => void> => {
    // A malformed depth is a foreground run, matching the executor frame parse.
    const level = Number.isInteger(depth) && depth > 0 ? depth : 0
    return new Promise<() => void>((resolve) => {
      if (level === 0) {
        if (foreground < max) {
          foreground += 1
          resolve(releaseOf("foreground"))
          return
        }
        foregroundWaiters.push(() => {
          foreground += 1
          resolve(releaseOf("foreground"))
        })
        return
      }
      const budget = headroomAt(level)
      if (budget.running < headroom) {
        budget.running += 1
        resolve(releaseOf(budget))
        return
      }
      budget.waiters.push(() => {
        budget.running += 1
        resolve(releaseOf(budget))
      })
    })
  }

  return {
    get running() {
      let total = foreground
      for (const budget of depths.values()) total += budget.running
      return total
    },
    get queued() {
      let total = foregroundWaiters.length
      for (const budget of depths.values()) total += budget.waiters.length
      return total
    },
    acquire,
  }
}

/**
 * The mutable materialization state shared by every worker whose isolate is
 * created on first use. The `materialize` policy owns the *why* (a lazy factory
 * ignores the session; a session-bound factory rebinds); this helper owns the
 * worker surface and the terminate race.
 */
interface MaterializingWorkerState {
  /** The live isolate, or `undefined` before the first materialization. */
  inner: ExecutorWorker | undefined
  /** Set once `terminate` runs; a late materialization must never publish. */
  terminated: boolean
  /** The in-flight materialization, so concurrent executes share one isolate. */
  materializing: Promise<ExecutorWorker> | undefined
}

/**
 * Build the `ExecutorWorker` surface around a `materialize` policy, which
 * resolves the isolate for a run's session and writes it back to `state.inner`.
 * The helper enforces the terminate race: a terminate during materialization
 * discards the fresh isolate rather than publishing a live worker. Before the
 * first materialization, `cancel`/`assertNotBusy`/`resultFor` are no-ops, which
 * is correct because no session can run on a worker that was never started.
 */
function createMaterializingExecutorWorker(
  id: number,
  materialize: (sessionID: SessionID, state: MaterializingWorkerState) => Promise<ExecutorWorker>,
  log: WorkerLogger = noopWorkerLogger,
  cap?: WorkerCap,
): ExecutorWorker {
  const state: MaterializingWorkerState = { inner: undefined, terminated: false, materializing: undefined }
  const worker: ExecutorWorker = {
    id,
    get alive() {
      return !state.terminated && (state.inner === undefined || state.inner.alive)
    },
    execute: async (run) => {
      // Cap the number of concurrently *running* foreground workers; extra
      // sessions queue here in FIFO order instead of oversubscribing the CPU.
      // A depth >= 1 child takes headroom at its own depth, so a parent that
      // awaits a foreground `task` child never deadlocks on the parent's slot.
      const release = await cap?.acquire(run.depth)
      try {
        return await (await materialize(run.sessionID, state)).execute(run)
      } finally {
        release?.()
      }
    },
    cancel: (sessionID) => (state.inner === undefined ? Promise.resolve() : state.inner.cancel(sessionID)),
    cancelBoot: () => state.inner?.cancelBoot?.() ?? Promise.resolve(undefined),
    assertNotBusy: (sessionID) =>
      state.inner === undefined ? Promise.resolve(false) : state.inner.assertNotBusy(sessionID),
    resultFor: (sessionID) => state.inner?.resultFor(sessionID),
    cancelPort: createExecutorCancelPort({ cancel: (sessionID) => worker.cancel(sessionID), log }),
    terminate: async () => {
      state.terminated = true
      state.materializing = undefined
      const current = state.inner
      state.inner = undefined
      if (current !== undefined) await current.terminate()
    },
  }
  return worker
}

/**
 * Wrap a worker factory so the underlying isolate is created on first use. The
 * dispatcher reads `alive`/builds slots at construction, so a lazy worker reports
 * itself alive (and free) before it exists; the isolate appears on the first
 * `execute`. The factory may be async (the production init is resolved on first
 * dispatch).
 */
export function createLazyExecutorWorker(
  id: number,
  create: LazyExecutorWorkerFactory,
  log: WorkerLogger = noopWorkerLogger,
  cap?: WorkerCap,
): ExecutorWorker {
  return createMaterializingExecutorWorker(
    id,
    async (_sessionID, state) => {
      if (state.terminated) throw new Error(`executor worker ${id} is terminated`)
      if (state.inner !== undefined) return state.inner
      state.materializing ??= Promise.resolve()
        .then(create)
        .then((worker) => {
          // A terminate raced the materialisation: never publish a live worker.
          if (state.terminated) {
            void worker.terminate()
            throw new Error(`executor worker ${id} is terminated`)
          }
          state.inner = worker
          return worker
        })
      return state.materializing
    },
    log,
    cap,
  )
}

/** Build one worker on first use; may be async (the production init is resolved on dispatch). */
export type LazyExecutorWorkerFactory = () => ExecutorWorker | Promise<ExecutorWorker>

export interface LazyExecutorWorkerPoolOptions {
  /** Build one worker on first use. */
  readonly create: (id: number) => ExecutorWorker | Promise<ExecutorWorker>
  /** Optional structured logger seam; absent defaults to a no-op (flag-off behavior). */
  readonly log?: WorkerLogger
  /** Concurrent-running cap; defaults to the CPU-core count. */
  readonly maxRunning?: number
}

/** A pool whose workers spawn on first use (per session). */
export function createLazyExecutorWorkerPool(options: LazyExecutorWorkerPoolOptions): ExecutorWorkerPool {
  const cap = createWorkerCap(options.maxRunning ?? os.availableParallelism())
  return createExecutorWorkerPool({
    spawn: (id) => createLazyExecutorWorker(id, () => options.create(id), options.log, cap),
  })
}

// ---------------------------------------------------------------------------
// Production pool + hub factories (steps 10 + 13 + 14).
// ---------------------------------------------------------------------------

declare global {
  /**
   * Compile-time path to the bundled session-worker isolate, injected by
   * `script/build.ts` (mirrors `OPENCODE_WORKER_PATH`). Absent in dev/test.
   */
  const OPENCODE_SESSION_WORKER_PATH: string | undefined
}

/** The mechanism a resolved session-worker entry came from. */
export type SessionWorkerEntrySource = "env" | "definedPath" | "default"

/** A resolved session-worker entry and where it came from. */
export interface DescribedSessionWorkerEntry {
  readonly entry: string | URL
  readonly source: SessionWorkerEntrySource
}

/**
 * Resolve the production session-worker entry AND the mechanism it came from:
 * the runtime env override (`"env"`), the build-time define / an explicit path
 * (`"definedPath"`), or the source-relative isolate (`"default"`). Pure, so the
 * resolution (and its diagnostics) are testable without re-importing the module.
 */
export function describeSessionWorkerEntry(
  options: { readonly env?: Record<string, string | undefined>; readonly definedPath?: string } = {},
): DescribedSessionWorkerEntry {
  const defined =
    options.definedPath ??
    (typeof OPENCODE_SESSION_WORKER_PATH !== "undefined" ? OPENCODE_SESSION_WORKER_PATH : undefined)
  const fromEnv = (options.env ?? process.env).OPENCODE_SESSION_WORKER_PATH
  // The env override wins when set (even an empty string, which falls through to
  // the default — matching `resolveSessionWorkerEntry`'s original precedence).
  const resolved = fromEnv ?? defined
  if (resolved === undefined || resolved === "") {
    return { entry: new URL("./session-worker.ts", import.meta.url), source: "default" }
  }
  return { entry: resolved, source: fromEnv !== undefined ? "env" : "definedPath" }
}

/**
 * Resolve the production session-worker entry: the runtime env override, else
 * the build-time define, else the source-relative isolate.
 */
export function resolveSessionWorkerEntry(
  options: { readonly env?: Record<string, string | undefined>; readonly definedPath?: string } = {},
): string | URL {
  return describeSessionWorkerEntry(options).entry
}

/** The production session-worker entry (the reduced-layer isolate). */
export const SESSION_WORKER_ENTRY: string | URL = resolveSessionWorkerEntry()

/**
 * Default run-reply watchdog budget. The watchdog pings the worker between run
 * frames, so a healthy long turn survives however long it takes; only a wedged
 * event loop (no `pong` for this window) times out. A value of `0` disables it.
 */
export const DEFAULT_RUN_IDLE_TIMEOUT_MS = 60_000

/**
 * Resolve the run-reply watchdog budget, defaulting it ON. An explicit option
 * wins (including `0` to disable). Otherwise an environment override is honored:
 * `0` disables it explicitly, a positive integer sets the window, and any
 * malformed value (negative, fractional, non-numeric) falls back to
 * {@link DEFAULT_RUN_IDLE_TIMEOUT_MS} with a WARN from
 * {@link runIdleTimeoutWarning}. An absent/empty variable uses the default.
 */
export function resolveRunIdleTimeoutMs(options: {
  readonly runIdleTimeoutMs?: number
  readonly env?: Record<string, string | undefined>
}): number {
  if (options.runIdleTimeoutMs !== undefined) return options.runIdleTimeoutMs
  const parsed = parseRunIdleTimeoutEnv((options.env ?? process.env)[SESSION_WORKER_IDLE_TIMEOUT_ENV])
  return parsed.kind === "value" ? parsed.ms : parsed.kind === "disabled" ? 0 : DEFAULT_RUN_IDLE_TIMEOUT_MS
}

/**
 * Why the watchdog budget fell back to the default, or `undefined` when the knob
 * is absent, empty, `0`, or a valid value. Callers emit this at WARN at startup.
 */
export function runIdleTimeoutWarning(options: {
  readonly runIdleTimeoutMs?: number
  readonly env?: Record<string, string | undefined>
}): string | undefined {
  if (options.runIdleTimeoutMs !== undefined) return undefined
  const parsed = parseRunIdleTimeoutEnv((options.env ?? process.env)[SESSION_WORKER_IDLE_TIMEOUT_ENV])
  if (parsed.kind !== "invalid") return undefined
  return `invalid ${SESSION_WORKER_IDLE_TIMEOUT_ENV}: ${parsed.raw} (expected a positive integer or 0); using default ${DEFAULT_RUN_IDLE_TIMEOUT_MS} ms`
}

type ParsedRunIdleTimeout =
  | { readonly kind: "absent" }
  | { readonly kind: "disabled" }
  | { readonly kind: "value"; readonly ms: number }
  | { readonly kind: "invalid"; readonly raw: string }

function parseRunIdleTimeoutEnv(raw: string | undefined): ParsedRunIdleTimeout {
  if (raw === undefined || raw.trim() === "") return { kind: "absent" }
  if (raw.trim() === "0") return { kind: "disabled" }
  const parsed = Number(raw)
  if (!Number.isInteger(parsed) || parsed <= 0) return { kind: "invalid", raw }
  return { kind: "value", ms: parsed }
}

/** Emit the malformed-watchdog warning once per resolved pool/hub. */
function logRunIdleTimeoutWarning(
  log: WorkerLogger,
  options: {
    readonly runIdleTimeoutMs?: number
    readonly env?: Record<string, string | undefined>
  },
): void {
  const warning = runIdleTimeoutWarning(options)
  if (warning === undefined) return
  log(workerLogMessage("run idle timeout invalid"), { level: "WARN", reason: warning })
}

/**
 * A worker `workerData` payload, or a (possibly async) resolver for one. The
 * resolver receives the session being dispatched, so the production hub resolves
 * the session's own model rather than reusing the first session's.
 */
export type SessionWorkerInitSource =
  | SessionWorkerInit
  | ((sessionID?: SessionID) => SessionWorkerInit | Promise<SessionWorkerInit>)

export interface ProductionWorkerPoolOptions {
  readonly init: SessionWorkerInitSource
  /** Worker entry; defaults to {@link SESSION_WORKER_ENTRY} (overridable for tests). */
  readonly entry?: string | URL
  readonly env?: Record<string, string | undefined>
  /** The SQLite file this pool's isolates use, for the live-worker/schema gate. */
  readonly databasePath?: string
  readonly timeoutMs?: number
  /** Run-reply liveness budget; absent uses the default-on budget, `0` disables it. */
  readonly runIdleTimeoutMs?: number
  readonly workKind?: string
  /** Per-session durable-egress barrier (plan 2.6 / R4#16). */
  readonly egressBarrier?: (sessionID: SessionID) => Promise<void> | undefined
  readonly onWorkerPorts?: (workerID: number, ports: WorkerTransferredPorts) => void
  readonly onHubProxyPort?: (workerID: number, port: MessagePort) => void
  /**
   * Fired when a worker rebinds away from a session (its isolate is retired and
   * respawned for a different session). The production hub drops that session's
   * per-session instance/context so those maps cannot grow without bound.
   */
  readonly onSessionRebound?: (sessionID: SessionID) => void
  /** Pool-side cleanup when a session's worker is retired (init cache eviction). */
  readonly onSessionReleased?: (sessionID: SessionID) => void
  /** One global spawn permit shared by every isolate this pool spawns/re-spawns. */
  readonly spawnGate?: SpawnGateHandle
  /** Boot-ready deadline; see {@link BunExecutorTransportOptions.bootTimeoutMs}. */
  readonly bootTimeoutMs?: number
  /** Optional structured logger seam; absent defaults to a no-op (flag-off behavior). */
  readonly log?: WorkerLogger
  /** Boot-cancellation telemetry: a cancel win or a death during abort. */
  readonly onBootTelemetry?: (event: BootTelemetry) => void
  /** Boot-stage phase reporting for the session this isolate is bound to. */
  readonly onPhase?: (sessionID: SessionID, phase: "boot" | "materialize") => void
  /** Concurrent-running cap; defaults to the CPU-core count. */
  readonly maxRunning?: number
}

/** Build one isolate for `sessionID` under a fixed worker id. */
export type SessionBoundSpawner = (id: number, sessionID: SessionID) => Promise<ExecutorWorker>

/**
 * A worker whose isolate is bound to ONE dispatched session. A worker isolate
 * owns exactly one serialized model (`reducedModelProviderLayer` rejects any
 * other with `WorkerModelMismatchError`), so when the dispatcher hands this
 * worker a *different* session it terminates the old isolate and respawns under
 * the same id with that session's init. Reuse across turns of the same session
 * (the common case) keeps the isolate.
 */
export function createSessionBoundExecutorWorker(
  id: number,
  spawnFor: SessionBoundSpawner,
  log: WorkerLogger = noopWorkerLogger,
  onRetire?: (sessionID: SessionID) => void,
  cap?: WorkerCap,
): ExecutorWorker {
  let boundSession: SessionID | undefined
  return createMaterializingExecutorWorker(
    id,
    (sessionID, state) => {
      if (state.terminated) throw new Error(`executor worker ${id} is terminated`)
      if (state.inner !== undefined && boundSession === sessionID) return Promise.resolve(state.inner)
      const previousSession = boundSession
      const previous = state.inner
      state.inner = undefined
      boundSession = sessionID
      // A worker rebinding from one session to another is the observable signal
      // that the isolate is being retired and rebuilt for a different model.
      if (previousSession !== undefined && previousSession !== sessionID) {
        // The old session's isolate is being retired; let the hub release its
        // per-session state before the new session's init is resolved.
        onRetire?.(previousSession)
        log(workerLogMessage("worker rebound"), {
          level: "INFO",
          workerID: id,
          fromSession: previousSession,
          toSession: sessionID,
          // The rebound-to session is always defined here (the branch guards
          // `previousSession !== undefined`), so the log seam hoists it into the
          // message prefix. `fromSession`/`toSession` stay for backward compat.
          sessionID,
        })
      }
      state.materializing = Promise.resolve()
        // Retire the old isolate first so two models never share one worker.
        .then(() => previous?.terminate().catch(() => undefined))
        .then(() => spawnFor(id, sessionID))
        .then((worker) => {
          if (state.terminated) {
            void worker.terminate()
            throw new Error(`executor worker ${id} is terminated`)
          }
          state.inner = worker
          return worker
        })
      return state.materializing
    },
    log,
    cap,
  )
}

/**
 * Production `ExecutorWorkerPool` over the session-worker entry. Lazy: building
 * the pool spawns no isolates. Each worker resolves its init for the session it
 * is asked to run (and rebinds when that session changes), so multiple sessions
 * with distinct models share a generation without a `WorkerModelMismatchError`.
 */
export function createProductionWorkerPool(options: ProductionWorkerPoolOptions): ExecutorWorkerPool {
  const log = options.log ?? noopWorkerLogger
  // Default the liveness watchdog ON unless the caller or env explicitly opts out.
  const runIdleTimeoutMs = resolveRunIdleTimeoutMs(options)
  logRunIdleTimeoutWarning(log, options)
  const resolveSource: (sessionID?: SessionID) => SessionWorkerInit | Promise<SessionWorkerInit> =
    typeof options.init === "function" ? options.init : () => options.init as SessionWorkerInit
  // One init per session: a session's model/variants are stable for its lifetime.
  const initBySession = new Map<string, Promise<SessionWorkerInit>>()
  const resolveInit = (sessionID: SessionID): Promise<SessionWorkerInit> => {
    const cached = initBySession.get(sessionID)
    if (cached !== undefined) return cached
    const resolved = Promise.resolve().then(() => resolveSource(sessionID))
    initBySession.set(sessionID, resolved)
    return resolved
  }
  const spawnFor: SessionBoundSpawner = async (id, sessionID) => {
    const init = await resolveInit(sessionID)
    return spawnBunExecutorWorker(id, {
      entry: options.entry ?? SESSION_WORKER_ENTRY,
      // Ship the pool id so the worker's `pid-report` frames are keyed to this
      // slot out of the box (the hub still treats its port association as authoritative).
      // The pool only spawns session workers, so the role is pinned `session`.
      workerData: { ...init, workerID: id, role: "session", sessionID },
      ...defined({
        env: options.env,
        databasePath: options.databasePath,
        timeoutMs: options.timeoutMs,
        runIdleTimeoutMs,
        workKind: options.workKind,
        egressBarrier: options.egressBarrier,
        log,
        // The session this isolate is bound to, so the B1 `worker spawning`
        // line names it (the init was already resolved for the same session).
        sessionID,
        spawnGate: options.spawnGate,
        bootTimeoutMs: options.bootTimeoutMs,
        onBootTelemetry: options.onBootTelemetry,
        ...(options.onPhase === undefined
          ? {}
          : { onPhase: (phase: "boot" | "materialize") => options.onPhase?.(sessionID, phase) }),
        onWorkerPorts: options.onWorkerPorts,
        onHubProxyPort: options.onHubProxyPort,
      }),
    })
  }
  const cap = createWorkerCap(options.maxRunning ?? os.availableParallelism())
  return createExecutorWorkerPool({
    spawn: (id) => createSessionBoundExecutorWorker(id, spawnFor, log, options.onSessionRebound, cap),
    onSessionReleased: (sessionID) => {
      initBySession.delete(sessionID)
      options.onSessionReleased?.(sessionID)
    },
    log,
  })
}

/** The port-transfer sinks a production pool routes worker messages to. */
export interface WorkerPoolSinks {
  readonly onWorkerPorts: (workerID: number, ports: WorkerTransferredPorts) => void
  readonly onHubProxyPort: (workerID: number, port: MessagePort) => void
}

/** Builds the executor pool from a lazy init resolver and port sinks (test seam). */
export type ProductionPoolFactory = (
  resolveInit: (sessionID?: SessionID) => Promise<SessionWorkerInit>,
  sinks: WorkerPoolSinks,
  /**
   * The dispatch cap resolved from `maxRunningWorkers` (or the CPU default). A
   * factory that builds a lazy pool should use it so the test/production cap is
   * the one actually applied.
   */
  runtime: { readonly maxRunning: number },
) => ExecutorWorkerPool

/** Mark a generation as serving the worker egress ports. */
function withSessionWorkerPorts(init: SessionWorkerInit): SessionWorkerInit {
  return { ...init, capabilities: { ...init.capabilities, sessionWorkerPorts: true } }
}

/**
 * Fallback instance context for a dispatch with no ambient `InstanceRef`.
 *
 * A dispatch that reaches the hub without an ambient instance is a hub-side gap,
 * but the worker still needs a real project identity: `buildWorkerSnapshot` ships
 * `ctx.project.id`/`ctx.project.vcs`, and `workerInstanceContext` rebuilds the
 * worker's `InstanceRef` from them. The global project is the canonical identity
 * for "no resolved project", so the worker never falls back to the directory
 * string as a project id.
 */
export function fallbackInstanceContext(): InstanceContext {
  const directory = process.cwd()
  return makeInstanceContext({ directory, projectID: Project.ID.global })
}

/** The ambient instance context for the current fiber, if any (the `attach` pattern). */
function captureInstanceRef(): InstanceContext | undefined {
  const fiber = Fiber.getCurrent()
  return fiber ? Context.getReferenceUnsafe(fiber.context, InstanceRef) : undefined
}

/**
 * The full context of the current fiber, if any. Captured at dispatch so a
 * later hub-proxy handler (invoked from a MessagePort callback, outside any
 * fiber) can run a hub authority effect under the dispatching instance's
 * `InstanceRef`/`WorkspaceRef` references.
 */
function captureRuntimeContext(): Context.Context<never> | undefined {
  return Fiber.getCurrent()?.context
}

/** The per-request plugin input the hub can resolve before a worker runs. */
interface DispatchedSessionRequest {
  readonly agent?: string
  readonly message?: unknown
}

/** The session-derived inputs the worker init needs, loaded in one `Session.get`. */
interface DispatchedSession {
  /** The session's persisted provider/model pair, when it has one. */
  readonly model?: WorkerModelSelection
  /** The per-request plugin input (agent + last user message), when resolvable. */
  readonly request?: DispatchedSessionRequest
  /** The session's persisted working directory, when the session was found. */
  readonly directory?: string
  /** The session's persisted project id, when the session was found. */
  readonly projectID?: string
  /**
   * The persisted parent edge, when this session is a child. The hub mirrors it
   * into the synchronous cascade index on worker boot.
   */
  readonly parentID?: SessionID
  /**
   * The persisted subagent depth (`metadata.depth`). Mirrored into the dispatch
   * depth index so a re-dispatched child keeps its cap headroom level.
   */
  readonly depth?: number
}

/**
 * Synthesize the resolved instance for a session from its PERSISTED directory.
 *
 * The hub may reach `resolveInit` without an ambient `InstanceRef` (the pool and
 * worker-build fibers are outside the request fiber), so the session's own
 * persisted `directory` is the authoritative fallback — never the server's
 * `process.cwd()` and never another project's directory. `worktree` is not
 * persisted, so it mirrors `directory`, matching `workerInstanceContext`'s
 * default (`session-worker.ts`).
 */
export function instanceFromSession(session: DispatchedSession): InstanceContext | undefined {
  const directory = session.directory
  if (directory === undefined) return undefined
  return makeInstanceContext({ directory, projectID: session.projectID ?? Project.ID.global })
}

/**
 * Load the dispatching session's init inputs from the caller context captured at
 * dispatch, in ONE `Session.get`. The hub node deliberately does not declare
 * `Session` as a dependency (the graph stays stable), so it is read optionally:
 * `undefined` means the ambient session store is absent, and the worker falls
 * back to the config/default model and the documented empty plugin input.
 *
 * This is the latest the hub can resolve: a worker binds to one session at init,
 * so the last user message at bind time is the message the first turn processes.
 * A later turn on the SAME worker (no rebind) stays approximate until it rebinds.
 */
async function loadDispatchedSession(
  context: Context.Context<never> | undefined,
  sessionID: SessionID | undefined,
): Promise<DispatchedSession | undefined> {
  if (context === undefined || sessionID === undefined) return undefined
  const session = Context.getOption(context, Session.Service)
  if (Option.isNone(session)) return undefined
  const info = await Effect.runPromise(
    session.value.get(sessionID).pipe(Effect.match({ onFailure: () => undefined, onSuccess: (value) => value })),
  )
  // `findMessage` searches newest-first, so this is the message being processed.
  const lastUser = await Effect.runPromise(
    session.value
      .findMessage(sessionID, (message) => message.info.role === "user")
      .pipe(Effect.match({ onFailure: () => Option.none(), onSuccess: (value) => value })),
  )
  const message = Option.isSome(lastUser) ? lastUser.value.info : undefined
  // The user message carries the per-request agent; the session's persisted agent
  // is the fallback when the message is absent.
  const agent = (message?.role === "user" ? message.agent : undefined) ?? info?.agent
  const model = info?.model
  return {
    ...(model === undefined ? {} : { model: { providerID: model.providerID, modelID: model.id } }),
    ...(agent === undefined && message === undefined
      ? {}
      : { request: { ...(agent === undefined ? {} : { agent }), ...(message === undefined ? {} : { message }) } }),
    // The session's own persisted project identity, so an init resolved without
    // an ambient `InstanceRef` still binds the worker to this session's project.
    ...(info?.directory === undefined ? {} : { directory: info.directory }),
    ...(info?.projectID === undefined ? {} : { projectID: String(info.projectID) }),
    ...(info?.parentID === undefined ? {} : { parentID: info.parentID }),
    // `metadata.depth` is the persisted nesting level `Session.spawn` stamped.
    // Recording it on boot keeps the cap headroom correct after a restart or a
    // worker retire/re-dispatch.
    ...(info === undefined ? {} : { depth: spawnDepth(info.metadata) }),
  }
}

/**
 * Resolve the hub's plugin hooks from the dispatch context. The hub node
 * deliberately does not declare `Plugin` as a dependency (the graph stays
 * stable), so it is read optionally: absent means the worker ships no plugin
 * effects and the reduced LLM keeps its no-op pass-through. Hook *functions*
 * cannot cross the isolate; the init resolver runs their effect first.
 */
async function resolveContextPluginSources(
  context: Context.Context<never> | undefined,
): Promise<readonly WorkerPluginSource[] | undefined> {
  if (context === undefined) return undefined
  const plugin = Context.getOption(context, Plugin.Service)
  if (Option.isNone(plugin)) return undefined
  // `Plugin.list` resolves the per-directory `InstanceState`, so run it under the
  // dispatching instance's captured context.
  const hooks = await Effect.runPromise(Effect.provide(plugin.value.list(), context))
  if (hooks.length === 0) return undefined
  return hooks.map((hook, index) => ({ name: `plugin-${index}`, hooks: hook as WorkerPluginHooks }))
}

/**
 * Resolve the clone-safe plugin-tool descriptors for a dispatch context. The real
 * plugin tools live on the hub (`Plugin.list()` → `Hooks.tool`); only their
 * clone-safe `{ id, description, jsonSchema }` projection crosses into the worker
 * init. Absent context/plugin resolves to no descriptors — a degraded boot simply
 * lists no plugin tools, never a failure.
 *
 * The `jsonSchema` is computed by the registry's exported `pluginToolJsonSchema`,
 * the single shared implementation the hub registry itself uses, so a shipped
 * descriptor can never drift from what `fromPlugin` would advertise. `hub.ts` may
 * import `@/tool/registry` here without a module cycle: the registry's only
 * `session/worker` dependency is the dependency-light `plugin-tool-wire` leaf.
 */
export async function resolveContextPluginToolDescriptors(
  context: Context.Context<never> | undefined,
): Promise<readonly PluginToolDescriptor[]> {
  if (context === undefined) return []
  const plugin = Context.getOption(context, Plugin.Service)
  if (Option.isNone(plugin)) return []
  // `Plugin.list` resolves the per-directory `InstanceState`, so run it under the
  // dispatching instance's captured context.
  const hooks = await Effect.runPromise(Effect.provide(plugin.value.list(), context))
  const descriptors: PluginToolDescriptor[] = []
  for (const hook of hooks) {
    for (const [id, definition] of Object.entries(hook.tool ?? {})) {
      descriptors.push({ id, description: definition.description, jsonSchema: pluginToolJsonSchema(definition) })
    }
  }
  return descriptors
}

export interface WorkerCancellationOptions {
  readonly graceMs?: number
  readonly schedule?: CancellationConfig["schedule"]
  readonly reap?: (worker: SessionWorker) => void
  /** Hub shutdown reap; defaults to the production PidRegistry's `reapAll`. */
  readonly reapAll?: () => void
  readonly childrenOf?: (sessionID: SessionID) => ReadonlyArray<SessionID>
  readonly onCancelled?: CancellationConfig["onCancelled"]
}

/**
 * Phase-11 cancellation wiring for a pool: `portFor` returns the worker's
 * cooperative `cancelPort` (step 13), and the grace defaults to
 * `OPENCODE_SESSION_CANCEL_GRACE_MS`.
 */
export function createWorkerCancellation(
  pool: ExecutorWorkerPool,
  options: WorkerCancellationOptions = {},
): CancellationConfig {
  return {
    portFor: (worker) => pool.byID(worker.id)?.cancelPort,
    graceMs: options.graceMs ?? resolveCancelGraceMs(),
    ...defined({
      schedule: options.schedule,
      reap: options.reap,
      reapAll: options.reapAll,
      childrenOf: options.childrenOf,
      onCancelled: options.onCancelled,
    }),
  }
}

/**
 * Configuration for {@link buildProductionPortBackends}. Every field is an
 * override; the defaults are the real hub authorities.
 */
export interface ProductionPortBackendsOptions {
  /** Override the egress drain; defaults to {@link drainEgressPort}. */
  readonly egress?: (workerID: number, port: MessagePort, control?: MessagePort) => () => void
  /**
   * Live-only event publisher. When supplied, the default egress backend serves
   * the worker's events port with {@link serveEventsPort} + {@link createHubEventSink}
   * (republishing each event on the hub); without it the port is only drained.
   */
  readonly events?: LiveEventPublisher
  /**
   * Per-worker live-only event publisher. A worker is bound to exactly one
   * session, so the production layer resolves the worker's session location here
   * rather than stamping every republished event with a first-dispatch directory.
   * Takes precedence over {@link events} for the worker's events port.
   */
  readonly eventsForWorker?: (workerID: number) => LiveEventPublisher | undefined
  /** Advisory hub cache invalidated by each durable event (never authoritative). */
  readonly cache?: HubCacheInvalidation
  /**
   * Per-worker mesh subscription backlog bound; defaults to the mesh's own
   * `MESH_SUBSCRIPTION_BOUND`. A small bound makes the overflow/resync path
   * deterministic in tests.
   */
  readonly egressMeshBound?: number
  /**
   * Handlers for the three hub-proxy authorities served over the worker control
   * port. Supplied by `productionSessionHubLayer`, which
   * yields the real `SessionStatus` / `Permission` / `Question` services.
   */
  readonly hubProxy?: HubProxyHandlers
  /** Per-worker hub-proxy handlers (each control port served under its worker's session). */
  readonly hubProxyFor?: (workerID: number) => HubProxyHandlers | undefined
  /** Per-worker streaming hub-proxy handlers (see {@link WorkerPortBackends.hubProxyStreamingFor}). */
  readonly hubProxyStreamingFor?: (workerID: number) => Readonly<Record<string, HubProxyStreamHandler>> | undefined
  /** Streaming hub-proxy handlers shared by every worker. */
  readonly hubProxyStreaming?: Readonly<Record<string, HubProxyStreamHandler>>
}

/** The production port authorities plus the disposer that tears their isolates down. */
export interface ProductionPortBackends extends WorkerPortBackends {
  /**
   * The ONE egress mesh the hub serves every worker's `egressControl` port
   * against. Exposed so a caller (or test) can observe the cross-isolate
   * `durable()` wakeups; commits are published on the events-port durable path.
   */
  readonly egressMesh: EgressMesh
  readonly dispose: () => Promise<void>
}

/**
 * Build the production hub-side port authorities:
 *   * `egress` — the draining receiver ({@link drainEgressPort}); the SSE route
 *     swaps in a real sink later.
 *
 * There is no writer authority (D5) and no broker (D20). `dispose` tears the
 * mesh/port state down; it is safe to call when nothing was attached.
 */
export function buildProductionPortBackends(options: ProductionPortBackendsOptions = {}): ProductionPortBackends {
  // ONE mesh per hub: every worker's transferred `egressControl` port attaches
  // here, and the durable commit path publishes to it.
  const egressMesh = createEgressMesh(options.egressMeshBound === undefined ? {} : { bound: options.egressMeshBound })
  return {
    egressMesh,
    ...(options.hubProxy === undefined ? {} : { hubProxy: options.hubProxy }),
    ...(options.hubProxyFor === undefined ? {} : { hubProxyFor: options.hubProxyFor }),
    ...(options.hubProxyStreaming === undefined ? {} : { hubProxyStreaming: options.hubProxyStreaming }),
    ...(options.hubProxyStreamingFor === undefined ? {} : { hubProxyStreamingFor: options.hubProxyStreamingFor }),
    // The default egress backend serves the data port AND attaches the worker's
    // control plane to the shared mesh; both the receiver and the attachment are
    // disposed with the port. A durable event reaching the data port is published
    // to the mesh, waking other workers' `durable()` subscribers.
    egress:
      options.egress ??
      ((workerID, port, control) => {
        const attachment = control === undefined ? undefined : attachMeshWorker(egressMesh, String(workerID), control)
        // Prefer the worker's own live publisher (its session's location); fall
        // back to the shared one for callers that supply a single publisher.
        const publisher = options.eventsForWorker?.(workerID) ?? options.events
        const disposeReceiver =
          publisher === undefined
            ? drainEgressPort(port)
            : serveEventsPort(
                port,
                createHubEventSink({
                  publisher,
                  ...(options.cache === undefined ? {} : { cache: options.cache }),
                }),
                undefined,
                (commit) => egressMesh.publish(commit),
              )
        return () => {
          disposeReceiver()
          attachment?.dispose()
        }
      }),
    dispose: async () => {},
  }
}

export interface ProductionSessionHubOptions {
  /** The `workerData` payload, or a resolver resolved once on the first dispatch. */
  readonly init: SessionWorkerInitSource
  /** Informational worker count carried on each worker snapshot (no pool prewarm). */
  readonly count?: number
  readonly entry?: string | URL
  readonly timeoutMs?: number
  /**
   * Run-reply liveness budget for the spawned workers. Defaults to
   * `OPENCODE_SESSION_WORKER_IDLE_TIMEOUT_MS`, then `DEFAULT_RUN_IDLE_TIMEOUT_MS`.
   */
  readonly runIdleTimeoutMs?: number
  readonly workKind?: string
  readonly env?: Record<string, string | undefined>
  /** Cancellation tuning; cooperative cancel is always wired (step 13). */
  readonly cancellation?: WorkerCancellationOptions
  /** Hub-side cleanup when a session's worker is retired (per-session maps). */
  readonly onSessionReleased?: (sessionID: SessionID) => void
  /**
   * Per-session durable-write barrier (plan 2.6 / R4#16) forwarded to the
   * dispatcher: a completed run's slot is held until it resolves.
   */
  readonly writeBarrier?: DispatcherOptions["writeBarrier"]
  /**
   * Per-session durable-egress barrier (plan 2.6 / R4#16) forwarded to every
   * spawned worker's executor.
   */
  readonly egressBarrier?: (sessionID: SessionID) => Promise<void> | undefined
  /**
   * Worker-port backends, ready or lazily built. When
   * present the resolved init advertises `capabilities.sessionWorkerPorts` and the
   * hub serves the ports.
   */
  readonly ports?: WorkerPortBackendsSource
  /** Off-loop hub SQL fallback; see `SessionHubOptions.db`. */
  readonly db?: DbExecutor
  /**
   * Live-worker count the schema gate must observe. Defaults to the scheduler's
   * derived state (a held spawn permit means a worker is live); tests may inject
   * a probe to exercise the refusal path.
   */
  readonly liveWorkers?: () => number
  /**
   * SQLite file the hub owns schema-ensure for. When present the hub applies the
   * shipped migrations (with zero live workers) before it can spawn anything.
   */
  readonly databasePath?: string
  /** Advisory cache shared with the port backends (never authoritative). */
  readonly cache?: HubTruthCache
  /** Persist/surface the typed D8 payload (see `SessionHubOptions.onWorkerCrashed`). */
  readonly onWorkerCrashed?: (payload: WorkerCrashed) => void | Promise<void>
  /** Invoked synchronously at the start of each dispatch (see `SessionHubOptions`). */
  readonly onDispatch?: (sessionID: SessionID) => void
  /** Resolve a session's subagent depth for the dispatch path (see `SessionHubOptions`). */
  readonly depthOf?: (sessionID: SessionID) => number | undefined
  /** Live-only event publisher (worker republish + `session.cancelled`). */
  readonly events?: LiveEventPublisher
  /** Per-session live publisher, so a cancel carries the cancelled session's location. */
  readonly eventPublisherFor?: (sessionID: SessionID) => LiveEventPublisher | undefined
  /** Abort in-flight plugin tools at the start of a cancel (see `SessionHubOptions`). */
  readonly onCancelRequested?: (sessionID: SessionID) => void
  /**
   * Fired when a worker rebinds away from a session; lets the production layer
   * drop that session's per-session instance/context.
   */
  readonly onSessionRebound?: (sessionID: SessionID) => void
  /** Test seam: build the pool from the lazy init resolver. Defaults to the production pool. */
  readonly poolFactory?: ProductionPoolFactory
  /**
   * Maximum number of session workers *running* at once. Extra sessions queue
   * in FIFO order (scheduling only, never rejection). Defaults to the CPU-core
   * count so a burst cannot oversubscribe the machine.
   */
  readonly maxRunningWorkers?: number
  /**
   * The one global spawn permit for this hub. Defaults to a fresh gate whose
   * acquisition timeout is {@link spawnTimeoutMs}. Tests may inject a gate to
   * observe cold-spawn serialization.
   */
  readonly spawnGate?: SpawnGateHandle
  /** Acquisition deadline for the default spawn gate; defaults to 30s. */
  readonly spawnTimeoutMs?: number
  /** Boot-ready deadline handed to every spawned worker; defaults to `timeoutMs`. */
  readonly bootTimeoutMs?: number
  /** Optional structured logger seam; absent defaults to a no-op (flag-off behavior). */
  readonly log?: WorkerLogger
  /** Boot-cancellation telemetry: a cancel win or a death during abort. */
  readonly onBootTelemetry?: (event: BootTelemetry) => void
}

/** Reaping is best-effort teardown: a failed kill must not block cancellation. */
function ignoreReapFailure(reap: () => void): void {
  try {
    reap()
  } catch {
    // The isolate is already being torn down; a stale mirror is the only loss.
  }
}

/** Whether `OPENCODE_SESSION_WORKER_COUNT` is set to a usable (non-empty) value. */
function isWorkerCountOverride(env: Record<string, string | undefined> | undefined): boolean {
  const raw = (env ?? process.env)[WORKER_COUNT_ENV]
  return raw !== undefined && raw.trim() !== ""
}

/**
 * Resolve the ambient instance a session's worker boots under: this session's
 * captured instance, else its persisted directory, else the resolving fiber's
 * ambient instance, else the documented global fallback (warned, because it is a
 * hub-side gap).
 */
function resolveSessionInstance(
  sessionID: SessionID | undefined,
  dispatched: DispatchedSession | undefined,
  instanceOfSession: (id: SessionID | undefined) => InstanceContext | undefined,
  log: WorkerLogger,
): InstanceContext {
  const resolved =
    instanceOfSession(sessionID) ??
    (dispatched === undefined ? undefined : instanceFromSession(dispatched)) ??
    captureInstanceRef()
  if (resolved !== undefined) return resolved
  const fallback = fallbackInstanceContext()
  log(workerLogMessage("instance fallback"), {
    level: "WARN",
    sessionID,
    directory: fallback.directory,
  })
  return fallback
}

/**
 * Compose the production pool + hub, with cooperative cancellation wired.
 */
export function createProductionSessionHub(options: ProductionSessionHubOptions): SessionHub {
  // Name the hub's main OS thread for human observability (Linux-only, fail-soft).
  // Role-gating keeps a session-worker isolate from ever building a nested hub (its
  // graph contributes no hub node — see `sessionWorkerNodes`), so this only names
  // the real hub process; no worker's `ocs<slot>` name can be clobbered.
  setThreadName("och")
  const log = options.log ?? noopWorkerLogger
  // One permit serializes every cold isolate spawn for this hub (and every
  // crash respawn, because the pool retains the same handle).
  const spawnGate =
    options.spawnGate ?? createSpawnGate({ timeoutMs: options.spawnTimeoutMs ?? DEFAULT_SPAWN_GATE_TIMEOUT_MS })
  // Schema-ensure runs before the pool exists and therefore before any spawn.
  // Its live count is the process-wide registry of constructed isolates, plus a
  // spawn that has acquired the one permit but not constructed its isolate yet.
  // A booted (permit-released) worker is exactly the case `spawnGate.held` alone
  // missed. `BEGIN IMMEDIATE` serializes concurrent hubs.
  if (options.databasePath !== undefined) {
    ensureProductionSchema({
      filename: options.databasePath,
      liveWorkers:
        options.liveWorkers?.() ?? liveSessionWorkerCount(options.databasePath) + (spawnGate.held === true ? 1 : 0),
    })
  }
  // No worker is spawned until a session dispatches; `workerCount` is diagnostic
  // (log line + worker snapshot), never a pool prewarm.
  const workerCount = options.count ?? 0
  // Resolve the run-reply liveness budget once, at the hub boundary: an explicit
  // option wins, else the env override, else the default-on watchdog budget.
  const runIdleTimeoutMs = resolveRunIdleTimeoutMs(options)
  logRunIdleTimeoutWarning(log, options)
  // Resolve the entry the pool will actually spawn (an explicit entry wins, then
  // the env override, then the build-time define, then the source-relative URL).
  const describedEntry: DescribedSessionWorkerEntry =
    options.entry !== undefined
      ? { entry: options.entry, source: "definedPath" }
      : describeSessionWorkerEntry({ env: options.env })
  log(workerLogMessage("pool resolved"), {
    level: "INFO",
    workerCount,
    override: isWorkerCountOverride(options.env),
    availableParallelism: os.availableParallelism(),
  })
  log(workerLogMessage("worker entry resolved"), {
    level: "INFO",
    entry: String(describedEntry.entry),
    source: describedEntry.source,
  })
  // The hub is assigned after the pool is built; port transfers arrive only on
  // dispatch (post-assignment), so the deferred reference is safe.
  // One lifecycle tracker owned by this hub: the pool reports boot/materialize,
  // workers report stream/tool/commit, and the dispatcher's crash path reads it.
  const lifecycle = createLifecycleTracker()
  let hub: SessionHub | undefined
  const sinks: WorkerPoolSinks = {
    onWorkerPorts: (workerID, ports) => hub?.attachWorkerPorts(workerID, ports),
    onHubProxyPort: (workerID, port) => hub?.attachHubProxyPort(workerID, port),
  }
  const resolveSource: (sessionID?: SessionID) => SessionWorkerInit | Promise<SessionWorkerInit> =
    typeof options.init === "function" ? options.init : () => options.init as SessionWorkerInit
  const resolveInit = async (sessionID?: SessionID): Promise<SessionWorkerInit> => {
    const init = await resolveSource(sessionID)
    return options.ports === undefined ? init : withSessionWorkerPorts(init)
  }
  // Persist every typed crash on the hub's off-loop DB thread unless the caller
  // supplied its own sink. The payload returned to the caller is the same value.
  const crashSink =
    options.onWorkerCrashed ??
    (options.db === undefined
      ? undefined
      : (payload: WorkerCrashed) => recordAsync(options.db!, payload).then(() => undefined))
  // One resolved cap for both pool paths: the production pool owns it internally,
  // a test/custom factory receives it so `maxRunningWorkers` is never silently
  // ignored when the factory builds the pool.
  const maxRunning = options.maxRunningWorkers ?? os.availableParallelism()
  const pool =
    options.poolFactory !== undefined
      ? options.poolFactory(resolveInit, sinks, { maxRunning })
      : createProductionWorkerPool({
          init: resolveInit,
          log,
          onPhase: (sessionID, phase) => lifecycle.phase(sessionID, phase),
          ...defined({
            entry: describedEntry.entry,
            databasePath: options.databasePath,
            timeoutMs: options.timeoutMs,
            runIdleTimeoutMs,
            workKind: options.workKind,
            env: options.env,
            egressBarrier: options.egressBarrier,
            onSessionRebound: options.onSessionRebound,
            // The pool hook stays internal (init-cache cleanup); the dispatcher
            // owns the caller's `onSessionReleased`, so wiring it here too would
            // fire it twice per retirement.
            spawnGate,
            bootTimeoutMs: options.bootTimeoutMs,
            onBootTelemetry: options.onBootTelemetry,
            maxRunning,
          }),
          ...sinks,
        })
  // The hub-side mirror of every worker child's process group (plan 11.5): a
  // terminate reaps the worker's mirrored PGIDs and shutdown reaps the rest.
  // Filled by the worker's `{type:"pid-report"}` control-port frames below; a
  // report for an already-mirrored group only advances its generation.
  const pids = createPidRegistry()
  const support = reapSupport()
  log(workerLogMessage("reap support"), { level: "INFO", supported: support.supported, reason: support.reason })
  const onPidReport = (workerID: number, report: PidReport): void => {
    log(workerLogMessage("pid report"), { level: "INFO", workerID, entries: report.entries.length })
    try {
      // `workerID` is the hub's slot association (authoritative); the registry
      // refuses a PGID reassignment to a different owner.
      registerReported(pids, report, workerID)
    } catch {
      // A bad/conflicting entry must not take the control port down.
    }
  }
  hub = createSessionHub({
    pool,
    log,
    lifecycle,
    ...defined({
      ports: options.ports,
      db: options.db,
      cache: options.cache,
      onWorkerCrashed: crashSink,
      onDispatch: options.onDispatch,
      depthOf: options.depthOf,
      events: options.events,
      eventPublisherFor: options.eventPublisherFor,
      onCancelRequested: options.onCancelRequested,
      writeBarrier: options.writeBarrier,
      onSessionReleased: options.onSessionReleased,
      onPidReport,
    }),
    cancellation: (pool) =>
      createWorkerCancellation(pool, {
        ...options.cancellation,
        reap: options.cancellation?.reap ?? ((worker) => ignoreReapFailure(() => void pids.reap(worker.id))),
        reapAll: options.cancellation?.reapAll ?? (() => ignoreReapFailure(() => void pids.reapAll())),
      }),
  })
  return hub
}

/**
 * Options for the production hub node. The `workerData` init is built INSIDE the
 * layer from the hub-authoritative services (Config/Auth/ModelsDev/Provider/
 * RuntimeFlags + the ambient `InstanceRef` directory) — it cannot be prebuilt at
 * module-load graph-build time, because those services are not available then.
 */
export interface ProductionSessionHubNodeOptions {
  readonly count?: number
  readonly entry?: string | URL
  readonly timeoutMs?: number
  /** Run-reply liveness budget; defaults to `OPENCODE_SESSION_WORKER_IDLE_TIMEOUT_MS`/`DEFAULT_RUN_IDLE_TIMEOUT_MS`. */
  readonly runIdleTimeoutMs?: number
  readonly workKind?: string
  readonly env?: Record<string, string | undefined>
  readonly cancellation?: WorkerCancellationOptions
  /**
   * Worker-port backends, ready or lazily built. Defaults to
   * `() => buildProductionPortBackends(backends)` — the real hub egress
   * authorities, constructed on the first dispatch — so the production
   * node advertises `capabilities.sessionWorkerPorts` without spawning anything at
   * graph-build time.
   */
  readonly ports?: WorkerPortBackendsSource
  /** Off-loop hub SQL fallback; defaults to a lazy executor over `databasePath`. */
  readonly db?: DbExecutor
  /** SQLite file the default hub executor opens; defaults to the app database. */
  readonly databasePath?: string
  /** Advisory cache shared by the hub, event sink, and port backends. */
  readonly cache?: HubTruthCache
  /** Persist/surface the typed D8 crash payload. */
  readonly onWorkerCrashed?: (payload: WorkerCrashed) => void | Promise<void>
  /** Tuning for the default production port backends (env). */
  readonly backends?: ProductionPortBackendsOptions
  /** Test seam: build the pool from the resolved init. Defaults to the lazy production pool. */
  readonly poolFactory?: ProductionPoolFactory
  /** Forwarded to `buildSessionWorkerInit` (snapshot/env/model tuning). */
  readonly initOptions?: BuildSessionWorkerInitOptions
  /**
   * Resolve the hub plugin hooks the worker should run (`chat.params` /
   * `chat.headers`). Defaults to reading `Plugin.Service` from the dispatch
   * context; the returned sources' hook functions never cross the isolate — the
   * hub resolves their effect and ships the resolved data.
   */
  readonly plugins?: () => Promise<readonly WorkerPluginSource[]>
  /**
   * Fired when a worker rebinds away from a session. The layer composes its own
   * per-session cleanup with this hook.
   */
  readonly onSessionRebound?: (sessionID: SessionID) => void
  /** Hub-side cleanup when a session's worker is retired (per-session maps). */
  readonly onSessionReleased?: (sessionID: SessionID) => void
}

/** The hub-authoritative services a per-session worker init is built from. */
interface ProductionInitServices {
  readonly config: Config.Interface
  readonly auth: Auth.Interface
  readonly modelsDev: ModelsDev.Interface
  readonly provider: Provider.Interface
  readonly flags: RuntimeFlags.Info
}

/** Inputs a {@link createPerSessionResolvers} factory closes over. */
interface PerSessionResolverDeps {
  readonly services: ProductionInitServices
  readonly bridge: EventV2.Interface
  readonly authorities: HubProxyAuthorities
  /** Resolved pool size, shipped on every generation's snapshot. */
  readonly count: number
  /** The hub-owned epoch sequence, so each generation's epoch strictly increases. */
  readonly epochSequence: EpochSequence
  readonly initOptions: BuildSessionWorkerInitOptions | undefined
  readonly plugins: (() => Promise<readonly WorkerPluginSource[]>) | undefined
  /**
   * Whether every worker gets a hub-hook control port. When true the worker
   * runs `chat.params` / `chat.headers` per request through the bridge, so the
   * init resolver must not invoke them a second time to build a boot diff.
   */
  readonly requestHooksViaHub: boolean
  readonly log: WorkerLogger
  /** The hub-resolved `subagent_depth` its `session.spawn` authority enforces. */
  readonly subagentDepth: number
  /** The session a worker is currently bound to, if known. */
  readonly sessionOfWorker: (workerID: number) => SessionID | undefined
}

/**
 * The per-session resolvers the production hub is composed from: the session →
 * instance/context registry, the live publishers stamped with the session's own
 * location, and the per-worker hub-proxy handlers and init resolver. One worker
 * serves one session at a time, so every resolver is keyed by that session
 * rather than latching the first
 * dispatched one.
 */
interface PerSessionResolvers {
  /** The shared live publisher (used when a caller reaches `createSessionHub` directly). */
  readonly events: LiveEventPublisher
  readonly eventsForSession: (sessionID: SessionID) => LiveEventPublisher
  readonly eventsForWorker: (workerID: number) => LiveEventPublisher | undefined
  readonly hubProxyFor: (workerID: number) => HubProxyHandlers
  /** The streaming half of a worker's hub-proxy map (`pluginTool.metadata`). */
  readonly hubProxyStreamingFor: (workerID: number) => Readonly<Record<string, HubProxyStreamHandler>>
  /** Abort every in-flight plugin tool of a session (the hub cancel cascade). */
  readonly abortPluginTools: (sessionID: SessionID) => void
  /**
   * Cancel every pending hub-side permission/question request of one session.
   * A blocked worker ask is then failed through its proxy reply and its pending
   * entry removed, instead of being abandoned when the worker is cancelled or
   * retired.
   */
  readonly cancelPendingRequests: (sessionID: SessionID) => void
  readonly instanceOfSession: (sessionID: SessionID | undefined) => InstanceContext | undefined
  readonly resolveInit: (sessionID?: SessionID) => Promise<SessionWorkerInit>
  readonly recordDispatch: (sessionID: SessionID) => void
  readonly releaseSession: (sessionID: SessionID) => void
  readonly clearSessions: () => void
  /**
   * The synchronous parent → children edge the dispatcher's crash/timeout
   * cascade reads. It is intentionally not a DB query: `cancelWithGrace` runs
   * on the crash path and must never block on SQLite.
   */
  readonly childrenOf: (sessionID: SessionID) => ReadonlyArray<SessionID>
  /** The session's subagent depth recorded from `metadata.depth` at spawn/boot. */
  readonly depthOf: (sessionID: SessionID) => number | undefined
}

/**
 * The production parent → children index. It mirrors the `parentID` edges the
 * hub observes when it spawns or dispatches a child, so the dispatcher can
 * recursively cancel descendants on a parent crash/timeout without a blocking
 * `Session.children` query.
 *
 * Registration happens at the two hub-observable boundaries:
 * - `sessionSpawnFor` registers the result of every hub-authoritative spawn
 *   (`task` and any other `Session.spawn` caller behind the proxy).
 * - `resolveInit` registers the persisted `parentID` of every dispatched child,
 *   which also covers children created outside the spawn authority (a raw
 *   `delegate`-shaped create + prompt) once their worker is materialised.
 *
 * A raw delegate still runs inline on its creator's worker until it is
 * dispatched on its own, so there is no separate worker to orphan before the
 * dispatch registration lands. `childrenByParent` is bounded by live workers:
 * `release` drops a session from its parent when that worker is reaped,
 * cancelled, or crashes; its own child edges remain until each descendant is
 * released, so a re-dispatched parent still cascades to live descendants.
 */
export interface SessionChildrenIndex {
  readonly childrenOf: (parentID: SessionID) => ReadonlyArray<SessionID>
  readonly register: (parentID: SessionID, childID: SessionID) => void
  readonly release: (sessionID: SessionID) => void
  readonly clear: () => void
}

export function createSessionChildrenIndex(): SessionChildrenIndex {
  const childrenByParent = new Map<SessionID, Set<SessionID>>()
  const parentByChild = new Map<SessionID, SessionID>()

  const childrenOf = (parentID: SessionID): ReadonlyArray<SessionID> => [...(childrenByParent.get(parentID) ?? [])]

  const register = (parentID: SessionID, childID: SessionID): void => {
    const previousParent = parentByChild.get(childID)
    if (previousParent !== undefined && previousParent !== parentID) {
      const previousSiblings = childrenByParent.get(previousParent)
      previousSiblings?.delete(childID)
      if (previousSiblings?.size === 0) childrenByParent.delete(previousParent)
    }
    parentByChild.set(childID, parentID)
    let siblings = childrenByParent.get(parentID)
    if (siblings === undefined) {
      siblings = new Set()
      childrenByParent.set(parentID, siblings)
    }
    siblings.add(childID)
  }

  const release = (sessionID: SessionID): void => {
    const parentID = parentByChild.get(sessionID)
    if (parentID === undefined) return
    parentByChild.delete(sessionID)
    const siblings = childrenByParent.get(parentID)
    siblings?.delete(sessionID)
    if (siblings?.size === 0) childrenByParent.delete(parentID)
  }

  const clear = (): void => {
    childrenByParent.clear()
    parentByChild.clear()
  }

  return { childrenOf, register, release, clear }
}

/**
 * Caller metadata is data, never classification. Strip a `depth` a worker may
 * have forged so the only depth on the spawned/created child is the one the hub
 * derives from the persisted parent.
 */
function withoutSpawnDepth(metadata: Record<string, unknown> | undefined): Record<string, unknown> | undefined {
  if (metadata === undefined) return undefined
  const { depth: _depth, ...rest } = metadata
  return rest
}

/**
 * The hub's authoritative child spawn. The hub supplies its own resolved depth
 * limit, so `Session.spawn` rejects an over-depth child before creating one even
 * when the worker forwarded no limit.
 */
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

/**
 * Resolve the worker's authoritative bound session. Only the hub registration
 * (the worker → session binding) can establish authority: an unregistered worker
 * cannot claim a session by naming it on the request. A request that names a
 * different session than the registration is a hard rejection too.
 */
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

/**
 * Whether `target` is a DIRECT child of `bound`. A bound worker owns exactly one
 * session, so it may only name its own direct children when it asks the hub to
 * read or cancel a child. Self, grandchild, ancestor, sibling and foreign ids all
 * fail this check.
 */
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
    // Caller metadata is never read for classification, so a worker cannot
    // create a depth-0 row (and claim foreground headroom) under a deep parent.
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

/**
 * Cancel the bound session (its own run, cascading to its descendants on the
 * hub) or one of its direct children. A grandchild, ancestor, sibling or foreign
 * id is rejected: the child worker owns its own subtree.
 */
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

/**
 * The dependencies a session-scope hub authority runs under. The runner carries
 * the dispatching worker's captured context, so `Session`/`SessionPrompt` are
 * resolved there rather than from the hub's ambient graph.
 */
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
        const prompt = yield* Effect.serviceOption(SessionPromptService)
        if (Option.isNone(prompt)) return yield* Effect.die(missingPromptService())
        return yield* cancelSessionOnHub(sessions.value, boundSessionID, request, (sessionID) =>
          prompt.value.cancel(sessionID),
        )
      }),
    )
}

/**
 * The hub's authoritative child spawn. A worker must be bound, may only spawn
 * under that bound session, and a supplied `childID` must already be a direct
 * child of it. Depth comes from the persisted parent (`nextDepth`), never from
 * the request metadata a worker controls.
 */
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
        // The child's depth is a property of the persisted parent, not of the
        // worker-supplied metadata (which a caller could forge).
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
        // Record the edge synchronously, before the child's own worker can
        // start. A parent crash between here and the child dispatch then still
        // cascades (the child is either queued or admitted, both cancellable).
        deps.registerChild(boundSessionID, spawned.childID)
        return spawned
      }),
    )
}

/**
 * Bind every `sessionStatus` operation to the worker's own session. A foreign
 * read never reaches the hub service (it resolves idle), a foreign write is
 * ignored, and the list exposes only the bound entry.
 */
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

/**
 * Bind a per-session cancel to the worker's own session. The hub release path
 * cancels only the session whose authority map it builds; a worker asking for
 * another session must fail, not silently cancel (or leave) that session's
 * pending requests.
 */
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
  readonly backgroundJob?: NonNullable<HubProxyAuthorities["backgroundJob"]>
}

/**
 * The production authority map for one worker. Every worker-controlled id is
 * bound to the session the worker is registered to: status writes, permission
 * and question replies/lists, and every session-scope RPC.
 */
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
    ...(deps.backgroundJob === undefined ? {} : { backgroundJob: deps.backgroundJob }),
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

/**
 * The hub-half of worker-job promotion. Each worker reports its running jobs
 * here; the mirror lives in the hub's own `BackgroundJob` registry so the
 * existing `list`/`promote` endpoint sees foreground jobs too. Promotion
 * resolves the worker's long-poll, which is the only way to run the worker-local
 * `onPromote` (and therefore the task notification) across the isolate.
 *
 * The mirror is keyed by job id and pruned when the worker reports settlement or
 * its session is released, so a dead worker cannot leave a permanently-running
 * ghost entry.
 */
export interface WorkerJobMirror {
  authorityFor(run: HubProxyRunner): WorkerBackgroundJobAuthority
  releaseSession(sessionID: string): void
}

export function createWorkerJobMirror(): WorkerJobMirror {
  const jobs = new Map<string, MirroredWorkerJob>()

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
    authorityFor: (run) => ({
      register: async (info) => {
        if (info.status !== "running") return
        const existing = jobs.get(info.id)
        if (existing !== undefined) {
          // A resumed foreground job is promoted worker-side; propagate that
          // state so the hub mirror stops reporting it as foreground. Promoting
          // an already-background mirror is an observation, not a new promotion.
          if (info.metadata?.background === true && !existing.promoted) {
            await run(existing.registry.promote(info.id))
          }
          return
        }
        const registry = await run(Effect.serviceOption(BackgroundJob.Service))
        if (Option.isNone(registry)) return
        const job: MirroredWorkerJob = {
          sessionID: info.parentID ?? info.id,
          registry: registry.value,
          run,
          done: Effect.runSync(Deferred.make<{ readonly output?: string; readonly error?: string }>()),
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
              ...(info.parentID === undefined ? {} : { parentID: info.parentID }),
              onPromote: Effect.sync(() => {
                job.promoted = true
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
      },
      settle: async (input) => {
        const job = jobs.get(input.id)
        if (job === undefined) return
        await settleMirror(job, input)
      },
      awaitPromotion: (id) => {
        const job = jobs.get(id)
        if (job === undefined || job.settled) return Promise.resolve(false)
        if (job.promoted) return Promise.resolve(true)
        return new Promise((resolve) => job.promotionWaiters.add(resolve))
      },
    }),
    releaseSession: (sessionID) => {
      for (const [id, job] of jobs) {
        if (job.sessionID !== sessionID) continue
        void settleMirror(job, { id, status: "cancelled" })
      }
    },
  }
}

function createPerSessionResolvers(deps: PerSessionResolverDeps): PerSessionResolvers {
  const { services, bridge, authorities, log, subagentDepth } = deps
  // The synchronous parent → children mirror the dispatcher's crash/timeout
  // cascade reads. Registered at the two hub-observable boundaries below and
  // pruned when a session's worker is released.
  const children = createSessionChildrenIndex()
  // The synchronous session → subagent-depth mirror the dispatch path reads
  // BEFORE a worker materializes. `Session.spawn` stamps `metadata.depth` on the
  // child and forwards it over the spawn proxy, so the hub records it here before
  // the child prompt dispatches; a foreground root is 0. Kept after a worker is
  // released so a re-dispatched child keeps its headroom level.
  const sessionDepths = new Map<SessionID, number>()
  const depthOf = (sessionID: SessionID): number | undefined => sessionDepths.get(sessionID)
  const recordDepth = (sessionID: SessionID, depth: number): void => {
    // Roots need no entry (absence resolves to 0); only nested children keep
    // headroom state, so the map stays bounded by the spawned subagent tree.
    if (depth > 0) sessionDepths.set(sessionID, depth)
    else sessionDepths.delete(sessionID)
  }
  // Per-session state, written on EVERY dispatch (never latched with `??=`).
  // A session's own ambient `InstanceRef` and full runtime context are stored
  // under its id, so a second project's worker is never built — and its
  // hub-proxy calls never run — under the first project's directory/instance.
  const instanceBySession = new Map<SessionID, InstanceContext>()
  const contextBySession = new Map<SessionID, Context.Context<never>>()
  // Worker-owned jobs are mirrored into the hub registry so the promotion
  // endpoint sees foreground jobs; release prunes a dead worker's ghosts.
  const jobMirrors = createWorkerJobMirror()
  // Release one session's per-session state; the finalizer clears the rest.
  // Called on every rebind and at teardown, so the maps stay bounded by the set
  // of live sessions. A session's own entry is dropped once its worker rebinds
  // away (the isolate is retired) or the hub tears down; there is no race-free
  // job-settle hook to release it earlier (the dispatcher surfaces no settle
  // callback here), and releasing on settle would race a later turn that
  // re-resolves its init.
  const releaseSession = (sessionID: SessionID): void => {
    instanceBySession.delete(sessionID)
    contextBySession.delete(sessionID)
    children.release(sessionID)
    jobMirrors.releaseSession(sessionID)
  }
  const clearSessions = (): void => {
    instanceBySession.clear()
    contextBySession.clear()
    children.clear()
    sessionDepths.clear()
  }
  // The session currently being dispatched. Updated on every dispatch; used only
  // as the fallback when a pool resolves an init without a session id.
  let dispatchedSession: SessionID | undefined
  // Every dispatch records the session's own ambient instance/context BEFORE the
  // pool materializes its worker (`onDispatch` runs synchronously at the top of
  // `ensureRunning`/`startShell`). A reaped/released entry is therefore
  // re-recorded from the next dispatch, and `resolveInit` also reads the session's
  // PERSISTED directory/project from that context — so a delegate-shaped child
  // (`parentID` + raw `session.prompt`) and an auto-continuation child boot under
  // their own persisted project even after their earlier entry was released.
  // There is deliberately no ancestor-chain fallback: the old job-registry parent
  // lookup was dead after the registry deletion, and a missing entry here means
  // a dispatch we never observed (a hub-side gap), not a child to inherit from.
  const instanceOfSession = (sessionID: SessionID | undefined): InstanceContext | undefined =>
    sessionID === undefined ? undefined : instanceBySession.get(sessionID)
  const contextOfSession = (sessionID: SessionID | undefined): Context.Context<never> | undefined =>
    sessionID === undefined ? undefined : contextBySession.get(sessionID)
  const contextOfWorker = (workerID: number): Context.Context<never> | undefined =>
    contextOfSession(deps.sessionOfWorker(workerID))
  // Live-only publisher, stamped with the emitting session's own location so a
  // republished worker event passes THAT session's `/event` SSE directory filter
  // — never a first-dispatch directory.
  const eventsForSession = (sessionID: SessionID): LiveEventPublisher =>
    createLiveEventPublisher(bridge, { location: () => instanceLocation(instanceOfSession(sessionID)) })
  const eventsForWorker = (workerID: number): LiveEventPublisher | undefined => {
    const sessionID = deps.sessionOfWorker(workerID)
    return sessionID === undefined ? undefined : eventsForSession(sessionID)
  }
  // Shared fallback publisher (a caller may reach `createSessionHub` directly).
  const events = createLiveEventPublisher(bridge)
  // A hub authority effect is closed over its own services but reads the ambient
  // `InstanceRef`; run it under the worker's session context so it resolves.
  const runHubProxyFor =
    (context: Context.Context<never> | undefined): HubProxyRunner =>
    (effect) =>
      Effect.runPromise(context === undefined ? effect : Effect.provide(effect, context))
  // The worker cannot run `experimental.chat.system.transform` itself: the hub
  // owns plugin loading (decision 70). Read `Plugin` from the worker's session
  // context and run the trigger under it, so the per-directory `InstanceState`
  // the plugin layer keys on is the emitting instance's.
  const systemTransformFor =
    (context: Context.Context<never> | undefined): NonNullable<HubProxyAuthorities["systemTransform"]> =>
    (input, output) => {
      const plugin = context === undefined ? Option.none() : Context.getOption(context, Plugin.Service)
      return Option.isNone(plugin)
        ? Promise.resolve(output)
        : runHubProxyFor(context)(plugin.value.trigger("experimental.chat.system.transform", input, output))
    }
  // The worker cannot run `experimental.chat.messages.transform` itself: the hub
  // owns plugin loading (decision 70). Read `Plugin` from the worker's session
  // context and run the trigger under it, so the per-directory `InstanceState`
  // the plugin layer keys on is the emitting instance's.
  const messagesTransformFor = (
    context: Context.Context<never> | undefined,
  ): NonNullable<HubProxyAuthorities["messagesTransform"]> =>
    messagesTransformAuthority({
      plugin: () => (context === undefined ? Option.none() : Context.getOption(context, Plugin.Service)),
      run: runHubProxyFor(context),
    })
  // The worker-facing bridge for every routed-but-hub-owned hook. The plugin
  // instance lives on the hub, so the worker only ships a clone-safe payload.
  const pluginTriggerFor = (
    context: Context.Context<never> | undefined,
  ): NonNullable<HubProxyAuthorities["pluginTrigger"]> =>
    pluginTriggerAuthority({
      plugin: () => (context === undefined ? Option.none() : Context.getOption(context, Plugin.Service)),
      run: runHubProxyFor(context),
    })
  // The per-hub in-flight plugin-tool abort registry. Owned here because the
  // plugin-tool authority (below) is the only registrar, and the hub cancel path
  // reaches it through the resolver's `abortPluginTools`.
  const pluginToolAborts = createPluginToolAbortRegistry()
  const pluginToolFor = (context: Context.Context<never> | undefined): PluginToolAuthority =>
    pluginToolAuthority({
      plugin: () => (context === undefined ? Option.none() : Context.getOption(context, Plugin.Service)),
      run: runHubProxyFor(context),
      registerAbort: (sessionID, callID) => pluginToolAborts.registerAbort(sessionID, callID),
    })
  // Build the authority map per worker under THAT worker's session context. A
  // worker is bound to exactly one session, so its control port reaches the
  // right `SessionStatus`/`Permission`/`Question`/`systemTransform`/
  // `messagesTransform`/`pluginTool` instance.
  const authoritiesFor = (
    context: Context.Context<never> | undefined,
    boundSessionID: SessionID | undefined,
  ): HubProxyAuthorities =>
    hubProxyAuthoritiesFor({
      run: runHubProxyFor(context),
      boundSessionID,
      subagentDepth,
      recordDepth,
      registerChild: children.register,
      sessionStatus: authorities.sessionStatus,
      permission: authorities.permission,
      question: authorities.question,
      systemTransform: systemTransformFor(context),
      messagesTransform: messagesTransformFor(context),
      pluginTrigger: pluginTriggerFor(context),
      // The worker forwards plugin-tool executes here; the definition (and its
      // plugin instance) stays on the hub. `registerAbort` keys each in-flight call
      // so the hub cancel path / `pluginTool.abort` can unwind it.
      pluginTool: pluginToolFor(context),
      // The lazy descriptor refresh: descriptors shipped at init go stale when a
      // plugin reloads, so the worker re-fetches them here.
      pluginToolList: () => resolveContextHubToolDescriptors(context),
      // Best-effort per-call abort the worker may request directly.
      pluginToolAbort: (input) => pluginToolAborts.abort(input.sessionID, input.callID),
      // Worker jobs have no hub process to live in: the mirror makes them visible
      // to the promotion endpoint, and the long-poll below reaches the worker.
      backgroundJob: jobMirrors.authorityFor(runHubProxyFor(context)),
    })
  const hubProxyFor = (workerID: number): HubProxyHandlers => {
    const context = contextOfWorker(workerID)
    return buildHubProxyHandlers(authoritiesFor(context, deps.sessionOfWorker(workerID)), runHubProxyFor(context))
  }
  // The streaming half of the map: `pluginTool.execute` pushes mid-execute
  // metadata to the awaiting worker. Served alongside the plain map.
  const hubProxyStreamingFor = (workerID: number): Readonly<Record<string, HubProxyStreamHandler>> => {
    const context = contextOfWorker(workerID)
    return buildHubProxyStreamHandlers(authoritiesFor(context, deps.sessionOfWorker(workerID)))
  }
  // Per-session: the pool passes the session it is building a worker for, so
  // this session's instance/model (and plugin effect) is the one shipped, never
  // the first dispatched session's. `dispatchedSession` is the fallback for a
  // custom pool that resolves without a session id.
  const resolveInit = async (sessionID?: SessionID): Promise<SessionWorkerInit> => {
    const session = sessionID ?? dispatchedSession
    // The dispatch path recorded this session's own context before the worker
    // materialized, so `loadDispatchedSession` can read its PERSISTED
    // directory/project even when the dispatching fiber had no ambient instance.
    const context = contextOfSession(session)
    // One `Session.get` yields the persisted model, directory/project and the
    // per-request plugin input (agent + last user message) the hooks resolve
    // against, so a hook keyed on either matches flag-off rather than an empty stub.
    const dispatched = await loadDispatchedSession(context, session)
    // A child created outside the spawn authority (a raw `delegate`-shaped
    // create + prompt) is still persisted with its parent. Register that edge
    // when its worker boots, so a later parent crash can cascade to it.
    if (session !== undefined && dispatched?.parentID !== undefined) {
      children.register(dispatched.parentID, session)
    }
    // Mirror the persisted depth so a re-dispatched child keeps its cap level
    // even when the in-memory spawn record is gone (restart/custom pool).
    if (session !== undefined && dispatched?.depth !== undefined) recordDepth(session, dispatched.depth)
    const instance = resolveSessionInstance(session, dispatched, instanceOfSession, log)
    const model = dispatched?.model
    const request = dispatched?.request
    const pluginSources = deps.plugins !== undefined ? await deps.plugins() : await resolveContextPluginSources(context)
    // The clone-safe projection of every plugin tool the worker may call. The
    // `execute` stays on the hub (this worker's control port reaches the
    // `pluginTool.execute` authority above); only the descriptor crosses.
    const pluginTools = await resolveContextHubToolDescriptors(context)
    const initOptions: BuildSessionWorkerInitOptions = {
      ...deps.initOptions,
      // The session's persisted model is authoritative; config/default only
      // applies when the dispatch carried no session model.
      model: model === undefined ? deps.initOptions?.model : { ...deps.initOptions?.model, session: model },
      snapshot: { ...deps.initOptions?.snapshot, epochSequence: deps.epochSequence },
      plugins:
        pluginSources === undefined
          ? deps.initOptions?.plugins
          : (resolvedModel) =>
              resolveWorkerPlugins({
                model: resolvedModel,
                sources: pluginSources,
                sessionID: session,
                ...(request ?? {}),
                // A bridged worker runs these hooks once per request; resolving
                // them here too would double-invoke and freeze the boot model.
                deferRequestHooks: deps.requestHooksViaHub,
              }),
      pluginTools,
    }
    // A non-serializable provider option or plugin hook is a configuration
    // defect: die loud rather than widen every graph's error channel with the
    // typed failure.
    return Effect.runPromise(
      buildSessionWorkerInit(initOptions).pipe(
        Effect.provideService(Config.Service, services.config),
        Effect.provideService(Auth.Service, services.auth),
        Effect.provideService(ModelsDev.Service, services.modelsDev),
        Effect.provideService(Provider.Service, services.provider),
        Effect.provideService(RuntimeFlags.Service, services.flags),
        Effect.provideService(InstanceRef, instance),
        Effect.orDie,
      ),
    )
  }
  const recordDispatch = (sessionID: SessionID): void => {
    // Overwrite on every dispatch (never `??=`): each session keeps its own
    // ambient instance/context, so a later project never inherits the first.
    const instance = captureInstanceRef()
    if (instance !== undefined) instanceBySession.set(sessionID, instance)
    const context = captureRuntimeContext()
    if (context !== undefined) contextBySession.set(sessionID, context)
    dispatchedSession = sessionID
  }
  return {
    events,
    eventsForSession,
    eventsForWorker,
    hubProxyFor,
    hubProxyStreamingFor,
    abortPluginTools: (sessionID) => pluginToolAborts.abortSession(sessionID),
    cancelPendingRequests: (sessionID) => {
      // Capture the session's own context before any asynchronous run so a
      // concurrent release cannot drop it out from under the authority effects.
      const context = contextOfSession(sessionID)
      const run = runHubProxyFor(context)
      const cancels: Array<Promise<void>> = []
      if (authorities.permission.cancelSession !== undefined) {
        cancels.push(run(authorities.permission.cancelSession(sessionID)))
      }
      if (authorities.question.cancelSession !== undefined) {
        cancels.push(run(authorities.question.cancelSession(sessionID)))
      }
      if (cancels.length === 0) return
      void Promise.all(cancels).catch((error: unknown) => {
        log(workerLogMessage("pending request cancel failed"), {
          level: "WARN",
          sessionID,
          error: error instanceof Error ? error.message : String(error),
        })
      })
    },
    instanceOfSession,
    resolveInit,
    recordDispatch,
    releaseSession,
    clearSessions,
    childrenOf: children.childrenOf,
    depthOf,
  }
}

/**
 * Provide `SessionHubService` from the production hub, terminating it on scope
 * close. The `workerData` init is resolved per dispatched session (never at graph
 * build): the hub-authoritative services are captured here, and the instance
 * directory comes from the dispatching session's ambient `InstanceRef` (the
 * `attach` pattern). Building the layer spawns no workers.
 */
export function productionSessionHubLayer(
  options: ProductionSessionHubNodeOptions = {},
): Layer.Layer<
  SessionHubService,
  never,
  | Config.Service
  | Auth.Service
  | ModelsDev.Service
  | Provider.Service
  | RuntimeFlags.Service
  | SessionStatus.Service
  | Permission.Service
  | Question.Service
  | EventV2Bridge.Service
> {
  return Layer.effect(
    SessionHubService,
    Effect.gen(function* () {
      const config = yield* Config.Service
      const auth = yield* Auth.Service
      const modelsDev = yield* ModelsDev.Service
      const provider = yield* Provider.Service
      const flags = yield* RuntimeFlags.Service
      // The resolved nesting limit the spawn authority enforces; the hub reads
      // the config once at layer build rather than trusting the worker. A
      // missing or throwing Config must not fail layer construction (the hub
      // falls back to the documented `1`), so the read is tolerant: `catchCause`
      // also absorbs the sync-throw / `UnimplementedError` defect a mock Config
      // raises, which `orElseSucceed` would not.
      const subagentDepth = yield* config.get().pipe(
        Effect.map((info) => info.subagent_depth ?? 1),
        Effect.catchCause(() => Effect.succeed(1)),
      )
      // The hub-proxy authorities a worker reaches over its
      // control port. The handlers run on the HUB, under the dispatching
      // instance's context (the hub runtime is global).
      const sessionStatus = yield* SessionStatus.Service
      const permission = yield* Permission.Service
      const question = yield* Question.Service
      // The opencode event boundary: worker events and `session.cancelled` are
      // republished live-only through it (never re-committed).
      const bridge = yield* EventV2Bridge.Service
      // v2 has no warm minimum; `count` is diagnostic only.
      const count = options.count ?? 0
      // The hub owns the snapshot epoch sequence for its lifetime, so each
      // generation's epoch is strictly greater than the last even when the wall
      // clock jumps backwards (an explicit holder, not hidden module state).
      const epochSequence = createEpochSequence()
      // The production narrative seam: an EffectBridge (R/E = never, so it adds no
      // graph dependency) whose `fork` carries the ambient logger, mirroring the
      // test-only `createSessionHubLayer`. Without this the production graph passed
      // no `log`, so A1–A3 / B2/B3 / D3 / E1/E3 and everything forwarded through
      // dispatch/execute/cancel were dormant. `bridge` names the EventV2Bridge above.
      const effectBridge = yield* EffectBridge.make()
      const log = workerLoggerFromBridge(effectBridge)
      // One advisory cache for the hub, its durable-event sink, and shutdown. It
      // starts cold (no persisted prompt state) and is never authoritative.
      const cache = options.cache ?? new StateCache<unknown>()
      // Construction spawns nothing; the executor's thread starts on the first
      // query. The same resolved path is handed to the hub so schema-ensure
      // migrates the exact file it opens.
      const databasePath = options.databasePath ?? Database.path()
      // The schema gate above has already created/migrated this file, so the
      // executor probes it with `create: false`: a missing or unmigrated
      // database is refused without leaving a stray file behind.
      const db = options.db ?? createDbExecutor({ filename: databasePath, create: false })
      // The hub is assigned after the pool is built; the resolver's per-worker
      // lookups run lazily on the first port attach (post-assignment), so the
      // deferred reference is safe.
      let hubRef: SessionHub | undefined
      // The per-session resolvers (the session → instance/context registry, the
      // live publishers, and the per-worker hub-proxy handlers and init) live in
      // one factory; the layer body only wires them to the hub.
      //
      // The hub ALWAYS resolves a port source below: the default production
      // backends serve the resolvers' own `hubProxyFor`, so a worker receives the
      // request-hook bridge even though `options.ports` is undefined here. Only a
      // caller-supplied object that exposes no hub-proxy handler can omit it.
      const requestHooksViaHub =
        options.ports === undefined ||
        typeof options.ports === "function" ||
        options.ports.hubProxy !== undefined ||
        options.ports.hubProxyFor !== undefined
      const resolvers = createPerSessionResolvers({
        services: { config, auth, modelsDev, provider, flags },
        bridge,
        authorities: { sessionStatus, permission, question },
        count,
        epochSequence,
        initOptions: options.initOptions,
        plugins: options.plugins,
        // A bridged worker runs `chat.params` / `chat.headers` once per request;
        // resolving them during init as well would double every invocation.
        requestHooksViaHub,
        log,
        subagentDepth,
        sessionOfWorker: (workerID) => hubRef?.sessionOfWorker(workerID),
      })
      // The production port authorities: supplied ready, or built on the first
      // dispatch (the source is only invoked when a worker attaches a port).
      const ports: WorkerPortBackendsSource =
        options.ports ??
        (() =>
          buildProductionPortBackends({
            ...options.backends,
            hubProxy: options.backends?.hubProxy,
            hubProxyFor: options.backends?.hubProxyFor ?? resolvers.hubProxyFor,
            hubProxyStreamingFor: options.backends?.hubProxyStreamingFor ?? resolvers.hubProxyStreamingFor,
            events: resolvers.events,
            eventsForWorker: resolvers.eventsForWorker,
            cache,
          }))
      const hub = createProductionSessionHub({
        ...options,
        count,
        databasePath,
        init: resolvers.resolveInit,
        ports,
        db,
        cache,
        events: resolvers.events,
        eventPublisherFor: resolvers.eventsForSession,
        log,
        // The dispatcher's crash/timeout cascade needs the synchronous index the
        // resolvers build as they observe spawns and dispatches. Without this,
        // production falls back to `() => []` and a parent crash leaves its
        // child subagent workers running.
        cancellation: {
          ...options.cancellation,
          childrenOf: options.cancellation?.childrenOf ?? resolvers.childrenOf,
        },
        onSessionRebound: (sessionID) => {
          // The rebinding worker's old session is done with its isolate; release
          // its state so the maps cannot grow without bound.
          resolvers.releaseSession(sessionID)
          options.onSessionRebound?.(sessionID)
        },
        onSessionReleased: (sessionID) => {
          // A retired generation can never answer a hub-side permission/question
          // request; fail and remove it before dropping the session's context.
          resolvers.cancelPendingRequests(sessionID)
          // Per-session workers never rebind: release state when the worker is
          // reaped/cancelled so the per-session maps stay bounded.
          resolvers.releaseSession(sessionID)
          options.onSessionReleased?.(sessionID)
        },
        onDispatch: resolvers.recordDispatch,
        // The production dispatch path must carry the child's `metadata.depth`;
        // without it `cap.acquire` always sees 0 and nested children deadlock
        // behind their parent's foreground slot.
        depthOf: resolvers.depthOf,
        onCancelRequested: (sessionID) => {
          // Abort hub-executed tools and fail blocked permission/question asks at
          // the moment of the cancel, not only after the grace window.
          resolvers.abortPluginTools(sessionID)
          resolvers.cancelPendingRequests(sessionID)
        },
      })
      hubRef = hub
      yield* Effect.addFinalizer(() =>
        Effect.promise(async () => {
          await hub.terminateAll().catch(() => undefined)
          // Release every per-session entry so a rebuild starts clean.
          resolvers.clearSessions()
        }),
      )
      return SessionHubService.of(hub)
    }),
  )
}

/**
 * A layer node providing the production `SessionHubService` (step 14). Its deps
 * are the services the init needs plus the three hub-proxy authorities the
 * worker's control port dispatches to; the init itself is resolved on the first
 * dispatch under the session's ambient `InstanceRef`. Flag-on app graphs include
 * this node; flag-off graphs never reference it.
 */
export function productionSessionHubNode(
  options: ProductionSessionHubNodeOptions = {},
): LayerNode.Node<SessionHubService, never, undefined> {
  return LayerNode.make({
    service: SessionHubService,
    layer: productionSessionHubLayer(options),
    deps: [
      Config.node,
      Auth.node,
      ModelsDev.node,
      Provider.node,
      RuntimeFlags.node,
      SessionStatus.node,
      Permission.node,
      Question.node,
      EventV2Bridge.node,
    ],
  })
}

// ---------------------------------------------------------------------------
// Pool-spawning layer node (decision line 58: reduced layer per role).
// ---------------------------------------------------------------------------

/** Provide `SessionHubService`, terminating the pool when the layer scope closes. */
export function createSessionHubLayer(options: SessionHubOptions): Layer.Layer<SessionHubService> {
  return Layer.effect(
    SessionHubService,
    Effect.gen(function* () {
      const bridge = yield* EffectBridge.make()
      const log = workerLoggerFromBridge(bridge)
      const hub = createSessionHub({ ...options, log })
      yield* Effect.addFinalizer(() => Effect.promise(() => hub.terminateAll().catch(() => undefined)))
      return SessionHubService.of(hub)
    }),
  )
}

/**
 * A layer node that spawns the pool and provides `SessionHubService`. Flag-on
 * app graphs include this node; flag-off graphs never reference it.
 */
export function makeSessionHubNode(options: SessionHubOptions): LayerNode.Node<SessionHubService, never, undefined> {
  return LayerNode.make({ service: SessionHubService, layer: createSessionHubLayer(options), deps: [] })
}

export * as SessionHubModule from "./hub"
