// Phase 12 — the pure hub (plan tasks 12.1–12.4, decisions lines 57/60).
//
// The hub is the composition of three landed layers:
//   * the Phase 4 `Dispatcher` — one active session per worker, bounded FIFO
//     admission, on-demand headroom, crash handling and the key→worker map;
//   * the Phase 5 executor pool — workers whose run-state machine lives inside
//     the isolate (`execute.ts`);
//   * the Phase 11 cooperative cancel path — `cancelWithGrace`.
//
// It owns NO session execution: every session runs in a worker isolate, the hub
// only schedules and forwards control. `SessionHubService` is the Effect service
// `run-state.ts` delegates to when `experimentalSessionWorkers` is on; the
// pool-spawning layer node (`makeSessionHubNode`) provides it and tears the pool
// down with the layer scope.

import { Context, Effect, Fiber, Layer, Option } from "effect"
import { z } from "zod"
import os from "node:os"
import type { Hooks, ToolContext as PluginToolContext, ToolDefinition } from "@opencode-ai/plugin"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { ModelsDev } from "@opencode-ai/core/models-dev"
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
import type { InstanceContext } from "@/project/instance-context"
import { mergeEnv } from "@/session/broker/env"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Project } from "@opencode-ai/schema/project"
import {
  createBroker,
  serveBrokerPort,
  type BrokerBackend,
  type BrokerClient,
  type BrokerOptions,
} from "@/session/broker/broker"
import { createEgressReceiver } from "@/session/egress/receiver"
import { attachMeshWorker, createEgressMesh, type DurableCommit, type EgressMesh } from "@/session/egress/mesh"
import { durableMetaOf, type EventV2Event, type EventsAckPayload, type EventsResyncPayload } from "./frame"
import { createLiveEventPublisher, instanceLocation, EventV2Bridge, type LiveEventPublisher } from "@/event-v2-bridge"
import type { EventV2 } from "@opencode-ai/core/event"
import {
  createWriterClient,
  serveWriterPort,
  type HubWriter,
  type WriterClient,
  type WriterClientOptions,
} from "@/session/writer/client"
import { migrationStatements } from "../writer/migration.statements.gen"
import type { SessionID } from "@/session/schema"
import { defined } from "@/util/object"
import { setThreadName } from "@/util/thread-name"
import {
  createDispatcher,
  deriveHeadroom,
  type CancellationConfig,
  type CancelledMode,
  type Dispatcher,
  type DispatcherOptions,
  type SessionWorker,
} from "./dispatch"
import { createJobRegistry, type JobInfo, type JobRegistry } from "./job-registry"
import { dispatchSubagent as dispatchSubagentToHub, type SubagentDispatchRequest } from "./subagent"
import { createResultRouter, jobPortFromMessagePort, serveJobRegistry, type JobPort } from "./job-client"
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
import { resolveCancelGraceMs, type CancelOutcome } from "./cancel"
import { resolveSessionWorkerIdleTimeoutMs, resolveWorkerCount, WORKER_COUNT_ENV } from "./pool"
import { createEpochSequence, makeInstanceContext, type EpochSequence } from "./snapshot"
import {
  buildSessionWorkerInit,
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
import type {
  MessagesTransformInput,
  MessagesTransformOutput,
  SystemTransformInput,
  SystemTransformOutput,
  WorkerPluginHooks,
} from "./plugin-host"
import type { HubProxyHandlers, HubProxyStreamHandler } from "./hub-proxy-wire"
import { serveHubProxy } from "./hub-proxy-wire"
import type {
  PluginToolDescriptor,
  PluginToolExecuteInput,
  PluginToolExecuteResult,
  PluginToolProgress,
  PluginToolProgressSink,
} from "./plugin-tool-wire"
import { PluginToolNotSerializableError } from "./plugin-tool-wire"
import { portFromMessagePort } from "./port"
import type { SessionWorkerInit } from "./session-worker"
import { noopWorkerLogger, workerLogMessage, workerLoggerFromBridge, type WorkerLogger } from "./log"
import { sessionThreadName } from "@/session/worker/thread-name"
import { Session } from "@/session/session"
import { Truncate } from "@/tool/truncate"
import { pluginToolJsonSchema } from "@/tool/registry"
import { Agent } from "@/agent/agent"
import type { ShellInput } from "@/session/prompt"

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
  /** The hub-hosted cross-worker job registry (plan decision line 54). */
  readonly jobs: JobRegistry
  /**
   * The session a worker is currently bound to (the dispatcher's key→worker map,
   * inverted). A worker isolate serves exactly one session at a time, so this is
   * the resolver the hub-proxy and live-event paths use to reach the worker's own
   * instance context instead of latching the first dispatched one.
   */
  sessionOfWorker(workerID: number): SessionID | undefined
  /**
   * Serve the hub registry to a worker's dedicated job port (plan task 9.2).
   * The hub routes a settled child's result to the worker hosting its
   * `parentID` (9.4). Returns a disposer; idempotent per worker.
   */
  attachJobPort(workerID: number, port: JobPort): () => void
  /**
   * Serve the worker's transferred writer / egress / broker ports against the
   * hub's authorities (plan tasks 17/19/8.1). Returns a disposer; idempotent per
   * worker. No-op for a port the hub has no backend for.
   */
  attachWorkerPorts(workerID: number, ports: WorkerTransferredPorts): () => void
  /**
   * Serve the three hub-proxy authorities (`SessionStatus` / `Permission` /
   * `Question`) over a worker's control port. Returns a disposer; idempotent per
   * worker; no-op when the hub has no hub-proxy handlers.
   */
  attachHubProxyPort(workerID: number, port: MessagePort): () => void
  /** Dispatch `sessionID` to a worker and resolve with its result (admission-aware). */
  ensureRunning(sessionID: SessionID): Promise<SessionExecutionResult>
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

/** The worker ports transferred as one `{type:"ports", …}` frame (session-worker.ts). */
export interface WorkerTransferredPorts {
  readonly writer?: MessagePort
  readonly egress?: MessagePort
  readonly egressControl?: MessagePort
  readonly broker?: MessagePort
}

/** The data ports present in a transfer, in a stable order (for diagnostics). */
const TRANSFERRED_PORT_NAMES = ["writer", "egress", "broker"] as const
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
 * The hub-resident authorities a worker's ports are served against. `writer` and
 * `broker` are required to serve their ports; `egress` defaults to a draining
 * receiver (the SSE route wires a real sink later); `hubProxy` is served over the
 * worker control port when present.
 */
export interface WorkerPortBackends {
  readonly writer: HubWriter
  readonly broker: BrokerBackend
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
   * Tear down the backends' own resources (a writer/broker isolate). Called once
   * by `SessionHub.terminateAll`, only when the backends were actually built.
   */
  readonly dispose?: () => void | Promise<void>
}

/**
 * The hub's port authorities, either ready to use or built on demand. A source
 * function is invoked at most once, on the first worker port attach (i.e. on the
 * first dispatch), so building a production node never spawns a writer/broker
 * isolate at graph-build time.
 */
export type WorkerPortBackendsSource = WorkerPortBackends | (() => WorkerPortBackends)

export class SessionHubService extends Context.Service<SessionHubService, SessionHub>()("@opencode/SessionHub") {}

/**
 * Builds the executor pool once the hub can hand it the job-port sink. A factory
 * keeps the pool lazy: `createSessionHub` passes its own `attachJobPort` here so
 * a worker's transferred job port is wired automatically (plan 9.2), and a
 * lazy pool spawns no isolates until the first dispatch (step 12).
 */
export type ExecutorWorkerPoolFactory = (onJobPort: (workerID: number, port: MessagePort) => void) => ExecutorWorkerPool

/**
 * Cancellation wiring, either pre-built or derived from the materialised pool
 * (the production case needs the pool to look up a worker's `cancelPort`).
 */
export type CancellationSource = CancellationConfig | ((pool: ExecutorWorkerPool) => CancellationConfig)

export interface SessionHubOptions {
  /** A ready pool, or a factory the hub calls with its job-port sink. */
  readonly pool: ExecutorWorkerPool | ExecutorWorkerPoolFactory
  /**
   * Hub-resident authorities for the worker writer / egress / broker ports and
   * the hub-proxy control port (plan tasks 8/17/19/24). When present, the hub
   * serves transferred worker ports; the production init advertises
   * `capabilities.sessionWorkerPorts`.
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
   * Invoked synchronously when a worker's `dispatchSubagent` RPC admits a child,
   * BEFORE the child is scheduled. Unlike {@link onDispatch}, this fires for the
   * raw-scheduler subagent path (`ensureRunning`/`startShell` never run for a
   * child), so the production layer uses it to copy the parent's captured
   * per-session instance/context onto the child.
   */
  readonly onSubagentDispatch?: (childSessionID: SessionID, parentSessionID: SessionID) => void
  readonly headroom?: number
  readonly admissionCapacity?: number
  readonly childCapacity?: number
  readonly headroomIdleReapMs?: number
  readonly cancellation?: CancellationSource
  readonly onDegraded?: (workerID: number) => void
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
   * Pre-built job registry (tests). Defaults to one wired to route a settled
   * child's result to the worker hosting its `parentID` (plan 9.4).
   */
  readonly jobs?: JobRegistry
  /**
   * Receive a worker's `{type:"pid-report"}` frame (plan 11.5) from its control
   * port. `workerID` is the authoritative owner (the port association), not the
   * worker's self-reported id. The production hub mirrors each entry into its
   * `PidRegistry`; absent means pid reports are ignored (a pure hub, no reaper).
   */
  readonly onPidReport?: (workerID: number, report: PidReport) => void
  /**
   * Resolved `subagent_depth` for the `dispatchSubagent` hub-proxy RPC; defaults
   * to `1`. The hub owns admission, so it is the authority for the nesting limit
   * a worker's `task` tool reaches it with.
   */
  readonly subagentDepth?: number
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
}

/**
 * A worker's events-port sink. Each event is republished live-only — never
 * re-committed, because a durable worker event already committed in its isolate.
 * `end`/`resync`/`ack` only update flow-control state: the sink owns no buffering,
 * so the overflow/resync policy lives with the receiver that owns backpressure.
 */
export interface HubEventSink {
  /** Republish one worker event live-only. */
  event(event: EventV2Event): void
  /** The worker signalled end-of-stream. */
  end(): void
  /** A backpressure resync: a non-durable delta was dropped; durable events continue. */
  resync(payload?: EventsResyncPayload): void
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
  const { publisher } = options
  let ended = false
  let resyncs = 0
  let acks = 0
  return {
    event(event) {
      // The event crosses verbatim as `{ id, type, properties }`; the frame's
      // `generation`/`ordinal` metadata is deliberately not republished.
      publisher.publish({ id: event.id, type: event.type, data: event.properties })
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
 * A writer-stamped durable event (`generation` + `ordinal`) is additionally
 * handed to `onDurable` — the hub's cross-isolate mesh commit path, so another
 * worker's `events.durable()` subscribers wake. The live republish is unchanged:
 * the mesh is an additive wakeup channel, never a re-commit.
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
      sink.event(frame.payload.event)
      if (onDurable === undefined) return
      // The canonical durability test (present only when BOTH `generation` and
      // `ordinal` are set), shared with the receiver and fan-out so the three
      // never drift on exactly which frames are durable.
      const meta = durableMetaOf(frame)
      if (meta !== undefined) onDurable({ meta, event: frame.payload.event })
    },
    onEnd: () => sink.end(),
    onResync: (payload) => sink.resync(payload),
    onAck: (payload) => sink.ack(payload),
    ...(onError === undefined ? {} : { onError }),
  })
  return () => receiver.close()
}

// ---------------------------------------------------------------------------
// Production hub-proxy handlers (plan decision 64).
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
   * Optional subagent-dispatch authority (Solution 1). Present only when a
   * caller wires the hub registry itself; `createSessionHub` installs its own
   * pool-aware authority on attach, so production leaves this absent.
   */
  readonly dispatchSubagent?: (request: SubagentDispatchRequest) => Promise<JobInfo>
  /**
   * Optional system-prompt transform authority. The worker cannot run
   * `experimental.chat.system.transform` itself (plugins are loaded on the hub),
   * so it calls the hub, which mutates and returns `output`. Absent means the
   * handler returns `output` unchanged (flag-off / no plugin).
   */
  readonly systemTransform?: (input: SystemTransformInput, output: SystemTransformOutput) => Promise<SystemTransformOutput>
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
   * Optional plugin-tool execute authority. The worker cannot run a plugin tool's
   * `execute` (the plugin instance lives on the hub), so it forwards the call
   * here, where the hub-owned definition runs and a clone-safe result returns.
   * `emit` is the streaming sink that `pluginTool.metadata` progress events are
   * pushed through; a non-streaming caller supplies a no-op. Unlike the transforms
   * there is no identity pass-through: absent means the handler rejects.
   */
  readonly pluginTool?: (input: PluginToolExecuteInput, emit: PluginToolProgressSink) => Promise<PluginToolExecuteResult>
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
}

/**
 * Executes a hub authority effect for a worker request. The hub is a single
 * global runtime while a worker belongs to the instance that dispatched it, so
 * the runner provides that instance's context (captured at dispatch).
 */
export type HubProxyRunner = <A, E>(effect: Effect.Effect<A, E>) => Promise<A>

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
    ...(authorities.dispatchSubagent === undefined
      ? {}
      : {
          dispatchSubagent: (request) => authorities.dispatchSubagent!(request as SubagentDispatchRequest),
        }),
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
    const definition =
      Option.isNone(current) ? undefined : findPluginToolDefinition(yield* current.value.list(), input.toolID)
    if (definition === undefined) {
      return yield* Effect.fail(new UnknownPluginToolError(input.toolID))
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
 * The per-worker port servers: the job link plus the transferred writer / egress
 * / broker and hub-proxy control ports. Each attach is idempotent per worker (a
 * re-attach disposes the previous server first), and `disposeAll` tears every
 * server down. The backend source and handler map are thunks because both
 * materialise lazily, after the pool and dispatcher exist.
 */
interface WorkerPortRegistryOptions {
  readonly log: WorkerLogger
  readonly jobs: JobRegistry
  readonly backends: () => WorkerPortBackends | undefined
  /** The handler map for one worker's control port (per-session context). */
  readonly hubProxyHandlersFor: (workerID: number) => HubProxyHandlers | undefined
  /** The streaming half of one worker's control port (e.g. `pluginTool.metadata`). */
  readonly hubProxyStreamingFor: (workerID: number) => Readonly<Record<string, HubProxyStreamHandler>> | undefined
  readonly onPidReport?: (workerID: number, report: PidReport) => void
}

interface WorkerPortRegistry {
  readonly jobPortFor: (workerID: number) => JobPort | undefined
  readonly attachJobPort: (workerID: number, port: JobPort) => () => void
  readonly attachWorkerPorts: (workerID: number, ports: WorkerTransferredPorts) => () => void
  readonly attachHubProxyPort: (workerID: number, port: MessagePort) => () => void
  readonly disposeAll: () => void
}

function createWorkerPortRegistry(options: WorkerPortRegistryOptions): WorkerPortRegistry {
  const { log } = options
  const jobPorts = new Map<number, JobPort>()
  const jobDisposers = new Map<number, () => void>()
  const workerPortDisposers = new Map<number, () => void>()
  const hubProxyDisposers = new Map<number, () => void>()

  const attachJobPort = (workerID: number, port: JobPort): (() => void) => {
    log(workerLogMessage("ports connected"), { level: "INFO", workerID, ports: ["job"] })
    jobDisposers.get(workerID)?.()
    jobPorts.set(workerID, port)
    const disposeServer = serveJobRegistry(options.jobs, port)
    const dispose = () => {
      disposeServer()
      jobPorts.delete(workerID)
      jobDisposers.delete(workerID)
    }
    jobDisposers.set(workerID, dispose)
    return dispose
  }

  // Serve a worker's transferred writer / egress / broker ports against the hub
  // authorities. A port the hub has no backend for is left unserved (the worker
  // only transfers ports the hub advertised in `capabilities.sessionWorkerPorts`).
  const attachWorkerPorts = (workerID: number, ports: WorkerTransferredPorts): (() => void) => {
    log(workerLogMessage("ports connected"), { level: "INFO", workerID, ports: transferredPortNames(ports) })
    workerPortDisposers.get(workerID)?.()
    const disposers: (() => void)[] = []
    const backends = options.backends()
    if (backends !== undefined) {
      if (ports.writer !== undefined) disposers.push(serveWriterPort(backends.writer, ports.writer))
      if (ports.broker !== undefined) disposers.push(serveBrokerPort(backends.broker, ports.broker))
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
    jobPortFor: (workerID) => jobPorts.get(workerID),
    attachJobPort,
    attachWorkerPorts,
    attachHubProxyPort,
    disposeAll: () => {
      for (const dispose of jobDisposers.values()) dispose()
      for (const dispose of workerPortDisposers.values()) dispose()
      for (const dispose of hubProxyDisposers.values()) dispose()
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
    // this can never re-commit the session state already written through the writer.
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
        mode: outcome.mode,
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

/** Compose the dispatcher + executor pool + cooperative cancel into one hub. */
export function createSessionHub(options: SessionHubOptions): SessionHub {
  const log = options.log ?? noopWorkerLogger
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
  // Assigned once the registry exists (below); read lazily by `portForSession`,
  // which the job registry calls only after a dispatch has attached a port.
  let ports: WorkerPortRegistry

  const workerForSession = (sessionID: string): number | undefined =>
    dispatcher.workerForSession(sessionID as SessionID)
  const portForSession = (sessionID: string): JobPort | undefined => {
    const workerID = workerForSession(sessionID)
    return workerID === undefined ? undefined : ports.jobPortFor(workerID)
  }
  const jobs = options.jobs ?? createJobRegistry({ routeResult: createResultRouter(portForSession) })
  // One registry for the whole hub: the job link routes a settled child to the
  // worker currently hosting its `parentID` (the dispatcher's key→worker map).
  ports = createWorkerPortRegistry({
    log,
    jobs,
    backends: portBackends,
    hubProxyHandlersFor,
    hubProxyStreamingFor,
    onPidReport: options.onPidReport,
  })

  /**
   * The served hub-proxy handler map for ONE worker: the caller's authorities
   * plus the pool-aware `dispatchSubagent` RPC. Built per attach so a worker
   * reads its OWN session context (a source backend may specialize by worker);
   * a static `hubProxy` remains the fallback for a plain test hub. The handler
   * closes over the registry and dispatcher, both materialised by attach time,
   * so it is built lazily. A caller-supplied `dispatchSubagent` authority wins.
   */
  function hubProxyHandlersFor(workerID: number): HubProxyHandlers | undefined {
    const backends = portBackends()
    const base = backends?.hubProxyFor?.(workerID) ?? backends?.hubProxy
    if (base === undefined) return undefined
    const dispatchSubagent = (...args: readonly unknown[]): Promise<JobInfo> =>
      dispatchSubagentToHub(
        {
          registry: jobs,
          scheduler: dispatcher,
          depthLimit: options.subagentDepth ?? 1,
          headroomFull: () => dispatcher.headroomSlotCount >= dispatcher.headroomCapacity,
          resultFor: (sessionID) => pool.resultFor(sessionID),
          ...defined({ onSubagentDispatch: options.onSubagentDispatch }),
        },
        args[0] as SubagentDispatchRequest,
      )
    return { ...base, dispatchSubagent: base.dispatchSubagent ?? dispatchSubagent }
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

  // Materialise the pool, wiring its transferred job ports into the registry.
  const onJobPort = (workerID: number, port: MessagePort): void => {
    ports.attachJobPort(workerID, jobPortFromMessagePort(port))
  }
  const pool = typeof options.pool === "function" ? options.pool(onJobPort) : options.pool
  const resolvedCancellation =
    typeof options.cancellation === "function" ? options.cancellation(pool) : options.cancellation
  // Every terminal cancel mode the dispatcher reports, keyed by session id. The
  // dispatcher cascades children-first but discards a descendant's own outcome,
  // so the reporter records the modes and reconstructs which descendants still
  // need a fallback idle (see `cancelWithReport`).
  // The hub's job registry is the cross-worker parent→child authority (9.4), so
  // an unwired cancellation config still cascades to subagents and still marks a
  // cancelled job. An explicit config always wins.
  const childrenOf: (sessionID: SessionID) => ReadonlyArray<SessionID> =
    resolvedCancellation?.childrenOf ?? ((sessionID) => jobs.childrenOf(sessionID).map((child) => child as SessionID))
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
            if (baseOnCancelled !== undefined) baseOnCancelled(event)
            else void jobs.cancel(event.sessionID)
          },
        }
  dispatcher = createDispatcher({
    pool,
    ...defined({
      headroom: options.headroom,
      admissionCapacity: options.admissionCapacity,
      childCapacity: options.childCapacity,
      headroomIdleReapMs: options.headroomIdleReapMs,
      cancellation,
      writeBarrier: options.writeBarrier,
      onDegraded: options.onDegraded,
      log,
    }),
  })
  // Production depth plumbing: a session's subagent depth lives on the hub job
  // registry (`subagent.dispatchSubagent` registers the child there), so resolve
  // it when the caller does not pass one. Without this every production dispatch
  // resolved `depth: 0` and the on-demand headroom / child-queue paths never ran.
  const dispatch: SessionDispatch = createSessionDispatch({
    dispatcher,
    pool,
    depthOf: (sessionID) => jobs.get(sessionID)?.depth ?? 0,
  })
  const onDispatch = options.onDispatch

  return {
    dispatcher,
    jobs,
    // The inverted key→worker map: a worker's session, or undefined when idle.
    sessionOfWorker: (workerID) => dispatcher.registry.sessionsOn(workerID)[0],
    attachJobPort: ports.attachJobPort,
    attachWorkerPorts: ports.attachWorkerPorts,
    attachHubProxyPort: ports.attachHubProxyPort,
    ensureRunning: (sessionID) => {
      onDispatch?.(sessionID)
      return dispatch.ensureRunning(sessionID)
    },
    startShell: (sessionID, shell) => {
      onDispatch?.(sessionID)
      return dispatch.startShell(sessionID, shell)
    },
    assertNotBusy: (sessionID) => dispatch.assertNotBusy(sessionID),
    cancel: async (sessionID) => (await cancelReporter.cancelWithReport(sessionID)).outcome,
    cancelWithReport: cancelReporter.cancelWithReport,
    cancelAll: () => dispatch.cancelAll(),
    async terminateAll() {
      ports.disposeAll()
      await dispatcher.terminateAll()
      // Only tear down backends we actually built (a source never attached to is
      // never constructed, so no isolate is spawned just to dispose it).
      const backends = disposables()
      await backends?.dispose?.()
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
  /**
   * OS thread name (`WorkerOptions.name`) applied at spawn, so the isolate is
   * born with the right Linux `comm` before it self-names. Absent leaves Bun's
   * default (`Worker`). Fail-soft: a name never affects spawning.
   */
  readonly name?: string
  readonly timeoutMs?: number
  /** Maximum buffered outbound frames kept for late `waitFor` matches. */
  readonly bufferCapacity?: number
  /** Called with each `MessagePort` the worker transfers (the job control link). */
  readonly onPort?: (port: MessagePort) => void
  /** Called with the worker's transferred writer / egress / broker ports. */
  readonly onWorkerPorts?: (ports: WorkerTransferredPorts) => void
  /** Called with the worker's transferred hub-proxy control port. */
  readonly onHubProxyPort?: (port: MessagePort) => void
  /** Optional structured logger seam; absent defaults to a no-op (flag-off behavior). */
  readonly log?: WorkerLogger
  /** Hub slot id for diagnostics (a boot/import failure is reported under it). */
  readonly workerID?: number
}

const DEFAULT_TRANSPORT_TIMEOUT_MS = 30_000

/** Cap on buffered frames, so a transport nobody waits on cannot grow unbounded. */
export const DEFAULT_TRANSPORT_BUFFER_CAPACITY = 1024

/** A port-bearing frame a worker may transfer; anything else is an executor frame. */
type PortFrame =
  | { readonly kind: "job-port"; readonly port: MessagePort }
  | { readonly kind: "worker-ports"; readonly ports: WorkerTransferredPorts }
  | { readonly kind: "hub-proxy"; readonly port: MessagePort }

const asMessagePort = (value: unknown): MessagePort | undefined =>
  typeof MessagePort !== "undefined" && value instanceof MessagePort ? value : undefined

/**
 * Classify a worker message as a port transfer, or `undefined` for an executor
 * frame. Recognises a bare port, `{type:"job-port", port}`,
 * `{type:"ports", writer, egress, broker}` and `{type:"hub-proxy", port}`.
 */
function portFrame(data: unknown): PortFrame | undefined {
  const bare = asMessagePort(data)
  if (bare !== undefined) return { kind: "job-port", port: bare }
  if (typeof data !== "object" || data === null) return undefined
  const frame = data as {
    readonly type?: unknown
    readonly port?: unknown
    readonly writer?: unknown
    readonly egress?: unknown
    readonly egressControl?: unknown
    readonly broker?: unknown
  }
  if (frame.type === "job-port") {
    const port = asMessagePort(frame.port)
    return port === undefined ? undefined : { kind: "job-port", port }
  }
  if (frame.type === "ports") {
    return {
      kind: "worker-ports",
      ports: {
        writer: asMessagePort(frame.writer),
        egress: asMessagePort(frame.egress),
        egressControl: asMessagePort(frame.egressControl),
        broker: asMessagePort(frame.broker),
      },
    }
  }
  if (frame.type === "hub-proxy") {
    const port = asMessagePort(frame.port)
    return port === undefined ? undefined : { kind: "hub-proxy", port }
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
 * A `MessagePort`-free `ExecutorTransport` over a real Bun worker: buffers every
 * outbound frame so a `waitFor` may match one that already arrived, and fails
 * loud on timeout or isolate error.
 */
export function createBunExecutorTransport(options: BunExecutorTransportOptions): ExecutorTransport {
  const log = options.log ?? noopWorkerLogger
  const timeoutMs = options.timeoutMs ?? DEFAULT_TRANSPORT_TIMEOUT_MS
  const bufferCapacity = options.bufferCapacity ?? DEFAULT_TRANSPORT_BUFFER_CAPACITY
  const worker = new Worker(options.entry, {
    env: mergeEnv(options.env),
    ...defined({ workerData: options.workerData, name: options.name }),
  })
  const messages: SessionExecutorOutbound[] = []
  const waiters = new Set<PendingWaiter>()
  let alive = true
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

  /** Remove and return the first buffered frame matching `predicate`, if any. */
  const takeBuffered = (
    predicate: (message: SessionExecutorOutbound) => boolean,
  ): SessionExecutorOutbound | undefined => {
    const index = messages.findIndex(predicate)
    if (index === -1) return undefined
    return messages.splice(index, 1)[0]
  }

  worker.onmessage = (event: MessageEvent) => {
    const frame = portFrame(event.data)
    if (frame !== undefined) {
      if (frame.kind === "job-port") options.onPort?.(frame.port)
      else if (frame.kind === "worker-ports") options.onWorkerPorts?.(frame.ports)
      else options.onHubProxyPort?.(frame.port)
      return
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

  worker.onerror = (event: ErrorEvent) => {
    alive = false
    // Detach so a terminating worker cannot re-enter this handler, then fail
    // every waiter and tear the isolate down.
    worker.onmessage = null
    worker.onerror = null
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
    void worker.terminate()
  }

  return {
    post(message: SessionExecutorInbound) {
      if (!alive) throw new Error("cannot post to a terminated session worker")
      worker.postMessage(message)
    },
    waitFor(predicate, waitOptions) {
      const buffered = takeBuffered(predicate)
      if (buffered !== undefined) return Promise.resolve(buffered)
      // A buffered frame still wins (it arrived before the failure); otherwise an
      // already-dead isolate fails the wait immediately rather than after the
      // full deadline.
      if (terminal !== undefined) return Promise.reject(terminal)
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
    async terminate(code = 0): Promise<number> {
      if (!alive) return code
      alive = false
      failWaiters(new Error("session worker terminated"))
      const result = await worker.terminate()
      return typeof result === "number" ? result : code
    },
  }
}

export interface BunExecutorWorkerPoolOptions {
  readonly k: number
  readonly entry: string | URL
  readonly env?: Record<string, string | undefined>
  readonly workerData?: unknown
  readonly timeoutMs?: number
  /** Run-reply liveness budget; `0`/absent disables the watchdog. */
  readonly runIdleTimeoutMs?: number
  readonly workKind?: string
  /**
   * Per-session durable-egress barrier (plan 2.6 / R4#16): `execute` holds the
   * run until this resolves, so a result is never recorded before the run's
   * durable egress write is acked. Absent makes the result immediate.
   */
  readonly egressBarrier?: (sessionID: SessionID) => Promise<void> | undefined
  /** Receive each worker's transferred job port, keyed by worker id (plan 9.2). */
  readonly onJobPort?: (workerID: number, port: MessagePort) => void
  /** Receive each worker's transferred writer / egress / broker ports (plan 8/17/19). */
  readonly onWorkerPorts?: (workerID: number, ports: WorkerTransferredPorts) => void
  /** Receive each worker's transferred hub-proxy control port (plan 24). */
  readonly onHubProxyPort?: (workerID: number, port: MessagePort) => void
  /** The session the isolate is bound to, when known at spawn (observability B1). */
  readonly sessionID?: SessionID
  /** Why this isolate is spawning; defaults to `"initial"` (observability B1). */
  readonly reason?: WorkerSpawnReason
  /** Optional structured logger seam; absent defaults to a no-op (flag-off behavior). */
  readonly log?: WorkerLogger
}

/** Create one eager Bun executor worker speaking the frame protocol. */
export function spawnBunExecutorWorker(id: number, options: BunExecutorWorkerPoolOptions): ExecutorWorker {
  const onJobPort = options.onJobPort
  const onWorkerPorts = options.onWorkerPorts
  const onHubProxyPort = options.onHubProxyPort
  const transport = createBunExecutorTransport({
    entry: options.entry,
    ...defined({
      env: options.env,
      workerData: options.workerData,
      // Born named `ocs<slot>` so the isolate is observable from creation, not
      // only once it self-names at boot.
      name: sessionThreadName(id),
      timeoutMs: options.timeoutMs,
      log: options.log,
      workerID: id,
      onPort: onJobPort === undefined ? undefined : (port: MessagePort) => onJobPort(id, port),
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
      runIdleTimeoutMs: options.runIdleTimeoutMs,
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

/** Production `ExecutorWorkerPool`: K executor workers speaking the frame protocol. */
export function createBunExecutorWorkerPool(options: BunExecutorWorkerPoolOptions): ExecutorWorkerPool {
  return createExecutorWorkerPool({
    k: options.k,
    spawn: (id) => spawnBunExecutorWorker(id, options),
    ...(options.log === undefined ? {} : { log: options.log }),
  })
}

// ---------------------------------------------------------------------------
// Lazy pool (step 12): no isolate is spawned until a worker is first used.
// ---------------------------------------------------------------------------

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
): ExecutorWorker {
  const state: MaterializingWorkerState = { inner: undefined, terminated: false, materializing: undefined }
  const worker: ExecutorWorker = {
    id,
    get alive() {
      return !state.terminated && (state.inner === undefined || state.inner.alive)
    },
    execute: async (run) => (await materialize(run.sessionID, state)).execute(run),
    cancel: (sessionID) => (state.inner === undefined ? Promise.resolve() : state.inner.cancel(sessionID)),
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
  )
}

/** Build one worker on first use; may be async (the production init is resolved on dispatch). */
export type LazyExecutorWorkerFactory = () => ExecutorWorker | Promise<ExecutorWorker>

export interface LazyExecutorWorkerPoolOptions {
  readonly k: number
  /** Build one worker on first use. */
  readonly create: (id: number) => ExecutorWorker | Promise<ExecutorWorker>
  /** Optional structured logger seam; absent defaults to a no-op (flag-off behavior). */
  readonly log?: WorkerLogger
}

/** A pool whose K foreground workers (and any headroom) spawn on first use. */
export function createLazyExecutorWorkerPool(options: LazyExecutorWorkerPoolOptions): ExecutorWorkerPool {
  return createExecutorWorkerPool({
    k: options.k,
    spawn: (id) => createLazyExecutorWorker(id, () => options.create(id), options.log),
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
 * A worker `workerData` payload, or a (possibly async) resolver for one. The
 * resolver receives the session being dispatched, so the production hub resolves
 * the session's own model rather than reusing the first session's.
 */
export type SessionWorkerInitSource =
  | SessionWorkerInit
  | ((sessionID?: SessionID) => SessionWorkerInit | Promise<SessionWorkerInit>)

export interface ProductionWorkerPoolOptions {
  readonly init: SessionWorkerInitSource
  readonly count: number
  /** Worker entry; defaults to {@link SESSION_WORKER_ENTRY} (overridable for tests). */
  readonly entry?: string | URL
  readonly env?: Record<string, string | undefined>
  readonly timeoutMs?: number
  /** Run-reply liveness budget; `0`/absent disables the watchdog. */
  readonly runIdleTimeoutMs?: number
  readonly workKind?: string
  /** Per-session durable-egress barrier (plan 2.6 / R4#16). */
  readonly egressBarrier?: (sessionID: SessionID) => Promise<void> | undefined
  readonly onJobPort?: (workerID: number, port: MessagePort) => void
  readonly onWorkerPorts?: (workerID: number, ports: WorkerTransferredPorts) => void
  readonly onHubProxyPort?: (workerID: number, port: MessagePort) => void
  /**
   * Fired when a worker rebinds away from a session (its isolate is retired and
   * respawned for a different session). The production hub drops that session's
   * per-session instance/context so those maps cannot grow without bound.
   */
  readonly onSessionRebound?: (sessionID: SessionID) => void
  /** Optional structured logger seam; absent defaults to a no-op (flag-off behavior). */
  readonly log?: WorkerLogger
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
      k: options.count,
      entry: options.entry ?? SESSION_WORKER_ENTRY,
      // Ship the pool id so the worker's `pid-report` frames are keyed to this
      // slot out of the box (the hub still treats its port association as authoritative).
      // The pool only spawns session workers, so the role is pinned `session`.
      workerData: { ...init, workerID: id, role: "session" },
      ...defined({
        env: options.env,
        timeoutMs: options.timeoutMs,
        runIdleTimeoutMs: options.runIdleTimeoutMs,
        workKind: options.workKind,
        egressBarrier: options.egressBarrier,
        log,
        // The session this isolate is bound to, so the B1 `worker spawning`
        // line names it (the init was already resolved for the same session).
        sessionID,
        onJobPort: options.onJobPort,
        onWorkerPorts: options.onWorkerPorts,
        onHubProxyPort: options.onHubProxyPort,
      }),
    })
  }
  return createExecutorWorkerPool({
    k: options.count,
    spawn: (id) => createSessionBoundExecutorWorker(id, spawnFor, log, options.onSessionRebound),
    log,
  })
}

/** The port-transfer sinks a production pool routes worker messages to. */
export interface WorkerPoolSinks {
  readonly onJobPort: (workerID: number, port: MessagePort) => void
  readonly onWorkerPorts: (workerID: number, ports: WorkerTransferredPorts) => void
  readonly onHubProxyPort: (workerID: number, port: MessagePort) => void
}

/** Builds the executor pool from a lazy init resolver and port sinks (test seam). */
export type ProductionPoolFactory = (
  resolveInit: (sessionID?: SessionID) => Promise<SessionWorkerInit>,
  sinks: WorkerPoolSinks,
) => ExecutorWorkerPool

/** Mark a generation as serving the worker writer / egress / broker ports. */
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
  /** Override the hub writer source; defaults to a fresh `createWriterClient()`. */
  readonly writer?: HubWriter
  /** Override the hub broker source; defaults to a fresh `createBroker()`. */
  readonly broker?: BrokerBackend
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
  /**
   * Per-worker mesh subscription backlog bound; defaults to the mesh's own
   * `MESH_SUBSCRIPTION_BOUND`. A small bound makes the overflow/resync path
   * deterministic in tests.
   */
  readonly egressMeshBound?: number
  /**
   * Handlers for the three hub-proxy authorities served over the worker control
   * port (plan decision 64). Supplied by `productionSessionHubLayer`, which
   * yields the real `SessionStatus` / `Permission` / `Question` services.
   */
  readonly hubProxy?: HubProxyHandlers
  /** Per-worker hub-proxy handlers (each control port served under its worker's session). */
  readonly hubProxyFor?: (workerID: number) => HubProxyHandlers | undefined
  /** Per-worker streaming hub-proxy handlers (see {@link WorkerPortBackends.hubProxyStreamingFor}). */
  readonly hubProxyStreamingFor?: (workerID: number) => Readonly<Record<string, HubProxyStreamHandler>> | undefined
  /** Streaming hub-proxy handlers shared by every worker. */
  readonly hubProxyStreaming?: Readonly<Record<string, HubProxyStreamHandler>>
  /**
   * The SQLite file the OWNED writer opens; defaults to the app database
   * ({@link Database.path}). Ignored when a `writer` is injected (the caller owns
   * its connection).
   */
  readonly databasePath?: string
  readonly writerOptions?: WriterClientOptions
  readonly brokerOptions?: BrokerOptions
}

/**
 * Wrap an owned hub writer so its isolate opens `filename` exactly once, on the
 * first request, then applies `statements` (the schema migrations) before any
 * request is served. The writer isolate spawns with NO connection — every
 * `publish`/`query` before `admin.open` throws `"writer connection is not open"`
 * — so the port server would reject without this. Opening lazily keeps graph
 * build free of I/O; a failed open/migrate resets the memo so a later call can
 * retry (the isolate's own respawn replays the open once it has succeeded).
 */
export function lazyOpenWriter(writer: WriterClient, filename: string, statements: ReadonlyArray<string>): HubWriter {
  let opened: Promise<unknown> | undefined
  const ensureOpen = (): Promise<unknown> => {
    if (opened !== undefined) return opened
    opened = writer
      .open(filename)
      .then(() => writer.migrate(statements))
      .catch((error) => {
        opened = undefined
        throw error
      })
    return opened
  }
  return {
    query: (method, sql, params) => ensureOpen().then(() => writer.query(method, sql, params)),
    publish: (event, options) => ensureOpen().then(() => writer.publish(event, options)),
    begin: () => ensureOpen().then(() => writer.begin()),
    txQuery: (handle, method, sql, params) => ensureOpen().then(() => writer.txQuery(handle, method, sql, params)),
    commit: (handle) => ensureOpen().then(() => writer.commit(handle)),
    rollback: (handle) => ensureOpen().then(() => writer.rollback(handle)),
  }
}

/** The production port authorities plus the disposer that tears their isolates down. */
export interface ProductionPortBackends extends WorkerPortBackends {
  readonly writer: HubWriter
  readonly broker: BrokerBackend
  /**
   * The ONE egress mesh the hub serves every worker's `egressControl` port
   * against. Exposed so a caller (or test) can observe the cross-isolate
   * `durable()` wakeups; commits are published on the events-port durable path.
   */
  readonly egressMesh: EgressMesh
  readonly dispose: () => Promise<void>
}

/**
 * Build the production hub-side port authorities from the real writer / broker /
 * egress implementations:
 *   * `writer` — a fresh hub `WriterClient` (the sole SQLite writer; structurally
 *     a `HubWriter`), which the hub serves with `serveWriterPort`;
 *   * `broker` — a fresh hub `BrokerClient` (structurally a `BrokerBackend`),
 *     served with `serveBrokerPort`;
 *   * `egress` — the draining receiver ({@link drainEgressPort}); the SSE route
 *     swaps in a real sink later.
 *
 * `createBroker` spawns its isolate eagerly at construction, so the caller MUST
 * build these lazily (as a {@link WorkerPortBackendsSource}), never at
 * graph-build time. `dispose` closes the writer isolate and tears the broker
 * down; it is a no-op for an injected authority the caller owns.
 */
export function buildProductionPortBackends(options: ProductionPortBackendsOptions = {}): ProductionPortBackends {
  let ownedWriter: WriterClient | undefined
  let ownedBroker: BrokerClient | undefined
  // An injected writer is trusted (the caller owns its connection); the owned
  // writer opens the app database lazily, on the first request.
  const writer: HubWriter =
    options.writer ??
    lazyOpenWriter(
      (ownedWriter = createWriterClient(options.writerOptions)),
      options.databasePath ?? Database.path(),
      migrationStatements,
    )
  const broker: BrokerBackend = options.broker ?? (ownedBroker = createBroker(options.brokerOptions))
  // ONE mesh per hub: every worker's transferred `egressControl` port attaches
  // here, and the durable commit path publishes to it (plan task 8.10/8.11).
  const egressMesh = createEgressMesh(
    options.egressMeshBound === undefined ? {} : { bound: options.egressMeshBound },
  )
  return {
    writer,
    broker,
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
        const attachment =
          control === undefined ? undefined : attachMeshWorker(egressMesh, String(workerID), control)
        // Prefer the worker's own live publisher (its session's location); fall
        // back to the shared one for callers that supply a single publisher.
        const publisher = options.eventsForWorker?.(workerID) ?? options.events
        const disposeReceiver =
          publisher === undefined
            ? drainEgressPort(port)
            : serveEventsPort(
                port,
                createHubEventSink({ publisher }),
                undefined,
                (commit) => egressMesh.publish(commit),
              )
        return () => {
          disposeReceiver()
          attachment?.dispose()
        }
      }),
    dispose: async () => {
      if (ownedWriter !== undefined) await ownedWriter.close().catch(() => undefined)
      if (ownedBroker !== undefined) await ownedBroker.teardown().catch(() => undefined)
    },
  }
}

export interface ProductionSessionHubOptions {
  /** The `workerData` payload, or a resolver resolved once on the first dispatch. */
  readonly init: SessionWorkerInitSource
  /** Pool size; defaults to `resolveWorkerCount()`. */
  readonly count?: number
  readonly entry?: string | URL
  readonly timeoutMs?: number
  /**
   * Run-reply liveness budget for the spawned workers. Defaults to
   * `OPENCODE_SESSION_WORKER_IDLE_TIMEOUT_MS` (`0` = disabled).
   */
  readonly runIdleTimeoutMs?: number
  readonly workKind?: string
  readonly env?: Record<string, string | undefined>
  readonly headroom?: number
  readonly admissionCapacity?: number
  readonly childCapacity?: number
  /** Cancellation tuning; cooperative cancel is always wired (step 13). */
  readonly cancellation?: WorkerCancellationOptions
  readonly onDegraded?: (workerID: number) => void
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
  /** Resolved `subagent_depth` for the hub-proxy `dispatchSubagent` RPC. */
  readonly subagentDepth?: number
  /**
   * Worker-port backends (plan tasks 8/17/19/24), ready or lazily built. When
   * present the resolved init advertises `capabilities.sessionWorkerPorts` and the
   * hub serves the ports.
   */
  readonly ports?: WorkerPortBackendsSource
  /** Invoked synchronously at the start of each dispatch (see `SessionHubOptions`). */
  readonly onDispatch?: (sessionID: SessionID) => void
  /**
   * Invoked when a worker's `dispatchSubagent` RPC admits a child (see
   * `SessionHubOptions`). The production layer copies the parent's captured
   * per-session instance/context onto the child here.
   */
  readonly onSubagentDispatch?: (childSessionID: SessionID, parentSessionID: SessionID) => void
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
  /** Optional structured logger seam; absent defaults to a no-op (flag-off behavior). */
  readonly log?: WorkerLogger
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
 * captured instance (or an ancestor's), else its persisted directory, else the
 * resolving fiber's ambient instance, else the documented global fallback
 * (warned, because it is a hub-side gap).
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
  const workerCount = options.count ?? resolveWorkerCount()
  // Resolve the run-reply liveness budget once, at the hub boundary: an explicit
  // option wins, else the env override, else disabled (0).
  const runIdleTimeoutMs = options.runIdleTimeoutMs ?? resolveSessionWorkerIdleTimeoutMs({ env: options.env })
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
    headroom: options.headroom ?? deriveHeadroom(workerCount),
  })
  log(workerLogMessage("worker entry resolved"), {
    level: "INFO",
    entry: String(describedEntry.entry),
    source: describedEntry.source,
  })
  // The hub is assigned after the pool is built; port transfers arrive only on
  // dispatch (post-assignment), so the deferred reference is safe.
  let hub: SessionHub | undefined
  const sinks: WorkerPoolSinks = {
    onJobPort: (workerID, port) => hub?.attachJobPort(workerID, jobPortFromMessagePort(port)),
    onWorkerPorts: (workerID, ports) => hub?.attachWorkerPorts(workerID, ports),
    onHubProxyPort: (workerID, port) => hub?.attachHubProxyPort(workerID, port),
  }
  const resolveSource: (sessionID?: SessionID) => SessionWorkerInit | Promise<SessionWorkerInit> =
    typeof options.init === "function" ? options.init : () => options.init as SessionWorkerInit
  const resolveInit = async (sessionID?: SessionID): Promise<SessionWorkerInit> => {
    const init = await resolveSource(sessionID)
    return options.ports === undefined ? init : withSessionWorkerPorts(init)
  }
  const pool =
    options.poolFactory !== undefined
      ? options.poolFactory(resolveInit, sinks)
      : createProductionWorkerPool({
          init: resolveInit,
          count: workerCount,
          log,
          ...defined({
            entry: describedEntry.entry,
            timeoutMs: options.timeoutMs,
            runIdleTimeoutMs,
            workKind: options.workKind,
            env: options.env,
            egressBarrier: options.egressBarrier,
            onSessionRebound: options.onSessionRebound,
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
    ...defined({
      ports: options.ports,
      onDispatch: options.onDispatch,
      onSubagentDispatch: options.onSubagentDispatch,
      events: options.events,
      eventPublisherFor: options.eventPublisherFor,
      onCancelRequested: options.onCancelRequested,
      writeBarrier: options.writeBarrier,
      onPidReport,
    }),
    cancellation: (pool) =>
      createWorkerCancellation(pool, {
        ...options.cancellation,
        reap: options.cancellation?.reap ?? ((worker) => ignoreReapFailure(() => void pids.reap(worker.id))),
        reapAll: options.cancellation?.reapAll ?? (() => ignoreReapFailure(() => void pids.reapAll())),
      }),
    ...defined({
      headroom: options.headroom,
      admissionCapacity: options.admissionCapacity,
      childCapacity: options.childCapacity,
      subagentDepth: options.subagentDepth,
      onDegraded: options.onDegraded,
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
  /** Run-reply liveness budget; defaults to `OPENCODE_SESSION_WORKER_IDLE_TIMEOUT_MS`. */
  readonly runIdleTimeoutMs?: number
  readonly workKind?: string
  readonly env?: Record<string, string | undefined>
  readonly headroom?: number
  readonly admissionCapacity?: number
  readonly childCapacity?: number
  readonly cancellation?: WorkerCancellationOptions
  readonly onDegraded?: (workerID: number) => void
  /**
   * Worker-port backends, ready or lazily built. Defaults to
   * `() => buildProductionPortBackends(backends)` — the real hub writer / broker /
   * egress authorities, constructed on the first dispatch — so the production
   * node advertises `capabilities.sessionWorkerPorts` without spawning anything at
   * graph-build time.
   */
  readonly ports?: WorkerPortBackendsSource
  /** Tuning for the default production port backends (writer/broker overrides, env). */
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
  readonly log: WorkerLogger
  /** The parent of a session (the job registry's parent chain), if known. */
  readonly parentOf: (sessionID: SessionID) => SessionID | undefined
  /** The session a worker is currently bound to, if known. */
  readonly sessionOfWorker: (workerID: number) => SessionID | undefined
}

/**
 * The per-session resolvers the production hub is composed from: the session →
 * instance/context registry, the derived deep lookups (ancestor chain), the live
 * publishers stamped with the session's own location, and the per-worker
 * hub-proxy handlers and init resolver. One worker serves one session at a time,
 * so every resolver is keyed by that session rather than latching the first
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
  readonly instanceOfSessionDeep: (sessionID: SessionID | undefined) => InstanceContext | undefined
  readonly resolveInit: (sessionID?: SessionID) => Promise<SessionWorkerInit>
  readonly recordDispatch: (sessionID: SessionID) => void
  readonly copyContextToChild: (childID: SessionID, parentID: SessionID) => void
  readonly releaseSession: (sessionID: SessionID) => void
  readonly clearSessions: () => void
}

function createPerSessionResolvers(deps: PerSessionResolverDeps): PerSessionResolvers {
  const { services, bridge, authorities, log } = deps
  // Per-session state, written on EVERY dispatch (never latched with `??=`).
  // A session's own ambient `InstanceRef` and full runtime context are stored
  // under its id, so a second project's worker is never built — and its
  // hub-proxy calls never run — under the first project's directory/instance.
  const instanceBySession = new Map<SessionID, InstanceContext>()
  const contextBySession = new Map<SessionID, Context.Context<never>>()
  // Release one session's per-session state; the finalizer clears the rest.
  // Called on every rebind and at teardown, so the maps stay bounded by the set
  // of live sessions. A session's own entry is dropped once its worker rebinds
  // away (the isolate is retired) or the hub tears down; there is no race-free
  // job-settle hook to release it earlier (the registry routes a settled child's
  // result but surfaces no settle callback here), and releasing on settle would
  // race a later turn that re-resolves its init. An ancestor's entry is still
  // reachable through the parent chain below.
  const releaseSession = (sessionID: SessionID): void => {
    instanceBySession.delete(sessionID)
    contextBySession.delete(sessionID)
  }
  const clearSessions = (): void => {
    instanceBySession.clear()
    contextBySession.clear()
  }
  // The session currently being dispatched. Updated on every dispatch; used only
  // as the fallback when a pool resolves an init without a session id.
  let dispatchedSession: SessionID | undefined
  // A session's per-session entry may be absent: a subagent child admitted by the
  // raw scheduler (whose parent's `copyContextToChild` was skipped), or a session
  // whose worker already rebound and released it. Resolve it through the job
  // registry's parent chain so the SAME project's ancestor supplies the
  // instance/context — never a last-writer-wins latch, which under concurrent
  // multi-project dispatch can hold ANOTHER project's instance. Subagents share
  // the parent's directory, so an ancestor entry is always the same project.
  // `seen` guards a corrupt/cyclic parent edge.
  const findUp = <T>(sessionID: SessionID | undefined, get: (id: SessionID) => T | undefined): T | undefined => {
    let id = sessionID
    const seen = new Set<string>()
    while (id !== undefined && !seen.has(id)) {
      seen.add(id)
      const value = get(id)
      if (value !== undefined) return value
      id = deps.parentOf(id)
    }
    return undefined
  }
  const instanceOfSessionDeep = (sessionID: SessionID | undefined): InstanceContext | undefined =>
    findUp(sessionID, (id) => instanceBySession.get(id))
  const contextOfSessionDeep = (sessionID: SessionID | undefined): Context.Context<never> | undefined =>
    findUp(sessionID, (id) => contextBySession.get(id))
  const contextOfWorker = (workerID: number): Context.Context<never> | undefined =>
    contextOfSessionDeep(deps.sessionOfWorker(workerID))
  // Live-only publisher, stamped with the emitting session's own location so a
  // republished worker event passes THAT session's `/event` SSE directory filter
  // — never a first-dispatch directory.
  const eventsForSession = (sessionID: SessionID): LiveEventPublisher =>
    createLiveEventPublisher(bridge, { location: () => instanceLocation(instanceOfSessionDeep(sessionID)) })
  const eventsForWorker = (workerID: number): LiveEventPublisher | undefined => {
    const sessionID = deps.sessionOfWorker(workerID)
    return sessionID === undefined ? undefined : eventsForSession(sessionID)
  }
  // Shared fallback publisher (a caller may reach `createSessionHub` directly).
  const events = createLiveEventPublisher(bridge)
  // A hub authority effect is closed over its own services but reads the ambient
  // `InstanceRef`; run it under the worker's session context so it resolves.
  const runHubProxyFor = (context: Context.Context<never> | undefined): HubProxyRunner => (effect) =>
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
  const hubProxyAuthoritiesFor = (context: Context.Context<never> | undefined): HubProxyAuthorities => ({
    sessionStatus: authorities.sessionStatus,
    permission: authorities.permission,
    question: authorities.question,
    systemTransform: systemTransformFor(context),
    messagesTransform: messagesTransformFor(context),
    // The worker forwards plugin-tool executes here; the definition (and its
    // plugin instance) stays on the hub. `registerAbort` keys each in-flight call
    // so the hub cancel path / `pluginTool.abort` can unwind it.
    pluginTool: pluginToolFor(context),
    // The lazy descriptor refresh: descriptors shipped at init go stale when a
    // plugin reloads, so the worker re-fetches them here.
    pluginToolList: () => resolveContextPluginToolDescriptors(context),
    // Best-effort per-call abort the worker may request directly.
    pluginToolAbort: (input) => pluginToolAborts.abort(input.sessionID, input.callID),
  })
  const hubProxyFor = (workerID: number): HubProxyHandlers => {
    const context = contextOfWorker(workerID)
    return buildHubProxyHandlers(hubProxyAuthoritiesFor(context), runHubProxyFor(context))
  }
  // The streaming half of the map: `pluginTool.execute` pushes mid-execute
  // metadata to the awaiting worker. Served alongside the plain map.
  const hubProxyStreamingFor = (workerID: number): Readonly<Record<string, HubProxyStreamHandler>> => {
    const context = contextOfWorker(workerID)
    return buildHubProxyStreamHandlers(hubProxyAuthoritiesFor(context))
  }
  // Per-session: the pool passes the session it is building a worker for, so
  // this session's instance/model (and plugin effect) is the one shipped, never
  // the first dispatched session's. `dispatchedSession` is the fallback for a
  // custom pool that resolves without a session id.
  const resolveInit = async (sessionID?: SessionID): Promise<SessionWorkerInit> => {
    const session = sessionID ?? dispatchedSession
    // The ancestor-chain context keeps `loadDispatchedSession` reachable for a
    // child whose own entry is missing, so its PERSISTED directory/project still
    // resolves instead of the hub cwd.
    const context = contextOfSessionDeep(session)
    // One `Session.get` yields the persisted model, directory/project and the
    // per-request plugin input (agent + last user message) the hooks resolve
    // against, so a hook keyed on either matches flag-off rather than an empty stub.
    const dispatched = await loadDispatchedSession(context, session)
    const instance = resolveSessionInstance(session, dispatched, instanceOfSessionDeep, log)
    const model = dispatched?.model
    const request = dispatched?.request
    const pluginSources = deps.plugins !== undefined ? await deps.plugins() : await resolveContextPluginSources(context)
    // The clone-safe projection of every plugin tool the worker may call. The
    // `execute` stays on the hub (this worker's control port reaches the
    // `pluginTool.execute` authority above); only the descriptor crosses.
    const pluginTools = await resolveContextPluginToolDescriptors(context)
    const initOptions: BuildSessionWorkerInitOptions = {
      ...deps.initOptions,
      // The session's persisted model is authoritative; config/default only
      // applies when the dispatch carried no session model.
      model: model === undefined ? deps.initOptions?.model : { ...deps.initOptions?.model, session: model },
      snapshot: { ...deps.initOptions?.snapshot, workerCount: deps.count, epochSequence: deps.epochSequence },
      plugins:
        pluginSources === undefined
          ? deps.initOptions?.plugins
          : (resolvedModel) =>
              resolveWorkerPlugins({
                model: resolvedModel,
                sources: pluginSources,
                sessionID: session,
                ...(request ?? {}),
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
  const copyContextToChild = (childID: SessionID, parentID: SessionID): void => {
    // A subagent child is dispatched by the raw scheduler, so `recordDispatch`
    // never runs for it. Copy the parent's captured per-session instance/context
    // onto the child BEFORE its worker materialises, so `resolveInit(child)`
    // binds the parent's directory (not the hub cwd) and the child's per-worker
    // hub-proxy handlers run under a live `InstanceRef`. Nested subagents inherit
    // recursively: a child's own entry is copied here when IT dispatches a
    // grandchild.
    const instance = instanceBySession.get(parentID)
    if (instance !== undefined) instanceBySession.set(childID, instance)
    const context = contextBySession.get(parentID)
    if (context !== undefined) contextBySession.set(childID, context)
  }
  return {
    events,
    eventsForSession,
    eventsForWorker,
    hubProxyFor,
    hubProxyStreamingFor,
    abortPluginTools: (sessionID) => pluginToolAborts.abortSession(sessionID),
    instanceOfSessionDeep,
    resolveInit,
    recordDispatch,
    copyContextToChild,
    releaseSession,
    clearSessions,
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
      // The resolved nesting limit the hub enforces for a worker's
      // `dispatchSubagent` RPC; the hub is the admission authority, so it reads
      // the config once at layer build rather than trusting the worker. A
      // missing or throwing Config must not fail layer construction (the hub
      // falls back to the documented `1`), so the read is tolerant: `catchCause`
      // also absorbs the sync-throw / `UnimplementedError` defect a mock Config
      // raises, which `orElseSucceed` would not.
      const subagentDepth = yield* config.get().pipe(
        Effect.map((info) => info.subagent_depth ?? 1),
        Effect.catchCause(() => Effect.succeed(1)),
      )
      // The hub-proxy authorities (plan decision 64) a worker reaches over its
      // control port. The handlers run on the HUB, under the dispatching
      // instance's context (the hub runtime is global).
      const sessionStatus = yield* SessionStatus.Service
      const permission = yield* Permission.Service
      const question = yield* Question.Service
      // The opencode event boundary: worker events and `session.cancelled` are
      // republished live-only through it (never re-committed).
      const bridge = yield* EventV2Bridge.Service
      const count = options.count ?? resolveWorkerCount()
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
      // The hub is assigned after the pool is built; the resolver's per-worker
      // lookups run lazily on the first port attach (post-assignment), so the
      // deferred reference is safe.
      let hubRef: SessionHub | undefined
      // The per-session resolvers (the session → instance/context registry, the
      // live publishers, and the per-worker hub-proxy handlers and init) live in
      // one factory; the layer body only wires them to the hub.
      const resolvers = createPerSessionResolvers({
        services: { config, auth, modelsDev, provider, flags },
        bridge,
        authorities: { sessionStatus, permission, question },
        count,
        epochSequence,
        initOptions: options.initOptions,
        plugins: options.plugins,
        log,
        parentOf: (sessionID) => hubRef?.jobs.get(sessionID)?.parentID as SessionID | undefined,
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
          }))
      const hub = createProductionSessionHub({
        ...options,
        count,
        init: resolvers.resolveInit,
        ports,
        events: resolvers.events,
        eventPublisherFor: resolvers.eventsForSession,
        log,
        subagentDepth,
        onSessionRebound: (sessionID) => {
          // The rebinding worker's old session is done with its isolate; release
          // its state so the maps cannot grow without bound.
          resolvers.releaseSession(sessionID)
          options.onSessionRebound?.(sessionID)
        },
        onDispatch: resolvers.recordDispatch,
        onSubagentDispatch: resolvers.copyContextToChild,
        onCancelRequested: (sessionID) => resolvers.abortPluginTools(sessionID),
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
