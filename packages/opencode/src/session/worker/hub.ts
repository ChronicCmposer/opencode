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
import os from "node:os"
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
import type { EventV2Event, EventsAckPayload, EventsResyncPayload } from "./frame"
import { createLiveEventPublisher, instanceLocation, EventV2Bridge, type LiveEventPublisher } from "@/event-v2-bridge"
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
import type { SystemTransformInput, SystemTransformOutput, WorkerPluginHooks } from "./plugin-host"
import type { HubProxyFrame, HubProxyHandlers, HubProxyReply } from "./layer"
import { portFromMessagePort } from "./port"
import type { SessionWorkerInit } from "./session-worker"
import { noopWorkerLogger, workerLogMessage, workerLoggerFromBridge, type WorkerLogger } from "./log"
import { sessionThreadName } from "@/session/worker/thread-name"
import { Session } from "@/session/session"

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
  startShell(sessionID: SessionID): Promise<SessionExecutionResult>
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
  readonly headroom?: number
  readonly admissionCapacity?: number
  readonly childCapacity?: number
  readonly headroomIdleReapMs?: number
  readonly cancellation?: CancellationSource
  readonly onDegraded?: (workerID: number) => void
  /**
   * Live-only event publisher. When present, the hub republishes worker events
   * from their port and publishes `session.cancelled` after a cancel.
   */
  readonly events?: LiveEventPublisher
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
 * Hub side: serve hub-proxy requests against `handlers`; returns a disposer.
 * Mirrors `layer.ts`'s `serveHubProxy` (imported type-only to avoid the
 * `hub → layer → run-state → hub` import cycle).
 */
function serveHubProxyPort(handlers: HubProxyHandlers, port: MessagePort): () => void {
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
        (error) =>
          reply({
            kind: "reply",
            id: frame.id,
            ok: false,
            message: error instanceof Error ? error.message : String(error),
          }),
      )
  })
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
      const { generation, ordinal } = frame.payload
      if (generation !== undefined && ordinal !== undefined) {
        onDurable({ meta: { generation, ordinal }, event: frame.payload.event })
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
}

/**
 * Executes a hub authority effect for a worker request. The hub is a single
 * global runtime while a worker belongs to the instance that dispatched it, so
 * the runner provides that instance's context (captured at dispatch).
 */
export type HubProxyRunner = <A, E>(effect: Effect.Effect<A, E>) => Promise<A>

/** Whether a hub-proxy request payload carries an array field (Question vs Permission). */
function hasArrayField(input: unknown, field: string): boolean {
  if (typeof input !== "object" || input === null) return false
  return Array.isArray((input as Record<string, unknown>)[field])
}

/**
 * Build the hub-side handler map for the worker hub-proxy wire. `SessionStatus`
 * owns `get`/`set`; `Question` owns `reject`; `ask`/`reply` are shared with
 * `Permission` and disambiguated by request shape (a Question request carries
 * `questions`/`answers`). `list` is shared by all three, which the single
 * multiplexed port cannot discriminate — the worker session loop never lists
 * (the UI lists on the hub), so it routes to `SessionStatus` so a worker call
 * resolves rather than hangs.
 */
export function buildHubProxyHandlers(authorities: HubProxyAuthorities, run: HubProxyRunner): HubProxyHandlers {
  return {
    get: (sessionID) => run(authorities.sessionStatus.get(sessionID as SessionID)),
    set: (sessionID, status) =>
      run(authorities.sessionStatus.set(sessionID as SessionID, status as SessionStatus.Info)),
    list: () => run(authorities.sessionStatus.list()).then((entries) => [...entries]),
    ask: (input) =>
      hasArrayField(input, "questions")
        ? run(authorities.question.ask(input as Parameters<Question.Interface["ask"]>[0]))
        : run(authorities.permission.ask(input as Parameters<Permission.Interface["ask"]>[0])),
    reply: (input) =>
      hasArrayField(input, "answers")
        ? run(authorities.question.reply(input as Parameters<Question.Interface["reply"]>[0]))
        : run(authorities.permission.reply(input as Parameters<Permission.Interface["reply"]>[0])),
    reject: (requestID) => run(authorities.question.reject(requestID as Parameters<Question.Interface["reject"]>[0])),
    // Always served: a worker's `experimental.chat.system.transform` reaches the
    // hub, which runs the plugin hook and returns the mutated `{system}`. Absent
    // an authority the output passes through unchanged (never an unknown method).
    systemTransform: (input, output) =>
      authorities.systemTransform === undefined
        ? (output as SystemTransformOutput)
        : authorities.systemTransform(input as SystemTransformInput, output as SystemTransformOutput),
    ...(authorities.dispatchSubagent === undefined
      ? {}
      : {
          dispatchSubagent: (request) => authorities.dispatchSubagent!(request as SubagentDispatchRequest),
        }),
  }
}

/** Compose the dispatcher + executor pool + cooperative cancel into one hub. */
export function createSessionHub(options: SessionHubOptions): SessionHub {
  const log = options.log ?? noopWorkerLogger
  // One registry for the whole hub. A settled child is routed to the worker
  // currently hosting its `parentID` (the dispatcher's key→worker map).
  const jobPorts = new Map<number, JobPort>()
  const jobDisposers = new Map<number, () => void>()
  // Worker writer/egress/broker and hub-proxy port servers, keyed by worker id.
  const workerPortDisposers = new Map<number, () => void>()
  const hubProxyDisposers = new Map<number, () => void>()
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
  // Assigned once the pool exists; read lazily by the routing closures below.
  let dispatcher: Dispatcher

  const workerForSession = (sessionID: string): number | undefined =>
    dispatcher.workerForSession(sessionID as SessionID)
  const portForSession = (sessionID: string): JobPort | undefined => {
    const workerID = workerForSession(sessionID)
    return workerID === undefined ? undefined : jobPorts.get(workerID)
  }
  const jobs = options.jobs ?? createJobRegistry({ routeResult: createResultRouter(portForSession) })

  const attachJobPort = (workerID: number, port: JobPort): (() => void) => {
    log(workerLogMessage("ports connected"), { level: "INFO", workerID, ports: ["job"] })
    jobDisposers.get(workerID)?.()
    jobPorts.set(workerID, port)
    const disposeServer = serveJobRegistry(jobs, port)
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
    const backends = portBackends()
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
    const handlers = hubProxyHandlers()
    if (handlers !== undefined) disposers.push(serveHubProxyPort(handlers, port))
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

  /**
   * The served hub-proxy handler map: the caller's three authorities plus the
   * pool-aware `dispatchSubagent` RPC. The handler closes over the registry and
   * dispatcher, both materialised by attach time, so it is built lazily. A
   * caller-supplied `dispatchSubagent` authority (a test) wins.
   */
  function hubProxyHandlers(): HubProxyHandlers | undefined {
    const base = portBackends()?.hubProxy
    if (base === undefined) return undefined
    const dispatchSubagent = (...args: readonly unknown[]): Promise<JobInfo> =>
      dispatchSubagentToHub(
        {
          registry: jobs,
          scheduler: dispatcher,
          depthLimit: options.subagentDepth ?? 1,
          headroomFull: () => dispatcher.headroomSlotCount >= dispatcher.headroomCapacity,
          resultFor: (sessionID) => pool.resultFor(sessionID),
        },
        args[0] as SubagentDispatchRequest,
      )
    return { ...base, dispatchSubagent: base.dispatchSubagent ?? dispatchSubagent }
  }

  // Materialise the pool, wiring its transferred job ports into `attachJobPort`.
  const onJobPort = (workerID: number, port: MessagePort): void => {
    attachJobPort(workerID, jobPortFromMessagePort(port))
  }
  const pool = typeof options.pool === "function" ? options.pool(onJobPort) : options.pool
  const resolvedCancellation =
    typeof options.cancellation === "function" ? options.cancellation(pool) : options.cancellation
  // Every terminal cancel mode the dispatcher reports, keyed by session id. The
  // dispatcher cascades children-first but discards a descendant's own outcome,
  // so the hub records the modes here and reconstructs which descendants still
  // need a fallback idle (see `cancelWithReport`).
  const cancelledModes = new Map<string, CancelledMode>()
  // The hub's job registry is the cross-worker parent→child authority (9.4), so
  // an unwired cancellation config still cascades to subagents and still marks a
  // cancelled job. An explicit config always wins.
  const childrenOf: (sessionID: SessionID) => ReadonlyArray<SessionID> =
    resolvedCancellation?.childrenOf ?? ((sessionID) => jobs.childrenOf(sessionID).map((child) => child as SessionID))
  const baseOnCancelled = resolvedCancellation?.onCancelled
  const cancellation: CancellationConfig | undefined =
    resolvedCancellation === undefined
      ? undefined
      : {
          ...resolvedCancellation,
          childrenOf,
          onCancelled: (event) => {
            cancelledModes.set(event.sessionID, event.mode)
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

  /**
   * Every descendant of `sessionID` (children-first, depth-first), as the
   * dispatcher's own cascade would visit them. A linked child that was never
   * admitted is included, so it still receives a fallback idle.
   */
  const descendantsOf = (sessionID: SessionID): SessionID[] => {
    const descendants: SessionID[] = []
    const visit = (id: SessionID): void => {
      for (const child of childrenOf(id)) {
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

    // The dispatcher cascades to the descendants itself (children-first); the
    // recorded modes below reconstruct each descendant's settlement.
    const outcome = await dispatcher.cancelWithGrace(sessionID)

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
      options.events?.publishDefinition(SessionV1.Event.Cancelled, {
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
    log(workerLogMessage("cancel cascade"), {
      level: "INFO",
      sessionID,
      descendants: reportDescendants.length,
      needsFallbackIdle: reportDescendants.filter((descendant) => descendant.needsFallbackIdle).length,
    })
    return { outcome, descendants: reportDescendants }
  }

  return {
    dispatcher,
    jobs,
    attachJobPort,
    attachWorkerPorts,
    attachHubProxyPort,
    ensureRunning: (sessionID) => {
      onDispatch?.(sessionID)
      return dispatch.ensureRunning(sessionID)
    },
    startShell: (sessionID) => {
      onDispatch?.(sessionID)
      return dispatch.startShell(sessionID)
    },
    assertNotBusy: (sessionID) => dispatch.assertNotBusy(sessionID),
    cancel: async (sessionID) => (await cancelWithReport(sessionID)).outcome,
    cancelWithReport,
    cancelAll: () => dispatch.cancelAll(),
    async terminateAll() {
      for (const dispose of jobDisposers.values()) dispose()
      for (const dispose of workerPortDisposers.values()) dispose()
      for (const dispose of hubProxyDisposers.values()) dispose()
      await dispatcher.terminateAll()
      // Only tear down backends we actually built (a source never attached to is
      // never constructed, so no isolate is spawned just to dispose it).
      const backends = backendsResolved ? resolvedBackends : typeof portSource === "function" ? undefined : portSource
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

  const failWaiters = (error: Error): void => {
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
    messages.push(message)
    if (messages.length > bufferCapacity) messages.shift()
    for (const waiter of [...waiters]) {
      const index = messages.indexOf(message)
      // A prior waiter consumed this frame; no other waiter may match it.
      if (index === -1) break
      if (!waiter.predicate(message)) continue
      messages.splice(index, 1)
      if (waiter.timer !== undefined) clearTimeout(waiter.timer)
      waiters.delete(waiter)
      waiter.resolve(message)
    }
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
 * Wrap a worker factory so the underlying isolate is created on first use. The
 * dispatcher reads `alive`/builds slots at construction, so a lazy worker reports
 * itself alive (and free) before it exists; the isolate appears on the first
 * `execute`. `cancel`/`assertNotBusy`/`resultFor` before materialisation are
 * no-ops, which is correct because no session can be running on a worker that was
 * never started. The factory may be async (the production init is resolved on
 * first dispatch).
 */
export function createLazyExecutorWorker(
  id: number,
  create: LazyExecutorWorkerFactory,
  log: WorkerLogger = noopWorkerLogger,
): ExecutorWorker {
  let inner: ExecutorWorker | undefined
  let materializing: Promise<ExecutorWorker> | undefined
  let terminated = false
  const materialize = async (): Promise<ExecutorWorker> => {
    if (terminated) throw new Error(`executor worker ${id} is terminated`)
    if (inner !== undefined) return inner
    materializing ??= Promise.resolve()
      .then(create)
      .then((worker) => {
        // A terminate raced the materialisation: never publish a live worker.
        if (terminated) {
          void worker.terminate()
          throw new Error(`executor worker ${id} is terminated`)
        }
        inner = worker
        return worker
      })
    return materializing
  }
  const worker: ExecutorWorker = {
    id,
    get alive() {
      return !terminated && (inner === undefined || inner.alive)
    },
    execute: async (run) => (await materialize()).execute(run),
    cancel: (sessionID) => (inner === undefined ? Promise.resolve() : inner.cancel(sessionID)),
    assertNotBusy: (sessionID) => (inner === undefined ? Promise.resolve(false) : inner.assertNotBusy(sessionID)),
    resultFor: (sessionID) => inner?.resultFor(sessionID),
    cancelPort: createExecutorCancelPort({ cancel: (sessionID) => worker.cancel(sessionID), log }),
    terminate: async () => {
      terminated = true
      materializing = undefined
      const current = inner
      inner = undefined
      if (current !== undefined) await current.terminate()
    },
  }
  return worker
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
  readonly onJobPort?: (workerID: number, port: MessagePort) => void
  readonly onWorkerPorts?: (workerID: number, ports: WorkerTransferredPorts) => void
  readonly onHubProxyPort?: (workerID: number, port: MessagePort) => void
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
): ExecutorWorker {
  let inner: ExecutorWorker | undefined
  let boundSession: SessionID | undefined
  let materializing: Promise<ExecutorWorker> | undefined
  let terminated = false

  const materialize = (sessionID: SessionID): Promise<ExecutorWorker> => {
    if (terminated) throw new Error(`executor worker ${id} is terminated`)
    if (inner !== undefined && boundSession === sessionID) return Promise.resolve(inner)
    const previousSession = boundSession
    const previous = inner
    inner = undefined
    boundSession = sessionID
    // A worker rebinding from one session to another is the observable signal
    // that the isolate is being retired and rebuilt for a different model.
    if (previousSession !== undefined && previousSession !== sessionID) {
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
    materializing = Promise.resolve()
      // Retire the old isolate first so two models never share one worker.
      .then(() => previous?.terminate().catch(() => undefined))
      .then(() => spawnFor(id, sessionID))
      .then((worker) => {
        if (terminated) {
          void worker.terminate()
          throw new Error(`executor worker ${id} is terminated`)
        }
        inner = worker
        return worker
      })
    return materializing
  }

  const worker: ExecutorWorker = {
    id,
    get alive() {
      return !terminated && (inner === undefined || inner.alive)
    },
    execute: async (run) => (await materialize(run.sessionID)).execute(run),
    cancel: (sessionID) => (inner === undefined ? Promise.resolve() : inner.cancel(sessionID)),
    assertNotBusy: (sessionID) => (inner === undefined ? Promise.resolve(false) : inner.assertNotBusy(sessionID)),
    resultFor: (sessionID) => inner?.resultFor(sessionID),
    cancelPort: createExecutorCancelPort({ cancel: (sessionID) => worker.cancel(sessionID), log }),
    terminate: async () => {
      terminated = true
      materializing = undefined
      const current = inner
      inner = undefined
      if (current !== undefined) await current.terminate()
    },
  }
  return worker
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
    spawn: (id) => createSessionBoundExecutorWorker(id, spawnFor, log),
    log,
  })
}

/** A pool factory suitable for `SessionHubOptions.pool` (wires job ports). */
export function productionWorkerPoolFactory(
  options: Omit<ProductionWorkerPoolOptions, "onJobPort">,
): ExecutorWorkerPoolFactory {
  return (onJobPort) => createProductionWorkerPool({ ...options, onJobPort })
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
  return {
    directory,
    worktree: directory,
    project: {
      id: Project.ID.global,
      worktree: directory,
      time: { created: 0, updated: 0 },
      sandboxes: [],
    },
  }
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

/**
 * Resolve the dispatching session's persisted provider/model pair from the
 * caller context captured at dispatch. The hub node deliberately does not declare
 * `Session` as a dependency (the graph stays stable), so it is read optionally:
 * when the ambient session store is absent (or the session has no model yet) the
 * worker falls back to the config/default model exactly as before.
 */
async function resolveDispatchedSessionModel(
  context: Context.Context<never> | undefined,
  sessionID: SessionID | undefined,
): Promise<WorkerModelSelection | undefined> {
  if (context === undefined || sessionID === undefined) return undefined
  const session = Context.getOption(context, Session.Service)
  if (Option.isNone(session)) return undefined
  const info = await Effect.runPromise(
    session.value.get(sessionID).pipe(Effect.match({ onFailure: () => undefined, onSuccess: (value) => value })),
  )
  const model = info?.model
  if (model === undefined) return undefined
  return { providerID: model.providerID, modelID: model.id }
}

/** The per-request plugin input the hub can resolve before a worker runs. */
interface DispatchedSessionRequest {
  readonly agent?: string
  readonly message?: unknown
}

/**
 * Resolve the dispatching session's per-request plugin input — its agent and the
 * last user message — from the caller context captured at dispatch, mirroring
 * {@link resolveDispatchedSessionModel}. `Session` is read optionally (the graph
 * stays stable), so `undefined` means the ambient session store is absent and the
 * hooks keep the documented empty fallback.
 *
 * This is the latest the hub can resolve: a worker binds to one session at init,
 * so the last user message at bind time is the message the first turn processes.
 * A later turn on the SAME worker (no rebind) stays approximate until it rebinds.
 */
async function resolveDispatchedSessionRequest(
  context: Context.Context<never> | undefined,
  sessionID: SessionID | undefined,
): Promise<DispatchedSessionRequest | undefined> {
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
  if (agent === undefined && message === undefined) return undefined
  return {
    ...(agent === undefined ? {} : { agent }),
    ...(message === undefined ? {} : { message }),
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
    // The default egress backend serves the data port AND attaches the worker's
    // control plane to the shared mesh; both the receiver and the attachment are
    // disposed with the port. A durable event reaching the data port is published
    // to the mesh, waking other workers' `durable()` subscribers.
    egress:
      options.egress ??
      ((workerID, port, control) => {
        const attachment =
          control === undefined ? undefined : attachMeshWorker(egressMesh, String(workerID), control)
        const disposeReceiver =
          options.events === undefined
            ? drainEgressPort(port)
            : serveEventsPort(
                port,
                createHubEventSink({ publisher: options.events }),
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
  /** Live-only event publisher (worker republish + `session.cancelled`). */
  readonly events?: LiveEventPublisher
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

/** Compose the production pool + hub, with cooperative cancellation wired. */
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
    ...defined({ ports: options.ports, onDispatch: options.onDispatch, events: options.events, onPidReport }),
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
      // Captured synchronously at the start of the first dispatch (below).
      let capturedInstance: InstanceContext | undefined
      let capturedContext: Context.Context<never> | undefined
      // The session currently being dispatched. Updated on every dispatch; used
      // only as the fallback when a pool resolves an init without a session id
      // (the production pool passes the session it is binding a worker to).
      let dispatchedSession: SessionID | undefined
      // Live-only publisher: stamps the dispatching instance's location so a
      // republished event passes the instance `/event` SSE directory filter.
      const events = createLiveEventPublisher(bridge, {
        location: () => instanceLocation(capturedInstance),
      })
      // A hub authority effect is closed over its own services but reads the
      // ambient `InstanceRef`; run it under the dispatch context so it resolves.
      const runHubProxy: HubProxyRunner = (effect) => {
        const context = capturedContext
        return Effect.runPromise(context === undefined ? effect : Effect.provide(effect, context))
      }
      // The worker cannot run `experimental.chat.system.transform` itself: the hub
      // owns plugin loading (decision 70). Read `Plugin` from the dispatch context
      // and run the trigger under it, so the per-directory `InstanceState` the
      // plugin layer keys on is the dispatching instance's. Absent a context (or
      // plugin) the output passes through unchanged.
      const systemTransform: NonNullable<HubProxyAuthorities["systemTransform"]> = (input, output) => {
        const context = capturedContext
        const plugin = context === undefined ? Option.none() : Context.getOption(context, Plugin.Service)
        return Option.isNone(plugin)
          ? Promise.resolve(output)
          : runHubProxy(plugin.value.trigger("experimental.chat.system.transform", input, output))
      }
      const hubProxy = buildHubProxyHandlers({ sessionStatus, permission, question, systemTransform }, runHubProxy)
      // The production port authorities: supplied ready, or built on the first
      // dispatch (the source is only invoked when a worker attaches a port).
      const ports: WorkerPortBackendsSource =
        options.ports ??
        (() =>
          buildProductionPortBackends({
            ...options.backends,
            hubProxy: options.backends?.hubProxy ?? hubProxy,
            events,
          }))
      // Per-session: the pool passes the session it is building a worker for, so
      // this session's model (and plugin effect) is the one shipped, never the
      // first dispatched session's. `dispatchedSession` is the fallback for a
      // custom pool that resolves without a session id.
      const resolveInit = async (sessionID?: SessionID): Promise<SessionWorkerInit> => {
        const instance = capturedInstance ?? captureInstanceRef() ?? fallbackInstanceContext()
        const session = await resolveDispatchedSessionModel(capturedContext, sessionID ?? dispatchedSession)
        // The real per-request input (agent + last user message) the plugin hooks
        // resolve against, so a hook keyed on either matches flag-off rather than
        // seeing an empty stub.
        const request = await resolveDispatchedSessionRequest(capturedContext, sessionID ?? dispatchedSession)
        const pluginSources =
          options.plugins !== undefined ? await options.plugins() : await resolveContextPluginSources(capturedContext)
        // A non-serializable provider option or plugin hook is a configuration
        // defect: die loud rather than widen every graph's error channel with the
        // typed failure.
        return Effect.runPromise(
          buildSessionWorkerInit({
            ...options.initOptions,
            // The session's persisted model is authoritative; config/default only
            // applies when the dispatch carried no session model.
            model: session === undefined ? options.initOptions?.model : { ...options.initOptions?.model, session },
            snapshot: { ...options.initOptions?.snapshot, workerCount: count },
            plugins:
              pluginSources === undefined
                ? options.initOptions?.plugins
                : (model) =>
                    resolveWorkerPlugins({
                      model,
                      sources: pluginSources,
                      sessionID: sessionID ?? dispatchedSession,
                      ...(request ?? {}),
                    }),
          }).pipe(
            Effect.provideService(Config.Service, config),
            Effect.provideService(Auth.Service, auth),
            Effect.provideService(ModelsDev.Service, modelsDev),
            Effect.provideService(Provider.Service, provider),
            Effect.provideService(RuntimeFlags.Service, flags),
            Effect.provideService(InstanceRef, instance),
            Effect.orDie,
          ),
        )
      }
      // The production narrative seam: an EffectBridge (R/E = never, so it adds no
      // graph dependency) whose `fork` carries the ambient logger, mirroring the
      // test-only `createSessionHubLayer`. Without this the production graph passed
      // no `log`, so A1–A3 / B2/B3 / D3 / E1/E3 and everything forwarded through
      // dispatch/execute/cancel were dormant. `bridge` names the EventV2Bridge
      // above, hence `effectBridge`.
      const effectBridge = yield* EffectBridge.make()
      const log = workerLoggerFromBridge(effectBridge)
      const hub = createProductionSessionHub({
        ...options,
        count,
        init: resolveInit,
        ports,
        events,
        log,
        subagentDepth,
        onDispatch: (sessionID) => {
          capturedInstance ??= captureInstanceRef()
          capturedContext ??= captureRuntimeContext()
          // Overwrite: each dispatch's session is the current one. A worker
          // rebinds per session, so a stale value cannot leak a first session's
          // model into a later one.
          dispatchedSession = sessionID
        },
      })
      yield* Effect.addFinalizer(() => Effect.promise(() => hub.terminateAll().catch(() => undefined)))
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
