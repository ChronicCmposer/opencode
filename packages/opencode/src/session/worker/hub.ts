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

import { Context, Effect, Fiber, Layer } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { ModelsDev } from "@opencode-ai/core/models-dev"
import { Database } from "@opencode-ai/core/database/database"
import { Auth } from "@/auth"
import { Config } from "@/config/config"
import { Provider } from "@/provider/provider"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { InstanceRef } from "@/effect/instance-ref"
import { SessionStatus } from "@/session/status"
import { Permission } from "@/permission"
import { Question } from "@/question"
import type { InstanceContext } from "@/project/instance-context"
import { mergeEnv } from "@/session/broker/env"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import {
  createBroker,
  serveBrokerPort,
  type BrokerBackend,
  type BrokerClient,
  type BrokerOptions,
} from "@/session/broker/broker"
import { createEgressReceiver } from "@/session/egress/receiver"
import type { EventV2Event, EventsAckPayload, EventsResyncPayload } from "./frame"
import { createLiveEventPublisher, instanceLocation, EventV2Bridge, type LiveEventPublisher } from "@/event-v2-bridge"
import {
  createWriterClient,
  serveWriterPort,
  type HubWriter,
  type WriterClient,
  type WriterClientOptions,
} from "@/session/writer/client"
import type { SessionID } from "@/session/schema"
import { defined } from "@/util/object"
import { createDispatcher, type CancellationConfig, type Dispatcher, type SessionWorker } from "./dispatch"
import { createJobRegistry, type JobRegistry } from "./job-registry"
import { createResultRouter, jobPortFromMessagePort, serveJobRegistry, type JobPort } from "./job-client"
import {
  createExecutorCancelPort,
  createExecutorWorker,
  createExecutorWorkerPool,
  createSessionDispatch,
  type ExecutorTransport,
  type ExecutorWorker,
  type ExecutorWorkerPool,
  type SessionDispatch,
  type SessionExecutionResult,
  type SessionExecutorInbound,
  type SessionExecutorOutbound,
} from "./execute"
import { resolveCancelGraceMs, type CancelOutcome } from "./cancel"
import { resolveWorkerCount } from "./pool"
import { buildSessionWorkerInit, type BuildSessionWorkerInitOptions } from "./init"
import type { HubProxyFrame, HubProxyHandlers, HubProxyReply } from "./layer"
import { portFromMessagePort } from "./port"
import type { SessionWorkerInit } from "./session-worker"

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
   * before any async work. Used by the production hub to capture the caller's
   * ambient `InstanceRef` (the `attach` pattern) so the worker init is resolved
   * under the dispatching session's instance context.
   */
  readonly onDispatch?: () => void
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
}

/**
 * The worker currently hosting a session, as the dispatcher knows it. Declared
 * locally so the hub can consume it without importing the dispatcher's registry.
 * `dispatch.ts` must add this method to `Dispatcher` (consolidation request).
 */
interface WorkerLookup {
  workerForSession(sessionID: SessionID): number | undefined
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
 */
export function serveEventsPort(port: MessagePort, sink: HubEventSink, onError?: (error: Error) => void): () => void {
  const receiver = createEgressReceiver({
    port,
    onFrame: (frame) => {
      if (frame.kind === "event") sink.event(frame.payload.event)
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
  }
}

/** Compose the dispatcher + executor pool + cooperative cancel into one hub. */
export function createSessionHub(options: SessionHubOptions): SessionHub {
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

  const workerForSession = (sessionID: string): number | undefined => {
    const lookup = dispatcher as unknown as Partial<WorkerLookup>
    return (
      lookup.workerForSession?.(sessionID as SessionID) ?? dispatcher.registry.get(sessionID as SessionID)?.workerID
    )
  }
  const portForSession = (sessionID: string): JobPort | undefined => {
    const workerID = workerForSession(sessionID)
    return workerID === undefined ? undefined : jobPorts.get(workerID)
  }
  const jobs = options.jobs ?? createJobRegistry({ routeResult: createResultRouter(portForSession) })

  const attachJobPort = (workerID: number, port: JobPort): (() => void) => {
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

  // Serve the hub-proxy authorities over a worker's control port.
  const attachHubProxyPort = (workerID: number, port: MessagePort): (() => void) => {
    hubProxyDisposers.get(workerID)?.()
    const handlers = portBackends()?.hubProxy
    if (handlers === undefined) return () => {}
    const dispose = serveHubProxyPort(handlers, port)
    hubProxyDisposers.set(workerID, dispose)
    return dispose
  }

  // Materialise the pool, wiring its transferred job ports into `attachJobPort`.
  const onJobPort = (workerID: number, port: MessagePort): void => {
    attachJobPort(workerID, jobPortFromMessagePort(port))
  }
  const pool = typeof options.pool === "function" ? options.pool(onJobPort) : options.pool
  const cancellation = typeof options.cancellation === "function" ? options.cancellation(pool) : options.cancellation
  dispatcher = createDispatcher({
    pool,
    ...defined({
      headroom: options.headroom,
      admissionCapacity: options.admissionCapacity,
      childCapacity: options.childCapacity,
      headroomIdleReapMs: options.headroomIdleReapMs,
      cancellation,
      onDegraded: options.onDegraded,
    }),
  })
  const dispatch: SessionDispatch = createSessionDispatch({ dispatcher, pool })
  const onDispatch = options.onDispatch

  return {
    dispatcher,
    jobs,
    attachJobPort,
    attachWorkerPorts,
    attachHubProxyPort,
    ensureRunning: (sessionID) => {
      onDispatch?.()
      return dispatch.ensureRunning(sessionID)
    },
    startShell: (sessionID) => {
      onDispatch?.()
      return dispatch.startShell(sessionID)
    },
    assertNotBusy: (sessionID) => dispatch.assertNotBusy(sessionID),
    cancel: async (sessionID) => {
      const outcome = await dispatcher.cancelWithGrace(sessionID)
      // Report the terminal outcome to every SSE client. `SessionV1.Event.Cancelled`
      // is non-durable, and the live-only publisher refuses a durable definition, so
      // this can never re-commit the session state already written through the writer.
      if (outcome !== undefined) {
        options.events?.publishDefinition(SessionV1.Event.Cancelled, {
          sessionID: outcome.sessionID,
          mode: outcome.mode,
          durationMs: outcome.durationMs,
          acknowledged: outcome.acknowledged,
        })
      }
      return outcome
    },
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
  readonly timeoutMs?: number
  /** Maximum buffered outbound frames kept for late `waitFor` matches. */
  readonly bufferCapacity?: number
  /** Called with each `MessagePort` the worker transfers (the job control link). */
  readonly onPort?: (port: MessagePort) => void
  /** Called with the worker's transferred writer / egress / broker ports. */
  readonly onWorkerPorts?: (ports: WorkerTransferredPorts) => void
  /** Called with the worker's transferred hub-proxy control port. */
  readonly onHubProxyPort?: (port: MessagePort) => void
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
  readonly timer: ReturnType<typeof setTimeout>
}

/**
 * A `MessagePort`-free `ExecutorTransport` over a real Bun worker: buffers every
 * outbound frame so a `waitFor` may match one that already arrived, and fails
 * loud on timeout or isolate error.
 */
export function createBunExecutorTransport(options: BunExecutorTransportOptions): ExecutorTransport {
  const timeoutMs = options.timeoutMs ?? DEFAULT_TRANSPORT_TIMEOUT_MS
  const bufferCapacity = options.bufferCapacity ?? DEFAULT_TRANSPORT_BUFFER_CAPACITY
  const worker = new Worker(options.entry, {
    env: mergeEnv(options.env),
    ...defined({ workerData: options.workerData }),
  })
  const messages: SessionExecutorOutbound[] = []
  const waiters = new Set<PendingWaiter>()
  let alive = true

  const failWaiters = (error: Error): void => {
    for (const waiter of waiters) {
      clearTimeout(waiter.timer)
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
      clearTimeout(waiter.timer)
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
    failWaiters(new Error(`session worker error: ${event.message}`))
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
        const timer = setTimeout(() => {
          waiters.delete(waiter)
          reject(new Error(`timed out waiting for a session worker reply after ${waitMs}ms`))
        }, waitMs)
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
  readonly workKind?: string
  /** Receive each worker's transferred job port, keyed by worker id (plan 9.2). */
  readonly onJobPort?: (workerID: number, port: MessagePort) => void
  /** Receive each worker's transferred writer / egress / broker ports (plan 8/17/19). */
  readonly onWorkerPorts?: (workerID: number, ports: WorkerTransferredPorts) => void
  /** Receive each worker's transferred hub-proxy control port (plan 24). */
  readonly onHubProxyPort?: (workerID: number, port: MessagePort) => void
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
      timeoutMs: options.timeoutMs,
      onPort: onJobPort === undefined ? undefined : (port: MessagePort) => onJobPort(id, port),
      onWorkerPorts:
        onWorkerPorts === undefined ? undefined : (ports: WorkerTransferredPorts) => onWorkerPorts(id, ports),
      onHubProxyPort: onHubProxyPort === undefined ? undefined : (port: MessagePort) => onHubProxyPort(id, port),
    }),
  })
  return createExecutorWorker({
    id,
    transport,
    ...defined({ workKind: options.workKind, timeoutMs: options.timeoutMs }),
  })
}

/** Production `ExecutorWorkerPool`: K executor workers speaking the frame protocol. */
export function createBunExecutorWorkerPool(options: BunExecutorWorkerPoolOptions): ExecutorWorkerPool {
  return createExecutorWorkerPool({ k: options.k, spawn: (id) => spawnBunExecutorWorker(id, options) })
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
export function createLazyExecutorWorker(id: number, create: LazyExecutorWorkerFactory): ExecutorWorker {
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
    cancelPort: createExecutorCancelPort({ cancel: (sessionID) => worker.cancel(sessionID) }),
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
}

/** A pool whose K foreground workers (and any headroom) spawn on first use. */
export function createLazyExecutorWorkerPool(options: LazyExecutorWorkerPoolOptions): ExecutorWorkerPool {
  return createExecutorWorkerPool({
    k: options.k,
    spawn: (id) => createLazyExecutorWorker(id, () => options.create(id)),
  })
}

// ---------------------------------------------------------------------------
// Production pool + hub factories (steps 10 + 13 + 14).
// ---------------------------------------------------------------------------

/** The production session-worker entry (the reduced-layer isolate). */
export const SESSION_WORKER_ENTRY = new URL("./session-worker.ts", import.meta.url)

/** A worker `workerData` payload, or a (possibly async) resolver for one. */
export type SessionWorkerInitSource = SessionWorkerInit | (() => SessionWorkerInit | Promise<SessionWorkerInit>)

export interface ProductionWorkerPoolOptions {
  readonly init: SessionWorkerInitSource
  readonly count: number
  /** Worker entry; defaults to {@link SESSION_WORKER_ENTRY} (overridable for tests). */
  readonly entry?: string | URL
  readonly env?: Record<string, string | undefined>
  readonly timeoutMs?: number
  readonly workKind?: string
  readonly onJobPort?: (workerID: number, port: MessagePort) => void
  readonly onWorkerPorts?: (workerID: number, ports: WorkerTransferredPorts) => void
  readonly onHubProxyPort?: (workerID: number, port: MessagePort) => void
}

/**
 * Production `ExecutorWorkerPool` over the session-worker entry. Lazy: building
 * the pool spawns no isolates. The init source is resolved once, on the first
 * dispatch, then reused as the `workerData` for every worker.
 */
export function createProductionWorkerPool(options: ProductionWorkerPoolOptions): ExecutorWorkerPool {
  const resolveSource = typeof options.init === "function" ? options.init : () => options.init as SessionWorkerInit
  let cached: Promise<SessionWorkerInit> | undefined
  const resolveInit = (): Promise<SessionWorkerInit> => (cached ??= Promise.resolve().then(resolveSource))
  return createLazyExecutorWorkerPool({
    k: options.count,
    create: async (id) => {
      const init = await resolveInit()
      return spawnBunExecutorWorker(id, {
        k: options.count,
        entry: options.entry ?? SESSION_WORKER_ENTRY,
        workerData: init,
        ...defined({
          env: options.env,
          timeoutMs: options.timeoutMs,
          workKind: options.workKind,
          onJobPort: options.onJobPort,
          onWorkerPorts: options.onWorkerPorts,
          onHubProxyPort: options.onHubProxyPort,
        }),
      })
    },
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
  resolveInit: () => Promise<SessionWorkerInit>,
  sinks: WorkerPoolSinks,
) => ExecutorWorkerPool

/** Mark a generation as serving the worker writer / egress / broker ports. */
function withSessionWorkerPorts(init: SessionWorkerInit): SessionWorkerInit {
  return { ...init, capabilities: { ...init.capabilities, sessionWorkerPorts: true } }
}

/**
 * Fallback instance context for a dispatch with no ambient `InstanceRef`.
 * `buildWorkerSnapshot` only reads `directory`; `worktree`/`project` are unused.
 */
function fallbackInstanceContext(): InstanceContext {
  const directory = process.cwd()
  return { directory, worktree: directory, project: undefined as never }
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

export interface WorkerCancellationOptions {
  readonly graceMs?: number
  readonly schedule?: CancellationConfig["schedule"]
  readonly reap?: (worker: SessionWorker) => void
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
 * first request. The writer isolate spawns with NO connection — every
 * `publish`/`query` before `admin.open` throws `"writer connection is not open"`
 * — so the port server would reject without this. Opening lazily keeps graph
 * build free of I/O; a failed open resets the memo so a later call can retry
 * (the isolate's own respawn replays the open once it has succeeded).
 */
function lazyOpenWriter(writer: WriterClient, filename: string): HubWriter {
  let opened: Promise<unknown> | undefined
  const ensureOpen = (): Promise<unknown> => {
    if (opened !== undefined) return opened
    opened = writer.open(filename).catch((error) => {
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
    lazyOpenWriter((ownedWriter = createWriterClient(options.writerOptions)), options.databasePath ?? Database.path())
  const broker: BrokerBackend = options.broker ?? (ownedBroker = createBroker(options.brokerOptions))
  return {
    writer,
    broker,
    ...(options.hubProxy === undefined ? {} : { hubProxy: options.hubProxy }),
    egress:
      options.egress ??
      ((_workerID, port) =>
        options.events === undefined
          ? drainEgressPort(port)
          : serveEventsPort(port, createHubEventSink({ publisher: options.events }))),
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
  readonly workKind?: string
  readonly env?: Record<string, string | undefined>
  readonly headroom?: number
  readonly admissionCapacity?: number
  readonly childCapacity?: number
  /** Cancellation tuning; cooperative cancel is always wired (step 13). */
  readonly cancellation?: WorkerCancellationOptions
  readonly onDegraded?: (workerID: number) => void
  /**
   * Worker-port backends (plan tasks 8/17/19/24), ready or lazily built. When
   * present the resolved init advertises `capabilities.sessionWorkerPorts` and the
   * hub serves the ports.
   */
  readonly ports?: WorkerPortBackendsSource
  /** Invoked synchronously at the start of each dispatch (see `SessionHubOptions`). */
  readonly onDispatch?: () => void
  /** Live-only event publisher (worker republish + `session.cancelled`). */
  readonly events?: LiveEventPublisher
  /** Test seam: build the pool from the lazy init resolver. Defaults to the production pool. */
  readonly poolFactory?: ProductionPoolFactory
}

/** Compose the production pool + hub, with cooperative cancellation wired. */
export function createProductionSessionHub(options: ProductionSessionHubOptions): SessionHub {
  // The hub is assigned after the pool is built; port transfers arrive only on
  // dispatch (post-assignment), so the deferred reference is safe.
  let hub: SessionHub | undefined
  const sinks: WorkerPoolSinks = {
    onJobPort: (workerID, port) => hub?.attachJobPort(workerID, jobPortFromMessagePort(port)),
    onWorkerPorts: (workerID, ports) => hub?.attachWorkerPorts(workerID, ports),
    onHubProxyPort: (workerID, port) => hub?.attachHubProxyPort(workerID, port),
  }
  const resolveSource = typeof options.init === "function" ? options.init : () => options.init as SessionWorkerInit
  const resolveInit: () => Promise<SessionWorkerInit> =
    options.ports === undefined
      ? async () => resolveSource()
      : async () => withSessionWorkerPorts(await resolveSource())
  const pool =
    options.poolFactory !== undefined
      ? options.poolFactory(resolveInit, sinks)
      : createProductionWorkerPool({
          init: resolveInit,
          count: options.count ?? resolveWorkerCount(),
          ...defined({
            entry: options.entry,
            timeoutMs: options.timeoutMs,
            workKind: options.workKind,
            env: options.env,
          }),
          ...sinks,
        })
  hub = createSessionHub({
    pool,
    ...defined({ ports: options.ports, onDispatch: options.onDispatch, events: options.events }),
    cancellation: (pool) => createWorkerCancellation(pool, options.cancellation ?? {}),
    ...defined({
      headroom: options.headroom,
      admissionCapacity: options.admissionCapacity,
      childCapacity: options.childCapacity,
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
}

/**
 * Provide `SessionHubService` from the production hub, terminating it on scope
 * close. The `workerData` init is resolved on the FIRST dispatch (never at graph
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
      const hubProxy = buildHubProxyHandlers({ sessionStatus, permission, question }, runHubProxy)
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
      const resolveInit = (): Promise<SessionWorkerInit> => {
        const instance = capturedInstance ?? captureInstanceRef() ?? fallbackInstanceContext()
        // A non-serializable provider option is a configuration defect: die loud
        // rather than widen every graph's error channel with the typed failure.
        return Effect.runPromise(
          buildSessionWorkerInit({
            ...options.initOptions,
            snapshot: { ...options.initOptions?.snapshot, workerCount: count },
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
      const hub = createProductionSessionHub({
        ...options,
        count,
        init: resolveInit,
        ports,
        events,
        onDispatch: () => {
          capturedInstance ??= captureInstanceRef()
          capturedContext ??= captureRuntimeContext()
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
      const hub = createSessionHub(options)
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
