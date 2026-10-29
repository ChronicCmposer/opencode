// Production hub layer/node/resolver wiring.
import { Context, Deferred, Effect, Fiber, Layer, Logger, Option, Scope } from "effect"
import os from "node:os"
import path from "node:path"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { ModelsDev } from "@opencode-ai/core/models-dev"
import { Database } from "@opencode-ai/core/database/database"
import { openConnection } from "@opencode-ai/core/database/connection"
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
import { Project } from "@opencode-ai/schema/project"
import { attachMeshWorker, createEgressMesh, type EgressMesh } from "@/session/egress/mesh"
import { createLiveEventPublisher, instanceLocation, EventV2Bridge, type LiveEventPublisher } from "@/event-v2-bridge"
import type { EventV2 } from "@opencode-ai/core/event"
import { DurableLog, StateCache } from "@opencode-ai/core/durable-log"
import type { SessionID } from "@/session/schema"
import { DbExecutor } from "./db-executor"
import { spawnDepth } from "@/session/depth"
import { defined } from "@/util/object"
import { setThreadName } from "@/util/thread-name"
import { type CancellationConfig, type DispatcherOptions, type SessionWorker } from "./dispatch"
import { createLifecycleTracker } from "@/session/ledger/lifecycle"
import { RunBarrier, type RunBarrierPromotionOutcome } from "@/session/run-barrier"
import { type ExecutorWorkerPool } from "./execute"
import { resolveCancelGraceMs, type BootTelemetry } from "./cancel"

import { createEpochSequence, makeInstanceContext, type EpochSequence } from "./snapshot"
import {
  buildSessionWorkerInit,
  resolveWorkerPlugins,
  type BuildSessionWorkerInitOptions,
  type WorkerModelSelection,
  type WorkerPluginSource,
} from "./init"
import { createPidRegistry, reapSupport, registerReported, type PidRegistry, type PidReport } from "./pid-registry"
import { Plugin } from "@/plugin"
import type { WorkerPluginHooks } from "./plugin-host"
import type { HubProxyHandlers, HubProxyStreamHandler } from "./hub-proxy-wire"
import type { SessionWorkerInit } from "./session-worker"
import { workerLogMessage, WorkerLogging } from "./worker-logging"
import { Session } from "@/session/session"
import { recordAsync } from "@/session/ledger/crash"
import { ensureProductionSchemaAsync } from "@/session/db/schema"

import {
  createDurableHealReader,
  createEgressDeliveryBarrier,
  createEventBlobResolver,
  createHubEventSink,
  createSessionHub,
  serveEventsPort,
  type DurableHealReader,
  type EgressDeliveryBarrier,
  type EventBlobResolver,
  type HubCacheInvalidation,
  type HubEventSink,
  type HubTruthCache,
  type SessionHub,
  type SessionHubOptions,
  type WorkerCrashed,
  type WorkerPortSeams,
} from "./hub"
import {
  buildHubProxyHandlers,
  buildHubProxyStreamHandlers,
  createPluginToolAbortRegistry,
  createWorkerJobMirror,
  hubProxyAuthoritiesFor,
  messagesTransformAuthority,
  pluginTriggerAuthority,
  pluginToolAuthority,
  resolveContextHubToolDescriptors,
  type HubProxyAuthorities,
  type HubProxyRunner,
  type PluginToolAuthority,
} from "./hub-proxy-authorities"
import {
  DEFAULT_SPAWN_GATE_TIMEOUT_MS,
  describeSessionWorkerEntry,
  liveSessionWorkerCount,
  logRunIdleTimeoutWarning,
  makeProduction,
  makeSpawnGate,
  resolveRunIdleTimeoutMs,
  withSessionWorkerPorts,
  type DescribedSessionWorkerEntry,
  type Interface as WorkerPoolsInterface,
  type ProductionPoolFactory,
  type ProductionWorkerPoolOptions,
  type SessionWorkerInitSource,
  type SpawnGateHandle,
  type WorkerPoolSinks,
} from "./worker-pools"
import {
  drainEgressPort,
  WorkerPorts as WorkerPortsModule,
  type WorkerPortBackends,
  type WorkerPortBackendsSource,
  type WorkerPorts as WorkerPortsInterface,
  type WorkerTransferredPorts,
} from "./worker-ports"
import { SessionHubService } from "./hub"
import { WorkerRuntime } from "./runtime-edge"
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
  const info = await WorkerRuntime.runPromise(
    session.value.get(sessionID).pipe(Effect.match({ onFailure: () => undefined, onSuccess: (value) => value })),
  )
  // `findMessage` searches newest-first, so this is the message being processed.
  const lastUser = await WorkerRuntime.runPromise(
    session.value
      .findMessage(sessionID, (message) => message.info.role === "user")
      .pipe(Effect.match({ onFailure: () => Option.none(), onSuccess: (value) => value })),
  )
  const message = Option.isSome(lastUser) ? lastUser.value.info : undefined
  // The user message carries the per-request agent; the session's persisted agent
  // is the fallback when the message is absent.
  const agent = (message?.role === "user" ? message.agent : undefined) ?? info?.agent
  // The dispatching user message carries the model this turn was requested with.
  // It is newer than `session.model` (which is updated asynchronously), so a
  // first turn after a model switch must key the generation on it.
  const userModel = message?.role === "user" ? message.model : undefined
  const model =
    userModel !== undefined
      ? {
          providerID: userModel.providerID,
          modelID: String("modelID" in userModel ? userModel.modelID : (userModel as { readonly id?: string }).id),
        }
      : info?.model === undefined
        ? undefined
        : { providerID: info.model.providerID, modelID: info.model.id }
  return {
    ...(model === undefined ? {} : { model }),
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
  const hooks = await WorkerRuntime.runPromise(Effect.provide(plugin.value.list(), context))
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
 * cooperative `cancelPort`, and the grace defaults to
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
  /**
   * Observe egress receiver failures. A rejected events frame (invalid JSON, a
   * non-string message, an oversized frame, or a durable ordering violation)
   * closes the port; the receiver must never stop delivery silently, so it is
   * logged here with the worker/session context.
   */
  readonly bridge?: EffectBridge.Shape
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
  /**
   * Observe the events port's durable/ack progress for the per-run egress
   * barrier. The production layer supplies its tracker so `egressBarrier` waits
   * on a real acknowledgement instead of an absent hook.
   */
  readonly egressDelivery?: EgressDeliveryBarrier
  /**
   * The session a worker is bound to. Egress progress is keyed by session; a
   * worker with no binding yet records nothing rather than inventing an owner.
   */
  readonly sessionOfWorker?: (workerID: number) => SessionID | undefined
  /**
   * Resolve an oversized durable event body from the shared blob before the
   * events port republishes it. The production layer supplies an off-loop
   * executor resolver; absent means a position reference is healed by a typed
   * resync instead of ever reaching the live publisher.
   */
  readonly resolveBlob?: EventBlobResolver
  /**
   * Read a missed durable range from the committed log so a worker gap is
   * healed on the live (SSE) surface, not just counted. The production layer
   * supplies a reader over the hub's off-loop SQL executor.
   */
  readonly readDurable?: DurableHealReader
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
  const bridge = options.bridge
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
        if (publisher === undefined) {
          // No live publisher: the port is only drained, so there is no decoded
          // ack stream to observe. The production layer always supplies one.
          const disposeDrain = drainEgressPort(port)
          return () => {
            disposeDrain()
            attachment?.dispose()
          }
        }
        const boundSession = (): SessionID | undefined => options.sessionOfWorker?.(workerID)
        const sink = createHubEventSink({
          publisher,
          ...(options.cache === undefined ? {} : { cache: options.cache }),
          ...(options.resolveBlob === undefined ? {} : { resolveBlob: options.resolveBlob }),
          ...(options.readDurable === undefined ? {} : { readDurable: options.readDurable }),
        })
        // Wrap the sink so the egress barrier sees the SAME ordered stream the
        // receiver decodes: durable frames first, then their ack.
        const observedSink: HubEventSink = {
          ...(sink.resolveBlob === undefined ? {} : { resolveBlob: sink.resolveBlob }),
          ...(sink.heal === undefined ? {} : { heal: sink.heal }),
          event(event, meta) {
            sink.event(event, meta)
            if (meta?.durable !== true) return
            const bound = boundSession()
            if (bound !== undefined) options.egressDelivery?.observeDurable(bound)
          },
          ack(payload) {
            sink.ack(payload)
            const bound = boundSession()
            if (bound !== undefined) options.egressDelivery?.observeAck(bound, payload?.count)
          },
          end: () => sink.end(),
          resync: (payload) => sink.resync(payload),
          get ended() {
            return sink.ended
          },
          get resyncs() {
            return sink.resyncs
          },
          get acks() {
            return sink.acks
          },
        }
        // A rejected frame is dropped and the worker is asked to resend; the
        // port stays open, so the rejection is logged rather than fatal.
        const onEgressError = (error: Error): void => {
          const bound = boundSession()
          void bridge?.fork(
            Effect.logError(workerLogMessage("egress frame rejected"), {
              workerID,
              ...(bound === undefined ? {} : { sessionID: bound }),
              error: error.message,
              errorName: error.name,
            }),
          )
        }
        const disposeReceiver = serveEventsPort(
          port,
          observedSink,
          onEgressError,
          (commit) => egressMesh.publish(commit),
          bridge,
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
  /** Cancellation tuning; cooperative cancel is always wired. */
  readonly cancellation?: WorkerCancellationOptions
  /** Upper bound on the dispatcher's async resume decision (see `SessionHubOptions`). */
  readonly resumeDecisionTimeoutMs?: number
  /** Generation-scoped release for an in-place replacement (see `SessionHubOptions`). */
  readonly onGenerationReleased?: (sessionID: SessionID, workerID?: number) => unknown
  /** Session-scoped release when a session leaves the hub (reap/delete/shutdown). */
  readonly onSessionReleased?: (sessionID: SessionID) => unknown
  /**
   * Cheap generation probe (the dispatching session's requested model), so an
   * unchanged generation reuses the cached worker init instead of rebuilding it.
   */
  readonly generationOf?: (sessionID: SessionID) => string | Promise<string>
  /**
   * Per-session durable-write barrier forwarded to the
   * dispatcher: a completed run's slot is held until it resolves.
   */
  readonly writeBarrier?: DispatcherOptions["writeBarrier"]
  /**
   * The run barrier. Required: the production layer yields `RunBarrier.Service`
   * and passes it through, so the hub never builds a private instance.
   */
  readonly barrier: RunBarrier.Interface
  /**
   * Per-session durable-egress barrier forwarded to every
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
  /** Idle WAL checkpoint cadence for the hub executor (see `DbExecutorOptions`). */
  readonly checkpointIntervalMs?: number
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
  readonly bridge?: EffectBridge.Shape
  /** Boot-cancellation telemetry: a cancel win or a death during abort. */
  readonly onBootTelemetry?: (event: BootTelemetry) => void
  /** The background-promotion decision the hub observed (see `SessionHubOptions`). */
  readonly promotionOutcome?: (sessionID: SessionID) => RunBarrierPromotionOutcome
  /**
   * The layer scope that owns the Wave-6 `WorkerPorts` / `WorkerPools` services
   * this hub creates internally. `productionSessionHubLayer` supplies it so those
   * services' finalizers run with the layer; a caller-supplied `workerPorts` /
   * `workerPools` is never finalized by this hub (see those options). A direct
   * caller without a scope keeps the factory adapters.
   */
  readonly scope?: Scope.Scope
  /**
   * Receives the resolved port/pool services so the layer can order teardown
   * (ports first, then the pool) and tests can observe the wiring.
   */
  readonly onServices?: (services: ProductionHubServices) => void
  /** A pre-built, caller-owned port service; the hub never registers its finalizer. */
  readonly workerPorts?: WorkerPortsInterface
  /** A pre-built, caller-owned pool service; the hub never registers its finalizer. */
  readonly workerPools?: WorkerPoolsInterface
}

/** The Wave-6 services the production hub composes for a worker's ports/pool. */
export interface ProductionHubServices {
  readonly workerPorts?: WorkerPortsInterface
  readonly workerPools: WorkerPoolsInterface
}

/** Reaping is best-effort teardown: a failed kill must not block cancellation. */
function ignoreReapFailure(reap: () => void): void {
  try {
    reap()
  } catch {
    // The isolate is already being torn down; a stale mirror is the only loss.
  }
}

/**
 * Best-effort startup sweep of leases left by earlier hub-process generations.
 * A `worker-<id>-<pid>-<uuid>` owner whose PID is gone is marked reclaimable so
 * the next admission does not wait for its TTL; a live generation is untouched.
 *
 * Runs once, after the schema gate has migrated the file, over a short-lived
 * connection. A failure is logged and swallowed: the successor reclaim path
 * still fences correctly, so this is defense-in-depth, never a boot blocker.
 */
export function sweepDeadLeasesAtStartup(filename: string, bridge?: EffectBridge.Shape): number {
  let connection: ReturnType<typeof openConnection> | undefined
  try {
    connection = openConnection({ filename, create: false, busyTimeoutMs: 250 })
    const released = DurableLog.create({ connection, ownerID: "hub-startup-lease-sweep" }).releaseDeadLeases()
    if (released > 0) {
      void bridge?.fork(Effect.logInfo(workerLogMessage("startup lease sweep"), { released }))
    }
    return released
  } catch (error) {
    void bridge?.fork(
      Effect.logWarning(workerLogMessage("startup lease sweep failed"), {
        error: error instanceof Error ? error.message : String(error),
      }),
    )
    return 0
  } finally {
    connection?.close()
  }
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
  bridge: EffectBridge.Shape | undefined,
): InstanceContext {
  const resolved =
    instanceOfSession(sessionID) ??
    (dispatched === undefined ? undefined : instanceFromSession(dispatched)) ??
    captureInstanceRef()
  if (resolved !== undefined) return resolved
  const fallback = fallbackInstanceContext()
  void bridge?.fork(
    Effect.logWarning(workerLogMessage("instance fallback"), {
      sessionID,
      directory: fallback.directory,
    }),
  )
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
  const bridge = options.bridge
  const scope = options.scope
  // A direct caller without a layer scope still gets the single scoped pool:
  // its `terminateAll` closes the pool's own cache scope on shutdown.
  const poolScope = scope ?? Scope.makeUnsafe()
  const runScoped = <A>(effect: Effect.Effect<A, never, Scope.Scope>): A =>
    WorkerRuntime.runSync(Scope.provide(poolScope)(effect))
  // One backend build for both the hub's dispose bookkeeping and the port
  // service: a function source is memoized here, so neither owner can construct
  // a second egress mesh. The resolved flag is set only AFTER the source
  // succeeds, so a transient build failure rethrows on the next attach instead
  // of being cached as "no backends".
  const portSource = options.ports
  let portBackendsBuilt = false
  let portBackendsValue: WorkerPortBackends | undefined
  const sharedPortSource: WorkerPortBackendsSource | undefined =
    typeof portSource === "function"
      ? () => {
          if (portBackendsBuilt) return portBackendsValue as WorkerPortBackends
          portBackendsValue = portSource()
          portBackendsBuilt = true
          return portBackendsValue
        }
      : portSource
  // One Effect semaphore bounds every cold isolate spawn for this hub (and every
  // crash respawn, because the pool retains the same handle).
  const spawnGate =
    options.spawnGate ?? makeSpawnGate({ timeoutMs: options.spawnTimeoutMs ?? DEFAULT_SPAWN_GATE_TIMEOUT_MS })
  // Schema-ensure runs off the hub's main thread and before any spawn. The gate
  // is lazy so construction stays sync (the layer body) while the actual migrate/
  // verify/mark work runs on the DbExecutor thread; the first dispatch awaits it.
  // Its live count is the process-wide registry of constructed isolates plus
  // the exact pre-construction reservations held by an acquired permit.
  // `BEGIN IMMEDIATE` serializes concurrent hubs.
  let bootGate: Promise<void> | undefined
  const awaitBootGate = (): Promise<void> => {
    const databasePath = options.databasePath
    if (databasePath === undefined) return Promise.resolve()
    // A caller-supplied gate without an exact `active` count (a test fake) keeps
    // the legacy "held" hint; the real semaphore gate never reads it.
    const heldHint = spawnGate.active === undefined && spawnGate.held === true ? 1 : 0
    bootGate ??= ensureProductionSchemaAsync({
      filename: databasePath,
      liveWorkers: options.liveWorkers?.() ?? liveSessionWorkerCount(databasePath) + heldHint,
      ...(options.db === undefined ? {} : { executor: options.db }),
    }).then(
      () => {
        // The schema gate has migrated/verified the file, so the lease table is
        // safe to sweep. Schedule after this promise resolves and never await:
        // a slow or failing sweep must not delay the first worker admission.
        setTimeout(() => sweepDeadLeasesAtStartup(databasePath, bridge), 0)
        return undefined
      },
      (error: unknown) => {
        // Reset so a later dispatch can retry after the cause is fixed; the
        // current dispatch still observes the typed failure.
        bootGate = undefined
        throw error
      },
    )
    return bootGate
  }
  // No worker is spawned until a session dispatches; `workerCount` is diagnostic
  // (log line + worker snapshot), never a pool prewarm.
  const workerCount = options.count ?? 0
  // Resolve the run-reply liveness budget once, at the hub boundary: an explicit
  // option wins, else the env override, else the default-on watchdog budget.
  const runIdleTimeoutMs = resolveRunIdleTimeoutMs(options)
  logRunIdleTimeoutWarning(bridge, options)
  // Resolve the entry the pool will actually spawn (an explicit entry wins, then
  // the env override, then the build-time define, then the source-relative URL).
  const describedEntry: DescribedSessionWorkerEntry =
    options.entry !== undefined
      ? { entry: options.entry, source: "definedPath" }
      : describeSessionWorkerEntry({ env: options.env })
  void bridge?.fork(
    Effect.logInfo(workerLogMessage("pool resolved"), {
      workerCount,
      availableParallelism: os.availableParallelism(),
    }),
  )
  void bridge?.fork(
    Effect.logInfo(workerLogMessage("worker entry resolved"), {
      entry: String(describedEntry.entry),
      source: describedEntry.source,
    }),
  )
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
    return sharedPortSource === undefined ? init : withSessionWorkerPorts(init)
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
  const poolOptions: ProductionWorkerPoolOptions = {
    init: resolveInit,
    ...(bridge === undefined ? {} : { bridge }),
    onPhase: (sessionID, phase) => lifecycle.phase(sessionID, phase),
    // An idle-cap eviction is a full generation retirement: drop the
    // dispatcher entry, terminate, and run the hub release (ports, asks,
    // lease). Wired lazily because the hub is created after the pool.
    onIdleEvicted: (sessionID) => hub?.dispatcher.retire(sessionID) ?? Promise.resolve(),
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
      ...(options.generationOf === undefined ? {} : { generationOf: options.generationOf }),
      beforeSpawn: awaitBootGate,
    }),
    // A same-session model switch replaces the isolate in place. Route the old
    // generation through the dispatcher's retirement gate (never `retireWorker`:
    // the successor reuses the same slot) so it gets the same release ordering,
    // port disposal and retired-lease release as crash/reap/cancel.
    onGenerationRetired: (sessionID, workerID) => hub?.dispatcher.retireGeneration(sessionID, workerID),
    ...sinks,
  }
  // Scope ownership: only services this hub creates internally are attached to
  // `options.scope` (their `make` installs the scope finalizer). A caller-supplied
  // `workerPorts` / `workerPools` stays caller-owned and is never registered for
  // teardown here. Pools are constructed before ports so the services' LIFO
  // finalizers dispose ports before terminating the pool; in production the
  // layer's hub finalizer is the single owner of that ordering.
  let workerPortSeams: WorkerPortSeams | undefined
  // The test pool seam adapts a caller-built structural pool; production always
  // uses the one scoped `makeProduction` layer.
  const adaptPool = (pool: ExecutorWorkerPool): WorkerPoolsInterface => ({
    executorPool: pool,
    spawn: (id) => Effect.sync(() => pool.spawn(id)),
    releaseSession: (sessionID, workerID) =>
      Effect.sync(() => {
        pool.releaseSession?.(sessionID, workerID)
      }),
    byID: (id) => Effect.sync(() => pool.byID(id)),
    resultFor: (sessionID) => Effect.sync(() => pool.resultFor(sessionID)),
    workers: Effect.sync(() => pool.workers),
    terminateAll: () => Effect.promise(() => pool.terminateAll()),
    stats: Effect.sync(() => {
      const bounded = pool as Partial<{ idleWorkers: number; idleCapacity: number; idleEvictions: number }>
      return {
        workers: pool.workers.length,
        idleWorkers: bounded.idleWorkers ?? 0,
        idleCapacity: bounded.idleCapacity ?? 0,
        idleEvictions: bounded.idleEvictions ?? 0,
      }
    }),
  })
  const workerPools: WorkerPoolsInterface =
    options.workerPools ??
    (options.poolFactory !== undefined
      ? adaptPool(options.poolFactory(resolveInit, sinks, { maxRunning }))
      : runScoped(makeProduction(poolOptions)))
  const workerPorts: WorkerPortsInterface | undefined =
    options.workerPorts ??
    (scope === undefined
      ? undefined
      : runScoped(
          WorkerPortsModule.make({
            ...(bridge === undefined ? {} : { bridge }),
            backends: sharedPortSource,
            hubProxyHandlersFor: (workerID) => workerPortSeams?.hubProxyHandlersFor(workerID),
            hubProxyStreamingFor: (workerID) => workerPortSeams?.hubProxyStreamingFor(workerID),
            onPidReport: (workerID, report) => workerPortSeams?.onPidReport?.(workerID, report),
          }),
        ))
  const pool = workerPools.executorPool
  // The hub-side mirror of every worker child's process group: a
  // terminate reaps the worker's mirrored PGIDs and shutdown reaps the rest.
  // Filled by the worker's `{type:"pid-report"}` control-port frames below; a
  // report for an already-mirrored group only advances its generation.
  const pids = createPidRegistry()
  const support = reapSupport()
  void bridge?.fork(
    Effect.logInfo(workerLogMessage("reap support"), { supported: support.supported, reason: support.reason }),
  )
  const onPidReport = (workerID: number, report: PidReport): void => {
    void bridge?.fork(
      Effect.logInfo(workerLogMessage("pid report"), { workerID, entries: report.entries.length }),
    )
    try {
      // `workerID` is the hub's slot association (authoritative); the registry
      // refuses a PGID reassignment to a different owner.
      registerReported(pids, report, workerID)
    } catch {
      // A bad/conflicting entry must not take the control port down.
    }
  }
  options.onServices?.({ ...(workerPorts === undefined ? {} : { workerPorts }), workerPools })
  hub = createSessionHub({
    pool,
    ...(bridge === undefined ? {} : { bridge }),
    lifecycle,
    barrier: options.barrier,
    ...defined({
      ports: sharedPortSource,
      db: options.db,
      cache: options.cache,
      onWorkerCrashed: crashSink,
      onDispatch: options.onDispatch,
      depthOf: options.depthOf,
      events: options.events,
      eventPublisherFor: options.eventPublisherFor,
      onCancelRequested: options.onCancelRequested,
      writeBarrier: options.writeBarrier,
      resumeDecisionTimeoutMs: options.resumeDecisionTimeoutMs,
      bootGate: awaitBootGate,
      onGenerationReleased: options.onGenerationReleased,
      onSessionReleased: options.onSessionReleased,
      promotionOutcome: options.promotionOutcome,
      onPidReport,
    }),
    ...(workerPorts === undefined
      ? {}
      : {
          workerPorts,
          bindWorkerPortSeams: (seams: WorkerPortSeams) => {
            workerPortSeams = seams
          },
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
  /** Idle WAL checkpoint cadence for the hub executor (see `DbExecutorOptions`). */
  readonly checkpointIntervalMs?: number
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
  /**
   * Per-session durable-egress barrier supplied to every spawned worker (the
   * run's result is not recorded until its durable egress write is acked).
   * Egress owns the ack barrier; the hub only forwards it to the executor.
   */
  readonly egressBarrier?: (sessionID: SessionID) => Promise<void> | undefined
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
  /**
   * Cheap generation probe (the dispatching session's requested model), so an
   * unchanged generation reuses the cached worker init instead of rebuilding it.
   */
  readonly generationOf?: (sessionID: SessionID) => string | Promise<string>
  /** The background-promotion decision the hub observed (see `SessionHubOptions`). */
  readonly promotionOutcome?: (sessionID: SessionID) => RunBarrierPromotionOutcome
  /** Receives the resolved port/pool services (layer teardown ordering, tests). */
  readonly onServices?: (services: ProductionHubServices) => void
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
  /** Re-entry bridge for lines emitted from the resolver's foreign callbacks. */
  readonly logBridge: EffectBridge.Shape | undefined
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
  /**
   * Release the pending hub-side requests bound to ONE retired worker
   * generation. Each ask is tagged with the session generation epoch it began
   * in; this advances the epoch and synchronously fails only the older epoch's
   * asks, so a successor's own asks can never be caught by the old generation's
   * retirement — even if this bounded release lands after the successor boots.
   */
  readonly releaseGeneration: (sessionID: SessionID, workerID?: number) => Promise<void>
  /** Diagnostic/test seam: the session's current generation-ask epoch. */
  readonly generationEpochOf: (sessionID: SessionID) => number
  /** Diagnostic/test seam: sessions that still hold a generation epoch record. */
  readonly generationSessionCount: () => number
  readonly instanceOfSession: (sessionID: SessionID | undefined) => InstanceContext | undefined
  readonly resolveInit: (sessionID?: SessionID) => Promise<SessionWorkerInit>
  readonly generationOfSession: (sessionID: SessionID) => Promise<string>
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
  /** The background-promotion decision the hub observed for the session. */
  readonly promotionOutcome: (sessionID: SessionID) => RunBarrierPromotionOutcome
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

/** One in-flight hub-proxy ask tagged with the session generation that began it. */
interface PendingGenerationAsk {
  readonly epoch: number
  readonly cancel: () => void
}

/**
 * The per-session generation-ask registry. Every hub-proxy ask records the
 * epoch current when its wrapper was INVOKED, not when its effect later runs:
 * an old generation's ask whose effect starts after retirement has already
 * advanced the epoch must still be tagged with the old epoch and fail.
 */
export interface GenerationAskTracker {
  /** The session's current epoch (0 before its first retirement). */
  current(sessionID: SessionID): number
  /**
   * Register and run one ask under an explicit epoch. The post-registration
   * check covers a retirement that advanced the epoch while this effect was
   * starting, before its entry joined the pending set.
   */
  track<A, E>(sessionID: SessionID, epoch: number, work: Effect.Effect<A, E>, rejected: () => E): Effect.Effect<A, E>
  /** Advance the epoch and synchronously fail every ask of an older epoch. */
  retire(sessionID: SessionID): void
  /** Drop the session's epoch record (session release; no unbounded growth). */
  forget(sessionID: SessionID): void
  /** Drop every epoch and pending ask (hub teardown). */
  clear(): void
  /** Diagnostic: asks currently registered for the session. */
  pending(sessionID: SessionID): number
  /** Diagnostic: sessions that still have an epoch record. */
  sessionCount(): number
}

export function createGenerationAskTracker(): GenerationAskTracker {
  const epochBySession = new Map<SessionID, number>()
  const pendingBySession = new Map<SessionID, Set<PendingGenerationAsk>>()
  const current = (sessionID: SessionID): number => epochBySession.get(sessionID) ?? 0
  const pendingSet = (sessionID: SessionID): Set<PendingGenerationAsk> | undefined => pendingBySession.get(sessionID)
  const track = <A, E>(
    sessionID: SessionID,
    epoch: number,
    work: Effect.Effect<A, E>,
    rejected: () => E,
  ): Effect.Effect<A, E> =>
    Effect.gen(function* () {
      const cancelled = yield* Deferred.make<never, E>()
      const entry: PendingGenerationAsk = {
        epoch,
        cancel: () => {
          Deferred.doneUnsafe(cancelled, Effect.fail(rejected()))
        },
      }
      let pending = pendingBySession.get(sessionID)
      if (pending === undefined) {
        pending = new Set()
        pendingBySession.set(sessionID, pending)
      }
      pending.add(entry)
      // A retirement that raced the registration already advanced the epoch.
      if (current(sessionID) > epoch) entry.cancel()
      return yield* Effect.raceFirst(work, Deferred.await(cancelled)).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            const live = pendingBySession.get(sessionID)
            if (live === undefined) return
            live.delete(entry)
            if (live.size === 0) pendingBySession.delete(sessionID)
          }),
        ),
      )
    })
  const retire = (sessionID: SessionID): void => {
    const epoch = current(sessionID) + 1
    epochBySession.set(sessionID, epoch)
    const pending = pendingSet(sessionID)
    if (pending === undefined) return
    for (const entry of [...pending]) {
      if (entry.epoch >= epoch) continue
      pending.delete(entry)
      entry.cancel()
    }
    if (pending.size === 0) pendingBySession.delete(sessionID)
  }
  const forget = (sessionID: SessionID): void => {
    epochBySession.delete(sessionID)
  }
  const clear = (): void => {
    for (const sessionID of pendingBySession.keys()) retire(sessionID)
    epochBySession.clear()
    pendingBySession.clear()
  }
  return {
    current,
    track,
    retire,
    forget,
    clear,
    pending: (sessionID) => pendingSet(sessionID)?.size ?? 0,
    sessionCount: () => epochBySession.size,
  }
}

/**
 * Tag an ask with the epoch current at the wrapper's call site. Reading the
 * epoch here — never inside the returned effect — is what lets a retirement
 * cancel an old generation's ask whose effect starts afterwards.
 */
export function askInGeneration<A, E>(
  tracker: GenerationAskTracker,
  sessionID: SessionID,
  work: Effect.Effect<A, E>,
  rejected: () => E,
): Effect.Effect<A, E> {
  const epoch = tracker.current(sessionID)
  return tracker.track(sessionID, epoch, work, rejected)
}

export function createPerSessionResolvers(deps: PerSessionResolverDeps): PerSessionResolvers {
  const { services, bridge, authorities, logBridge, subagentDepth } = deps
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
    // A session's pending asks belong to its live generation; a rebind or
    // terminal release must not leave them blocked. The epoch record itself is
    // dropped afterwards: a re-dispatch starts at 0 and no old ask survives, so
    // keeping one entry per session would grow without bound.
    generationAsks.retire(sessionID)
    generationAsks.forget(sessionID)
  }
  const clearSessions = (): void => {
    instanceBySession.clear()
    contextBySession.clear()
    children.clear()
    sessionDepths.clear()
    generationAsks.clear()
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
      WorkerRuntime.runPromise(context === undefined ? effect : Effect.provide(effect, context))
  // Every hub-proxy permission/question ask is tagged with the generation epoch
  // of the session it belongs to. Retiring a generation advances that session's
  // epoch and synchronously fails only asks tagged with an older epoch, so a
  // successor's asks — tagged with the advanced epoch — can never be cancelled
  // by the old generation's retirement, even if its cleanup lands after the
  // successor is admitted. The tracker owns both maps so the epoch and its
  // pending set are always released together.
  const generationAsks = createGenerationAskTracker()

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
  // Revert is hub-owned: the worker's `Session.clearRevert` forwards here, where
  // the hub clears its own row under the dispatching session's context.
  const clearRevertFor =
    (context: Context.Context<never> | undefined): NonNullable<HubProxyAuthorities["clearRevert"]> =>
    async (sessionID) => {
      const sessions = context === undefined ? Option.none() : Context.getOption(context, Session.Service)
      if (Option.isNone(sessions)) {
        throw new Error("the hub has no Session authority in the dispatched context; cannot clear a session revert")
      }
      await runHubProxyFor(context)(sessions.value.clearRevert(sessionID as SessionID))
    }
  // A worker's permission/question asks are bound to its generation: on
  // retirement the session epoch advances and only the old epoch's asks fail.
  // Unbound workers keep the raw authority (they cannot ask anyway).
  const generationScopedPermission = (
    inner: Permission.Interface,
    sessionID: SessionID | undefined,
  ): Permission.Interface =>
    sessionID === undefined
      ? inner
      : {
          ...inner,
          ask: (input) =>
            askInGeneration(generationAsks, sessionID, inner.ask(input), () => new PermissionV1.RejectedError()),
        }
  const generationScopedQuestion = (inner: Question.Interface, sessionID: SessionID | undefined): Question.Interface =>
    sessionID === undefined
      ? inner
      : {
          ...inner,
          ask: (input) =>
            askInGeneration(generationAsks, sessionID, inner.ask(input), () => new Question.RejectedError()),
        }
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
      permission: generationScopedPermission(authorities.permission, boundSessionID),
      question: generationScopedQuestion(authorities.question, boundSessionID),
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
      // Revert clearing is hub authority: the worker forwards its own session
      // and the hub clears the row; a foreign target is rejected by the
      // binding before it reaches `Session.clearRevert`.
      clearRevert: clearRevertFor(context),
      // Worker jobs have no hub process to live in: the mirror makes them visible
      // to the promotion endpoint, and the long-poll below reaches the worker.
      // The mirror is session-bound: an unbound worker gets no job authority.
      ...(boundSessionID === undefined
        ? {}
        : { backgroundJob: jobMirrors.authorityFor(runHubProxyFor(context), boundSessionID) }),
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
    const instance = resolveSessionInstance(session, dispatched, instanceOfSession, logBridge)
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
    return WorkerRuntime.runPromise(
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
  /**
   * Cheap generation probe: read the dispatching session's requested model
   * without building the full worker init. The scoped production pool caches
   * the init per `(session, generation)`, so an unchanged model never rebuilds
   * the graph/plugin bootstrap on every turn.
   */
  const generationOfSession = async (sessionID: SessionID): Promise<string> => {
    const dispatched = await loadDispatchedSession(contextOfSession(sessionID), sessionID)
    const model = dispatched?.model
    return model === undefined ? "unknown" : `${model.providerID}/${model.modelID}`
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
  const cancelPendingRequests = (sessionID: SessionID): Promise<void> => {
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
    if (cancels.length === 0) return Promise.resolve()
    return Promise.all(cancels).then(
      () => undefined,
      (error: unknown) => {
        void logBridge?.fork(
          Effect.logWarning(workerLogMessage("pending request cancel failed"), {
            sessionID,
            error: error instanceof Error ? error.message : String(error),
          }),
        )
      },
    )
  }
  // A retired generation's pending asks can never be answered by its successor.
  // Asks are tagged with the session generation epoch captured when their proxy
  // wrapper was invoked; retiring advances the epoch and synchronously fails only
  // the older epoch's asks, so a successor's asks are never caught — even if this
  // hook's cleanup lands after the successor is admitted. The binding guard
  // refuses a worker id already reassigned to another session.
  //
  // Precondition: every ask that can outlive a generation crossed this tracked
  // proxy (`permission.ask` / `question.ask`). That is why a known worker id
  // deliberately skips the session-wide `cancelPendingRequests`: an untracked
  // ask path would have no generation tag and could only be reached by that
  // session-wide cancel.
  const releaseGeneration = (sessionID: SessionID, workerID?: number): Promise<void> => {
    const bound = workerID === undefined ? undefined : deps.sessionOfWorker(workerID)
    if (bound !== undefined && bound !== sessionID) return Promise.resolve()
    // Synchronous: the successor is admitted only after this epoch advance lands.
    generationAsks.retire(sessionID)
    // With no worker id there is no generation to scope; fall back to the
    // session-wide cancel for asks that never crossed the tracked proxy.
    return workerID === undefined ? cancelPendingRequests(sessionID) : Promise.resolve()
  }
  return {
    events,
    eventsForSession,
    eventsForWorker,
    hubProxyFor,
    hubProxyStreamingFor,
    abortPluginTools: (sessionID) => pluginToolAborts.abortSession(sessionID),
    cancelPendingRequests: (sessionID) => {
      void cancelPendingRequests(sessionID)
    },
    releaseGeneration,
    generationEpochOf: (sessionID) => generationAsks.current(sessionID),
    generationSessionCount: () => generationAsks.sessionCount(),
    instanceOfSession,
    resolveInit,
    generationOfSession,
    recordDispatch,
    releaseSession,
    clearSessions,
    childrenOf: children.childrenOf,
    depthOf,
    promotionOutcome: (sessionID) => jobMirrors.promotionOutcome(sessionID),
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
  | RunBarrier.Service
> {
  return Layer.effect(
    SessionHubService,
    Effect.gen(function* () {
      // The layer scope owns the Wave-6 port/pool services created below.
      const scope = yield* Scope.Scope
      const barrier = yield* RunBarrier.Service
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
      // no `log`, so worker, cancel, and lifecycle log lines and everything forwarded through
      // dispatch/execute/cancel were dormant. `bridge` names the EventV2Bridge above.
      const loggers = yield* Logger.CurrentLoggers
      const effectBridge = yield* EffectBridge.make().pipe(
        Effect.provide(WorkerLogging.forwardingLayer(loggers)),
      )
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
      // TEMPORARY EDGE: `SessionHubOptions.db` and the hub internals are still
      // Promise-shaped. The service owns the worker and its scope; this facade
      // only lets those Promise internals call the Effect methods. Remove it
      // when the hub itself is effectified.
      let db = options.db
      if (db === undefined) {
        const service = yield* DbExecutor.make({
          filename: databasePath,
          create: false,
          ...(options.checkpointIntervalMs === undefined ? {} : { checkpointIntervalMs: options.checkpointIntervalMs }),
        })
        db = DbExecutor.toPromiseExecutor(service)
      }
      // Oversized durable events reach the hub as a committed position. Resolve
      // them off-loop through the same executor that owns hub SQL: no core
      // `Database.Service` dependency is added, so the hub node graph is unchanged.
      const blobDir = DurableLog.blobDirectoryFor(databasePath)
      const resolveBlob =
        options.backends?.resolveBlob ??
        (blobDir === undefined
          ? undefined
          : createEventBlobResolver({ query: (sql, params) => db.query(sql, params), blobDir }))
      // A worker gap is healed by reading the missed committed range off-loop.
      const readDurable =
        options.backends?.readDurable ?? createDurableHealReader({ query: (sql, params) => db.query(sql, params) })
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
        logBridge: effectBridge,
        subagentDepth,
        sessionOfWorker: (workerID) => hubRef?.sessionOfWorker(workerID),
      })
      // The production durable-egress barrier: the events port observes each
      // durable frame and its ack, and `execute` awaits the ack after the run
      // reply. Supplying it keeps the run from being recorded before its durable
      // egress write reached the hub.
      const egressDelivery = createEgressDeliveryBarrier({ bridge: effectBridge })
      // The production port authorities: supplied ready, or built on the first
      // dispatch (the source is only invoked when a worker attaches a port).
      const ports: WorkerPortBackendsSource =
        options.ports ??
        (() =>
          buildProductionPortBackends({
            ...options.backends,
            bridge: effectBridge,
            ...(resolveBlob === undefined ? {} : { resolveBlob }),
            readDurable,
            hubProxy: options.backends?.hubProxy,
            hubProxyFor: options.backends?.hubProxyFor ?? resolvers.hubProxyFor,
            hubProxyStreamingFor: options.backends?.hubProxyStreamingFor ?? resolvers.hubProxyStreamingFor,
            events: resolvers.events,
            eventsForWorker: resolvers.eventsForWorker,
            cache,
            egressDelivery,
            sessionOfWorker: (workerID) => hubRef?.sessionOfWorker(workerID),
          }))
      const hub = createProductionSessionHub({
        ...options,
        scope,
        count,
        barrier,
        databasePath,
        init: resolvers.resolveInit,
        generationOf: resolvers.generationOfSession,
        ports,
        db,
        cache,
        events: resolvers.events,
        eventPublisherFor: resolvers.eventsForSession,
        egressBarrier: options.egressBarrier ?? egressDelivery.wait,
        promotionOutcome: options.promotionOutcome ?? resolvers.promotionOutcome,
        bridge: effectBridge,
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
        onGenerationReleased: (sessionID, workerID) => resolvers.releaseGeneration(sessionID, workerID),
        onSessionReleased: (sessionID) => {
          // A session leaving the hub (reap/delete/shutdown) drops its
          // per-session maps. The retired generation was already released before
          // this hook, so pending asks/ports/lease are gone.
          resolvers.releaseSession(sessionID)
          egressDelivery.release(sessionID)
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
 * A layer node providing the production `SessionHubService`. Its deps
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
      RunBarrier.node,
    ],
  })
}

// ---------------------------------------------------------------------------
// Pool-spawning layer node (reduced layer per role).
// ---------------------------------------------------------------------------

/** Provide `SessionHubService`, terminating the pool when the layer scope closes. */
export function createSessionHubLayer(
  options: SessionHubOptions,
): Layer.Layer<SessionHubService, never, RunBarrier.Service> {
  return Layer.effect(
    SessionHubService,
    Effect.gen(function* () {
      const loggers = yield* Logger.CurrentLoggers
      const log = yield* EffectBridge.make().pipe(Effect.provide(WorkerLogging.forwardingLayer(loggers)))
      const barrier = yield* RunBarrier.Service
      const hub = createSessionHub({ ...options, bridge: log, barrier })
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
  return LayerNode.make({
    service: SessionHubService,
    layer: createSessionHubLayer(options),
    deps: [RunBarrier.node],
  })
}
