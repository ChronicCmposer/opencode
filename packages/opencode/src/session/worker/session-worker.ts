// Phase 12 — production session-worker entry.
//
// This is the isolate the hub's `createBunExecutorWorkerPool` spawns. It builds
// the reduced session layer (`buildSessionWorkerLayer`, Phase 6), wires the four
// Wave-1 port-backed clients (writer / egress / broker / background-job) and
// installs the executor with the real session work kind.
//
// Initialisation arrives through `workerData` (structured-cloned, never a
// closure): the immutable `WorkerSnapshot`, the `WorkerEnv` overlay, and the
// model bootstrap. The function-valued client handles are rebuilt in-worker over
// transferred `MessagePort`s (they cannot cross an isolate boundary).
//
// Protocol: the `execute.ts` frame protocol over the default worker channel.
// The worker additionally transfers its writer / egress / broker ports and its
// dedicated job control link to the hub.
//
// Thread naming (Linux-only, fail-soft): the isolate names its OS thread so
// `ps -T`/`top -H`/htop label the slot instead of the process (htop needs
// "Show custom thread names" enabled). Because the hub's main thread is `och`,
// the process `comm` no longer matches `pgrep opencode`/`ps -C opencode`; use
// `pgrep -f 'oc serve'` instead. Names are clipped to the kernel's 15-byte
// `comm` limit (see `@/util/thread-name`).

import { threadId, workerData } from "node:worker_threads"
import { Effect, Layer, Option } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { isRole, setProcessRole } from "@/effect/process-role"
import { Project } from "@opencode-ai/schema/project"
import { SessionPrompt } from "@/session/prompt"
import { SessionRunState } from "@/session/run-state"
import { Snapshot } from "@/snapshot"
import { InstanceRef } from "@/effect/instance-ref"
import { EffectBridge } from "@/effect/bridge"
import { setThreadName } from "@/util/thread-name"
import type { InstanceContext } from "@/project/instance-context"
import { Provider } from "@/provider/provider"
import { createWorkerWriterClient, type WorkerWriterClient } from "./writer-client"
import { createWorkerEgressClient, type WorkerEgressClient } from "./egress-client"
import { createWorkerBrokerClient } from "./broker-client"
import { createBackgroundJobClient, createJobClient, jobPortFromMessagePort, type JobClient } from "./job-client"
import { noopWorkerLogger, workerLogMessage, workerLoggerFromBridge, type WorkerLogger } from "./log"
import { messageOf } from "./port"
import { sessionThreadName } from "./thread-name"
import type { JobInfo } from "./job-registry"
import {
  DEFAULT_WORK_KIND,
  UnknownWorkKindError,
  installSessionExecutor,
  type SessionExecutionResult,
  type SessionRunControl,
  type SessionWork,
  type SessionWorkFactory,
} from "./execute"
import { createWorkerPluginHost, type WorkerPlugin } from "./plugin-host"
import {
  SessionPipeline,
  PIPELINE_SESSION_NODES,
  backgroundJobClientLayer,
  buildSessionWorkerLayer,
  createHubProxyClient,
  llmModelClientLayer,
  reducedLlmLayer,
  reducedModelProviderLayer,
  type BackgroundJobClientHandle,
  type BrokerClientHandle,
  type HubProxyClients,
  type PipelineMessage,
  type WorkerEnv,
  type WorkerModelConfig,
  type WorkerSnapshot,
  type WriterClientHandle,
} from "./layer"
import { workerSnapshotBoundaryLayer } from "./snapshot"
import { PidReporter, type PidReport, type PidReporterInterface } from "./pid-registry"
import type { Role } from "./node-table"

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
}

/**
 * Capabilities the hub advertises for a worker generation. `sessionWorkerPorts`
 * is set once the hub serves the worker's writer / egress / broker ports (see
 * `serveWriterPort` and the egress/broker servers); until then the entry uses an
 * in-worker fallback so a session cannot hang on an unserved port.
 */
export interface SessionWorkerCapabilities {
  readonly sessionWorkerPorts?: boolean
}

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
   * Worker-hosted plugin hooks (Phase 6.3, plan decision 70): the hub owns plugin
   * loading but the model-facing `chat.params`/`chat.headers` hooks run in the
   * isolate. Supplied when the hub can hand the loaded hooks across (in-process
   * tests today); absent means the reduced LLM uses its no-op pass-through.
   */
  readonly plugins?: readonly WorkerPlugin[]
}

interface WorkerScope {
  postMessage(message: unknown, transfer?: readonly Transferable[]): void
  onmessage: ((event: { data: unknown }) => void) | null
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
  if (typeof candidate.snapshot !== "object" || candidate.snapshot === null) {
    throw new Error("session worker workerData.snapshot is required")
  }
  if (typeof candidate.env !== "object" || candidate.env === null) {
    throw new Error("session worker workerData.env is required")
  }
  if (candidate.model === undefined || typeof candidate.model.baseUrl !== "string") {
    throw new Error("session worker workerData.model.baseUrl is required")
  }
  if (!isRole(candidate.role)) {
    throw new Error(`session worker workerData.role must be a valid process role, received ${JSON.stringify(candidate.role)}`)
  }
  if (
    candidate.workerID !== undefined &&
    (typeof candidate.workerID !== "number" || !Number.isInteger(candidate.workerID) || candidate.workerID < 0)
  ) {
    throw new Error(`session worker workerData.workerID must be a non-negative integer, received ${candidate.workerID}`)
  }
  return candidate as SessionWorkerInit
}

// ---------------------------------------------------------------------------
// Client aggregation (step 22).
// ---------------------------------------------------------------------------

/** The transferred ports a worker rebuilds its function-valued clients over. */
export interface WorkerClientPorts {
  readonly writer?: MessagePort | undefined
  readonly egress?: MessagePort | undefined
  /** Optional dedicated egress control (mesh) port. */
  readonly egressControl?: MessagePort | undefined
  readonly broker?: MessagePort | undefined
  readonly backgroundJob?: MessagePort | undefined
}

/** Raised when a required worker client port was not supplied. */
export class MissingWorkerPortError extends Error {
  constructor(readonly field: string) {
    super(`session worker is missing the "${field}" MessagePort`)
    this.name = "MissingWorkerPortError"
  }
}

function requirePort(port: MessagePort | undefined, field: string): MessagePort {
  if (port === undefined) throw new MissingWorkerPortError(field)
  return port
}

/**
 * The four client handles the reduced session layer installs. The writer is the
 * full `WorkerWriterClient`, so the production worker can build a real
 * writer-backed `Database` (statements AND `begin`/`txQuery`/`commit`/`rollback`).
 */
export interface WorkerClients {
  readonly writer: WorkerWriterClient
  /** The full egress client, so the production control-plane mesh is reachable. */
  readonly egress: WorkerEgressClient
  readonly broker: BrokerClientHandle
  readonly backgroundJob: BackgroundJobClientHandle
}

/**
 * Wire the four Wave-1 port-backed clients. Every port is required: a missing
 * one fails loud rather than degrading to a mock that silently drops writes.
 *
 * A pre-built `jobClient` may be supplied so the same `MessagePort` backs both
 * the reduced `BackgroundJobClientHandle` seam and the real `BackgroundJob`
 * node; two clients on one port would steal each other's replies.
 *
 * `workerID`, when supplied, is threaded onto every client so their E2 lifecycle
 * log lines carry the worker's owner id. It is omitted (never fabricated) when
 * the caller does not know it.
 *
 * `log`, when supplied, is threaded into every port-backed client so the E2
 * peer lifecycle lines (`writer connected`/`egress connected`/`broker connected`
 * and their closes) actually fire in the isolate. Absent keeps the no-op default.
 */
export function buildWorkerClients(
  ports: WorkerClientPorts,
  options: { jobClient?: JobClient; workerID?: number; log?: WorkerLogger } = {},
): WorkerClients {
  const writer = requirePort(ports.writer, "writer")
  const egress = requirePort(ports.egress, "egress")
  const broker = requirePort(ports.broker, "broker")
  const job = options.jobClient ?? createBackgroundJobClient(requirePort(ports.backgroundJob, "backgroundJob"))
  const workerID = options.workerID
  const withLog = options.log === undefined ? {} : { log: options.log }
  return {
    writer: createWorkerWriterClient(writer, {
      ...(workerID === undefined ? {} : { workerID }),
      ...withLog,
    }),
    egress: createWorkerEgressClient(egress, {
      ...(ports.egressControl === undefined ? {} : { controlPort: ports.egressControl }),
      ...(workerID === undefined ? {} : { workerID }),
      ...withLog,
    }),
    broker: createWorkerBrokerClient(broker, {
      ...(workerID === undefined ? {} : { workerID }),
      ...withLog,
    }),
    backgroundJob: backgroundJobHandle(job),
  }
}

/** Narrow the full job client to the reduced `BackgroundJobClientHandle` seam. */
function backgroundJobHandle(client: Pick<JobClient, "wait" | "cancel">): BackgroundJobClientHandle {
  return { wait: (id, timeoutMs) => client.wait(id, timeoutMs), cancel: (id) => client.cancel(id) }
}

// ---------------------------------------------------------------------------
// Child-result routing (9.4 handshake).
// ---------------------------------------------------------------------------

/**
 * A settled subagent result the hub registry routes to the worker hosting the
 * child's `parentID`. The worker subscribes once at boot; consumers attach
 * through {@link SessionWorkerBootstrap.onChildResult}.
 */
export type ChildResultSink = (result: JobInfo) => void

/**
 * The handle {@link bootstrapSessionWorker} returns: the registration handshake
 * for hub-routed child results plus teardown. The subscription is installed
 * unconditionally in production, so a routed child result is never silently
 * dropped; a consumer (or test) attaches a {@link ChildResultSink}.
 */
export interface SessionWorkerBootstrap {
  /** Subscribe to settled child results routed to this worker; returns an unsubscribe. */
  onChildResult(sink: ChildResultSink): () => void
  /** Tear down the bootstrapped child-result subscription. */
  dispose(): void
}

// ---------------------------------------------------------------------------
// Worker→hub PID reporting (P5 prerequisite).
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
  /** Optional structured logger seam; absent defaults to a no-op (flag-off behavior). */
  readonly log?: WorkerLogger
}

/**
 * Builds the worker's control-port PID reporter. Production uses
 * {@link createControlPidReporter} with the worker's forwarded `log`; the factory
 * seam lets a test hand the reporter a throwing port so the E4
 * `pid report failed` path (and the forwarded `log`) is observable without a
 * real closed port.
 */
export type ControlPidReporterFactory = (port: MessagePort, workerID: number, log: WorkerLogger) => PidReporterInterface

export function createControlPidReporter(
  port: MessagePort,
  workerID: number,
  options: ControlPidReporterOptions = {},
): PidReporterInterface {
  const log = options.log ?? noopWorkerLogger
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
        // E4 — the port is closed. Emit the WARN before rethrowing so the report
        // is never silently dropped AND the reason is observable.
        log(workerLogMessage("pid report failed"), { level: "warn", workerID, reason: messageOf(error) })
        throw error
      }
    },
  }
}

// ---------------------------------------------------------------------------
// Model bootstrap.
// ---------------------------------------------------------------------------

/**
 * The reduced-provider defaults for a legacy bootstrap that predates the
 * descriptor fields (only the non-owned test helper still emits one). A real
 * bootstrap always carries its own capabilities/limit.
 */
const DEFAULT_WORKER_CAPABILITIES: SerializedModelCapabilities = {
  temperature: true,
  reasoning: false,
  attachment: false,
  toolcall: true,
  interleaved: false,
}
const DEFAULT_WORKER_LIMIT: SerializedModelLimit = { context: 128_000, output: 4096 }

/** The descriptor fields the reduced provider consumes (see `layer.ts`). */
function modelConfig(model: SessionWorkerModel): WorkerModelConfig {
  // The frozen wire shape names the model `modelID`; a legacy bootstrap still
  // names it `model`. Accept both at this one boundary so an old payload cannot
  // reach the reduced provider as `undefined`.
  const legacy = model as SessionWorkerModel & { readonly model?: string }
  const modelID = model.modelID ?? legacy.model ?? ""
  return {
    providerID: model.providerID ?? modelID,
    modelID,
    apiId: model.apiId ?? modelID,
    npm: model.npm ?? "@ai-sdk/openai-compatible",
    baseUrl: model.baseUrl,
    apiKey: model.apiKey,
    headers: model.headers ?? {},
    // The wire shapes (`SerializedModelCapabilities` / a loose variants record)
    // are the frozen isolate contract; `WorkerModelConfig` is the reduced
    // provider's stricter projection. Narrowing here is the single boundary.
    capabilities: (model.capabilities ?? DEFAULT_WORKER_CAPABILITIES) as WorkerModelConfig["capabilities"],
    limit: model.limit ?? DEFAULT_WORKER_LIMIT,
    ...(model.options === undefined ? {} : { options: model.options }),
    ...(model.variants === undefined ? {} : { variants: model.variants as WorkerModelConfig["variants"] }),
    ...(model.providerOptions === undefined ? {} : { providerOptions: model.providerOptions }),
  }
}

// ---------------------------------------------------------------------------
// Session work (step 27) + work-kind registry (step 28).
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

export interface SessionWorkOptions {
  /** The compiled reduced session layer the work runs against. */
  readonly layer: Layer.Layer<unknown, unknown>
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
   * Register the worker-local run control (Phase 4). When the graph provides
   * `SessionRunState`, `run` resolves it and hands over a `SessionRunControl`
   * whose `cancel`/`busy` reach the real `SessionPrompt.loop` runner (table B),
   * so an executor cancel actually stops the loop (and `assertNotBusy` stops
   * lying). Pipeline-only graphs have no `SessionRunState`, so nothing registers.
   */
  readonly registerControl?: (control: SessionRunControl) => void
}

/**
 * The real `SessionWork`. When the layer provides `SessionPrompt` and the
 * caller allows it, the worker runs the actual opencode session loop —
 * `SessionPrompt.loop` — which drives `SessionProcessor` over the worker-local
 * `LLM` and persists through the writer boundary. Otherwise it falls back to the
 * one-shot `SessionPipeline` reduction (pipeline-only graphs and the degraded
 * no-hub-port boot).
 *
 * A carried `prompt` (Solution 1 / B2) routes through the full
 * `SessionPrompt.prompt` path — persist the user message for `sessionID`, then
 * loop — instead of the bare `loop` that expects the row to already exist.
 */
export function createSessionWork(options: SessionWorkOptions): SessionWork {
  return (sessionID, carriedPrompt) => {
    const run = Effect.gen(function* () {
      if (options.registerControl !== undefined) {
        const runState = yield* Effect.serviceOption(SessionRunState.Service)
        if (Option.isSome(runState)) {
          // `SessionRunState` methods resolve their per-directory state through
          // `InstanceRef`; a control call is made outside the run's ambient
          // context, so re-provide the worker instance the run was built with.
          const withInstance = <A, E>(effect: Effect.Effect<A, E>): Effect.Effect<A, E> =>
            options.instance === undefined ? effect : Effect.provideService(effect, InstanceRef, options.instance)
          options.registerControl({
            cancel: (id) => Effect.runPromise(withInstance(runState.value.cancel(id)).pipe(Effect.asVoid)),
            busy: (id) =>
              Effect.runPromise(
                withInstance(runState.value.assertNotBusy(id)).pipe(
                  Effect.match({ onFailure: () => true, onSuccess: () => false }),
                ),
              ),
          })
        }
      }
      if (options.prompt !== false) {
        const prompt = yield* Effect.serviceOption(SessionPrompt.Service)
        if (Option.isSome(prompt)) {
          // A carried prompt persists its user message (`createUserMessage`)
          // before looping; the run's `sessionID` is authoritative so the row
          // always lands on the session this run was admitted for.
          const result =
            carriedPrompt === undefined
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
    }).pipe(Effect.provide(options.layer), Effect.orDie)
    // The layer build (and any fiber it forks) must see the worker's ambient
    // instance context, so `InstanceState`-backed services resolve a directory.
    return options.instance === undefined ? run : Effect.provideService(run, InstanceRef, options.instance)
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

/** The production registry: `"session"` (and the legacy default) run session work. */
export function productionWorkRegistry(session: SessionWork): SessionWorkFactory {
  const factories: Record<string, SessionWork> = { session }
  // Alias the legacy default kind when it differs, built dynamically so the
  // computed key never statically collides with the `session` shorthand (TS1117).
  if (DEFAULT_WORK_KIND !== "session") factories[DEFAULT_WORK_KIND] = session
  return createWorkRegistry(factories)
}

// ---------------------------------------------------------------------------
// Entry bootstrap.
// ---------------------------------------------------------------------------

/**
 * The client handles used when the hub does not (yet) serve the worker's
 * writer / egress / broker ports: in-process adapters so a reduced turn can
 * still complete. Durable writes and egress are the hub's authority, so the
 * fallback reports a non-committed publish and emits nothing.
 */
function directClients(backgroundJob: BackgroundJobClientHandle): WorkerClients {
  // Degraded boot: there is no writer authority, so every write/transaction
  // primitive fails loud instead of silently returning `undefined`.
  const unavailable = (method: string) => async (): Promise<never> => {
    throw new Error(`session worker has no writer authority for "${method}" (degraded boot)`)
  }
  return {
    writer: {
      query: async () => [],
      publish: async () => ({ seq: null, committed: false }),
      begin: unavailable("begin"),
      txQuery: unavailable("txQuery"),
      commit: unavailable("commit"),
      rollback: unavailable("rollback"),
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
    broker: { call: async () => null },
    backgroundJob,
  }
}

/**
 * The worker-appropriate graph replacements: the reduced provider (the isolate
 * serves exactly one serialized model) and the fail-soft `Snapshot` boundary (the
 * isolate cannot run `git`). Kept as one seam so the production entry and its
 * tests wire the identical replacements.
 */
export function workerGraphReplacements(config: WorkerModelConfig): LayerNode.Replacements {
  return [
    [Provider.node, reducedModelProviderLayer(config)],
    [Snapshot.node, workerSnapshotBoundaryLayer()],
  ]
}

/**
 * The worker-side logger. The isolate runs no long-lived Effect fiber (each frame
 * is executed with `Effect.runPromise`), so a bridge is created synchronously from
 * the worker's own instance context. `bridge.fork` then runs each `Effect.logInfo`
 * on the default Effect runtime, so the E2 peer lifecycle lines reach the worker's
 * output instead of being dropped by the no-op default.
 */
export function workerBridgeLogger(snapshot: WorkerSnapshot): WorkerLogger {
  return workerLoggerFromBridge(
    Effect.runSync(EffectBridge.make().pipe(Effect.provideService(InstanceRef, workerInstanceContext(snapshot)))),
  )
}

/** Options for {@link bootstrapSessionWorker}; every field is additive. */
export interface BootstrapSessionWorkerOptions {
  /**
   * Override the worker-side logger. Production omits it (a bridge logger is
   * derived from the snapshot); a test may inject a recorder to observe the E2
   * lines the client construction emits.
   */
  readonly log?: WorkerLogger
  /**
   * Test seam: build the control-port PID reporter. Defaults to
   * `createControlPidReporter(port, workerID, { log })`, so the worker's forwarded
   * `log` reaches the reporter's E4 `pid report failed` line.
   */
  readonly pidReporterFactory?: ControlPidReporterFactory
}

/**
 * Build the reduced layer and install the executor. Split from module scope so
 * tests can import this module (`buildWorkerClients`, the registry) without a
 * worker context; the module only bootstraps when `workerData` is present.
 *
 * The dedicated job control link is always transferred. The writer / egress
 * (plus its dedicated control plane) / broker ports are transferred only when
 * the hub advertises `capabilities.sessionWorkerPorts`, because a port nobody
 * serves would make a session hang; the production hub serves all four.
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
  const log = options.log ?? workerBridgeLogger(init.snapshot)
  // The control-port PID reporter's log is the worker's forwarded `log` (the same
  // seam the client lifecycles use). A test may swap the factory to observe E4.
  const createPidReporter =
    options.pidReporterFactory ??
    ((port: MessagePort, workerID: number, forwarded: WorkerLogger) =>
      createControlPidReporter(port, workerID, { log: forwarded }))
  const jobChannel = new MessageChannel()
  scope.postMessage({ type: "job-port", port: jobChannel.port2 }, [jobChannel.port2 as unknown as Transferable])
  // One full job client backs both the reduced `BackgroundJobClientHandle` seam
  // and the real `BackgroundJob` node (a second client on the same port would
  // steal replies).
  const jobClient = createJobClient(jobPortFromMessagePort(jobChannel.port1))
  // The hub registry routes a settled child to the worker hosting its
  // `parentID`; subscribe at boot so the push reaches consumers. The sink set is
  // the registration handshake returned to the caller.
  const childResultSinks = new Set<ChildResultSink>()
  const unsubscribeChildResults = jobClient.onChildResult((result) => {
    for (const sink of [...childResultSinks]) sink(result)
  })

  let clients: WorkerClients
  let hubProxies: HubProxyClients | undefined
  let pidReporter: PidReporterInterface | undefined
  // The real `SessionPrompt` loop needs the writer-backed Database and an
  // ambient `InstanceRef`; only the full hub-port wiring provides them. The
  // degraded (no-port) boot runs the reduced pipeline.
  const usePrompt = init.capabilities?.sessionWorkerPorts === true
  if (usePrompt) {
    const writerChannel = new MessageChannel()
    const egressChannel = new MessageChannel()
    // A dedicated control plane, so durable wakeups and resync never queue
    // behind the delta stream; without it the egress mesh is dead in production.
    const egressControlChannel = new MessageChannel()
    const brokerChannel = new MessageChannel()
    scope.postMessage(
      {
        type: "ports",
        writer: writerChannel.port2,
        egress: egressChannel.port2,
        egressControl: egressControlChannel.port2,
        broker: brokerChannel.port2,
      },
      [writerChannel.port2, egressChannel.port2, egressControlChannel.port2, brokerChannel.port2].map(
        (port) => port as unknown as Transferable,
      ),
    )
    clients = buildWorkerClients(
      {
        writer: writerChannel.port1,
        egress: egressChannel.port1,
        egressControl: egressControlChannel.port1,
        broker: brokerChannel.port1,
      },
      { jobClient, workerID: reportedWorkerID(init, threadId), log },
    )
    // The hub serves the `SessionStatus` / `Permission` / `Question`
    // authorities over one control port; a single multiplexing client backs all
    // three proxies (each call is correlated by the client's own request id).
    const hubProxyChannel = new MessageChannel()
    scope.postMessage({ type: "hub-proxy", port: hubProxyChannel.port2 }, [
      hubProxyChannel.port2 as unknown as Transferable,
    ])
    const hubProxyClient = createHubProxyClient(hubProxyChannel.port1)
    hubProxies = { sessionStatus: hubProxyClient, permission: hubProxyClient, question: hubProxyClient }
    // The shell tool's detached children are reaped by the hub, so the same
    // control port carries their `pid-report` frames (the hub distinguishes the
    // two frame shapes). Only the full-port boot owns children.
    pidReporter = createPidReporter(hubProxyChannel.port1, reportedWorkerID(init, threadId), log)
  } else {
    clients = directClients(backgroundJobHandle(jobClient))
  }

  const config = modelConfig(init.model)
  // The worker runs the model-facing plugin hooks in its own isolate; build the
  // host from the shipped hooks so their `chat.params`/`chat.headers` mutations
  // reach the reduced LLM exactly as flag-off. Absent hooks keep the no-op.
  const plugins = init.plugins === undefined ? undefined : createWorkerPluginHost(init.plugins)
  const modelClient = llmModelClientLayer(config).pipe(
    Layer.provide(reducedLlmLayer(init.snapshot, config, undefined, plugins)),
  )

  const runtime = buildSessionWorkerLayer({
    // The full runnable graph needs the writer-backed Database and the serialized
    // model catalog the hub ships; the degraded boot runs the reduced pipeline.
    // The runnable graph also carries the worker EventV2 bridge, which rides the
    // transferred `egress` port and is disposed with the layer scope, so it is
    // built exactly when the events/egress port is available (`usePrompt`).
    ...(usePrompt ? {} : { nodes: PIPELINE_SESSION_NODES }),
    snapshot: init.snapshot,
    env: init.env,
    clients,
    modelClient,
    backgroundJob: backgroundJobClientLayer(jobClient),
    // The worker serves exactly one serialized model, so the reduced provider is
    // the graph's provider (the real loop's `LLM` and `SessionPrompt` use it), and
    // snapshot tracking is a fail-soft no-op (the isolate cannot run `git`).
    replacements: workerGraphReplacements(config),
    // The worker-hosted plugin host reaches the graph's `LLM`/`Plugin` node too,
    // not just the `ModelClient` sealed below, so plugin-set sampling/headers
    // apply identically in both. Absent keeps the graph's no-op pass-through.
    ...(plugins === undefined ? {} : { plugins }),
    ...(hubProxies === undefined ? {} : { hubProxies }),
    ...(pidReporter === undefined ? {} : { pidReporter }),
  })

  // The worker-local run control is registered once the first `run` resolves the
  // worker's `SessionRunState`; the executor proxy reads it live.
  let control: SessionRunControl | undefined
  const work = productionWorkRegistry(
    createSessionWork({
      layer: runtime.layer,
      writer: clients.writer,
      threadId,
      prompt: usePrompt,
      instance: workerInstanceContext(init.snapshot),
      registerControl: (registered) => {
        control = registered
      },
    }),
  )
  const slot = init.workerID
  installSessionExecutor(scope, {
    work,
    threadId,
    // The control is registered when the first `run` resolves the worker's
    // `SessionRunState`; the proxy reads it live so a cancel after registration
    // reaches the real loop (table B). Before that there is nothing to cancel.
    control: {
      cancel: (sessionID) => control?.cancel(sessionID) ?? Promise.resolve(),
      busy: (sessionID) => control?.busy(sessionID) ?? Promise.resolve(false),
    },
    // Dynamic OS thread name (Linux-only, fail-soft): `ocs<slot>` at rest and
    // `ocs<slot>:<last6>` while a run is live. `onIdle` fires on every terminal
    // (settle, cooperative cancel, interrupt), so the name always resets. Both
    // callbacks are fail-soft in `execute.ts`, so naming never unwinds a run.
    // Skipped when the hub shipped no slot id.
    ...(slot === undefined
      ? {}
      : {
          onRunStart: (sessionID) => setThreadName(sessionThreadName(slot, sessionID)),
          onIdle: () => Effect.sync(() => setThreadName(sessionThreadName(slot))),
        }),
  })

  return {
    onChildResult(sink) {
      childResultSinks.add(sink)
      return () => childResultSinks.delete(sink)
    },
    dispose() {
      unsubscribeChildResults()
      childResultSinks.clear()
    },
  }
}

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
  const worktree = snapshot.worktree ?? snapshot.directory
  return {
    directory: snapshot.directory,
    worktree,
    project: {
      id: Project.ID.make(snapshot.projectId ?? snapshot.directory),
      worktree,
      ...(snapshot.projectVcs === undefined ? {} : { vcs: snapshot.projectVcs }),
      // `Project.Info` requires a time span and sandboxes; the readers only touch
      // `id`/`vcs`, so a zero span and no sandboxes is a faithful minimum.
      time: { created: 0, updated: 0 },
      sandboxes: [],
    },
  }
}

if (workerData !== null && workerData !== undefined) {
  const init = parseSessionWorkerInit(workerData)
  // Adopt the shipped role BEFORE any heavy bootstrap, so a dynamic app-runtime
  // import during bootstrap sees `session` and never contributes a nested hub.
  setProcessRole(init.role)
  // Name the OS thread for its slot before any heavy bootstrap. Fail-soft and
  // Linux-only (see `@/util/thread-name`); skipped when the hub shipped no slot.
  if (init.workerID !== undefined) setThreadName(sessionThreadName(init.workerID))
  bootstrapSessionWorker(init, globalThis as unknown as WorkerScope)
}
