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

import { threadId, workerData } from "node:worker_threads"
import { Effect, Layer, Option } from "effect"
import { SessionPrompt } from "@/session/prompt"
import { InstanceRef } from "@/effect/instance-ref"
import type { InstanceContext } from "@/project/instance-context"
import { Provider } from "@/provider/provider"
import { createWorkerWriterClient } from "./writer-client"
import { createWorkerEgressClient } from "./egress-client"
import { createWorkerBrokerClient } from "./broker-client"
import { createBackgroundJobClient, createJobClient, jobPortFromMessagePort, type JobClient } from "./job-client"
import {
  DEFAULT_WORK_KIND,
  UnknownWorkKindError,
  installSessionExecutor,
  type SessionExecutionResult,
  type SessionWork,
  type SessionWorkFactory,
} from "./execute"
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
  type EventEgressClientHandle,
  type HubProxyClients,
  type PipelineMessage,
  type WorkerEnv,
  type WorkerModelConfig,
  type WorkerSnapshot,
  type WriterClientHandle,
} from "./layer"

export interface SessionWorkerModel {
  readonly baseUrl: string
  readonly apiKey: string
  readonly model: string
  /** Provider identity; defaults to `model` for the reduced bootstrap. */
  readonly providerID?: string
  /** Provider API model id; defaults to `model`. */
  readonly apiId?: string
  /** Provider npm package; defaults to the OpenAI-compatible client. */
  readonly npm?: string
  readonly headers?: Readonly<Record<string, string>>
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
  readonly capabilities?: SessionWorkerCapabilities
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

/** The four client handles the reduced session layer installs. */
export interface WorkerClients {
  readonly writer: WriterClientHandle
  readonly egress: EventEgressClientHandle
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
 */
export function buildWorkerClients(ports: WorkerClientPorts, options: { jobClient?: JobClient } = {}): WorkerClients {
  const writer = requirePort(ports.writer, "writer")
  const egress = requirePort(ports.egress, "egress")
  const broker = requirePort(ports.broker, "broker")
  const job = options.jobClient ?? createBackgroundJobClient(requirePort(ports.backgroundJob, "backgroundJob"))
  return {
    writer: createWorkerWriterClient(writer),
    egress: createWorkerEgressClient(egress, ports.egressControl === undefined ? {} : { controlPort: ports.egressControl }),
    broker: createWorkerBrokerClient(broker),
    backgroundJob: backgroundJobHandle(job),
  }
}

/** Narrow the full job client to the reduced `BackgroundJobClientHandle` seam. */
function backgroundJobHandle(client: Pick<JobClient, "wait" | "cancel">): BackgroundJobClientHandle {
  return { wait: (id, timeoutMs) => client.wait(id, timeoutMs), cancel: (id) => client.cancel(id) }
}

// ---------------------------------------------------------------------------
// Model bootstrap.
// ---------------------------------------------------------------------------

function modelConfig(model: SessionWorkerModel): WorkerModelConfig {
  return {
    providerID: model.providerID ?? model.model,
    modelID: model.model,
    apiId: model.apiId ?? model.model,
    npm: model.npm ?? "@ai-sdk/openai-compatible",
    baseUrl: model.baseUrl,
    apiKey: model.apiKey,
    headers: model.headers ?? {},
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
}

/**
 * The real `SessionWork`. When the layer provides `SessionPrompt` and the
 * caller allows it, the worker runs the actual opencode session loop —
 * `SessionPrompt.loop` — which drives `SessionProcessor` over the worker-local
 * `LLM` and persists through the writer boundary. Otherwise it falls back to the
 * one-shot `SessionPipeline` reduction (pipeline-only graphs and the degraded
 * no-hub-port boot).
 */
export function createSessionWork(options: SessionWorkOptions): SessionWork {
  return (sessionID) => {
    const run = Effect.gen(function* () {
      if (options.prompt !== false) {
        const prompt = yield* Effect.serviceOption(SessionPrompt.Service)
        if (Option.isSome(prompt)) {
          const result = yield* prompt.value.loop(new SessionPrompt.LoopInput({ sessionID }))
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
  return {
    writer: { query: async () => [], publish: async () => ({ seq: null, committed: false }) },
    egress: { emit: () => {} },
    broker: { call: async () => null },
    backgroundJob,
  }
}

/**
 * Build the reduced layer and install the executor. Split from module scope so
 * tests can import this module (`buildWorkerClients`, the registry) without a
 * worker context; the module only bootstraps when `workerData` is present.
 *
 * The dedicated job control link is always transferred. The writer / egress /
 * broker ports are transferred only when the hub advertises
 * `capabilities.sessionWorkerPorts`, because a port nobody serves would make a
 * session hang (consolidation: the hub must serve them).
 */
export function bootstrapSessionWorker(init: SessionWorkerInit, scope: WorkerScope): void {
  const jobChannel = new MessageChannel()
  scope.postMessage({ type: "job-port", port: jobChannel.port2 }, [jobChannel.port2 as unknown as Transferable])
  // One full job client backs both the reduced `BackgroundJobClientHandle` seam
  // and the real `BackgroundJob` node (a second client on the same port would
  // steal replies).
  const jobClient = createJobClient(jobPortFromMessagePort(jobChannel.port1))

  let clients: WorkerClients
  let hubProxies: HubProxyClients | undefined
  // The real `SessionPrompt` loop needs the writer-backed Database and an
  // ambient `InstanceRef`; only the full hub-port wiring provides them. The
  // degraded (no-port) boot runs the reduced pipeline.
  const usePrompt = init.capabilities?.sessionWorkerPorts === true
  if (usePrompt) {
    const writerChannel = new MessageChannel()
    const egressChannel = new MessageChannel()
    const brokerChannel = new MessageChannel()
    scope.postMessage(
      {
        type: "ports",
        writer: writerChannel.port2,
        egress: egressChannel.port2,
        broker: brokerChannel.port2,
      },
      [writerChannel.port2, egressChannel.port2, brokerChannel.port2].map(
        (port) => port as unknown as Transferable,
      ),
    )
    clients = buildWorkerClients(
      { writer: writerChannel.port1, egress: egressChannel.port1, broker: brokerChannel.port1 },
      { jobClient },
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
  } else {
    clients = directClients(backgroundJobHandle(jobClient))
  }

  const config = modelConfig(init.model)
  const modelClient = llmModelClientLayer(config).pipe(Layer.provide(reducedLlmLayer(init.snapshot, config)))

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
    // the graph's provider (the real loop's `LLM` and `SessionPrompt` use it).
    replacements: [[Provider.node, reducedModelProviderLayer(config)]],
    ...(hubProxies === undefined ? {} : { hubProxies }),
  })

  const work = productionWorkRegistry(
    createSessionWork({
      layer: runtime.layer,
      writer: clients.writer,
      threadId,
      prompt: usePrompt,
      instance: workerInstanceContext(init.snapshot.directory),
    }),
  )
  installSessionExecutor(scope, { work, threadId })
}

/**
 * The worker's ambient `InstanceRef`. The isolate owns no project loader, so the
 * snapshot directory is both the working directory and the worktree; `project`
 * is unused by the reduced session graph.
 */
function workerInstanceContext(directory: string): InstanceContext {
  return { directory, worktree: directory, project: undefined as never }
}

if (workerData !== null && workerData !== undefined) {
  bootstrapSessionWorker(parseSessionWorkerInit(workerData), globalThis as unknown as WorkerScope)
}
