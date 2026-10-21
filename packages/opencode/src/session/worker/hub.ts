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

import { Context, Effect, Layer } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import type { SessionID } from "@/session/schema"
import { createDispatcher, type CancellationConfig, type Dispatcher } from "./dispatch"
import { createJobRegistry, type JobRegistry } from "./job-registry"
import { createResultRouter, serveJobRegistry, type JobPort } from "./job-client"
import {
  createExecutorWorker,
  createExecutorWorkerPool,
  createSessionDispatch,
  type ExecutorTransport,
  type ExecutorWorkerPool,
  type SessionDispatch,
  type SessionExecutionResult,
  type SessionExecutorInbound,
  type SessionExecutorOutbound,
} from "./execute"
import type { CancelOutcome } from "./cancel"

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
  /** Dispatch `sessionID` to a worker and resolve with its result (admission-aware). */
  ensureRunning(sessionID: SessionID): Promise<SessionExecutionResult>
  startShell(sessionID: SessionID): Promise<SessionExecutionResult>
  assertNotBusy(sessionID: SessionID): Promise<boolean>
  /** The UI-stop path: cooperative checkpoint, then terminate after the grace window. */
  cancel(sessionID: SessionID): Promise<CancelOutcome | undefined>
  cancelAll(): Promise<void>
  terminateAll(): Promise<void>
}

export class SessionHubService extends Context.Service<SessionHubService, SessionHub>()("@opencode/SessionHub") {}

export interface SessionHubOptions {
  readonly pool: ExecutorWorkerPool
  readonly headroom?: number
  readonly admissionCapacity?: number
  readonly childCapacity?: number
  readonly headroomIdleReapMs?: number
  readonly cancellation?: CancellationConfig
  readonly onDegraded?: (workerID: number) => void
  /**
   * Pre-built job registry (tests). Defaults to one wired to route a settled
   * child's result to the worker hosting its `parentID` (plan 9.4).
   */
  readonly jobs?: JobRegistry
}

/** Compose the dispatcher + executor pool + cooperative cancel into one hub. */
export function createSessionHub(options: SessionHubOptions): SessionHub {
  const dispatcher = createDispatcher({
    pool: options.pool,
    ...(options.headroom === undefined ? {} : { headroom: options.headroom }),
    ...(options.admissionCapacity === undefined ? {} : { admissionCapacity: options.admissionCapacity }),
    ...(options.childCapacity === undefined ? {} : { childCapacity: options.childCapacity }),
    ...(options.headroomIdleReapMs === undefined ? {} : { headroomIdleReapMs: options.headroomIdleReapMs }),
    ...(options.cancellation === undefined ? {} : { cancellation: options.cancellation }),
    ...(options.onDegraded === undefined ? {} : { onDegraded: options.onDegraded }),
  })
  const dispatch: SessionDispatch = createSessionDispatch({ dispatcher, pool: options.pool })

  // One registry for the whole hub. A settled child is routed to the worker
  // currently hosting its `parentID` (the dispatcher's key→worker map).
  const jobPorts = new Map<number, JobPort>()
  const portForSession = (sessionID: string): JobPort | undefined => {
    const workerID = dispatcher.registry.get(sessionID as SessionID)?.workerID
    return workerID === undefined ? undefined : jobPorts.get(workerID)
  }
  const jobs = options.jobs ?? createJobRegistry({ routeResult: createResultRouter(portForSession) })
  const jobDisposers = new Map<number, () => void>()

  const attachJobPort = (workerID: number, port: JobPort): () => void => {
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

  return {
    dispatcher,
    jobs,
    attachJobPort,
    ensureRunning: (sessionID) => dispatch.ensureRunning(sessionID),
    startShell: (sessionID) => dispatch.startShell(sessionID),
    assertNotBusy: (sessionID) => dispatch.assertNotBusy(sessionID),
    cancel: (sessionID) => dispatcher.cancelWithGrace(sessionID),
    cancelAll: () => dispatch.cancelAll(),
    async terminateAll() {
      for (const dispose of jobDisposers.values()) dispose()
      await dispatcher.terminateAll()
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
  /** Called with each `MessagePort` the worker transfers (the job control link). */
  readonly onPort?: (port: MessagePort) => void
}

const DEFAULT_TRANSPORT_TIMEOUT_MS = 30_000

/** Extract a transferred port from a worker frame (`{type:"job-port", port}` or a bare port). */
function transferredPort(data: unknown): MessagePort | undefined {
  if (typeof MessagePort !== "undefined" && data instanceof MessagePort) return data
  if (typeof data !== "object" || data === null) return undefined
  const frame = data as { readonly type?: unknown; readonly port?: unknown }
  if (frame.type !== "job-port") return undefined
  return typeof MessagePort !== "undefined" && frame.port instanceof MessagePort ? frame.port : undefined
}

function mergeEnv(overlay?: Record<string, string | undefined>): Record<string, string> {
  const env: Record<string, string> = {}
  for (const [key, value] of Object.entries(process.env)) {
    if (value !== undefined) env[key] = value
  }
  for (const [key, value] of Object.entries(overlay ?? {})) {
    if (value === undefined) delete env[key]
    else env[key] = value
  }
  return env
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
  const worker = new Worker(options.entry, {
    env: mergeEnv(options.env),
    ...(options.workerData === undefined ? {} : { workerData: options.workerData }),
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

  worker.onmessage = (event: MessageEvent) => {
    const port = transferredPort(event.data)
    if (port !== undefined) {
      options.onPort?.(port)
      return
    }
    const message = event.data as SessionExecutorOutbound
    messages.push(message)
    for (const waiter of waiters) {
      if (!waiter.predicate(message)) continue
      clearTimeout(waiter.timer)
      waiters.delete(waiter)
      waiter.resolve(message)
    }
  }

  worker.onerror = (event: ErrorEvent) => {
    alive = false
    failWaiters(new Error(`session worker error: ${event.message}`))
  }

  return {
    post(message: SessionExecutorInbound) {
      if (!alive) throw new Error("cannot post to a terminated session worker")
      worker.postMessage(message)
    },
    waitFor(predicate, waitOptions) {
      const buffered = messages.find(predicate)
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
}

/** Production `ExecutorWorkerPool`: K executor workers speaking the frame protocol. */
export function createBunExecutorWorkerPool(options: BunExecutorWorkerPoolOptions): ExecutorWorkerPool {
  return createExecutorWorkerPool({
    k: options.k,
    spawn: (id) => {
      const onJobPort = options.onJobPort
      const transport = createBunExecutorTransport({
        entry: options.entry,
        ...(options.env === undefined ? {} : { env: options.env }),
        ...(options.workerData === undefined ? {} : { workerData: options.workerData }),
        ...(options.timeoutMs === undefined ? {} : { timeoutMs: options.timeoutMs }),
        ...(onJobPort === undefined ? {} : { onPort: (port) => onJobPort(id, port) }),
      })
      return createExecutorWorker({
        id,
        transport,
        ...(options.workKind === undefined ? {} : { workKind: options.workKind }),
        ...(options.timeoutMs === undefined ? {} : { timeoutMs: options.timeoutMs }),
      })
    },
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
