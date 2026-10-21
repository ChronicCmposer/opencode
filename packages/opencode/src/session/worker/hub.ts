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
import { mergeEnv } from "@/session/broker/env"
import type { SessionID } from "@/session/schema"
import { defined } from "@/util/object"
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

/**
 * The worker currently hosting a session, as the dispatcher knows it. Declared
 * locally so the hub can consume it without importing the dispatcher's registry.
 * `dispatch.ts` must add this method to `Dispatcher` (consolidation request).
 */
interface WorkerLookup {
  workerForSession(sessionID: SessionID): number | undefined
}

/** Compose the dispatcher + executor pool + cooperative cancel into one hub. */
export function createSessionHub(options: SessionHubOptions): SessionHub {
  const dispatcher = createDispatcher({
    pool: options.pool,
    ...defined({
      headroom: options.headroom,
      admissionCapacity: options.admissionCapacity,
      childCapacity: options.childCapacity,
      headroomIdleReapMs: options.headroomIdleReapMs,
      cancellation: options.cancellation,
      onDegraded: options.onDegraded,
    }),
  })
  const dispatch: SessionDispatch = createSessionDispatch({ dispatcher, pool: options.pool })

  // One registry for the whole hub. A settled child is routed to the worker
  // currently hosting its `parentID` (the dispatcher's key→worker map).
  const jobPorts = new Map<number, JobPort>()
  // The hub asks the dispatcher for a session's worker instead of reaching into
  // its registry. `dispatch.ts` still needs to implement `workerForSession`
  // (consolidation request); the registry read is a temporary fallback.
  const workerForSession = (sessionID: string): number | undefined => {
    const lookup = dispatcher as unknown as Partial<WorkerLookup>
    return lookup.workerForSession?.(sessionID as SessionID) ?? dispatcher.registry.get(sessionID as SessionID)?.workerID
  }
  const portForSession = (sessionID: string): JobPort | undefined => {
    const workerID = workerForSession(sessionID)
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
  /** Maximum buffered outbound frames kept for late `waitFor` matches. */
  readonly bufferCapacity?: number
  /** Called with each `MessagePort` the worker transfers (the job control link). */
  readonly onPort?: (port: MessagePort) => void
}

const DEFAULT_TRANSPORT_TIMEOUT_MS = 30_000

/** Cap on buffered frames, so a transport nobody waits on cannot grow unbounded. */
export const DEFAULT_TRANSPORT_BUFFER_CAPACITY = 1024

/** Extract a transferred port from a worker frame (`{type:"job-port", port}` or a bare port). */
function transferredPort(data: unknown): MessagePort | undefined {
  if (typeof MessagePort !== "undefined" && data instanceof MessagePort) return data
  if (typeof data !== "object" || data === null) return undefined
  const frame = data as { readonly type?: unknown; readonly port?: unknown }
  if (frame.type !== "job-port") return undefined
  return typeof MessagePort !== "undefined" && frame.port instanceof MessagePort ? frame.port : undefined
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
  const takeBuffered = (predicate: (message: SessionExecutorOutbound) => boolean): SessionExecutorOutbound | undefined => {
    const index = messages.findIndex(predicate)
    if (index === -1) return undefined
    return messages.splice(index, 1)[0]
  }

  worker.onmessage = (event: MessageEvent) => {
    const port = transferredPort(event.data)
    if (port !== undefined) {
      options.onPort?.(port)
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
}

/** Production `ExecutorWorkerPool`: K executor workers speaking the frame protocol. */
export function createBunExecutorWorkerPool(options: BunExecutorWorkerPoolOptions): ExecutorWorkerPool {
  return createExecutorWorkerPool({
    k: options.k,
    spawn: (id) => {
      const onJobPort = options.onJobPort
      const transport = createBunExecutorTransport({
        entry: options.entry,
        ...defined({
          env: options.env,
          workerData: options.workerData,
          timeoutMs: options.timeoutMs,
          onPort: onJobPort === undefined ? undefined : (port: MessagePort) => onJobPort(id, port),
        }),
      })
      return createExecutorWorker({
        id,
        transport,
        ...defined({ workKind: options.workKind, timeoutMs: options.timeoutMs }),
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
