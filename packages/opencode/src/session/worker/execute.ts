// Phase 5 — session execution seam (plan tasks 5.1–5.7).
//
// The run-state machine (`Runner` per session key) MOVES INTO the worker; the
// hub keeps only the key→worker map + admission (owned by `dispatch.ts`). This
// module provides the portable worker-local machine, the executor wire protocol,
// the worker entry installer, and the hub-side adapters that satisfy the
// `WorkerPool`/`SessionWorker` seams and forward control (`cancel`/
// `assertNotBusy`).
//
// Transport is the worker's default channel; frames are JSON. Work crosses the
// isolate as a `kind` string (never a closure): `SessionWorkFactory` resolves a
// kind to a session work Effect. The default kind is `"session"`; the worker
// registry (`session-worker.ts`) resolves it to the reduced-`AppLayer` pipeline
// and aliases the legacy `"noop"`.

import { Cause, Effect, Exit, Schema, Scope } from "effect"
import type { Latch } from "effect"
import { Runner } from "@/effect/runner"
import type { SessionID } from "@/session/schema"
import type { CancelFrame, CancelPort } from "./cancel"

/** JSON-safe result of one session run (the full message crosses via egress). */
export interface SessionExecutionResult {
  readonly sessionID: string
  readonly value?: unknown
}

export type SessionWork = (sessionID: SessionID) => Effect.Effect<SessionExecutionResult>
export type SessionWorkFactory = (kind: string) => SessionWork

/**
 * The run control the worker-local `SessionRunState` exposes to the executor
 * (Phase 4). The executor's own machine (table A) is only an outer `ensureRunning`
 * guard; the real `SessionPrompt.loop` runs on the worker-local `SessionRunState`
 * runner (table B). Routing `cancel`/`assertNotBusy` here makes cancel actually
 * interrupt the loop — and makes `assertNotBusy` tell the truth after a cancel.
 *
 * The methods cross no isolate boundary (the worker resolves table B in-process
 * and hands the executor a control): the worker `createSessionWork` registers it.
 */
export interface SessionRunControl {
  cancel(sessionID: SessionID): Promise<void>
  busy(sessionID: SessionID): Promise<boolean>
}

/**
 * Production work kind. The worker registry (`session-worker.ts`
 * `productionWorkRegistry`) resolves `"session"` to the real reduced-layer
 * session work and aliases the legacy `"noop"` to it, so the hub and worker
 * agree.
 *
 * Typed as `string` (not the `"session"` literal) because the worker registry
 * currently builds its alias table as `{ session, [DEFAULT_WORK_KIND]: session }`;
 * a literal type would make the computed key statically collide with the
 * shorthand (TS1117). Consolidation: `session-worker.ts` (not owned here) should
 * alias the literal `"noop"` instead, after which this can be narrowed.
 */
export const DEFAULT_WORK_KIND: string = "session"

/** Raised when a worker is asked for a work kind it has no factory for. */
export class UnknownWorkKindError extends Schema.TaggedErrorClass<UnknownWorkKindError>()("UnknownWorkKindError", {
  message: Schema.String,
  kind: Schema.String,
}) {}

/** Worker-local busy error; mirrors `Session.BusyError` without the heavy import. */
export class SessionBusyError extends Schema.TaggedErrorClass<SessionBusyError>()("SessionBusyError", {
  sessionID: Schema.String,
}) {}

export interface SessionRunMachineOptions {
  readonly scope: Scope.Scope
  readonly onIdle?: (sessionID: SessionID) => Effect.Effect<void>
  readonly onBusy?: (sessionID: SessionID) => Effect.Effect<void>
}

/** The worker-local run-state machine: one `Runner` per session key. */
export interface SessionRunMachine {
  busy(sessionID: SessionID): boolean
  ensureRunning(
    sessionID: SessionID,
    work: Effect.Effect<SessionExecutionResult>,
  ): Effect.Effect<SessionExecutionResult>
  startShell(
    sessionID: SessionID,
    work: Effect.Effect<SessionExecutionResult>,
    ready?: Latch.Latch,
  ): Effect.Effect<SessionExecutionResult, Runner.Busy>
  cancel(sessionID: SessionID): Effect.Effect<void>
  assertNotBusy(sessionID: SessionID): Effect.Effect<void, SessionBusyError>
  readonly cancelAll: Effect.Effect<void>
}

export function createSessionRunMachine(options: SessionRunMachineOptions): SessionRunMachine {
  const table = Runner.createRunnerTable<SessionID, SessionExecutionResult>({
    scope: options.scope,
    onIdle: options.onIdle,
    onBusy: options.onBusy,
  })

  return {
    busy: (sessionID) => table.busy(sessionID),
    ensureRunning: (sessionID, work) => table.ensureRunning(sessionID, work),
    startShell: (sessionID, work, ready) => table.startShell(sessionID, work, ready),
    cancel: (sessionID) => table.cancel(sessionID),
    assertNotBusy: (sessionID) =>
      table.busy(sessionID) ? Effect.fail(new SessionBusyError({ sessionID })) : Effect.void,
    cancelAll: table.cancelAll,
  }
}

// --- Executor wire protocol -------------------------------------------------

export type SessionExecutorInbound =
  | { readonly type: "run"; readonly id: number; readonly sessionID: SessionID; readonly kind: string }
  | { readonly type: "startShell"; readonly id: number; readonly sessionID: SessionID; readonly kind: string }
  | { readonly type: "cancel"; readonly id: number; readonly sessionID: SessionID }
  | { readonly type: "assertNotBusy"; readonly id: number; readonly sessionID: SessionID }
  | { readonly type: "ping"; readonly id: number }

export interface SessionExecutorReady {
  readonly type: "ready"
  readonly threadId: number
}
export interface SessionExecutorPong {
  readonly type: "pong"
  readonly id: number
}
export interface SessionExecutorBusy {
  readonly type: "busy"
  readonly id: number
  readonly busy: boolean
}
export interface SessionExecutorCancelled {
  readonly type: "cancelled"
  readonly id: number
}
export interface SessionExecutorDone {
  readonly type: "done"
  readonly id: number
  readonly result: SessionExecutionResult
}
export interface SessionExecutorFailed {
  readonly type: "error"
  readonly id: number
  readonly tag?: string
  readonly message: string
}

export type SessionExecutorOutbound =
  | SessionExecutorReady
  | SessionExecutorPong
  | SessionExecutorBusy
  | SessionExecutorCancelled
  | SessionExecutorDone
  | SessionExecutorFailed

export type SessionEffectRunner = <A, E>(effect: Effect.Effect<A, E>) => Promise<A>

const runEffect: SessionEffectRunner = (effect) => Effect.runPromise(effect)

const tagOf = (error: unknown): string | undefined =>
  error !== null &&
  typeof error === "object" &&
  "_tag" in error &&
  typeof (error as { _tag: unknown })._tag === "string"
    ? (error as { _tag: string })._tag
    : undefined

const messageOf = (error: unknown): string => (error instanceof Error ? error.message : String(error))

const isAddressed = (message: SessionExecutorOutbound, id: number): boolean => "id" in message && message.id === id

/** Shape any thrown/rejected value into a failed reply, preserving its `_tag`. */
const failed = (id: number, error: unknown): SessionExecutorFailed => {
  const tag = tagOf(error)
  return { type: "error", id, ...(tag === undefined ? {} : { tag }), message: messageOf(error) }
}

export interface SessionExecutorOptions {
  readonly machine: SessionRunMachine
  readonly work: SessionWorkFactory
  /**
   * Worker-local run control (table B). When supplied, `cancel`/`assertNotBusy`
   * route here so they reach the real `SessionPrompt.loop`; otherwise the
   * executor's own machine answers.
   */
  readonly control?: SessionRunControl
  /** Effect runner seam; defaults to `Effect.runPromise`. */
  readonly run?: SessionEffectRunner
}

export interface SessionExecutor {
  handle(message: SessionExecutorInbound): Promise<SessionExecutorOutbound>
}

/** Route one inbound frame to the worker-local machine and produce one reply. */
export function createSessionExecutor(options: SessionExecutorOptions): SessionExecutor {
  const run = options.run ?? runEffect

  /** Whether the worker-local run (table B) is busy; the machine answers otherwise. */
  const busy = (sessionID: SessionID): Promise<boolean> =>
    options.control === undefined ? Promise.resolve(options.machine.busy(sessionID)) : options.control.busy(sessionID)

  /** Cancel the worker-local run (table B); the machine answers otherwise. */
  const cancel = (sessionID: SessionID): Promise<void> =>
    options.control === undefined ? run(options.machine.cancel(sessionID)) : options.control.cancel(sessionID)

  const settle = (
    id: number,
    effect: Effect.Effect<SessionExecutionResult, unknown>,
  ): Effect.Effect<SessionExecutorOutbound> =>
    effect.pipe(
      // `matchCause` (not `match`) so a defect — not just a typed failure —
      // folds into a structured error frame instead of rejecting the handler.
      Effect.matchCause({
        onFailure: (cause): SessionExecutorOutbound => failed(id, Cause.squash(cause)),
        onSuccess: (result): SessionExecutorOutbound => ({ type: "done", id, result }),
      }),
    )

  const runWork = async (
    id: number,
    sessionID: SessionID,
    kind: string,
    mode: "run" | "startShell",
  ): Promise<SessionExecutorOutbound> => {
    let work: SessionWork
    try {
      work = options.work(kind)
    } catch (error) {
      return failed(id, error)
    }
    const effect =
      mode === "run"
        ? options.machine.ensureRunning(sessionID, work(sessionID))
        : options.machine.startShell(sessionID, work(sessionID))
    return run(settle(id, effect))
  }

  const handle = async (message: SessionExecutorInbound): Promise<SessionExecutorOutbound> => {
    switch (message.type) {
      case "ping":
        return { type: "pong", id: message.id }
      case "assertNotBusy":
        return { type: "busy", id: message.id, busy: await busy(message.sessionID) }
      case "cancel":
        await cancel(message.sessionID)
        return { type: "cancelled", id: message.id }
      case "run":
        return runWork(message.id, message.sessionID, message.kind, "run")
      case "startShell":
        return runWork(message.id, message.sessionID, message.kind, "startShell")
      default:
        return { type: "error", id: 0, message: "unknown executor request" }
    }
  }

  return { handle }
}

export interface ExecutorWorkerScope {
  onmessage: ((event: { data: unknown }) => void) | null
  postMessage(message: SessionExecutorOutbound): void
}

export interface SessionExecutorInstallOptions {
  readonly work: SessionWorkFactory
  /** Worker-local run control; forwarded to the installed executor (Phase 4). */
  readonly control?: SessionRunControl
  readonly onIdle?: (sessionID: SessionID) => Effect.Effect<void>
  readonly onBusy?: (sessionID: SessionID) => Effect.Effect<void>
  readonly threadId?: number
  /** Scope seam for tests; a fresh scope is created (and owned) when omitted. */
  readonly scope?: Scope.Scope
}

/** An installed executor plus disposal of the scope it owns when none was supplied. */
export interface InstalledSessionExecutor extends SessionExecutor {
  /** Close the executor's owned worker scope; no-op when a scope was supplied. */
  dispose(): Promise<void>
}

/**
 * Install the executor on a worker global scope: announce readiness, then route
 * every inbound frame. Mirrors `bootstrap.ts`'s ready + request/response shape.
 */
export function installSessionExecutor(
  scope: ExecutorWorkerScope,
  options: SessionExecutorInstallOptions,
): InstalledSessionExecutor {
  const ownsScope = options.scope === undefined
  const workerScope = options.scope ?? Effect.runSync(Scope.make())
  const machine = createSessionRunMachine({ scope: workerScope, onIdle: options.onIdle, onBusy: options.onBusy })
  const executor = createSessionExecutor({ machine, work: options.work, control: options.control })

  scope.onmessage = (event) => {
    const message = event.data as SessionExecutorInbound | undefined
    if (message === null || message === undefined || typeof message !== "object") return
    if (typeof (message as { type?: unknown }).type !== "string") return
    void executor.handle(message).then(
      (outbound) => scope.postMessage(outbound),
      (error) =>
        scope.postMessage({
          type: "error",
          id: (message as { id?: number }).id ?? 0,
          message: messageOf(error),
        }),
    )
  }

  scope.postMessage({ type: "ready", threadId: options.threadId ?? 0 })
  return {
    handle: executor.handle,
    dispose: async () => {
      if (!ownsScope) return
      await Effect.runPromise(Scope.close(workerScope, Exit.void))
    },
  }
}

// --- Hub-side adapter -------------------------------------------------------

export interface ExecutorRun {
  readonly sessionID: SessionID
  readonly depth: number
}

/** Structural mirror of dispatch's `SessionWorker` (keeps this module decoupled). */
export interface ExecutorSessionWorker {
  readonly id: number
  readonly alive: boolean
  execute(run: ExecutorRun): Promise<void>
  terminate(): Promise<void>
}

export type ExecutorWorkerKind = "foreground" | "headroom"

/** The transport the hub adapter needs; a worker harness handle satisfies it. */
export interface ExecutorTransport {
  post(message: SessionExecutorInbound): void
  waitFor(
    match: (message: SessionExecutorOutbound) => boolean,
    options?: { timeoutMs?: number },
  ): Promise<SessionExecutorOutbound>
  readonly alive: boolean
  terminate(code?: number): Promise<number>
}

export interface ExecutorWorker extends ExecutorSessionWorker {
  cancel(sessionID: SessionID): Promise<void>
  assertNotBusy(sessionID: SessionID): Promise<boolean>
  resultFor(sessionID: SessionID): SessionExecutionResult | undefined
  /**
   * Cooperative cancel control port (plan 11.2). `dispatch.ts`'s
   * `CancellationConfig.portFor` returns this so a cancel notifies the worker and
   * observes its ack within the grace window. Optional so minimal test fakes
   * remain valid; `createExecutorWorker` always provides it.
   */
  readonly cancelPort?: CancelPort
}

export interface ExecutorCancelPortOptions {
  readonly cancel: (sessionID: SessionID) => Promise<void>
}

/**
 * Adapt an executor worker's request/reply `cancel` into the Phase-11
 * `CancelPort` shape: a `cancel` frame drives `cancel`, and its settlement is
 * echoed back as a `cancel-ack` (accepted on success, rejected otherwise).
 */
export function createExecutorCancelPort(options: ExecutorCancelPortOptions): CancelPort {
  const handlers = new Set<(frame: CancelFrame) => void>()
  const emit = (frame: CancelFrame): void => {
    for (const handler of [...handlers]) handler(frame)
  }
  return {
    notify(frame) {
      if (frame.type !== "cancel") return
      options.cancel(frame.sessionID as SessionID).then(
        () => emit({ type: "cancel-ack", sessionID: frame.sessionID, accepted: true }),
        () => emit({ type: "cancel-ack", sessionID: frame.sessionID, accepted: false }),
      )
    },
    onMessage(handler) {
      handlers.add(handler)
      return () => handlers.delete(handler)
    },
  }
}

export interface ExecutorWorkerOptions {
  readonly id: number
  readonly transport: ExecutorTransport
  readonly workKind?: string
  readonly timeoutMs?: number
}

export function createExecutorWorker(options: ExecutorWorkerOptions): ExecutorWorker {
  const { transport } = options
  const workKind = options.workKind ?? DEFAULT_WORK_KIND
  const waitOptions = options.timeoutMs === undefined ? undefined : { timeoutMs: options.timeoutMs }
  const results = new Map<SessionID, SessionExecutionResult>()
  let nextRequestId = 1

  const request = async (
    build: (id: number) => SessionExecutorInbound,
    accept: (message: SessionExecutorOutbound) => boolean,
  ): Promise<SessionExecutorOutbound> => {
    const id = nextRequestId++
    transport.post(build(id))
    return transport.waitFor((message) => isAddressed(message, id) && accept(message), waitOptions)
  }

  const execute = async (run: ExecutorRun): Promise<void> => {
    const message = await request(
      (id) => ({ type: "run", id, sessionID: run.sessionID, kind: workKind }),
      (reply) => reply.type === "done" || reply.type === "error",
    )
    if (message.type === "error") throw new Error(message.message)
    if (message.type !== "done") throw new Error(`unexpected executor reply: ${message.type}`)
    results.set(run.sessionID, message.result)
  }

  const cancel = async (sessionID: SessionID): Promise<void> => {
    await request(
      (id) => ({ type: "cancel", id, sessionID }),
      (reply) => reply.type === "cancelled",
    )
  }

  const assertNotBusy = async (sessionID: SessionID): Promise<boolean> => {
    const message = await request(
      (id) => ({ type: "assertNotBusy", id, sessionID }),
      (reply) => reply.type === "busy",
    )
    return message.type === "busy" ? message.busy : false
  }

  const cancelPort = createExecutorCancelPort({ cancel })

  return {
    id: options.id,
    get alive() {
      return transport.alive
    },
    execute,
    cancel,
    cancelPort,
    assertNotBusy,
    resultFor: (sessionID) => results.get(sessionID),
    terminate: async () => {
      // Evict recorded results with the isolate so the map cannot grow without
      // bound across a long-lived pool.
      results.clear()
      await transport.terminate()
    },
  }
}

export interface ExecutorWorkerPoolOptions {
  readonly k: number
  /** Spawn one executor worker (production: a Bun worker + transport). */
  readonly spawn: (id: number) => ExecutorWorker
}

/** Structural mirror of dispatch's `WorkerPool` plus result/worker lookup. */
export interface ExecutorWorkerPool {
  readonly workers: ReadonlyArray<ExecutorSessionWorker>
  spawn(kind: ExecutorWorkerKind): ExecutorSessionWorker
  terminateAll(): Promise<void>
  byID(id: number): ExecutorWorker | undefined
  resultFor(sessionID: SessionID): SessionExecutionResult | undefined
}

export function createExecutorWorkerPool(options: ExecutorWorkerPoolOptions): ExecutorWorkerPool {
  const foreground = Array.from({ length: options.k }, (_, index) => options.spawn(index + 1))
  const extra: ExecutorWorker[] = []
  // One monotonic counter shared by foreground and extra, so a slot that is
  // terminated and replaced never reuses an id.
  let nextId = options.k + 1
  const all = (): ExecutorWorker[] => [...foreground, ...extra]

  return {
    get workers() {
      return foreground.slice()
    },
    spawn() {
      const worker = options.spawn(nextId++)
      extra.push(worker)
      const wrapped: ExecutorWorker = {
        get id() {
          return worker.id
        },
        get alive() {
          return worker.alive
        },
        execute: (run) => worker.execute(run),
        cancel: (sessionID) => worker.cancel(sessionID),
        assertNotBusy: (sessionID) => worker.assertNotBusy(sessionID),
        resultFor: (sessionID) => worker.resultFor(sessionID),
        terminate: async () => {
          // Drop the terminated worker from the headroom set so the pool's
          // bookkeeping tracks live workers only.
          const index = extra.indexOf(worker)
          if (index >= 0) extra.splice(index, 1)
          await worker.terminate()
        },
      }
      return wrapped
    },
    async terminateAll() {
      const workers = all()
      extra.length = 0
      await Promise.all(workers.map((worker) => worker.terminate().catch(() => undefined)))
    },
    byID: (id) => all().find((worker) => worker.id === id),
    resultFor: (sessionID) => {
      for (const worker of all()) {
        const result = worker.resultFor(sessionID)
        if (result !== undefined) return result
      }
      return undefined
    },
  }
}

// --- Hub-side control forwarder --------------------------------------------

/** Structural mirror of dispatch's `Dispatcher` (only the hub surface we use). */
export interface SessionScheduler {
  dispatch(request: { readonly sessionID: SessionID; readonly depth?: number }): Promise<void>
  cancel(sessionID: SessionID): Promise<void>
  terminateAll(): Promise<void>
  readonly registry: { get(sessionID: SessionID): { readonly workerID: number | undefined } | undefined }
}

export interface SessionDispatch {
  ensureRunning(sessionID: SessionID): Promise<SessionExecutionResult>
  startShell(sessionID: SessionID): Promise<SessionExecutionResult>
  cancel(sessionID: SessionID): Promise<void>
  assertNotBusy(sessionID: SessionID): Promise<boolean>
  cancelAll(): Promise<void>
}

export interface SessionDispatchOptions {
  readonly dispatcher: SessionScheduler
  readonly pool: ExecutorWorkerPool
}

/**
 * The hub-side surface `SessionRunState` will delegate to when the rollout flag
 * is on (Phase 6 wires it into the layer). Scheduling, admission and the
 * key→worker map come from `dispatch.ts`; `assertNotBusy` is answered by the
 * owning worker.
 */
export function createSessionDispatch(options: SessionDispatchOptions): SessionDispatch {
  const workerFor = (sessionID: SessionID): ExecutorWorker | undefined => {
    const workerID = options.dispatcher.registry.get(sessionID)?.workerID
    return workerID === undefined ? undefined : options.pool.byID(workerID)
  }

  const requireResult = (sessionID: SessionID): SessionExecutionResult => {
    const result = options.pool.resultFor(sessionID)
    if (result === undefined) throw new Error(`no session result recorded for ${sessionID}`)
    return result
  }

  const ensureRunning = async (sessionID: SessionID): Promise<SessionExecutionResult> => {
    await options.dispatcher.dispatch({ sessionID, depth: 0 })
    return requireResult(sessionID)
  }

  const assertNotBusy = async (sessionID: SessionID): Promise<boolean> => {
    const worker = workerFor(sessionID)
    return worker === undefined ? false : worker.assertNotBusy(sessionID)
  }

  const startShell = async (sessionID: SessionID): Promise<SessionExecutionResult> => {
    if (await assertNotBusy(sessionID)) throw new SessionBusyError({ sessionID })
    return ensureRunning(sessionID)
  }

  return {
    ensureRunning,
    startShell,
    assertNotBusy,
    cancel: (sessionID) => options.dispatcher.cancel(sessionID),
    cancelAll: () => options.dispatcher.terminateAll(),
  }
}

export * as SessionExecute from "./execute"
