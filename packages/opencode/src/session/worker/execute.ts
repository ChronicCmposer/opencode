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
// registry (`session-worker.ts`) resolves it to the reduced-`AppLayer` pipeline.

import { Cause, Effect, Exit, Schema, Scope } from "effect"
import type { Latch } from "effect"
import { Runner } from "@/effect/runner"
import type { SessionID } from "@/session/schema"
import type { PromptInput } from "@/session/prompt"
import type { CancelFrame, CancelPort } from "./cancel"
import { noopWorkerLogger, workerLogMessage, type WorkerLogger } from "./log"

/** JSON-safe result of one session run (the full message crosses via egress). */
export interface SessionExecutionResult {
  readonly sessionID: string
  readonly value?: unknown
}

/**
 * One unit of session work over a session key. `prompt`, when present, is the
 * sub-agent turn the parent would have persisted (Solution 1 / B2): the worker
 * runs the full `SessionPrompt.prompt` path — persist the user message, then
 * loop — instead of the bare `loop` that expects the message to already exist.
 * Optional so a prompt-less run (the existing foreground path) is unchanged.
 */
export type SessionWork = (
  sessionID: SessionID,
  prompt?: PromptInput,
) => Effect.Effect<SessionExecutionResult>
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
 * session work (and aliases this constant as a second key when it differs), so
 * the hub and worker agree.
 *
 * Typed as `string` (not the `"session"` literal) because the worker registry
 * builds its alias table with a computed key derived from this constant; a
 * literal type would make that key statically collide with the `session`
 * shorthand (TS1117).
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

/**
 * Raised when a worker reply does not arrive within its wait deadline. Defined
 * here (not in `hub.ts`) because `dispatch.ts` imports `execute.ts` while
 * `hub.ts` imports `dispatch.ts`; placing it in `hub.ts` would close a cycle.
 */
export class SessionWorkerReplyTimeoutError extends Error {
  constructor(readonly waitMs: number) {
    super(`timed out waiting for a session worker reply after ${waitMs}ms`)
    this.name = "SessionWorkerReplyTimeoutError"
  }
}

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
  | {
      readonly type: "run"
      readonly id: number
      readonly sessionID: SessionID
      readonly kind: string
      /** The sub-agent prompt to persist before looping; absent for a plain run. */
      readonly prompt?: PromptInput
    }
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
  /**
   * Run-start seam: called once with the run's `sessionID` after the work is
   * resolved and before it executes (e.g. to rename the worker's thread).
   * Invoked fail-soft, so a throwing callback never unwinds or fails the run.
   */
  readonly onRunStart?: (sessionID: SessionID) => void
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
    prompt?: PromptInput,
  ): Promise<SessionExecutorOutbound> => {
    let work: SessionWork
    try {
      work = options.work(kind)
    } catch (error) {
      return failed(id, error)
    }
    // Fail-soft: a throwing run-start callback must never unwind or fail the run.
    try {
      options.onRunStart?.(sessionID)
    } catch {
      // Intentionally swallowed — the seam is best-effort observability.
    }
    const effect =
      mode === "run"
        ? options.machine.ensureRunning(sessionID, work(sessionID, prompt))
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
        return runWork(message.id, message.sessionID, message.kind, "run", message.prompt)
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
  /** Run-start seam; forwarded to the installed executor. */
  readonly onRunStart?: (sessionID: SessionID) => void
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
  const executor = createSessionExecutor({
    machine,
    work: options.work,
    control: options.control,
    onRunStart: options.onRunStart,
  })

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

/** The executor frame a run takes: a plain session run or the shell-start path. */
export type SessionRunMode = "run" | "startShell"

export interface ExecutorRun {
  readonly sessionID: SessionID
  readonly depth: number
  /** Frame to produce; defaults to `"run"` so a plain run stays `{sessionID, depth}`. */
  readonly mode?: SessionRunMode
  /**
   * The sub-agent prompt to run on the worker (Solution 1 / B2). When present,
   * the worker persists the user message for `sessionID` and loops, instead of
   * a bare `loop` that expects the message to already exist. Absent keeps a
   * plain run byte-for-byte unchanged.
   */
  readonly prompt?: PromptInput
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
  /**
   * Resolve with the first frame matching `match`. A `timeoutMs` that is
   * `undefined` uses the transport default; `<= 0` means "no deadline" (used
   * for a long `run`/`startShell` reply — a full LLM turn — so it is never
   * bounded by the control-frame budget).
   */
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
  /** Optional structured logger seam; absent defaults to a no-op (observability D2). */
  readonly log?: WorkerLogger
}

/**
 * Terminal disposition of a worker cancel (observability D2). `"cooperative"`
 * means the worker accepted the cancel and will settle at a checkpoint;
 * `"terminate"` means it rejected it and the hub must terminate the isolate.
 * `"queued"` is the dispatch-level outcome for a session not yet on a worker, so
 * a worker's cancel port never emits it — it is part of the shared vocabulary.
 */
export type ExecutorCancelMode = "cooperative" | "terminate" | "queued"

/**
 * Adapt an executor worker's request/reply `cancel` into the Phase-11
 * `CancelPort` shape: a `cancel` frame drives `cancel`, and its settlement is
 * echoed back as a `cancel-ack` (accepted on success, rejected otherwise).
 */
export function createExecutorCancelPort(options: ExecutorCancelPortOptions): CancelPort {
  const log = options.log ?? noopWorkerLogger
  const handlers = new Set<(frame: CancelFrame) => void>()
  const emit = (frame: CancelFrame): void => {
    for (const handler of [...handlers]) handler(frame)
  }
  const outcome = (sessionID: string, mode: ExecutorCancelMode, accepted: boolean): void =>
    log(workerLogMessage("cancel outcome"), { sessionID, mode, accepted })
  return {
    notify(frame) {
      if (frame.type !== "cancel") return
      options.cancel(frame.sessionID as SessionID).then(
        () => {
          emit({ type: "cancel-ack", sessionID: frame.sessionID, accepted: true })
          outcome(frame.sessionID, "cooperative", true)
        },
        () => {
          emit({ type: "cancel-ack", sessionID: frame.sessionID, accepted: false })
          outcome(frame.sessionID, "terminate", false)
        },
      )
    },
    onMessage(handler) {
      handlers.add(handler)
      return () => handlers.delete(handler)
    },
  }
}

/** Why a worker isolate is spawning (observability B1). */
export type WorkerSpawnReason = "initial" | "rebind" | "respawn"

export interface ExecutorWorkerOptions {
  readonly id: number
  readonly transport: ExecutorTransport
  readonly workKind?: string
  readonly timeoutMs?: number
  /**
   * Liveness watchdog budget for a `run`/`startShell` reply, in ms. When `> 0`,
   * a watchdog probes the worker with a `ping` every interval while the run is in
   * flight and rejects with `SessionWorkerReplyTimeoutError` if a matching `pong`
   * does not arrive within that interval. Defaults to `0` (disabled), so the run
   * reply keeps its unbounded wait unless a caller opts in.
   */
  readonly runIdleTimeoutMs?: number
  /** The session the isolate is bound to, when known at spawn (observability B1). */
  readonly sessionID?: SessionID
  /** Why this isolate is spawning; defaults to `"initial"` (observability B1). */
  readonly reason?: WorkerSpawnReason
  /** Optional structured logger seam; absent defaults to a no-op (flag-off behavior). */
  readonly log?: WorkerLogger
}

export function createExecutorWorker(options: ExecutorWorkerOptions): ExecutorWorker {
  const { transport } = options
  const log = options.log ?? noopWorkerLogger
  const workKind = options.workKind ?? DEFAULT_WORK_KIND
  const waitOptions = options.timeoutMs === undefined ? undefined : { timeoutMs: options.timeoutMs }
  const runIdleTimeoutMs = options.runIdleTimeoutMs ?? 0
  const results = new Map<SessionID, SessionExecutionResult>()
  let nextRequestId = 1

  log(workerLogMessage("worker spawning"), {
    workerID: options.id,
    kind: workKind,
    ...(options.sessionID === undefined ? {} : { sessionID: options.sessionID }),
    reason: options.reason ?? "initial",
  })

  const request = async (
    build: (id: number) => SessionExecutorInbound,
    accept: (message: SessionExecutorOutbound) => boolean,
    wait: { timeoutMs?: number } | undefined,
  ): Promise<SessionExecutorOutbound> => {
    const id = nextRequestId++
    transport.post(build(id))
    return transport.waitFor((message) => isAddressed(message, id) && accept(message), wait)
  }

  // A `run`/`startShell` reply is a whole `SessionPrompt.loop` including a full
  // LLM turn, so it must never be bounded by the short control-frame deadline.
  // `timeoutMs: 0` is the transport's "no deadline" sentinel.
  const NO_REPLY_DEADLINE = { timeoutMs: 0 } as const

  /**
   * Await the run reply while probing the worker's liveness. Every `intervalMs`
   * the watchdog posts a `ping` and requires the matching `pong` within the same
   * window. A worker whose event loop is wedged cannot answer, so the probe
   * rejects with `SessionWorkerReplyTimeoutError` (the dispatcher then terminates
   * the isolate); a healthy worker answers each ping promptly, so a long run
   * resolves however many intervals it spans.
   *
   * The pong waiter and the run waiter coexist: the transport matches each frame
   * by predicate and consumes it for exactly one waiter, and the two predicates
   * (`pong` vs `done`/`error`) are disjoint.
   */
  const awaitReplyWithLiveness = async (
    reply: Promise<SessionExecutorOutbound>,
    intervalMs: number,
  ): Promise<SessionExecutorOutbound> => {
    let timer: ReturnType<typeof setTimeout> | undefined
    let stopped = false
    const wedged = new Promise<never>((_resolve, reject) => {
      const probe = (): void => {
        if (stopped) return
        const pingId = nextRequestId++
        try {
          transport.post({ type: "ping", id: pingId })
          transport
            .waitFor((message) => message.type === "pong" && message.id === pingId, { timeoutMs: intervalMs })
            .then(
              () => {
                if (!stopped) timer = setTimeout(probe, intervalMs)
              },
              () => {
                if (!stopped) reject(new SessionWorkerReplyTimeoutError(intervalMs))
              },
            )
        } catch (error) {
          // `post` throws once the isolate is gone; surface it the same way a
          // dead worker is surfaced rather than escaping the timer callback.
          if (!stopped) reject(error instanceof Error ? error : new Error(String(error)))
        }
      }
      timer = setTimeout(probe, intervalMs)
    })
    try {
      return await Promise.race([reply, wedged])
    } finally {
      // Settle the watchdog the moment the run does, so no timer outlives it.
      stopped = true
      if (timer !== undefined) clearTimeout(timer)
    }
  }

  const execute = async (run: ExecutorRun): Promise<void> => {
    const type: SessionRunMode = run.mode ?? "run"
    const id = nextRequestId++
    // Carry the prompt only when present, so a plain run keeps its minimal
    // `{type, id, sessionID, kind}` frame.
    transport.post({
      type,
      id,
      sessionID: run.sessionID,
      kind: workKind,
      ...(run.prompt === undefined || type !== "run" ? {} : { prompt: run.prompt }),
    })
    const reply = transport.waitFor(
      (message) => isAddressed(message, id) && (message.type === "done" || message.type === "error"),
      NO_REPLY_DEADLINE,
    )
    const message = runIdleTimeoutMs > 0 ? await awaitReplyWithLiveness(reply, runIdleTimeoutMs) : await reply
    if (message.type === "error") throw new Error(message.message)
    if (message.type !== "done") throw new Error(`unexpected executor reply: ${message.type}`)
    results.set(run.sessionID, message.result)
  }

  const cancel = async (sessionID: SessionID): Promise<void> => {
    await request(
      (id) => ({ type: "cancel", id, sessionID }),
      (reply) => reply.type === "cancelled",
      waitOptions,
    )
  }

  const assertNotBusy = async (sessionID: SessionID): Promise<boolean> => {
    const message = await request(
      (id) => ({ type: "assertNotBusy", id, sessionID }),
      (reply) => reply.type === "busy",
      waitOptions,
    )
    return message.type === "busy" ? message.busy : false
  }

  const cancelPort = createExecutorCancelPort({ cancel, log })

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
  /** Optional structured logger seam; absent defaults to a no-op (flag-off behavior). */
  readonly log?: WorkerLogger
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
  const log = options.log ?? noopWorkerLogger
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
  dispatch(request: {
    readonly sessionID: SessionID
    readonly depth?: number
    readonly mode?: SessionRunMode
  }): Promise<void>
  cancel(sessionID: SessionID): Promise<void>
  terminateAll(): Promise<void>
  readonly registry: { get(sessionID: SessionID): { readonly workerID: number | undefined } | undefined }
}

export interface SessionDispatch {
  /** Dispatch `sessionID` at `depth` (default 0, or `depthOf(sessionID)`) and resolve its result. */
  ensureRunning(sessionID: SessionID, depth?: number): Promise<SessionExecutionResult>
  /** Dispatch `sessionID` on the shell-start path at `depth` and resolve its result. */
  startShell(sessionID: SessionID, depth?: number): Promise<SessionExecutionResult>
  cancel(sessionID: SessionID): Promise<void>
  assertNotBusy(sessionID: SessionID): Promise<boolean>
  cancelAll(): Promise<void>
}

export interface SessionDispatchOptions {
  readonly dispatcher: SessionScheduler
  readonly pool: ExecutorWorkerPool
  /**
   * Resolve a session's subagent depth when the caller does not pass one. The
   * production hub supplies this from its job registry, so a child session
   * scheduled through `ensureRunning(sessionID)` still lands on headroom.
   * Defaults to 0 (a foreground root).
   */
  readonly depthOf?: (sessionID: SessionID) => number
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

  /** Admit `sessionID` at its resolved depth and frame mode, then await admission. */
  const admit = (sessionID: SessionID, depth: number | undefined, mode: SessionRunMode): Promise<void> =>
    options.dispatcher.dispatch({
      sessionID,
      depth: depth ?? options.depthOf?.(sessionID) ?? 0,
      mode,
    })

  const ensureRunning = async (sessionID: SessionID, depth?: number): Promise<SessionExecutionResult> => {
    await admit(sessionID, depth, "run")
    return requireResult(sessionID)
  }

  const assertNotBusy = async (sessionID: SessionID): Promise<boolean> => {
    const worker = workerFor(sessionID)
    return worker === undefined ? false : worker.assertNotBusy(sessionID)
  }

  const startShell = async (sessionID: SessionID, depth?: number): Promise<SessionExecutionResult> => {
    if (await assertNotBusy(sessionID)) throw new SessionBusyError({ sessionID })
    await admit(sessionID, depth, "startShell")
    return requireResult(sessionID)
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
