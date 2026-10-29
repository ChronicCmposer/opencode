// Session execution seam.
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
import type { PromptInput, ShellInput } from "@/session/prompt"
import type { EffectBridge } from "@/effect/bridge"
import type { BootCancelResult, CancelFrame, CancelPort } from "./cancel"
import { workerLogMessage } from "./worker-logging"
import { WorkerRuntime } from "./runtime-edge"
import { committedMessage, resetCommitted } from "@/session/ledger/lifecycle"
import { errorTagOf, messageOf } from "./port"
import { readyFrame, type ReadyBuildFrame } from "./build-hash"
import type { DispatchOrigin } from "@/session/origin"
import {
  createSupervision,
  SessionWorkerDeadError,
  type DispatchDecision,
  type Supervision,
} from "./supervision"

/** JSON-safe result of one session run (the full message crosses via egress). */
export interface SessionExecutionResult {
  readonly sessionID: string
  readonly value?: unknown
  /**
   * The assistant message this run committed, or `null` when it committed none.
   * Carried in the worker's `done` reply so the hub barrier settles from one
   * authoritative message (F11) instead of a separate best-effort report.
   */
  readonly committed?: string | null
}

/**
 * One unit of session work over a session key. `prompt`, when present, is the
 * sub-agent turn the parent would have persisted: the worker
 * runs the full `SessionPrompt.prompt` path — persist the user message, then
 * loop — instead of the bare `loop` that expects the message to already exist.
 * Optional so a prompt-less run (the existing foreground path) is unchanged.
 */
export type SessionWork = (
  sessionID: SessionID,
  prompt?: PromptInput,
  shell?: ShellInput,
) => Effect.Effect<SessionExecutionResult>
export type SessionWorkFactory = (kind: string) => SessionWork

/**
 * The run control the worker-local `SessionRunState` exposes to the executor
 * The executor's own machine (table A) is only an outer `ensureRunning`
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
 * `productionWorkRegistry`) maps the literal `"session"` to the real
 * reduced-layer session work, so the hub and worker agree.
 */
export const DEFAULT_WORK_KIND = "session"

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

/**
 * A control request (`cancel` / `assertNotBusy` / `hasActiveWork`) that the
 * worker answered with a structured `error` frame. The remote `tag` is preserved
 * as both `tag` and `_tag`, so `errorTagOf` and callers that branch on Effect
 * error tags see the worker's classification instead of a generic failure.
 */
export class SessionExecutorRequestError extends Error {
  readonly tag: string | undefined
  readonly _tag: string
  constructor(
    readonly requestID: number,
    tag: string | undefined,
    message: string,
  ) {
    super(message)
    this.name = "SessionExecutorRequestError"
    this.tag = tag
    this._tag = tag ?? "SessionExecutorRequestError"
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
      /** Subagent depth carried from dispatch; observability only. */
      readonly depth?: number
      /** The sub-agent prompt to persist before looping; absent for a plain run. */
      readonly prompt?: PromptInput
    }
  | {
      readonly type: "startShell"
      readonly id: number
      readonly sessionID: SessionID
      readonly kind: string
      /** Subagent depth carried from dispatch; observability only. */
      readonly depth?: number
      /** The shell command to run; carried so the worker resolves the real work. */
      readonly shell?: ShellInput
    }
  | { readonly type: "cancel"; readonly id: number; readonly sessionID: SessionID }
  | { readonly type: "assertNotBusy"; readonly id: number; readonly sessionID: SessionID }
  | { readonly type: "hasActiveWork"; readonly id: number; readonly sessionID: SessionID }
  | { readonly type: "ping"; readonly id: number }

export type SessionExecutorReady = ReadyBuildFrame
export interface SessionExecutorPong {
  readonly type: "pong"
  readonly id: number
}
export interface SessionExecutorBusy {
  readonly type: "busy"
  readonly id: number
  readonly busy: boolean
}
/**
 * Reply to a `hasActiveWork` probe: whether the worker still owns work that must
 * outlive an idle turn (a running process-local background job). Distinct from
 * `busy`, which owns the run/waiters state and gates a new prompt.
 */
export interface SessionExecutorActiveWork {
  readonly type: "activeWork"
  readonly id: number
  readonly active: boolean
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
  | SessionExecutorActiveWork
  | SessionExecutorCancelled
  | SessionExecutorDone
  | SessionExecutorFailed

export type SessionEffectRunner = <A, E>(effect: Effect.Effect<A, E>) => Promise<A>

const runEffect: SessionEffectRunner = (effect) => WorkerRuntime.runPromise(effect)

const isAddressed = (message: SessionExecutorOutbound, id: number): boolean => "id" in message && message.id === id

/** The correlation id an unrecognised inbound frame carries, else `0`. */
const requestIdOf = (message: unknown): number =>
  typeof (message as { readonly id?: unknown }).id === "number" ? (message as { readonly id: number }).id : 0

/**
 * The dispatch depth on a run frame. Only a finite non-negative integer is
 * trusted; a malformed value falls back to 0 (a foreground root), matching
 * dispatch's own default.
 */
const frameDepth = (message: { readonly depth?: number }): number =>
  typeof message.depth === "number" && Number.isInteger(message.depth) && message.depth >= 0 ? message.depth : 0

/** Shape any thrown/rejected value into a failed reply, preserving its `_tag`. */
const failed = (id: number, error: unknown): SessionExecutorFailed => {
  const tag = errorTagOf(error)
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
   * Worker-local background-work probe. Answers whether `sessionID` still owns
   * work that must outlive an idle turn (a running process-local background
   * job). Deliberately separate from `control.busy`: background work must not
   * make a new prompt fail `assertNotBusy`, but it must keep the isolate alive
   * across an idle-reap window. Absent means "no background work".
   */
  readonly activeWork?: (sessionID: SessionID) => Promise<boolean>
  /**
   * Run-start seam: called once with the run's `sessionID` (and its dispatch
   * depth, observability only) after the work is resolved and before it runs
   * (e.g. to rename the worker's thread). Invoked fail-soft, so a throwing
   * callback never unwinds or fails the run.
   */
  readonly onRunStart?: (sessionID: SessionID, depth: number) => void
  /** Effect runner seam; defaults to the shared runtime boundary. */
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
        onSuccess: (result): SessionExecutorOutbound => {
          const committed = committedMessage(result.sessionID)
          return {
            type: "done",
            id,
            result: committed === undefined ? result : { ...result, committed },
          }
        },
      }),
    )

  const runWork = async (
    id: number,
    sessionID: SessionID,
    kind: string,
    mode: "run" | "startShell",
    depth: number,
    prompt?: PromptInput,
    shell?: ShellInput,
  ): Promise<SessionExecutorOutbound> => {
    resetCommitted(sessionID)
    let work: SessionWork
    try {
      work = options.work(kind)
    } catch (error) {
      return failed(id, error)
    }
    // Fail-soft: a throwing run-start callback must never unwind or fail the run.
    try {
      options.onRunStart?.(sessionID, depth)
    } catch {
      // Intentionally swallowed — the seam is best-effort observability.
    }
    const effect =
      mode === "run"
        ? options.machine.ensureRunning(sessionID, work(sessionID, prompt))
        : options.machine.startShell(sessionID, work(sessionID, undefined, shell))
    return run(settle(id, effect))
  }

  const handle = async (message: SessionExecutorInbound): Promise<SessionExecutorOutbound> => {
    switch (message.type) {
      case "ping":
        return { type: "pong", id: message.id }
      case "assertNotBusy":
        return { type: "busy", id: message.id, busy: await busy(message.sessionID) }
      case "hasActiveWork":
        return {
          type: "activeWork",
          id: message.id,
          active: options.activeWork === undefined ? false : await options.activeWork(message.sessionID),
        }
      case "cancel":
        await cancel(message.sessionID)
        return { type: "cancelled", id: message.id }
      case "run":
        return runWork(message.id, message.sessionID, message.kind, "run", frameDepth(message), message.prompt)
      case "startShell":
        return runWork(
          message.id,
          message.sessionID,
          message.kind,
          "startShell",
          frameDepth(message),
          undefined,
          message.shell,
        )
      default:
        // An unrecognised frame still carries the caller's correlation id; echo
        // it so the sender can match the failure instead of waiting on id 0.
        return { type: "error", id: requestIdOf(message), message: "unknown executor request" }
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
  /** Worker-local run control; forwarded to the installed executor. */
  readonly control?: SessionRunControl
  /** Worker-local background-work probe; forwarded to the installed executor. */
  readonly activeWork?: (sessionID: SessionID) => Promise<boolean>
  /** Run-start seam; forwarded to the installed executor. */
  readonly onRunStart?: (sessionID: SessionID, depth: number) => void
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
  const workerScope = options.scope ?? Scope.makeUnsafe()
  const machine = createSessionRunMachine({ scope: workerScope, onIdle: options.onIdle, onBusy: options.onBusy })
  const executor = createSessionExecutor({
    machine,
    work: options.work,
    control: options.control,
    activeWork: options.activeWork,
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

  scope.postMessage(readyFrame(options.threadId ?? 0))
  return {
    handle: executor.handle,
    dispose: async () => {
      if (!ownsScope) return
      await WorkerRuntime.runPromise(Scope.close(workerScope, Exit.void))
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
   * The sub-agent prompt to run on the worker. When present,
   * the worker persists the user message for `sessionID` and loops, instead of
   * a bare `loop` that expects the message to already exist. Absent keeps a
   * plain run byte-for-byte unchanged.
   */
  readonly prompt?: PromptInput
  /**
   * The model the caller requested for this run. A session worker is bound to
   * one model, so a changed request selects a new worker generation even when
   * the persisted session row has not caught up yet.
   */
  readonly model?: WorkerRunModel
  /** The shell command for a `"startShell"` run; absent for a plain run. */
  readonly shell?: ShellInput
}

/** The requested provider/model pair carried on a run frame. */
export interface WorkerRunModel {
  readonly providerID: string
  readonly modelID: string
}

/** Structural mirror of dispatch's `SessionWorker` (keeps this module decoupled). */
export interface ExecutorSessionWorker {
  readonly id: number
  readonly alive: boolean
  execute(run: ExecutorRun): Promise<void>
  terminate(): Promise<void>
  /**
   * Idle-reap liveness probe: whether the worker still owns work that must
   * outlive an idle turn (a running process-local background job). Optional so
   * minimal test fakes remain valid; `createExecutorWorker` always provides it.
   * Kept distinct from `assertNotBusy`, which gates a new prompt.
   */
  hasActiveWork?(sessionID: SessionID): Promise<boolean>
  /**
   * Cancel a worker that is still booting (the spawn permit is held, the ready
   * handshake has not started the run). Resolves the typed boot outcome, or
   * `undefined` when the run already started. Optional so minimal test fakes
   * remain valid; `createExecutorWorker` always provides it.
   */
  cancelBoot?(): Promise<BootCancelResult | undefined>
}

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
    options?: { timeoutMs?: number; signal?: AbortSignal },
  ): Promise<SessionExecutorOutbound>
  readonly alive: boolean
  /**
   * Orderly close of the isolate's durable resources (native WAL handle, bus
   * lease) before the thread is destroyed. Optional so lightweight test
   * transports remain valid; production always provides it.
   */
  close?(): Promise<void>
  terminate(code?: number): Promise<number>
  /**
   * Cancel the pre-ready boot interval. Resolves the typed boot outcome when a
   * cancel wins, `undefined` once the run started (normal cancel applies), or
   * when the transport boots eagerly. Absent means no boot accounting.
   */
  cancelBoot?(): Promise<BootCancelResult | undefined>
}

export interface ExecutorWorker extends ExecutorSessionWorker {
  cancel(sessionID: SessionID): Promise<void>
  assertNotBusy(sessionID: SessionID): Promise<boolean>
  /**
   * Idle-reap liveness probe: whether `sessionID` still owns a running
   * process-local background job. The dispatcher re-arms the reap timer instead
   * of retiring a worker that answers `true`. Distinct from `assertNotBusy`, so
   * background work never blocks a new prompt. Optional for test fakes.
   */
  hasActiveWork?(sessionID: SessionID): Promise<boolean>
  resultFor(sessionID: SessionID): SessionExecutionResult | undefined
  /**
   * Cooperative cancel control port. `dispatch.ts`'s
   * `CancellationConfig.portFor` returns this so a cancel notifies the worker and
   * observes its ack within the grace window. Optional so minimal test fakes
   * remain valid; `createExecutorWorker` always provides it.
   */
  readonly cancelPort?: CancelPort
  /**
   * Per-session write barrier: resolves once the run's durable egress
   * write is acked. The dispatcher awaits it before releasing the worker to its
   * next run, so a re-dispatch never overtakes the prior run's egress. Optional
   * so minimal test fakes remain valid.
   */
  awaitEgress?(sessionID: SessionID): Promise<void>
}

export interface ExecutorCancelPortOptions {
  readonly cancel: (sessionID: SessionID) => Promise<void>
  /** Optional re-entry bridge for the cancel outcome line. */
  readonly bridge?: EffectBridge.Shape
}

/**
 * The cancel dispositions a worker's cancel port can report:
 * `"cooperative"` means the worker accepted the cancel and will settle at a
 * checkpoint; `"terminate"` means it rejected it and the hub must terminate the
 * isolate. A session not yet on a worker is a dispatch-level concern, so it can
 * never reach this port (dispatch owns its own vocabulary).
 */
export const EXECUTOR_CANCEL_MODES = ["cooperative", "terminate"] as const
export type ExecutorCancelMode = (typeof EXECUTOR_CANCEL_MODES)[number]

/**
 * Adapt an executor worker's request/reply `cancel` into the Phase-11
 * `CancelPort` shape: a `cancel` frame drives `cancel`, and its settlement is
 * echoed back as a `cancel-ack` (accepted on success, rejected otherwise).
 */
export function createExecutorCancelPort(options: ExecutorCancelPortOptions): CancelPort {
  const bridge = options.bridge
  const handlers = new Set<(frame: CancelFrame) => void>()
  const emit = (frame: CancelFrame): void => {
    for (const handler of [...handlers]) handler(frame)
  }
  const outcome = (sessionID: string, mode: ExecutorCancelMode, accepted: boolean): void =>
    void bridge?.fork(Effect.logInfo(workerLogMessage("cancel outcome"), { sessionID, mode, accepted }))
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

/** Why a worker isolate is spawning. */
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
  /** The session the isolate is bound to, when known at spawn. */
  readonly sessionID?: SessionID
  /** Why this isolate is spawning; defaults to `"initial"`. */
  readonly reason?: WorkerSpawnReason
  /**
   * Per-session durable-egress barrier. When supplied, `execute` does
   * not resolve until the run's egress write is acked, so the dispatcher cannot
   * hand the worker its next run early. Absent leaves the reply path unchanged.
   */
  readonly egressBarrier?: (sessionID: SessionID) => Promise<void> | undefined
  /** Optional re-entry bridge for the worker spawn line. */
  readonly bridge?: EffectBridge.Shape
}

/**
 * Post `build(id)` and await the first frame addressed to that id and accepted by
 * `accept`, or a structured `error` frame for that id (which becomes a rejection
 * carrying the remote tag). The id comes from the caller's monotonic counter so
 * every request is correlated without a shared mutable field.
 */
async function requestOn(
  transport: ExecutorTransport,
  nextId: () => number,
  build: (id: number) => SessionExecutorInbound,
  accept: (message: SessionExecutorOutbound) => boolean,
  wait: { timeoutMs?: number } | undefined,
): Promise<SessionExecutorOutbound> {
  const id = nextId()
  transport.post(build(id))
  const message = await transport.waitFor(
    (candidate) => isAddressed(candidate, id) && (accept(candidate) || candidate.type === "error"),
    wait,
  )
  if (message.type === "error") throw new SessionExecutorRequestError(message.id, message.tag, message.message)
  return message
}

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
export async function awaitReplyWithLiveness(
  transport: ExecutorTransport,
  reply: Promise<SessionExecutorOutbound>,
  intervalMs: number,
  nextId: () => number,
): Promise<SessionExecutorOutbound> {
  // Abort the in-flight probe's `waitFor` when the run settles, so the ping
  // waiter is removed immediately instead of lingering until its own deadline.
  const probes = new AbortController()

  const probeOnce = (): Promise<void> => {
    const pingId = nextId()
    try {
      transport.post({ type: "ping", id: pingId })
    } catch (error) {
      // `post` throws once the isolate is gone; surface it the same way a dead
      // worker is surfaced rather than letting it escape the probe fiber.
      return Promise.reject(error instanceof Error ? error : new Error(String(error)))
    }
    return transport
      .waitFor((message) => message.type === "pong" && message.id === pingId, {
        timeoutMs: intervalMs,
        signal: probes.signal,
      })
      .then(() => undefined)
  }

  // One probe per interval: sleep first (a run that settles inside the interval
  // never pings), then require the matching pong within the same window. The
  // sleep is Effect's clock, so `raceFirst` interrupts it the moment the run
  // settles instead of leaving a raw timer behind.
  const probeLoop = Effect.gen(function* () {
    while (true) {
      yield* Effect.sleep(intervalMs)
      yield* Effect.tryPromise({
        try: probeOnce,
        catch: (error) =>
          error instanceof SessionWorkerReplyTimeoutError
            ? error
            : new Error(messageOf(error)),
      })
    }
  }).pipe(Effect.andThen(Effect.never))

  try {
    // Bridge the interruptible probe at the one Promise-shaped edge: the run
    // reply and the probe race, and the loser's fiber is interrupted.
    return await WorkerRuntime.runPromise(
      Effect.raceFirst(
        Effect.tryPromise({ try: () => reply, catch: (error) => error }),
        probeLoop,
      ),
    )
  } finally {
    // The race interrupts the probe fiber; abort the transport waiter too so a
    // settled run never leaves a ping waiter registered until its deadline.
    probes.abort()
  }
}

export function createExecutorWorker(options: ExecutorWorkerOptions): ExecutorWorker {
  const { transport } = options
  const bridge = options.bridge
  const workKind = options.workKind ?? DEFAULT_WORK_KIND
  const waitOptions = options.timeoutMs === undefined ? undefined : { timeoutMs: options.timeoutMs }
  const runIdleTimeoutMs = options.runIdleTimeoutMs ?? 0
  // One remembered result: a worker serves one session at a time, so the last
  // settled run simply replaces the previous one (no map, no eviction policy).
  let lastResult: SessionExecutionResult | undefined
  let nextRequestId = 1
  // One monotonic id source shared by every request and the liveness probe, so a
  // late reply can never be matched by a later request.
  const nextId = (): number => nextRequestId++
  const recordResult = (result: SessionExecutionResult): void => {
    lastResult = result
  }

  void bridge?.fork(
    Effect.logInfo(workerLogMessage("worker spawning"), {
      workerID: options.id,
      kind: workKind,
      ...(options.sessionID === undefined ? {} : { sessionID: options.sessionID }),
      reason: options.reason ?? "initial",
    }),
  )

  const request = (
    build: (id: number) => SessionExecutorInbound,
    accept: (message: SessionExecutorOutbound) => boolean,
    wait: { timeoutMs?: number } | undefined,
  ): Promise<SessionExecutorOutbound> => requestOn(transport, nextId, build, accept, wait)

  // A `run`/`startShell` reply is a whole `SessionPrompt.loop` including a full
  // LLM turn, so it must never be bounded by the short control-frame deadline.
  // `timeoutMs: 0` is the transport's "no deadline" sentinel.
  const NO_REPLY_DEADLINE = { timeoutMs: 0 } as const

  const execute = async (run: ExecutorRun): Promise<void> => {
    const type: SessionRunMode = run.mode ?? "run"
    const id = nextId()
    // Carry the prompt only when present, so a plain run keeps its minimal
    // `{type, id, sessionID, kind}` frame.
    transport.post({
      type,
      id,
      sessionID: run.sessionID,
      kind: workKind,
      depth: run.depth,
      ...(run.prompt === undefined || type !== "run" ? {} : { prompt: run.prompt }),
      ...(run.shell === undefined || type !== "startShell" ? {} : { shell: run.shell }),
    })
    const reply = transport.waitFor(
      (message) => isAddressed(message, id) && (message.type === "done" || message.type === "error"),
      NO_REPLY_DEADLINE,
    )
    const message = runIdleTimeoutMs > 0 ? await awaitReplyWithLiveness(transport, reply, runIdleTimeoutMs, nextId) : await reply
    if (message.type === "error") throw new Error(message.message)
    if (message.type !== "done") throw new Error(`unexpected executor reply: ${message.type}`)
    // Do not resolve the run until its durable egress write is acked, so
    // the dispatcher cannot hand this worker the next run ahead of that write.
    await options.egressBarrier?.(run.sessionID)
    recordResult(message.result)
  }

  const cancelBoot = (): Promise<BootCancelResult | undefined> =>
    transport.cancelBoot?.() ?? Promise.resolve(undefined)

  const cancel = async (sessionID: SessionID): Promise<void> => {
    // A cancel that lands before the ready handshake wins the boot interval; the
    // queued run never starts, so no worker-side cancel frame is sent.
    if ((await cancelBoot()) !== undefined) return
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

  /**
   * Ask the worker whether it still owns background work for `sessionID`. The
   * reply crosses an isolate boundary, so `active` is re-checked at runtime: a
   * malformed or unexpected reply fails loud instead of silently keeping (or
   * reaping) the worker on a guess.
   */
  const hasActiveWork = async (sessionID: SessionID): Promise<boolean> => {
    const message = await request(
      (id) => ({ type: "hasActiveWork", id, sessionID }),
      (reply) => reply.type === "activeWork",
      waitOptions,
    )
    if (message.type !== "activeWork") throw new Error(`unexpected executor reply: ${message.type}`)
    const active: unknown = message.active
    if (typeof active !== "boolean") {
      throw new Error(`malformed "activeWork" reply: active must be a boolean (got ${JSON.stringify(active)})`)
    }
    return active
  }

  const cancelPort = createExecutorCancelPort({ cancel, ...(bridge === undefined ? {} : { bridge }) })

  return {
    id: options.id,
    get alive() {
      return transport.alive
    },
    execute,
    cancel,
    cancelBoot,
    cancelPort,
    assertNotBusy,
    hasActiveWork,
    ...(options.egressBarrier === undefined
      ? {}
      : { awaitEgress: (sessionID: SessionID) => Promise.resolve(options.egressBarrier?.(sessionID)) }),
    resultFor: (sessionID) => (lastResult?.sessionID === sessionID ? lastResult : undefined),
    terminate: async () => {
      // Drop the result with the isolate so a retired worker never leaks it.
      lastResult = undefined
      // Route every retire through the orderly close first: `terminate()` alone
      // can destroy a realm still holding the WAL write lock/lease.
      await transport.close?.().catch(() => undefined)
      await transport.terminate()
    },
  }
}

export interface ExecutorWorkerPoolOptions {
  /** Spawn one executor worker (production: a Bun worker + transport). */
  readonly spawn: (id: number) => ExecutorWorker
  /** Pool-side cleanup when a session's worker is retired (e.g. init cache). */
  readonly onSessionReleased?: (sessionID: SessionID) => void
}

/** Structural mirror of dispatch's `WorkerPool` plus result/worker lookup. */
export interface ExecutorWorkerPool {
  /** Every worker currently materialized (retired workers are removed). */
  readonly workers: ReadonlyArray<ExecutorSessionWorker>
  /**
   * Materialize one fresh worker. The dispatcher passes its registry id so the
   * session map and the isolate agree; direct callers may omit it and let the
   * pool assign the next monotonic id.
   */
  spawn(id?: number): ExecutorSessionWorker
  /**
   * Release a retired session's worker: drop its pool entry (when `workerID` is
   * supplied) and run the pool's own per-session cleanup. A retired worker is no
   * longer findable via {@link byID} or {@link resultFor}, so the pool stays
   * bounded at the number of live sessions, not every session ever dispatched.
   */
  releaseSession?(sessionID: SessionID, workerID?: number): void
  terminateAll(): Promise<void>
  byID(id: number): ExecutorWorker | undefined
  resultFor(sessionID: SessionID): SessionExecutionResult | undefined
}

export function createExecutorWorkerPool(options: ExecutorWorkerPoolOptions): ExecutorWorkerPool {
  // Keyed by id for O(1) lookup and removal on retirement; insertion order is
  // preserved so `workers` stays stable.
  const materialized = new Map<number, ExecutorWorker>()
  // One monotonic counter, so a retired/replacement worker never reuses an id.
  let nextId = 1
  const create = (id?: number): ExecutorWorker => {
    // Honour a dispatcher-supplied id (the registry owns the monotonic source
    // there), while direct/eager callers keep the pool's own counter. A supplied
    // id already held by a live worker (mixed eager + dispatch use) must never
    // overwrite it, so fall through to the next free id.
    const workerID = id === undefined || materialized.has(id) ? nextId : id
    nextId = Math.max(nextId, workerID + 1)
    const worker = options.spawn(workerID)
    materialized.set(worker.id, worker)
    return worker
  }

  return {
    get workers() {
      return [...materialized.values()]
    },
    spawn: create,
    releaseSession: (sessionID, workerID) => {
      if (workerID !== undefined) materialized.delete(workerID)
      options.onSessionReleased?.(sessionID)
    },
    async terminateAll() {
      const workers = [...materialized.values()]
      materialized.clear()
      await Promise.all(workers.map((worker) => worker.terminate().catch(() => undefined)))
    },
    byID: (id) => materialized.get(id),
    resultFor: (sessionID) => {
      for (const worker of materialized.values()) {
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
    readonly shell?: ShellInput
  }): Promise<void>
  /** Effect-native admission; the Promise form remains for external callers. */
  dispatchEffect?(request: {
    readonly sessionID: SessionID
    readonly depth?: number
    readonly mode?: SessionRunMode
    readonly shell?: ShellInput
  }): Effect.Effect<void, unknown>
  cancel(sessionID: SessionID): Promise<void>
  terminateAll(): Promise<void>
  readonly registry: { get(sessionID: SessionID): { readonly workerID: number | undefined } | undefined }
}

export interface SessionDispatch {
  /**
   * Dispatch `sessionID` at `depth` (default 0, or `depthOf(sessionID)`) and
   * resolve its result. `origin` defaults to `"user"`: an explicit prompt always
   * resets crash supervision, while background/auto work fails fast on a dead
   * session.
   */
  ensureRunning(sessionID: SessionID, depth?: number, origin?: DispatchOrigin): Promise<SessionExecutionResult>
  /** Effect-native ensureRunning used by `SessionRunState`. */
  ensureRunningEffect(
    sessionID: SessionID,
    depth?: number,
    origin?: DispatchOrigin,
  ): Effect.Effect<SessionExecutionResult, unknown>
  /**
   * Dispatch `sessionID` on the shell-start path, carrying `shell` to the worker
   * so it resolves the real shell work, at `depth` (default 0).
   */
  startShell(
    sessionID: SessionID,
    shell?: ShellInput,
    depth?: number,
    origin?: DispatchOrigin,
  ): Promise<SessionExecutionResult>
  cancel(sessionID: SessionID): Promise<void>
  assertNotBusy(sessionID: SessionID): Promise<boolean>
  cancelAll(): Promise<void>
}

export interface SessionDispatchOptions {
  readonly dispatcher: SessionScheduler
  readonly pool: ExecutorWorkerPool
  /**
   * Resolve a session's subagent depth when the caller does not pass one. The
   * production hub passes the depth explicitly on every run (from
   * `Session.spawn`'s persisted `metadata.depth`), so it leaves this absent.
   * Defaults to 0 (a foreground root).
   */
  readonly depthOf?: (sessionID: SessionID) => number
  /**
   * The hub's authoritative commit outcome for a finished run, read before its
   * active-run record is deleted. The run's `done` reply carries it so the
   * barrier settles from one message (F11).
   */
  readonly commitOutcome?: (sessionID: SessionID) => { readonly committed: string | null } | undefined
  /** Per-session crash policy; defaults to a fresh in-process policy. */
  readonly supervision?: Supervision
}

/**
 * The hub-side surface `SessionRunState` will delegate to when the rollout flag
 * is on (wired into the layer). Scheduling, admission and the
 * key→worker map come from `dispatch.ts`; `assertNotBusy` is answered by the
 * owning worker.
 */
export function createSessionDispatch(options: SessionDispatchOptions): SessionDispatch {
  const supervision = options.supervision ?? createSupervision()
  // Coalesced dispatches share one rejection instance: every waiter's `admit`
  // observes the exact same crash error. Count each crash event once, not once
  // per joined waiter, so five waiters cannot mark a session dead for one crash.
  const countedCrashEvents = new WeakSet<object>()
  const recordCrashOnce = (sessionID: SessionID, error: unknown): void => {
    if (typeof error === "object" && error !== null) {
      if (countedCrashEvents.has(error)) return
      countedCrashEvents.add(error)
    }
    supervision.recordCrash(sessionID)
  }

  const workerFor = (sessionID: SessionID): ExecutorWorker | undefined => {
    const workerID = options.dispatcher.registry.get(sessionID)?.workerID
    return workerID === undefined ? undefined : options.pool.byID(workerID)
  }

  const requireResult = (sessionID: SessionID): SessionExecutionResult => {
    const result = options.pool.resultFor(sessionID)
    if (result === undefined) throw new Error(`no session result recorded for ${sessionID}`)
    return result
  }

  /** Fail fast when supervision forbids this dispatch before any worker spawns. */
  const requireAdmission = (sessionID: SessionID, origin: DispatchOrigin): void => {
    const decision: DispatchDecision = supervision.decideDispatch(sessionID, origin)
    if (decision.kind === "allow") return
    if (decision.kind === "dead") throw new SessionWorkerDeadError(sessionID)
    throw new Error(`session ${sessionID} worker crashed recently; retry in ${decision.delayMs}ms`)
  }

  /**
   * Admit `sessionID` at its resolved depth and frame mode, then await
   * admission. A worker crash is counted per session; a successful run resets
   * the count. The in-flight run still rejects at-most-once, so a crash never
   * silently retries.
   */
  const admit = async (
    sessionID: SessionID,
    depth: number | undefined,
    mode: SessionRunMode,
    shell?: ShellInput,
    origin: DispatchOrigin = "user",
  ): Promise<void> => {
    requireAdmission(sessionID, origin)
    try {
      await options.dispatcher.dispatch({
        sessionID,
        depth: depth ?? options.depthOf?.(sessionID) ?? 0,
        mode,
        ...(shell === undefined ? {} : { shell }),
      })
    } catch (error) {
      if (errorTagOf(error) === "WorkerCrashedError") recordCrashOnce(sessionID, error)
      throw error
    }
    supervision.recordHealthy(sessionID)
  }

  /** Attach the hub's authoritative commit outcome to a finished run. */
  const withCommitOutcome = (result: SessionExecutionResult): SessionExecutionResult => {
    const outcome = options.commitOutcome?.(result.sessionID as SessionID)
    return outcome === undefined ? result : { ...result, committed: outcome.committed }
  }

  const admitEffect = (
    sessionID: SessionID,
    depth: number | undefined,
    mode: SessionRunMode,
    shell?: ShellInput,
    origin: DispatchOrigin = "user",
  ): Effect.Effect<void, unknown> =>
    Effect.suspend(() => {
      try {
        requireAdmission(sessionID, origin)
      } catch (error) {
        return Effect.fail(error)
      }
      const request = {
        sessionID,
        depth: depth ?? options.depthOf?.(sessionID) ?? 0,
        mode,
        ...(shell === undefined ? {} : { shell }),
      }
      const admitted =
        options.dispatcher.dispatchEffect === undefined
          ? Effect.tryPromise({ try: () => options.dispatcher.dispatch(request), catch: (error) => error })
          : options.dispatcher.dispatchEffect(request)
      return admitted.pipe(
        Effect.tap(() => Effect.sync(() => supervision.recordHealthy(sessionID))),
        Effect.tapError((error) =>
          Effect.sync(() => {
            if (errorTagOf(error) === "WorkerCrashedError") recordCrashOnce(sessionID, error)
          }),
        ),
      )
    })

  const ensureRunningEffect = (
    sessionID: SessionID,
    depth?: number,
    origin: DispatchOrigin = "user",
  ): Effect.Effect<SessionExecutionResult, unknown> =>
    admitEffect(sessionID, depth, "run", undefined, origin).pipe(
      Effect.andThen(Effect.sync(() => withCommitOutcome(requireResult(sessionID)))),
    )

  const ensureRunning = async (
    sessionID: SessionID,
    depth?: number,
    origin: DispatchOrigin = "user",
  ): Promise<SessionExecutionResult> => {
    await admit(sessionID, depth, "run", undefined, origin)
    return withCommitOutcome(requireResult(sessionID))
  }

  const assertNotBusy = async (sessionID: SessionID): Promise<boolean> => {
    // A registry reservation (including a run still queued in `cap.acquire`)
    // means the session owns a run even before it has a materialized isolate.
    // The worker-local probe cannot see that queue, so it would report idle.
    if (options.dispatcher.registry.get(sessionID) !== undefined) return true
    const worker = workerFor(sessionID)
    return worker === undefined ? false : worker.assertNotBusy(sessionID)
  }

  const startShell = async (
    sessionID: SessionID,
    shell?: ShellInput,
    depth?: number,
    origin: DispatchOrigin = "user",
  ): Promise<SessionExecutionResult> => {
    if (await assertNotBusy(sessionID)) throw new SessionBusyError({ sessionID })
    await admit(sessionID, depth, "startShell", shell, origin)
    return withCommitOutcome(requireResult(sessionID))
  }

  return {
    ensureRunning,
    ensureRunningEffect,
    startShell,
    assertNotBusy,
    cancel: (sessionID) => options.dispatcher.cancel(sessionID),
    cancelAll: () => options.dispatcher.terminateAll(),
  }
}

export * as SessionExecute from "./execute"
