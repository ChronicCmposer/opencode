// Terminate-based cancel.
//
// Synchronous JavaScript cannot be preempted inside a thread, and 3.10 proved
// `terminate()` runs no finalizers. Cancellation is therefore terminate-based:
// the hub sends a `cancel` frame, waits up to the grace window
// (`OPENCODE_SESSION_CANCEL_GRACE_MS`, default 500 ms) for the run's fiber to
// unwind, and if the run does not stop the hub terminates the isolate
// unconditionally and reaps the mirrored PGIDs itself. The fiber-interrupt
// finalizers already perform the graceful cleanup (message finalize, processor
// halt, child-process group kill), so no worker-side cancel server is needed.
//
// This module is intentionally standalone (it does not import `dispatch.ts`) so
// the scheduler can consume it without a cycle. The only cross-module type is
// `SessionID`, imported as a type (erased at runtime).

import type { SessionID } from "@/session/schema"
import { defined } from "@/util/object"
import { parsePositiveInt } from "@/util/parse"
import { noopWorkerLogger, workerLogMessage, type WorkerLogger } from "./log"
import { defaultSchedule } from "./port"

export const CANCEL_GRACE_ENV = "OPENCODE_SESSION_CANCEL_GRACE_MS"
export const DEFAULT_CANCEL_GRACE_MS = 500

export type Env = Record<string, string | undefined>

/** Resolve the cancel grace window, failing loud on a malformed override. */
export function resolveCancelGraceMs(env: Env = process.env): number {
  const raw = env[CANCEL_GRACE_ENV]
  if (raw === undefined || raw.trim() === "") return DEFAULT_CANCEL_GRACE_MS
  return parsePositiveInt(raw, CANCEL_GRACE_ENV)
}

// ---- control-port frames ------------------------------------------------------

export type CancelFrame =
  | { readonly type: "cancel"; readonly sessionID: string; readonly reason?: string }
  | { readonly type: "cancel-ack"; readonly sessionID: string; readonly accepted: boolean }

/**
 * The slice of the control channel cancellation needs. `EgressControl.ControlLink`
 * already satisfies it structurally (`notify` + `onMessage`), so the hub wiring
 * composes a cancel frame handler with the egress handler rather than owning a
 * second port.
 */
export interface CancelPort {
  notify(frame: CancelFrame): void
  /** Register a frame handler; returns an unsubscribe function. */
  onMessage(handler: (frame: CancelFrame) => void): () => void
}

// ---- first-writer-wins outcome ------------------------------------------------

export type Outcome = "cancel" | "completion"

export interface OutcomeGate {
  readonly winner: Outcome | undefined
  /**
   * Claim the outcome. Returns `true` only for the very first claimant; every
   * later claim (even for the same outcome) returns `false` and never overwrites
   * the winner. This is the single first-writer-wins arbiter for cancel vs
   * completion.
   */
  claim(outcome: Outcome): boolean
}

export function createOutcomeGate(): OutcomeGate {
  let winner: Outcome | undefined
  return {
    get winner() {
      return winner
    },
    claim(outcome) {
      if (winner !== undefined) return false
      winner = outcome
      return true
    },
  }
}

// ---- hub-side controller ------------------------------------------------------

/**
 * `boot` is the typed outcome of cancelling a run that had not completed its
 * first boot: the boot lifecycle aborted the partial isolate and released the
 * spawn permit, so there was never a run to cooperate with or terminate.
 */
export type CancelMode = "cooperative" | "terminated" | "completed" | "boot"

export interface CancelOutcome {
  readonly sessionID: SessionID
  readonly mode: CancelMode
  readonly winner: Outcome
  /**
   * Whether this request observed a matching `cancel-ack` by the time the outcome
   * was decided. Scoped to this request only: a stale ack from an earlier run is
   * never reused, and a completion that skipped the cancel is never acknowledged.
   */
  readonly acknowledged: boolean
  readonly durationMs: number
}

export type CancelScheduler = (delayMs: number, run: () => void) => () => void

export interface CancelRequestOptions {
  /** The run's completion promise; resolving within the grace is cooperative. */
  readonly complete?: Promise<unknown>
  readonly reason?: string
}

export interface CancelControllerOptions {
  readonly port: CancelPort
  readonly terminate: () => void | Promise<void>
  readonly graceMs?: number
  readonly schedule?: CancelScheduler
  /** Reap the worker's mirrored PGIDs when the terminate path runs (plan 11.5). */
  readonly reap?: () => void | Promise<void>
  readonly now?: () => number
  /** The worker this controller belongs to, when known (observability D1). */
  readonly workerID?: number
  /** Optional structured logger seam; absent defaults to a no-op (observability D1). */
  readonly log?: WorkerLogger
}

export interface CancelController {
  /** Request cancellation; joining a second request for the same session (idempotent). */
  cancel(sessionID: SessionID, options?: CancelRequestOptions): Promise<CancelOutcome>
  readonly pending: number
  dispose(): void
}

/** An in-flight cancel plus the reject handle `dispose` uses to settle it. */
interface InFlightRequest {
  /** The promise a caller receives; the `.finally` chain that clears the entry. */
  promise: Promise<CancelOutcome>
  readonly reject: (error: unknown) => void
}

export function createCancelController(options: CancelControllerOptions): CancelController {
  const graceMs = options.graceMs ?? resolveCancelGraceMs()
  const schedule = options.schedule ?? defaultSchedule
  const now = options.now ?? (() => Date.now())
  const log = options.log ?? noopWorkerLogger
  // Each in-flight request keeps its settle handle so `dispose` can fail it
  // loud instead of leaving a caller waiting on a promise that will never settle.
  const inFlight = new Map<SessionID, InFlightRequest>()
  // Per-run ack waiters, keyed by the raw session id string (no brand cast). The
  // waiter is registered before the cancel frame is sent and pruned in `run`'s
  // finally, so a late ack from a previous request can never satisfy a later one.
  const ackWaiters = new Map<string, (accepted: boolean) => void>()
  let disposed = false

  const unsubscribe = options.port.onMessage((frame) => {
    if (frame.type !== "cancel-ack") return
    ackWaiters.get(frame.sessionID)?.(frame.accepted)
  })

  const finish = (
    sessionID: SessionID,
    mode: CancelMode,
    winner: Outcome,
    started: number,
    acknowledged: boolean,
  ): CancelOutcome => ({
    sessionID,
    mode,
    winner,
    acknowledged,
    durationMs: now() - started,
  })

  const run = async (sessionID: SessionID, request: CancelRequestOptions | undefined): Promise<CancelOutcome> => {
    const started = now()
    const gate = createOutcomeGate()
    const complete = request?.complete

    let acknowledged = false
    ackWaiters.set(sessionID, (accepted) => {
      acknowledged = accepted
    })

    try {
      // First-writer-wins: a completion that already landed beats the cancel.
      // The completion claims the gate; the cancel claims it one microtask later.
      // When `complete` is already settled its reaction is queued first, so the
      // race deterministically resolves to the completion.
      if (complete !== undefined) {
        const completionWon = await Promise.race([
          complete.then(
            () => gate.claim("completion"),
            () => gate.claim("completion"),
          ),
          Promise.resolve().then(() => false),
        ])
        if (completionWon) {
          return finish(sessionID, "completed", "completion", started, acknowledged)
        }
      }

      gate.claim("cancel")
      options.port.notify({
        type: "cancel",
        sessionID,
        ...defined({ reason: request?.reason }),
      })

      let cancelTimer: (() => void) | undefined
      const grace = new Promise<"grace">((resolve) => {
        cancelTimer = schedule(graceMs, () => resolve("grace"))
      })
      const settled: Promise<"complete"> =
        complete === undefined
          ? new Promise<"complete">(() => {})
          : complete.then(
              () => "complete" as const,
              () => "complete" as const,
            )

      const first = await Promise.race([settled, grace])
      cancelTimer?.()

      if (first === "grace") {
        await options.terminate()
        await options.reap?.()
        return finish(sessionID, "terminated", "cancel", started, acknowledged)
      }
      return finish(sessionID, "cooperative", "cancel", started, acknowledged)
    } finally {
      ackWaiters.delete(sessionID)
    }
  }

  return {
    cancel(sessionID, request) {
      // A disposed controller is a caller error, surfaced as a rejected promise
      // (never a synchronous throw) so the async contract is uniform.
      if (disposed) return Promise.reject(new Error("cancel controller has been disposed"))
      const existing = inFlight.get(sessionID)
      if (existing !== undefined) return existing.promise
      // A new (non-joining) request is the one that emits; the idempotent join
      // above never re-logs.
      log(workerLogMessage("cancel requested"), { sessionID, workerID: options.workerID, graceMs })
      let rejectRequest!: (error: unknown) => void
      const inner = new Promise<CancelOutcome>((resolve, reject) => {
        rejectRequest = reject
        run(sessionID, request).then(resolve, reject)
      })
      const entry: InFlightRequest = { promise: inner, reject: rejectRequest }
      // The caller receives the `.finally` chain, so the entry is cleared BEFORE
      // the caller's `await` resumes — a request that arrives right after a settle
      // starts fresh instead of joining the already-settled entry.
      entry.promise = inner.finally(() => {
        if (inFlight.get(sessionID) === entry) inFlight.delete(sessionID)
      })
      inFlight.set(sessionID, entry)
      return entry.promise
    },
    get pending() {
      return inFlight.size
    },
    dispose() {
      disposed = true
      unsubscribe()
      const pending = [...inFlight.values()]
      inFlight.clear()
      ackWaiters.clear()
      // Settle every in-flight request loud: its caller must not hang forever.
      for (const entry of pending) entry.reject(new Error("cancel controller has been disposed"))
    },
  }
}

// ---- boot cancellation --------------------------------------------------------

/**
 * The D11 boot outcome. It is a typed value, not an exception: a cancelled boot
 * is a normal terminal state and appends no message.
 */
export interface BootCancelResult {
  readonly mode: "cancelled"
  readonly winner: "cancel"
  readonly phase: "boot"
}

export type BootState = "booting" | "ready" | "started" | "cancelled" | "failed"

export type BootTelemetry =
  | { readonly type: "boot-cancelled"; readonly phase: "boot" }
  | { readonly type: "abort-death"; readonly phase: "boot"; readonly reason: string }

/** The permit slice boot cancellation needs; `spawn-gate.SpawnPermit` satisfies it. */
export interface BootPermit {
  release(outcome?: "ready" | "failed", error?: unknown): void
}

export interface BootLifecycleOptions<T> {
  /** The already-acquired spawn permit. Boot cancellation releases it exactly once. */
  readonly permit: BootPermit
  /** Construction/startup work; aborting the signal must settle it. */
  readonly materialize: (signal: AbortSignal) => Promise<T>
  /** Terminate a partial isolate (materialized, possibly ready, never started). */
  readonly terminate: (partial: T | undefined) => void | Promise<void>
  /** Invoked once when the caller starts the run. Never invoked after cancel. */
  readonly onRun?: (worker: T) => void
  readonly onTelemetry?: (event: BootTelemetry) => void
}

export interface BootLifecycle<T> {
  /** Materialization result, or the typed cancel outcome when boot was cancelled. */
  readonly ready: Promise<T | BootCancelResult>
  readonly state: BootState
  /** Mark the run started; refused once cancelled. */
  readonly start: (worker: T) => boolean
  /** Cancel boot exactly once. `undefined` once the run already started. */
  readonly cancel: () => Promise<BootCancelResult | undefined>
}

export const BOOT_CANCELLED: BootCancelResult = Object.freeze({
  mode: "cancelled",
  winner: "cancel",
  phase: "boot",
})

/**
 * Own one boot interval: permit → materialize → (start). A cancel aborts
 * construction, terminates any partial isolate, releases the permit once, and
 * returns {@link BOOT_CANCELLED}. A graph that is ready but not started takes the
 * same path. Death during abort is still a cancel win plus telemetry.
 */
export function createBootLifecycle<T>(options: BootLifecycleOptions<T>): BootLifecycle<T> {
  const abort = new AbortController()
  let state: BootState = "booting"
  let partial: T | undefined
  let released = false
  let cancelling: Promise<BootCancelResult> | undefined

  const releasePermit = (): void => {
    if (released) return
    released = true
    options.permit.release("failed")
  }

  const materialization = options.materialize(abort.signal)
  const ready: Promise<T | BootCancelResult> = materialization.then(
    (worker) => {
      partial = worker
      if (state === "cancelled") return BOOT_CANCELLED
      state = "ready"
      return worker
    },
    (error: unknown) => {
      releasePermit()
      if (state === "cancelled") return BOOT_CANCELLED
      state = "failed"
      throw error
    },
  )

  const cancel = (): Promise<BootCancelResult | undefined> => {
    if (state === "started") return Promise.resolve(undefined)
    if (state === "failed") return Promise.resolve(undefined)
    if (cancelling !== undefined) return cancelling
    state = "cancelled"
    abort.abort()
    cancelling = (async () => {
      let abortDeath = false
      try {
        await materialization
      } catch {
        abortDeath = true
      }
      if (partial !== undefined) {
        try {
          await options.terminate(partial)
        } catch {
          abortDeath = true
        }
      }
      releasePermit()
      options.onTelemetry?.(
        abortDeath
          ? { type: "abort-death", phase: "boot", reason: "materialization or termination failed during abort" }
          : { type: "boot-cancelled", phase: "boot" },
      )
      return BOOT_CANCELLED
    })()
    return cancelling
  }

  return {
    ready,
    get state() {
      return state
    },
    start(worker) {
      if (state !== "ready") return false
      state = "started"
      options.onRun?.(worker)
      return true
    },
    cancel,
  }
}

export * as CancelModule from "./cancel"
