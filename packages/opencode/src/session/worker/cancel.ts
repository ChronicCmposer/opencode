// Phase 11 — terminate-based cancel (plan tasks 11.3, 11.5, 11.9; decisions
// lines 45, 48, 49).
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

export type CancelMode = "cooperative" | "terminated" | "completed"

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
}

export interface CancelController {
  /** Request cancellation; joining a second request for the same session (idempotent). */
  cancel(sessionID: SessionID, options?: CancelRequestOptions): Promise<CancelOutcome>
  readonly pending: number
  dispose(): void
}

export function createCancelController(options: CancelControllerOptions): CancelController {
  const graceMs = options.graceMs ?? resolveCancelGraceMs()
  const schedule = options.schedule ?? defaultSchedule
  const now = options.now ?? (() => Date.now())
  const inFlight = new Map<SessionID, Promise<CancelOutcome>>()
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
      if (disposed) throw new Error("cancel controller has been disposed")
      const existing = inFlight.get(sessionID)
      if (existing !== undefined) return existing
      const promise = run(sessionID, request).finally(() => inFlight.delete(sessionID))
      inFlight.set(sessionID, promise)
      return promise
    },
    get pending() {
      return inFlight.size
    },
    dispose() {
      disposed = true
      unsubscribe()
      inFlight.clear()
      ackWaiters.clear()
    },
  }
}
