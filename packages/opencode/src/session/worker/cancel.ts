// Phase 11 — terminate-based cancel & cooperative checkpoints
// (plan tasks 11.1–11.4, 11.8–11.9; decisions lines 45, 48, 49).
//
// Synchronous JavaScript cannot be preempted inside a thread, and 3.10 proved
// `terminate()` runs no finalizers. Cancellation is therefore two-layered:
//
//   1. COOPERATIVE — the run exposes checkpoints before/after every model step
//      and tool call. A `cancel` frame on the worker's control port flips a
//      token; the next checkpoint aborts the run, which kills its own children
//      and closes its session Scope before returning to the pool.
//   2. TERMINATE — if the run does not stop within the grace window
//      (`OPENCODE_SESSION_CANCEL_GRACE_MS`, default 500 ms) the hub terminates
//      the isolate unconditionally and reaps the mirrored PGIDs itself.
//
// This module is intentionally standalone (it does not import `dispatch.ts`) so
// the scheduler can consume it without a cycle. The only cross-module type is
// `SessionID`, imported as a type (erased at runtime).

import { Schema } from "effect"
import type { SessionID } from "@/session/schema"
import { defined } from "@/util/object"
import { parsePositiveInt } from "@/util/parse"
import { defaultSchedule, portFromMessagePort } from "./port"

export const CANCEL_GRACE_ENV = "OPENCODE_SESSION_CANCEL_GRACE_MS"
export const DEFAULT_CANCEL_GRACE_MS = 500

export type Env = Record<string, string | undefined>

/** Resolve the cancel grace window, failing loud on a malformed override. */
export function resolveCancelGraceMs(env: Env = process.env): number {
  const raw = env[CANCEL_GRACE_ENV]
  if (raw === undefined || raw.trim() === "") return DEFAULT_CANCEL_GRACE_MS
  return parsePositiveInt(raw, CANCEL_GRACE_ENV)
}

/** Raised at a checkpoint once cancellation has been requested. */
export class CheckpointCancelledError extends Schema.TaggedErrorClass<CheckpointCancelledError>()(
  "CheckpointCancelledError",
  {
    message: Schema.String,
    checkpoint: Schema.String,
  },
) {}

// ---- cooperative token --------------------------------------------------------

export interface CancelToken {
  readonly cancelled: boolean
  readonly reason: string | undefined
  /** Request cancellation; first-writer-wins, idempotent. Returns `true` if this call won. */
  cancel(reason?: string): boolean
  /** Throw `CheckpointCancelledError` when already cancelled. */
  throwIfCancelled(checkpoint?: string): void
  onCancel(listener: (reason: string | undefined) => void): () => void
}

export function createCancelToken(): CancelToken {
  const listeners = new Set<(reason: string | undefined) => void>()
  let cancelled = false
  let reason: string | undefined

  return {
    get cancelled() {
      return cancelled
    },
    get reason() {
      return reason
    },
    cancel(next) {
      if (cancelled) return false
      cancelled = true
      reason = next
      for (const listener of [...listeners]) listener(reason)
      return true
    },
    throwIfCancelled(checkpoint = "token") {
      if (!cancelled) return
      throw new CheckpointCancelledError({
        message: `cancelled at checkpoint ${checkpoint}`,
        checkpoint,
      })
    },
    onCancel(listener) {
      listeners.add(listener)
      return () => listeners.delete(listener)
    },
  }
}

// ---- checkpoints --------------------------------------------------------------

export interface Checkpoints {
  readonly boundaries: ReadonlyArray<string>
  /** Record the boundary, then throw when cancellation has been requested. */
  at(label: string): void
}

export function createCheckpoints(token: CancelToken): Checkpoints {
  const boundaries: string[] = []
  return {
    get boundaries() {
      return boundaries.slice()
    },
    at(label) {
      boundaries.push(label)
      token.throwIfCancelled(label)
    },
  }
}

/** Checkpoint before and after a model step (plan 11.2). */
export async function runModelStep<T>(
  checkpoints: Checkpoints,
  label: string,
  step: () => T | Promise<T>,
): Promise<T> {
  checkpoints.at(`model:${label}:before`)
  try {
    return await step()
  } finally {
    // The `:after` boundary is recorded even when the step throws, so the
    // checkpoint trace always brackets the step. Cancellation at this boundary
    // still wins over a step error, which is the intent during a cancel.
    checkpoints.at(`model:${label}:after`)
  }
}

/** Checkpoint before and after a tool call (plan 11.2). */
export async function runToolCall<T>(
  checkpoints: Checkpoints,
  label: string,
  call: () => T | Promise<T>,
): Promise<T> {
  checkpoints.at(`tool:${label}:before`)
  try {
    return await call()
  } finally {
    checkpoints.at(`tool:${label}:after`)
  }
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

export function cancelPortFromMessagePort(port: MessagePort): CancelPort {
  const channel = portFromMessagePort<CancelFrame>(port)
  return { notify: channel.post, onMessage: channel.onMessage }
}

// ---- worker side --------------------------------------------------------------

export interface WorkerCancelHooks {
  /** Kill this worker's own children (graceful path; plan 11.4). */
  readonly killChildren?: () => void | Promise<void>
  /** Close the per-session Scope (graceful path; plan 11.4). */
  readonly closeScope?: () => void | Promise<void>
}

export interface CancelServer {
  readonly handled: number
  dispose(): void
}

/**
 * Worker-side cancel handler: on a `cancel` frame for the session this worker
 * owns, cancel the token, run the graceful cleanup (children → Scope), then
 * acknowledge. Cleanup is serialized and runs once; a repeat `cancel` merely
 * acknowledges (idempotent). A frame for a different session is rejected with
 * `accepted: false` and never touches the token, so a late cancel for a previous
 * run cannot abort the run currently in flight.
 *
 * A throwing `killChildren`/`closeScope` is acknowledged with `accepted: false`
 * and never breaks the queue: the next cancel is still processed.
 */
export function serveCancel(
  port: CancelPort,
  token: CancelToken,
  ownedSessionID: SessionID,
  hooks: WorkerCancelHooks = {},
): CancelServer {
  let handled = 0
  let queue: Promise<void> = Promise.resolve()

  const ack = (sessionID: string, accepted: boolean): void => {
    try {
      port.notify({ type: "cancel-ack", sessionID, accepted })
    } catch {
      // The control channel is gone; there is nothing left to notify.
    }
  }

  const unsubscribe = port.onMessage((frame) => {
    if (frame.type !== "cancel") return
    if (frame.sessionID !== ownedSessionID) {
      ack(frame.sessionID, false)
      return
    }
    if (!token.cancel(frame.reason)) {
      ack(frame.sessionID, true)
      return
    }
    // Keep the queue fulfilled even if a hook throws: catch inside the chain so
    // one failed cleanup cannot stall every later cancel.
    queue = queue.then(async () => {
      let accepted = true
      try {
        if (hooks.killChildren) await hooks.killChildren()
        if (hooks.closeScope) await hooks.closeScope()
      } catch {
        accepted = false
      } finally {
        handled++
        ack(frame.sessionID, accepted)
      }
    })
  })

  return {
    get handled() {
      return handled
    },
    dispose: unsubscribe,
  }
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
