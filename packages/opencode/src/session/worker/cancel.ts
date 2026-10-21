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

export const CANCEL_GRACE_ENV = "OPENCODE_SESSION_CANCEL_GRACE_MS"
export const DEFAULT_CANCEL_GRACE_MS = 500

export type Env = Record<string, string | undefined>

/** Resolve the cancel grace window, failing loud on a malformed override. */
export function resolveCancelGraceMs(env: Env = process.env): number {
  const raw = env[CANCEL_GRACE_ENV]
  if (raw === undefined || raw.trim() === "") return DEFAULT_CANCEL_GRACE_MS
  const parsed = Number(raw)
  if (!Number.isInteger(parsed) || parsed < 1) {
    throw new Error(`invalid ${CANCEL_GRACE_ENV}: ${raw} (expected a positive integer)`)
  }
  return parsed
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
  const result = await step()
  checkpoints.at(`model:${label}:after`)
  return result
}

/** Checkpoint before and after a tool call (plan 11.2). */
export async function runToolCall<T>(
  checkpoints: Checkpoints,
  label: string,
  call: () => T | Promise<T>,
): Promise<T> {
  checkpoints.at(`tool:${label}:before`)
  const result = await call()
  checkpoints.at(`tool:${label}:after`)
  return result
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
  return {
    notify: (frame) => port.postMessage(frame),
    onMessage: (handler) => {
      const listener = (event: MessageEvent) => handler(event.data as CancelFrame)
      port.addEventListener("message", listener)
      port.start()
      return () => port.removeEventListener("message", listener)
    },
  }
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
 * Worker-side cancel handler: on a `cancel` frame, cancel the token, run the
 * graceful cleanup (children → Scope), then acknowledge. Cleanup is serialized
 * and runs once; a repeat `cancel` merely acknowledges (idempotent).
 */
export function serveCancel(port: CancelPort, token: CancelToken, hooks: WorkerCancelHooks = {}): CancelServer {
  let handled = 0
  let queue: Promise<void> = Promise.resolve()

  const unsubscribe = port.onMessage((frame) => {
    if (frame.type !== "cancel") return
    const won = token.cancel(frame.reason)
    if (!won) {
      port.notify({ type: "cancel-ack", sessionID: frame.sessionID, accepted: true })
      return
    }
    queue = queue.then(async () => {
      if (hooks.killChildren) await hooks.killChildren()
      if (hooks.closeScope) await hooks.closeScope()
      handled++
      port.notify({ type: "cancel-ack", sessionID: frame.sessionID, accepted: true })
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
  /** Claim the outcome; returns `true` only for the first claimant. */
  claim(outcome: Outcome): boolean
}

export function createOutcomeGate(): OutcomeGate {
  let winner: Outcome | undefined
  return {
    get winner() {
      return winner
    },
    claim(outcome) {
      if (winner !== undefined) return winner === outcome
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

const defaultCancelSchedule: CancelScheduler = (delayMs, run) => {
  const timer = setTimeout(run, delayMs)
  ;(timer as { unref?: () => void }).unref?.()
  return () => clearTimeout(timer)
}

export function createCancelController(options: CancelControllerOptions): CancelController {
  const graceMs = options.graceMs ?? resolveCancelGraceMs()
  const schedule = options.schedule ?? defaultCancelSchedule
  const now = options.now ?? (() => Date.now())
  const inFlight = new Map<SessionID, Promise<CancelOutcome>>()
  const acknowledged = new Set<SessionID>()
  let disposed = false

  const unsubscribe = options.port.onMessage((frame) => {
    if (frame.type === "cancel-ack") acknowledged.add(frame.sessionID as SessionID)
  })

  const run = async (sessionID: SessionID, request: CancelRequestOptions | undefined): Promise<CancelOutcome> => {
    const started = now()
    const gate = createOutcomeGate()
    const complete = request?.complete

    // First-writer-wins: a completion that already landed beats the cancel.
    if (complete !== undefined) {
      let completionLanded = false
      void complete.then(
        () => {
          completionLanded = true
        },
        () => {
          completionLanded = true
        },
      )
      await Promise.resolve()
      if (completionLanded) {
        gate.claim("completion")
        return finish(sessionID, "completed", "completion", started)
      }
    }

    gate.claim("cancel")
    options.port.notify({
      type: "cancel",
      sessionID,
      ...(request?.reason === undefined ? {} : { reason: request.reason }),
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
      return finish(sessionID, "terminated", "cancel", started)
    }
    return finish(sessionID, "cooperative", "cancel", started)
  }

  const finish = (sessionID: SessionID, mode: CancelMode, winner: Outcome, started: number): CancelOutcome => ({
    sessionID,
    mode,
    winner,
    acknowledged: acknowledged.has(sessionID),
    durationMs: now() - started,
  })

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
    },
  }
}
