// Per-session lifecycle truth shared by the hub, the crash ledger and auto-resume.

import { Schema } from "effect"

import type { WorkerCrashed, WorkerCrashPhase } from "./crash"
import type { IndeterminateCall } from "./resume"

/** The four commit-side phases one run must settle before the next run starts. */
export const LIFECYCLE_PHASES = ["commits", "projectors", "tools", "promotion"] as const

export type LifecyclePhase = (typeof LIFECYCLE_PHASES)[number]

export interface LifecycleCrashState {
  readonly phase?: WorkerCrashPhase
  readonly lastCommittedMessageID: string | null
  readonly inFlightCallIDs: readonly string[]
}

export interface LifecycleRunPhases {
  readonly commits: Promise<unknown>
  readonly projectors: Promise<unknown>
  readonly tools: Promise<unknown>
  readonly promotion: Promise<unknown>
}

export interface LifecycleTrackerOptions {
  /** Maximum sessions retained; oldest terminal sessions evict first. Defaults to 1024. */
  readonly maxSessions?: number
}

interface Deferred {
  readonly promise: Promise<void>
  readonly resolve: () => void
}

function deferred(): Deferred {
  let resolve!: () => void
  const promise = new Promise<void>((res) => {
    resolve = res
  })
  return { promise, resolve }
}

interface Phases {
  readonly commits: Deferred
  readonly projectors: Deferred
  readonly tools: Deferred
  readonly promotion: Deferred
}

function freshPhases(): Phases {
  return {
    commits: deferred(),
    projectors: deferred(),
    tools: deferred(),
    promotion: deferred(),
  }
}

interface SessionRecord {
  lastCommittedMessageID: string | null
  inFlight: Map<string, boolean>
  attempts: number
  cancelled: boolean
  phase: WorkerCrashPhase | undefined
  phases: Phases
}

function requireSession(sessionID: string): string {
  if (typeof sessionID !== "string" || sessionID.length === 0) {
    throw new Error("session lifecycle requires a non-empty sessionID")
  }
  return sessionID
}

export interface LifecycleTracker {
  /** Record the newest durably committed assistant message. */
  commit(sessionID: string, messageID: string): void
  /** Record a tool call that started before its effect. */
  toolStarted(sessionID: string, callID: string, sideEffecting?: boolean): void
  /** Record a tool call whose result committed. */
  toolFinished(sessionID: string, callID: string): void
  /** Record the phase a run reached, so a crash without an isolate payload is classified. */
  phase(sessionID: string, phase: WorkerCrashPhase): void
  /** The dispatcher's synchronous crash fallback. */
  crashState(sessionID: string): LifecycleCrashState
  /** Started calls that never finished, in start order. */
  indeterminate(sessionID: string): readonly IndeterminateCall[]
  /** Cancel wins over every later resume attempt for this run. */
  cancel(sessionID: string): void
  cancelled(sessionID: string): boolean
  attempts(sessionID: string): number
  /** Count one auto-resume attempt and return the new count. */
  recordAttempt(sessionID: string): number
  /** A healthy run resets the per-run auto-resume cap. */
  resetAttempts(sessionID: string): void
  /** Start a fresh run: new barrier phases, attempts and cancel reset. */
  beginRun(sessionID: string): void
  settlePhase(sessionID: string, phase: LifecyclePhase): void
  phases(sessionID: string): LifecycleRunPhases
  /** Drop all state for a retired session. */
  forget(sessionID: string): void
  /** Observe a typed crash payload without changing the committed-message truth. */
  observeCrash(payload: WorkerCrashed): void
  readonly size: number
}

export function createLifecycleTracker(options: LifecycleTrackerOptions = {}): LifecycleTracker {
  const maxSessions = options.maxSessions ?? 1024
  if (!Number.isInteger(maxSessions) || maxSessions < 1) {
    throw new Error(`invalid lifecycle session bound: ${maxSessions}`)
  }

  const records = new Map<string, SessionRecord>()

  const create = (): SessionRecord => ({
    lastCommittedMessageID: null,
    inFlight: new Map(),
    attempts: 0,
    cancelled: false,
    phase: undefined,
    phases: freshPhases(),
  })

  const touch = (sessionID: string): SessionRecord => {
    const existing = records.get(sessionID)
    if (existing !== undefined) {
      // Re-insert so Map insertion order is the LRU order.
      records.delete(sessionID)
      records.set(sessionID, existing)
      return existing
    }
    const created = create()
    records.set(sessionID, created)
    while (records.size > maxSessions) {
      const oldest = records.keys().next().value
      if (oldest === undefined) break
      records.delete(oldest)
    }
    return created
  }

  const peek = (sessionID: string): SessionRecord => records.get(sessionID) ?? create()

  return {
    commit(sessionID, messageID) {
      const record = touch(requireSession(sessionID))
      if (typeof messageID !== "string" || messageID.length === 0) {
        throw new Error("committed message id must be a non-empty string")
      }
      record.lastCommittedMessageID = messageID
    },
    toolStarted(sessionID, callID, sideEffecting = true) {
      const record = touch(requireSession(sessionID))
      if (typeof callID !== "string" || callID.length === 0) {
        throw new Error("tool call id must be a non-empty string")
      }
      record.inFlight.set(callID, sideEffecting)
    },
    toolFinished(sessionID, callID) {
      const record = touch(requireSession(sessionID))
      if (typeof callID !== "string" || callID.length === 0) {
        throw new Error("tool call id must be a non-empty string")
      }
      // Idempotent by construction: a late duplicate finish (or a recovered
      // stale call) cannot decrement another call or double-resolve the phase.
      record.inFlight.delete(callID)
      if (record.inFlight.size === 0) record.phases.tools.resolve()
    },
    phase(sessionID, phase) {
      touch(requireSession(sessionID)).phase = phase
    },
    crashState(sessionID) {
      const record = peek(requireSession(sessionID))
      return {
        ...(record.phase === undefined ? {} : { phase: record.phase }),
        lastCommittedMessageID: record.lastCommittedMessageID,
        inFlightCallIDs: [...record.inFlight.keys()],
      }
    },
    indeterminate(sessionID) {
      return [...peek(requireSession(sessionID)).inFlight.entries()].map(([callID, sideEffecting]) => ({
        callID,
        sideEffecting,
      }))
    },
    cancel(sessionID) {
      touch(requireSession(sessionID)).cancelled = true
    },
    cancelled(sessionID) {
      return peek(requireSession(sessionID)).cancelled
    },
    attempts(sessionID) {
      return peek(requireSession(sessionID)).attempts
    },
    recordAttempt(sessionID) {
      const record = touch(requireSession(sessionID))
      record.attempts += 1
      return record.attempts
    },
    resetAttempts(sessionID) {
      touch(requireSession(sessionID)).attempts = 0
    },
    beginRun(sessionID) {
      const record = touch(requireSession(sessionID))
      record.cancelled = false
      record.phase = undefined
      record.phases = freshPhases()
    },
    settlePhase(sessionID, phase) {
      touch(requireSession(sessionID)).phases[phase].resolve()
    },
    phases(sessionID) {
      const phases = peek(requireSession(sessionID)).phases
      return {
        commits: phases.commits.promise,
        projectors: phases.projectors.promise,
        tools: phases.tools.promise,
        promotion: phases.promotion.promise,
      }
    },
    forget(sessionID) {
      records.delete(requireSession(sessionID))
    },
    observeCrash(payload) {
      const record = touch(requireSession(payload.sessionID))
      record.phase = payload.phase
    },
    get size() {
      return records.size
    },
  }
}

export * as LifecycleLedger from "./lifecycle"

/** The worker-side reporting seam. */
export interface LifecycleReporting {
  commit(sessionID: string, messageID: string): Promise<void>
  toolStarted(sessionID: string, callID: string, sideEffecting?: boolean): Promise<void>
  toolFinished(sessionID: string, callID: string): Promise<void>
  phase(sessionID: string, phase: WorkerCrashPhase): Promise<void>
}

let installed: LifecycleReporting | undefined

/** The worker-local last committed assistant message per session in this isolate. */
const committedBySession = new Map<string, string>()

/** Start a fresh worker-local commit record for one run. */
export function resetCommitted(sessionID: string): void {
  committedBySession.delete(sessionID)
}

/** The committed message this isolate observed for its current run. */
export function committedMessage(sessionID: string): string | null | undefined {
  // No reporting seam means this is not a real worker run (unit tests, pipeline
  // reductions): leave the historical result shape untouched.
  if (installed === undefined) return undefined
  return committedBySession.get(sessionID) ?? null
}

/** Install the current isolate's reporting seam (`session-worker` boot). */
export function installReporting(reporting: LifecycleReporting | undefined): void {
  installed = reporting
}

/** The current isolate's reporting seam, or `undefined` outside a session worker. */
export function reporting(): LifecycleReporting | undefined {
  return installed
}

/**
 * Bound for the lifecycle reports that may be dropped on teardown (`toolStarted`, `toolFinished`, `phase`).
 */
export const LIFECYCLE_RPC_TIMEOUT_MS = 5_000

export class LifecycleRpcTimeoutError extends Schema.TaggedErrorClass<LifecycleRpcTimeoutError>(
  "LifecycleRpcTimeoutError",
)("Lifecycle.RpcTimeout", { method: Schema.String, timeoutMs: Schema.Number }) {
  readonly tag = "LIFECYCLE_RPC_TIMEOUT"
  constructor(
    override readonly method: string,
    override readonly timeoutMs: number,
  ) {
    super({ method, timeoutMs })
    this.name = "LifecycleRpcTimeoutError"
  }
  override get message() {
    return `lifecycle RPC ${this.method} was not acknowledged within ${this.timeoutMs}ms`
  }
}

export interface ProxyReportingOptions {
  /** Per-RPC bound. Defaults to {@link LIFECYCLE_RPC_TIMEOUT_MS}. */
  readonly timeoutMs?: number
}

function withDeadline<T>(
  method: string,
  timeoutMs: number,
  operation: () => Promise<T>,
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    let settled = false
    const timer = setTimeout(() => {
      if (settled) return
      settled = true
      reject(new LifecycleRpcTimeoutError(method, timeoutMs))
    }, timeoutMs)
    timer.unref?.()
    const finish = (settle: () => void) => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      settle()
    }
    operation().then(
      (value) => finish(() => resolve(value)),
      (error) => finish(() => reject(error)),
    )
  })
}

/** Build the hub-proxy backed reporting seam. */
export function createProxyReporting(
  call: (method: string, args: readonly unknown[]) => Promise<unknown>,
  options: ProxyReportingOptions = {},
): LifecycleReporting {
  const timeoutMs = options.timeoutMs ?? LIFECYCLE_RPC_TIMEOUT_MS
  const send = (method: string, args: readonly unknown[]): Promise<void> =>
    withDeadline(method, timeoutMs, () => call(method, args)).then(() => undefined)
  const bestEffort = (method: string, args: readonly unknown[]): Promise<void> =>
    send(method, args).then(
      () => undefined,
      () => undefined,
    )
  return {
    // Record the commit locally first, then await the hub's acknowledgement within the lifecycle deadline.
    commit: async (sessionID, messageID) => {
      if (typeof messageID === "string" && messageID.length > 0) committedBySession.set(sessionID, messageID)
      try {
        await withDeadline("lifecycle.commit", timeoutMs, () => call("lifecycle.commit", [sessionID, messageID]))
      } catch {
        // Best effort on teardown: the durable message row remains the truth.
      }
    },
    toolStarted: (sessionID, callID, sideEffecting = true) =>
      send("lifecycle.toolStarted", [sessionID, callID, sideEffecting]),
    toolFinished: (sessionID, callID) => bestEffort("lifecycle.toolFinished", [sessionID, callID]),
    phase: (sessionID, phase) => bestEffort("lifecycle.phase", [sessionID, phase]),
  }
}
