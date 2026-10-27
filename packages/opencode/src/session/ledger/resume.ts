import type { WorkerCrashPhase } from "./crash"

export const AUTO_RESUME_CAP = 10

/** Boot never resumes: no run existed, so there is nothing to replay. */
export const RESUMABLE_PHASES = ["materialize", "stream", "tool", "commit"] as const

export type ResumeRefusal = "cancelled" | "cap" | "no-committed-message" | "phase" | "indeterminate"

export interface IndeterminateCall {
  readonly callID: string
  readonly sideEffecting: boolean
}

export interface AutoResumeInput {
  readonly sessionID: string
  readonly phase: WorkerCrashPhase
  /** Auto-resumes already performed for this run. */
  readonly attempts: number
  readonly cancelled?: boolean
  readonly lastCommittedMessageID: string | null
  /** Orphaned `started` calls from the ledger; any side-effecting one blocks resume. */
  readonly indeterminate?: readonly IndeterminateCall[]
}

export type AutoResumeDecision =
  | {
      readonly resume: true
      readonly sessionID: string
      readonly attempt: number
      readonly phase: WorkerCrashPhase
      readonly fromMessageID: string
    }
  | {
      readonly resume: false
      readonly sessionID: string
      readonly phase: WorkerCrashPhase
      readonly reason: ResumeRefusal
    }

function isResumablePhase(phase: WorkerCrashPhase): boolean {
  return (RESUMABLE_PHASES as readonly string[]).includes(phase)
}

/**
 * Decide whether a crash may auto-resume. Cancel always wins, the cap is per run,
 * and any side-effecting orphaned call permanently makes the attempt ambiguous.
 */
export function decideAutoResume(input: AutoResumeInput): AutoResumeDecision {
  if (typeof input.sessionID !== "string" || input.sessionID.length === 0) {
    throw new Error("auto-resume requires a non-empty sessionID")
  }
  if (!Number.isInteger(input.attempts) || input.attempts < 0) {
    throw new Error(`auto-resume attempts must be a non-negative integer, received ${String(input.attempts)}`)
  }
  if (input.cancelled === true) {
    return { resume: false, sessionID: input.sessionID, phase: input.phase, reason: "cancelled" }
  }
  if (input.attempts >= AUTO_RESUME_CAP) {
    return { resume: false, sessionID: input.sessionID, phase: input.phase, reason: "cap" }
  }
  if (!isResumablePhase(input.phase)) {
    return { resume: false, sessionID: input.sessionID, phase: input.phase, reason: "phase" }
  }
  if (input.lastCommittedMessageID === null) {
    return { resume: false, sessionID: input.sessionID, phase: input.phase, reason: "no-committed-message" }
  }
  if (input.indeterminate?.some((call) => call.sideEffecting) === true) {
    return { resume: false, sessionID: input.sessionID, phase: input.phase, reason: "indeterminate" }
  }
  return {
    resume: true,
    sessionID: input.sessionID,
    attempt: input.attempts + 1,
    phase: input.phase,
    fromMessageID: input.lastCommittedMessageID,
  }
}

export interface AutoResumeController {
  readonly attempts: (sessionID: string) => number
  readonly decide: (input: Omit<AutoResumeInput, "attempts">) => AutoResumeDecision
  /** Record that the resume started; returns the attempt number. */
  readonly record: (sessionID: string) => number
  /** A completed/healthy run resets the cap for the next run. */
  readonly reset: (sessionID: string) => void
}

export interface AutoResumeControllerOptions {
  /** Maximum sessions retained; the oldest attempt count evicts. Defaults to 1024. */
  readonly maxSessions?: number
}

export function createAutoResumeController(options: AutoResumeControllerOptions = {}): AutoResumeController & {
  readonly size: number
} {
  const maxSessions = options.maxSessions ?? 1024
  if (!Number.isInteger(maxSessions) || maxSessions < 1) {
    throw new Error(`invalid auto-resume session bound: ${maxSessions}`)
  }
  const attempts = new Map<string, number>()
  const remember = (sessionID: string, value: number): number => {
    attempts.delete(sessionID)
    attempts.set(sessionID, value)
    while (attempts.size > maxSessions) {
      const oldest = attempts.keys().next().value
      if (oldest === undefined) break
      attempts.delete(oldest)
    }
    return value
  }
  return {
    attempts: (sessionID) => attempts.get(sessionID) ?? 0,
    decide: (input) => decideAutoResume({ ...input, attempts: attempts.get(input.sessionID) ?? 0 }),
    record(sessionID) {
      return remember(sessionID, (attempts.get(sessionID) ?? 0) + 1)
    },
    reset(sessionID) {
      attempts.delete(sessionID)
    },
    get size() {
      return attempts.size
    },
  }
}

export * as AutoResume from "./resume"
