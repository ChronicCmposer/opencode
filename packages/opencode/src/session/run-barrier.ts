// Per-Session run barrier (D18).
//
// A run is not finished when the model loop stops. Before the next run may be
// admitted, one Session must observe four things:
//
//   1. its durable commits landed,
//   2. the projectors ran (inside the committing transaction),
//   3. tools settled, and
//   4. the background-promotion decision was made.
//
// Delta acknowledgements are deliberately absent: deltas are advisory and may be
// dropped (D17), so they can never be a correctness barrier.
//
// Phases settle only when their real observation is reported. A begun run opens
// an unreported report set; `RunBarrierRun` accepts one report per phase from the
// code that actually observed it. A zero-tool run settles `tools` explicitly
// through `noTools`; it is never inferred from the run having started. A missing
// report is a typed `RunBarrierTimeout` (or a `RunBarrierReportError` refusal for
// an invalid/stale report), and cancellation wins over every drain.
//
// The barrier is per Session and bounded at 5 s. Expiry raises the typed
// `RunBarrierTimeout` and leaves the Session in `timed-out`, so the next run is
// refused until an explicit `reset` (an operator retry) or `abandon` (recovery
// for a barrier whose owning run/worker is gone and can never settle). A live
// worker keeps the refusal: timed-out is not a free pass.

/** The hard bound every per-Session barrier is subject to. */
export const RUN_BARRIER_TIMEOUT_MS = 5000

/** The four commit-side phases a run must settle before the next run starts. */
export const RUN_BARRIER_PHASES = ["commits", "projectors", "tools", "promotion"] as const

export type RunBarrierPhase = (typeof RUN_BARRIER_PHASES)[number]

/** The background-promotion outcome a run observed (or deliberately decided). */
export type RunBarrierPromotionOutcome = "promoted" | "not-promoted" | "no-job"

/**
 * What a reporter actually observed. The observation kind must match the phase:
 * a commit cannot stand in for a projector, and `tool-none` is the explicit
 * zero-tool settlement.
 */
export type RunBarrierObservation =
  | { readonly kind: "commit"; readonly messageID: string }
  | { readonly kind: "projector"; readonly projector: string }
  | { readonly kind: "tool"; readonly callID: string }
  | { readonly kind: "tool-none" }
  | { readonly kind: "promotion"; readonly outcome: RunBarrierPromotionOutcome }

export interface RunBarrierPhaseReport {
  readonly sessionID: string
  readonly phase: RunBarrierPhase
  readonly observation: RunBarrierObservation
}

/**
 * The report handle for one run. Reports are only accepted for the run that
 * opened them: a late token cannot settle a successor run.
 */
export interface RunBarrierRun {
  readonly sessionID: string
  /** Record a real observation. Returns `false` when this run was superseded. */
  report(report: RunBarrierPhaseReport): boolean
  commit(messageID: string): boolean
  projector(projector: string): boolean
  tool(callID: string): boolean
  /** Settle a run that observed no tool calls; never inferred from run start. */
  noTools(): boolean
  promotion(outcome: RunBarrierPromotionOutcome): boolean
  reported(phase: RunBarrierPhase): boolean
  /** Phases whose report has not arrived yet, in canonical order. */
  missing(): readonly RunBarrierPhase[]
}

/** Caller-supplied phases. A test seam; production uses reported phases. */
export interface RunBarrierPhases {
  readonly commits: Promise<unknown>
  readonly projectors: Promise<unknown>
  readonly tools: Promise<unknown>
  readonly promotion: Promise<unknown>
}

export interface RunBarrierOutcome {
  readonly sessionID: string
  readonly status: "drained" | "cancelled"
}

export type RunBarrierState = "idle" | "draining" | "drained" | "timed-out" | "cancelled" | "failed"

/** Injectable timer seam, so the bound is deterministic in tests. */
export interface RunBarrierScheduler {
  schedule(callback: () => void, delayMs: number): unknown
  cancel(handle: unknown): void
}

const defaultScheduler: RunBarrierScheduler = {
  schedule: (callback, delayMs) => setTimeout(callback, delayMs),
  cancel: (handle) => {
    if (typeof handle === "number" || (typeof handle === "object" && handle !== null)) {
      clearTimeout(handle as ReturnType<typeof setTimeout>)
    }
  },
}

export class RunBarrierError extends Error {
  readonly tag: string = "RunBarrierError"
  constructor(message: string) {
    super(message)
    this.name = "RunBarrierError"
  }
}

/** Raised when the commit-side phases did not settle within the 5 s bound. */
export class RunBarrierTimeout extends RunBarrierError {
  override readonly tag = "RunBarrierTimeout"
  constructor(
    readonly sessionID: string,
    readonly timeoutMs: number,
  ) {
    super(`run barrier for session ${sessionID} timed out after ${timeoutMs}ms`)
    this.name = "RunBarrierTimeout"
  }
}

/** A second barrier for a Session whose previous barrier is still draining. */
export class RunBarrierBusyError extends RunBarrierError {
  override readonly tag = "RunBarrierBusyError"
  constructor(readonly sessionID: string) {
    super(`run barrier for session ${sessionID} is still draining`)
    this.name = "RunBarrierBusyError"
  }
}

/** A phase report that cannot belong to the run it names. */
export class RunBarrierReportError extends RunBarrierError {
  override readonly tag: string = "RunBarrierReportError"
  constructor(
    readonly sessionID: string,
    message: string,
  ) {
    super(message)
    this.name = "RunBarrierReportError"
  }
}

/**
 * The barrier was asked to wait for a run that never opened a report set, so no
 * phase can ever settle. This is a typed refusal, not a timeout.
 */
export class RunBarrierMissingReportError extends RunBarrierReportError {
  override readonly tag: string = "RunBarrierMissingReportError"
  constructor(
    override readonly sessionID: string,
    readonly missing: readonly RunBarrierPhase[],
  ) {
    super(
      sessionID,
      `run barrier for session ${sessionID} has no begun run; missing reports: ${missing.join(", ")}`,
    )
    this.name = "RunBarrierMissingReportError"
  }
}

export interface RunBarrierOptions {
  /** Barrier bound; defaults to {@link RUN_BARRIER_TIMEOUT_MS}. */
  readonly timeoutMs?: number
  readonly scheduler?: RunBarrierScheduler
  /**
   * Maximum Session states retained. Terminal sessions evict LRU once the cap is
   * reached; a draining barrier is never evicted. Defaults to 1024.
   */
  readonly maxSessions?: number
}

export interface RunBarrier {
  /**
   * Open a fresh report set for a run. No phase is settled by this call: the
   * caller must report each phase from the place that observed it.
   */
  beginRun(sessionID: string): RunBarrierRun
  /** Admit the next run only after all four phases were really reported. */
  wait(sessionID: string): Promise<RunBarrierOutcome>
  /** Admit after caller-supplied phases settle (test seam, not production). */
  wait(sessionID: string, phases: RunBarrierPhases): Promise<RunBarrierOutcome>
  /** Cancel the active barrier (or the next one) for `sessionID`. */
  cancel(sessionID: string): void
  state(sessionID: string): RunBarrierState
  /** Clear a timed-out/cancelled/drained state so a later run may be admitted. */
  reset(sessionID: string): void
  /**
   * Forget a barrier whose owning run can no longer settle (its worker was
   * retired by a crash or terminate). Refuses a still-draining barrier: a live
   * run keeps ownership of its drain. Idempotent.
   */
  abandon(sessionID: string): void
  /** Number of retained Session states (bounded). */
  readonly size: number
}

interface ActiveBarrier {
  settleCancelled(): void
}

interface RunRecord {
  readonly sessionID: string
  readonly epoch: number
  readonly reported: Set<RunBarrierPhase>
  readonly kinds: Map<RunBarrierPhase, RunBarrierObservation["kind"]>
  readonly waiters: Set<() => void>
  invalid: RunBarrierReportError | undefined
}

const OBSERVATION_KINDS: Record<RunBarrierPhase, readonly RunBarrierObservation["kind"][]> = {
  commits: ["commit"],
  projectors: ["projector"],
  tools: ["tool", "tool-none"],
  promotion: ["promotion"],
}

function requireText(value: unknown, label: string, sessionID: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new RunBarrierReportError(sessionID, `run barrier report ${label} must be a non-empty string`)
  }
  return value
}

function validateReport(sessionID: string, report: RunBarrierPhaseReport): RunBarrierPhaseReport {
  if (report === null || typeof report !== "object") {
    throw new RunBarrierReportError(sessionID, "run barrier report must be an object")
  }
  if (report.sessionID !== sessionID) {
    throw new RunBarrierReportError(
      sessionID,
      `run barrier report for session ${sessionID} came from ${String(report.sessionID)}`,
    )
  }
  if (!RUN_BARRIER_PHASES.includes(report.phase)) {
    throw new RunBarrierReportError(sessionID, `run barrier report has unknown phase ${String(report.phase)}`)
  }
  const observation = report.observation
  if (observation === null || typeof observation !== "object") {
    throw new RunBarrierReportError(sessionID, `run barrier ${report.phase} report has no observation`)
  }
  if (!OBSERVATION_KINDS[report.phase].includes(observation.kind)) {
    throw new RunBarrierReportError(
      sessionID,
      `run barrier ${report.phase} report cannot use observation ${String(observation.kind)}`,
    )
  }
  switch (observation.kind) {
    case "commit":
      requireText(observation.messageID, "messageID", sessionID)
      break
    case "projector":
      requireText(observation.projector, "projector", sessionID)
      break
    case "tool":
      requireText(observation.callID, "callID", sessionID)
      break
    case "tool-none":
      break
    case "promotion":
      if (
        observation.outcome !== "promoted" &&
        observation.outcome !== "not-promoted" &&
        observation.outcome !== "no-job"
      ) {
        throw new RunBarrierReportError(
          sessionID,
          `run barrier promotion report has unknown outcome ${String(observation.outcome)}`,
        )
      }
      break
  }
  return report
}

export function createRunBarrier(options: RunBarrierOptions = {}): RunBarrier {
  const timeoutMs = options.timeoutMs ?? RUN_BARRIER_TIMEOUT_MS
  if (!Number.isInteger(timeoutMs) || timeoutMs < 1) {
    throw new Error(`invalid run barrier timeout: ${timeoutMs} (expected a positive integer)`)
  }
  const scheduler = options.scheduler ?? defaultScheduler
  const maxSessions = options.maxSessions ?? 1024
  if (!Number.isInteger(maxSessions) || maxSessions < 1) {
    throw new Error(`invalid run barrier session bound: ${maxSessions}`)
  }

  const states = new Map<string, RunBarrierState>()
  const failures = new Map<string, Error>()
  const active = new Map<string, ActiveBarrier>()
  const runs = new Map<string, RunRecord>()
  let runEpoch = 0

  const stateOf = (sessionID: string): RunBarrierState => states.get(sessionID) ?? "idle"

  const notify = (record: RunRecord): void => {
    if (record.reported.size !== RUN_BARRIER_PHASES.length) return
    for (const waiter of [...record.waiters]) waiter()
    record.waiters.clear()
  }

  const makeRun = (record: RunRecord): RunBarrierRun => {
    const current = (): boolean => runs.get(record.sessionID)?.epoch === record.epoch && record.invalid === undefined
    const invalidate = (error: RunBarrierReportError): never => {
      record.invalid = error
      throw error
    }
    const report = (input: RunBarrierPhaseReport): boolean => {
      if (!current()) return false
      let report: RunBarrierPhaseReport
      try {
        report = validateReport(record.sessionID, input)
      } catch (error) {
        if (error instanceof RunBarrierReportError) return invalidate(error)
        throw error
      }
      const observed = record.kinds.get(report.phase)
      // `tools` has two possible observations; a run cannot claim both an
      // explicit zero-tool settle and a real tool settlement.
      if (observed !== undefined && observed !== report.observation.kind) {
        return invalidate(
          new RunBarrierReportError(
            record.sessionID,
            `run barrier tools report conflicts: ${observed} then ${report.observation.kind}`,
          ),
        )
      }
      record.kinds.set(report.phase, report.observation.kind)
      if (!record.reported.has(report.phase)) {
        record.reported.add(report.phase)
        notify(record)
      }
      return true
    }
    return {
      sessionID: record.sessionID,
      report,
      commit: (messageID) => report({ sessionID: record.sessionID, phase: "commits", observation: { kind: "commit", messageID } }),
      projector: (projector) =>
        report({ sessionID: record.sessionID, phase: "projectors", observation: { kind: "projector", projector } }),
      tool: (callID) => report({ sessionID: record.sessionID, phase: "tools", observation: { kind: "tool", callID } }),
      noTools: () => report({ sessionID: record.sessionID, phase: "tools", observation: { kind: "tool-none" } }),
      promotion: (outcome) =>
        report({ sessionID: record.sessionID, phase: "promotion", observation: { kind: "promotion", outcome } }),
      reported: (phase) => record.reported.has(phase),
      missing: () => RUN_BARRIER_PHASES.filter((phase) => !record.reported.has(phase)),
    }
  }

  /** Insert/refresh LRU order, then evict the oldest evictable state. */
  const remember = (sessionID: string, state: RunBarrierState): void => {
    states.delete(sessionID)
    states.set(sessionID, state)
    if (state !== "failed") failures.delete(sessionID)
    while (states.size > maxSessions) {
      let evicted = false
      for (const [key, value] of states) {
        if (key === sessionID || value === "draining") continue
        states.delete(key)
        runs.delete(key)
        failures.delete(key)
        evicted = true
        break
      }
      if (!evicted) break
    }
  }

  return {
    beginRun(sessionID) {
      if (sessionID.length === 0) throw new RunBarrierError("run barrier requires a non-empty sessionID")
      if (active.has(sessionID) || stateOf(sessionID) === "draining") throw new RunBarrierBusyError(sessionID)
      const record: RunRecord = {
        sessionID,
        epoch: ++runEpoch,
        reported: new Set(),
        kinds: new Map(),
        waiters: new Set(),
        invalid: undefined,
      }
      runs.set(sessionID, record)
      return makeRun(record)
    },
    wait(sessionID: string, phases?: RunBarrierPhases) {
      if (sessionID.length === 0) throw new RunBarrierError("run barrier requires a non-empty sessionID")

      const state = stateOf(sessionID)
      if (state === "timed-out") return Promise.reject(new RunBarrierTimeout(sessionID, timeoutMs))
      if (state === "failed") {
        return Promise.reject(failures.get(sessionID) ?? new RunBarrierError(`run barrier for session ${sessionID} previously failed`))
      }
      // A cancelled Session does not start a next run either; reset re-admits it.
      if (state === "cancelled") return Promise.resolve({ sessionID, status: "cancelled" })
      if (state === "draining") return Promise.reject(new RunBarrierBusyError(sessionID))

      let run: RunRecord | undefined
      if (phases === undefined) {
        run = runs.get(sessionID)
        if (run === undefined) {
          const error = new RunBarrierMissingReportError(sessionID, [...RUN_BARRIER_PHASES])
          failures.set(sessionID, error)
          remember(sessionID, "failed")
          return Promise.reject(error)
        }
        if (run.invalid !== undefined) {
          failures.set(sessionID, run.invalid)
          remember(sessionID, "failed")
          return Promise.reject(run.invalid)
        }
      }

      remember(sessionID, "draining")

      return new Promise<RunBarrierOutcome>((resolve, reject) => {
        let settled = false
        let timer: unknown
        // Assigned below; referenced by `detach`/`fail` from callbacks that only
        // run after the assignment.
        let check: () => void = () => {}

        const detach = (): void => {
          if (run !== undefined) run.waiters.delete(check)
        }

        const finish = (outcome: RunBarrierOutcome): void => {
          if (settled) return
          settled = true
          detach()
          if (timer !== undefined) scheduler.cancel(timer)
          timer = undefined
          active.delete(sessionID)
          remember(sessionID, outcome.status)
          resolve(outcome)
        }

        const fail = (error: Error): void => {
          if (settled) return
          settled = true
          detach()
          if (timer !== undefined) scheduler.cancel(timer)
          timer = undefined
          active.delete(sessionID)
          const state: RunBarrierState = error instanceof RunBarrierTimeout ? "timed-out" : "failed"
          if (state === "failed") failures.set(sessionID, error)
          remember(sessionID, state)
          reject(error)
        }

        const settleCancelled = (): void => finish({ sessionID, status: "cancelled" })
        active.set(sessionID, { settleCancelled })

        timer = scheduler.schedule(() => fail(new RunBarrierTimeout(sessionID, timeoutMs)), timeoutMs)

        check = (): void => {
          if (settled) return
          if (stateOf(sessionID) === "cancelled") {
            settleCancelled()
            return
          }
          if (run === undefined) return
          if (run.invalid !== undefined) {
            fail(run.invalid)
            return
          }
          if (run.reported.size === RUN_BARRIER_PHASES.length) finish({ sessionID, status: "drained" })
        }

        if (phases !== undefined) {
          Promise.all([phases.commits, phases.projectors, phases.tools, phases.promotion]).then(
            () => {
              // Cancellation wins: `cancel` ran before this continuation, so the
              // active entry is either gone (already cancelled) or this check sees
              // the cancelled state.
              if (settled || stateOf(sessionID) === "cancelled") {
                settleCancelled()
                return
              }
              finish({ sessionID, status: "drained" })
            },
            (error: unknown) => {
              // Phase failures are caller bugs and must not be swallowed; keep the
              // barrier closed so the next run is not admitted after a broken drain.
              if (settled) return
              fail(error instanceof Error ? error : new RunBarrierError(String(error)))
            },
          )
          return
        }

        // Report path: a run that never reports a phase stays draining until
        // the bound expires. `check` also handles reports that landed before
        // `wait` was called.
        run!.waiters.add(check)
        check()
      })
    },
    cancel(sessionID) {
      const current = active.get(sessionID)
      if (current) {
        // `settleCancelled` is idempotent via the `settled` flag in `wait`.
        remember(sessionID, "cancelled")
        current.settleCancelled()
        return
      }
      remember(sessionID, "cancelled")
    },
    state: stateOf,
    reset(sessionID) {
      if (active.has(sessionID)) throw new RunBarrierBusyError(sessionID)
      states.delete(sessionID)
      failures.delete(sessionID)
      runs.delete(sessionID)
    },
    abandon(sessionID) {
      if (sessionID.length === 0) throw new RunBarrierError("run barrier requires a non-empty sessionID")
      // A live drain still owns the phases; abandoning it would let the next run
      // overtake work that is genuinely in flight.
      if (active.has(sessionID)) throw new RunBarrierBusyError(sessionID)
      states.delete(sessionID)
      failures.delete(sessionID)
      runs.delete(sessionID)
    },
    get size() {
      return states.size
    },
  }
}

export * as SessionRunBarrier from "./run-barrier"
