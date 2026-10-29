
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Context, Deferred, Effect, Layer, Schema } from "effect"

/** The hard bound every per-Session barrier is subject to. */
export const RUN_BARRIER_TIMEOUT_MS = 5000

/** The four commit-side phases a run must settle before the next run starts. */
export const RUN_BARRIER_PHASES = ["commits", "projectors", "tools", "promotion"] as const

export type RunBarrierPhase = (typeof RUN_BARRIER_PHASES)[number]

/** The background-promotion outcome a run observed (or deliberately decided). */
export type RunBarrierPromotionOutcome = "promoted" | "not-promoted" | "no-job"

/** What a reporter actually observed. */
export type RunBarrierObservation =
  | { readonly kind: "commit"; readonly messageID: string }
  | { readonly kind: "projector"; readonly projector: string }
  | { readonly kind: "tool"; readonly callID: string }
  | { readonly kind: "tool-none" }
  | { readonly kind: "promotion"; readonly outcome: RunBarrierPromotionOutcome }
  /** The explicit zero-commit settlement: nothing was committed, nothing projected. */
  | { readonly kind: "none" }

export interface RunBarrierPhaseReport {
  readonly sessionID: string
  readonly phase: RunBarrierPhase
  readonly observation: RunBarrierObservation
}

/** The commit-side outcome a run recorded. `null` is the explicit zero-commit settlement. */
export interface RunBarrierCommitOutcome {
  readonly committed: string | null
}

/** The report handle for one run. */
export interface RunBarrierRun {
  readonly sessionID: string
  /** Record a real observation. Returns `false` when this run was superseded. */
  report(report: RunBarrierPhaseReport): boolean
  commit(messageID: string): boolean
  projector(projector: string): boolean
  tool(callID: string): boolean
  /** Settle a run that observed no tool calls; never inferred from run start. */
  noTools(): boolean
  /** Settle a run that committed nothing (a shell turn, a loop that exited before its first model step, a compaction stop). */
  noCommit(): boolean
  promotion(outcome: RunBarrierPromotionOutcome): boolean
  reported(phase: RunBarrierPhase): boolean
  /** Phases whose report has not arrived yet, in canonical order. */
  missing(): readonly RunBarrierPhase[]
  /** The commit outcome recorded by this run, once the commits phase settled. */
  commitOutcome(): RunBarrierCommitOutcome | undefined
}

export interface RunBarrierOutcome {
  readonly sessionID: string
  readonly status: "drained" | "cancelled"
}

export type RunBarrierState = "idle" | "draining" | "drained" | "timed-out" | "cancelled" | "failed"

/** Raised when the commit-side phases did not settle within the 5 s bound. */
export class RunBarrierTimeout extends Schema.TaggedErrorClass<RunBarrierTimeout>()("RunBarrier.Timeout", {
  sessionID: Schema.String,
  timeoutMs: Schema.Number,
}) {
  override get message(): string {
    return `run barrier for session ${this.sessionID} timed out after ${this.timeoutMs}ms`
  }
}

/** A second barrier for a Session whose previous barrier is still draining. */
export class RunBarrierBusyError extends Schema.TaggedErrorClass<RunBarrierBusyError>()("RunBarrier.Busy", {
  sessionID: Schema.String,
}) {
  override get message(): string {
    return `run barrier for session ${this.sessionID} is still draining`
  }
}

/** A phase report that cannot belong to the run it names. */
export class RunBarrierReportError extends Schema.TaggedErrorClass<RunBarrierReportError>()("RunBarrier.Report", {
  sessionID: Schema.String,
  message: Schema.String,
}) {}

/** The barrier was asked to wait for a run that never opened a report set, so no phase can ever settle. */
export class RunBarrierMissingReportError extends Schema.TaggedErrorClass<RunBarrierMissingReportError>()(
  "RunBarrier.MissingReport",
  {
    sessionID: Schema.String,
    missing: Schema.Array(Schema.Literals(RUN_BARRIER_PHASES)),
  },
) {
  override get message(): string {
    return `run barrier for session ${this.sessionID} has no begun run; missing reports: ${this.missing.join(", ")}`
  }
}

/** The active wait was cancelled; `wait` converts this into a `cancelled` outcome. */
export class RunBarrierCancelled extends Schema.TaggedErrorClass<RunBarrierCancelled>()("RunBarrier.Cancelled", {
  sessionID: Schema.String,
}) {
  override get message(): string {
    return `run barrier for session ${this.sessionID} was cancelled`
  }
}

export type RunBarrierWaitError =
  | RunBarrierTimeout
  | RunBarrierBusyError
  | RunBarrierMissingReportError
  | RunBarrierReportError

interface RunRecord {
  readonly sessionID: string
  readonly epoch: number
  readonly reported: Set<RunBarrierPhase>
  readonly kinds: Map<RunBarrierPhase, RunBarrierObservation["kind"]>
  readonly done: Deferred.Deferred<RunBarrierOutcome, RunBarrierCancelled | RunBarrierReportError>
  invalid: RunBarrierReportError | undefined
  commitOutcome: RunBarrierCommitOutcome | undefined
}

const OBSERVATION_KINDS: Record<RunBarrierPhase, readonly RunBarrierObservation["kind"][]> = {
  commits: ["commit", "none"],
  projectors: ["projector", "none"],
  tools: ["tool", "tool-none"],
  promotion: ["promotion"],
}

/** Maximum retained Session states. A draining barrier is never evicted. */
export const RUN_BARRIER_MAX_SESSIONS = 1024

function textError(value: unknown, label: string, sessionID: string): RunBarrierReportError | undefined {
  if (typeof value === "string" && value.length > 0) return undefined
  return new RunBarrierReportError({
    sessionID,
    message: `run barrier report ${label} must be a non-empty string`,
  })
}

function reportError(sessionID: string, message: string): RunBarrierReportError {
  return new RunBarrierReportError({ sessionID, message })
}

function validateReport(sessionID: string, report: RunBarrierPhaseReport): RunBarrierReportError | undefined {
  if (report === null || typeof report !== "object") return reportError(sessionID, "run barrier report must be an object")
  if (report.sessionID !== sessionID) {
    return reportError(
      sessionID,
      `run barrier report for session ${sessionID} came from ${String(report.sessionID)}`,
    )
  }
  if (!RUN_BARRIER_PHASES.includes(report.phase)) {
    return reportError(sessionID, `run barrier report has unknown phase ${String(report.phase)}`)
  }
  const observation = report.observation
  if (observation === null || typeof observation !== "object") {
    return reportError(sessionID, `run barrier ${report.phase} report has no observation`)
  }
  if (!OBSERVATION_KINDS[report.phase].includes(observation.kind)) {
    return reportError(
      sessionID,
      `run barrier ${report.phase} report cannot use observation ${String(observation.kind)}`,
    )
  }
  switch (observation.kind) {
    case "commit":
      return textError(observation.messageID, "messageID", sessionID)
    case "projector":
      return textError(observation.projector, "projector", sessionID)
    case "tool":
      return textError(observation.callID, "callID", sessionID)
    case "tool-none":
    case "none":
      return undefined
    case "promotion":
      if (
        observation.outcome !== "promoted" &&
        observation.outcome !== "not-promoted" &&
        observation.outcome !== "no-job"
      ) {
        return reportError(
          sessionID,
          `run barrier promotion report has unknown outcome ${String(observation.outcome)}`,
        )
      }
      return undefined
  }
}

/** The barrier surface every consumer uses. */
export interface Interface {
  /** Open a fresh report set for a run. */
  readonly beginRun: (sessionID: string) => Effect.Effect<RunBarrierRun, RunBarrierBusyError | RunBarrierReportError>
  /** Admit the next run only after all four phases were really reported. */
  readonly wait: (sessionID: string) => Effect.Effect<RunBarrierOutcome, RunBarrierWaitError>
  /** Cancel the active barrier (or the next one) for `sessionID`. */
  readonly cancel: (sessionID: string) => Effect.Effect<void>
  readonly state: (sessionID: string) => RunBarrierState
  /** Clear a timed-out/cancelled/drained state so a later run may be admitted. */
  readonly reset: (sessionID: string) => Effect.Effect<void, RunBarrierBusyError>
  /** Forget a barrier whose owning run can no longer settle (its worker was retired by a crash or terminate). */
  readonly abandon: (sessionID: string) => Effect.Effect<void, RunBarrierBusyError | RunBarrierReportError>
  /** Number of retained Session states (bounded). */
  readonly size: number
}

export class Service extends Context.Service<Service, Interface>()("@opencode/RunBarrier") {}

function makeBarrier(): Interface {
  const states = new Map<string, RunBarrierState>()
  const failures = new Map<string, RunBarrierWaitError>()
  const active = new Map<string, RunRecord>()
  const runs = new Map<string, RunRecord>()
  let runEpoch = 0

  const stateOf = (sessionID: string): RunBarrierState => states.get(sessionID) ?? "idle"

  const finishRun = (record: RunRecord, outcome: RunBarrierOutcome): void => {
    Deferred.doneUnsafe(record.done, Effect.succeed(outcome))
  }

  const makeRun = (record: RunRecord): RunBarrierRun => {
    const current = (): boolean => runs.get(record.sessionID)?.epoch === record.epoch && record.invalid === undefined
    const invalidate = (error: RunBarrierReportError): never => {
      record.invalid = error
      Deferred.doneUnsafe(record.done, Effect.fail(error))
      throw error
    }
    const report = (input: RunBarrierPhaseReport): boolean => {
      if (!current()) return false
      const invalid = validateReport(record.sessionID, input)
      if (invalid !== undefined) return invalidate(invalid)
      const report = input
      const observed = record.kinds.get(report.phase)
      if (observed !== undefined && observed !== report.observation.kind) {
        return invalidate(
          new RunBarrierReportError({
            sessionID: record.sessionID,
            message: `run barrier ${report.phase} report conflicts: ${observed} then ${report.observation.kind}`,
          }),
        )
      }
      record.kinds.set(report.phase, report.observation.kind)
      if (report.phase === "commits") {
        record.commitOutcome = {
          committed: report.observation.kind === "commit" ? report.observation.messageID : null,
        }
      }
      if (!record.reported.has(report.phase)) {
        record.reported.add(report.phase)
        if (record.reported.size === RUN_BARRIER_PHASES.length) {
          finishRun(record, { sessionID: record.sessionID, status: "drained" })
        }
      }
      return true
    }
    return {
      sessionID: record.sessionID,
      report,
      commit: (messageID) =>
        report({ sessionID: record.sessionID, phase: "commits", observation: { kind: "commit", messageID } }),
      projector: (projector) =>
        report({ sessionID: record.sessionID, phase: "projectors", observation: { kind: "projector", projector } }),
      tool: (callID) => report({ sessionID: record.sessionID, phase: "tools", observation: { kind: "tool", callID } }),
      noTools: () => report({ sessionID: record.sessionID, phase: "tools", observation: { kind: "tool-none" } }),
      noCommit: () =>
        report({ sessionID: record.sessionID, phase: "commits", observation: { kind: "none" } }) &&
        report({ sessionID: record.sessionID, phase: "projectors", observation: { kind: "none" } }),
      promotion: (outcome) =>
        report({ sessionID: record.sessionID, phase: "promotion", observation: { kind: "promotion", outcome } }),
      reported: (phase) => record.reported.has(phase),
      missing: () => RUN_BARRIER_PHASES.filter((phase) => !record.reported.has(phase)),
      commitOutcome: () => record.commitOutcome,
    }
  }

  /** Insert/refresh LRU order, then evict the oldest evictable state. */
  const remember = (sessionID: string, state: RunBarrierState): void => {
    states.delete(sessionID)
    states.set(sessionID, state)
    if (state !== "failed") failures.delete(sessionID)
    while (states.size > RUN_BARRIER_MAX_SESSIONS) {
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

  const failureOf = (sessionID: string): RunBarrierWaitError =>
    failures.get(sessionID) ??
    new RunBarrierReportError({
      sessionID,
      message: `run barrier for session ${sessionID} previously failed`,
    })

  const beginRun = Effect.fn("RunBarrier.beginRun")(function* (sessionID: string) {
    if (sessionID.length === 0) {
      return yield* reportError(sessionID, "run barrier requires a non-empty sessionID")
    }
    if (active.has(sessionID) || stateOf(sessionID) === "draining") {
      return yield* new RunBarrierBusyError({ sessionID })
    }
    const record: RunRecord = {
      sessionID,
      epoch: ++runEpoch,
      reported: new Set(),
      kinds: new Map(),
      done: Deferred.makeUnsafe(),
      invalid: undefined,
      commitOutcome: undefined,
    }
    runs.set(sessionID, record)
    return makeRun(record)
  })

  const wait = Effect.fn("RunBarrier.wait")(function* (sessionID: string) {
    if (sessionID.length === 0) {
      return yield* new RunBarrierReportError({ sessionID, message: "run barrier requires a non-empty sessionID" })
    }

    const state = stateOf(sessionID)
    if (state === "timed-out") {
      return yield* new RunBarrierTimeout({ sessionID, timeoutMs: RUN_BARRIER_TIMEOUT_MS })
    }
    if (state === "failed") return yield* failureOf(sessionID)
    if (state === "cancelled") return { sessionID, status: "cancelled" as const }
    if (state === "draining") return yield* new RunBarrierBusyError({ sessionID })

    const record = runs.get(sessionID)
    if (record === undefined) {
      const error = new RunBarrierMissingReportError({ sessionID, missing: [...RUN_BARRIER_PHASES] })
      failures.set(sessionID, error)
      remember(sessionID, "failed")
      return yield* Effect.fail(error)
    }
    if (record.invalid !== undefined) {
      failures.set(sessionID, record.invalid)
      remember(sessionID, "failed")
      return yield* Effect.fail(record.invalid)
    }

    remember(sessionID, "draining")
    active.set(sessionID, record)

    const outcome = yield* Deferred.await(record.done).pipe(
      Effect.timeoutOrElse({
        duration: RUN_BARRIER_TIMEOUT_MS,
        orElse: () =>
          Effect.suspend(() => {
            active.delete(sessionID)
            remember(sessionID, "timed-out")
            return Effect.fail(new RunBarrierTimeout({ sessionID, timeoutMs: RUN_BARRIER_TIMEOUT_MS }))
          }),
      }),
      Effect.catchTag("RunBarrier.Cancelled", () =>
        Effect.sync(() => {
          active.delete(sessionID)
          remember(sessionID, "cancelled")
          return { sessionID, status: "cancelled" as const }
        }),
      ),
      Effect.catchTag("RunBarrier.Report", (error) =>
        Effect.suspend(() => {
          active.delete(sessionID)
          failures.set(sessionID, error)
          remember(sessionID, "failed")
          return Effect.fail(error)
        }),
      ),
      Effect.onInterrupt(() =>
        Effect.sync(() => {
          active.delete(sessionID)
          if (stateOf(sessionID) === "draining") remember(sessionID, "idle")
        }),
      ),
    )

    active.delete(sessionID)
    if (stateOf(sessionID) !== "cancelled") remember(sessionID, "drained")
    return outcome
  })

  return {
    beginRun,
    wait,
    cancel: (sessionID) =>
      Effect.sync(() => {
        const record = active.get(sessionID)
        remember(sessionID, "cancelled")
        if (record !== undefined) {
          Deferred.doneUnsafe(record.done, Effect.fail(new RunBarrierCancelled({ sessionID })))
        }
      }),
    state: stateOf,
    reset: Effect.fn("RunBarrier.reset")(function* (sessionID: string) {
      if (active.has(sessionID)) return yield* new RunBarrierBusyError({ sessionID })
      states.delete(sessionID)
      failures.delete(sessionID)
      runs.delete(sessionID)
    }),
    abandon: Effect.fn("RunBarrier.abandon")(function* (sessionID: string) {
      if (sessionID.length === 0) {
        return yield* reportError(sessionID, "run barrier requires a non-empty sessionID")
      }
      // A live drain still owns the phases; abandoning would let the next run overtake in-flight work.
      if (active.has(sessionID)) return yield* new RunBarrierBusyError({ sessionID })
      states.delete(sessionID)
      failures.delete(sessionID)
      runs.delete(sessionID)
    }),
    get size() {
      return states.size
    },
  }
}

/** Build the service. `layer` and the synchronous hub compatibility path use it. */
export const make = Effect.sync(() => makeBarrier())

/** Install the real barrier service; all state lives in this layer's closure. */
export const layer = Layer.effect(Service, make)

/** The unbound graph node for {@link Service}. */
export const node = LayerNode.make({ service: Service, layer, deps: [] })

export * as RunBarrier from "./run-barrier"
