import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import type { DeepMutable } from "@opencode-ai/core/schema"
import { InstanceState } from "@/effect/instance-state"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Runner } from "@/effect/runner"
import { BackgroundJob } from "@/background/job"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Effect, Latch, Layer, Option, Schema, Scope, Context } from "effect"
import { Session } from "./session"
import { SessionID } from "./schema"
import type { DispatchOrigin } from "./origin"
import { SessionStatus } from "./status"
import { SessionHubService, type SessionHub } from "./worker/hub"
import { SessionBusyError, type SessionExecutionResult } from "./worker/execute"
import { SessionCancelledError, WorkerCrashedError } from "./worker/dispatch"
import type { CancelOutcome } from "./worker/cancel"
import type { ShellInput } from "./prompt"

/**
 * A hub rejection that is neither a cancel nor a crash. It is still a typed
 * failure (never a defect), so a broken control channel can be handled by the
 * caller instead of tearing down the fiber's cause.
 */
export class SessionRunFailureError extends Schema.TaggedErrorClass<SessionRunFailureError>()(
  "SessionRunFailureError",
  {
    message: Schema.String,
    cause: Schema.Defect(),
  },
) {}

/**
 * Every typed way a hub-hosted run can fail: a cancel, a worker crash, or an
 * unexpected control-channel failure. None of these are defects.
 */
export type RunError = SessionCancelledError | WorkerCrashedError | SessionRunFailureError

export interface Interface {
  readonly assertNotBusy: (sessionID: SessionID) => Effect.Effect<void, Session.BusyError>
  readonly cancel: (sessionID: SessionID) => Effect.Effect<CancelOutcome | undefined>
  readonly ensureRunning: (
    sessionID: SessionID,
    onInterrupt: Effect.Effect<SessionV1.WithParts>,
    work: Effect.Effect<SessionV1.WithParts>,
    origin?: DispatchOrigin,
  ) => Effect.Effect<SessionV1.WithParts, RunError>
  readonly startShell: (
    sessionID: SessionID,
    onInterrupt: Effect.Effect<SessionV1.WithParts>,
    work: Effect.Effect<SessionV1.WithParts>,
    ready?: Latch.Latch,
    shell?: ShellInput,
  ) => Effect.Effect<SessionV1.WithParts, Session.BusyError | RunError>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionRunState") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const background = yield* BackgroundJob.Service
    const status = yield* SessionStatus.Service

    // Flag-on seam: when `experimentalSessionWorkers` is on AND the hub
    // pool layer is present, the run-state machine lives in a worker isolate and
    // every method delegates to the hub (`session/worker/hub.ts`). Both services
    // are resolved from the CALLER's context (not the layer's declared deps), so
    // the flag-off layer graph is byte-identical to the pre-Phase-12 hub path.
    // `Effect.cached` resolves the pair ONCE per layer instance: the graph is
    // fixed for a runtime, so re-reading it on every call is pure waste.
    const hubIfEnabled: Effect.Effect<SessionHub | undefined> = yield* Effect.cached(
      Effect.gen(function* () {
        const hub = yield* Effect.serviceOption(SessionHubService)
        if (Option.isNone(hub)) return undefined
        const flags = yield* Effect.serviceOption(RuntimeFlags.Service)
        if (Option.isNone(flags) || !flags.value.experimentalSessionWorkers) return undefined
        return hub.value
      }),
    )

    // The run-state machine is the portable `Runner` table shared with the
    // session worker (`session/worker/execute.ts`): flag off it lives here in
    // the hub, flag on it lives inside the worker isolate.
    const state = yield* InstanceState.make(
      Effect.fn("SessionRunState.state")(function* () {
        const scope = yield* Scope.Scope
        const runners = Runner.createRunnerTable<SessionID, SessionV1.WithParts>({
          scope,
          onIdle: (sessionID) => status.set(sessionID, { type: "idle" }),
          onBusy: (sessionID) => status.set(sessionID, { type: "busy" }),
        })
        yield* Effect.addFinalizer(() => runners.cancelAll)
        return runners
      }),
    )

    const assertNotBusy = Effect.fn("SessionRunState.assertNotBusy")(function* (sessionID: SessionID) {
      const hub = yield* hubIfEnabled
      if (hub !== undefined) {
        // Fail closed: only the hub can prove a worker dead, and it reports that
        // as `false`. A probe that throws is not proof of idleness, so the
        // caller (SessionRevert) must not proceed; surface it as the domain
        // busy error instead of swallowing it.
        const busy = yield* Effect.tryPromise({
          try: () => hub.assertNotBusy(sessionID),
          catch: () => busyError(sessionID),
        })
        if (busy) yield* busyError(sessionID)
        return
      }
      const runners = yield* InstanceState.get(state)
      if (runners.busy(sessionID)) yield* busyError(sessionID)
    })

    const cancel = Effect.fn("SessionRunState.cancel")(function* (sessionID: SessionID) {
      const hub = yield* hubIfEnabled
      if (hub !== undefined) {
        // The hub cancels the session; `SessionPrompt.cancel` has already walked
        // `Session.children` children-first, so this call only needs the
        // session's own outcome and fallback idle. A hub that fails to REPORT
        // (e.g. its worker crashed while settling) must still yield a typed
        // terminal outcome: cancellation already happened, only the report was
        // lost.
        const report = yield* Effect.tryPromise({
          try: () => hub.cancelWithReport(sessionID),
          catch: asRunError,
        }).pipe(
          Effect.catch(() =>
            Effect.succeed({
              outcome: unreportedCancelOutcome(sessionID),
              descendants: [],
            }),
          ),
        )
        yield* cancelBackgroundJobs(background, sessionID)
        // Single-idle authority, extended to the subagent cascade. The worker
        // publishes its own terminal idle only for a cooperative settle or a
        // natural completion; a queued or grace-terminated child never does, so
        // run-state writes the fallback for exactly those descendants.
        const handled = new Set<SessionID>()
        for (const descendant of report.descendants) {
          handled.add(descendant.sessionID)
          if (descendant.needsFallbackIdle) yield* status.set(descendant.sessionID, { type: "idle" })
        }
        if (!workerPublishedIdle(report.outcome)) yield* status.set(sessionID, { type: "idle" })
        // Children the hub registry did not link (a subagent job that never
        // registered) are still swept: each is cancelled through the hub and its
        // own outcome decides its single idle.
        const session = yield* Effect.serviceOption(Session.Service)
        if (Option.isSome(session)) {
          const children = yield* session.value.children(sessionID)
          for (const child of children) {
            if (handled.has(child.id)) continue
            // A child cancel that fails to report is an unreported cancel too:
            // the descendant still gets its fallback idle, never a defect.
            const childOutcome = yield* Effect.tryPromise({
              try: () => hub.cancel(child.id),
              catch: asRunError,
            }).pipe(Effect.catch(() => Effect.succeed(undefined)))
            if (!workerPublishedIdle(childOutcome)) yield* status.set(child.id, { type: "idle" })
          }
        }
        return report.outcome
      }
      yield* cancelBackgroundJobs(background, sessionID)
      const runners = yield* InstanceState.get(state)
      if (!runners.busy(sessionID)) {
        yield* status.set(sessionID, { type: "idle" })
        return completedOutcome(sessionID)
      }
      const started = Date.now()
      yield* runners.cancel(sessionID)
      return cooperativeOutcome(sessionID, Date.now() - started)
    })

    const ensureRunning = Effect.fn("SessionRunState.ensureRunning")(function* (
      sessionID: SessionID,
      onInterrupt: Effect.Effect<SessionV1.WithParts>,
      work: Effect.Effect<SessionV1.WithParts>,
      origin?: DispatchOrigin,
    ) {
      const hub = yield* hubIfEnabled
      if (hub !== undefined) {
        // A hub-hosted run reports cancel/crash/control failures through its
        // promise rejection. Map them into the typed `RunError` channel so the
        // caller can branch on them; never let a rejection become a defect.
        // A clean cancel is the exception: the caller's `onInterrupt` is the run's
        // intended result (flag-off parity), not a failure, so a cancelled prompt
        // handler returns the interrupted message instead of `BadRequest`.
        const run = Effect.tryPromise({
          try: () => hub.ensureRunning(sessionID, origin),
          catch: asRunError,
        })
        return yield* settleHubRun(interruptibleHubRun(run, hub, sessionID, onInterrupt), onInterrupt)
      }
      const runners = yield* InstanceState.get(state)
      return yield* runners.ensureRunning(sessionID, work, onInterrupt)
    })

    const startShell = Effect.fn("SessionRunState.startShell")(function* (
      sessionID: SessionID,
      onInterrupt: Effect.Effect<SessionV1.WithParts>,
      work: Effect.Effect<SessionV1.WithParts>,
      ready?: Latch.Latch,
      shell?: ShellInput,
    ) {
      const hub = yield* hubIfEnabled
      if (hub !== undefined) {
        // The hub protocol carries the session id and the shell command: the
        // worker resolves the work by kind and owns the interrupt, so the
        // caller's `work`/`onInterrupt` closures cannot cross the isolate
        // (exactly as `ensureRunning` does). The `ready` latch has no hub-side
        // equivalent, so open it here rather than leave a caller waiting on a
        // signal the hub will never send.
        if (ready !== undefined) yield* ready.open
        // `hub.startShell` performs its own busy assertion and rejects with a
        // worker-local `SessionBusyError`; map it onto the domain `Session.BusyError`.
        const run = Effect.tryPromise({
          try: () => hub.startShell(sessionID, shell),
          // A worker-local `SessionBusyError` maps onto the domain
          // `Session.BusyError`; every other failure is a typed run error.
          catch: (error) => (error instanceof SessionBusyError ? busyError(sessionID) : asRunError(error)),
        })
        return yield* settleHubRun(interruptibleHubRun(run, hub, sessionID, onInterrupt), onInterrupt)
      }
      const runners = yield* InstanceState.get(state)
      return yield* runners
        .startShell(sessionID, work, ready, onInterrupt)
        .pipe(Effect.catchTag("RunnerBusy", () => Effect.fail(busyError(sessionID))))
    })

    return Service.of({ assertNotBusy, cancel, ensureRunning, startShell })
  }),
)

const cancelBackgroundJobs = Effect.fn("SessionRunState.cancelBackgroundJobs")(function* (
  background: BackgroundJob.Interface,
  sessionID: SessionID,
) {
  // Bounded, transitive cancel: a matched job joins the pending set, so its own
  // children (matched by `sessionId`/`parentSessionId`) are cancelled next round.
  const pending = new Set<string>([sessionID])
  const cancelled = new Set<string>()

  const jobs = yield* background.list()
  const matches = (job: BackgroundJob.Info) => {
    if (job.status !== "running") return false
    if (cancelled.has(job.id)) return false
    if (pending.has(job.id)) return true
    if (typeof job.metadata?.sessionId === "string" && pending.has(job.metadata.sessionId)) return true
    return typeof job.metadata?.parentSessionId === "string" && pending.has(job.metadata.parentSessionId)
  }
  let batch = jobs.filter(matches)
  while (batch.length > 0) {
    yield* Effect.forEach(
      batch,
      (job) =>
        background.cancel(job.id).pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              cancelled.add(job.id)
              pending.add(job.id)
              if (typeof job.metadata?.sessionId === "string") pending.add(job.metadata.sessionId)
            }),
          ),
        ),
      { concurrency: "unbounded", discard: true },
    )
    batch = jobs.filter(matches)
  }

})

function busyError(sessionID: SessionID) {
  return new Session.BusyError({ sessionID })
}

/** Recognise the hub's typed rejections; everything else is a control failure. */
function asRunError(error: unknown): RunError {
  if (error instanceof SessionCancelledError || error instanceof WorkerCrashedError) return error
  return new SessionRunFailureError({
    message: error instanceof Error ? error.message : String(error),
    cause: error,
  })
}

/**
 * The typed outcome of a cancel whose report was lost. The cancellation itself
 * is terminal (the hub either retired the worker or never had a run), so report
 * the conservative terminate outcome; the caller still writes the fallback idle.
 */
function unreportedCancelOutcome(sessionID: SessionID): CancelOutcome {
  return { sessionID, mode: "terminated", winner: "cancel", acknowledged: false, durationMs: 0 }
}

/**
 * Whether a cancel outcome's worker already published the session's terminal
 * idle through the hub-proxy (table B): a cooperative settle publishes before it
 * acks, and a natural completion publishes on `finishRun`. A queued session, an
 * idle no-op, or a grace-expired terminate never does — the caller must write
 * the fallback idle exactly once.
 */
function workerPublishedIdle(outcome: CancelOutcome | undefined): boolean {
  return outcome !== undefined && (outcome.acknowledged || outcome.winner === "completion")
}

/**
 * The flag-off idle outcome: no run was in flight, so the run had already
 * settled and completion wins. There is no worker protocol to acknowledge, so
 * `acknowledged` is `false`: only a real worker `cancel-ack` may set it true.
 */
function completedOutcome(sessionID: SessionID): CancelOutcome {
  return { sessionID, mode: "completed", winner: "completion", acknowledged: false, durationMs: 0 }
}

/**
 * The flag-off busy outcome: the in-process runner unwound at its next
 * checkpoint. There is no worker ack in the flag-off path, so `acknowledged`
 * is `false`.
 */
function cooperativeOutcome(sessionID: SessionID, durationMs: number): CancelOutcome {
  return { sessionID, mode: "cooperative", winner: "cancel", acknowledged: false, durationMs }
}

/**
 * A caller-side interruption (HTTP abort, TUI cancel) must reach the hub: the
 * promise rejection alone cannot stop the worker's run. On interrupt ask the hub
 * to cancel the session (children-first report) and then run the caller's own
 * interrupt finalizer, so the assistant message is persisted exactly as the
 * flag-off runner does.
 */
function interruptibleHubRun<A, E>(
  run: Effect.Effect<A, E>,
  hub: SessionHub,
  sessionID: SessionID,
  onInterrupt: Effect.Effect<SessionV1.WithParts>,
): Effect.Effect<A, E> {
  return Effect.onInterrupt(run, () =>
    Effect.gen(function* () {
      yield* Effect.promise(() => hub.cancelWithReport(sessionID).catch(() => undefined))
      yield* onInterrupt
    }).pipe(Effect.asVoid),
  )
}

/**
 * Fold a hub-hosted run's outcome: a clean cancel yields the caller's
 * `onInterrupt` result (the interrupted assistant message), exactly as the
 * flag-off runner does, while every other typed failure still propagates.
 */
function settleHubRun(
  run: Effect.Effect<SessionExecutionResult, RunError>,
  onInterrupt: Effect.Effect<SessionV1.WithParts>,
): Effect.Effect<SessionV1.WithParts, Exclude<RunError, SessionCancelledError>>
function settleHubRun(
  run: Effect.Effect<SessionExecutionResult, RunError | Session.BusyError>,
  onInterrupt: Effect.Effect<SessionV1.WithParts>,
): Effect.Effect<SessionV1.WithParts, Exclude<RunError, SessionCancelledError> | Session.BusyError>
function settleHubRun(
  run: Effect.Effect<SessionExecutionResult, RunError | Session.BusyError>,
  onInterrupt: Effect.Effect<SessionV1.WithParts>,
): Effect.Effect<SessionV1.WithParts, Exclude<RunError, SessionCancelledError> | Session.BusyError> {
  return Effect.gen(function* () {
    const result = yield* run.pipe(
      Effect.map(Option.some),
      Effect.catchTag("SessionCancelledError", () => Effect.succeed(Option.none<SessionExecutionResult>())),
    )
    if (Option.isNone(result)) return yield* onInterrupt
    return yield* parseSessionResult(result.value)
  })
}

/** Parse a worker's JSON result into the trusted `WithParts` at the hub boundary. */
function parseSessionResult(result: SessionExecutionResult): Effect.Effect<SessionV1.WithParts> {
  // The schema decodes to a deeply `readonly` tree while the runner contract is
  // the mutable `WithParts` alias, so the boundary widens via `DeepMutable`
  // (the repo's parse idiom, cf. `config/parse.ts`). A malformed worker result
  // is a defect (a broken isolate), not a recoverable typed failure: `orDie`.
  return Schema.decodeUnknownEffect(SessionV1.WithParts)(result.value).pipe(
    Effect.orDie,
    Effect.map((decoded) => decoded as DeepMutable<typeof decoded>),
  )
}

export const node = LayerNode.make({ service: Service, layer: layer, deps: [BackgroundJob.node, SessionStatus.node] })

export * as SessionRunState from "./run-state"
