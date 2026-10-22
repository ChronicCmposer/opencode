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
import { SessionStatus } from "./status"
import { SessionHubService, type SessionHub } from "./worker/hub"
import { SessionBusyError, type SessionExecutionResult } from "./worker/execute"
import type { CancelOutcome } from "./worker/cancel"

export interface Interface {
  readonly assertNotBusy: (sessionID: SessionID) => Effect.Effect<void, Session.BusyError>
  readonly cancel: (sessionID: SessionID) => Effect.Effect<CancelOutcome | undefined>
  readonly ensureRunning: (
    sessionID: SessionID,
    onInterrupt: Effect.Effect<SessionV1.WithParts>,
    work: Effect.Effect<SessionV1.WithParts>,
  ) => Effect.Effect<SessionV1.WithParts>
  readonly startShell: (
    sessionID: SessionID,
    onInterrupt: Effect.Effect<SessionV1.WithParts>,
    work: Effect.Effect<SessionV1.WithParts>,
    ready?: Latch.Latch,
  ) => Effect.Effect<SessionV1.WithParts, Session.BusyError>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionRunState") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const background = yield* BackgroundJob.Service
    const status = yield* SessionStatus.Service

    // Phase 12 flag-on seam: when `experimentalSessionWorkers` is on AND the hub
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
    // the hub, flag on it lives inside the worker isolate (plan decision 18).
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
        if (yield* Effect.promise(() => hub.assertNotBusy(sessionID))) yield* busyError(sessionID)
        return
      }
      const runners = yield* InstanceState.get(state)
      if (runners.busy(sessionID)) yield* busyError(sessionID)
    })

    const cancel = Effect.fn("SessionRunState.cancel")(function* (sessionID: SessionID) {
      const hub = yield* hubIfEnabled
      if (hub !== undefined) {
        const outcome = yield* Effect.promise(() => hub.cancel(sessionID))
        // Phase 6.1 parity: background subagents/jobs must not outlive the cancel.
        // The flag-off branch cancels them before it settles; mirror that here
        // after the hub cancel resolves (so the outcome is known).
        yield* cancelBackgroundJobs(background, sessionID)
        // Single-idle authority. The run-state machine lives in the worker
        // isolate, and its runner's `onIdle` publishes the terminal idle through
        // the hub-proxy (`session/worker/run-state` table B). That worker emit is
        // authoritative whenever the run reached its own run-state, which the hub
        // reports back exactly two ways: the worker acknowledged the cancel
        // (`acknowledged`, it publishes idle before acking) or the run completed
        // naturally (`winner` "completion", it publishes idle on `finishRun`).
        // Only when neither happened — a queued session, an idle no-op, or a
        // grace-expired terminate that killed the isolate — is no worker
        // publishing, so this branch emits the fallback idle itself. That keeps
        // exactly one idle for every cancel, matching the flag-off branch below.
        const workerPublishedIdle = outcome !== undefined && (outcome.acknowledged || outcome.winner === "completion")
        if (!workerPublishedIdle) yield* status.set(sessionID, { type: "idle" })
        return outcome
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
    ) {
      const hub = yield* hubIfEnabled
      if (hub !== undefined) {
        return yield* Effect.promise(() => hub.ensureRunning(sessionID)).pipe(Effect.flatMap(parseSessionResult))
      }
      const runners = yield* InstanceState.get(state)
      return yield* runners.ensureRunning(sessionID, work, onInterrupt)
    })

    const startShell = Effect.fn("SessionRunState.startShell")(function* (
      sessionID: SessionID,
      onInterrupt: Effect.Effect<SessionV1.WithParts>,
      work: Effect.Effect<SessionV1.WithParts>,
      ready?: Latch.Latch,
    ) {
      const hub = yield* hubIfEnabled
      if (hub !== undefined) {
        // The hub protocol carries only the session id: the worker resolves the
        // work by kind and owns the interrupt, so the caller's `work`/`onInterrupt`
        // closures cannot cross the isolate (exactly as `ensureRunning` does). The
        // `ready` latch has no hub-side equivalent, so open it here rather than
        // leave a caller waiting on a signal the hub will never send.
        if (ready !== undefined) yield* ready.open
        // `hub.startShell` performs its own busy assertion and rejects with a
        // worker-local `SessionBusyError`; map it onto the domain `Session.BusyError`.
        return yield* Effect.tryPromise({
          try: () => hub.startShell(sessionID),
          catch: (error) => error,
        }).pipe(
          Effect.flatMap(parseSessionResult),
          Effect.catch((error) =>
            error instanceof SessionBusyError ? Effect.fail(busyError(sessionID)) : Effect.die(error),
          ),
        )
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
  const jobs = yield* background.list()
  const pending = new Set<string>([sessionID])
  const cancelled = new Set<string>()
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
