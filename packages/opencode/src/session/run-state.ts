import { LayerNode } from "@opencode-ai/core/effect/layer-node"
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
import type { SessionExecutionResult } from "./worker/execute"

export interface Interface {
  readonly assertNotBusy: (sessionID: SessionID) => Effect.Effect<void, Session.BusyError>
  readonly cancel: (sessionID: SessionID) => Effect.Effect<void>
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
    const hubIfEnabled: Effect.Effect<SessionHub | undefined> = Effect.gen(function* () {
      const hub = yield* Effect.serviceOption(SessionHubService)
      if (Option.isNone(hub)) return undefined
      const flags = yield* Effect.serviceOption(RuntimeFlags.Service)
      if (Option.isNone(flags) || !flags.value.experimentalSessionWorkers) return undefined
      return hub.value
    })

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
        yield* Effect.promise(() => hub.cancel(sessionID))
        return
      }
      yield* cancelBackgroundJobs(background, sessionID)
      const runners = yield* InstanceState.get(state)
      if (!runners.busy(sessionID)) {
        yield* status.set(sessionID, { type: "idle" })
        return
      }
      yield* runners.cancel(sessionID)
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
        if (yield* Effect.promise(() => hub.assertNotBusy(sessionID))) yield* busyError(sessionID)
        return yield* Effect.promise(() => hub.ensureRunning(sessionID)).pipe(Effect.flatMap(parseSessionResult))
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

/** Parse a worker's JSON result into the trusted `WithParts` at the hub boundary. */
function parseSessionResult(result: SessionExecutionResult): Effect.Effect<SessionV1.WithParts> {
  return Effect.sync(() => Schema.decodeUnknownSync(SessionV1.WithParts)(result.value) as SessionV1.WithParts)
}

export const node = LayerNode.make({ service: Service, layer: layer, deps: [BackgroundJob.node, SessionStatus.node] })

export * as SessionRunState from "./run-state"
