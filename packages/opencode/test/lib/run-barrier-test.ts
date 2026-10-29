// Helpers for integration tests that exercise the real `RunBarrier` service.
//
// Every helper delegates to the service; none reimplements its state machine.
// `runBarrierLayer` is the plain graph node. `clockedRunBarrierLayer` captures
// the test `Clock` so a hub's internal `Effect.runPromise(barrier.wait(...))`
// still observes `TestClock.adjust`. Without that capture the hub's private
// runtime would fall back to the live clock and a test could not drive the 5 s
// bound deterministically.

import { Clock, Effect, Layer } from "effect"
import * as TestClock from "effect/testing/TestClock"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"

import {
  RUN_BARRIER_TIMEOUT_MS,
  RunBarrier,
  type RunBarrierPromotionOutcome,
  type RunBarrierRun,
} from "@/session/run-barrier"

/** The real barrier graph node, for tests that drive `wait` in the test fiber. */
export const runBarrierLayer = LayerNode.compile(LayerNode.group([RunBarrier.node]))

/**
 * A fresh real barrier instance for a synchronous hub constructor
 * (`createSessionHub` requires one). A test that owns an Effect graph should
 * pass `yield* RunBarrier.Service` instead so it observes the same instance.
 */
export const createTestBarrier = (): RunBarrier.Interface => Effect.runSync(RunBarrier.make)

/**
 * The real barrier service bound to the current test `Clock`. Use this when a
 * hub owns the `wait` call: the hub runs `Effect.runPromise(barrier.wait(...))`,
 * so the clock must travel with the effect for `TestClock.adjust` to advance it.
 */
export const clockedRunBarrierLayer = Layer.effect(
  RunBarrier.Service,
  Effect.gen(function* () {
    const barrier = yield* RunBarrier.make
    const clock = yield* Clock.Clock
    return RunBarrier.Service.of({
      beginRun: (sessionID) => barrier.beginRun(sessionID),
      wait: (sessionID) => Effect.provideService(barrier.wait(sessionID), Clock.Clock, clock),
      cancel: (sessionID) => barrier.cancel(sessionID),
      state: (sessionID) => barrier.state(sessionID),
      reset: (sessionID) => barrier.reset(sessionID),
      abandon: (sessionID) => barrier.abandon(sessionID),
      get size() {
        return barrier.size
      },
    })
  }),
)

/** What a run observed, expressed through the service's report handle. */
export interface RunPhaseReports {
  /** A durable message id, or `null` for an explicit zero-commit run. */
  readonly commit?: string | null
  readonly projector?: string
  /** Real tool call ids, or `"none"` for a deliberate zero-tool run. */
  readonly tools?: readonly string[] | "none"
  readonly promotion?: RunBarrierPromotionOutcome
}

/**
 * Report every phase on a real run handle. The default is a committed,
 * zero-tool, no-job run so a caller only states what differs.
 */
export function reportRun(run: RunBarrierRun, reports: RunPhaseReports = {}): RunBarrierRun {
  const commit = reports.commit === undefined ? "msg_run_barrier_test" : reports.commit
  if (commit === null) {
    run.noCommit()
  } else {
    run.commit(commit)
    run.projector(reports.projector ?? "session-row")
  }
  const tools = reports.tools ?? "none"
  if (tools === "none") {
    run.noTools()
  } else {
    for (const callID of tools) run.tool(callID)
  }
  run.promotion(reports.promotion ?? "no-job")
  return run
}

/** Open a run, report it, and wait for the real service to admit the next one. */
export function drainRun(barrier: RunBarrier.Interface, sessionID: string, reports: RunPhaseReports = {}) {
  return Effect.suspend(() => {
    reportRun(barrier.beginRun(sessionID), reports)
    return barrier.wait(sessionID)
  })
}

/** Advance the test clock past the barrier's typed 5 s bound. */
export const advanceBarrierTimeout = () => TestClock.adjust(RUN_BARRIER_TIMEOUT_MS)
