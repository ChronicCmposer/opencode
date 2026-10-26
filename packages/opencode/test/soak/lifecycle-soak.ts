// Lifecycle soak for the per-session worker scheduler (fake isolates).
//
// This exercises admission, per-session worker materialization, nested
// `task`-shaped child sessions and idle reap through the real hub/dispatcher and
// the in-memory executor pool. It deliberately uses the existing
// `FakeExecutorWorker` and a mock-free graph: no provider or model is called.
// The goal is lifecycle/scheduling confidence (unbounded admission, one worker
// per session, reap back to baseline), not a model/CPU/network soak.
//
// Determinism rules:
//   * admission is synchronous at the dispatcher boundary, then confirmed with
//     `pollUntil` (a published state signal, not a fixed sleep);
//   * workers are settled explicitly with `cancel` (the fake's cooperative
//     settle), then awaited through their dispatch promises;
//   * the only wall-clock wait is the real reap window configured through
//     `OPENCODE_SESSION_WORKER_REAP_MS`; completion is observed through the
//     `onSessionReleased` promise signal.

import { createExecutorWorkerPool, type ExecutorRun, type SessionExecutionResult } from "@/session/worker/execute"
import { createSessionHub, type SessionHub } from "@/session/worker/hub"
import { type Scheduler } from "@/session/worker/dispatch"
import { REAP_MS_ENV, resolveReapMs } from "@/session/worker/pool"
import { defaultSchedule } from "@/session/worker/port"
import type { SessionID } from "@/session/schema"
import { FakeExecutorWorker } from "../lib/fake-worker-pool"
import { createLeakDetector, type TrackedPort } from "../lib/leak-detector"
import { pollUntil } from "../lib/poll"
import { settleOf, sid } from "../lib/async"

/** Concurrent roots. */
export const ROOT_SESSIONS = 50
/** Direct `task` children per root (depth 1). */
export const CHILDREN_PER_ROOT = 2
/** Roots that also get one nested `task` child (depth 2). */
export const NESTED_EVERY_NTH_ROOT = 5
/** Idle reap window; small enough for a test, large enough to be a real timer. */
export const SOAK_REAP_MS = 1_500
/** Fail-fast guard around the reap promise signal. */
const REAP_TIMEOUT_MS = 15_000

export interface LifecycleSoakMetrics {
  readonly roots: number
  readonly children: number
  readonly grandchildren: number
  readonly sessions: number
  /** Worker runs observed per depth ({ "0": roots, "1": children, ... }). */
  readonly depthCounts: Readonly<Record<string, number>>
  readonly peakWorkers: number
  readonly queuedAtPeak: number
  readonly settledSessions: number
  readonly failedSessions: number
  readonly releasedSessions: number
  readonly postReapWorkers: number
  readonly postReapRegistrySize: number
  readonly outstandingTimers: number
  readonly leakedWorkers: ReadonlyArray<number>
  readonly leakedPorts: ReadonlyArray<string>
  readonly rssBeforeBytes: number
  readonly rssPeakBytes: number
  readonly rssAfterReapBytes: number
  readonly reapMs: number
  readonly admissionMs: number
  readonly settleMs: number
  readonly reapWaitMs: number
  readonly totalMs: number
}

/**
 * The fake worker plus one real `MessageChannel`. The channel stands in for the
 * per-session control link and is tracked by the leak detector, so the soak
 * asserts the same close-on-retire lifecycle the real transport relies on.
 */
class SoakWorker extends FakeExecutorWorker {
  readonly runs: ExecutorRun[] = []
  private live = true
  private closeControl: (() => void) | undefined

  constructor(id: number) {
    super(id, { mode: "blocking" })
  }

  override get alive(): boolean {
    return this.live
  }

  /** Record every run so the scheduler's depth convention is observable. */
  override execute(run: ExecutorRun): Promise<void> {
    this.runs.push(run)
    return super.execute(run)
  }

  /** Close the tracked control link when the dispatcher retires this worker. */
  onTerminate(close: () => void): void {
    this.closeControl = close
  }

  override async terminate(): Promise<void> {
    if (!this.live) return
    this.live = false
    this.closeControl?.()
    this.closeControl = undefined
    await super.terminate()
  }
}

/** A scheduler wrapper that tracks outstanding timers without changing timing. */
function createTrackedSchedule(): { readonly schedule: Scheduler; readonly outstanding: () => number } {
  const pending = new Set<object>()
  const schedule: Scheduler = (delayMs, run) => {
    const token = {}
    pending.add(token)
    const cancel = defaultSchedule(delayMs, () => {
      // A cancelled timer never runs; a fired timer is no longer outstanding.
      if (!pending.delete(token)) return
      run()
    })
    return () => {
      if (!pending.delete(token)) return
      cancel()
    }
  }
  return { schedule, outstanding: () => pending.size }
}

/** Resolve `signal` or reject with `message()` after `timeoutMs`. */
function withTimeout(signal: Promise<void>, timeoutMs: number, message: () => string): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(message())), timeoutMs)
    signal.then(
      () => {
        clearTimeout(timer)
        resolve()
      },
      (error: unknown) => {
        clearTimeout(timer)
        reject(error)
      },
    )
  })
}

/** Build the task-shaped session graph: roots, direct children, nested children. */
function sessionGraph(): {
  readonly roots: ReadonlyArray<SessionID>
  readonly children: ReadonlyArray<SessionID>
  readonly grandchildren: ReadonlyArray<SessionID>
} {
  const roots = Array.from({ length: ROOT_SESSIONS }, (_, index) => sid(`ses_soak_root_${index}`))
  const children = roots.flatMap((_, rootIndex) =>
    Array.from({ length: CHILDREN_PER_ROOT }, (_, childIndex) => sid(`ses_soak_root_${rootIndex}_child_${childIndex}`)),
  )
  const grandchildren = roots.flatMap((_, rootIndex) =>
    rootIndex % NESTED_EVERY_NTH_ROOT === 0 ? [sid(`ses_soak_root_${rootIndex}_child_0_task`)] : [],
  )
  return { roots, children, grandchildren }
}

/** Create the hub with the reap env applied only for the synchronous construction. */
function createHub(options: {
  readonly pool: ReturnType<typeof createExecutorWorkerPool>
  readonly schedule: Scheduler
  readonly onSessionReleased: (sessionID: SessionID) => void
}): SessionHub {
  const previous = process.env[REAP_MS_ENV]
  process.env[REAP_MS_ENV] = String(SOAK_REAP_MS)
  try {
    if (resolveReapMs() !== SOAK_REAP_MS) {
      throw new Error(`${REAP_MS_ENV} did not resolve to ${SOAK_REAP_MS}`)
    }
    return createSessionHub(options)
  } finally {
    if (previous === undefined) delete process.env[REAP_MS_ENV]
    else process.env[REAP_MS_ENV] = previous
  }
}

/**
 * Run the soak once and return its metrics. Any lifecycle failure (admission
 * timeout, unexpected rejection, reap timeout, leak) throws descriptively so the
 * test never passes on a partial observation.
 */
export async function runLifecycleSoak(): Promise<LifecycleSoakMetrics> {
  const startedAt = performance.now()
  const graph = sessionGraph()
  const sessionIDs = [...graph.roots, ...graph.children, ...graph.grandchildren]
  const total = sessionIDs.length

  const leaks = createLeakDetector()
  const created: SoakWorker[] = []
  const pool = createExecutorWorkerPool({
    spawn: (id) => {
      const control = new MessageChannel()
      const worker = new SoakWorker(id)
      const port: TrackedPort = leaks.trackPort(control.port1, `soak-worker-${id}-control`)
      worker.onTerminate(() => {
        port.close()
        control.port2.close()
      })
      leaks.trackWorker(worker)
      created.push(worker)
      return worker
    },
  })

  const released = new Set<SessionID>()
  let markAllReleased!: () => void
  const allReleased = new Promise<void>((resolve) => {
    markAllReleased = resolve
  })
  const schedule = createTrackedSchedule()

  let hub: SessionHub | undefined
  const rssBeforeBytes = process.memoryUsage().rss
  let rssPeakBytes = rssBeforeBytes
  const sampleRss = (): void => {
    rssPeakBytes = Math.max(rssPeakBytes, process.memoryUsage().rss)
  }

  try {
    const sessionHub = createHub({
      pool,
      schedule: schedule.schedule,
      onSessionReleased: (sessionID) => {
        released.add(sessionID)
        if (released.size === total) markAllReleased()
      },
    })
    hub = sessionHub

    // Admit every session. A root goes through the hub's ensureRunning surface;
    // a child is dispatched at parent+1 (depth 2 for the nested task child),
    // exactly the depth convention `Session.spawn` stamps.
    const admissionStartedAt = performance.now()
    const dispatches: Promise<SessionExecutionResult | void>[] = []
    for (const root of graph.roots) {
      dispatches.push(sessionHub.ensureRunning(root))
      sampleRss()
    }
    for (const child of graph.children) {
      dispatches.push(sessionHub.dispatcher.dispatch({ sessionID: child, depth: 1 }))
      sampleRss()
    }
    for (const grandchild of graph.grandchildren) {
      dispatches.push(sessionHub.dispatcher.dispatch({ sessionID: grandchild, depth: 2 }))
      sampleRss()
    }

    await pollUntil(() => sessionHub.dispatcher.busySlotCount === total, {
      timeoutMs: 5_000,
      message: `only ${sessionHub.dispatcher.busySlotCount}/${total} sessions were admitted`,
    })
    const admissionMs = performance.now() - admissionStartedAt
    const peakWorkers = created.filter((worker) => worker.alive).length
    const queuedAtPeak = sessionHub.dispatcher.queuedCount
    sampleRss()

    if (queuedAtPeak !== 0) throw new Error(`queued ${queuedAtPeak} sessions at peak admission`)
    if (peakWorkers !== total) throw new Error(`peak live workers ${peakWorkers}, expected ${total}`)

    const depthBySession = new Map<SessionID, number>()
    for (const worker of created) {
      for (const run of worker.runs) depthBySession.set(run.sessionID, run.depth)
    }
    const depthCounts: Record<string, number> = {}
    for (const depth of depthBySession.values()) {
      const key = String(depth)
      depthCounts[key] = (depthCounts[key] ?? 0) + 1
    }

    // Settle each fake run cooperatively; the fake records a result so the hub's
    // ensureRunning surface also completes.
    const settleStartedAt = performance.now()
    for (const sessionID of sessionIDs) {
      const workerID = sessionHub.dispatcher.workerForSession(sessionID)
      if (workerID === undefined) continue
      await pool.byID(workerID)?.cancel(sessionID)
    }
    const outcomes = await Promise.all(dispatches.map((dispatch) => settleOf(dispatch)))
    const failures = outcomes.filter((outcome) => !outcome.ok)
    const settleMs = performance.now() - settleStartedAt

    if (failures.length > 0) {
      const messages = failures.map((outcome) => {
        if (outcome.ok) return ""
        return outcome.error instanceof Error ? outcome.error.message : String(outcome.error)
      })
      throw new Error(`${failures.length}/${total} dispatches rejected: ${messages.join("; ")}`)
    }
    if (sessionHub.dispatcher.busySlotCount !== 0) {
      throw new Error(`${sessionHub.dispatcher.busySlotCount} sessions still busy after settle`)
    }

    // The only wall-clock wait: the real reap window. Completion arrives through
    // the release callback, never a sleep.
    const reapStartedAt = performance.now()
    await withTimeout(allReleased, REAP_TIMEOUT_MS, () => `only ${released.size}/${total} sessions were reaped`)
    const reapWaitMs = performance.now() - reapStartedAt
    sampleRss()

    const leak = leaks.detect()
    const metrics: LifecycleSoakMetrics = {
      roots: graph.roots.length,
      children: graph.children.length,
      grandchildren: graph.grandchildren.length,
      sessions: total,
      depthCounts,
      peakWorkers,
      queuedAtPeak,
      settledSessions: outcomes.length,
      failedSessions: failures.length,
      releasedSessions: released.size,
      postReapWorkers: pool.workers.length,
      postReapRegistrySize: sessionHub.dispatcher.registry.size,
      outstandingTimers: schedule.outstanding(),
      leakedWorkers: leak.workers,
      leakedPorts: leak.ports,
      rssBeforeBytes,
      rssPeakBytes,
      rssAfterReapBytes: process.memoryUsage().rss,
      reapMs: SOAK_REAP_MS,
      admissionMs,
      settleMs,
      reapWaitMs,
      totalMs: performance.now() - startedAt,
    }

    if (metrics.postReapWorkers !== 0) throw new Error(`pool retained ${metrics.postReapWorkers} workers after reap`)
    if (metrics.postReapRegistrySize !== 0) {
      throw new Error(`registry retained ${metrics.postReapRegistrySize} sessions after reap`)
    }
    if (metrics.releasedSessions !== total) throw new Error(`released ${metrics.releasedSessions}/${total} sessions`)
    if (metrics.outstandingTimers !== 0) throw new Error(`${metrics.outstandingTimers} reap timers still armed`)
    if (!leak.clean) throw new Error(`leak report not clean: ${JSON.stringify(leak.leaks)}`)

    return metrics
  } finally {
    await hub?.terminateAll().catch(() => undefined)
  }
}
