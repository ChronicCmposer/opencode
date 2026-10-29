// Terminate-based cancel.
//
// Covers the cancel module (control-port frames, grace controller, first-writer-
// wins outcome gate) plus the additive dispatch wiring (cooperative vs terminate,
// PGID reap on terminate, cascade, idempotency, crash taxonomy). The real
// process evidence for `terminate()` interrupting a tight sync loop comes from
// the 3.10 spike; the case here is a regression guard over a real Bun isolate.

import { describe, expect, test } from "bun:test"
import path from "node:path"
import type { SessionID } from "@/session/schema"
import {
  CANCEL_GRACE_ENV,
  DEFAULT_CANCEL_GRACE_MS,
  createCancelController,
  createOutcomeGate,
  resolveCancelGraceMs,
  type CancelFrame,
  type CancelPort,
  type CancelScheduler,
} from "@/session/worker/cancel"
import { SessionCancelledError, WorkerCrashedError, type CancellationConfig, type SessionRun, type SessionWorker, type WorkerPool } from "@/session/worker/dispatch"
import { createDispatcher } from "../../lib/dispatcher"
import { createWorkerHarness } from "../../lib/worker-harness"
import { capture, flush, sid } from "../../lib/async"
import { createFakeWorkerPool, type FakeSessionWorker } from "../../lib/fake-worker-pool"
import { createListenerPair } from "../../lib/port-pair"
import { createRecordingScheduler } from "../../lib/scheduler"
import { Logger } from "effect"
import { recordingBridge } from "../../lib/recorders"

/** A recording bridge: captures every `(message, fields)` pair in call order. */
function recordingLog(): {
  readonly bridge: ReturnType<typeof recordingBridge>
  readonly entries: Array<{ message: string; fields: Record<string, unknown> }>
} {
  const entries: Array<{ message: string; fields: Record<string, unknown> }> = []
  const sink = Logger.make<unknown, void>(({ message }) => {
    const [head, fields] = Array.isArray(message) ? message : [message, undefined]
    entries.push({
      message: typeof head === "string" ? head : String(head),
      fields: typeof fields === "object" && fields !== null ? (fields as Record<string, unknown>) : {},
    })
  })
  return { bridge: recordingBridge(sink), entries }
}

/** An in-process pair of cancel ports; `hub.notify` reaches the worker side. */
function createPortPair(): { hub: CancelPort; worker: CancelPort } {
  const pair = createListenerPair<CancelFrame>()
  return {
    hub: { notify: (frame) => pair.hub.post(frame), onMessage: (handler) => pair.hub.onMessage(handler) },
    worker: { notify: (frame) => pair.worker.post(frame), onMessage: (handler) => pair.worker.onMessage(handler) },
  }
}

describe("11.3 grace: cooperative checkpoint then terminate after the grace window", () => {
  test("defaults to 500 ms and honors OPENCODE_SESSION_CANCEL_GRACE_MS", () => {
    expect(DEFAULT_CANCEL_GRACE_MS).toBe(500)
    expect(resolveCancelGraceMs({})).toBe(500)
    expect(resolveCancelGraceMs({ [CANCEL_GRACE_ENV]: "1500" })).toBe(1500)
    expect(() => resolveCancelGraceMs({ [CANCEL_GRACE_ENV]: "0" })).toThrow()
    expect(() => resolveCancelGraceMs({ [CANCEL_GRACE_ENV]: "nope" })).toThrow()
  })

  test("a run that completes within the grace is cooperative; no terminate", async () => {
    const pair = createPortPair()
    const scheduler = createRecordingScheduler<CancelScheduler>()
    let terminated = 0
    let reaped = 0
    const controller = createCancelController({
      port: pair.hub,
      terminate: () => {
        terminated++
      },
      reap: () => {
        reaped++
      },
      graceMs: 500,
      schedule: scheduler.schedule,
    })

    let finish!: () => void
    const complete = new Promise<void>((resolve) => {
      finish = resolve
    })
    const outcome = controller.cancel(sid("ses_1"), { complete })
    await flush()

    expect(controller.pending).toBe(1)
    expect(scheduler.pending()).toHaveLength(1)
    finish()
    expect(await outcome).toMatchObject({ mode: "cooperative", winner: "cancel" })
    expect(scheduler.pending()[0]).toBeUndefined()
    expect(scheduler.timers[0]?.cancelled).toBe(true)
    expect(terminated).toBe(0)
    expect(reaped).toBe(0)
    controller.dispose()
  })

  test("grace expiry terminates unconditionally and reaps mirrored PGIDs", async () => {
    const pair = createPortPair()
    const scheduler = createRecordingScheduler<CancelScheduler>()
    const terminated: number[] = []
    const reaped: number[] = []
    const controller = createCancelController({
      port: pair.hub,
      terminate: () => {
        terminated.push(1)
      },
      reap: () => {
        reaped.push(1)
      },
      graceMs: 500,
      schedule: scheduler.schedule,
    })

    const never = new Promise<void>(() => {})
    const outcome = controller.cancel(sid("ses_1"), { complete: never })
    await flush()

    const grace = scheduler.pending()[0]
    expect(grace).toBeDefined()
    grace?.run()

    expect(await outcome).toMatchObject({ mode: "terminated", winner: "cancel" })
    expect(terminated).toHaveLength(1)
    expect(reaped).toHaveLength(1)
    controller.dispose()
  })

  test("cancel is idempotent: a second request joins the first", async () => {
    const pair = createPortPair()
    const scheduler = createRecordingScheduler<CancelScheduler>()
    const cancellations: CancelFrame[] = []
    pair.worker.onMessage((frame) => cancellations.push(frame))
    const controller = createCancelController({ port: pair.hub, terminate: () => {}, schedule: scheduler.schedule })

    const never = new Promise<void>(() => {})
    const first = controller.cancel(sid("ses_1"), { complete: never })
    const second = controller.cancel(sid("ses_1"), { complete: never })
    await flush()

    expect(cancellations.filter((frame) => frame.type === "cancel")).toHaveLength(1)
    scheduler.pending()[0]?.run()
    expect(await first).toEqual(await second)
    controller.dispose()
  })
})

describe("11.9 cancel-vs-completion first-writer-wins", () => {
  test("the outcome gate admits exactly one winner", () => {
    const gate = createOutcomeGate()
    expect(gate.claim("cancel")).toBe(true)
    expect(gate.claim("completion")).toBe(false)
    expect(gate.winner).toBe("cancel")
  })

  test("the outcome gate is first-only: a repeat claim never overwrites the winner", () => {
    const gate = createOutcomeGate()
    expect(gate.claim("cancel")).toBe(true)
    expect(gate.claim("cancel")).toBe(false)
    expect(gate.claim("completion")).toBe(false)
    expect(gate.winner).toBe("cancel")
  })

  test("a completion that already landed wins; the controller sends no cancel", async () => {
    const pair = createPortPair()
    const scheduler = createRecordingScheduler<CancelScheduler>()
    const sent: CancelFrame[] = []
    pair.worker.onMessage((frame) => sent.push(frame))
    const controller = createCancelController({ port: pair.hub, terminate: () => {}, schedule: scheduler.schedule })

    const outcome = controller.cancel(sid("ses_1"), { complete: Promise.resolve() })
    expect(await outcome).toMatchObject({ mode: "completed", winner: "completion" })
    expect(sent).toHaveLength(0)
    controller.dispose()
  })

  test("an ack is scoped to its in-flight request and never leaks into a later one", async () => {
    const pair = createPortPair()
    const scheduler = createRecordingScheduler<CancelScheduler>()
    let cancels = 0
    pair.worker.onMessage((frame) => {
      if (frame.type !== "cancel") return
      cancels++
      // The worker acks only the first cancel; the second must not inherit it.
      if (cancels === 1) pair.worker.notify({ type: "cancel-ack", sessionID: frame.sessionID, accepted: true })
    })
    const controller = createCancelController({
      port: pair.hub,
      terminate: () => {},
      graceMs: 500,
      schedule: scheduler.schedule,
    })

    let finishFirst!: () => void
    const completeFirst = new Promise<void>((resolve) => {
      finishFirst = resolve
    })
    const first = controller.cancel(sid("ses_1"), { complete: completeFirst })
    await flush()
    finishFirst()
    expect(await first).toMatchObject({ mode: "cooperative", acknowledged: true })

    let finishSecond!: () => void
    const completeSecond = new Promise<void>((resolve) => {
      finishSecond = resolve
    })
    const second = controller.cancel(sid("ses_1"), { complete: completeSecond })
    await flush()
    finishSecond()
    expect(await second).toMatchObject({ mode: "cooperative", acknowledged: false })

    controller.dispose()
  })
})

describe("11.1 terminate() interrupts a tight synchronous loop", () => {
  test("terminate settles fast and the busy worker never finishes spinning", async () => {
    await using harness = createWorkerHarness({ cores: 1 })
    const entry = path.join(import.meta.dir, "../../fixture/harness-worker.ts")
    const worker = harness.spawn(entry)
    await worker.waitFor("ready")

    worker.post({ type: "spin", ms: 10_000 })
    // Give the worker a moment to enter the synchronous loop; it cannot process
    // messages while spinning, so only terminate() can stop it (3.10 evidence).
    await new Promise((resolve) => setTimeout(resolve, 20))

    const started = Date.now()
    const code = await worker.terminate()
    const elapsed = Date.now() - started

    expect(worker.alive).toBe(false)
    expect(elapsed).toBeLessThan(2_000)
    expect(typeof code).toBe("number")
    expect(worker.messages.some((message) => message.type === "spun")).toBe(false)
  })
})

// ---- additive dispatch wiring -------------------------------------------------

interface PendingRun {
  readonly resolve: () => void
  readonly reject: (error: unknown) => void
}

/**
 * A `SessionWorker` that owns a cancel port. In cooperative mode a `cancel`
 * frame makes the run settle on its next checkpoint (simulated by a microtask);
 * `terminateCount` proves whether the terminate path ran.
 */
class CooperativeWorker implements SessionWorker {
  alive = true
  readonly runs: SessionRun[] = []
  readonly hub: CancelPort
  readonly worker: CancelPort
  terminateCount = 0
  private readonly pending = new Map<SessionID, PendingRun>()

  constructor(
    readonly id: number,
    private readonly cooperative: boolean,
  ) {
    const pair = createPortPair()
    this.hub = pair.hub
    this.worker = pair.worker
    this.worker.onMessage((frame) => {
      if (frame.type !== "cancel") return
      this.worker.notify({ type: "cancel-ack", sessionID: frame.sessionID, accepted: true })
      if (!this.cooperative) return
      const pending = this.pending.get(frame.sessionID as SessionID)
      queueMicrotask(() => pending?.resolve())
    })
  }

  execute(run: SessionRun): Promise<void> {
    this.runs.push(run)
    return new Promise<void>((resolve, reject) => {
      this.pending.set(run.sessionID, { resolve, reject })
    })
  }

  finish(sessionID: SessionID): void {
    const pending = this.pending.get(sessionID)
    this.pending.delete(sessionID)
    pending?.resolve()
  }

  async terminate(): Promise<void> {
    this.alive = false
    this.terminateCount++
    const pending = [...this.pending.values()]
    this.pending.clear()
    for (const run of pending) run.reject(new SessionCancelledError({ message: "terminated", sessionID: "" }))
  }
}

class PortPool implements WorkerPool {
  readonly workers: CooperativeWorker[]
  readonly spawned: CooperativeWorker[] = []

  constructor(
    k: number,
    private readonly cooperative = true,
  ) {
    this.workers = Array.from({ length: k }, (_, index) => this.make(index + 1, cooperative))
  }

  private make(id: number, cooperative: boolean): CooperativeWorker {
    return new CooperativeWorker(id, cooperative)
  }

  spawn(): SessionWorker {
    const worker = this.make(100 + this.spawned.length, this.cooperative)
    this.spawned.push(worker)
    return worker
  }

  async terminateAll(): Promise<void> {}
}

function cancellationConfig(pool: PortPool, overrides: Partial<CancellationConfig> = {}): CancellationConfig {
  return {
    portFor: (worker) => (worker as CooperativeWorker).hub,
    graceMs: 500,
    schedule: createRecordingScheduler<CancelScheduler>().schedule,
    reap: () => {},
    ...overrides,
  }
}

describe("11.4 graceful cancel reuses the worker; terminate path replaces it", () => {
  test("cooperative cancel kills nothing, closes the run, returns the worker to the pool", async () => {
    const pool = new PortPool(1)
    const cancelled: string[] = []
    const dispatcher = createDispatcher({
      pool,
      cancellation: cancellationConfig(pool, {
        onCancelled: (event) => cancelled.push(`${event.sessionID}:${event.mode}`),
      }),
    })

    const run = capture(dispatcher.dispatch({ sessionID: sid("ses_1") }))
    await flush()

    const outcome = await dispatcher.cancelWithGrace(sid("ses_1"))
    expect(outcome).toMatchObject({ mode: "cooperative", winner: "cancel", acknowledged: true })
    expect(await run).toEqual({ ok: false, error: expect.any(SessionCancelledError) })

    // 11.4: the per-session worker was NOT terminated and is reused by the
    // same session's next turn.
    const worker = pool.spawned[0]
    expect(pool.spawned).toHaveLength(1)
    expect(worker.alive).toBe(true)
    expect(worker.terminateCount).toBe(0)
    expect(dispatcher.registry.has(sid("ses_1"))).toBe(false)
    expect(cancelled).toEqual(["ses_1:cooperative"])

    const next = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()
    expect(worker.runs.map((entry) => entry.sessionID)).toEqual([sid("ses_1"), sid("ses_1")])
    worker.finish(sid("ses_1"))
    await next
  })

  test("a run that ignores the checkpoint is terminated and replaced (11.6)", async () => {
    const pool = new PortPool(1, false)
    const scheduler = createRecordingScheduler<CancelScheduler>()
    const reaped: number[] = []
    const cancelled: Array<{ mode: string; resumeFromDb: boolean }> = []
    const dispatcher = createDispatcher({
      pool,
      cancellation: cancellationConfig(pool, {
        schedule: scheduler.schedule,
        reap: () => reaped.push(1),
        onCancelled: (event) => cancelled.push({ mode: event.mode, resumeFromDb: event.resumeFromDb }),
      }),
    })

    const run = capture(dispatcher.dispatch({ sessionID: sid("ses_1") }))
    await flush()

    const pending = dispatcher.cancelWithGrace(sid("ses_1"))
    await flush()
    scheduler.pending().find((timer) => timer.delayMs === 500)?.run()

    const outcome = await pending
    expect(outcome).toMatchObject({ mode: "terminated", winner: "cancel" })
    expect(await run).toEqual({ ok: false, error: expect.any(SessionCancelledError) })
    // The controller terminates once; the dispatcher may re-terminate the same
    // slot idempotently, so only "at least one terminate" is contractual here.
    expect(pool.spawned[0].terminateCount).toBeGreaterThanOrEqual(1)
    expect(pool.spawned[0].alive).toBe(false)
    expect(reaped).toHaveLength(1)
    // 11.6: the terminated session worker is dropped; a later dispatch lazily
    // spawns a fresh one.
    expect(pool.spawned).toHaveLength(1)
    expect(cancelled).toEqual([{ mode: "terminated", resumeFromDb: true }])
    expect(dispatcher.registry.size).toBe(0)
  })
})

describe("11.7 crash taxonomy", () => {
  test("WorkerCrashedError is the single crash identity (tag-matched, no reparent)", () => {
    const crash = new WorkerCrashedError({ message: "worker 1 crashed", workerID: 1 })
    expect(crash).toBeInstanceOf(WorkerCrashedError)
    expect(crash._tag).toBe("WorkerCrashedError")
    expect(Object.getPrototypeOf(crash)).toBe(WorkerCrashedError.prototype)
  })
})

describe("11.8 parent cancel cascades to subagents, children first", () => {
  test("cancels every child before the parent", async () => {
    const pool = new PortPool(3)
    const order: string[] = []
    const dispatcher = createDispatcher({
      pool,
      cancellation: cancellationConfig(pool, {
        childrenOf: (sessionID) => (sessionID === sid("ses_parent") ? [sid("ses_child_a"), sid("ses_child_b")] : []),
        onCancelled: (event) => order.push(event.sessionID),
      }),
    })

    const parent = capture(dispatcher.dispatch({ sessionID: sid("ses_parent") }))
    const childA = capture(dispatcher.dispatch({ sessionID: sid("ses_child_a") }))
    const childB = capture(dispatcher.dispatch({ sessionID: sid("ses_child_b") }))
    await flush()

    await dispatcher.cancelWithGrace(sid("ses_parent"))

    expect(await parent).toEqual({ ok: false, error: expect.any(SessionCancelledError) })
    expect(await childA).toEqual({ ok: false, error: expect.any(SessionCancelledError) })
    expect(await childB).toEqual({ ok: false, error: expect.any(SessionCancelledError) })
    expect(order).toEqual(["ses_child_a", "ses_child_b", "ses_parent"])
  })
})

/**
 * A `SessionWorker` that owns a cancel port but never acknowledges the cancel:
 * the run simply completes on its own. This is the completion-wins side of the
 * cancel-vs-completion race (Q11) once the cancel is already in flight.
 */
class CompletingWorker implements SessionWorker {
  alive = true
  readonly runs: SessionRun[] = []
  readonly hub: CancelPort
  private readonly worker: CancelPort
  private readonly pending = new Map<SessionID, PendingRun>()

  constructor(readonly id: number) {
    const pair = createPortPair()
    this.hub = pair.hub
    this.worker = pair.worker
    this.worker.onMessage((frame) => {
      if (frame.type !== "cancel") return
      // Deliberately no `cancel-ack`: the run finishes naturally instead.
      queueMicrotask(() => this.pending.get(frame.sessionID as SessionID)?.resolve())
    })
  }

  execute(run: SessionRun): Promise<void> {
    this.runs.push(run)
    return new Promise<void>((resolve, reject) => {
      this.pending.set(run.sessionID, { resolve, reject })
    })
  }

  async terminate(): Promise<void> {
    this.alive = false
    const pending = [...this.pending.values()]
    this.pending.clear()
    for (const run of pending) run.reject(new SessionCancelledError({ message: "terminated", sessionID: "" }))
  }
}

class CompletingPool implements WorkerPool {
  readonly workers: CompletingWorker[]
  readonly spawned: CompletingWorker[] = []

  constructor(k: number) {
    this.workers = Array.from({ length: k }, (_, index) => new CompletingWorker(index + 1))
  }

  spawn(): SessionWorker {
    const worker = new CompletingWorker(100 + this.spawned.length)
    this.spawned.push(worker)
    return worker
  }

  async terminateAll(): Promise<void> {}
}

describe("11.9 completion wins the cancel race (Q11)", () => {
  test("a run that completes during the grace returns completed and the caller resolves", async () => {
    const pool = new CompletingPool(0)
    const cancelled: string[] = []
    const dispatcher = createDispatcher({
      pool,
      cancellation: {
        portFor: (worker) => (worker as CompletingWorker).hub,
        graceMs: 500,
        schedule: createRecordingScheduler<CancelScheduler>().schedule,
        reap: () => {},
        onCancelled: (event) => cancelled.push(`${event.sessionID}:${event.mode}`),
      },
    })

    const run = capture(dispatcher.dispatch({ sessionID: sid("ses_1") }))
    await flush()

    const outcome = await dispatcher.cancelWithGrace(sid("ses_1"))
    expect(outcome).toMatchObject({ mode: "completed", winner: "completion", acknowledged: false })
    // The caller is reconciled with the reported winner — completion wins, so
    // the dispatch promise RESOLVES (capture reports `{ ok: true }`).
    expect(await run).toEqual({ ok: true, value: undefined })
    // The run completed on its own: the per-session worker was neither
    // terminated nor replaced.
    expect(pool.spawned).toHaveLength(1)
    expect(pool.spawned[0].alive).toBe(true)
    expect(cancelled).toEqual(["ses_1:completed"])
  })
})

// A prompt dispatched while a cancel owns the grace window must not be
// promoted onto the worker the cancel is about to terminate.
describe("cancel-vs-new-dispatch race", () => {
  test("a prompt arriving during grace runs cleanly on a fresh worker", async () => {
    const pool = createFakeWorkerPool(1, { onCancel: "ignore" })
    const scheduler = createRecordingScheduler<CancelScheduler>()
    const dispatcher = createDispatcher({
      pool,
      cancellation: {
        portFor: (worker) => (worker as FakeSessionWorker).cancelPort,
        graceMs: 500,
        schedule: scheduler.schedule,
        reap: () => {},
      },
    })

    const runA = capture(dispatcher.dispatch({ sessionID: sid("ses_race"), depth: 0 }))
    await flush()
    const workerA = pool.spawned[0]

    // Run A ignores the cooperative checkpoint; the grace timer is still armed.
    const cancel = dispatcher.cancelWithGrace(sid("ses_race"))
    await flush()

    // Prompt B arrives during the grace window.
    const runB = capture(dispatcher.dispatch({ sessionID: sid("ses_race"), depth: 1 }))
    await flush()
    expect(workerA.runs.map((run) => run.depth)).toEqual([0])

    // Grace expires: A is terminated. B must not be started on A's corpse.
    scheduler.pending().find((timer) => timer.delayMs === 500)?.run()
    await cancel
    await flush()

    expect(await runA).toMatchObject({ ok: false, error: expect.any(SessionCancelledError) })
    expect(workerA.alive).toBe(false)

    // B starts on a fresh worker and settles normally (no "terminated" rejection).
    expect(pool.spawned).toHaveLength(2)
    const workerB = pool.spawned[1]
    expect(workerB.runs.map((run) => run.depth)).toEqual([1])
    workerB.finish(sid("ses_race"))
    expect(await runB).toEqual({ ok: true, value: undefined })

    await dispatcher.terminateAll()
  })
})

describe("12 D1 cancel requested logging", () => {
  test("createCancelController emits 'cancel requested' once with sessionID, workerID and graceMs", () => {
    const pair = createPortPair()
    const scheduler = createRecordingScheduler<CancelScheduler>()
    const { bridge, entries } = recordingLog()
    const controller = createCancelController({
      port: pair.hub,
      terminate: () => {},
      graceMs: 250,
      workerID: 5,
      schedule: scheduler.schedule,
      bridge,
    })

    const never = new Promise<void>(() => {})
    const first = controller.cancel(sid("ses_1"), { complete: never })
    // `dispose` settles the in-flight request loud, so handle the rejection.
    first.catch(() => undefined)

    expect(entries).toEqual([
      {
        message: "session workers: cancel requested",
        fields: { sessionID: "ses_1", workerID: 5, graceMs: 250 },
      },
    ])

    // An idempotent join is not a new request: no second line.
    const second = controller.cancel(sid("ses_1"), { complete: never })
    second.catch(() => undefined)
    expect(entries).toHaveLength(1)

    controller.dispose()
  })
})

// ---------------------------------------------------------------------------
// Dispose settles in-flight requests; cancel returns a rejected promise.
// ---------------------------------------------------------------------------

describe("dispose settles in-flight cancels", () => {
  test("dispose rejects an in-flight cancel instead of leaving it pending", async () => {
    const pair = createPortPair()
    const scheduler = createRecordingScheduler<CancelScheduler>()
    const controller = createCancelController({
      port: pair.hub,
      terminate: () => {},
      graceMs: 500,
      schedule: scheduler.schedule,
    })

    const never = new Promise<void>(() => {})
    const first = controller.cancel(sid("ses_1"), { complete: never })
    controller.dispose()

    await expect(first).rejects.toThrow(/disposed/)
    expect(controller.pending).toBe(0)
  })

  test("cancel after dispose returns a rejected promise, not a synchronous throw", async () => {
    const pair = createPortPair()
    const controller = createCancelController({ port: pair.hub, terminate: () => {} })
    controller.dispose()

    let outcome: Promise<unknown> | undefined
    expect(() => {
      outcome = controller.cancel(sid("ses_1"))
    }).not.toThrow()
    await expect(outcome!).rejects.toThrow(/disposed/)
  })
})
