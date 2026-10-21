// Phase 11 — terminate-based cancel & cooperative checkpoints (plan tasks 11.1–11.9).
//
// Covers the cancel module (tokens, checkpoints, control-port frames, grace
// controller) plus the additive dispatch wiring (cooperative vs terminate,
// PGID reap on terminate, cascade, idempotency, crash taxonomy). The real
// process evidence for `terminate()` interrupting a tight sync loop comes from
// the 3.10 spike; the case here is a regression guard over a real Bun isolate.

import { describe, expect, test } from "bun:test"
import path from "node:path"
import type { SessionID } from "@/session/schema"
import {
  CANCEL_GRACE_ENV,
  CheckpointCancelledError,
  DEFAULT_CANCEL_GRACE_MS,
  cancelPortFromMessagePort,
  createCancelController,
  createCancelToken,
  createCheckpoints,
  createOutcomeGate,
  resolveCancelGraceMs,
  runModelStep,
  runToolCall,
  serveCancel,
  type CancelFrame,
  type CancelPort,
  type CancelScheduler,
} from "@/session/worker/cancel"
import {
  createDispatcher,
  SessionCancelledError,
  SessionCrashedError,
  WorkerCrashedError,
  type CancellationConfig,
  type SessionRun,
  type SessionWorker,
  type WorkerKind,
  type WorkerPool,
} from "@/session/worker/dispatch"
import { createWorkerHarness } from "../../lib/worker-harness"

const sid = (value: string) => value as SessionID
const flush = () => new Promise<void>((resolve) => setTimeout(resolve, 0))
const capture = (promise: Promise<unknown>): Promise<unknown> =>
  promise.then(
    () => undefined,
    (error: unknown) => error,
  )

/** An in-process pair of cancel ports; `hub.notify` reaches the worker side. */
function createPortPair(): { hub: CancelPort; worker: CancelPort } {
  const hubListeners = new Set<(frame: CancelFrame) => void>()
  const workerListeners = new Set<(frame: CancelFrame) => void>()
  const deliver = (listeners: Set<(frame: CancelFrame) => void>, frame: CancelFrame): void => {
    for (const listener of [...listeners]) listener(frame)
  }
  return {
    hub: {
      notify: (frame) => deliver(workerListeners, frame),
      onMessage: (handler) => {
        hubListeners.add(handler)
        return () => hubListeners.delete(handler)
      },
    },
    worker: {
      notify: (frame) => deliver(hubListeners, frame),
      onMessage: (handler) => {
        workerListeners.add(handler)
        return () => workerListeners.delete(handler)
      },
    },
  }
}

interface TestTimer {
  readonly delayMs: number
  readonly run: () => void
  cancelled: boolean
}

/** A scheduler that records timers so grace expiry can be fired deterministically. */
function createTestScheduler() {
  const timers: TestTimer[] = []
  const schedule: CancelScheduler = (delayMs, run) => {
    const timer: TestTimer = { delayMs, run, cancelled: false }
    timers.push(timer)
    return () => {
      timer.cancelled = true
    }
  }
  return {
    schedule,
    timers,
    pending: () => timers.filter((timer) => !timer.cancelled),
  }
}

describe("11.2 checkpoints before/after each model step and tool call", () => {
  test("records the boundary and throws CheckpointCancelledError after cancel", () => {
    const token = createCancelToken()
    const checkpoints = createCheckpoints(token)

    checkpoints.at("model:turn:before")
    expect(checkpoints.boundaries).toEqual(["model:turn:before"])

    token.cancel("ui:stop")
    expect(() => checkpoints.at("model:turn:after")).toThrow(CheckpointCancelledError)
  })

  test("runModelStep and runToolCall checkpoint before AND after the body", async () => {
    const token = createCancelToken()
    const checkpoints = createCheckpoints(token)

    const model = await runModelStep(checkpoints, "turn", () => 21 * 2)
    expect(model).toBe(42)
    expect(checkpoints.boundaries).toEqual(["model:turn:before", "model:turn:after"])

    const tool = await runToolCall(checkpoints, "read", async () => "ok")
    expect(tool).toBe("ok")
    expect(checkpoints.boundaries).toEqual([
      "model:turn:before",
      "model:turn:after",
      "tool:read:before",
      "tool:read:after",
    ])
  })

  test("a cancel during a step aborts at the following checkpoint", async () => {
    const token = createCancelToken()
    const checkpoints = createCheckpoints(token)

    await expect(
      runModelStep(checkpoints, "turn", () => {
        token.cancel()
        return "unused"
      }),
    ).rejects.toBeInstanceOf(CheckpointCancelledError)
    expect(checkpoints.boundaries).toEqual(["model:turn:before", "model:turn:after"])
  })
})

describe("11.2/11.4 worker side: cancel+ack on the control port, clean up then ack", () => {
  test("serveCancel cancels the token, kills children, closes the scope, then acks", async () => {
    const pair = createPortPair()
    const token = createCancelToken()
    const events: string[] = []
    const server = serveCancel(pair.worker, token, {
      killChildren: () => {
        events.push("kill-children")
      },
      closeScope: () => {
        events.push("close-scope")
      },
    })
    const acks: CancelFrame[] = []
    pair.hub.onMessage((frame) => acks.push(frame))

    pair.hub.notify({ type: "cancel", sessionID: "ses_1", reason: "ui:stop" })
    await flush()

    expect(token.cancelled).toBe(true)
    expect(token.reason).toBe("ui:stop")
    expect(events).toEqual(["kill-children", "close-scope"])
    expect(server.handled).toBe(1)
    expect(acks).toEqual([{ type: "cancel-ack", sessionID: "ses_1", accepted: true }])

    server.dispose()
  })

  test("cancelPortFromMessagePort round-trips frames over a real MessageChannel", () => {
    const channel = new MessageChannel()
    const hubPort = cancelPortFromMessagePort(channel.port1)
    const workerPort = cancelPortFromMessagePort(channel.port2)

    const received: CancelFrame[] = []
    workerPort.onMessage((frame) => received.push(frame))
    hubPort.notify({ type: "cancel", sessionID: "ses_x" })

    return new Promise<void>((resolve) => {
      setTimeout(() => {
        expect(received).toEqual([{ type: "cancel", sessionID: "ses_x" }])
        resolve()
      }, 20)
    })
  })
})

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
    const scheduler = createTestScheduler()
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
    const scheduler = createTestScheduler()
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
    const scheduler = createTestScheduler()
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

  test("a completion that already landed wins; the controller sends no cancel", async () => {
    const pair = createPortPair()
    const scheduler = createTestScheduler()
    const sent: CancelFrame[] = []
    pair.worker.onMessage((frame) => sent.push(frame))
    const controller = createCancelController({ port: pair.hub, terminate: () => {}, schedule: scheduler.schedule })

    const outcome = controller.cancel(sid("ses_1"), { complete: Promise.resolve() })
    expect(await outcome).toMatchObject({ mode: "completed", winner: "completion" })
    expect(sent).toHaveLength(0)
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
  readonly kind = new Map<number, WorkerKind>()

  constructor(
    k: number,
    cooperative = true,
  ) {
    this.workers = Array.from({ length: k }, (_, index) => this.make(index + 1, cooperative))
    for (const worker of this.workers) this.kind.set(worker.id, "foreground")
  }

  private make(id: number, cooperative: boolean): CooperativeWorker {
    return new CooperativeWorker(id, cooperative)
  }

  spawn(kind: WorkerKind): SessionWorker {
    const worker = this.make(100 + this.spawned.length, true)
    this.kind.set(worker.id, kind)
    this.spawned.push(worker)
    return worker
  }

  async terminateAll(): Promise<void> {}
}

function cancellationConfig(pool: PortPool, overrides: Partial<CancellationConfig> = {}): CancellationConfig {
  return {
    portFor: (worker) => (worker as CooperativeWorker).hub,
    graceMs: 500,
    schedule: createTestScheduler().schedule,
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
    expect(outcome).toMatchObject({ mode: "cooperative", winner: "cancel" })
    expect(await run).toBeInstanceOf(SessionCancelledError)

    // 11.4: the worker was NOT terminated and is reused for the next session.
    expect(pool.workers[0].alive).toBe(true)
    expect(pool.workers[0].terminateCount).toBe(0)
    expect(pool.spawned).toHaveLength(0)
    expect(dispatcher.registry.has(sid("ses_1"))).toBe(false)
    expect(cancelled).toEqual(["ses_1:cooperative"])

    const next = dispatcher.dispatch({ sessionID: sid("ses_2") })
    await flush()
    expect(pool.workers[0].runs.map((entry) => entry.sessionID)).toEqual([sid("ses_1"), sid("ses_2")])
    pool.workers[0].finish(sid("ses_2"))
    await next
  })

  test("a run that ignores the checkpoint is terminated and replaced (11.6)", async () => {
    const pool = new PortPool(1, false)
    const scheduler = createTestScheduler()
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
    expect(await run).toBeInstanceOf(SessionCancelledError)
    expect(pool.workers[0].terminateCount).toBe(1)
    expect(pool.workers[0].alive).toBe(false)
    expect(reaped).toHaveLength(1)
    // 11.6: a fresh worker respawns into the freed slot and is ready for reuse.
    expect(pool.spawned).toHaveLength(1)
    expect(cancelled).toEqual([{ mode: "terminated", resumeFromDb: true }])
    expect(dispatcher.registry.size).toBe(0)
  })
})

describe("11.7 crash taxonomy", () => {
  test("WorkerCrashedError is a SessionCrashedError (session failed)", () => {
    const crash = new WorkerCrashedError({ message: "worker 1 crashed", workerID: 1 })
    expect(crash).toBeInstanceOf(WorkerCrashedError)
    expect(crash).toBeInstanceOf(SessionCrashedError)
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

    expect(await parent).toBeInstanceOf(SessionCancelledError)
    expect(await childA).toBeInstanceOf(SessionCancelledError)
    expect(await childB).toBeInstanceOf(SessionCancelledError)
    expect(order).toEqual(["ses_child_a", "ses_child_b", "ses_parent"])
  })
})
