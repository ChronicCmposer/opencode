import { describe, expect, test } from "bun:test"
import { Effect } from "effect"

import {
  RUN_BARRIER_PHASES,
  RUN_BARRIER_TIMEOUT_MS,
  RunBarrierBusyError,
  RunBarrierMissingReportError,
  RunBarrierReportError,
  RunBarrierTimeout,
  createRunBarrier,
  fromBarrier,
  layer,
  make,
  node,
  Service,
  type RunBarrier,
  type RunBarrierPhase,
  type RunBarrierPhases,
  type RunBarrierRun,
  type RunBarrierScheduler,
} from "@/session/run-barrier"
import { it } from "../../lib/effect"

interface Deferred<T> {
  readonly promise: Promise<T>
  resolve(value: T): void
  reject(error: unknown): void
}

function deferred<T = void>(): Deferred<T> {
  let resolve!: (value: T) => void
  let reject!: (error: unknown) => void
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

interface FakeScheduler {
  readonly scheduler: RunBarrierScheduler
  advance(ms: number): void
  readonly pending: number
}

function createFakeScheduler(): FakeScheduler {
  let now = 0
  let nextId = 1
  const timers = new Map<unknown, { readonly at: number; readonly run: () => void }>()
  return {
    scheduler: {
      schedule: (run, delayMs) => {
        const id = nextId++
        timers.set(id, { at: now + delayMs, run })
        return id
      },
      cancel: (handle) => {
        timers.delete(handle)
      },
    },
    advance(ms) {
      now += ms
      const due = [...timers.entries()].filter(([, timer]) => timer.at <= now).sort((a, b) => a[1].at - b[1].at)
      for (const [id, timer] of due) {
        timers.delete(id)
        timer.run()
      }
    },
    get pending() {
      return timers.size
    },
  }
}

function phases(overrides: Partial<RunBarrierPhases> = {}): RunBarrierPhases {
  const settled = Promise.resolve()
  return {
    commits: settled,
    projectors: settled,
    tools: settled,
    promotion: settled,
    ...overrides,
  }
}

describe("per-Session run barrier", () => {
  test("drains only after commits, projectors, tool settlement, and promotion settle", async () => {
    const fake = createFakeScheduler()
    const barrier = createRunBarrier({ scheduler: fake.scheduler })
    const commits = deferred()
    const projectors = deferred()
    const tools = deferred()
    const promotion = deferred()
    const pending = barrier.wait("s1", {
      commits: commits.promise,
      projectors: projectors.promise,
      tools: tools.promise,
      promotion: promotion.promise,
    })

    commits.resolve()
    projectors.resolve()
    tools.resolve()
    await Promise.resolve()
    expect(barrier.state("s1")).toBe("draining")

    promotion.resolve()
    expect(await pending).toEqual({ sessionID: "s1", status: "drained" })
    expect(barrier.state("s1")).toBe("drained")
    expect(fake.pending).toBe(0)
  })

  test("admits the next run after the previous barrier drained", async () => {
    const fake = createFakeScheduler()
    const barrier = createRunBarrier({ scheduler: fake.scheduler })

    expect(await barrier.wait("s1", phases())).toEqual({ sessionID: "s1", status: "drained" })
    expect(await barrier.wait("s1", phases())).toEqual({ sessionID: "s1", status: "drained" })
    expect(barrier.state("s1")).toBe("drained")
  })

  test("rejects a concurrent barrier for the same Session while draining", async () => {
    const fake = createFakeScheduler()
    const barrier = createRunBarrier({ scheduler: fake.scheduler })
    const stuck = deferred()
    const pending = barrier.wait("s1", phases({ promotion: stuck.promise }))

    await expect(barrier.wait("s1", phases())).rejects.toBeInstanceOf(RunBarrierBusyError)

    stuck.resolve()
    expect(await pending).toEqual({ sessionID: "s1", status: "drained" })
  })

  test("expires at the 5 s bound with a typed RunBarrierTimeout and blocks the next run", async () => {
    const fake = createFakeScheduler()
    const barrier = createRunBarrier({ scheduler: fake.scheduler })
    const promotion = deferred()
    const pending = barrier.wait(
      "s1",
      phases({
        promotion: promotion.promise,
      }),
    )

    fake.advance(RUN_BARRIER_TIMEOUT_MS - 1)
    expect(barrier.state("s1")).toBe("draining")
    fake.advance(1)

    await expect(pending).rejects.toBeInstanceOf(RunBarrierTimeout)
    expect(barrier.state("s1")).toBe("timed-out")
    // No next run starts after expiry until the barrier is explicitly reset.
    await expect(barrier.wait("s1", phases())).rejects.toBeInstanceOf(RunBarrierTimeout)
  })

  test("cancellation wins over barrier completion", async () => {
    const fake = createFakeScheduler()
    const barrier = createRunBarrier({ scheduler: fake.scheduler })
    const pending = barrier.wait("s1", phases())

    barrier.cancel("s1")

    expect(await pending).toEqual({ sessionID: "s1", status: "cancelled" })
    expect(barrier.state("s1")).toBe("cancelled")
    expect(fake.pending).toBe(0)
  })

  test("cancellation wins while phases are still pending and cannot time out later", async () => {
    const fake = createFakeScheduler()
    const barrier = createRunBarrier({ scheduler: fake.scheduler })
    const commits = deferred()
    const pending = barrier.wait("s1", phases({ commits: commits.promise }))

    barrier.cancel("s1")
    expect(await pending).toEqual({ sessionID: "s1", status: "cancelled" })

    fake.advance(RUN_BARRIER_TIMEOUT_MS)
    expect(barrier.state("s1")).toBe("cancelled")
  })

  test("a timed-out Session does not block another Session's barrier", async () => {
    const fake = createFakeScheduler()
    const barrier = createRunBarrier({ scheduler: fake.scheduler })
    const stuck = deferred()
    const timedOut = barrier.wait("s1", phases({ promotion: stuck.promise }))
    const other = barrier.wait("s2", phases())

    expect(await other).toEqual({ sessionID: "s2", status: "drained" })
    fake.advance(RUN_BARRIER_TIMEOUT_MS)
    await expect(timedOut).rejects.toBeInstanceOf(RunBarrierTimeout)
    expect(barrier.state("s2")).toBe("drained")
  })

  test("abandon re-admits a Session whose timed-out owner is gone", async () => {
    const fake = createFakeScheduler()
    const barrier = createRunBarrier({ scheduler: fake.scheduler })
    const stuck = deferred()
    const pending = barrier.wait("s1", phases({ promotion: stuck.promise }))
    fake.advance(RUN_BARRIER_TIMEOUT_MS)
    await expect(pending).rejects.toBeInstanceOf(RunBarrierTimeout)
    expect(barrier.state("s1")).toBe("timed-out")

    barrier.abandon("s1")
    expect(barrier.state("s1")).toBe("idle")
    expect(await barrier.wait("s1", phases())).toEqual({ sessionID: "s1", status: "drained" })
    // Idempotent: a second recovery of the same dead barrier is harmless.
    barrier.abandon("s1")
  })

  test("abandon refuses a barrier that is still draining", async () => {
    const fake = createFakeScheduler()
    const barrier = createRunBarrier({ scheduler: fake.scheduler })
    const stuck = deferred()
    const pending = barrier.wait("s1", phases({ tools: stuck.promise }))

    expect(() => barrier.abandon("s1")).toThrow(RunBarrierBusyError)
    expect(barrier.state("s1")).toBe("draining")
    stuck.resolve()
    expect(await pending).toEqual({ sessionID: "s1", status: "drained" })
  })

  test("reset re-admits a Session after a timeout", async () => {
    const fake = createFakeScheduler()
    const barrier = createRunBarrier({ scheduler: fake.scheduler })
    const stuck = deferred()
    const pending = barrier.wait("s1", phases({ promotion: stuck.promise }))
    fake.advance(RUN_BARRIER_TIMEOUT_MS)
    await expect(pending).rejects.toBeInstanceOf(RunBarrierTimeout)

    barrier.reset("s1")
    expect(barrier.state("s1")).toBe("idle")
    expect(await barrier.wait("s1", phases())).toEqual({ sessionID: "s1", status: "drained" })
  })
})

describe("per-Session run barrier — real phase reports", () => {
  function reportAll(run: RunBarrierRun, skip?: RunBarrierPhase): void {
    if (skip !== "commits") run.commit("msg_1")
    if (skip !== "projectors") run.projector("session-row")
    if (skip !== "tools") run.noTools()
    if (skip !== "promotion") run.promotion("no-job")
  }

  test("drains only after commits, projectors, tools, and promotion are reported", async () => {
    const fake = createFakeScheduler()
    const barrier = createRunBarrier({ scheduler: fake.scheduler })
    const run = barrier.beginRun("s1")
    run.commit("msg_1")
    run.projector("session-row")
    run.promotion("no-job")

    const pending = barrier.wait("s1")
    await Promise.resolve()
    expect(barrier.state("s1")).toBe("draining")

    run.noTools()
    expect(await pending).toEqual({ sessionID: "s1", status: "drained" })
    expect(barrier.state("s1")).toBe("drained")
    expect(fake.pending).toBe(0)
  })

  test("a missing reporter is never fabricated into a settlement", async () => {
    const fake = createFakeScheduler()
    const barrier = createRunBarrier({ scheduler: fake.scheduler })
    const run = barrier.beginRun("s1")
    reportAll(run, "tools")

    const pending = barrier.wait("s1")
    await Promise.resolve()
    expect(barrier.state("s1")).toBe("draining")

    fake.advance(RUN_BARRIER_TIMEOUT_MS)
    await expect(pending).rejects.toBeInstanceOf(RunBarrierTimeout)
    expect(barrier.state("s1")).toBe("timed-out")
    // The next run is refused: a started run is not proof that tools settled.
    await expect(barrier.wait("s1")).rejects.toBeInstanceOf(RunBarrierTimeout)
  })

  test("a zero-tool run settles tools only through an explicit noTools report", async () => {
    const fake = createFakeScheduler()
    const barrier = createRunBarrier({ scheduler: fake.scheduler })

    const abandoned = barrier.beginRun("s1")
    reportAll(abandoned, "tools")
    const timedOut = barrier.wait("s1")
    fake.advance(RUN_BARRIER_TIMEOUT_MS)
    await expect(timedOut).rejects.toBeInstanceOf(RunBarrierTimeout)
    barrier.reset("s1")

    const run = barrier.beginRun("s1")
    run.commit("msg_1")
    run.projector("session-row")
    run.promotion("no-job")
    run.noTools()
    expect(await barrier.wait("s1")).toEqual({ sessionID: "s1", status: "drained" })
  })

  test("a no-commit run settles commits and projectors explicitly", async () => {
    const fake = createFakeScheduler()
    const barrier = createRunBarrier({ scheduler: fake.scheduler })
    const run = barrier.beginRun("s1")
    run.noCommit()
    run.noTools()
    run.promotion("no-job")

    expect(run.reported("commits")).toBe(true)
    expect(run.reported("projectors")).toBe(true)
    expect(await barrier.wait("s1")).toEqual({ sessionID: "s1", status: "drained" })
    expect(fake.pending).toBe(0)
  })

  test("a no-commit report cannot overwrite a real commit report", async () => {
    const barrier = createRunBarrier()
    const run = barrier.beginRun("s1")
    run.commit("msg_1")
    run.projector("session-row")

    expect(() => run.noCommit()).toThrow(RunBarrierReportError)
    // The conflicting observation fails loud instead of draining a broken run.
    await expect(barrier.wait("s1")).rejects.toBeInstanceOf(RunBarrierReportError)
  })

  test("a none observation cannot settle a phase that requires a real observation", () => {
    const barrier = createRunBarrier()
    const run = barrier.beginRun("s1")

    expect(() => run.report({ sessionID: "s1", phase: "tools", observation: { kind: "none" } })).toThrow(
      RunBarrierReportError,
    )
  })

  test.each([...RUN_BARRIER_PHASES])("refuses a run whose %s reporter never arrived", async (missing) => {
    const fake = createFakeScheduler()
    const barrier = createRunBarrier({ scheduler: fake.scheduler })
    const sessionID = `ses_missing_${missing}`
    const run = barrier.beginRun(sessionID)
    reportAll(run, missing)

    const pending = barrier.wait(sessionID)
    fake.advance(RUN_BARRIER_TIMEOUT_MS)
    await expect(pending).rejects.toBeInstanceOf(RunBarrierTimeout)
    expect(barrier.state(sessionID)).toBe("timed-out")
  })

  test("a run with an in-flight tool waits for the tool report, not for run start", async () => {
    const fake = createFakeScheduler()
    const barrier = createRunBarrier({ scheduler: fake.scheduler })
    const run = barrier.beginRun("s1")
    run.commit("msg_1")
    run.projector("session-row")
    run.promotion("no-job")

    const pending = barrier.wait("s1")
    await Promise.resolve()
    expect(barrier.state("s1")).toBe("draining")

    run.tool("call_write")
    expect(await pending).toEqual({ sessionID: "s1", status: "drained" })
  })

  test("waiting without a begun run is a typed refusal", async () => {
    const barrier = createRunBarrier()
    await expect(barrier.wait("s1")).rejects.toBeInstanceOf(RunBarrierMissingReportError)
    expect(barrier.state("s1")).toBe("failed")
  })

  test("a report for the wrong phase or observation is refused", async () => {
    const fake = createFakeScheduler()
    const barrier = createRunBarrier({ scheduler: fake.scheduler })
    const run = barrier.beginRun("s1")

    expect(() =>
      run.report({ sessionID: "s1", phase: "projectors", observation: { kind: "commit", messageID: "msg_1" } }),
    ).toThrow(RunBarrierReportError)

    const pending = barrier.wait("s1")
    await expect(pending).rejects.toBeInstanceOf(RunBarrierReportError)
    expect(barrier.state("s1")).toBe("failed")
  })

  test("a report for another session is refused", async () => {
    const barrier = createRunBarrier()
    const run = barrier.beginRun("s1")
    expect(() =>
      run.report({ sessionID: "s2", phase: "commits", observation: { kind: "commit", messageID: "msg_1" } }),
    ).toThrow(RunBarrierReportError)
  })

  test("a zero-tool and a real tool report cannot both settle the same run", async () => {
    const barrier = createRunBarrier()
    const run = barrier.beginRun("s1")
    run.noTools()
    expect(() => run.tool("call_after_no_tools")).toThrow(RunBarrierReportError)
  })

  test("a stale run token cannot settle the next run", async () => {
    const fake = createFakeScheduler()
    const barrier = createRunBarrier({ scheduler: fake.scheduler })
    const first = barrier.beginRun("s1")
    reportAll(first)
    expect(await barrier.wait("s1")).toEqual({ sessionID: "s1", status: "drained" })

    const second = barrier.beginRun("s1")
    expect(first.commit("late_msg")).toBe(false)

    const pending = barrier.wait("s1")
    second.projector("session-row")
    second.noTools()
    second.promotion("no-job")
    fake.advance(RUN_BARRIER_TIMEOUT_MS)
    await expect(pending).rejects.toBeInstanceOf(RunBarrierTimeout)
  })

  test("cancellation wins while the report set is incomplete", async () => {
    const fake = createFakeScheduler()
    const barrier = createRunBarrier({ scheduler: fake.scheduler })
    const run = barrier.beginRun("s1")
    run.commit("msg_1")

    const pending = barrier.wait("s1")
    barrier.cancel("s1")

    expect(await pending).toEqual({ sessionID: "s1", status: "cancelled" })
    expect(barrier.state("s1")).toBe("cancelled")
    fake.advance(RUN_BARRIER_TIMEOUT_MS)
    expect(barrier.state("s1")).toBe("cancelled")
  })

  test("a begun run cannot overlap a still-draining run", async () => {
    const fake = createFakeScheduler()
    const barrier = createRunBarrier({ scheduler: fake.scheduler })
    barrier.beginRun("s1")
    const pending = barrier.wait("s1")

    expect(() => barrier.beginRun("s1")).toThrow(RunBarrierBusyError)
    fake.advance(RUN_BARRIER_TIMEOUT_MS)
    await expect(pending).rejects.toBeInstanceOf(RunBarrierTimeout)
  })
})

describe("run barrier service", () => {
  test("node exposes the unbound RunBarrier service tag", () => {
    const entry = node()
    expect(entry.kind).toBe("layer")
    expect(entry.name).toBe("@opencode/RunBarrier")
    expect(entry.service).toBe(Service)
    expect(entry.dependencies).toEqual([])
  })

  it.live("layer installs a barrier that preserves report settlement", () =>
    Effect.gen(function* () {
      const barrier = yield* Service
      const run = yield* barrier.beginRun("s1")
      expect(run.commit("msg_1")).toBe(true)
      expect(run.projector("session-row")).toBe(true)
      expect(run.noTools()).toBe(true)
      expect(run.promotion("no-job")).toBe(true)

      expect(yield* barrier.wait("s1")).toEqual({ sessionID: "s1", status: "drained" })
      expect(yield* barrier.state("s1")).toBe("drained")
      expect(barrier.size).toBe(1)
    }).pipe(Effect.provide(layer({ scheduler: createFakeScheduler().scheduler }))),
  )

  it.live("make and fromBarrier match the createRunBarrier adapter", () =>
    Effect.gen(function* () {
      const service = yield* make({ scheduler: createFakeScheduler().scheduler })
      const raw = createRunBarrier({ scheduler: createFakeScheduler().scheduler })
      expect(service.barrier).toBeDefined()
      expect(fromBarrier(raw).barrier).toBe(raw)

      const serviceRun = yield* service.beginRun("s1")
      serviceRun.noCommit()
      serviceRun.noTools()
      serviceRun.promotion("no-job")
      expect(yield* service.wait("s1")).toEqual({ sessionID: "s1", status: "drained" })

      const rawRun = raw.beginRun("s1")
      rawRun.noCommit()
      rawRun.noTools()
      rawRun.promotion("no-job")
      expect(yield* Effect.promise(() => raw.wait("s1"))).toEqual({ sessionID: "s1", status: "drained" })

      expect(yield* service.state("s1")).toBe(raw.state("s1"))
      expect(service.size).toBe(raw.size)

      yield* service.cancel("s2")
      expect(yield* service.state("s2")).toBe("cancelled")
      yield* service.reset("s2")
      expect(yield* service.state("s2")).toBe("idle")
      yield* service.abandon("s1")
      expect(yield* service.state("s1")).toBe("idle")
    }),
  )
})
