import { describe, expect, test } from "bun:test"

import {
  RUN_BARRIER_TIMEOUT_MS,
  RunBarrierBusyError,
  RunBarrierTimeout,
  createRunBarrier,
  type RunBarrierPhases,
  type RunBarrierScheduler,
} from "@/session/run-barrier"

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
