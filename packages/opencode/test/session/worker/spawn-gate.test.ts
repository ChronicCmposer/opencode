// The spawn gate is the one global permit that serializes `new Worker` until
// the spawned worker has booted (its first frame). These tests pin the
// lifecycle: one holder at a time, FIFO waiters, release only on
// `ready()`/`fail()`, and an acquisition timeout that fails the spawn instead
// of the session.

import { describe, expect, test } from "bun:test"
import { createSpawnGate } from "@/session/worker/spawn-gate"

/** Track settlement without leaving an unhandled rejection behind. */
function observe<T>(promise: Promise<T>) {
  const state: { settled: boolean; value?: T; error?: unknown } = { settled: false }
  promise.then(
    (value) => {
      state.settled = true
      state.value = value
    },
    (error) => {
      state.settled = true
      state.error = error
    },
  )
  return state
}

const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 10))

describe("createSpawnGate", () => {
  test("grants one permit at a time", async () => {
    const gate = createSpawnGate({ timeoutMs: 1_000, capacity: 1 })

    const first = await gate.acquire()
    const second = gate.acquire()
    const secondState = observe(second)

    await tick()
    expect(secondState.settled).toBe(false)

    first.release()
    const secondPermit = await second
    expect(secondPermit).toBeDefined()
    secondPermit.release()
  })

  test("releases are not on acquire or construction", async () => {
    const gate = createSpawnGate({ timeoutMs: 1_000, capacity: 1 })
    const first = await gate.acquire()
    const second = gate.acquire()
    const secondState = observe(second)

    await tick()
    expect(secondState.settled).toBe(false)
    expect(secondState.error).toBeUndefined()

    first.release()
    const secondPermit = await second
    secondPermit.release()

    const thirdPermit = await gate.acquire()
    thirdPermit.release()
  })

  test("queues concurrent acquisitions FIFO", async () => {
    const gate = createSpawnGate({ timeoutMs: 1_000, capacity: 1 })
    const first = await gate.acquire()
    const order: string[] = []

    const second = gate.acquire().then((permit) => {
      order.push("second")
      return permit
    })
    const third = gate.acquire().then((permit) => {
      order.push("third")
      return permit
    })

    first.release()
    const secondPermit = await second
    await tick()
    expect(order).toEqual(["second"])

    secondPermit.release()
    const thirdPermit = await third
    expect(order).toEqual(["second", "third"])
    thirdPermit.release()
  })

  test("a waiter is not rejected while it waits", async () => {
    const gate = createSpawnGate({ timeoutMs: 1_000, capacity: 1 })
    const first = await gate.acquire()
    const second = gate.acquire()
    const secondState = observe(second)

    await tick()
    expect(secondState.error).toBeUndefined()

    first.release()
    const secondPermit = await second
    expect(secondState.error).toBeUndefined()
    secondPermit.release()
  })

  test("a failed release frees the permit for the next waiter", async () => {
    const gate = createSpawnGate({ timeoutMs: 1_000, capacity: 1 })
    const first = await gate.acquire()
    const second = gate.acquire()

    first.release("failed", new Error("worker boot failed"))

    const secondPermit = await second
    secondPermit.release()
  })

  test("release is idempotent", async () => {
    const gate = createSpawnGate({ timeoutMs: 1_000, capacity: 1 })
    const first = await gate.acquire()
    first.release()
    first.release()
    first.release("failed", new Error("late failure"))

    const second = await gate.acquire()
    const third = gate.acquire()
    const thirdState = observe(third)

    await tick()
    expect(thirdState.settled).toBe(false)

    second.release()
    const thirdPermit = await third
    thirdPermit.release()
  })

  test("release with different outcomes still releases once", async () => {
    const gate = createSpawnGate({ timeoutMs: 1_000, capacity: 1 })
    const first = await gate.acquire()
    first.release("failed", new Error("worker boot failed"))
    first.release()

    const second = await gate.acquire()
    const third = gate.acquire()
    const thirdState = observe(third)
    await tick()
    expect(thirdState.settled).toBe(false)

    second.release()
    ;(await third).release()
  })

  test("times out a waiting acquisition with a descriptive error", async () => {
    const gate = createSpawnGate({ timeoutMs: 20, capacity: 1 })
    const first = await gate.acquire()

    const timeout = gate.acquire()
    const timeoutState = observe(timeout)
    await new Promise<void>((resolve) => setTimeout(resolve, 40))

    expect(timeoutState.settled).toBe(true)
    expect(timeoutState.error).toBeInstanceOf(Error)
    expect((timeoutState.error as Error).message).toContain("spawn gate")
    expect((timeoutState.error as Error).message).toContain("20ms")

    // The timed-out waiter must not leak capacity.
    first.release()
    const next = await gate.acquire()
    next.release()
  })

  test("release accepts an optional outcome and the permit has no fail method", async () => {
    const gate = createSpawnGate({ timeoutMs: 1_000, capacity: 1 })

    const first = await gate.acquire()
    expect("fail" in first).toBe(false)
    expect("ready" in first).toBe(false)
    expect(typeof first.release).toBe("function")
    first.release()

    const second = await gate.acquire()
    second.release("ready")

    const third = await gate.acquire()
    third.release("failed", new Error("worker boot failed"))

    const fourth = await gate.acquire()
    fourth.release()
  })

  test("rejects an invalid timeout at construction", () => {
    expect(() => createSpawnGate({ timeoutMs: 0 })).toThrow(/timeoutMs/)
    expect(() => createSpawnGate({ timeoutMs: Number.NaN })).toThrow(/timeoutMs/)
  })

  test("rejects an invalid capacity at construction", () => {
    expect(() => createSpawnGate({ timeoutMs: 1_000, capacity: 0 })).toThrow(/capacity/)
    expect(() => createSpawnGate({ timeoutMs: 1_000, capacity: 1.5 })).toThrow(/capacity/)
  })
})

describe("cap-bounded parallel boots", () => {
  test("admits capacity boots concurrently and queues the rest FIFO", async () => {
    const gate = createSpawnGate({ timeoutMs: 1_000, capacity: 2 })
    expect(gate.capacity).toBe(2)

    const first = await gate.acquire()
    const second = await gate.acquire()
    expect(gate.active).toBe(2)
    expect(gate.held).toBe(true)

    const third = gate.acquire()
    const thirdState = observe(third)
    await tick()
    expect(thirdState.settled).toBe(false)
    expect(gate.waiting).toBe(1)

    // A stalled boot occupies one slot, not the whole gate: releasing one slot
    // admits exactly one waiter.
    first.release()
    const thirdPermit = await third
    expect(gate.active).toBe(2)
    expect(gate.waiting).toBe(0)

    second.release()
    thirdPermit.release()
    expect(gate.active).toBe(0)
    expect(gate.held).toBe(false)
  })

  test("defaults to a bounded parallel capacity, not a single permit", async () => {
    const gate = createSpawnGate({ timeoutMs: 1_000 })
    expect(gate.capacity).toBeGreaterThan(1)
    const permits = await Promise.all([gate.acquire(), gate.acquire()])
    expect(gate.active).toBe(2)
    for (const permit of permits) permit.release()
  })

  test("a stalled boot does not block an unrelated boot", async () => {
    const gate = createSpawnGate({ timeoutMs: 1_000, capacity: 2 })
    const stalled = await gate.acquire()
    const other = await gate.acquire()
    // The stalled holder never releases; the other boot still completes.
    other.release()
    expect(gate.active).toBe(1)
    stalled.release()
  })
})

describe("abortable acquisitions", () => {
  test("aborting a queued acquisition removes its waiter without consuming a permit", async () => {
    const gate = createSpawnGate({ timeoutMs: 1_000, capacity: 1 })
    const first = await gate.acquire()
    const controller = new AbortController()
    const queued = gate.acquire(controller.signal)
    const state = observe(queued)
    await tick()
    expect(gate.waiting).toBe(1)

    controller.abort()
    await tick()
    expect(state.settled).toBe(true)
    expect(state.error).toBeInstanceOf(Error)
    expect((state.error as Error).message).toContain("aborted")
    expect(gate.waiting).toBe(0)
    expect(gate.active).toBe(1)

    // The aborted waiter never took the slot: the next FIFO waiter is admitted
    // by the single release, and the permit accounting stays exact.
    const next = gate.acquire()
    const nextState = observe(next)
    await tick()
    expect(nextState.settled).toBe(false)
    first.release()
    const permit = await next
    expect(gate.active).toBe(1)
    permit.release()
    expect(gate.active).toBe(0)
  })

  test("an already-aborted signal rejects without queueing", async () => {
    const gate = createSpawnGate({ timeoutMs: 1_000, capacity: 1 })
    const controller = new AbortController()
    controller.abort()
    await expect(gate.acquire(controller.signal)).rejects.toThrow(/aborted/)
    expect(gate.waiting).toBe(0)
    expect(gate.active).toBe(0)
    const permit = await gate.acquire()
    permit.release()
  })
})

describe("abort listener lifecycle", () => {
  test("a timed-out acquisition removes its abort listener", async () => {
    const gate = createSpawnGate({ timeoutMs: 20, capacity: 1 })
    const first = await gate.acquire()
    let added = 0
    let removed = 0
    const signal = {
      aborted: false,
      addEventListener: () => {
        added += 1
      },
      removeEventListener: () => {
        removed += 1
      },
    } as unknown as AbortSignal

    const waiting = gate.acquire(signal)
    const state = observe(waiting)
    await new Promise<void>((resolve) => setTimeout(resolve, 40))
    expect(state.settled).toBe(true)
    expect(state.error).toBeInstanceOf(Error)
    expect(added).toBe(1)
    // The deadline path must detach the listener so a long-lived signal does not
    // retain every timed-out acquisition.
    expect(removed).toBe(1)
    first.release()
  })
})
