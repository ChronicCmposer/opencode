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
    const gate = createSpawnGate({ timeoutMs: 1_000 })

    const first = await gate.acquire()
    const second = gate.acquire()
    const secondState = observe(second)

    await tick()
    expect(secondState.settled).toBe(false)

    first.ready()
    const secondPermit = await second
    expect(secondPermit).toBeDefined()
    secondPermit.ready()
  })

  test("releases are not on acquire or construction", async () => {
    const gate = createSpawnGate({ timeoutMs: 1_000 })
    const first = await gate.acquire()
    const second = gate.acquire()
    const secondState = observe(second)

    await tick()
    expect(secondState.settled).toBe(false)
    expect(secondState.error).toBeUndefined()

    first.ready()
    const secondPermit = await second
    secondPermit.ready()

    const thirdPermit = await gate.acquire()
    thirdPermit.ready()
  })

  test("queues concurrent acquisitions FIFO", async () => {
    const gate = createSpawnGate({ timeoutMs: 1_000 })
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

    first.ready()
    const secondPermit = await second
    await tick()
    expect(order).toEqual(["second"])

    secondPermit.ready()
    const thirdPermit = await third
    expect(order).toEqual(["second", "third"])
    thirdPermit.ready()
  })

  test("a waiter is not rejected while it waits", async () => {
    const gate = createSpawnGate({ timeoutMs: 1_000 })
    const first = await gate.acquire()
    const second = gate.acquire()
    const secondState = observe(second)

    await tick()
    expect(secondState.error).toBeUndefined()

    first.ready()
    const secondPermit = await second
    expect(secondState.error).toBeUndefined()
    secondPermit.ready()
  })

  test("permit.fail releases the permit for the next waiter", async () => {
    const gate = createSpawnGate({ timeoutMs: 1_000 })
    const first = await gate.acquire()
    const second = gate.acquire()

    first.fail(new Error("worker boot failed"))

    const secondPermit = await second
    secondPermit.ready()
  })

  test("ready and fail are idempotent", async () => {
    const gate = createSpawnGate({ timeoutMs: 1_000 })
    const first = await gate.acquire()
    first.ready()
    first.ready()
    first.fail(new Error("late failure"))

    const second = await gate.acquire()
    const third = gate.acquire()
    const thirdState = observe(third)

    await tick()
    expect(thirdState.settled).toBe(false)

    second.ready()
    const thirdPermit = await third
    thirdPermit.ready()
  })

  test("fail then ready releases once", async () => {
    const gate = createSpawnGate({ timeoutMs: 1_000 })
    const first = await gate.acquire()
    first.fail(new Error("worker boot failed"))
    first.ready()

    const second = await gate.acquire()
    const third = gate.acquire()
    const thirdState = observe(third)
    await tick()
    expect(thirdState.settled).toBe(false)

    second.ready()
    ;(await third).ready()
  })

  test("times out a waiting acquisition with a descriptive error", async () => {
    const gate = createSpawnGate({ timeoutMs: 20 })
    const first = await gate.acquire()

    const timeout = gate.acquire()
    const timeoutState = observe(timeout)
    await new Promise<void>((resolve) => setTimeout(resolve, 40))

    expect(timeoutState.settled).toBe(true)
    expect(timeoutState.error).toBeInstanceOf(Error)
    expect((timeoutState.error as Error).message).toContain("spawn gate")
    expect((timeoutState.error as Error).message).toContain("20ms")

    // The timed-out waiter must not leak capacity.
    first.ready()
    const next = await gate.acquire()
    next.ready()
  })

  test("rejects an invalid timeout at construction", () => {
    expect(() => createSpawnGate({ timeoutMs: 0 })).toThrow(/timeoutMs/)
    expect(() => createSpawnGate({ timeoutMs: Number.NaN })).toThrow(/timeoutMs/)
  })
})
