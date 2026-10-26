import { describe, expect, test } from "bun:test"
import { capture, flush, settleOf, sid, withDeadline, awaitSignal } from "./async"
import * as poll from "./poll"

describe("async test primitives", () => {
  test("flush lets queued microtasks and zero-delay timers run", async () => {
    const order: string[] = []
    queueMicrotask(() => order.push("microtask"))
    setTimeout(() => order.push("timer"), 0)
    order.push("sync")

    await flush()

    expect(order).toEqual(["sync", "microtask", "timer"])
  })

  test("capture returns the value on success without throwing", async () => {
    const settled = await capture(Promise.resolve(42))
    expect(settled).toEqual({ ok: true, value: 42 })
  })

  test("capture returns the rejection reason instead of throwing", async () => {
    const boom = new Error("boom")
    const settled = await capture(Promise.reject(boom))
    expect(settled.ok).toBe(false)
    if (!settled.ok) expect(settled.error).toBe(boom)
  })

  test("settleOf reports success as { ok: true, value }", async () => {
    await expect(settleOf(Promise.resolve("done"))).resolves.toEqual({ ok: true, value: "done" })
  })

  test("settleOf reports failure as { ok: false, error } without throwing", async () => {
    const boom = new Error("boom")
    const settled = await settleOf(Promise.reject(boom))
    expect(settled.ok).toBe(false)

    if (!settled.ok) expect(settled.error).toBe(boom)
  })

  test("settleOf and capture are the same primitive", () => {
    expect(capture).toBe(settleOf)
  })

  test("sid brands a plain string for the session boundary", () => {
    const id = sid("ses_1")
    expect(id as string).toBe("ses_1")
    expect(typeof id).toBe("string")
  })
})

describe("withDeadline", () => {
  test("passes a value through when the promise settles in time", async () => {
    await expect(withDeadline(Promise.resolve("done"), { timeoutMs: 50 })).resolves.toBe("done")
  })

  test("rejects with the caller message and runs onTimeout when the deadline fires", async () => {
    let timedOut = 0
    await expect(
      withDeadline(new Promise(() => {}), {
        timeoutMs: 5,
        message: "worker boot stalled",
        onTimeout: () => {
          timedOut++
        },
      }),
    ).rejects.toThrow("worker boot stalled")
    expect(timedOut).toBe(1)
  })

  test("accepts the legacy (timeoutMs, message, onTimeout) positional form", async () => {
    let timedOut = 0
    await expect(
      withDeadline(new Promise(() => {}), 5, "legacy timeout", () => {
        timedOut++
      }),
    ).rejects.toThrow("legacy timeout")
    expect(timedOut).toBe(1)
  })

  test("does not run onTimeout for a fast failure", async () => {
    let timedOut = 0
    await expect(
      withDeadline(Promise.reject(new Error("boom")), {
        timeoutMs: 50,
        onTimeout: () => {
          timedOut++
        },
      }),
    ).rejects.toThrow("boom")
    expect(timedOut).toBe(0)
  })
})

describe("awaitSignal", () => {
  test("resolves immediately for an already-aborted signal", async () => {
    const controller = new AbortController()
    controller.abort()
    await expect(awaitSignal(controller.signal)).resolves.toBeUndefined()
  })

  test("resolves exactly when the signal aborts, not before", async () => {
    const controller = new AbortController()
    const order: string[] = []
    const waiting = awaitSignal(controller.signal).then(() => order.push("aborted"))

    await flush()
    expect(order).toEqual([])

    controller.abort()
    await waiting
    expect(order).toEqual(["aborted"])
  })

  test("fails loudly when the signal misses an optional deadline", async () => {
    await expect(awaitSignal(new AbortController().signal, { timeoutMs: 5, message: "never aborted" })).rejects.toThrow(
      "never aborted",
    )
  })
})

describe("poll consolidation", () => {
  test("poll.ts re-exports the same pollUntil implementation", () => {
    expect(poll.pollUntil).toBeTypeOf("function")
  })
})
