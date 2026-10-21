import { describe, expect, test } from "bun:test"
import { capture, flush, sid } from "./async"

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

  test("sid brands a plain string for the session boundary", () => {
    const id = sid("ses_1")
    expect(id as string).toBe("ses_1")
    expect(typeof id).toBe("string")
  })
})
