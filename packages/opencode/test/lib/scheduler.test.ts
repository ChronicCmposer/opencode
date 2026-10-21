import { describe, expect, test } from "bun:test"
import { createRecordingScheduler, type SchedulerFn } from "./scheduler"

describe("createRecordingScheduler", () => {
  test("records timers instead of arming real ones", () => {
    const scheduler = createRecordingScheduler()
    scheduler.schedule(5000, () => {})

    expect(scheduler.timers).toHaveLength(1)
    expect(scheduler.timers[0]?.delayMs).toBe(5000)
    expect(scheduler.pending()).toHaveLength(1)
  })

  test("fire runs every pending timer in schedule order", () => {
    const scheduler = createRecordingScheduler()
    const order: number[] = []
    scheduler.schedule(10, () => order.push(10))
    scheduler.schedule(5, () => order.push(5))

    const ran = scheduler.fire()

    expect(ran).toBe(2)
    expect(order).toEqual([10, 5])
    expect(scheduler.pending()).toHaveLength(0)
  })

  test("a cancel handle marks a timer cancelled so it never fires", () => {
    const scheduler = createRecordingScheduler()
    let fired = false
    const cancel = scheduler.schedule(100, () => {
      fired = true
    })

    cancel()
    const ran = scheduler.fire()

    expect(ran).toBe(0)
    expect(fired).toBe(false)
    expect(scheduler.pending()).toHaveLength(0)
  })

  test("fireDue only runs timers at or below the given delay", () => {
    const scheduler = createRecordingScheduler()
    const order: number[] = []
    scheduler.schedule(100, () => order.push(100))
    scheduler.schedule(200, () => order.push(200))

    const ran = scheduler.fireDue(150)

    expect(ran).toBe(1)
    expect(order).toEqual([100])
    expect(scheduler.pending()).toHaveLength(1)
    expect(scheduler.pending()[0]?.delayMs).toBe(200)
  })

  test("cancelAll clears every pending timer", () => {
    const scheduler = createRecordingScheduler()
    scheduler.schedule(1, () => {})
    scheduler.schedule(2, () => {})

    scheduler.cancelAll()

    expect(scheduler.pending()).toEqual([])
    expect(scheduler.fire()).toBe(0)
  })

  test("can be specialised to a named schedule function type", () => {
    type CancelScheduler = (delayMs: number, run: () => void) => () => void
    const scheduler = createRecordingScheduler<CancelScheduler>()
    const cancel: () => void = scheduler.schedule(1, () => {})
    expect(typeof cancel).toBe("function")

    const generic: SchedulerFn = scheduler.schedule
    expect(typeof generic).toBe("function")
  })
})
