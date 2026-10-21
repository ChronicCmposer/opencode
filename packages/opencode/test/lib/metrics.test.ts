import { describe, expect, test } from "bun:test"
import { createMetrics } from "./metrics"

describe("metrics collector", () => {
  test("counters accumulate from a zero default", () => {
    const metrics = createMetrics()

    expect(metrics.counter("hub.dispatch")).toBe(0)
    expect(metrics.increment("hub.dispatch")).toBe(1)
    expect(metrics.increment("hub.dispatch", 4)).toBe(5)
    expect(metrics.counter("hub.dispatch")).toBe(5)
  })

  test("gauges hold the last value", () => {
    const metrics = createMetrics()

    expect(metrics.gaugeValue("hub.queue_depth")).toBeUndefined()
    metrics.gauge("hub.queue_depth", 3)
    metrics.gauge("hub.queue_depth", 7)

    expect(metrics.gaugeValue("hub.queue_depth")).toBe(7)
  })

  test("snapshot is a frozen, isolated copy", () => {
    const metrics = createMetrics()
    metrics.increment("a")
    metrics.gauge("b", 1)

    const snapshot = metrics.snapshot()
    metrics.increment("a")
    metrics.gauge("b", 2)

    expect(snapshot.counters).toEqual({ a: 1 })
    expect(snapshot.gauges).toEqual({ b: 1 })
    expect(Object.isFrozen(snapshot)).toBe(true)
    expect(Object.isFrozen(snapshot.counters)).toBe(true)
    expect(Object.isFrozen(snapshot.gauges)).toBe(true)
  })

  test("fails loudly on an invalid name or value", () => {
    const metrics = createMetrics()

    expect(() => metrics.increment("")).toThrow(/metric name/)
    expect(() => metrics.increment("a", Number.NaN)).toThrow(/finite/)
    expect(() => metrics.increment("a", 0)).toThrow(/positive/)
    expect(() => metrics.gauge("a", Number.POSITIVE_INFINITY)).toThrow(/finite/)
    expect(() => metrics.counter("")).toThrow(/metric name/)
    expect(() => metrics.gaugeValue("")).toThrow(/metric name/)
  })

  test("reset clears counters and gauges", () => {
    const metrics = createMetrics()
    metrics.increment("a")
    metrics.gauge("b", 2)

    metrics.reset()

    expect(metrics.counter("a")).toBe(0)
    expect(metrics.gaugeValue("b")).toBeUndefined()
    expect(metrics.snapshot()).toEqual({ counters: {}, gauges: {} })
  })
})
