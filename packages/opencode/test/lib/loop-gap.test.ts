import { describe, expect, test } from "bun:test"
import path from "node:path"
import { createMetrics } from "./metrics"
import {
  LINUX_CLOCK_TICKS_PER_SECOND,
  type LoopGapSample,
  createLoopGapProbe,
  isPosix,
  measureLoopGap,
  readThreadCpuTicks,
  recordLoopGap,
} from "./loop-gap"
import { createWorkerHarness } from "./worker-harness"

describe("POSIX thread CPU reader", () => {
  test("isPosix matches the platform", () => {
    expect(isPosix()).toBe(process.platform !== "win32")
  })

  // POSIX-only: /proc/thread-self/stat is a Linux facility. On Windows it does not
  // exist, so this test is skipped there with a documented reason (plan: Windows
  // verification is out of scope, `skipIf(win32)`).
  test.skipIf(process.platform === "win32")("reads the calling thread's CPU ticks from /proc", () => {
    const reading = readThreadCpuTicks()

    expect(reading).toBeDefined()
    if (!reading) return
    expect(reading.threadId).toBeGreaterThan(0)
    expect(reading.userTicks).toBeGreaterThanOrEqual(0)
    expect(reading.systemTicks).toBeGreaterThanOrEqual(0)
    expect(reading.totalTicks).toBe(reading.userTicks + reading.systemTicks)
    expect(LINUX_CLOCK_TICKS_PER_SECOND).toBeGreaterThan(0)
  })

  test("returns undefined for an impossible thread id", () => {
    expect(readThreadCpuTicks(-1)).toBeUndefined()
    expect(readThreadCpuTicks(Number.NaN)).toBeUndefined()
  })
})

describe("event-loop-gap probe", () => {
  // Records the maximum timer-drift gap only. Per the repo convention there is
  // deliberately NO machine-dependent threshold: we assert the sample was collected
  // and is well-formed, never that the gap falls under a bound.
  test("collects timer-drift ticks over a window", async () => {
    const sample = await measureLoopGap({ windowMs: 60, intervalMs: 10 })

    expect(sample.windowMs).toBeGreaterThan(0)
    expect(sample.intervalMs).toBe(10)
    expect(sample.ticks).toBeGreaterThanOrEqual(1)
    expect(Number.isFinite(sample.maxGapMs)).toBe(true)
    expect(sample.maxGapMs).toBeGreaterThanOrEqual(0)
    expect(Number.isFinite(sample.meanGapMs)).toBe(true)
    expect(sample.meanGapMs).toBeGreaterThanOrEqual(0)
    expect(["proc", "timer"]).toContain(sample.source)

    if (sample.source === "proc") {
      expect(sample.threadId).toBeGreaterThan(0)
      expect(sample.cpuMs).toBeGreaterThanOrEqual(0)
    }
  })

  test("probe accumulates live and stop is idempotent", async () => {
    const probe = createLoopGapProbe({ intervalMs: 5 })
    expect(probe.running).toBe(true)

    await new Promise((resolve) => setTimeout(resolve, 30))

    const live = probe.sample()
    expect(live.ticks).toBeGreaterThanOrEqual(1)

    const stopped = probe.stop()
    expect(probe.running).toBe(false)
    expect(stopped.ticks).toBeGreaterThanOrEqual(live.ticks)
    expect(probe.stop().ticks).toBe(stopped.ticks)
  })

  test("fails loudly on a non-positive interval or window", () => {
    expect(() => createLoopGapProbe({ intervalMs: 0 })).toThrow(/intervalMs/)
    expect(() => createLoopGapProbe({ intervalMs: -1 })).toThrow(/intervalMs/)
    expect(() => measureLoopGap({ windowMs: 0 })).toThrow(/windowMs/)
  })

  test("collects a sample on a worker thread", async () => {
    await using harness = createWorkerHarness({ cores: 1, timeoutMs: 10000 })
    const entry = path.join(import.meta.dir, "../fixture/loop-gap-worker.ts")
    const worker = harness.spawn<{ type: string; sample: LoopGapSample }>(entry)

    const message = await worker.waitFor("sample")

    expect(message.sample.ticks).toBeGreaterThanOrEqual(1)
    expect(Number.isFinite(message.sample.maxGapMs)).toBe(true)
    expect(["proc", "timer"]).toContain(message.sample.source)
    if (message.sample.source === "proc") {
      expect(message.sample.threadId).toBeGreaterThan(0)
      expect(message.sample.cpuMs).toBeGreaterThanOrEqual(0)
    }
  })
})

describe("metrics + loop-gap scenario", () => {
  // The Phase 13.3 acceptance gate: record hub metrics while collecting a loop-gap
  // sample. Asserts scenario completion and collection only — never a timing bound.
  test("records hub metrics alongside a loop-gap sample", async () => {
    const metrics = createMetrics()
    const probe = createLoopGapProbe({ intervalMs: 5 })

    // Simulate a hub loop doing I/O-only work: bump a per-tick counter.
    const workload = setInterval(() => metrics.increment("hub.io_ticks"), 5)
    const sample = await measureLoopGap({ windowMs: 50, intervalMs: 5 })
    clearInterval(workload)
    probe.stop()

    recordLoopGap(metrics, sample)

    const snapshot = metrics.snapshot()
    expect(snapshot.counters["hub.io_ticks"]).toBeGreaterThan(0)
    expect(snapshot.counters["hub.loop_gap.ticks"]).toBeGreaterThanOrEqual(1)
    expect(snapshot.gauges["hub.loop_gap.max_ms"]).toBeGreaterThanOrEqual(0)
    expect(snapshot.gauges["hub.loop_gap.mean_ms"]).toBeGreaterThanOrEqual(0)
  })
})
