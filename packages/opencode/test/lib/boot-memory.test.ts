import { describe, expect, test } from "bun:test"
import { measureBootMemory, readRssBytes } from "./boot-memory"

describe("boot/memory measurement harness", () => {
  // Records raw numbers only. Per the repo convention there is deliberately NO
  // machine-dependent threshold assertion: we assert scenario completion and that
  // metrics were collected, never that boot ms or RSS falls under a bound.
  test("records per-worker boot ms and RSS at K and at K+2", async () => {
    // Explicit `k` keeps this test booting 4 isolates; omitting it would boot
    // the per-session default of one baseline worker plus two extra.
    const report = await measureBootMemory({ cores: 4, k: 2, extra: 2 }) // K = 2, K+2 = 4

    expect(report.k).toBe(2)
    expect(report.extra).toBe(2)
    expect(report.atK.workers).toBe(2)
    expect(report.atK.samples).toHaveLength(2)
    expect(report.atKPlusExtra.workers).toBe(4)
    expect(report.atKPlusExtra.samples).toHaveLength(4)

    for (const sample of report.atKPlusExtra.samples) {
      expect(sample.workerId).toBeGreaterThan(0)
      expect(Number.isFinite(sample.bootMs)).toBe(true)
      expect(sample.bootMs).toBeGreaterThan(0)
    }

    expect(report.atK.rssBytes).toBeGreaterThan(0)
    expect(report.atKPlusExtra.rssBytes).toBeGreaterThan(0)
    expect(report.baselineRssBytes).toBeGreaterThan(0)
    expect(report.sumWorkerBootMs).toBeGreaterThan(0)
    expect(report.sumWorkerBootMs).toBe(
      report.atKPlusExtra.samples.reduce((sum, sample) => sum + sample.bootMs, 0),
    )
    expect(report.atK.maxBootMs).toBeGreaterThanOrEqual(report.atK.medianBootMs)
  })

  test("honors an explicit k override", async () => {
    const report = await measureBootMemory({ cores: 8, k: 1, extra: 1 })

    expect(report.k).toBe(1)
    expect(report.atK.samples).toHaveLength(1)
    expect(report.atKPlusExtra.samples).toHaveLength(2)
  })

  test("defaults to one worker per session when k is omitted", async () => {
    const report = await measureBootMemory({ cores: 4, extra: 1 })

    expect(report.k).toBe(1)
    expect(report.atK.samples).toHaveLength(1)
    expect(report.atKPlusExtra.samples).toHaveLength(2)
  })

  test("readRssBytes returns a positive byte count", () => {
    expect(readRssBytes()).toBeGreaterThan(0)
  })
})
