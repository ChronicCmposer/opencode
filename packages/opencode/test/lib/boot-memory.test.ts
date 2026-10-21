import { describe, expect, test } from "bun:test"
import { deriveWorkerCount, measureBootMemory, readRssBytes } from "./boot-memory"

describe("worker count derivation", () => {
  test("K = availableParallelism - 2, floor 1, single worker below 4 cores", () => {
    expect(deriveWorkerCount(1)).toBe(1)
    expect(deriveWorkerCount(3)).toBe(1)
    expect(deriveWorkerCount(4)).toBe(2)
    expect(deriveWorkerCount(8)).toBe(6)
  })

  test("fails loudly on an invalid core count", () => {
    expect(() => deriveWorkerCount(0)).toThrow(/core count/)
    expect(() => deriveWorkerCount(2.5)).toThrow(/core count/)
  })
})

describe("boot/memory measurement harness", () => {
  // Records raw numbers only. Per the repo convention there is deliberately NO
  // machine-dependent threshold assertion: we assert scenario completion and that
  // metrics were collected, never that boot ms or RSS falls under a bound.
  test("records per-worker boot ms and RSS at K and at K+2", async () => {
    const report = await measureBootMemory({ cores: 4 }) // K = 2, extra = 2 -> K+2 = 4

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
    expect(report.totalBootMs).toBeGreaterThan(0)
    expect(report.atK.maxBootMs).toBeGreaterThanOrEqual(report.atK.medianBootMs)
  })

  test("honors an explicit k override", async () => {
    const report = await measureBootMemory({ cores: 8, k: 1, extra: 1 })

    expect(report.k).toBe(1)
    expect(report.atK.samples).toHaveLength(1)
    expect(report.atKPlusExtra.samples).toHaveLength(2)
  })

  test("readRssBytes returns a positive byte count", () => {
    expect(readRssBytes()).toBeGreaterThan(0)
  })
})
