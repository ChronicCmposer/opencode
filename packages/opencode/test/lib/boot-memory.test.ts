import { describe, expect, test } from "bun:test"
import { deriveWorkerCount as canonicalDeriveWorkerCount } from "@/session/worker/pool"
import { deriveWorkerCount, measureBootMemory, readRssBytes } from "./boot-memory"

describe("worker count derivation", () => {
  // WS2: `boot-memory` no longer owns sizing. It re-exports the canonical pool
  // helper so the harness and production sizing can never drift apart.
  test("re-exports the canonical pool sizing helper", () => {
    expect(deriveWorkerCount).toBe(canonicalDeriveWorkerCount)
  })

  test("oversubscribes: floor(cores * factor), default factor 2", () => {
    expect(deriveWorkerCount(4)).toBe(8)
    expect(deriveWorkerCount(8)).toBe(16)
  })

  test("agrees with the canonical helper across core counts (incl. small machines)", () => {
    for (const cores of [1, 2, 3, 4, 6, 8, 16, 32]) {
      expect(deriveWorkerCount(cores)).toBe(canonicalDeriveWorkerCount(cores))
    }
  })

  test("fails loudly on an invalid core count", () => {
    expect(() => deriveWorkerCount(0)).toThrow(/availableParallelism/)
    expect(() => deriveWorkerCount(2.5)).toThrow(/availableParallelism/)
  })
})

describe("boot/memory measurement harness", () => {
  // Records raw numbers only. Per the repo convention there is deliberately NO
  // machine-dependent threshold assertion: we assert scenario completion and that
  // metrics were collected, never that boot ms or RSS falls under a bound.
  test("records per-worker boot ms and RSS at K and at K+2", async () => {
    // Explicit `k` keeps this test booting 4 isolates rather than the
    // oversubscribed default of `cores * 2` (which would be 8 here).
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

  test("readRssBytes returns a positive byte count", () => {
    expect(readRssBytes()).toBeGreaterThan(0)
  })
})
