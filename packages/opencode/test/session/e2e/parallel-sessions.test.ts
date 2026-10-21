// Phase 13.1/13.3/13.5/13.6 — end-to-end proof over the mock-SSE session pool.
//
// Contract under test: the mock-SSE harness drives N concurrent sessions through
// the REAL `createDispatcher` pool, each session fetching a streamed reply from
// the hub's mock provider and burning CPU (the simulated per-delta assembly cost).
// A 1-worker pool therefore pins exactly one hot thread; a K-worker pool spreads
// the same work over ≥2 threads. All timing/RSS numbers are RECORDED, never
// asserted against a machine-dependent threshold (repo convention).
//
// 13.4 (full `bun test` flag off/on) and the K-session admission/shutdown paths are
// integration-gated on Phase 9/12; see `.parallel-implement/phase13-e2e.md`.

import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { testEffect } from "../../lib/effect"
import { TestLLMServer } from "../../lib/llm-server"
import { measureSessionWorkerBoot, runParallelSessions } from "../../lib/mock-sse-harness"

const it = testEffect(TestLLMServer.layer)

const CORES = 4
const SESSIONS = 4
const BURN_ITERATIONS = 20_000_000

/** The POSIX per-thread CPU reader (and thus busy-thread accounting) is available. */
const hasThreadCpu = (source: string): boolean => source === "proc"

describe("13.1 mock-SSE harness drives N concurrent sessions through the pool", () => {
  it.live("a 1-worker pool runs every session on one hot thread", () =>
    Effect.gen(function* () {
      const llm = yield* TestLLMServer
      const report = yield* Effect.promise(() =>
        runParallelSessions({
          sessions: SESSIONS,
          poolSize: 1,
          baseUrl: llm.url,
          cores: CORES,
          burnIterations: BURN_ITERATIONS,
        }),
      )

      expect(report.results).toHaveLength(SESSIONS)
      for (const result of report.results) expect(result.value).toMatchObject({ text: "ok" })
      expect(report.wallMs).toBeGreaterThan(0)
      expect(report.throughputPerSec).toBeGreaterThan(0)

      // Structural baseline: exactly one worker thread accrued CPU.
      if (hasThreadCpu(report.hubLoopGap.source)) expect(report.busyThreads).toBe(1)
    }),
  )

  it.live("a K-worker pool spreads the same sessions over at least two busy threads", () =>
    Effect.gen(function* () {
      const llm = yield* TestLLMServer
      const report = yield* Effect.promise(() =>
        runParallelSessions({
          sessions: SESSIONS,
          poolSize: 2,
          baseUrl: llm.url,
          cores: CORES,
          burnIterations: BURN_ITERATIONS,
        }),
      )

      expect(report.results).toHaveLength(SESSIONS)
      for (const result of report.results) expect(result.value).toMatchObject({ text: "ok" })
      if (hasThreadCpu(report.hubLoopGap.source)) expect(report.busyThreads).toBeGreaterThanOrEqual(2)
    }),
  )
})

describe("13.3 hub load under K sessions is recorded (no threshold)", () => {
  it.live("records the hub loop-gap and metrics while K sessions run", () =>
    Effect.gen(function* () {
      const llm = yield* TestLLMServer
      const report = yield* Effect.promise(() =>
        runParallelSessions({
          sessions: SESSIONS,
          poolSize: 2,
          baseUrl: llm.url,
          cores: CORES,
          burnIterations: BURN_ITERATIONS,
        }),
      )

      expect(report.hubLoopGap.ticks).toBeGreaterThanOrEqual(1)
      expect(Number.isFinite(report.hubLoopGap.maxGapMs)).toBe(true)
      expect(Number.isFinite(report.hubLoopGap.meanGapMs)).toBe(true)
      expect(report.metrics.gauges["hub.loop_gap.max_ms"]).toBeGreaterThanOrEqual(0)
      expect(report.metrics.gauges["hub.loop_gap.mean_ms"]).toBeGreaterThanOrEqual(0)
      expect(report.metrics.gauges["parallel.sessions"]).toBe(SESSIONS)
      expect(report.metrics.gauges["parallel.pool_size"]).toBe(2)
      expect(report.metrics.gauges["parallel.busy_threads"]).toBe(report.busyThreads)
    }),
  )
})

describe("13.5 leak detector finds no leaked workers/ports/children", () => {
  it.live("teardown leaves zero live workers, ports and process groups", () =>
    Effect.gen(function* () {
      const llm = yield* TestLLMServer
      const report = yield* Effect.promise(() =>
        runParallelSessions({
          sessions: 2,
          poolSize: 1,
          baseUrl: llm.url,
          cores: CORES,
          burnIterations: BURN_ITERATIONS,
        }),
      )

      expect(report.leak.clean).toBe(true)
      expect(report.leak.leaks).toEqual([])
      expect(report.leak.workers).toEqual([])
      expect(report.leak.ports).toEqual([])
      expect(report.leak.processGroups).toEqual([])
    }),
  )
})

describe("13.6 per-worker boot ms + RSS at K and K+2 are recorded", () => {
  // Explicit generous budget: four sequential boot measurements, each with a
  // bounded retry, must not race the default per-test timeout when the whole test
  // directory runs in parallel. Boot is a one-time cost; the fast path stays ~3s.
  test("records boot timing for the real session-worker entry at K and K+2", async () => {
    const report = await measureSessionWorkerBoot({ cores: 4 }) // K = 2, extra = 2 -> K+2 = 4

    expect(report.k).toBe(2)
    expect(report.extra).toBe(2)
    expect(report.atK.samples).toHaveLength(2)
    expect(report.atKPlusExtra.samples).toHaveLength(4)
    for (const sample of report.atKPlusExtra.samples) {
      expect(sample.workerId).toBeGreaterThan(0)
      expect(Number.isFinite(sample.bootMs)).toBe(true)
      expect(sample.bootMs).toBeGreaterThan(0)
    }
    expect(report.baselineRssBytes).toBeGreaterThan(0)
    expect(report.atK.rssBytes).toBeGreaterThan(0)
    expect(report.atKPlusExtra.rssBytes).toBeGreaterThan(0)
    expect(report.atK.maxBootMs).toBeGreaterThanOrEqual(report.atK.medianBootMs)
  }, 300_000)
})
