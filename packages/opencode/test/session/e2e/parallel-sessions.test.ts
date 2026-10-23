// Phase 13.1/13.3/13.5/13.6 — end-to-end proof over the mock-SSE session pool.
//
// Contract under test: the mock-SSE harness drives N concurrent sessions through
// the REAL `createDispatcher` pool, each session fetching a streamed reply from
// the hub's mock provider and burning CPU (the simulated per-delta assembly cost).
// A 1-worker pool therefore pins exactly one hot thread; a K-worker pool spreads
// the same work over ≥2 threads. An oversubscribed pool (Option B) keeps more
// threads genuinely busy than the legacy `nproc − 2` ceiling allowed. All
// timing/RSS numbers are RECORDED, never asserted against a machine-dependent
// threshold (repo convention).
//
// 13.4 (full `bun test` flag off/on) and the K-session admission/shutdown paths are
// integration-gated on Phase 9/12; see `.parallel-implement/phase13-e2e.md`.

import { describe, expect, test } from "bun:test"
import { existsSync } from "node:fs"
import { Effect } from "effect"
import { testEffect } from "../../lib/effect"
import { TestLLMServer } from "../../lib/llm-server"
import { DEFAULT_BURN_ITERATIONS, measureSessionWorkerBoot, runParallelSessions } from "../../lib/mock-sse-harness"

const it = testEffect(TestLLMServer.layer)

const CORES = 4
const SESSIONS = 4
const BURN_ITERATIONS = DEFAULT_BURN_ITERATIONS

/**
 * Option B oversubscription: on a 4-core host the legacy `nproc − 2` sizing
 * capped the pool at 2 workers. A pool of 6 (with the sessions to fill it) must
 * therefore keep more than 2 threads busy at once.
 */
const OVERSUBSCRIBED_CORES = 4
const OVERSUBSCRIBED_POOL_SIZE = 6
const OVERSUBSCRIBED_SESSIONS = 6
const LEGACY_BUSY_THREAD_CEILING = 2

/**
 * The per-thread CPU reader (and therefore busy-thread accounting) needs the
 * Linux `/proc` thread-stat files. When they are absent the CPU-law tests are
 * skipped loudly instead of passing vacuously.
 */
const procCpuAvailable = existsSync("/proc/thread-self") || existsSync("/proc/self/task")
const cpuIt = procCpuAvailable ? it.live : it.live.skip

describe("13.1 mock-SSE harness drives N concurrent sessions through the pool", () => {
  cpuIt("a 1-worker pool runs every session on one hot thread", () =>
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
      expect(report.busyThreads).toBe(1)
    }),
  )

  cpuIt("a K-worker pool spreads the same sessions over at least two busy threads", () =>
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
      expect(report.busyThreads).toBeGreaterThanOrEqual(2)
    }),
  )

  cpuIt("an oversubscribed K=6 pool on 4 cores keeps work beyond the legacy nproc-2 ceiling", () =>
    Effect.gen(function* () {
      const llm = yield* TestLLMServer
      const report = yield* Effect.promise(() =>
        runParallelSessions({
          sessions: OVERSUBSCRIBED_SESSIONS,
          poolSize: OVERSUBSCRIBED_POOL_SIZE,
          baseUrl: llm.url,
          cores: OVERSUBSCRIBED_CORES,
          burnIterations: BURN_ITERATIONS,
        }),
      )

      expect(report.cores).toBe(OVERSUBSCRIBED_CORES)
      expect(report.poolSize).toBe(OVERSUBSCRIBED_POOL_SIZE)
      expect(report.results).toHaveLength(OVERSUBSCRIBED_SESSIONS)
      for (const result of report.results) expect(result.value).toMatchObject({ text: "ok" })

      // The legacy `nproc − 2` pool capped a 4-core host at 2 workers; the
      // oversubscribed pool keeps strictly more threads than that ceiling busy.
      expect(report.poolSize).toBeGreaterThan(LEGACY_BUSY_THREAD_CEILING)
      expect(report.busyThreads).toBeGreaterThan(LEGACY_BUSY_THREAD_CEILING)
    }),
    // Six live session isolates boot and burn concurrently; under host load this
    // can outrun the 5s default, so give it the same generous budget as 13.6.
    30_000,
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
    // Pin K=2 explicitly: the Option B default for 4 cores is 8, so relying on the
    // implicit default would boot 8 (then 10) isolates here.
    const report = await measureSessionWorkerBoot({ cores: 4, k: 2 }) // K = 2, extra = 2 -> K+2 = 4

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
