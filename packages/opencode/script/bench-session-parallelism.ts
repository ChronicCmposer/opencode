// Phase 13.2 — opt-in session-parallelism benchmark.
//
// Runs the mock-SSE session workload serially (pool of 1) and then across a
// K-worker pool, and RECORDS the throughput ratio, busy-thread count, hub
// loop-gap and RSS. Per repo convention there is NO machine-dependent CI
// threshold: the ≥1.5× target is reported informationally and the script always
// exits 0. Below 4 cores it SKIPS (a single-core host cannot show wall speedup).
//
// Usage (from `packages/opencode`):
//   bun run script/bench-session-parallelism.ts
//   OPENCODE_BENCH_SESSIONS=8 OPENCODE_BENCH_BURN=50000000 bun run script/bench-session-parallelism.ts
//
// No real model APIs: the provider is a self-contained in-process SSE mock.

import os from "node:os"
import { DEFAULT_BURN_ITERATIONS, startMockSseServer, runParallelSessions } from "../test/lib/mock-sse-harness"

const TARGET_SPEEDUP = 1.5
const MIN_CORES = 4
const DEFAULT_REPEATS = 3

function readPositiveInt(name: string, fallback: number): number {
  const raw = process.env[name]
  if (raw === undefined || raw.trim() === "") return fallback
  const parsed = Number(raw)
  if (!Number.isInteger(parsed) || parsed < 1) {
    throw new Error(`invalid ${name}: ${raw} (expected a positive integer)`)
  }
  return parsed
}

function median(values: ReadonlyArray<number>): number {
  const sorted = values.slice().sort((left, right) => left - right)
  const middle = Math.floor(sorted.length / 2)
  return sorted.length % 2 === 1 ? sorted[middle]! : (sorted[middle - 1]! + sorted[middle]!) / 2
}

interface Measurement {
  readonly poolSize: number
  readonly wallMs: number
  readonly throughputPerSec: number
  readonly busyThreads: number
  readonly hubLoopGapMs: number
  readonly hubCpuMs: number | undefined
  readonly rssBytes: number
  readonly runs: ReadonlyArray<number>
}

async function measure(baseUrl: string, sessions: number, poolSize: number, burn: number, repeats: number): Promise<Measurement> {
  const walls: number[] = []
  let busyThreads = 0
  let hubLoopGapMs = 0
  let hubCpuMs: number | undefined
  let rssBytes = 0

  for (let run = 0; run < repeats; run++) {
    const report = await runParallelSessions({ sessions, poolSize, baseUrl, burnIterations: burn })
    walls.push(report.wallMs)
    busyThreads = Math.max(busyThreads, report.busyThreads)
    hubLoopGapMs = Math.max(hubLoopGapMs, report.hubLoopGap.maxGapMs)
    if (report.hubLoopGap.cpuMs !== undefined) hubCpuMs = Math.max(hubCpuMs ?? 0, report.hubLoopGap.cpuMs)
    rssBytes = Math.max(rssBytes, process.memoryUsage().rss)
    if (!report.leak.clean) {
      throw new Error(`leak detected after run ${run}: ${JSON.stringify(report.leak.leaks)}`)
    }
  }

  const wallMs = median(walls)
  return { poolSize, wallMs, throughputPerSec: (sessions / wallMs) * 1000, busyThreads, hubLoopGapMs, hubCpuMs, rssBytes, runs: walls }
}

async function main(): Promise<void> {
  const cores = os.availableParallelism()
  if (cores < MIN_CORES) {
    console.log(JSON.stringify({ skipped: true, reason: `requires >= ${MIN_CORES} cores`, cores }))
    return
  }

  const k = cores - 2
  const sessions = readPositiveInt("OPENCODE_BENCH_SESSIONS", Math.max(4, k * 2))
  const burn = readPositiveInt("OPENCODE_BENCH_BURN", DEFAULT_BURN_ITERATIONS * 2)
  const repeats = readPositiveInt("OPENCODE_BENCH_REPEATS", DEFAULT_REPEATS)

  const server = startMockSseServer()
  try {
    const baseline = await measure(server.url, sessions, 1, burn, repeats)
    const parallel = await measure(server.url, sessions, k, burn, repeats)
    const speedup = baseline.wallMs / parallel.wallMs
    const throughputGain = parallel.throughputPerSec / baseline.throughputPerSec

    const report = {
      cores,
      k,
      sessions,
      burnIterations: burn,
      repeats,
      baseline,
      parallel,
      speedup,
      throughputGain,
      target: TARGET_SPEEDUP,
      targetMet: speedup >= TARGET_SPEEDUP,
      busyThreadsAtK: parallel.busyThreads,
    }
    console.log(JSON.stringify(report, null, 2))
  } finally {
    server.close()
  }
}

await main()
