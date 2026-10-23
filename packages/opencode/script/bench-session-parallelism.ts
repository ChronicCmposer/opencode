// Phase 13.2 — opt-in session-parallelism benchmark.
//
// Runs the mock-SSE session workload serially (pool of 1) and then across the
// oversubscribed K-worker pool (`K = resolveWorkerCount()`, i.e. `floor(cores × F)`
// with the default factor F = `DEFAULT_OVERSUBSCRIPTION_FACTOR` = 2), and RECORDS
// the throughput ratio, busy-thread count, hub loop-gap and RSS. Per repo
// convention there is NO machine-dependent CI threshold: the ≥1.5× target is
// reported informationally and the script always exits 0. Below 4 cores it SKIPS
// (a single-core host cannot show wall speedup).
//
// Usage (from `packages/opencode`):
//   bun run script/bench-session-parallelism.ts
//   OPENCODE_BENCH_SESSIONS=8 OPENCODE_BENCH_BURN=50000000 bun run script/bench-session-parallelism.ts
//
// No real model APIs: the provider is a self-contained in-process SSE mock.

import os from "node:os"
import { parsePositiveInt } from "@/util/parse"
import { DEFAULT_OVERSUBSCRIPTION_FACTOR, OVERSUBSCRIPTION_ENV, resolveWorkerCount } from "@/session/worker/pool"
import { runParallelSessions } from "../test/lib/session-parallelism"
import { DEFAULT_BURN_ITERATIONS } from "../test/lib/mock-sse-worker"
import { startMockProvider } from "../test/lib/mock-provider"
import { median } from "../test/lib/worker-boot"
import type { Leak } from "../test/lib/leak-detector"

const TARGET_SPEEDUP = 1.5
const MIN_CORES = 4
const DEFAULT_REPEATS = 3

function readPositiveInt(name: string, fallback: number): number {
  const raw = process.env[name]
  if (raw === undefined || raw.trim() === "") return fallback
  return parsePositiveInt(raw, name)
}

interface MeasureOptions {
  readonly baseUrl: string
  readonly sessions: number
  readonly poolSize: number
  readonly burn: number
  readonly repeats: number
}

/** The leak fields the bench reports; a subset of the harness's `LeakReport`. */
interface LeakSummary {
  readonly leaks: ReadonlyArray<Leak>
  readonly clean: boolean
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
  /** Leaks observed across every repeat; the bench reports rather than throws. */
  readonly leak: LeakSummary
}

async function measure(options: MeasureOptions): Promise<Measurement> {
  const { baseUrl, sessions, poolSize, burn, repeats } = options
  const walls: number[] = []
  let busyThreads = 0
  let hubLoopGapMs = 0
  let hubCpuMs: number | undefined
  let rssBytes = 0
  const leaks: Leak[] = []

  for (let run = 0; run < repeats; run++) {
    const report = await runParallelSessions({ sessions, poolSize, baseUrl, burnIterations: burn })
    walls.push(report.wallMs)
    busyThreads = Math.max(busyThreads, report.busyThreads)
    hubLoopGapMs = Math.max(hubLoopGapMs, report.hubLoopGap.maxGapMs)
    if (report.hubLoopGap.cpuMs !== undefined) hubCpuMs = Math.max(hubCpuMs ?? 0, report.hubLoopGap.cpuMs)
    rssBytes = Math.max(rssBytes, process.memoryUsage().rss)
    leaks.push(...report.leak.leaks)
  }

  const wallMs = median(walls)
  return {
    poolSize,
    wallMs,
    throughputPerSec: (sessions / wallMs) * 1000,
    busyThreads,
    hubLoopGapMs,
    hubCpuMs,
    rssBytes,
    runs: walls,
    leak: { leaks, clean: leaks.length === 0 },
  }
}

/** `null` when the denominator is zero, so the report never contains `NaN`/`Infinity`. */
const ratioOrNull = (numerator: number, denominator: number): number | null =>
  denominator === 0 ? null : numerator / denominator

async function main(): Promise<void> {
  const cores = os.availableParallelism()
  if (cores < MIN_CORES) {
    console.log(JSON.stringify({ skipped: true, reason: `requires >= ${MIN_CORES} cores`, cores }))
    return
  }

  // K is the canonical oversubscribed pool target: `floor(cores × F)`, default F = 2.
  const k = resolveWorkerCount()
  const sessions = readPositiveInt("OPENCODE_BENCH_SESSIONS", Math.max(4, k * 2))
  const burn = readPositiveInt("OPENCODE_BENCH_BURN", DEFAULT_BURN_ITERATIONS * 2)
  const repeats = readPositiveInt("OPENCODE_BENCH_REPEATS", DEFAULT_REPEATS)

  // Same streamed shape the retired `startMockSseServer` produced: 8 content deltas.
  const server = startMockProvider({ chunks: Array.from({ length: 8 }, (_, index) => `token${index} `) })
  try {
    const baseline = await measure({ baseUrl: server.baseUrl, sessions, poolSize: 1, burn, repeats })
    const parallel = await measure({ baseUrl: server.baseUrl, sessions, poolSize: k, burn, repeats })
    const speedup = ratioOrNull(baseline.wallMs, parallel.wallMs)
    const throughputGain = ratioOrNull(parallel.throughputPerSec, baseline.throughputPerSec)

    const report = {
      cores,
      k,
      oversubscriptionFactor: DEFAULT_OVERSUBSCRIPTION_FACTOR,
      oversubscriptionEnv: OVERSUBSCRIPTION_ENV,
      sessions,
      burnIterations: burn,
      repeats,
      baseline,
      parallel,
      speedup,
      throughputGain,
      target: TARGET_SPEEDUP,
      targetMet: speedup !== null && speedup >= TARGET_SPEEDUP,
      leakDetected: !baseline.leak.clean || !parallel.leak.clean,
      busyThreadsAtK: parallel.busyThreads,
    }
    console.log(JSON.stringify(report, null, 2))
  } finally {
    await server.stop()
  }
}

await main()
