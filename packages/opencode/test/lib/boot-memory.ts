// Boot/memory measurement harness.
//
// Records how long each session worker takes to boot and the process RSS at K and
// at K+2 workers. These are MACHINE-DEPENDENT numbers: the harness only collects
// and reports them. It never asserts a boot-time or memory threshold — acceptance
// is scenario completion plus metric collection (repo convention).
//
// Bun workers are threads sharing one process address space, so RSS is process-wide;
// `reportedRssBytes` is an observed in-worker sample, `rssBytes` is the hub reading.

import path from "node:path"
import { measureBootLevels, type BootLevelSummary } from "./worker-boot"
import { createWorkerHarness } from "./worker-harness"

const DEFAULT_TIMEOUT_MS = 10000
const DEFAULT_EXTRA_WORKERS = 2

export interface BootSample {
  readonly workerId: number
  readonly bootMs: number
  /** In-worker `process.memoryUsage().rss` at readiness (process-wide). */
  readonly reportedRssBytes: number
}

/** One boot level: the shared summary shape over this harness's sample type. */
export type BootLevelReport = BootLevelSummary<BootSample>

export interface BootMemoryReport {
  readonly cores: number
  readonly k: number
  readonly extra: number
  readonly baselineRssBytes: number
  readonly atK: BootLevelReport
  readonly atKPlusExtra: BootLevelReport
  /**
   * Sum of the individual per-worker boot durations (boots run sequentially).
   * This is NOT wall-clock elapsed measurement time — the harness reads RSS
   * between levels — so it is named for what it sums.
   */
  readonly sumWorkerBootMs: number
}

export interface MeasureBootMemoryOptions {
  /** Injected core count; defaults to `os.availableParallelism()`. */
  readonly cores?: number
  /**
   * Baseline worker count. Defaults to one worker per session: v2 materializes
   * one worker per session lazily, so there is no pool-size derivation.
   */
  readonly k?: number
  /** Extra workers spawned beyond K. Defaults to 2 (i.e. K+2). */
  readonly extra?: number
  /** Worker entry point; defaults to the boot-worker fixture. */
  readonly entry?: string | URL
  readonly timeoutMs?: number
}

interface BootMessage {
  readonly type: string
  readonly workerId: number
  readonly rssBytes: number
}

export function readRssBytes(): number {
  const rss = process.memoryUsage().rss
  if (!Number.isFinite(rss) || rss <= 0) {
    throw new Error(`invalid RSS reading: ${rss}`)
  }
  return rss
}

export async function measureBootMemory(options: MeasureBootMemoryOptions = {}): Promise<BootMemoryReport> {
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS
  const harness = createWorkerHarness({ cores: options.cores, timeoutMs })
  const cores = harness.cores
  const k = options.k ?? 1
  const extra = options.extra ?? DEFAULT_EXTRA_WORKERS
  const entry = options.entry ?? path.join(import.meta.dir, "../fixture/boot-worker.ts")

  const bootWorker = async (): Promise<BootSample> => {
    const started = performance.now()
    const worker = harness.spawn<BootMessage>(entry)
    const ready = await worker.waitFor("ready", { timeoutMs })
    return { workerId: worker.id, bootMs: performance.now() - started, reportedRssBytes: ready.rssBytes }
  }

  try {
    const levels = await measureBootLevels({ k, extra, bootOne: bootWorker, readRss: readRssBytes })
    return {
      cores,
      k,
      extra,
      baselineRssBytes: levels.baselineRssBytes,
      atK: levels.atK,
      atKPlusExtra: levels.atKPlusExtra,
      sumWorkerBootMs: levels.atKPlusExtra.bootMs.reduce((sum, bootMs) => sum + bootMs, 0),
    }
  } finally {
    await harness.terminateAll()
  }
}
