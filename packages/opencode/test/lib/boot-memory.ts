// Boot/memory measurement harness (plan task 3.11).
//
// Records how long each session worker takes to boot and the process RSS at K and
// at K+2 workers. These are MACHINE-DEPENDENT numbers: the harness only collects
// and reports them. It never asserts a boot-time or memory threshold — acceptance
// is scenario completion plus metric collection (repo convention).
//
// Bun workers are threads sharing one process address space, so RSS is process-wide;
// `reportedRssBytes` is an observed in-worker sample, `rssBytes` is the hub reading.

import path from "node:path"
import { createWorkerHarness } from "./worker-harness"

const DEFAULT_TIMEOUT_MS = 10000
const DEFAULT_EXTRA_WORKERS = 2

export interface BootSample {
  readonly workerId: number
  readonly bootMs: number
  /** In-worker `process.memoryUsage().rss` at readiness (process-wide). */
  readonly reportedRssBytes: number
}

export interface BootLevelReport {
  readonly workers: number
  readonly samples: ReadonlyArray<BootSample>
  readonly bootMs: ReadonlyArray<number>
  readonly medianBootMs: number
  readonly maxBootMs: number
  /** Hub-side process RSS after this level was reached. */
  readonly rssBytes: number
}

export interface BootMemoryReport {
  readonly cores: number
  readonly k: number
  readonly extra: number
  readonly baselineRssBytes: number
  readonly atK: BootLevelReport
  readonly atKPlusExtra: BootLevelReport
  readonly totalBootMs: number
}

export interface MeasureBootMemoryOptions {
  /** Injected core count; defaults to `os.availableParallelism()`. */
  readonly cores?: number
  /** Baseline worker count; defaults to `deriveWorkerCount(cores)`. */
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

/** K = availableParallelism − 2, floor 1, single worker below 4 cores. */
export function deriveWorkerCount(cores: number): number {
  if (!Number.isInteger(cores) || cores < 1) {
    throw new Error(`invalid core count: ${cores} (expected a positive integer)`)
  }
  if (cores < 4) return 1
  return cores - 2
}

export function readRssBytes(): number {
  const rss = process.memoryUsage().rss
  if (!Number.isFinite(rss) || rss <= 0) {
    throw new Error(`invalid RSS reading: ${rss}`)
  }
  return rss
}

function median(values: ReadonlyArray<number>): number {
  if (values.length === 0) return 0
  const sorted = values.slice().sort((left, right) => left - right)
  const middle = Math.floor(sorted.length / 2)
  if (sorted.length % 2 === 1) return sorted[middle]!
  return (sorted[middle - 1]! + sorted[middle]!) / 2
}

function levelReport(workers: number, samples: ReadonlyArray<BootSample>, rssBytes: number): BootLevelReport {
  const bootMs = samples.map((sample) => sample.bootMs)
  return {
    workers,
    samples,
    bootMs,
    medianBootMs: median(bootMs),
    maxBootMs: bootMs.length === 0 ? 0 : Math.max(...bootMs),
    rssBytes,
  }
}

export async function measureBootMemory(options: MeasureBootMemoryOptions = {}): Promise<BootMemoryReport> {
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS
  const harness = createWorkerHarness({ cores: options.cores, timeoutMs })
  const cores = harness.cores
  const k = options.k ?? deriveWorkerCount(cores)
  const extra = options.extra ?? DEFAULT_EXTRA_WORKERS

  if (!Number.isInteger(k) || k < 0) throw new Error(`invalid worker count k: ${k} (expected a non-negative integer)`)
  if (!Number.isInteger(extra) || extra < 0) {
    throw new Error(`invalid extra worker count: ${extra} (expected a non-negative integer)`)
  }

  const entry = options.entry ?? path.join(import.meta.dir, "../fixture/boot-worker.ts")
  const baselineRssBytes = readRssBytes()

  const bootWorker = async (): Promise<BootSample> => {
    const started = performance.now()
    const worker = harness.spawn<BootMessage>(entry)
    const ready = await worker.waitFor("ready", { timeoutMs })
    return { workerId: worker.id, bootMs: performance.now() - started, reportedRssBytes: ready.rssBytes }
  }

  const bootMany = async (count: number): Promise<BootSample[]> => {
    const samples: BootSample[] = []
    for (let index = 0; index < count; index++) samples.push(await bootWorker())
    return samples
  }

  try {
    const atKSamples = await bootMany(k)
    const atKRssBytes = readRssBytes()
    const extraSamples = await bootMany(extra)
    const atKPlusExtraRssBytes = readRssBytes()

    const allSamples = [...atKSamples, ...extraSamples]
    return {
      cores,
      k,
      extra,
      baselineRssBytes,
      atK: levelReport(atKSamples.length, atKSamples, atKRssBytes),
      atKPlusExtra: levelReport(allSamples.length, allSamples, atKPlusExtraRssBytes),
      totalBootMs: allSamples.reduce((sum, sample) => sum + sample.bootMs, 0),
    }
  } finally {
    await harness.terminateAll()
  }
}
