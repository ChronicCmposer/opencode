// Shared boot-measurement helpers.
//
// The 3.11 boot/RSS harness (`boot-memory.ts`) and the 13.6 session-worker boot
// harness (`session-boot.ts`) both summarise a level of boot timings the same
// way: a median, a max and the process RSS observed once the level was reached.
// Those pure aggregations live here so both harnesses stay in lockstep.
//
// These are MACHINE-DEPENDENT numbers: the helpers only compute, they never
// assert a boot-time or memory threshold.

/** Median of a numeric sample; `0` for an empty sample. Does not mutate input. */
export function median(values: ReadonlyArray<number>): number {
  if (values.length === 0) return 0
  const sorted = values.slice().sort((left, right) => left - right)
  const middle = Math.floor(sorted.length / 2)
  if (sorted.length % 2 === 1) return sorted[middle]!
  return (sorted[middle - 1]! + sorted[middle]!) / 2
}

/** Maximum of a numeric sample; `0` for an empty sample. */
export function maxOf(values: ReadonlyArray<number>): number {
  if (values.length === 0) return 0
  return Math.max(...values)
}

/** Common shape of one boot level; both harness sample types are `{ bootMs }`. */
export interface BootLevelSummary<TSample extends { readonly bootMs: number }> {
  readonly workers: number
  readonly samples: ReadonlyArray<TSample>
  readonly bootMs: ReadonlyArray<number>
  readonly medianBootMs: number
  readonly maxBootMs: number
  readonly rssBytes: number
}

/** Summarise one boot level from its samples and the RSS read at that level. */
export function summarizeBootLevel<TSample extends { readonly bootMs: number }>(
  workers: number,
  samples: ReadonlyArray<TSample>,
  rssBytes: number,
): BootLevelSummary<TSample> {
  const bootMs = samples.map((sample) => sample.bootMs)
  return {
    workers,
    samples,
    bootMs,
    medianBootMs: median(bootMs),
    maxBootMs: maxOf(bootMs),
    rssBytes,
  }
}

/** Boot `count` workers sequentially, collecting each sample in spawn order. */
export async function bootMany<TSample>(count: number, bootOne: () => Promise<TSample>): Promise<TSample[]> {
  const samples: TSample[] = []
  for (let index = 0; index < count; index++) samples.push(await bootOne())
  return samples
}

export interface MeasureBootLevelsOptions<TSample extends { readonly bootMs: number }> {
  /** Baseline worker count. */
  readonly k: number
  /** Extra workers spawned beyond K. */
  readonly extra: number
  /** Boot one worker and return its sample. */
  readonly bootOne: () => Promise<TSample>
  /** Read process RSS after a level is reached. */
  readonly readRss: () => number
}

/** The two boot levels every measurement harness records: K and K+extra. */
export interface BootLevels<TSample extends { readonly bootMs: number }> {
  /** Process RSS before any worker booted. */
  readonly baselineRssBytes: number
  readonly atK: BootLevelSummary<TSample>
  readonly atKPlusExtra: BootLevelSummary<TSample>
}

/**
 * The shared boot-measurement loop: boot K workers, read RSS, boot `extra` more,
 * read RSS, and summarise each level. Both the boot/RSS harness and the
 * session-worker boot harness measure the same levels; only `bootOne` (and the
 * RSS reader) differ, so the loop lives once here.
 */
export async function measureBootLevels<TSample extends { readonly bootMs: number }>(
  options: MeasureBootLevelsOptions<TSample>,
): Promise<BootLevels<TSample>> {
  const { k, extra, bootOne, readRss } = options
  if (!Number.isInteger(k) || k < 0) throw new Error(`invalid worker count k: ${k} (expected a non-negative integer)`)
  if (!Number.isInteger(extra) || extra < 0) {
    throw new Error(`invalid extra worker count: ${extra} (expected a non-negative integer)`)
  }

  const baselineRssBytes = readRss()
  const atKSamples = await bootMany(k, bootOne)
  const atKRssBytes = readRss()
  const extraSamples = await bootMany(extra, bootOne)
  const atKPlusExtraRssBytes = readRss()
  const allSamples = [...atKSamples, ...extraSamples]

  return {
    baselineRssBytes,
    atK: summarizeBootLevel(atKSamples.length, atKSamples, atKRssBytes),
    atKPlusExtra: summarizeBootLevel(allSamples.length, allSamples, atKPlusExtraRssBytes),
  }
}
