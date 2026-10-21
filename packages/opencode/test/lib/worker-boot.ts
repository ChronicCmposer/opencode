// Shared boot-measurement helpers (plan consolidation).
//
// The 3.11 boot/RSS harness (`boot-memory.ts`) and the 13.6 session-worker boot
// harness (`mock-sse-harness.ts`) both summarise a level of boot timings the same
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
