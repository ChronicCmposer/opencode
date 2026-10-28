// Hub-side session-worker boot measurement (split from the former mock-sse-harness).
//
// Boot the REAL session-worker entry K times, then `extra` more, recording each
// worker's boot ms and the process RSS at each level. Bun workers are threads in
// one address space, so RSS is process-wide and recorded, not attributed.
//
// These are MACHINE-DEPENDENT numbers: the harness only collects and reports
// them, never asserting a boot-time threshold.

import { readRssBytes } from "./boot-memory"
import { MOCK_SSE_WORKER_ENTRY } from "./mock-sse-worker"
import { measureBootLevels, type BootLevelSummary } from "./worker-boot"
import { createWorkerHarness } from "./worker-harness"

/**
 * Boot waits are more tolerant than per-message waits: a cold worker pays the
 * full module-graph load, which can stretch past the per-message budget when the
 * whole test directory runs in parallel (flake isolation).
 */
const DEFAULT_BOOT_TIMEOUT_MS = 30_000
/** A worker that misses its per-attempt budget is retried on a fresh worker, bounded here. */
const BOOT_ATTEMPTS = 2
const DEFAULT_EXTRA_WORKERS = 2

export interface SessionBootSample {
  readonly workerId: number
  readonly bootMs: number
}

/** One boot level: the shared summary shape over the session-boot sample type. */
export type SessionBootLevel = BootLevelSummary<SessionBootSample>

export interface SessionBootReport {
  readonly cores: number
  readonly k: number
  readonly extra: number
  readonly baselineRssBytes: number
  readonly atK: SessionBootLevel
  readonly atKPlusExtra: SessionBootLevel
}

export interface MeasureSessionBootOptions {
  readonly cores?: number
  /** Baseline worker count; defaults to 1 (one worker per session). */
  readonly k?: number
  readonly extra?: number
  readonly entry?: string | URL
  readonly timeoutMs?: number
}

/**
 * Boot the REAL session-worker entry K times, then `extra` more, recording each
 * worker's boot ms and the process RSS at each level. Bun workers are threads in
 * one address space, so RSS is process-wide and recorded, not attributed.
 */
export async function measureSessionWorkerBoot(options: MeasureSessionBootOptions = {}): Promise<SessionBootReport> {
  const timeoutMs = options.timeoutMs ?? DEFAULT_BOOT_TIMEOUT_MS
  const harness = createWorkerHarness({ cores: options.cores, timeoutMs })
  const cores = harness.cores
  // One worker per session: no pool-size derivation. `k` is only a harness knob.
  const k = options.k ?? 1
  const extra = options.extra ?? DEFAULT_EXTRA_WORKERS
  const entry = options.entry ?? MOCK_SSE_WORKER_ENTRY

  const bootOne = async (): Promise<SessionBootSample> => {
    let lastError: unknown
    for (let attempt = 1; attempt <= BOOT_ATTEMPTS; attempt++) {
      const startedAt = performance.now()
      const handle = harness.spawn(entry)
      try {
        await handle.waitFor("ready", { timeoutMs })
        return { workerId: handle.id, bootMs: performance.now() - startedAt }
      } catch (error) {
        lastError = error
        await handle.terminate().catch(() => undefined)
      }
    }
    const reason = lastError instanceof Error ? lastError.message : String(lastError)
    throw new Error(`session worker did not report ready after ${BOOT_ATTEMPTS} attempts: ${reason}`)
  }

  try {
    const levels = await measureBootLevels({ k, extra, bootOne, readRss: readRssBytes })
    return {
      cores,
      k,
      extra,
      baselineRssBytes: levels.baselineRssBytes,
      atK: levels.atK,
      atKPlusExtra: levels.atKPlusExtra,
    }
  } finally {
    await harness.dispose()
  }
}
