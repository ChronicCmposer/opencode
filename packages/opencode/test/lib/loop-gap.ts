// Event-loop-gap probe.
//
// Measures the maximum event-loop stall on a thread over a window, backing the
// Hub-load budget and the "main loop idle" / "hub loop I/O-only"
// observability requirements.
//
// The gap itself is measured by TIMER DRIFT: a repeating timer at `intervalMs`;
// each tick's lateness (`now - lastTickAt`) is the loop's stall, and `maxGapMs`
// is the largest observed lateness. This is portable and is the same technique
// used by the parallelism spike (`spikes/multicore/parallelism-check.ts`).
//
// On Linux the probe additionally reads the calling thread's CPU time from
// `/proc/thread-self/stat` (an alias for `/proc/self/task/<tid>/stat`) before and
// after the window. That CPU delta is the HUB-LOAD signal: a busy hub loop
// accrues CPU, an I/O-only loop does not. The reader is POSIX-only; on Windows it
// returns `undefined` with a documented reason and the probe falls back to
// `source: "timer"` (Windows verification is out of scope).
//
// This harness RECORDS raw numbers. It never asserts a machine-dependent timing
// threshold — acceptance is scenario completion plus sample collection.

import { readFileSync, readlinkSync } from "node:fs"
import type { Metrics } from "./metrics"

/** Linux `CLK_TCK` (USER_HZ); the per-thread stat file's `utime`/`stime` are in these ticks. */
export const LINUX_CLOCK_TICKS_PER_SECOND = 100

const THREAD_SELF_STAT = "/proc/thread-self/stat"
const THREAD_SELF_LINK = "/proc/thread-self"
const DEFAULT_TASK_DIR = "/proc/self/task"
const DEFAULT_INTERVAL_MS = 10
const DEFAULT_WINDOW_MS = 100

export interface ThreadCpuTicks {
  readonly threadId: number
  readonly userTicks: number
  readonly systemTicks: number
  readonly totalTicks: number
}

export interface ReadThreadCpuOptions {
  /** Override the task directory (defaults to `/proc/self/task`). */
  readonly taskDir?: string
}

export type LoopGapSource = "proc" | "timer"

export interface LoopGapSample {
  /** OS thread id; `undefined` when the POSIX reader was unavailable. */
  readonly threadId: number | undefined
  readonly windowMs: number
  readonly intervalMs: number
  readonly ticks: number
  readonly maxGapMs: number
  readonly meanGapMs: number
  /** Thread CPU consumed during the window; `undefined` without `/proc`. */
  readonly cpuMs: number | undefined
  /** `"proc"` when the Linux reader participated, else `"timer"`. */
  readonly source: LoopGapSource
}

export interface LoopGapProbeOptions {
  /** Timer period in ms. Defaults to 10. */
  readonly intervalMs?: number
  /** Explicit OS thread id; defaults to the calling thread. */
  readonly threadId?: number
  /** Injectable clock (ms). Defaults to `performance.now`. */
  readonly clock?: () => number
  /** Override the task directory (defaults to `/proc/self/task`). */
  readonly taskDir?: string
}

export interface MeasureLoopGapOptions extends LoopGapProbeOptions {
  /** Measurement window in ms. Defaults to 100. */
  readonly windowMs?: number
}

export interface LoopGapProbe {
  readonly threadId: number | undefined
  readonly source: LoopGapSource
  readonly running: boolean
  /** Largest timer-drift gap observed so far, in ms. */
  maxGapMs(): number
  /** Live cumulative sample (does not stop the probe). */
  sample(): LoopGapSample
  /** Stop the probe and return the final, frozen sample (idempotent). */
  stop(): LoopGapSample
}

/** POSIX platforms are everything except Windows (single source: `process-tree`). */
export { isPosix } from "./process-tree"

/** OS thread id of the calling thread, or `undefined` when `/proc` is unavailable. */
export function currentThreadId(): number | undefined {
  try {
    const link = readlinkSync(THREAD_SELF_LINK)
    const tid = Number(link.slice(link.lastIndexOf("/") + 1))
    return Number.isInteger(tid) && tid > 0 ? tid : undefined
  } catch {
    return undefined
  }
}

function parseThreadStat(statText: string): { userTicks: number; systemTicks: number } | undefined {
  // `comm` may contain spaces and parentheses; parse only the fields after the
  // final ')'. There, `utime` is token index 11 and `stime` is token index 12.
  const close = statText.lastIndexOf(")")
  if (close < 0) return undefined
  const fields = statText.slice(close + 2).split(" ")
  const userTicks = Number(fields[11])
  const systemTicks = Number(fields[12])
  if (!Number.isInteger(userTicks) || !Number.isInteger(systemTicks)) return undefined
  return { userTicks, systemTicks }
}

/**
 * Read a thread's cumulative CPU ticks from `/proc`.
 *
 * With no `threadId`, reads the calling thread via `/proc/thread-self/stat`.
 * Returns `undefined` on non-POSIX platforms (documented skip) or when the
 * reading is unavailable/unparseable, so callers degrade to timer drift.
 */
export function readThreadCpuTicks(threadId?: number, options: ReadThreadCpuOptions = {}): ThreadCpuTicks | undefined {
  if (threadId !== undefined && (!Number.isInteger(threadId) || threadId <= 0)) return undefined

  const statPath =
    threadId === undefined ? THREAD_SELF_STAT : `${options.taskDir ?? DEFAULT_TASK_DIR}/${threadId}/stat`
  const resolvedThreadId = threadId ?? currentThreadId()
  if (resolvedThreadId === undefined) return undefined

  let statText: string
  try {
    statText = readFileSync(statPath, "utf8")
  } catch {
    return undefined
  }

  const parsed = parseThreadStat(statText)
  if (parsed === undefined) return undefined

  return {
    threadId: resolvedThreadId,
    userTicks: parsed.userTicks,
    systemTicks: parsed.systemTicks,
    totalTicks: parsed.userTicks + parsed.systemTicks,
  }
}

function assertPositive(value: number, what: string): void {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
    throw new Error(`${what} must be a positive number, got: ${String(value)}`)
  }
}

export function createLoopGapProbe(options: LoopGapProbeOptions = {}): LoopGapProbe {
  const intervalMs = options.intervalMs ?? DEFAULT_INTERVAL_MS
  assertPositive(intervalMs, "intervalMs")

  const clock = options.clock ?? (() => performance.now())
  const readOptions = options.taskDir === undefined ? undefined : { taskDir: options.taskDir }

  const cpuBefore = readThreadCpuTicks(options.threadId, readOptions)
  const threadId = options.threadId ?? cpuBefore?.threadId
  const source: LoopGapSource = cpuBefore === undefined ? "timer" : "proc"

  const startedAt = clock()
  let lastTickAt = startedAt
  let maxGap = 0
  let sumGap = 0
  let ticks = 0
  let finalSample: LoopGapSample | undefined

  const timer = setInterval(() => {
    const now = clock()
    const gap = now - lastTickAt
    lastTickAt = now
    if (gap > maxGap) maxGap = gap
    sumGap += gap
    ticks++
  }, intervalMs)

  const readCpuMs = (): number | undefined => {
    if (cpuBefore === undefined || threadId === undefined) return undefined
    const after = readThreadCpuTicks(threadId, readOptions)
    if (after === undefined) return undefined
    return ((after.totalTicks - cpuBefore.totalTicks) / LINUX_CLOCK_TICKS_PER_SECOND) * 1000
  }

  const sample = (): LoopGapSample =>
    Object.freeze({
      threadId,
      // `finalSample.windowMs` is already relative to `startedAt`, so it must be
      // returned as-is; only the live branch subtracts `startedAt`.
      windowMs: finalSample?.windowMs ?? clock() - startedAt,
      intervalMs,
      ticks,
      maxGapMs: maxGap,
      meanGapMs: ticks === 0 ? 0 : sumGap / ticks,
      cpuMs: readCpuMs(),
      source,
    })

  const stop = (): LoopGapSample => {
    if (finalSample !== undefined) return finalSample
    const stoppedAt = clock()
    clearInterval(timer)
    finalSample = Object.freeze({
      threadId,
      windowMs: stoppedAt - startedAt,
      intervalMs,
      ticks,
      maxGapMs: maxGap,
      meanGapMs: ticks === 0 ? 0 : sumGap / ticks,
      cpuMs: readCpuMs(),
      source,
    })
    return finalSample
  }

  return {
    threadId,
    source,
    get running() {
      return finalSample === undefined
    },
    maxGapMs: () => maxGap,
    sample,
    stop,
  }
}

export function measureLoopGap(options: MeasureLoopGapOptions = {}): Promise<LoopGapSample> {
  const windowMs = options.windowMs ?? DEFAULT_WINDOW_MS
  assertPositive(windowMs, "windowMs")

  const probe = createLoopGapProbe(options)
  return new Promise<LoopGapSample>((resolve) => {
    setTimeout(() => resolve(probe.stop()), windowMs)
  }).finally(() => {
    // Idempotent: guarantee the sampling interval is cleared even if the window
    // wait or the resolution path is interrupted.
    probe.stop()
  })
}

/** Record a loop-gap sample into a metrics collector under `prefix` (default "hub"). */
export function recordLoopGap(metrics: Metrics, sample: LoopGapSample, prefix = "hub"): void {
  metrics.gauge(`${prefix}.loop_gap.max_ms`, sample.maxGapMs)
  metrics.gauge(`${prefix}.loop_gap.mean_ms`, sample.meanGapMs)
  if (sample.ticks > 0) metrics.increment(`${prefix}.loop_gap.ticks`, sample.ticks)
  if (sample.cpuMs !== undefined) metrics.gauge(`${prefix}.loop_gap.cpu_ms`, sample.cpuMs)
}
