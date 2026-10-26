// Eight-session benchmark harness for the D2 kill criterion.
//
// The harness is deliberately provider-agnostic: a caller supplies one
// `runSession` function (real worker pool, in-process loop, or a synthetic
// fixture) and reports each turn's CPU split. The harness records wall time,
// throughput, turn p95, the Session-part JSON/tokenization CPU share, and
// cross-session leakage. It never asserts machine-dependent numbers; the
// recorded result is compared against a baseline separately.

import { withDeadline } from "./async"

/** Bump when the persisted result shape changes. */
export const BENCHMARK_SCHEMA_VERSION = 1

/** D2 measures the worker divergence at exactly eight concurrent sessions. */
export const DEFAULT_BENCHMARK_SESSIONS = 8

export type BenchmarkMode = "in-process" | "worker"

/** One turn's measured CPU, split so the JSON/tokenization share is observable. */
export interface BenchmarkTurnSample {
  readonly turnMs: number
  readonly cpuMs: number
  readonly jsonTokenizationCpuMs: number
}

/** Everything one session reports back to the benchmark runner. */
export interface BenchmarkSessionSample {
  readonly turns: ReadonlyArray<BenchmarkTurnSample>
  /** Session ids this session touched (its own id is expected, others are leaks). */
  readonly observedSessionIDs?: ReadonlyArray<string>
  /** Event ids this session published or observed, for cross-session duplicate detection. */
  readonly eventIDs?: ReadonlyArray<string>
}

export interface BenchmarkSessionInput {
  readonly index: number
  readonly sessionID: string
  readonly signal: AbortSignal
}

export type BenchmarkSessionRunner = (
  input: BenchmarkSessionInput,
) => Promise<BenchmarkSessionSample> | BenchmarkSessionSample

export interface BenchmarkCpu {
  readonly totalMs: number
  readonly jsonTokenizationMs: number
  /** `jsonTokenizationMs / totalMs` in [0, 1]; 0 when no CPU was recorded. */
  readonly share: number
}

export interface CrossSessionLeak {
  readonly ownerSessionID: string
  readonly touchedSessionID: string
}

export interface SharedEventLeak {
  readonly eventID: string
  readonly sessionIDs: ReadonlyArray<string>
}

export interface LeakageReport {
  readonly clean: boolean
  readonly crossSession: ReadonlyArray<CrossSessionLeak>
  readonly sharedEvents: ReadonlyArray<SharedEventLeak>
}

/** The persisted result schema; parse it at every boundary. */
export interface BenchmarkResult {
  readonly schemaVersion: number
  readonly mode: BenchmarkMode
  readonly sessions: number
  readonly turns: number
  readonly wallMs: number
  readonly throughputPerSec: number
  readonly p95TurnMs: number
  readonly cpu: BenchmarkCpu
  readonly leakage: LeakageReport
}

function assertNonNegativeFinite(value: number, label: string): number {
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`${label} must be a finite, non-negative number, got ${value}`)
  }
  return value
}

function assertPositiveInteger(value: number, label: string): number {
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(`${label} must be a positive integer, got ${value}`)
  }
  return value
}

/**
 * Nearest-rank percentile over a non-empty sample, so p95 is deterministic for a
 * given set of observations and never interpolates a fabricated value.
 */
export function percentile(values: ReadonlyArray<number>, p: number): number {
  if (values.length === 0) throw new Error("percentile requires at least one observation")
  if (!Number.isFinite(p) || p < 0 || p > 100) throw new Error(`percentile must be in [0, 100], got ${p}`)
  const sorted = [...values].sort((left, right) => left - right)
  const rank = Math.ceil((p / 100) * sorted.length)
  const index = Math.min(Math.max(rank - 1, 0), sorted.length - 1)
  return sorted[index]!
}

/** `turn p95`, the D2 latency measure. */
export function p95(values: ReadonlyArray<number>): number {
  return percentile(values, 95)
}

/** Throughput guard: a non-positive wall time records `0`, never `Infinity`/`NaN`. */
export function throughputPerSec(sessions: number, wallMs: number): number {
  if (!(wallMs > 0)) return 0
  return sessions / (wallMs / 1000)
}

/**
 * Find cross-session reads/writes and event-id collisions. The report is sorted,
 * so the same observations always produce the same value.
 */
export function detectLeakage(
  entries: ReadonlyArray<{ readonly sessionID: string; readonly sample: BenchmarkSessionSample }>,
): LeakageReport {
  const crossSession = new Map<string, CrossSessionLeak>()
  for (const entry of entries) {
    for (const touched of entry.sample.observedSessionIDs ?? []) {
      if (touched === entry.sessionID) continue
      const key = `${entry.sessionID}\u0000${touched}`
      crossSession.set(key, { ownerSessionID: entry.sessionID, touchedSessionID: touched })
    }
  }

  const ownersByEvent = new Map<string, Set<string>>()
  for (const entry of entries) {
    for (const eventID of entry.sample.eventIDs ?? []) {
      const owners = ownersByEvent.get(eventID) ?? new Set<string>()
      owners.add(entry.sessionID)
      ownersByEvent.set(eventID, owners)
    }
  }
  const sharedEvents = [...ownersByEvent.entries()]
    .filter(([, owners]) => owners.size > 1)
    .map(([eventID, owners]) => ({ eventID, sessionIDs: [...owners].sort() }))
    .sort((left, right) => left.eventID.localeCompare(right.eventID))

  const cross = [...crossSession.values()].sort((left, right) =>
    `${left.ownerSessionID}\u0000${left.touchedSessionID}`.localeCompare(
      `${right.ownerSessionID}\u0000${right.touchedSessionID}`,
    ),
  )

  return {
    clean: cross.length === 0 && sharedEvents.length === 0,
    crossSession: cross,
    sharedEvents,
  }
}

/** Summarize already-collected session observations into the persisted schema. */
export function summarizeBenchmark(input: {
  readonly mode: BenchmarkMode
  readonly sessions: number
  readonly wallMs: number
  readonly entries: ReadonlyArray<{ readonly sessionID: string; readonly sample: BenchmarkSessionSample }>
}): BenchmarkResult {
  const sessions = assertPositiveInteger(input.sessions, "sessions")
  if (input.entries.length !== sessions) {
    throw new Error(`benchmark collected ${input.entries.length} session(s), expected ${sessions}`)
  }
  const wallMs = assertNonNegativeFinite(input.wallMs, "wallMs")

  const turns = input.entries.flatMap((entry) => entry.sample.turns)
  if (turns.length === 0) throw new Error("benchmark collected no turns")
  let totalCpuMs = 0
  let jsonCpuMs = 0
  const turnLatencies: number[] = []
  for (const turn of turns) {
    totalCpuMs += assertNonNegativeFinite(turn.cpuMs, "turn cpuMs")
    jsonCpuMs += assertNonNegativeFinite(turn.jsonTokenizationCpuMs, "turn jsonTokenizationCpuMs")
    turnLatencies.push(assertNonNegativeFinite(turn.turnMs, "turn turnMs"))
  }
  if (jsonCpuMs > totalCpuMs) {
    throw new Error(`Session-part JSON/tokenization CPU (${jsonCpuMs}ms) exceeds total turn CPU (${totalCpuMs}ms)`)
  }

  return {
    schemaVersion: BENCHMARK_SCHEMA_VERSION,
    mode: input.mode,
    sessions,
    turns: turns.length,
    wallMs,
    throughputPerSec: throughputPerSec(sessions, wallMs),
    p95TurnMs: p95(turnLatencies),
    cpu: {
      totalMs: totalCpuMs,
      jsonTokenizationMs: jsonCpuMs,
      share: totalCpuMs === 0 ? 0 : jsonCpuMs / totalCpuMs,
    },
    leakage: detectLeakage(input.entries),
  }
}

export interface RunBenchmarkOptions {
  readonly mode: BenchmarkMode
  readonly runSession: BenchmarkSessionRunner
  /** Defaults to 8 (the D2 measurement point). */
  readonly sessions?: number
  readonly timeoutMs?: number
}

/**
 * Run `sessions` sessions concurrently and record the D2 measures. A timed-out
 * run aborts the sessions and fails loud: a partial benchmark is not evidence.
 */
export async function runEightSessionBenchmark(options: RunBenchmarkOptions): Promise<BenchmarkResult> {
  const sessions = assertPositiveInteger(options.sessions ?? DEFAULT_BENCHMARK_SESSIONS, "sessions")
  const timeoutMs = options.timeoutMs ?? 120_000
  const controller = new AbortController()
  const sessionIDs = Array.from({ length: sessions }, (_, index) => `ses_bench_${index}`)
  const startedAt = performance.now()

  const work = Promise.all(
    sessionIDs.map(async (sessionID, index) => ({
      sessionID,
      sample: await options.runSession({ index, sessionID, signal: controller.signal }),
    })),
  )
  const entries = await withDeadline(work, {
    timeoutMs,
    message: `benchmark run timed out after ${timeoutMs}ms`,
    onTimeout: () => controller.abort(),
  })
  const wallMs = Math.max(performance.now() - startedAt, 0)

  return summarizeBenchmark({ mode: options.mode, sessions, wallMs, entries })
}

// --- Persisted result and baseline -----------------------------------------

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

function readNonNegative(value: unknown, label: string): number {
  if (typeof value !== "number") throw new Error(`${label} must be a number`)
  return assertNonNegativeFinite(value, label)
}

function readPositiveInteger(value: unknown, label: string): number {
  if (typeof value !== "number") throw new Error(`${label} must be a number`)
  return assertPositiveInteger(value, label)
}

function readString(value: unknown, label: string): string {
  if (typeof value !== "string" || value.length === 0) throw new Error(`${label} must be a non-empty string`)
  return value
}

function parseMode(value: unknown, label: string): BenchmarkMode {
  if (value !== "in-process" && value !== "worker") {
    throw new Error(`${label} must be "in-process" or "worker", got ${String(value)}`)
  }
  return value
}

function parseLeakage(value: unknown, label: string): LeakageReport {
  if (!isRecord(value)) throw new Error(`${label} must be an object`)
  if (typeof value.clean !== "boolean") throw new Error(`${label}.clean must be a boolean`)
  if (!Array.isArray(value.crossSession)) throw new Error(`${label}.crossSession must be an array`)
  if (!Array.isArray(value.sharedEvents)) throw new Error(`${label}.sharedEvents must be an array`)

  const crossSession = value.crossSession.map((entry, index) => {
    if (!isRecord(entry)) throw new Error(`${label}.crossSession[${index}] must be an object`)
    return {
      ownerSessionID: readString(entry.ownerSessionID, `${label}.crossSession[${index}].ownerSessionID`),
      touchedSessionID: readString(entry.touchedSessionID, `${label}.crossSession[${index}].touchedSessionID`),
    }
  })
  const sharedEvents = value.sharedEvents.map((entry, index) => {
    if (!isRecord(entry)) throw new Error(`${label}.sharedEvents[${index}] must be an object`)
    if (!Array.isArray(entry.sessionIDs)) throw new Error(`${label}.sharedEvents[${index}].sessionIDs must be an array`)
    return {
      eventID: readString(entry.eventID, `${label}.sharedEvents[${index}].eventID`),
      sessionIDs: entry.sessionIDs.map((id, position) =>
        readString(id, `${label}.sharedEvents[${index}].sessionIDs[${position}]`),
      ),
    }
  })
  return { clean: value.clean, crossSession, sharedEvents }
}

/** Parse an untrusted persisted result, failing loud on the first bad field. */
export function parseBenchmarkResult(value: unknown, path = "<benchmark>"): BenchmarkResult {
  if (!isRecord(value)) throw new Error(`${path} must be an object`)
  const schemaVersion = readPositiveInteger(value.schemaVersion, `${path}.schemaVersion`)
  if (schemaVersion !== BENCHMARK_SCHEMA_VERSION) {
    throw new Error(`${path}.schemaVersion is ${schemaVersion}, expected ${BENCHMARK_SCHEMA_VERSION}`)
  }
  if (!isRecord(value.cpu)) throw new Error(`${path}.cpu must be an object`)
  return {
    schemaVersion,
    mode: parseMode(value.mode, `${path}.mode`),
    sessions: readPositiveInteger(value.sessions, `${path}.sessions`),
    turns: readPositiveInteger(value.turns, `${path}.turns`),
    wallMs: readNonNegative(value.wallMs, `${path}.wallMs`),
    throughputPerSec: readNonNegative(value.throughputPerSec, `${path}.throughputPerSec`),
    p95TurnMs: readNonNegative(value.p95TurnMs, `${path}.p95TurnMs`),
    cpu: {
      totalMs: readNonNegative(value.cpu.totalMs, `${path}.cpu.totalMs`),
      jsonTokenizationMs: readNonNegative(value.cpu.jsonTokenizationMs, `${path}.cpu.jsonTokenizationMs`),
      share: readNonNegative(value.cpu.share, `${path}.cpu.share`),
    },
    leakage: parseLeakage(value.leakage, `${path}.leakage`),
  }
}

export interface BenchmarkThresholds {
  readonly minJsonShare: number
  readonly minThroughputRatio: number
  readonly maxThroughputRatio: number
  readonly maxP95DeltaRatio: number
  readonly maxLeaks: number
}

/** The D2 gates, recorded here so a baseline file can override them explicitly. */
export const DEFAULT_BENCHMARK_THRESHOLDS: BenchmarkThresholds = Object.freeze({
  minJsonShare: 0.25,
  minThroughputRatio: 1.5,
  maxThroughputRatio: 2,
  maxP95DeltaRatio: 0.2,
  maxLeaks: 0,
})

export interface BenchmarkBaseline {
  readonly schemaVersion: number
  readonly status: "stub" | "recorded"
  readonly inProcess: BenchmarkResult | null
  readonly worker: BenchmarkResult | null
  readonly thresholds: BenchmarkThresholds
}

function parseThresholds(value: unknown, path: string): BenchmarkThresholds {
  if (!isRecord(value)) throw new Error(`${path} must be an object`)
  return {
    minJsonShare: readNonNegative(value.minJsonShare, `${path}.minJsonShare`),
    minThroughputRatio: readNonNegative(value.minThroughputRatio, `${path}.minThroughputRatio`),
    maxThroughputRatio: readNonNegative(value.maxThroughputRatio, `${path}.maxThroughputRatio`),
    maxP95DeltaRatio: readNonNegative(value.maxP95DeltaRatio, `${path}.maxP95DeltaRatio`),
    maxLeaks: readNonNegative(value.maxLeaks, `${path}.maxLeaks`),
  }
}

/** A baseline before any measurement is recorded; it can never pass the gate. */
export function benchmarkBaselineStub(): BenchmarkBaseline {
  return {
    schemaVersion: BENCHMARK_SCHEMA_VERSION,
    status: "stub",
    inProcess: null,
    worker: null,
    thresholds: DEFAULT_BENCHMARK_THRESHOLDS,
  }
}

/** Parse an untrusted baseline file, failing loud rather than guessing. */
export function parseBenchmarkBaseline(value: unknown, path = "<benchmark-baseline>"): BenchmarkBaseline {
  if (!isRecord(value)) throw new Error(`${path} must be an object`)
  const schemaVersion = readPositiveInteger(value.schemaVersion, `${path}.schemaVersion`)
  if (schemaVersion !== BENCHMARK_SCHEMA_VERSION) {
    throw new Error(`${path}.schemaVersion is ${schemaVersion}, expected ${BENCHMARK_SCHEMA_VERSION}`)
  }
  if (value.status !== "stub" && value.status !== "recorded") {
    throw new Error(`${path}.status must be "stub" or "recorded", got ${String(value.status)}`)
  }
  return {
    schemaVersion,
    status: value.status,
    inProcess: value.inProcess === null ? null : parseBenchmarkResult(value.inProcess, `${path}.inProcess`),
    worker: value.worker === null ? null : parseBenchmarkResult(value.worker, `${path}.worker`),
    thresholds: parseThresholds(value.thresholds, `${path}.thresholds`),
  }
}

export interface KillCriterionMeasurements {
  readonly jsonShare: number
  readonly throughputRatio: number
  readonly p95DeltaRatio: number
  readonly leaks: number
}

export interface KillCriterionEvaluation {
  readonly pass: boolean
  readonly failures: ReadonlyArray<string>
  readonly measurements: KillCriterionMeasurements
}

function safeRatio(numerator: number, denominator: number, label: string): number {
  if (!(denominator > 0)) throw new Error(`${label} denominator must be positive, got ${denominator}`)
  return numerator / denominator
}

/**
 * Evaluate D2 from recorded measurements. Every failed condition is reported, so
 * a kill decision is auditable rather than a single opaque boolean.
 */
export function evaluateKillCriterion(
  worker: BenchmarkResult,
  inProcess: BenchmarkResult,
  thresholds: BenchmarkThresholds = DEFAULT_BENCHMARK_THRESHOLDS,
): KillCriterionEvaluation {
  const failures: string[] = []
  if (worker.sessions !== DEFAULT_BENCHMARK_SESSIONS || inProcess.sessions !== DEFAULT_BENCHMARK_SESSIONS) {
    failures.push(
      `benchmark must run ${DEFAULT_BENCHMARK_SESSIONS} sessions (worker ${worker.sessions}, in-process ${inProcess.sessions})`,
    )
  }

  if (worker.cpu.share < thresholds.minJsonShare) {
    failures.push(
      `Session-part JSON/tokenization CPU share ${(worker.cpu.share * 100).toFixed(1)}% is below ${(thresholds.minJsonShare * 100).toFixed(1)}%`,
    )
  }

  const throughputRatio = safeRatio(worker.throughputPerSec, inProcess.throughputPerSec, "throughput ratio")
  if (throughputRatio < thresholds.minThroughputRatio) {
    failures.push(
      `throughput ratio ${throughputRatio.toFixed(2)}x is below ${thresholds.minThroughputRatio.toFixed(2)}x`,
    )
  }
  if (throughputRatio > thresholds.maxThroughputRatio) {
    failures.push(
      `throughput ratio ${throughputRatio.toFixed(2)}x is above ${thresholds.maxThroughputRatio.toFixed(2)}x`,
    )
  }

  const p95DeltaRatio = safeRatio(
    worker.p95TurnMs - inProcess.p95TurnMs,
    inProcess.p95TurnMs,
    "p95 delta ratio",
  )
  if (p95DeltaRatio > thresholds.maxP95DeltaRatio) {
    failures.push(
      `turn p95 regression ${(p95DeltaRatio * 100).toFixed(1)}% exceeds ${(thresholds.maxP95DeltaRatio * 100).toFixed(1)}%`,
    )
  }

  const leaks = worker.leakage.crossSession.length + worker.leakage.sharedEvents.length
  if (leaks > thresholds.maxLeaks) failures.push(`cross-session leakage: ${leaks} finding(s)`)
  if (!worker.leakage.clean) failures.push("worker leakage report is not clean")
  if (!inProcess.leakage.clean) failures.push("in-process leakage report is not clean")

  return {
    pass: failures.length === 0,
    failures,
    measurements: {
      jsonShare: worker.cpu.share,
      throughputRatio,
      p95DeltaRatio,
      leaks,
    },
  }
}

export type BaselineEvaluation =
  | { readonly status: "stub"; readonly pass: false; readonly failures: ReadonlyArray<string> }
  | (KillCriterionEvaluation & { readonly status: "recorded" })

/** Evaluate a baseline file, refusing to pass while it is still a stub. */
export function evaluateBenchmarkBaseline(baseline: BenchmarkBaseline): BaselineEvaluation {
  if (baseline.status !== "recorded" || baseline.inProcess === null || baseline.worker === null) {
    return {
      status: "stub",
      pass: false,
      failures: ["benchmark baseline is a stub; record in-process and worker runs before the kill criterion"],
    }
  }
  return {
    status: "recorded",
    ...evaluateKillCriterion(baseline.worker, baseline.inProcess, baseline.thresholds),
  }
}
