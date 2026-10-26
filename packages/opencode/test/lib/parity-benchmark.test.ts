import { describe, expect, test } from "bun:test"
import {
  BENCHMARK_SCHEMA_VERSION,
  DEFAULT_BENCHMARK_SESSIONS,
  DEFAULT_BENCHMARK_THRESHOLDS,
  benchmarkBaselineStub,
  detectLeakage,
  evaluateBenchmarkBaseline,
  evaluateKillCriterion,
  p95,
  parseBenchmarkBaseline,
  parseBenchmarkResult,
  percentile,
  runEightSessionBenchmark,
  summarizeBenchmark,
  throughputPerSec,
  type BenchmarkMode,
  type BenchmarkResult,
  type BenchmarkSessionSample,
} from "./parity-benchmark"

function sample(index: number, jsonShare = 0.3): BenchmarkSessionSample {
  return {
    turns: [{ turnMs: 100 + index, cpuMs: 100, jsonTokenizationCpuMs: 100 * jsonShare }],
    observedSessionIDs: [`ses_bench_${index}`],
    eventIDs: [`evt_${index}`],
  }
}

function result(
  mode: BenchmarkMode,
  options: { wallMs?: number; turnMs?: number; jsonShare?: number } = {},
): BenchmarkResult {
  const wallMs = options.wallMs ?? 1_000
  const turnMs = options.turnMs ?? 100
  const jsonShare = options.jsonShare ?? 0.3
  return summarizeBenchmark({
    mode,
    sessions: DEFAULT_BENCHMARK_SESSIONS,
    wallMs,
    entries: Array.from({ length: DEFAULT_BENCHMARK_SESSIONS }, (_, index) => ({
      sessionID: `ses_bench_${index}`,
      sample: {
        turns: [{ turnMs, cpuMs: 100, jsonTokenizationCpuMs: 100 * jsonShare }],
        observedSessionIDs: [`ses_bench_${index}`],
        eventIDs: [`evt_${mode}_${index}`],
      },
    })),
  })
}

describe("benchmark statistics", () => {
  test("nearest-rank p95 is deterministic and total", () => {
    expect(p95(Array.from({ length: 100 }, (_, index) => index + 1))).toBe(95)
    expect(p95([7])).toBe(7)
    expect(() => percentile([], 95)).toThrow(/at least one observation/)
    expect(() => percentile([1], 101)).toThrow(/percentile must be in/)
  })

  test("throughput never produces Infinity or NaN", () => {
    expect(throughputPerSec(8, 4_000)).toBe(2)
    expect(throughputPerSec(8, 0)).toBe(0)
    expect(throughputPerSec(8, -1)).toBe(0)
  })
})

describe("eight-session benchmark runner", () => {
  test("runs eight sessions concurrently and records the D2 measures", async () => {
    const started: string[] = []
    const benchmark = await runEightSessionBenchmark({
      mode: "worker",
      timeoutMs: 5_000,
      runSession: async ({ index, sessionID }) => {
        started.push(sessionID)
        await Promise.resolve()
        return sample(index)
      },
    })

    expect(started).toHaveLength(DEFAULT_BENCHMARK_SESSIONS)
    expect(benchmark.sessions).toBe(DEFAULT_BENCHMARK_SESSIONS)
    expect(benchmark.turns).toBe(DEFAULT_BENCHMARK_SESSIONS)
    expect(benchmark.schemaVersion).toBe(BENCHMARK_SCHEMA_VERSION)
    expect(benchmark.cpu.jsonTokenizationMs).toBeCloseTo(240)
    expect(benchmark.cpu.totalMs).toBeCloseTo(800)
    expect(benchmark.cpu.share).toBeCloseTo(0.3)
    expect(benchmark.throughputPerSec).toBeGreaterThan(0)
    expect(benchmark.p95TurnMs).toBe(107)
    expect(benchmark.leakage).toEqual({ clean: true, crossSession: [], sharedEvents: [] })
  })

  test("a timed-out run aborts its sessions and fails loud", async () => {
    let aborted = false
    await expect(
      runEightSessionBenchmark({
        mode: "worker",
        timeoutMs: 20,
        runSession: ({ signal }) =>
          new Promise((resolve) => {
            signal.addEventListener(
              "abort",
              () => {
                aborted = true
                resolve(sample(0))
              },
              { once: true },
            )
          }),
      }),
    ).rejects.toThrow(/timed out/)
    expect(aborted).toBe(true)
  })

  test("leakage detection reports cross-session touches and shared event ids", () => {
    const leakage = detectLeakage([
      {
        sessionID: "ses_a",
        sample: { turns: [], observedSessionIDs: ["ses_a", "ses_b"], eventIDs: ["evt_shared"] },
      },
      { sessionID: "ses_b", sample: { turns: [], observedSessionIDs: ["ses_b"], eventIDs: ["evt_shared"] } },
    ])

    expect(leakage.clean).toBe(false)
    expect(leakage.crossSession).toEqual([{ ownerSessionID: "ses_a", touchedSessionID: "ses_b" }])
    expect(leakage.sharedEvents).toEqual([{ eventID: "evt_shared", sessionIDs: ["ses_a", "ses_b"] }])
  })
})

describe("benchmark summary and result schema", () => {
  test("invalid observations fail loud", () => {
    expect(() =>
      summarizeBenchmark({ mode: "worker", sessions: 8, wallMs: 1, entries: [] }),
    ).toThrow(/collected 0 session/)
    expect(() =>
      summarizeBenchmark({
        mode: "worker",
        sessions: 1,
        wallMs: 1,
        entries: [{ sessionID: "ses_a", sample: { turns: [] } }],
      }),
    ).toThrow(/collected no turns/)
    expect(() =>
      summarizeBenchmark({
        mode: "worker",
        sessions: 1,
        wallMs: 1,
        entries: [
          {
            sessionID: "ses_a",
            sample: { turns: [{ turnMs: 1, cpuMs: 10, jsonTokenizationCpuMs: 11 }] },
          },
        ],
      }),
    ).toThrow(/exceeds total turn CPU/)
  })

  test("a persisted result round-trips through the parser and rejects drift", () => {
    const value = result("worker")
    expect(parseBenchmarkResult(JSON.parse(JSON.stringify(value)))).toEqual(value)
    expect(() => parseBenchmarkResult({ ...value, schemaVersion: 999 })).toThrow(/schemaVersion/)
    expect(() => parseBenchmarkResult({ ...value, mode: "auto" })).toThrow(/must be "in-process" or "worker"/)
    expect(() => parseBenchmarkResult({ ...value, leakage: undefined })).toThrow(/leakage/)
  })
})

describe("D2 kill criterion", () => {
  test("a passing recording satisfies every threshold", () => {
    const worker = result("worker", { wallMs: 500, turnMs: 115, jsonShare: 0.3 })
    const inProcess = result("in-process", { wallMs: 1_000, turnMs: 100, jsonShare: 0.1 })
    const evaluation = evaluateKillCriterion(worker, inProcess)
    expect(evaluation.pass).toBe(true)
    expect(evaluation.failures).toEqual([])
    expect(evaluation.measurements.throughputRatio).toBeCloseTo(2)
    expect(evaluation.measurements.p95DeltaRatio).toBeCloseTo(0.15)
  })

  test("every missed condition is reported with its measurement", () => {
    const worker = result("worker", { wallMs: 1_000, turnMs: 130, jsonShare: 0.2 })
    const inProcess = result("in-process", { wallMs: 1_000, turnMs: 100, jsonShare: 0.1 })
    const evaluation = evaluateKillCriterion(worker, inProcess)
    expect(evaluation.pass).toBe(false)
    expect(evaluation.failures.join(" ")).toContain("JSON/tokenization CPU share")
    expect(evaluation.failures.join(" ")).toContain("throughput ratio")
    expect(evaluation.failures.join(" ")).toContain("turn p95 regression")
  })

  test("leakage fails the criterion even when the timing gates pass", () => {
    const worker = result("worker", { wallMs: 500, turnMs: 110, jsonShare: 0.3 })
    const withLeak: BenchmarkResult = {
      ...worker,
      leakage: {
        clean: false,
        crossSession: [{ ownerSessionID: "ses_bench_0", touchedSessionID: "ses_bench_1" }],
        sharedEvents: [],
      },
    }
    const evaluation = evaluateKillCriterion(
      withLeak,
      result("in-process", { wallMs: 1_000, turnMs: 100, jsonShare: 0.1 }),
    )
    expect(evaluation.pass).toBe(false)
    expect(evaluation.failures.join(" ")).toContain("cross-session leakage")
  })

  test("a stub baseline never passes and a recorded baseline evaluates", () => {
    const stub = parseBenchmarkBaseline(benchmarkBaselineStub())
    expect(evaluateBenchmarkBaseline(stub)).toEqual({
      status: "stub",
      pass: false,
      failures: ["benchmark baseline is a stub; record in-process and worker runs before the kill criterion"],
    })

    const recorded = parseBenchmarkBaseline({
      schemaVersion: BENCHMARK_SCHEMA_VERSION,
      status: "recorded",
      inProcess: result("in-process", { wallMs: 1_000, turnMs: 100, jsonShare: 0.1 }),
      worker: result("worker", { wallMs: 500, turnMs: 110, jsonShare: 0.3 }),
      thresholds: DEFAULT_BENCHMARK_THRESHOLDS,
    })
    const evaluation = evaluateBenchmarkBaseline(recorded)
    expect(evaluation.status).toBe("recorded")
    expect(evaluation.pass).toBe(true)
  })
})
