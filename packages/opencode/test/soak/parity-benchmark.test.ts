// Benchmark acceptance harness.
//
// This proves the persisted baseline path and the eight-session runner without
// asserting machine-dependent numbers. The real worker/in-process recordings
// replace the synthetic `runSession` when the production drivers land.

import { describe, expect, test } from "bun:test"
import path from "node:path"
import {
  DEFAULT_BENCHMARK_SESSIONS,
  evaluateBenchmarkBaseline,
  parseBenchmarkBaseline,
  parseBenchmarkResult,
  runEightSessionBenchmark,
} from "../lib/parity-benchmark"

const baselinePath = path.resolve(import.meta.dir, "../../../../.parallel-implement/wave-v2/benchmark-baseline.json")

describe("parity benchmark baseline", () => {
  test("the checked-in baseline is a stub and never passes", async () => {
    const baseline = parseBenchmarkBaseline(await Bun.file(baselinePath).json(), baselinePath)
    expect(baseline.status).toBe("stub")
    const evaluation = evaluateBenchmarkBaseline(baseline)
    expect(evaluation.pass).toBe(false)
    expect(evaluation.failures.join(" ")).toContain("stub")
  })

  test("an eight-session synthetic recording round-trips through the result schema", async () => {
    const result = await runEightSessionBenchmark({
      mode: "worker",
      sessions: DEFAULT_BENCHMARK_SESSIONS,
      timeoutMs: 5_000,
      runSession: ({ index }) => ({
        turns: [{ turnMs: 100, cpuMs: 100, jsonTokenizationCpuMs: 30 }],
        observedSessionIDs: [`ses_bench_${index}`],
        eventIDs: [`evt_${index}`],
      }),
    })

    expect(result.sessions).toBe(DEFAULT_BENCHMARK_SESSIONS)
    expect(result.cpu.share).toBeCloseTo(0.3)
    expect(result.leakage.clean).toBe(true)
    expect(parseBenchmarkResult(JSON.parse(JSON.stringify(result)))).toEqual(result)
  })
})
