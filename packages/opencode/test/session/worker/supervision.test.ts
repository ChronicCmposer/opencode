// Phase 4.7 (crash-loop half) — supervision backoff + degraded pool.
//
// Backoff doubles from 1 s and caps at 30 s; five crashes on one slot mark it
// degraded (the pool runs at K−1). A healthy run resets the counter.

import { describe, expect, test } from "bun:test"
import {
  CRASH_ATTEMPTS_BEFORE_DEGRADED,
  CRASH_BACKOFF_INITIAL_MS,
  CRASH_BACKOFF_MAX_MS,
  crashBackoffMs,
  createSupervision,
} from "@/session/worker/supervision"

describe("crash backoff schedule", () => {
  test("doubles from 1s and caps at 30s", () => {
    expect(crashBackoffMs(1)).toBe(1_000)
    expect(crashBackoffMs(2)).toBe(2_000)
    expect(crashBackoffMs(3)).toBe(4_000)
    expect(crashBackoffMs(4)).toBe(8_000)
    expect(crashBackoffMs(5)).toBe(16_000)
    expect(crashBackoffMs(6)).toBe(30_000)
    expect(crashBackoffMs(20)).toBe(30_000)
  })

  test("the approved constants are 1s / 30s / 5 attempts", () => {
    expect(CRASH_BACKOFF_INITIAL_MS).toBe(1_000)
    expect(CRASH_BACKOFF_MAX_MS).toBe(30_000)
    expect(CRASH_ATTEMPTS_BEFORE_DEGRADED).toBe(5)
  })

  test("caps without overflow and maps non-finite counts to no backoff", () => {
    expect(crashBackoffMs(1e9)).toBe(30_000)
    expect(crashBackoffMs(Number.POSITIVE_INFINITY)).toBe(0)
    expect(crashBackoffMs(Number.NaN)).toBe(0)
    expect(crashBackoffMs(0)).toBe(0)
  })
})

describe("supervision", () => {
  test("the fifth crash on one slot degrades it", () => {
    const supervision = createSupervision()
    for (let attempt = 1; attempt < CRASH_ATTEMPTS_BEFORE_DEGRADED; attempt++) {
      const decision = supervision.recordCrash(1)
      expect(decision.attempts).toBe(attempt)
      expect(decision.degraded).toBe(false)
      expect(decision.delayMs).toBe(crashBackoffMs(attempt))
    }

    const final = supervision.recordCrash(1)
    expect(final.attempts).toBe(5)
    expect(final.degraded).toBe(true)
    expect(supervision.isDegraded(1)).toBe(true)
    expect(supervision.degradedCount).toBe(1)
  })

  test("a healthy run resets the crash counter and clears degradation", () => {
    const supervision = createSupervision()
    for (let i = 0; i < CRASH_ATTEMPTS_BEFORE_DEGRADED; i++) supervision.recordCrash(1)
    expect(supervision.degradedCount).toBe(1)

    supervision.recordHealthy(1)
    expect(supervision.isDegraded(1)).toBe(false)
    expect(supervision.degradedCount).toBe(0)
    expect(supervision.recordCrash(1).attempts).toBe(1)
  })

  test("degradation is tracked per slot", () => {
    const supervision = createSupervision()
    for (let i = 0; i < CRASH_ATTEMPTS_BEFORE_DEGRADED; i++) supervision.recordCrash(1)

    expect(supervision.isDegraded(1)).toBe(true)
    expect(supervision.isDegraded(2)).toBe(false)
    expect(supervision.recordCrash(2).degraded).toBe(false)
    expect(supervision.degradedCount).toBe(1)
  })

  test("forget drops a removed slot's state so degraded entries do not leak", () => {
    const supervision = createSupervision()
    for (let i = 0; i < CRASH_ATTEMPTS_BEFORE_DEGRADED; i++) supervision.recordCrash(7)
    expect(supervision.isDegraded(7)).toBe(true)
    expect(supervision.degradedCount).toBe(1)

    supervision.forget(7)
    expect(supervision.isDegraded(7)).toBe(false)
    expect(supervision.degradedCount).toBe(0)
    expect(supervision.recordCrash(7).attempts).toBe(1)
  })

  test("fails loud on an invalid worker id", () => {
    const supervision = createSupervision()
    expect(() => supervision.recordCrash(-1)).toThrow()
    expect(() => supervision.recordCrash(1.5)).toThrow()
    expect(() => supervision.recordHealthy(Number.NaN)).toThrow()
    expect(() => supervision.isDegraded(-2)).toThrow()
    expect(() => supervision.forget(-3)).toThrow()
  })
})
