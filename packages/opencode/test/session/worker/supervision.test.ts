// Per-session crash supervision: five consecutive crashes mark a session dead;
// backoff doubles from 1s; a healthy run or a user-initiated dispatch resets the
// policy; background/auto dispatch on a dead session fails fast.

import { describe, expect, test } from "bun:test"
import {
  CRASH_ATTEMPTS_BEFORE_DEAD,
  CRASH_BACKOFF_INITIAL_MS,
  CRASH_BACKOFF_MAX_MS,
  MAX_SUPERVISION_SESSIONS,
  crashBackoffMs,
  createSupervision,
} from "@/session/worker/supervision"
import { createRecordingScheduler } from "../../lib/scheduler"

function setup() {
  const clock = { now: 0 }
  const scheduler = createRecordingScheduler()
  const supervision = createSupervision({ now: () => clock.now, schedule: scheduler.schedule })
  return { clock, scheduler, supervision }
}

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
    expect(CRASH_ATTEMPTS_BEFORE_DEAD).toBe(5)
  })

  test("caps without overflow and maps non-finite counts to no backoff", () => {
    expect(crashBackoffMs(1e9)).toBe(30_000)
    expect(crashBackoffMs(Number.POSITIVE_INFINITY)).toBe(0)
    expect(crashBackoffMs(Number.NaN)).toBe(0)
    expect(crashBackoffMs(0)).toBe(0)
  })
})

describe("per-session crash supervision", () => {
  test("the fifth consecutive crash marks the session dead", () => {
    const { scheduler, supervision } = setup()

    for (let attempt = 1; attempt < CRASH_ATTEMPTS_BEFORE_DEAD; attempt++) {
      const decision = supervision.recordCrash("ses_a")
      expect(decision.attempts).toBe(attempt)
      expect(decision.dead).toBe(false)
      expect(decision.delayMs).toBe(crashBackoffMs(attempt))
    }

    const final = supervision.recordCrash("ses_a")
    expect(final.attempts).toBe(5)
    expect(final.dead).toBe(true)
    expect(supervision.isDead("ses_a")).toBe(true)
    expect(scheduler.timers.map((timer) => timer.delayMs)).toEqual([1_000, 2_000, 4_000, 8_000])
  })

  test("crash state is isolated per session", () => {
    const { supervision } = setup()
    for (let i = 0; i < CRASH_ATTEMPTS_BEFORE_DEAD; i++) supervision.recordCrash("ses_a")

    expect(supervision.isDead("ses_a")).toBe(true)
    expect(supervision.isDead("ses_b")).toBe(false)
    expect(supervision.recordCrash("ses_b").dead).toBe(false)
  })

  test("a healthy run resets the crash counter", () => {
    const { scheduler, supervision } = setup()
    for (let i = 0; i < CRASH_ATTEMPTS_BEFORE_DEAD; i++) supervision.recordCrash("ses_a")
    expect(supervision.isDead("ses_a")).toBe(true)

    supervision.recordHealthy("ses_a")
    expect(supervision.isDead("ses_a")).toBe(false)
    expect(supervision.recordCrash("ses_a").attempts).toBe(1)
    expect(scheduler.timers.at(-1)?.delayMs).toBe(1_000)
  })

  test("auto dispatch waits out the backoff then proceeds", () => {
    const { clock, scheduler, supervision } = setup()
    supervision.recordCrash("ses_a")

    expect(supervision.decideDispatch("ses_a", "auto")).toEqual({ kind: "backoff", delayMs: 1_000 })

    clock.now = 400
    expect(supervision.decideDispatch("ses_a", "auto")).toEqual({ kind: "backoff", delayMs: 600 })

    clock.now = 1_200
    scheduler.fire()
    expect(supervision.decideDispatch("ses_a", "auto")).toEqual({ kind: "allow" })
  })

  test("a user-initiated dispatch resets a dead session and respawns", () => {
    const { supervision } = setup()
    for (let i = 0; i < CRASH_ATTEMPTS_BEFORE_DEAD; i++) supervision.recordCrash("ses_a")
    expect(supervision.decideDispatch("ses_a", "auto")).toEqual({ kind: "dead" })

    expect(supervision.decideDispatch("ses_a", "user")).toEqual({ kind: "allow" })
    expect(supervision.isDead("ses_a")).toBe(false)
    expect(supervision.recordCrash("ses_a").attempts).toBe(1)
  })

  test("background/auto dispatch on a dead session fails fast", () => {
    const { scheduler, supervision } = setup()
    for (let i = 0; i < CRASH_ATTEMPTS_BEFORE_DEAD; i++) supervision.recordCrash("ses_a")
    const timersAfterDeath = scheduler.timers.length

    expect(supervision.decideDispatch("ses_a", "auto")).toEqual({ kind: "dead" })
    expect(supervision.isDead("ses_a")).toBe(true)
    expect(scheduler.timers).toHaveLength(timersAfterDeath)
  })

  test("forget clears state and cancels a pending backoff timer", () => {
    const { scheduler, supervision } = setup()
    supervision.recordCrash("ses_a")
    expect(scheduler.pending()).toHaveLength(1)

    supervision.forget("ses_a")
    expect(supervision.isDead("ses_a")).toBe(false)
    expect(scheduler.pending()).toHaveLength(0)
    expect(supervision.recordCrash("ses_a").attempts).toBe(1)
  })

  test("crash history is bounded and evicts the oldest session", () => {
    const { scheduler, supervision } = setup()
    for (let index = 0; index < MAX_SUPERVISION_SESSIONS; index++) {
      supervision.recordCrash(`ses_${index}`)
    }
    expect(supervision.size).toBe(MAX_SUPERVISION_SESSIONS)
    expect(scheduler.pending()).toHaveLength(MAX_SUPERVISION_SESSIONS)

    supervision.recordCrash("ses_new")
    expect(supervision.size).toBe(MAX_SUPERVISION_SESSIONS)
    expect(scheduler.pending()).toHaveLength(MAX_SUPERVISION_SESSIONS)
    // The oldest entry (and its timer) is gone; the new one is retained.
    expect(supervision.recordCrash("ses_0").attempts).toBe(1)
    expect(supervision.recordCrash("ses_new").attempts).toBe(2)
  })

  test("fails loud on an invalid session id", () => {
    const { supervision } = setup()
    expect(() => supervision.recordCrash("")).toThrow()
    expect(() => supervision.recordHealthy("")).toThrow()
    expect(() => supervision.isDead("")).toThrow()
    expect(() => supervision.decideDispatch("", "auto")).toThrow()
    expect(() => supervision.forget("")).toThrow()
  })
})
