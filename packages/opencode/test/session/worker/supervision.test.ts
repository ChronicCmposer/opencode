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

function setup() {
  const clock = { now: 0 }
  const supervision = createSupervision({ now: () => clock.now })
  return { clock, supervision }
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
    const { supervision } = setup()

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
  })

  test("crash state is isolated per session", () => {
    const { supervision } = setup()
    for (let i = 0; i < CRASH_ATTEMPTS_BEFORE_DEAD; i++) supervision.recordCrash("ses_a")

    expect(supervision.isDead("ses_a")).toBe(true)
    expect(supervision.isDead("ses_b")).toBe(false)
    expect(supervision.recordCrash("ses_b").dead).toBe(false)
  })

  test("a healthy run resets the crash counter", () => {
    const { supervision } = setup()
    for (let i = 0; i < CRASH_ATTEMPTS_BEFORE_DEAD; i++) supervision.recordCrash("ses_a")
    expect(supervision.isDead("ses_a")).toBe(true)

    supervision.recordHealthy("ses_a")
    expect(supervision.isDead("ses_a")).toBe(false)
    expect(supervision.recordCrash("ses_a").attempts).toBe(1)
  })

  test("auto dispatch waits out the backoff then proceeds", () => {
    const { clock, supervision } = setup()
    supervision.recordCrash("ses_a")

    expect(supervision.decideDispatch("ses_a", "auto")).toEqual({ kind: "backoff", delayMs: 1_000 })

    clock.now = 400
    expect(supervision.decideDispatch("ses_a", "auto")).toEqual({ kind: "backoff", delayMs: 600 })

    clock.now = 1_200
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
    const { supervision } = setup()
    for (let i = 0; i < CRASH_ATTEMPTS_BEFORE_DEAD; i++) supervision.recordCrash("ses_a")

    expect(supervision.decideDispatch("ses_a", "auto")).toEqual({ kind: "dead" })
    expect(supervision.isDead("ses_a")).toBe(true)
  })

  test("forget clears the session's backoff state", () => {
    const { supervision } = setup()
    supervision.recordCrash("ses_a")

    supervision.forget("ses_a")
    expect(supervision.isDead("ses_a")).toBe(false)
    expect(supervision.decideDispatch("ses_a", "auto")).toEqual({ kind: "allow" })
    expect(supervision.recordCrash("ses_a").attempts).toBe(1)
  })

  test("crash history is bounded and evicts the oldest session", () => {
    const { supervision } = setup()
    for (let index = 0; index < MAX_SUPERVISION_SESSIONS; index++) {
      supervision.recordCrash(`ses_${index}`)
    }
    expect(supervision.size).toBe(MAX_SUPERVISION_SESSIONS)

    supervision.recordCrash("ses_new")
    expect(supervision.size).toBe(MAX_SUPERVISION_SESSIONS)
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

describe("backoff is pure retryAt arithmetic", () => {
  test("recordCrash schedules no timer; expiry alone gates the next dispatch", () => {
    const clock = { now: 0 }
    const supervision = createSupervision({ now: () => clock.now })

    supervision.recordCrash("ses_a")
    expect(supervision.decideDispatch("ses_a", "auto")).toEqual({ kind: "backoff", delayMs: 1_000 })
    clock.now = 999
    expect(supervision.decideDispatch("ses_a", "auto")).toEqual({ kind: "backoff", delayMs: 1 })
    clock.now = 1_000
    expect(supervision.decideDispatch("ses_a", "auto")).toEqual({ kind: "allow" })
  })

  test("every reset path (healthy, forget, user dispatch) clears the same state", () => {
    const clock = { now: 0 }
    const supervision = createSupervision({ now: () => clock.now })

    for (const reset of [
      () => supervision.recordHealthy("ses_a"),
      () => supervision.forget("ses_a"),
      () => supervision.decideDispatch("ses_a", "user"),
    ]) {
      supervision.recordCrash("ses_a")
      supervision.recordCrash("ses_a")
      expect(supervision.isDead("ses_a")).toBe(false)
      reset()
      expect(supervision.isDead("ses_a")).toBe(false)
      expect(supervision.recordCrash("ses_a").attempts).toBe(1)
    }
  })
})
