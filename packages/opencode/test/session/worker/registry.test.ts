// Phase 4.8 — key→worker map lifecycle (plan task 4.8, plan line 61).
//
// The hub's only per-key state: a `SessionID`→worker reservation with the
// `idle → reserved → running → cancelling → released` state machine. Insert on
// dispatch, delete on completion/cancel/crash, clear on terminate.

import { describe, expect, test } from "bun:test"
import type { SessionID } from "@/session/schema"
import { createSessionRegistry } from "@/session/worker/registry"

const sid = (value: string) => value as SessionID

describe("session registry", () => {
  test("reserve inserts a key→worker mapping in the reserved state", () => {
    const registry = createSessionRegistry()
    const reservation = registry.reserve(sid("ses_a"), 7)

    expect(reservation.state).toBe("reserved")
    expect(reservation.workerID).toBe(7)
    expect(registry.has(sid("ses_a"))).toBe(true)
    expect(registry.get(sid("ses_a"))?.workerID).toBe(7)
    expect(registry.size).toBe(1)
  })

  test("advances through running → cancelling → released", () => {
    const registry = createSessionRegistry()
    registry.reserve(sid("ses_a"), 1)

    expect(registry.markRunning(sid("ses_a")).state).toBe("running")
    expect(registry.markCancelling(sid("ses_a")).state).toBe("cancelling")

    registry.release(sid("ses_a"))
    expect(registry.has(sid("ses_a"))).toBe(false)
    expect(registry.size).toBe(0)
  })

  test("reserving the same key twice fails loud", () => {
    const registry = createSessionRegistry()
    registry.reserve(sid("ses_a"), 1)
    expect(() => registry.reserve(sid("ses_a"), 2)).toThrow(/ses_a/)
  })

  test("markRunning / markCancelling on an unknown key fail loud", () => {
    const registry = createSessionRegistry()
    expect(() => registry.markRunning(sid("ses_missing"))).toThrow(/ses_missing/)
    expect(() => registry.markCancelling(sid("ses_missing"))).toThrow(/ses_missing/)
  })

  test("release is idempotent (completion, cancel and crash may race)", () => {
    const registry = createSessionRegistry()
    registry.reserve(sid("ses_a"), 1)
    registry.release(sid("ses_a"))
    expect(() => registry.release(sid("ses_a"))).not.toThrow()
  })

  test("sessionsOn lists the sessions a worker is handling", () => {
    const registry = createSessionRegistry()
    registry.reserve(sid("ses_a"), 1)
    registry.reserve(sid("ses_b"), 1)
    registry.reserve(sid("ses_c"), 2)

    expect(registry.sessionsOn(1)).toEqual([sid("ses_a"), sid("ses_b")])
    expect(registry.sessionsOn(2)).toEqual([sid("ses_c")])
    expect(registry.sessionsOn(9)).toEqual([])
  })

  test("clear wipes every mapping (terminate)", () => {
    const registry = createSessionRegistry()
    registry.reserve(sid("ses_a"), 1)
    registry.reserve(sid("ses_b"), 2)

    registry.clear()
    expect(registry.size).toBe(0)
    expect(registry.entries()).toEqual([])
  })
})
