// Lease heartbeat/release writes must survive a transient SQLite busy lock:
// retry within a bounded budget, then swallow and log the failure so a dispose
// still completes and a heartbeat interval never throws.

import { beforeEach, describe, expect, mock, test } from "bun:test"
import { WORKER_LOG_PREFIX } from "@/session/worker/worker-logging"

interface TraceInput {
  readonly prefix: string
  readonly level?: "debug" | "info" | "warn" | "error"
  readonly event: string
  readonly fields?: Record<string, unknown>
}

const traces: TraceInput[] = []

// Replace the file sink with an in-memory spy before the worker module loads, so
// the assertions observe exactly what `runLeaseWrite` emits.
await mock.module("@opencode-ai/core/observability/sync-trace", () => ({
  syncTrace: (input: TraceInput) => {
    traces.push(input)
  },
  traceOnce: () => true,
  dbTrace: () => {},
}))

const { LEASE_BUSY_RETRY_CONFIG, runLeaseWrite } = await import("@/session/worker/session-worker")

const SESSION_ID = "ses_lease_busy"

const busy = () => Object.assign(new Error("SQLITE_BUSY: database is locked"), { code: "SQLITE_BUSY" })

describe("runLeaseWrite", () => {
  beforeEach(() => {
    traces.length = 0
  })

  test("retries a busy heartbeat until it succeeds", () => {
    let attempts = 0
    const result = runLeaseWrite("heartbeat", SESSION_ID, () => {
      attempts += 1
      if (attempts < 3) throw busy()
    })

    expect(result).toBeUndefined()
    expect(attempts).toBe(3)
    expect(traces).toEqual([])
  })

  test("retries a busy release until it succeeds", () => {
    let attempts = 0
    const result = runLeaseWrite("release", SESSION_ID, () => {
      attempts += 1
      if (attempts < 3) throw busy()
    })

    expect(result).toBeUndefined()
    expect(attempts).toBe(3)
    expect(traces).toEqual([])
  })

  test("heartbeat exhaustion is logged at debug and swallowed", () => {
    let attempts = 0
    expect(() =>
      runLeaseWrite("heartbeat", SESSION_ID, () => {
        attempts += 1
        throw busy()
      }),
    ).not.toThrow()

    expect(attempts).toBe(LEASE_BUSY_RETRY_CONFIG.retries + 1)
    expect(traces).toHaveLength(1)
    expect(traces[0]).toMatchObject({
      prefix: WORKER_LOG_PREFIX,
      level: "debug",
      event: "worker lease heartbeat failed",
      fields: { sessionID: SESSION_ID },
    })
    expect(traces[0]?.fields?.reason).toContain("remained busy")
  })

  test("release exhaustion is logged at warn and swallowed", () => {
    let attempts = 0
    expect(() =>
      runLeaseWrite("release", SESSION_ID, () => {
        attempts += 1
        throw busy()
      }),
    ).not.toThrow()

    expect(attempts).toBe(LEASE_BUSY_RETRY_CONFIG.retries + 1)
    expect(traces).toHaveLength(1)
    expect(traces[0]).toMatchObject({
      prefix: WORKER_LOG_PREFIX,
      level: "warn",
      event: "worker lease release failed",
      fields: { sessionID: SESSION_ID },
    })
    expect(traces[0]?.fields?.reason).toContain("remained busy")
  })

  test("a non-busy failure is not retried but is still swallowed and logged", () => {
    let attempts = 0
    expect(() =>
      runLeaseWrite("release", SESSION_ID, () => {
        attempts += 1
        throw new Error("no such table: event_sequence")
      }),
    ).not.toThrow()

    expect(attempts).toBe(1)
    expect(traces).toHaveLength(1)
    expect(traces[0]?.fields?.reason).toContain("no such table: event_sequence")
  })
})
