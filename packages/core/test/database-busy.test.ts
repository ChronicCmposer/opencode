// b-core — bounded busy-retry for the main SQLite connection.
//
// The main-process read/write connection contends with the worker writer isolate.
// When a lock is held longer than `busy_timeout`, the driver surfaces a busy
// failure (`LockTimeoutError` reason / `SQLITE_BUSY` / "database is locked").
// `withBusyRetry` recovers with bounded exponential backoff and still fails loud
// once the budget is spent. These tests pin the classifier, the retry control
// flow, the exhaustion error, and the environment config.

import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { LockTimeoutError, SqlError } from "effect/unstable/sql/SqlError"
import {
  BusyRetriesExhaustedError,
  isBusySqlError,
  resolveBusyRetryConfig,
  withBusyRetry,
} from "@opencode-ai/core/database/busy"

const lockTimeoutSqlError = () =>
  new SqlError({ reason: new LockTimeoutError({ cause: new Error("database is locked") }) })

const busyFailure = { code: "SQLITE_BUSY" } as const
const nonBusyFailure = new Error("no such table: users")

describe("busy classifier", () => {
  test("recognizes a SqlError whose reason is a LockTimeoutError", () => {
    expect(isBusySqlError(lockTimeoutSqlError())).toBe(true)
  })

  test("recognizes SQLITE_BUSY and SQLITE_LOCKED codes", () => {
    expect(isBusySqlError({ code: "SQLITE_BUSY" })).toBe(true)
    expect(isBusySqlError({ code: "SQLITE_LOCKED" })).toBe(true)
    // The writer-side classifier also reads `sqliteCode`; mirror that here.
    expect(isBusySqlError({ sqliteCode: "SQLITE_BUSY" })).toBe(true)
  })

  test("recognizes a message containing database is locked", () => {
    expect(isBusySqlError(new Error("database is locked"))).toBe(true)
    expect(isBusySqlError(new Error("SQLITE_BUSY: database is locked"))).toBe(true)
  })

  test("rejects non-busy errors and non-objects", () => {
    expect(isBusySqlError(nonBusyFailure)).toBe(false)
    expect(isBusySqlError({ code: "SQLITE_CONSTRAINT" })).toBe(false)
    expect(isBusySqlError(undefined)).toBe(false)
    expect(isBusySqlError("SQLITE_BUSY")).toBe(false)
  })
})

describe("withBusyRetry", () => {
  test("retries a busy failure until it succeeds, with bounded exponential backoff", async () => {
    let attempts = 0
    const delays: number[] = []
    const effect = Effect.suspend(() => {
      attempts += 1
      return attempts <= 2 ? Effect.fail(busyFailure) : Effect.succeed("committed")
    })

    const result = await Effect.runPromise(
      withBusyRetry(effect, {
        retries: 5,
        baseDelayMs: 10,
        maxDelayMs: 100,
        sleep: (ms) =>
          Effect.sync(() => {
            delays.push(ms)
          }),
      }),
    )

    expect(result).toBe("committed")
    expect(attempts).toBe(3)
    expect(delays).toEqual([10, 20])
  })

  test("caps the backoff at maxDelayMs", async () => {
    let attempts = 0
    const delays: number[] = []
    const effect = Effect.suspend(() => {
      attempts += 1
      return attempts <= 4 ? Effect.fail(busyFailure) : Effect.succeed("ok")
    })

    await Effect.runPromise(
      withBusyRetry(effect, {
        retries: 5,
        baseDelayMs: 10,
        maxDelayMs: 25,
        sleep: (ms) =>
          Effect.sync(() => {
            delays.push(ms)
          }),
      }),
    )

    expect(delays).toEqual([10, 20, 25, 25])
  })

  test("exhausts with BusyRetriesExhaustedError carrying attempts and last cause", async () => {
    let attempts = 0
    const effect = Effect.suspend(() => {
      attempts += 1
      return Effect.fail(busyFailure)
    })

    const defect = await Effect.runPromise(
      withBusyRetry(effect, { retries: 2, baseDelayMs: 1, maxDelayMs: 2, sleep: () => Effect.void }).pipe(
        Effect.catchDefect((cause) => Effect.succeed(cause)),
      ),
    )

    expect(defect).toBeInstanceOf(BusyRetriesExhaustedError)
    const error = defect as BusyRetriesExhaustedError
    expect(error.attempts).toBe(3)
    expect(error.lastCause).toEqual(busyFailure)
    expect(attempts).toBe(3)
  })

  test("retries: 0 exhausts on the first busy failure", async () => {
    let attempts = 0
    const effect = Effect.suspend(() => {
      attempts += 1
      return Effect.fail(busyFailure)
    })

    const defect = await Effect.runPromise(
      withBusyRetry(effect, { retries: 0, baseDelayMs: 1, maxDelayMs: 1, sleep: () => Effect.void }).pipe(
        Effect.catchDefect((cause) => Effect.succeed(cause)),
      ),
    )

    expect(defect).toBeInstanceOf(BusyRetriesExhaustedError)
    expect(attempts).toBe(1)
  })

  test("does not retry non-busy failures and passes the typed error through", async () => {
    let attempts = 0
    const effect = Effect.suspend(() => {
      attempts += 1
      return Effect.fail(nonBusyFailure)
    })

    const error = await Effect.runPromise(
      withBusyRetry(effect, { retries: 5, baseDelayMs: 1, maxDelayMs: 1, sleep: () => Effect.void }).pipe(Effect.flip),
    )

    expect(error).toBe(nonBusyFailure)
    expect(attempts).toBe(1)
  })

  test("a successful attempt passes through unchanged", async () => {
    const result = await Effect.runPromise(withBusyRetry(Effect.succeed(42), { sleep: () => Effect.void }))
    expect(result).toBe(42)
  })
})

describe("resolveBusyRetryConfig", () => {
  test("applies defaults when the environment is empty", () => {
    expect(resolveBusyRetryConfig({})).toEqual({ retries: 5, baseDelayMs: 50, maxDelayMs: 1000 })
  })

  test("parses overrides and falls back on malformed values", () => {
    expect(
      resolveBusyRetryConfig({
        OPENCODE_DB_BUSY_RETRIES: "9",
        OPENCODE_DB_BUSY_BASE_DELAY_MS: "5",
        OPENCODE_DB_BUSY_MAX_DELAY_MS: "250",
      }),
    ).toEqual({ retries: 9, baseDelayMs: 5, maxDelayMs: 250 })

    expect(
      resolveBusyRetryConfig({
        OPENCODE_DB_BUSY_RETRIES: "not-a-number",
        OPENCODE_DB_BUSY_BASE_DELAY_MS: "-1",
        OPENCODE_DB_BUSY_MAX_DELAY_MS: "3.5",
      }),
    ).toEqual({ retries: 5, baseDelayMs: 50, maxDelayMs: 1000 })
  })
})
