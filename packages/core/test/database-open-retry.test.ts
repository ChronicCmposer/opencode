import { describe, expect, test } from "bun:test"
import { OPEN_BUSY_RETRIES, retryOpenWhileBusy, type BusyRetryEvent } from "../src/database/connection"

const busy = () => Object.assign(new Error("database is locked"), { name: "SQLiteError", code: "SQLITE_BUSY", errno: 5 })

describe("open busy retry", () => {
  test("backs off 20ms doubling to a 400ms cap before each replay", () => {
    const delays: number[] = []
    let attempts = 0
    const opened = retryOpenWhileBusy(
      () => {
        attempts += 1
        if (attempts <= OPEN_BUSY_RETRIES) throw busy()
        return "handle"
      },
      (ms) => delays.push(ms),
    )
    expect(opened).toBe("handle")
    expect(delays).toEqual([20, 40, 80, 160, 320, 400, 400, 400])
  })

  test("gives up with the busy error after the retry budget", () => {
    let attempts = 0
    expect(() =>
      retryOpenWhileBusy(
        () => {
          attempts += 1
          throw busy()
        },
        () => {},
      ),
    ).toThrow("database is locked")
    expect(attempts).toBe(OPEN_BUSY_RETRIES + 1)
  })

  test("does not retry a non-busy failure", () => {
    let attempts = 0
    expect(() =>
      retryOpenWhileBusy(
        () => {
          attempts += 1
          throw new Error("unable to open database file")
        },
        () => {},
      ),
    ).toThrow("unable to open")
    expect(attempts).toBe(1)
  })

  test("reports every busy retry with its attempt and delay before succeeding", () => {
    const events: BusyRetryEvent[] = []
    let attempts = 0
    const opened = retryOpenWhileBusy(
      () => {
        attempts += 1
        if (attempts <= 2) throw busy()
        return "handle"
      },
      () => {},
      (event) => events.push(event),
    )

    expect(opened).toBe("handle")
    expect(events.map((event) => ({ attempt: event.attempt, delayMs: event.delayMs }))).toEqual([
      { attempt: 0, delayMs: 20 },
      { attempt: 1, delayMs: 40 },
    ])
    expect(events.every((event) => event.error instanceof Error)).toBe(true)
  })

  test("does not report a non-busy failure as a retry", () => {
    const events: BusyRetryEvent[] = []
    expect(() =>
      retryOpenWhileBusy(
        () => {
          throw new Error("unable to open database file")
        },
        () => {},
        (event) => events.push(event),
      ),
    ).toThrow("unable to open")

    expect(events).toEqual([])
  })
})
