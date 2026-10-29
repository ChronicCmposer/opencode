import { describe, expect, test } from "bun:test"
import { OPEN_BUSY_RETRIES, retryOpenWhileBusy } from "../src/database/connection"

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
})
