import { describe, expect, test } from "bun:test"
import { workerLogMessage } from "@/session/worker/log"
import { lineFor, recordingLogger, recordingPublisher } from "./recorders"

describe("recordingLogger", () => {
  test("captures message and fields, omitting an absent level", () => {
    const { log, lines } = recordingLogger()

    log(workerLogMessage("session admitted"), { workerID: 1 })

    expect(lines).toEqual([{ message: workerLogMessage("session admitted"), fields: { workerID: 1 } }])
  })

  test("captures an explicit severity level", () => {
    const { log, lines } = recordingLogger()

    log(workerLogMessage("worker crashed"), { workerID: 2 }, "error")

    expect(lines).toEqual([{ message: workerLogMessage("worker crashed"), fields: { workerID: 2 }, level: "error" }])
  })
})

describe("lineFor", () => {
  test("returns the recorded line for an event", () => {
    const { log, lines } = recordingLogger()
    log(workerLogMessage("pool resolved"), { count: 2 })

    expect(lineFor(lines, "pool resolved").fields).toEqual({ count: 2 })
  })

  test("fails loudly when the event was never logged, naming what was seen", () => {
    const { log, lines } = recordingLogger()
    log(workerLogMessage("session admitted"), { workerID: 1 })

    expect(() => lineFor(lines, "pool resolved")).toThrow(/no log line for "pool resolved"/)
  })
})

describe("recordingPublisher", () => {
  test("records every published event in order", () => {
    const { publisher, published } = recordingPublisher()

    publisher.publish({ type: "session.cancelled", data: { sessionID: "ses_1" } })
    publisher.publish({ type: "session.error", data: { message: "boom" } })

    expect(published).toEqual([
      { type: "session.cancelled", data: { sessionID: "ses_1" } },
      { type: "session.error", data: { message: "boom" } },
    ])
  })
})
