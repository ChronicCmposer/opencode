import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { workerLogMessage } from "@/session/worker/worker-logging"
import { lineFor, recordingLogger, recordingPublisher } from "./recorders"

describe("recordingLogger", () => {
  test("captures the emitted message, fields and level", () => {
    const { bridge, lines } = recordingLogger()

    bridge.fork(Effect.logInfo(workerLogMessage("session admitted"), { workerID: 1 }))

    expect(lines).toEqual([
      { message: workerLogMessage("session admitted"), fields: { workerID: 1 }, level: "info" },
    ])
  })

  test("captures a warning severity", () => {
    const { bridge, lines } = recordingLogger()

    bridge.fork(Effect.logWarning(workerLogMessage("worker crashed"), { workerID: 2 }))

    expect(lines).toEqual([
      { message: workerLogMessage("worker crashed"), fields: { workerID: 2 }, level: "warn" },
    ])
  })
})

describe("lineFor", () => {
  test("returns the recorded line for an event", () => {
    const { bridge, lines } = recordingLogger()
    bridge.fork(Effect.logInfo(workerLogMessage("pool resolved"), { count: 2 }))

    expect(lineFor(lines, "pool resolved").fields).toEqual({ count: 2 })
  })

  test("fails loudly when the event was never logged, naming what was seen", () => {
    const { bridge, lines } = recordingLogger()
    bridge.fork(Effect.logInfo(workerLogMessage("session admitted"), { workerID: 1 }))

    expect(() => lineFor(lines, "pool resolved")).toThrow(/no log line for "pool resolved"/)
  })
})

describe("recordingPublisher", () => {
  test("records every published event in order", () => {
    const { publisher, published } = recordingPublisher()

    publisher.publish({ type: "one", data: {} } as never)
    publisher.publishDefinition({ type: "two" } as never, { value: 2 } as never)

    expect(published).toEqual([
      { type: "one", data: {} },
      { type: "two", data: { value: 2 } },
    ])
  })
})
