// The session-worker log surface is Effect-native: emission sites call
// `Effect.log*`, the per-session boundary attaches `Effect.annotateLogs`, and
// the forwarding logger renders the operator-visible `session workers[...]`
// prefix. These tests port the old prefix/severity/session-id contract onto that
// path; the removed `WorkerLogger` callable and its bridge adapter are gone.

import { describe, expect, test } from "bun:test"
import { Effect, Logger } from "effect"
import {
  forwardingLogger,
  WORKER_LOG_PREFIX,
  workerLogMessage,
} from "@/session/worker/worker-logging"

interface CapturedLogEntry {
  readonly logLevel: string
  readonly message: unknown
}

const emit = async (
  entries: CapturedLogEntry[],
  log: (message: string, fields: Record<string, unknown>) => Effect.Effect<void>,
  message: string,
  fields: Record<string, unknown> = {},
  sessionID?: string,
): Promise<void> => {
  const sink = Logger.make<unknown, void>(({ logLevel, message }) => {
    entries.push({ logLevel, message })
  })
  const annotated = sessionID === undefined ? log(message, fields) : log(message, fields).pipe(Effect.annotateLogs({ sessionID }))
  const effect = annotated.pipe(Effect.provide(Logger.layer([forwardingLogger(sink)], { mergeWithExisting: false })))
  await Effect.runPromise(effect)
}

describe("worker log", () => {
  test("workerLogMessage prefixes the event with the worker log prefix", () => {
    expect(WORKER_LOG_PREFIX).toBe("session workers: ")
    expect(workerLogMessage("pool resolved")).toBe("session workers: pool resolved")
  })
})

describe("worker log severity routing", () => {
  test("defaults to INFO", async () => {
    const entries: CapturedLogEntry[] = []
    await emit(entries, (message, fields) => Effect.logInfo(message, fields), "pool resolved", { workers: 4 })
    expect(entries).toEqual([{ logLevel: "Info", message: ["session workers: pool resolved", { workers: 4 }] }])
  })

  test("routes WARNING and ERROR from the Effect call", async () => {
    const warn: CapturedLogEntry[] = []
    await emit(warn, (message, fields) => Effect.logWarning(message, fields), "queue full", { depth: 10 })
    expect(warn[0].logLevel).toBe("Warn")

    const error: CapturedLogEntry[] = []
    await emit(error, (message, fields) => Effect.logError(message, fields), "boot failed", { reason: "boom" })
    expect(error[0].logLevel).toBe("Error")
  })
})

describe("worker log session prefix", () => {
  test("hoists a boundary `sessionID` annotation into the message prefix as a bare ULID", async () => {
    const entries: CapturedLogEntry[] = []
    await emit(entries, (message, fields) => Effect.logInfo(message, fields), workerLogMessage("pool resolved"), { sessionID: "ses_abc" }, "ses_abc")

    expect(entries).toHaveLength(1)
    expect(entries[0].message).toEqual(["session workers[abc]: pool resolved", { sessionID: "ses_abc" }])
  })

  test("hoists an alternate `session.id` field into the message prefix as a bare ULID", async () => {
    const entries: CapturedLogEntry[] = []
    await emit(entries, (message, fields) => Effect.logInfo(message, fields), workerLogMessage("queue full"), { "session.id": "ses_xyz" })

    expect(entries[0].message).toEqual(["session workers[xyz]: queue full", { "session.id": "ses_xyz" }])
  })

  test("shows an id without the `ses_` prefix unchanged (no over-stripping)", async () => {
    const entries: CapturedLogEntry[] = []
    await emit(entries, (message, fields) => Effect.logInfo(message, fields), workerLogMessage("pool resolved"), { sessionID: "01JRAW" })

    expect(entries[0].message).toEqual(["session workers[01JRAW]: pool resolved", { sessionID: "01JRAW" }])
  })

  test("strips only a leading `ses_`: an id that merely contains it is shown as-is", async () => {
    const entries: CapturedLogEntry[] = []
    await emit(entries, (message, fields) => Effect.logInfo(message, fields), workerLogMessage("pool resolved"), { sessionID: "xses_abc" })

    expect(entries[0].message).toEqual(["session workers[xses_abc]: pool resolved", { sessionID: "xses_abc" }])
  })

  test("keeps the plain prefix when no session id is present", async () => {
    const entries: CapturedLogEntry[] = []
    await emit(entries, (message, fields) => Effect.logInfo(message, fields), workerLogMessage("pool resolved"), { workers: 4 })

    expect(entries[0].message).toEqual(["session workers: pool resolved", { workers: 4 }])
  })

  test("keeps the full `sessionID` field in the emitted fields alongside the bare prefix", async () => {
    const entries: CapturedLogEntry[] = []
    await emit(entries, (message, fields) => Effect.logInfo(message, fields), workerLogMessage("slot acquired"), { sessionID: "ses_keep", slot: 3 })

    expect(entries[0].message).toEqual(["session workers[keep]: slot acquired", { sessionID: "ses_keep", slot: 3 }])
  })

  test("treats an empty-string session id as absent", async () => {
    const entries: CapturedLogEntry[] = []
    await emit(entries, (message, fields) => Effect.logInfo(message, fields), workerLogMessage("pool resolved"), { sessionID: "" })

    expect(entries[0].message).toEqual(["session workers: pool resolved", { sessionID: "" }])
  })

  test("hoists the bare session prefix while routing severity", async () => {
    const entries: CapturedLogEntry[] = []
    await emit(entries, (message, fields) => Effect.logWarning(message, fields), workerLogMessage("worker crashed"), { sessionID: "ses_warn" })

    expect(entries[0].logLevel).toBe("Warn")
    expect(entries[0].message).toEqual(["session workers[warn]: worker crashed", { sessionID: "ses_warn" }])
  })
})
