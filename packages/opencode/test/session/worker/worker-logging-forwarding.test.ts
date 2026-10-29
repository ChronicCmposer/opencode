// The forwarding logger is the single user-visible path for session-worker
// lines. These tests pin the new Effect-native contract: emission sites call
// `Effect.log*`, the per-session/per-worker fiber boundary attaches
// `Effect.annotateLogs({ sessionID })`, and the forwarding logger renders the
// `session workers[<bare id>]: <event>` prefix without truncating the fields.

import { describe, expect, test } from "bun:test"
import { Effect, Layer, Logger, References } from "effect"
import { forwardingLogger, workerLogMessage } from "@/session/worker/worker-logging"

interface CapturedEntry {
  readonly level: string
  readonly message: unknown
  readonly annotations: Record<string, unknown>
}

/** A recording sink with no formatting of its own. */
function recordingSink(entries: CapturedEntry[]): Logger.Logger<unknown, void> {
  return Logger.make<unknown, void>((options) => {
    entries.push({
      level: options.logLevel,
      message: options.message,
      annotations: { ...options.fiber.getRef(References.CurrentLogAnnotations) },
    })
  })
}

const forwardingLayerFor = (entries: CapturedEntry[]) =>
  Logger.layer([forwardingLogger(recordingSink(entries))], { mergeWithExisting: false })

describe("worker forwarding logger: annotations at the fiber boundary", () => {
  test("hoists a boundary `sessionID` annotation into the bare-ULID message prefix", async () => {
    const entries: CapturedEntry[] = []
    await Effect.runPromise(
      Effect.gen(function* () {
        yield* Effect.logInfo(workerLogMessage("pool resolved"), { workers: 4 })
      }).pipe(
        Effect.annotateLogs({ sessionID: "ses_abc" }),
        Effect.provide(forwardingLayerFor(entries)),
      ),
    )

    expect(entries).toHaveLength(1)
    expect(entries[0].level).toBe("Info")
    expect(entries[0].message).toEqual(["session workers[abc]: pool resolved", { workers: 4 }])
    // The full branded id remains available to the sink as an annotation.
    expect(entries[0].annotations).toEqual({ sessionID: "ses_abc" })
  })

  test("keeps the plain prefix when no session id is attached", async () => {
    const entries: CapturedEntry[] = []
    await Effect.runPromise(
      Effect.gen(function* () {
        yield* Effect.logInfo(workerLogMessage("pool resolved"), { workers: 4 })
      }).pipe(Effect.provide(forwardingLayerFor(entries))),
    )

    expect(entries[0].message).toEqual(["session workers: pool resolved", { workers: 4 }])
  })

  test("routes the Effect severity instead of a `level` field", async () => {
    const entries: CapturedEntry[] = []
    await Effect.runPromise(
      Effect.gen(function* () {
        yield* Effect.logWarning(workerLogMessage("queue full"), { depth: 10 })
        yield* Effect.logError(workerLogMessage("boot failed"), { reason: "boom" })
      }).pipe(
        Effect.annotateLogs({ sessionID: "ses_warn" }),
        Effect.provide(forwardingLayerFor(entries)),
      ),
    )

    expect(entries.map((entry) => entry.level)).toEqual(["Warn", "Error"])
    expect(entries[0].message).toEqual(["session workers[warn]: queue full", { depth: 10 }])
    expect(entries[1].message).toEqual(["session workers[warn]: boot failed", { reason: "boom" }])
  })

  test("keeps a full, branded `sessionID` field alongside the bare prefix", async () => {
    const entries: CapturedEntry[] = []
    await Effect.runPromise(
      Effect.gen(function* () {
        yield* Effect.logInfo(workerLogMessage("slot acquired"), { sessionID: "ses_keep", slot: 3 })
      }).pipe(Effect.provide(forwardingLayerFor(entries))),
    )

    expect(entries[0].message).toEqual(["session workers[keep]: slot acquired", { sessionID: "ses_keep", slot: 3 }])
  })

  test("a bare event without the worker prefix still gains the full anchor", async () => {
    const entries: CapturedEntry[] = []
    await Effect.runPromise(
      Effect.gen(function* () {
        yield* Effect.logInfo("bare event", { slot: 1 })
      }).pipe(
        Effect.annotateLogs({ sessionID: "01JRAW" }),
        Effect.provide(forwardingLayerFor(entries)),
      ),
    )

    expect(entries[0].message).toEqual(["session workers[01JRAW]: bare event", { slot: 1 }])
  })
})
