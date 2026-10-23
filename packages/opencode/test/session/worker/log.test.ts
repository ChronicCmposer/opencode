// FOUNDATION (dbwal-12-00) — shared logging seam for the session-worker subsystem.
//
// Purely additive: a worker logger is a synchronous function that either no-ops
// (flag-off / no logger wired) or forks a severity-matched Effect through the
// `EffectBridge` (preserving the ambient logger/InstanceRef/WorkspaceRef). These
// tests pin the contract surfaces: the prefixed message, the no-op, the bridge
// adapter, and severity routing (explicit argument > case-insensitive `level`
// field > INFO, with the `level` field stripped from output).

import { describe, expect, test } from "bun:test"
import { Effect, Fiber, Logger } from "effect"
import type { EffectBridge } from "@/effect/bridge"
import {
  noopWorkerLogger,
  WORKER_LOG_PREFIX,
  workerLoggerFromBridge,
  workerLogMessage,
} from "@/session/worker/log"

/** A minimal bridge whose `fork` records the effect it is handed (never runs it). */
function recordingBridge(captured: Effect.Effect<void>[]): EffectBridge.Shape {
  return {
    promise: () => Promise.reject(new Error("recording bridge: promise is unused")),
    fork: <A, E, R>(effect: Effect.Effect<A, E, R>): Fiber.Fiber<A, E> => {
      captured.push(effect as unknown as Effect.Effect<void>)
      return undefined as unknown as Fiber.Fiber<A, E>
    },
    run: <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E> =>
      effect as unknown as Effect.Effect<A, E>,
    bind: <Args extends readonly unknown[], Result>(fn: (...args: Args) => Result) => fn,
  }
}

interface CapturedLogEntry {
  readonly logLevel: string
  readonly message: unknown
}

/**
 * A bridge whose `fork` runs the effect synchronously under a capturing logger.
 * The severity tests only need the logging side effect, so the returned fiber is
 * a dummy.
 */
function severityBridge(entries: CapturedLogEntry[]): EffectBridge.Shape {
  const logger = Logger.make<unknown, void>(({ logLevel, message }) => {
    entries.push({ logLevel, message })
  })
  return {
    promise: () => Promise.reject(new Error("severity bridge: promise is unused")),
    fork: <A, E, R>(effect: Effect.Effect<A, E, R>): Fiber.Fiber<A, E> => {
      // Test-only cast: the logging seam forks a fully-provided effect (R = never).
      Effect.runSync((effect as unknown as Effect.Effect<A, E, never>).pipe(Effect.provide(Logger.layer([logger]))))
      return undefined as unknown as Fiber.Fiber<A, E>
    },
    run: <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E> =>
      effect as unknown as Effect.Effect<A, E>,
    bind: <Args extends readonly unknown[], Result>(fn: (...args: Args) => Result) => fn,
  }
}

describe("worker log", () => {
  test("workerLogMessage prefixes the event with the worker log prefix", () => {
    expect(WORKER_LOG_PREFIX).toBe("session workers: ")
    expect(workerLogMessage("pool resolved")).toBe("session workers: pool resolved")
  })

  test("noopWorkerLogger is callable and returns undefined", () => {
    expect(noopWorkerLogger("x", {})).toBeUndefined()
  })

  test("workerLoggerFromBridge forks exactly one Effect.logInfo per call", () => {
    const captured: Effect.Effect<void>[] = []
    const log = workerLoggerFromBridge(recordingBridge(captured))

    log("pool resolved", { workers: 4 })

    expect(captured).toHaveLength(1)
    const messages: unknown[] = []
    Effect.runSync(
      captured[0].pipe(
        Effect.provide(
          Logger.layer([
            Logger.make<unknown, void>((entry) => {
              messages.push(entry.message)
            }),
          ]),
        ),
      ),
    )
    expect(messages).toEqual([["pool resolved", { workers: 4 }]])
  })
})

describe("worker log severity routing", () => {
  test("defaults to INFO when neither a level argument nor a level field is present", () => {
    const entries: CapturedLogEntry[] = []
    const log = workerLoggerFromBridge(severityBridge(entries))

    log("pool resolved", { workers: 4 })

    expect(entries).toEqual([{ logLevel: "Info", message: ["pool resolved", { workers: 4 }] }])
  })

  test("routes an uppercase WARN level field to WARNING", () => {
    const entries: CapturedLogEntry[] = []
    const log = workerLoggerFromBridge(severityBridge(entries))

    log("queue full", { level: "WARN", depth: 10 })

    expect(entries).toHaveLength(1)
    expect(entries[0].logLevel).toBe("Warn")
  })

  test("routes a lowercase warn level field to WARNING", () => {
    const entries: CapturedLogEntry[] = []
    const log = workerLoggerFromBridge(severityBridge(entries))

    log("queue full", { level: "warn", depth: 10 })

    expect(entries).toHaveLength(1)
    expect(entries[0].logLevel).toBe("Warn")
  })

  test("routes an ERROR level field to ERROR", () => {
    const entries: CapturedLogEntry[] = []
    const log = workerLoggerFromBridge(severityBridge(entries))

    log("boot failed", { level: "ERROR", reason: "boom" })

    expect(entries).toHaveLength(1)
    expect(entries[0].logLevel).toBe("Error")
  })

  test("ignores an unrecognized level field (treats it as INFO) without throwing", () => {
    const entries: CapturedLogEntry[] = []
    const log = workerLoggerFromBridge(severityBridge(entries))

    expect(() => log("odd", { level: "fatal" })).not.toThrow()
    expect(entries).toHaveLength(1)
    expect(entries[0].logLevel).toBe("Info")
  })

  test("strips the level field from the emitted fields", () => {
    const entries: CapturedLogEntry[] = []
    const log = workerLoggerFromBridge(severityBridge(entries))

    log("queue full", { level: "warn", depth: 10 })

    expect(entries).toHaveLength(1)
    expect(entries[0].message).toEqual(["queue full", { depth: 10 }])
  })

  test("an explicit level argument overrides a level field", () => {
    const entries: CapturedLogEntry[] = []
    const log = workerLoggerFromBridge(severityBridge(entries))

    log("boot failed", { level: "info", reason: "boom" }, "error")

    expect(entries).toHaveLength(1)
    expect(entries[0].logLevel).toBe("Error")
    expect(entries[0].message).toEqual(["boot failed", { reason: "boom" }])
  })
})

describe("worker log session prefix", () => {
  test("hoists a `sessionID` field into the message prefix as a bare ULID", () => {
    const entries: CapturedLogEntry[] = []
    const log = workerLoggerFromBridge(severityBridge(entries))

    log(workerLogMessage("pool resolved"), { sessionID: "ses_abc" })

    expect(entries).toHaveLength(1)
    expect(entries[0].message).toEqual(["session workers[abc]: pool resolved", { sessionID: "ses_abc" }])
  })

  test("hoists an alternate `session.id` field into the message prefix as a bare ULID", () => {
    const entries: CapturedLogEntry[] = []
    const log = workerLoggerFromBridge(severityBridge(entries))

    log(workerLogMessage("queue full"), { "session.id": "ses_xyz" })

    expect(entries).toHaveLength(1)
    expect(entries[0].message).toEqual(["session workers[xyz]: queue full", { "session.id": "ses_xyz" }])
  })

  test("shows an id without the `ses_` prefix unchanged (no over-stripping)", () => {
    const entries: CapturedLogEntry[] = []
    const log = workerLoggerFromBridge(severityBridge(entries))

    log(workerLogMessage("pool resolved"), { sessionID: "01JRAW" })

    expect(entries).toHaveLength(1)
    expect(entries[0].message).toEqual(["session workers[01JRAW]: pool resolved", { sessionID: "01JRAW" }])
  })

  test("strips only a leading `ses_`: an id that merely contains it is shown as-is", () => {
    const entries: CapturedLogEntry[] = []
    const log = workerLoggerFromBridge(severityBridge(entries))

    log(workerLogMessage("pool resolved"), { sessionID: "xses_abc" })

    expect(entries).toHaveLength(1)
    expect(entries[0].message).toEqual(["session workers[xses_abc]: pool resolved", { sessionID: "xses_abc" }])
  })

  test("keeps the plain prefix when no session id is present", () => {
    const entries: CapturedLogEntry[] = []
    const log = workerLoggerFromBridge(severityBridge(entries))

    log(workerLogMessage("pool resolved"), { workers: 4 })

    expect(entries).toHaveLength(1)
    expect(entries[0].message).toEqual(["session workers: pool resolved", { workers: 4 }])
  })

  test("keeps the full `sessionID` field in the emitted fields alongside the bare prefix", () => {
    const entries: CapturedLogEntry[] = []
    const log = workerLoggerFromBridge(severityBridge(entries))

    log(workerLogMessage("slot acquired"), { sessionID: "ses_keep", slot: 3 })

    expect(entries).toHaveLength(1)
    expect(entries[0].message).toEqual([
      "session workers[keep]: slot acquired",
      { sessionID: "ses_keep", slot: 3 },
    ])
  })

  test("treats an empty-string session id as absent", () => {
    const entries: CapturedLogEntry[] = []
    const log = workerLoggerFromBridge(severityBridge(entries))

    log(workerLogMessage("pool resolved"), { sessionID: "" })

    expect(entries).toHaveLength(1)
    expect(entries[0].message).toEqual(["session workers: pool resolved", { sessionID: "" }])
  })

  test("hoists the bare session prefix while routing severity", () => {
    const entries: CapturedLogEntry[] = []
    const log = workerLoggerFromBridge(severityBridge(entries))

    log(workerLogMessage("worker crashed"), { sessionID: "ses_warn", level: "warn" })

    expect(entries).toHaveLength(1)
    expect(entries[0].logLevel).toBe("Warn")
    expect(entries[0].message).toEqual(["session workers[warn]: worker crashed", { sessionID: "ses_warn" }])
  })
})
