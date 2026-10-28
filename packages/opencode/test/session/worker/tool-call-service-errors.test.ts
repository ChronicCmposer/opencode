// The ToolCallLedger service boundary is additive: the synchronous
// `createToolCallLedger` adapter keeps throwing the legacy error classes, while
// `fromHandle` maps those same throws to typed, tagged service failures. These
// tests pin both halves of that contract plus the finishWithResult span name.

import { describe, expect, test } from "bun:test"
import { Cause, Effect, Exit, Tracer } from "effect"
import {
  LEDGER_SQL,
  StaleWorkerGenerationError,
  ToolCallAlreadyStartedError,
  ToolCallAlreadyStartedServiceError,
  ToolCallLedgerError,
  ToolCallLedgerServiceError,
  ToolCallNotStartedError,
  ToolCallNotStartedServiceError,
  ToolCallStaleGenerationServiceError,
  createToolCallLedger,
  fromHandle,
  make,
} from "@/session/ledger/tool-call"
import { createBunSqliteConnection } from "@/session/writer/connection"
import { it } from "../../lib/effect"

const sessionID = "ses_tool_call_service_errors"

function freshConnection() {
  const connection = createBunSqliteConnection({ filename: ":memory:" })
  connection.exec(LEDGER_SQL)
  return connection
}

function fencedConnection(leaseOwner: string) {
  const connection = freshConnection()
  connection.exec(
    "CREATE TABLE event_sequence (aggregate_id TEXT PRIMARY KEY, seq INTEGER NOT NULL, owner_id TEXT, worker_owner_id TEXT, worker_heartbeat_at INTEGER, worker_expires_at INTEGER)",
  )
  connection.run("INSERT INTO event_sequence (aggregate_id, seq, worker_owner_id) VALUES (?, -1, ?)", [
    sessionID,
    leaseOwner,
  ])
  return connection
}

function squashedFailure(exit: Exit.Exit<unknown, unknown>): unknown {
  return Exit.isFailure(exit) ? Cause.squash(exit.cause) : undefined
}

describe("tool-call ledger direct adapter", () => {
  test("still throws the legacy ledger error classes", () => {
    const ledger = createToolCallLedger(freshConnection())
    const input = { callID: "direct-call", sessionID, tool: "ledger" } as const

    ledger.start(input)

    let duplicate: unknown
    try {
      ledger.start(input)
    } catch (error) {
      duplicate = error
    }
    expect(duplicate).toBeInstanceOf(ToolCallAlreadyStartedError)
    expect(duplicate).toBeInstanceOf(ToolCallLedgerError)

    let missing: unknown
    try {
      ledger.finish({ callID: "never-started", sessionID, outcome: "ok" })
    } catch (error) {
      missing = error
    }
    expect(missing).toBeInstanceOf(ToolCallNotStartedError)
    expect(missing).toBeInstanceOf(ToolCallLedgerError)
  })
})

describe("tool-call ledger service errors", () => {
  it.live("maps a legacy throw to a tagged service error with cause and legacy tag", () =>
    Effect.gen(function* () {
      const service = yield* make(freshConnection())
      const input = { callID: "service-call", sessionID, tool: "ledger" } as const
      yield* service.start(input)

      const duplicate = squashedFailure(yield* Effect.exit(service.start(input)))
      expect(duplicate).toBeInstanceOf(ToolCallAlreadyStartedServiceError)
      if (!(duplicate instanceof ToolCallAlreadyStartedServiceError)) return

      expect(duplicate._tag).toBe("ToolCallLedger.AlreadyStarted")
      expect(duplicate.message).toContain("already recorded")
      expect(duplicate.legacyTag).toBe("TOOL_CALL_LEDGER")
      expect(duplicate.cause).toBeInstanceOf(ToolCallAlreadyStartedError)

      const missing = squashedFailure(
        yield* Effect.exit(service.finish({ callID: "never-started", sessionID, outcome: "ok" })),
      )
      expect(missing).toBeInstanceOf(ToolCallNotStartedServiceError)
      if (!(missing instanceof ToolCallNotStartedServiceError)) return

      expect(missing._tag).toBe("ToolCallLedger.NotStarted")
      expect(missing.legacyTag).toBe("TOOL_CALL_LEDGER")
      expect(missing.cause).toBeInstanceOf(ToolCallNotStartedError)
    }),
  )

  it.live("leaves a non-ledger defect as a defect", () => {
    const handle = createToolCallLedger(freshConnection())
    const service = fromHandle({
      ...handle,
      start: () => {
        throw new Error("not a ledger failure")
      },
    })

    return Effect.gen(function* () {
      const exit = yield* Effect.exit(service.start({ callID: "defect-call", sessionID, tool: "ledger" }))
      expect(Exit.isFailure(exit)).toBe(true)
      if (!Exit.isFailure(exit)) return

      expect(exit.cause.reasons.some(Cause.isDieReason)).toBe(true)
      const defect = Cause.squash(exit.cause)
      expect(defect).toBeInstanceOf(Error)
      if (!(defect instanceof Error)) return
      expect(defect.message).toBe("not a ledger failure")
      expect(defect).not.toBeInstanceOf(ToolCallLedgerServiceError)
    })
  })

  it.live("maps a stale fenced write to the generation service error", () =>
    Effect.gen(function* () {
      const service = yield* make(fencedConnection("worker-current-generation"), {
        fence: { ownerID: "worker-stale-generation" },
      })

      const failure = squashedFailure(
        yield* Effect.exit(service.start({ callID: "stale-call", sessionID, tool: "bash" })),
      )
      expect(failure).toBeInstanceOf(ToolCallStaleGenerationServiceError)
      if (!(failure instanceof ToolCallStaleGenerationServiceError)) return

      expect(failure._tag).toBe("ToolCallLedger.StaleGeneration")
      expect(failure.operation).toBe("start")
      expect(failure.legacyTag).toBe("TOOL_CALL_LEDGER")
      expect(failure.cause).toBeInstanceOf(StaleWorkerGenerationError)
      expect(yield* service.list(sessionID)).toEqual([])
    }),
  )

  it.live("maps a generic ledger failure to the fallback service error", () =>
    Effect.gen(function* () {
      const service = yield* make(freshConnection())

      const failure = squashedFailure(yield* Effect.exit(service.start({ callID: "", sessionID, tool: "ledger" })))
      expect(failure).toBeInstanceOf(ToolCallLedgerServiceError)
      if (!(failure instanceof ToolCallLedgerServiceError)) return

      expect(failure._tag).toBe("ToolCallLedger.ServiceError")
      expect(failure.operation).toBe("start")
      expect(failure.message).toContain("callID")
      expect(failure.legacyTag).toBe("TOOL_CALL_LEDGER")
      expect(failure.cause).toBeInstanceOf(ToolCallLedgerError)
      expect(failure.cause).not.toBeInstanceOf(ToolCallAlreadyStartedError)
    }),
  )

  it.live("maps the standalone start and finish paths", () =>
    Effect.gen(function* () {
      const service = yield* make(freshConnection())
      const input = { callID: "standalone-call", sessionID, tool: "ledger" } as const

      expect(yield* service.startStandalone(input)).toMatchObject({ callID: input.callID, state: "started" })

      const duplicate = squashedFailure(yield* Effect.exit(service.startStandalone(input)))
      expect(duplicate).toBeInstanceOf(ToolCallAlreadyStartedServiceError)
      if (duplicate instanceof ToolCallAlreadyStartedServiceError) {
        expect(duplicate._tag).toBe("ToolCallLedger.AlreadyStarted")
        expect(duplicate.operation).toBe("startStandalone")
      }

      expect(
        yield* service.finishStandalone({ callID: input.callID, sessionID, outcome: "ok" }),
      ).toMatchObject({ state: "finished", outcome: "ok" })

      const missing = squashedFailure(
        yield* Effect.exit(service.finishStandalone({ callID: "never-started", sessionID, outcome: "ok" })),
      )
      expect(missing).toBeInstanceOf(ToolCallNotStartedServiceError)
      if (missing instanceof ToolCallNotStartedServiceError) {
        expect(missing._tag).toBe("ToolCallLedger.NotStarted")
        expect(missing.operation).toBe("finishStandalone")
      }
    }),
  )

  it.live("maps a synchronous exclusive throw on the standalone paths", () => {
    const exclusive = <A>(_operation: () => A): Effect.Effect<A> => {
      throw new ToolCallLedgerError("exclusive unavailable")
    }

    return Effect.gen(function* () {
      const service = yield* make(freshConnection(), { exclusive })
      const start = squashedFailure(
        yield* Effect.exit(service.startStandalone({ callID: "sync-start", sessionID, tool: "ledger" })),
      )
      expect(start).toBeInstanceOf(ToolCallLedgerServiceError)
      if (start instanceof ToolCallLedgerServiceError) expect(start.operation).toBe("startStandalone")

      const finish = squashedFailure(
        yield* Effect.exit(service.finishStandalone({ callID: "sync-finish", sessionID, outcome: "ok" })),
      )
      expect(finish).toBeInstanceOf(ToolCallLedgerServiceError)
      if (finish instanceof ToolCallLedgerServiceError) expect(finish.operation).toBe("finishStandalone")
    })
  })

  it.live("names the finishWithResult adapter span", () => {
    const names: string[] = []
    const tracer = Tracer.make({
      span(options) {
        names.push(options.name)
        return new Tracer.NativeSpan(options)
      },
    })

    return Effect.gen(function* () {
      const service = yield* make(freshConnection())
      const input = { callID: "span-call", sessionID, tool: "ledger" } as const
      yield* service.start(input)

      const result = yield* service.finishWithResult({ callID: input.callID, sessionID, outcome: "ok" }, () => "result")

      expect(result).toBe("result")
      expect(yield* service.list(sessionID)).toEqual([
        expect.objectContaining({ callID: input.callID, state: "finished", outcome: "ok" }),
      ])
    }).pipe(
      Effect.provideService(Tracer.Tracer, tracer),
      Effect.map(() => expect(names).toContain("ToolCallLedger.finishWithResult")),
    )
  })
})
