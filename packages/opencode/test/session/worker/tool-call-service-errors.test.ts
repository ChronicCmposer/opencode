// The ToolCallLedger service boundary is additive: the synchronous
// `createToolCallLedger` adapter keeps throwing the legacy error classes, while
// `fromHandle` maps those same throws to typed, tagged service failures. These
// tests pin both halves of that contract plus the finishWithResult span name.

import { describe, expect, test } from "bun:test"
import { Cause, Effect, Exit, Tracer } from "effect"
import {
  LEDGER_SQL,
  ToolCallAlreadyStartedError,
  ToolCallAlreadyStartedServiceError,
  ToolCallLedgerError,
  ToolCallNotStartedError,
  ToolCallNotStartedServiceError,
  createToolCallLedger,
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
