import { describe, expect, test } from "bun:test"
import { createBunSqliteConnection } from "@/session/writer/connection"
import {
  LEDGER_SQL,
  ToolCallAlreadyStartedError,
  ToolCallNotStartedError,
  createToolCallLedger,
  type LedgerConnection,
} from "@/session/ledger/tool-call"
import { crashes, record, workerCrashed, WORKER_CRASH_PHASES } from "@/session/ledger/crash"

function ledgerConnection(): LedgerConnection {
  const connection = createBunSqliteConnection({ filename: ":memory:" })
  connection.exec(LEDGER_SQL)
  return connection
}

describe("tool_call ledger (D10)", () => {
  test("the schema is unique on (call_id, session) and rejects a duplicate start", () => {
    const connection = ledgerConnection()
    const ledger = createToolCallLedger(connection)
    ledger.start({ callID: "call_1", sessionID: "ses_1", tool: "write", timeStarted: 1 })

    expect(() => ledger.start({ callID: "call_1", sessionID: "ses_1", tool: "write" })).toThrow(
      ToolCallAlreadyStartedError,
    )
    // The same call id in another session is a distinct call.
    expect(ledger.start({ callID: "call_1", sessionID: "ses_2", tool: "write" }).sessionID).toBe("ses_2")
  })

  test("started is durable before the effect and an unfinished call is indeterminate", () => {
    const connection = ledgerConnection()
    const ledger = createToolCallLedger(connection)
    ledger.start({ callID: "call_1", sessionID: "ses_1", tool: "bash", timeStarted: 10 })

    expect(ledger.list("ses_1")[0]?.state).toBe("started")
    expect(ledger.indeterminate("ses_1")).toEqual([
      expect.objectContaining({ callID: "call_1", state: "indeterminate", sideEffecting: true, timeFinished: null }),
    ])
    expect(ledger.hasSideEffectingStarted("ses_1")).toBe(true)
  })

  test("finish only accepts a started call and records the outcome", () => {
    const connection = ledgerConnection()
    const ledger = createToolCallLedger(connection)
    expect(() => ledger.finish({ callID: "call_missing", sessionID: "ses_1", outcome: "ok" })).toThrow(
      ToolCallNotStartedError,
    )

    ledger.start({ callID: "call_1", sessionID: "ses_1", tool: "read", sideEffecting: false })
    const finished = ledger.finish({ callID: "call_1", sessionID: "ses_1", outcome: "ok", timeFinished: 20 })
    expect(finished).toMatchObject({ state: "finished", outcome: "ok", timeFinished: 20 })
    expect(ledger.indeterminate("ses_1")).toEqual([])
    expect(ledger.hasSideEffectingStarted("ses_1")).toBe(false)
  })

  test("the result transaction and the finish commit or roll back together", () => {
    const connection = ledgerConnection()
    const ledger = createToolCallLedger(connection)
    ledger.start({ callID: "call_1", sessionID: "ses_1", tool: "write" })

    expect(() =>
      ledger.finishWithResult({ callID: "call_1", sessionID: "ses_1", outcome: "ok" }, () => {
        throw new Error("result write failed")
      }),
    ).toThrow("result write failed")
    expect(ledger.indeterminate("ses_1")[0]?.state).toBe("indeterminate")

    ledger.finishWithResult({ callID: "call_1", sessionID: "ses_1", outcome: "ok" }, () => undefined)
    expect(ledger.list("ses_1")[0]?.state).toBe("finished")
  })
})

describe("worker_crash persistence (D8)", () => {
  test("every phase persists and returns the identical typed payload", () => {
    const connection = ledgerConnection()
    for (const phase of WORKER_CRASH_PHASES) {
      const payload = record(connection, {
        sessionID: "ses_1",
        workerID: 7,
        phase,
        lastCommittedMessageID: phase === "boot" ? null : "msg_1",
        inFlightCallIDs: phase === "tool" ? ["call_1"] : [],
        reason: `crashed during ${phase}`,
        crashID: `crash_${phase}`,
        timeCreated: 1,
      })
      expect(payload.retryable).toBe(false)
      expect(payload.phase).toBe(phase)
    }

    const stored = crashes(connection, "ses_1")
    expect(stored).toHaveLength(WORKER_CRASH_PHASES.length)
    expect(stored.map((row) => row.phase).sort()).toEqual([...WORKER_CRASH_PHASES].sort())
    expect(stored.every((row) => row.retryable === false)).toBe(true)
    expect(stored.find((row) => row.phase === "tool")?.inFlightCallIDs).toEqual(["call_1"])
  })

  test("a crash after the durable commit keeps the committed message and empty in-flight set", () => {
    const connection = ledgerConnection()
    const payload = workerCrashed({
      sessionID: "ses_1",
      workerID: 9,
      phase: "commit",
      lastCommittedMessageID: "msg_committed",
      inFlightCallIDs: [],
      reason: "isolate died after COMMIT",
    })
    record(connection, { ...payload, crashID: "crash_commit", timeCreated: 1 })

    expect(crashes(connection, "ses_1")[0]).toMatchObject({
      phase: "commit",
      lastCommittedMessageID: "msg_committed",
      inFlightCallIDs: [],
      retryable: false,
    })
  })
})
