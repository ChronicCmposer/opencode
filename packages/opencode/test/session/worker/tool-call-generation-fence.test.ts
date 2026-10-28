// Tool-call rows are fenced to one worker generation.
//
// The session's `event_sequence.worker_owner_id` is the authoritative lease
// owner (each worker generation gets a unique owner id). The ledger records it
// on every row and rejects a start or finish from a superseded generation in the
// same transaction as the write.

import { describe, expect, test } from "bun:test"
import { createBunSqliteConnection } from "@/session/writer/connection"
import {
  LEDGER_SQL,
  StaleWorkerGenerationError,
  createToolCallLedger,
} from "@/session/ledger/tool-call"

const SESSION = "ses_generation_fence"
const CURRENT_OWNER = "worker-2-100-generation"
const STALE_OWNER = "worker-1-90-generation"

function fencedLedger(ownerID: string, leaseOwner: string = CURRENT_OWNER) {
  const connection = createBunSqliteConnection({ filename: ":memory:" })
  connection.exec(LEDGER_SQL)
  connection.exec(
    "CREATE TABLE event_sequence (aggregate_id TEXT PRIMARY KEY, seq INTEGER NOT NULL, owner_id TEXT, worker_owner_id TEXT, worker_heartbeat_at INTEGER, worker_expires_at INTEGER)",
  )
  connection.run(
    "INSERT INTO event_sequence (aggregate_id, seq, worker_owner_id) VALUES (?, -1, ?)",
    [SESSION, leaseOwner],
  )
  return { connection, ledger: createToolCallLedger(connection, { fence: { ownerID } }) }
}

describe("tool-call generation fence", () => {
  test("a stale generation cannot start a call and leaves no row", () => {
    const { ledger } = fencedLedger(STALE_OWNER)

    expect(() => ledger.start({ callID: "call_stale", sessionID: SESSION, tool: "bash" })).toThrow(
      StaleWorkerGenerationError,
    )
    expect(ledger.list(SESSION)).toEqual([])
  })

  test("the current generation records its owner on the row", () => {
    const { ledger } = fencedLedger(CURRENT_OWNER)
    const row = ledger.start({ callID: "call_current", sessionID: SESSION, tool: "bash" })

    expect(row.workerOwnerID).toBe(CURRENT_OWNER)
    expect(ledger.list(SESSION)).toEqual([
      expect.objectContaining({ callID: "call_current", state: "started", workerOwnerID: CURRENT_OWNER }),
    ])
  })

  test("a stale generation cannot finish a successor's call", () => {
    const current = fencedLedger(CURRENT_OWNER)
    current.ledger.start({ callID: "call_current", sessionID: SESSION, tool: "bash" })

    // A stale ledger over the same database (its owned connection would be the
    // real worker's); its fence no longer matches the lease.
    const stale = createToolCallLedger(current.connection, { fence: { ownerID: STALE_OWNER } })
    expect(() => stale.finish({ callID: "call_current", sessionID: SESSION, outcome: "ok" })).toThrow(
      StaleWorkerGenerationError,
    )
    expect(current.ledger.list(SESSION)[0]?.state).toBe("started")

    const finished = current.ledger.finish({ callID: "call_current", sessionID: SESSION, outcome: "ok" })
    expect(finished).toMatchObject({ state: "finished", outcome: "ok", workerOwnerID: CURRENT_OWNER })
  })

  test("the fence and the write share a transaction", () => {
    const { connection, ledger } = fencedLedger(CURRENT_OWNER)
    ledger.start({ callID: "call_tx", sessionID: SESSION, tool: "bash" })

    // Move the lease to a successor between calls; the next finish must reject
    // and must not partially update the row.
    connection.run("UPDATE event_sequence SET worker_owner_id = ? WHERE aggregate_id = ?", [STALE_OWNER, SESSION])

    expect(() => ledger.finish({ callID: "call_tx", sessionID: SESSION, outcome: "ok" })).toThrow(
      StaleWorkerGenerationError,
    )
    expect(connection.all("SELECT state, outcome FROM tool_call WHERE call_id = ?", ["call_tx"])).toEqual([
      { state: "started", outcome: null },
    ])
  })
})
