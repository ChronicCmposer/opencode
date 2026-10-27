import { readFileSync } from "node:fs"
import { LEDGER_SQL_FALLBACK } from "./sql/fallback.gen"

export const TOOL_CALL_STATES = ["started", "finished", "indeterminate"] as const

export type ToolCallState = (typeof TOOL_CALL_STATES)[number]

/**
 * The ledger's connection surface. It is deliberately structural: the Bus
 * connection, the worker WAL connection, and a transaction handle all satisfy it,
 * so a finish can run inside the same transaction as its durable result.
 */
export interface LedgerConnection {
  all(sql: string, params?: ReadonlyArray<unknown>): ReadonlyArray<Record<string, unknown>>
  run(sql: string, params?: ReadonlyArray<unknown>): { readonly changes: number }
  exec(sql: string): void
}

/**
 * The hub-only schema. A worker never executes DDL; tests and the migration
 * generator apply `sql/001-tool-call.sql`. The generated fallback is the bundled-binary
 * copy (a compiled entry has no adjacent `.sql` file).
 */

export const LEDGER_SQL = (() => {
  try {
    return readFileSync(new URL("./sql/001-tool-call.sql", import.meta.url), "utf8")
  } catch {
    return LEDGER_SQL_FALLBACK
  }
})()

export interface ToolCallRow {
  readonly callID: string
  readonly sessionID: string
  readonly tool: string
  readonly sideEffecting: boolean
  readonly state: ToolCallState
  readonly outcome: string | null
  readonly timeStarted: number
  readonly timeFinished: number | null
}

export interface StartInput {
  readonly callID: string
  readonly sessionID: string
  readonly tool: string
  /** Missing side-effecting tools are treated as side-effecting (the safe default). */
  readonly sideEffecting?: boolean
  readonly timeStarted?: number
}

export interface FinishInput {
  readonly callID: string
  readonly sessionID: string
  readonly outcome: string
  readonly timeFinished?: number
}

export class ToolCallLedgerError extends Error {
  readonly tag = "TOOL_CALL_LEDGER"
  constructor(message: string) {
    super(message)
    this.name = "ToolCallLedgerError"
  }
}

export class ToolCallAlreadyStartedError extends ToolCallLedgerError {
  constructor(callID: string, sessionID: string) {
    super(`tool call ${callID} is already recorded for session ${sessionID}`)
    this.name = "ToolCallAlreadyStartedError"
  }
}

export class ToolCallNotStartedError extends ToolCallLedgerError {
  constructor(callID: string, sessionID: string) {
    super(`tool call ${callID} was never started for session ${sessionID}`)
    this.name = "ToolCallNotStartedError"
  }
}

function requireText(value: string, field: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new ToolCallLedgerError(`${field} must be a non-empty string`)
  }
  return value
}

function rowFromRecord(record: Record<string, unknown>): ToolCallRow {
  const state = record.state
  if (state !== "started" && state !== "finished") {
    throw new ToolCallLedgerError(`tool_call.state is not a stored state: ${String(state)}`)
  }
  return {
    callID: String(record.call_id),
    sessionID: String(record.session_id),
    tool: String(record.tool),
    sideEffecting: Number(record.side_effecting) === 1,
    state,
    outcome: record.outcome === null || record.outcome === undefined ? null : String(record.outcome),
    timeStarted: Number(record.time_started),
    timeFinished: record.time_finished === null || record.time_finished === undefined ? null : Number(record.time_finished),
  }
}

/**
 * Mark a call started before its effect begins. A duplicate `(call_id, session)`
 * fails loud: replaying the effect is never the recovery path.
 */
export function start(connection: LedgerConnection, input: StartInput): ToolCallRow {
  const callID = requireText(input.callID, "callID")
  const sessionID = requireText(input.sessionID, "sessionID")
  const tool = requireText(input.tool, "tool")
  const existing = connection.all("SELECT call_id FROM tool_call WHERE call_id = ? AND session_id = ?", [
    callID,
    sessionID,
  ])
  if (existing.length > 0) throw new ToolCallAlreadyStartedError(callID, sessionID)
  connection.run(
    "INSERT INTO tool_call (call_id, session_id, tool, side_effecting, state, outcome, time_started) VALUES (?, ?, ?, ?, 'started', NULL, ?)",
    [callID, sessionID, tool, input.sideEffecting === false ? 0 : 1, input.timeStarted ?? Date.now()],
  )
  const stored = connection.all("SELECT * FROM tool_call WHERE call_id = ? AND session_id = ?", [callID, sessionID])[0]
  if (stored === undefined) throw new ToolCallLedgerError(`tool call ${callID} disappeared after start`)
  return rowFromRecord(stored)
}

/**
 * Finish a started call. This statement is meant to run inside the same
 * transaction as the durable result; use {@link finishWithResult} when the result
 * needs a transaction the ledger owns.
 */
export function finish(connection: LedgerConnection, input: FinishInput): ToolCallRow {
  const callID = requireText(input.callID, "callID")
  const sessionID = requireText(input.sessionID, "sessionID")
  const outcome = requireText(input.outcome, "outcome")
  const changed = connection.run(
    "UPDATE tool_call SET state = 'finished', outcome = ?, time_finished = ? WHERE call_id = ? AND session_id = ? AND state = 'started'",
    [outcome, input.timeFinished ?? Date.now(), callID, sessionID],
  )
  if (changed.changes !== 1) throw new ToolCallNotStartedError(callID, sessionID)
  const stored = connection.all("SELECT * FROM tool_call WHERE call_id = ? AND session_id = ?", [callID, sessionID])[0]
  if (stored === undefined) throw new ToolCallLedgerError(`tool call ${callID} disappeared after finish`)
  return rowFromRecord(stored)
}

/**
 * Run the result write and the ledger finish atomically. A failure anywhere rolls
 * both back, so a crash between them leaves the orphaned `started` row instead of
 * a finished call whose result was lost.
 */
export function finishWithResult<T>(connection: LedgerConnection, input: FinishInput, result: () => T): T {
  connection.exec("BEGIN IMMEDIATE")
  try {
    finish(connection, input)
    const value = result()
    connection.exec("COMMIT")
    return value
  } catch (error) {
    connection.exec("ROLLBACK")
    throw error
  }
}

/** Calls still in `started`: indeterminate work that is never auto-retried. */
export function indeterminate(connection: LedgerConnection, sessionID: string): readonly ToolCallRow[] {
  return connection
    .all("SELECT * FROM tool_call WHERE session_id = ? AND state = 'started' ORDER BY time_started, call_id", [
      requireText(sessionID, "sessionID"),
    ])
    .map((record) => ({ ...rowFromRecord(record), state: "indeterminate" as const }))
}

export function list(connection: LedgerConnection, sessionID: string): readonly ToolCallRow[] {
  return connection
    .all("SELECT * FROM tool_call WHERE session_id = ? ORDER BY time_started, call_id", [
      requireText(sessionID, "sessionID"),
    ])
    .map(rowFromRecord)
}

export function hasSideEffectingStarted(connection: LedgerConnection, sessionID: string): boolean {
  return (
    connection.all(
      "SELECT 1 AS present FROM tool_call WHERE session_id = ? AND state = 'started' AND side_effecting = 1 LIMIT 1",
      [requireText(sessionID, "sessionID")],
    ).length > 0
  )
}

export interface ToolCallLedgerHandle {
  readonly start: (input: StartInput) => ToolCallRow
  readonly finish: (input: FinishInput) => ToolCallRow
  readonly finishWithResult: <T>(input: FinishInput, result: () => T) => T
  readonly indeterminate: (sessionID: string) => readonly ToolCallRow[]
  readonly list: (sessionID: string) => readonly ToolCallRow[]
  readonly hasSideEffectingStarted: (sessionID: string) => boolean
}

export function createToolCallLedger(connection: LedgerConnection): ToolCallLedgerHandle {
  return {
    start: (input) => start(connection, input),
    finish: (input) => finish(connection, input),
    finishWithResult: (input, result) => finishWithResult(connection, input, result),
    indeterminate: (sessionID) => indeterminate(connection, sessionID),
    list: (sessionID) => list(connection, sessionID),
    hasSideEffectingStarted: (sessionID) => hasSideEffectingStarted(connection, sessionID),
  }
}

export * as ToolCallLedger from "./tool-call"
