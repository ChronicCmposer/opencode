import { readFileSync } from "node:fs"
import { Effect } from "effect"
import type { Connection, WriteOutcome } from "@opencode-ai/core/database/connection"
import type { RepositoryValues, WorkerRepository } from "@/session/worker/repository/types"
import { connectedRepository } from "@/session/worker/repository/write"
import { LEDGER_SQL_FALLBACK } from "./sql/fallback.gen"

export const TOOL_CALL_STATES = ["started", "finished", "indeterminate"] as const

export type ToolCallState = (typeof TOOL_CALL_STATES)[number]

/**
 * The ledger's raw connection shape. It remains the hub/test seam (and the
 * crash ledger's connection), but the tool-call writes themselves go through
 * the typed repository: a `WorkerRepository` is the worker's only write path.
 */
export type LedgerConnection = Connection

/**
 * The typed ledger surface. `WorkerRepository` is the worker's write path;
 * `LedgerConnection` is accepted only so the hub and the ledger's unit tests can
 * build an unbound repository over their own connection.
 */
export type ToolCallLedgerSource = WorkerRepository | LedgerConnection

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

/**
 * Serialize a standalone ledger write with the worker's other connection owner.
 * The tool path supplies the Effect SQL transaction acquirer, so a `started`
 * row cannot be written inside another tool's result transaction (and cannot be
 * erased by that transaction's rollback).
 */
export type LedgerExclusive = <A>(operation: () => A) => Effect.Effect<A>

export interface ToolCallLedgerOptions {
  readonly exclusive?: LedgerExclusive
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
    timeFinished:
      record.time_finished === null || record.time_finished === undefined ? null : Number(record.time_finished),
  }
}

function isTypedRepository(source: ToolCallLedgerSource): source is WorkerRepository {
  return "insert" in source && typeof source.insert === "function"
}

/**
 * Resolve the typed write surface: a worker repository, or an unbound repository
 * over a raw connection. Both carry the same savepoint-scoped `atomic`.
 */
function repositoryFor(source: ToolCallLedgerSource): WorkerRepository {
  if (isTypedRepository(source)) return source
  return connectedRepository(source)
}

function stored(repository: WorkerRepository, callID: string, sessionID: string): Record<string, unknown> {
  const row = repository.select("tool_call", { call_id: callID, session_id: sessionID })[0]
  if (row === undefined) throw new ToolCallLedgerError(`tool call ${callID} disappeared`)
  return row as Record<string, unknown>
}

function startWith(repository: WorkerRepository, input: StartInput): ToolCallRow {
  const callID = requireText(input.callID, "callID")
  const sessionID = requireText(input.sessionID, "sessionID")
  const tool = requireText(input.tool, "tool")
  const where: RepositoryValues<"tool_call"> = { call_id: callID, session_id: sessionID }
  if (repository.select("tool_call", where).length > 0) throw new ToolCallAlreadyStartedError(callID, sessionID)
  repository.insert("tool_call", {
    call_id: callID,
    session_id: sessionID,
    tool,
    side_effecting: input.sideEffecting === false ? 0 : 1,
    state: "started",
    outcome: null,
    time_started: input.timeStarted ?? Date.now(),
  })
  return rowFromRecord(stored(repository, callID, sessionID))
}

function finishWith(repository: WorkerRepository, input: FinishInput): ToolCallRow {
  const callID = requireText(input.callID, "callID")
  const sessionID = requireText(input.sessionID, "sessionID")
  const outcome = requireText(input.outcome, "outcome")
  const changed: WriteOutcome = repository.update(
    "tool_call",
    { state: "finished", outcome, time_finished: input.timeFinished ?? Date.now() },
    { call_id: callID, session_id: sessionID, state: "started" },
  )
  if (changed.changes !== 1) throw new ToolCallNotStartedError(callID, sessionID)
  return rowFromRecord(stored(repository, callID, sessionID))
}

/**
 * Mark a call started. Direct callers run outside a result transaction; the
 * production tool path uses {@link startStandaloneWith} so a parallel tool
 * cannot record its start inside another tool's result transaction.
 */
export function start(source: ToolCallLedgerSource, input: StartInput): ToolCallRow {
  return startWith(repositoryFor(source), input)
}

function startStandaloneWith(
  repository: WorkerRepository,
  exclusive: LedgerExclusive | undefined,
  input: StartInput,
): Effect.Effect<ToolCallRow> {
  const write = () => startWith(repository, input)
  return exclusive === undefined ? Effect.sync(write) : exclusive(write)
}

/**
 * Finish a started call. This write joins whatever transaction the worker's one
 * connection already owns; use {@link finishWithResult} when the result needs a
 * transaction the ledger owns.
 */
export function finish(source: ToolCallLedgerSource, input: FinishInput): ToolCallRow {
  return finishWith(repositoryFor(source), input)
}

function finishStandaloneWith(
  repository: WorkerRepository,
  exclusive: LedgerExclusive | undefined,
  input: FinishInput,
): Effect.Effect<ToolCallRow> {
  const write = () => finishWith(repository, input)
  return exclusive === undefined ? Effect.sync(write) : exclusive(write)
}

/**
 * Run the result write and the ledger finish atomically. A failure anywhere rolls
 * both back, so a crash between them leaves the orphaned `started` row instead of
 * a finished call whose result was lost.
 *
 * The scope comes from the repository's savepoint capability, never a raw
 * `BEGIN`/`COMMIT`: the worker connection is shared with the durable result
 * transaction, and a nested savepoint composes with that owner. With no outer
 * transaction the scope commits the pair itself.
 */
function finishWithResultWith<T>(repository: WorkerRepository, input: FinishInput, result: () => T): T {
  return repository.atomic(() => {
    finishWith(repository, input)
    return result()
  })
}

export function finishWithResult<T>(source: ToolCallLedgerSource, input: FinishInput, result: () => T): T {
  return finishWithResultWith(repositoryFor(source), input, result)
}

/** Calls still in `started`: indeterminate work that is never auto-retried. */
function indeterminateWith(repository: WorkerRepository, sessionID: string): readonly ToolCallRow[] {
  const required = requireText(sessionID, "sessionID")
  const rows = repository
    .select("tool_call", { session_id: required, state: "started" })
    .map((record) => ({ ...rowFromRecord(record as Record<string, unknown>), state: "indeterminate" as const }))
  return rows.sort((left, right) => left.timeStarted - right.timeStarted || left.callID.localeCompare(right.callID))
}

export function indeterminate(source: ToolCallLedgerSource, sessionID: string): readonly ToolCallRow[] {
  return indeterminateWith(repositoryFor(source), sessionID)
}

function listWith(repository: WorkerRepository, sessionID: string): readonly ToolCallRow[] {
  const required = requireText(sessionID, "sessionID")
  const rows = repository
    .select("tool_call", { session_id: required })
    .map((record) => rowFromRecord(record as Record<string, unknown>))
  return rows.sort((left, right) => left.timeStarted - right.timeStarted || left.callID.localeCompare(right.callID))
}

export function list(source: ToolCallLedgerSource, sessionID: string): readonly ToolCallRow[] {
  return listWith(repositoryFor(source), sessionID)
}

function hasSideEffectingStartedWith(repository: WorkerRepository, sessionID: string): boolean {
  return (
    repository.select("tool_call", {
      session_id: requireText(sessionID, "sessionID"),
      state: "started",
      side_effecting: 1,
    }).length > 0
  )
}

export function hasSideEffectingStarted(source: ToolCallLedgerSource, sessionID: string): boolean {
  return hasSideEffectingStartedWith(repositoryFor(source), sessionID)
}

export interface ToolCallLedgerHandle {
  /** Standalone start: waits for the connection's exclusive owner first. */
  readonly startStandalone: (input: StartInput) => Effect.Effect<ToolCallRow>
  /** Legacy synchronous start for direct callers outside a result transaction. */
  readonly start: (input: StartInput) => ToolCallRow
  /** Joining finish: a plain write that joins the owning result transaction. */
  readonly finish: (input: FinishInput) => ToolCallRow
  /** Standalone finish for abort/cleanup paths that own no transaction. */
  readonly finishStandalone: (input: FinishInput) => Effect.Effect<ToolCallRow>
  readonly finishWithResult: <T>(input: FinishInput, result: () => T) => T
  readonly indeterminate: (sessionID: string) => readonly ToolCallRow[]
  readonly list: (sessionID: string) => readonly ToolCallRow[]
  readonly hasSideEffectingStarted: (sessionID: string) => boolean
}

export function createToolCallLedger(
  source: ToolCallLedgerSource,
  options: ToolCallLedgerOptions = {},
): ToolCallLedgerHandle {
  const repository = repositoryFor(source)
  return {
    startStandalone: (input) => startStandaloneWith(repository, options.exclusive, input),
    start: (input) => startWith(repository, input),
    finish: (input) => finishWith(repository, input),
    finishStandalone: (input) => finishStandaloneWith(repository, options.exclusive, input),
    finishWithResult: (input, result) => finishWithResultWith(repository, input, result),
    indeterminate: (sessionID) => indeterminateWith(repository, sessionID),
    list: (sessionID) => listWith(repository, sessionID),
    hasSideEffectingStarted: (sessionID) => hasSideEffectingStartedWith(repository, sessionID),
  }
}

export * as ToolCallLedger from "./tool-call"
