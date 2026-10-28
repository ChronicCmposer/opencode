import { readFileSync } from "node:fs"
import { Context, Effect, Layer } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
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

/**
 * The generation-fence column is additive. `CREATE TABLE IF NOT EXISTS` cannot
 * add it to a `tool_call` table that a P0 build already created, so the hub must
 * issue this ALTER before it marks that database current. Without it the worker
 * boot registry gate sees a column drift and every fenced write fails.
 */
const FENCE_COLUMN = "worker_owner_id"
const ADD_FENCE_COLUMN = `ALTER TABLE tool_call ADD COLUMN ${FENCE_COLUMN} TEXT`

/** The read-only column probe; the `Connection` and its async twin both satisfy it. */
export interface LedgerSchemaQuery {
  all(sql: string): ReadonlyArray<Record<string, unknown>>
}

/** The hub executor's read surface (the off-loop schema-ensure path). */
export interface LedgerSchemaExecutor {
  query(sql: string): Promise<ReadonlyArray<Record<string, unknown>>>
}

/** No table yet (a fresh create) and an already-current table both need no DDL. */
function fenceUpgrade(columns: readonly string[]): readonly string[] {
  if (columns.length === 0) return []
  return columns.includes(FENCE_COLUMN) ? [] : [ADD_FENCE_COLUMN]
}

/** The upgrade statements for the live `tool_call` table; empty when current. */
export function toolCallUpgradeStatements(connection: LedgerSchemaQuery): readonly string[] {
  return fenceUpgrade(connection.all("PRAGMA table_info(tool_call)").map((column) => String(column.name)))
}

export async function toolCallUpgradeStatementsAsync(executor: LedgerSchemaExecutor): Promise<readonly string[]> {
  return fenceUpgrade((await executor.query("PRAGMA table_info(tool_call)")).map((column) => String(column.name)))
}

/** Apply the additive upgrade in place (used by tests and the sync hub gate). */
export function upgradeToolCallSchema(connection: LedgerSchemaQuery & { exec(sql: string): void }): void {
  for (const statement of toolCallUpgradeStatements(connection)) connection.exec(statement)
}

export interface ToolCallRow {
  readonly callID: string
  readonly sessionID: string
  readonly tool: string
  readonly sideEffecting: boolean
  readonly state: ToolCallState
  readonly outcome: string | null
  readonly timeStarted: number
  readonly timeFinished: number | null
  /** The lease-generation owner that recorded the row; null for unbound/tests. */
  readonly workerOwnerID: string | null
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
  /**
   * Bind every write to one worker generation. When set, a start/finish whose
   * generation is not the session's current event-sequence lease owner is
   * rejected inside the same transaction as the write.
   */
  readonly fence?: ToolCallFence
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

/**
 * A write from a superseded worker generation. The session's event-sequence
 * lease already belongs to another owner, so this isolate may no longer record
 * or settle tool calls for it.
 */
export class StaleWorkerGenerationError extends ToolCallLedgerError {
  constructor(sessionID: string, expected: string, actual: string | undefined) {
    super(`worker generation ${expected} is stale for session ${sessionID}: current lease owner is ${actual ?? "none"}`)
    this.name = "StaleWorkerGenerationError"
  }
}

/** The lease-generation identity of the ledger's writer. */
export interface ToolCallFence {
  readonly ownerID: string
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
    outcome: typeof record.outcome === "string" ? record.outcome : null,
    timeStarted: Number(record.time_started),
    timeFinished:
      record.time_finished === null || record.time_finished === undefined ? null : Number(record.time_finished),
    workerOwnerID: typeof record.worker_owner_id === "string" ? record.worker_owner_id : null,
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

/** The event-sequence lease owner, the authoritative worker generation. */
function currentLeaseOwner(repository: WorkerRepository, sessionID: string): string | undefined {
  const row = repository.select("event_sequence", { aggregate_id: sessionID })[0]
  const owner = row?.worker_owner_id
  return typeof owner === "string" && owner.length > 0 ? owner : undefined
}

function assertCurrentGeneration(
  repository: WorkerRepository,
  sessionID: string,
  fence: ToolCallFence | undefined,
): void {
  if (fence === undefined) return
  const owner = currentLeaseOwner(repository, sessionID)
  if (owner !== undefined && owner !== fence.ownerID) {
    throw new StaleWorkerGenerationError(sessionID, fence.ownerID, owner)
  }
}

function startWith(repository: WorkerRepository, input: StartInput, fence?: ToolCallFence): ToolCallRow {
  const callID = requireText(input.callID, "callID")
  const sessionID = requireText(input.sessionID, "sessionID")
  const tool = requireText(input.tool, "tool")
  // The generation check, duplicate check and insert share one transaction so a
  // successor's claim cannot interleave between the fence read and the write.
  return repository.atomic(() => {
    assertCurrentGeneration(repository, sessionID, fence)
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
      ...(fence === undefined ? {} : { worker_owner_id: fence.ownerID }),
    })
    return rowFromRecord(stored(repository, callID, sessionID))
  })
}

function finishWith(repository: WorkerRepository, input: FinishInput, fence?: ToolCallFence): ToolCallRow {
  const callID = requireText(input.callID, "callID")
  const sessionID = requireText(input.sessionID, "sessionID")
  const outcome = requireText(input.outcome, "outcome")
  return repository.atomic(() => {
    assertCurrentGeneration(repository, sessionID, fence)
    const where: RepositoryValues<"tool_call"> = {
      call_id: callID,
      session_id: sessionID,
      state: "started",
      ...(fence === undefined ? {} : { worker_owner_id: fence.ownerID }),
    }
    const changed: WriteOutcome = repository.update(
      "tool_call",
      { state: "finished", outcome, time_finished: input.timeFinished ?? Date.now() },
      where,
    )
    if (changed.changes !== 1) throw new ToolCallNotStartedError(callID, sessionID)
    return rowFromRecord(stored(repository, callID, sessionID))
  })
}

/**
 * Mark a call started. Direct callers run outside a result transaction; the
 * production tool path uses {@link startStandaloneWith} so a parallel tool
 * cannot record its start inside another tool's result transaction.
 */
export function start(source: ToolCallLedgerSource, input: StartInput, fence?: ToolCallFence): ToolCallRow {
  return startWith(repositoryFor(source), input, fence)
}

function startStandaloneWith(
  repository: WorkerRepository,
  exclusive: LedgerExclusive | undefined,
  input: StartInput,
  fence?: ToolCallFence,
): Effect.Effect<ToolCallRow> {
  const write = () => startWith(repository, input, fence)
  return exclusive === undefined ? Effect.sync(write) : exclusive(write)
}

/**
 * Finish a started call. This write joins whatever transaction the worker's one
 * connection already owns; use {@link finishWithResult} when the result needs a
 * transaction the ledger owns.
 */
export function finish(source: ToolCallLedgerSource, input: FinishInput, fence?: ToolCallFence): ToolCallRow {
  return finishWith(repositoryFor(source), input, fence)
}

function finishStandaloneWith(
  repository: WorkerRepository,
  exclusive: LedgerExclusive | undefined,
  input: FinishInput,
  fence?: ToolCallFence,
): Effect.Effect<ToolCallRow> {
  const write = () => finishWith(repository, input, fence)
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
function finishWithResultWith<T>(
  repository: WorkerRepository,
  input: FinishInput,
  result: () => T,
  fence?: ToolCallFence,
): T {
  return repository.atomic(() => {
    finishWith(repository, input, fence)
    return result()
  })
}

export function finishWithResult<T>(
  source: ToolCallLedgerSource,
  input: FinishInput,
  result: () => T,
  fence?: ToolCallFence,
): T {
  return finishWithResultWith(repositoryFor(source), input, result, fence)
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
  const fence = options.fence
  return {
    startStandalone: (input) => startStandaloneWith(repository, options.exclusive, input, fence),
    start: (input) => startWith(repository, input, fence),
    finish: (input) => finishWith(repository, input, fence),
    finishStandalone: (input) => finishStandaloneWith(repository, options.exclusive, input, fence),
    finishWithResult: (input, result) => finishWithResultWith(repository, input, result, fence),
    indeterminate: (sessionID) => indeterminateWith(repository, sessionID),
    list: (sessionID) => listWith(repository, sessionID),
    hasSideEffectingStarted: (sessionID) => hasSideEffectingStartedWith(repository, sessionID),
  }
}

// ---------------------------------------------------------------------------
// Effect service shape.
//
// The `ToolCallLedger` service is the same ledger behavior expressed as an
// Effect service. `createToolCallLedger` stays the synchronous adapter; every
// service method lifts one handle operation without changing the repository,
// fence, or savepoint semantics. State stays in the adapter's closure.
// ---------------------------------------------------------------------------

/** The Effect-facing ledger surface. `ledger` is the synchronous adapter. */
export interface Interface {
  readonly ledger: ToolCallLedgerHandle
  readonly startStandalone: (input: StartInput) => Effect.Effect<ToolCallRow>
  readonly start: (input: StartInput) => Effect.Effect<ToolCallRow>
  readonly finish: (input: FinishInput) => Effect.Effect<ToolCallRow>
  readonly finishStandalone: (input: FinishInput) => Effect.Effect<ToolCallRow>
  readonly finishWithResult: <T>(input: FinishInput, result: () => T) => Effect.Effect<T>
  readonly indeterminate: (sessionID: string) => Effect.Effect<readonly ToolCallRow[]>
  readonly list: (sessionID: string) => Effect.Effect<readonly ToolCallRow[]>
  readonly hasSideEffectingStarted: (sessionID: string) => Effect.Effect<boolean>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/ToolCallLedger") {}

/** Adapt the synchronous handle without owning, reordering, or retrying anything. */
export function fromHandle(ledger: ToolCallLedgerHandle): Interface {
  const startStandalone = Effect.fn("ToolCallLedger.startStandalone")(function* (input: StartInput) {
    return yield* ledger.startStandalone(input)
  })
  const start = Effect.fn("ToolCallLedger.start")(function* (input: StartInput) {
    return ledger.start(input)
  })
  const finish = Effect.fn("ToolCallLedger.finish")(function* (input: FinishInput) {
    return ledger.finish(input)
  })
  const finishStandalone = Effect.fn("ToolCallLedger.finishStandalone")(function* (input: FinishInput) {
    return yield* ledger.finishStandalone(input)
  })
  const indeterminate = Effect.fn("ToolCallLedger.indeterminate")(function* (sessionID: string) {
    return ledger.indeterminate(sessionID)
  })
  const list = Effect.fn("ToolCallLedger.list")(function* (sessionID: string) {
    return ledger.list(sessionID)
  })
  const hasSideEffectingStarted = Effect.fn("ToolCallLedger.hasSideEffectingStarted")(function* (sessionID: string) {
    return ledger.hasSideEffectingStarted(sessionID)
  })
  return {
    ledger,
    startStandalone,
    start,
    finish,
    finishStandalone,
    finishWithResult: (input, result) => Effect.sync(() => ledger.finishWithResult(input, result)),
    indeterminate,
    list,
    hasSideEffectingStarted,
  }
}

/** Build a ledger over the given source; no resource needs a scope. */
export function make(source: ToolCallLedgerSource, options: ToolCallLedgerOptions = {}): Effect.Effect<Interface> {
  return Effect.sync(() => fromHandle(createToolCallLedger(source, options)))
}

/** Install a `ToolCallLedger` implementation into the context. */
export function layer(source: ToolCallLedgerSource, options: ToolCallLedgerOptions = {}): Layer.Layer<Service> {
  return Layer.effect(Service, make(source, options))
}

/** The unbound graph node for {@link Service}; this lane does not wire a consumer. */
export function node(
  source: ToolCallLedgerSource,
  options: ToolCallLedgerOptions = {},
): LayerNode.Node<Service, never, undefined> {
  return LayerNode.make({ service: Service, layer: layer(source, options), deps: [] })
}

export * as ToolCallLedger from "./tool-call"
