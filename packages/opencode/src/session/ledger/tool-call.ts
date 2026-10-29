import { readFileSync } from "node:fs"
import { Context, Effect, Layer, Option, Schema } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Database } from "@opencode-ai/core/database/database"
import type { Connection, WriteOutcome } from "@opencode-ai/core/database/connection"
import type { RepositoryValues, WorkerRepository } from "@/session/worker/repository/types"
import { connectedRepository } from "@/session/worker/repository/write"
import { WorkerDb } from "@/session/worker/db"
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

/** The lease-generation identity of the ledger's writer. */
export interface ToolCallFence {
  readonly ownerID: string
}

export class ToolCallLedgerError extends Schema.TaggedErrorClass<ToolCallLedgerError>("ToolCallLedgerError")(
  "ToolCallLedger.Error",
  { message: Schema.String },
) {
  readonly tag = "TOOL_CALL_LEDGER"
  constructor(message: string) {
    super({ message })
    this.name = "ToolCallLedgerError"
  }
}

export class ToolCallAlreadyStartedError extends ToolCallLedgerError {
  constructor(callID: string, sessionID: string) {
    super(`tool call ${callID} is already recorded for session ${sessionID}`)
    Object.defineProperty(this, "_tag", { value: "ToolCallLedger.AlreadyStarted", enumerable: true, configurable: true })
    this.name = "ToolCallAlreadyStartedError"
  }
}

export class ToolCallNotStartedError extends ToolCallLedgerError {
  constructor(callID: string, sessionID: string) {
    super(`tool call ${callID} was never started for session ${sessionID}`)
    Object.defineProperty(this, "_tag", { value: "ToolCallLedger.NotStarted", enumerable: true, configurable: true })
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
    Object.defineProperty(this, "_tag", { value: "ToolCallLedger.StaleGeneration", enumerable: true, configurable: true })
    this.name = "StaleWorkerGenerationError"
  }
}

/**
 * The service-level failure for a ledger operation. Every tagged class wraps the
 * exact legacy throw in `cause` (so callers can still recover with `instanceof`)
 * and carries the legacy error's stable `tag` (`TOOL_CALL_LEDGER`). Only the
 * Effect service raises these; the synchronous adapter keeps throwing the legacy
 * classes unchanged.
 */
const serviceErrorFields = {
  operation: Schema.String,
  message: Schema.String,
  legacyTag: Schema.String,
  cause: Schema.Defect(),
} as const

/** A generic ledger failure surfaced through the Effect service boundary. */
export class ToolCallLedgerServiceError extends Schema.TaggedErrorClass<ToolCallLedgerServiceError>()(
  "ToolCallLedger.ServiceError",
  serviceErrorFields,
) {}

/** A duplicate start surfaced through the Effect service boundary. */
export class ToolCallAlreadyStartedServiceError extends Schema.TaggedErrorClass<ToolCallAlreadyStartedServiceError>()(
  "ToolCallLedger.AlreadyStarted",
  serviceErrorFields,
) {}

/** A finish without a started row surfaced through the Effect service boundary. */
export class ToolCallNotStartedServiceError extends Schema.TaggedErrorClass<ToolCallNotStartedServiceError>()(
  "ToolCallLedger.NotStarted",
  serviceErrorFields,
) {}

/** A superseded-generation write surfaced through the Effect service boundary. */
export class ToolCallStaleGenerationServiceError extends Schema.TaggedErrorClass<ToolCallStaleGenerationServiceError>()(
  "ToolCallLedger.StaleGeneration",
  serviceErrorFields,
) {}

/** Every typed way a ledger service operation can fail. */
export type ServiceError =
  | ToolCallLedgerServiceError
  | ToolCallAlreadyStartedServiceError
  | ToolCallNotStartedServiceError
  | ToolCallStaleGenerationServiceError

/**
 * A synchronous outcome with no thrown control flow. The transaction callbacks
 * return one of these so the service can raise a typed error in its generator;
 * the legacy handle unwraps it into the exact class direct callers expect.
 */
type LedgerFailure =
  | { readonly kind: "invalid"; readonly message: string }
  | { readonly kind: "alreadyStarted"; readonly callID: string; readonly sessionID: string }
  | { readonly kind: "notStarted"; readonly callID: string; readonly sessionID: string }
  | { readonly kind: "stale"; readonly sessionID: string; readonly expected: string; readonly actual: string | undefined }

type LedgerResult<A> = { readonly ok: true; readonly value: A } | { readonly ok: false; readonly error: LedgerFailure }

const OK = <A>(value: A): LedgerResult<A> => ({ ok: true, value })
const FAIL = (error: LedgerFailure): LedgerResult<never> => ({ ok: false, error })

function failureError(failure: LedgerFailure): ToolCallLedgerError {
  switch (failure.kind) {
    case "invalid":
      return new ToolCallLedgerError(failure.message)
    case "alreadyStarted":
      return new ToolCallAlreadyStartedError(failure.callID, failure.sessionID)
    case "notStarted":
      return new ToolCallNotStartedError(failure.callID, failure.sessionID)
    case "stale":
      return new StaleWorkerGenerationError(failure.sessionID, failure.expected, failure.actual)
  }
}

/** Map a synchronous failure to its tagged service error. */
function failureServiceError(operation: string, failure: LedgerFailure): ServiceError {
  const cause = failureError(failure)
  const fields = { operation, message: cause.message, legacyTag: cause.tag, cause }
  switch (failure.kind) {
    case "alreadyStarted":
      return new ToolCallAlreadyStartedServiceError(fields)
    case "notStarted":
      return new ToolCallNotStartedServiceError(fields)
    case "stale":
      return new ToolCallStaleGenerationServiceError(fields)
    case "invalid":
      return new ToolCallLedgerServiceError(fields)
  }
}

/**
 * Map a legacy throw to its tagged service error. Anything that is not a legacy
 * ledger error is not ours to reinterpret, so it stays a defect.
 */
function serviceError(operation: string, error: unknown): ServiceError | undefined {
  if (!(error instanceof ToolCallLedgerError)) return undefined
  const fields = { operation, message: error.message, legacyTag: error.tag, cause: error }
  if (error instanceof ToolCallAlreadyStartedError) return new ToolCallAlreadyStartedServiceError(fields)
  if (error instanceof ToolCallNotStartedError) return new ToolCallNotStartedServiceError(fields)
  if (error instanceof StaleWorkerGenerationError) return new ToolCallStaleGenerationServiceError(fields)
  return new ToolCallLedgerServiceError(fields)
}

/** Lift a handle operation, turning a legacy throw into a typed service error. */
function mapLegacyErrors<A, E, R>(
  operation: string,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E | ServiceError, R> {
  return effect.pipe(
    Effect.catchDefect((defect) => {
      const failure = serviceError(operation, defect)
      return failure === undefined ? Effect.die(defect) : Effect.fail(failure)
    }),
  )
}

function requireText(value: string, field: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new ToolCallLedgerError(`${field} must be a non-empty string`)
  }
  return value
}

function textFailure(value: string, field: string): LedgerFailure | undefined {
  return typeof value === "string" && value.length > 0
    ? undefined
    : { kind: "invalid", message: `${field} must be a non-empty string` }
}

function validateStart(input: StartInput): LedgerFailure | undefined {
  return textFailure(input.callID, "callID") ?? textFailure(input.sessionID, "sessionID") ?? textFailure(input.tool, "tool")
}

function validateFinish(input: FinishInput): LedgerFailure | undefined {
  return (
    textFailure(input.callID, "callID") ??
    textFailure(input.sessionID, "sessionID") ??
    textFailure(input.outcome, "outcome")
  )
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

/**
 * The lease-generation fence. A released lease has `worker_owner_id = NULL` and
 * `worker_expires_at = 0`; either state means this generation may no longer
 * write. An absent `event_sequence` row is fenced the same way: no lease exists,
 * so no generation may record a call.
 */
function currentGenerationFailure(
  repository: WorkerRepository,
  sessionID: string,
  fence: ToolCallFence | undefined,
): LedgerFailure | undefined {
  if (fence === undefined) return undefined
  const row = repository.select("event_sequence", { aggregate_id: sessionID })[0]
  const rawOwner = row?.worker_owner_id
  const owner = typeof rawOwner === "string" && rawOwner.length > 0 ? rawOwner : undefined
  const expires = typeof row?.worker_expires_at === "number" ? row.worker_expires_at : undefined
  if (row === undefined || owner === undefined || expires === 0 || owner !== fence.ownerID) {
    return { kind: "stale", sessionID, expected: fence.ownerID, actual: owner }
  }
  return undefined
}

function attemptStart(repository: WorkerRepository, input: StartInput, fence?: ToolCallFence): LedgerResult<ToolCallRow> {
  const invalid = validateStart(input)
  if (invalid !== undefined) return FAIL(invalid)
  // The generation check, duplicate check and insert share one transaction so a
  // successor's claim cannot interleave between the fence read and the write.
  return repository.atomic(() => {
    const fenced = currentGenerationFailure(repository, input.sessionID, fence)
    if (fenced !== undefined) return FAIL(fenced)
    const where: RepositoryValues<"tool_call"> = { call_id: input.callID, session_id: input.sessionID }
    if (repository.select("tool_call", where).length > 0) {
      return FAIL({ kind: "alreadyStarted", callID: input.callID, sessionID: input.sessionID })
    }
    repository.insert("tool_call", {
      call_id: input.callID,
      session_id: input.sessionID,
      tool: input.tool,
      side_effecting: input.sideEffecting === false ? 0 : 1,
      state: "started",
      outcome: null,
      time_started: input.timeStarted ?? Date.now(),
      ...(fence === undefined ? {} : { worker_owner_id: fence.ownerID }),
    })
    return OK(rowFromRecord(stored(repository, input.callID, input.sessionID)))
  })
}

function attemptFinish(
  repository: WorkerRepository,
  input: FinishInput,
  fence?: ToolCallFence,
): LedgerResult<ToolCallRow> {
  return repository.atomic(() => {
    const fenced = currentGenerationFailure(repository, input.sessionID, fence)
    if (fenced !== undefined) return FAIL(fenced)
    const where: RepositoryValues<"tool_call"> = {
      call_id: input.callID,
      session_id: input.sessionID,
      state: "started",
      ...(fence === undefined ? {} : { worker_owner_id: fence.ownerID }),
    }
    const changed: WriteOutcome = repository.update(
      "tool_call",
      { state: "finished", outcome: input.outcome, time_finished: input.timeFinished ?? Date.now() },
      where,
    )
    if (changed.changes !== 1) {
      return FAIL({ kind: "notStarted", callID: input.callID, sessionID: input.sessionID })
    }
    return OK(rowFromRecord(stored(repository, input.callID, input.sessionID)))
  })
}

function attemptFinishWithResult<T>(
  repository: WorkerRepository,
  input: FinishInput,
  result: () => T,
  fence?: ToolCallFence,
): LedgerResult<T> {
  return repository.atomic(() => {
    const finished = attemptFinish(repository, input, fence)
    if (!finished.ok) return finished
    return OK(result())
  })
}

function attemptIndeterminate(
  repository: WorkerRepository,
  sessionID: string,
): LedgerResult<readonly ToolCallRow[]> {
  const invalid = textFailure(sessionID, "sessionID")
  if (invalid !== undefined) return FAIL(invalid)
  const rows = repository
    .select("tool_call", { session_id: sessionID, state: "started" })
    .map((record) => ({ ...rowFromRecord(record as Record<string, unknown>), state: "indeterminate" as const }))
  return OK(rows.sort((left, right) => left.timeStarted - right.timeStarted || left.callID.localeCompare(right.callID)))
}

function attemptList(repository: WorkerRepository, sessionID: string): LedgerResult<readonly ToolCallRow[]> {
  const invalid = textFailure(sessionID, "sessionID")
  if (invalid !== undefined) return FAIL(invalid)
  const rows = repository
    .select("tool_call", { session_id: sessionID })
    .map((record) => rowFromRecord(record as Record<string, unknown>))
  return OK(rows.sort((left, right) => left.timeStarted - right.timeStarted || left.callID.localeCompare(right.callID)))
}

function attemptHasSideEffectingStarted(repository: WorkerRepository, sessionID: string): LedgerResult<boolean> {
  const invalid = textFailure(sessionID, "sessionID")
  if (invalid !== undefined) return FAIL(invalid)
  return OK(
    repository.select("tool_call", { session_id: sessionID, state: "started", side_effecting: 1 }).length > 0,
  )
}

function unwrap<A>(result: LedgerResult<A>): A {
  if (result.ok) return result.value
  throw failureError(result.error)
}

function startWith(repository: WorkerRepository, input: StartInput, fence?: ToolCallFence): ToolCallRow {
  return unwrap(attemptStart(repository, input, fence))
}

function finishWith(repository: WorkerRepository, input: FinishInput, fence?: ToolCallFence): ToolCallRow {
  const invalid = validateFinish(input)
  if (invalid !== undefined) throw failureError(invalid)
  return unwrap(attemptFinish(repository, input, fence))
}

/**
 * Mark a call started. Direct callers run outside a result transaction; the
 * production tool path uses the service's `startStandalone` so a parallel tool
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
  return exclusive === undefined ? Effect.sync(write) : Effect.suspend(() => exclusive(write))
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
  return exclusive === undefined ? Effect.sync(write) : Effect.suspend(() => exclusive(write))
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
  const invalid = validateFinish(input)
  if (invalid !== undefined) throw failureError(invalid)
  return unwrap(attemptFinishWithResult(repository, input, result, fence))
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
export function indeterminate(source: ToolCallLedgerSource, sessionID: string): readonly ToolCallRow[] {
  return unwrap(attemptIndeterminate(repositoryFor(source), sessionID))
}

export function list(source: ToolCallLedgerSource, sessionID: string): readonly ToolCallRow[] {
  return unwrap(attemptList(repositoryFor(source), sessionID))
}

export function hasSideEffectingStarted(source: ToolCallLedgerSource, sessionID: string): boolean {
  return unwrap(attemptHasSideEffectingStarted(repositoryFor(source), sessionID))
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

/**
 * The synchronous adapter. New code calls the Effect service; this remains for
 * direct callers that cannot yet compose effects (the ledger's unit tests and
 * the legacy hub seam). Production `src` callers use {@link Service}.
 */
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
    indeterminate: (sessionID) => unwrap(attemptIndeterminate(repository, sessionID)),
    list: (sessionID) => unwrap(attemptList(repository, sessionID)),
    hasSideEffectingStarted: (sessionID) => unwrap(attemptHasSideEffectingStarted(repository, sessionID)),
  }
}

// ---------------------------------------------------------------------------
// Effect service shape.
//
// The `ToolCallLedger` service owns the ledger behavior. Its methods run the
// repository write and the generation fence in one savepoint and raise a typed
// failure with `yield*`; nothing is thrown through the service boundary. The
// synchronous handle above is a shim over the same primitives.
// ---------------------------------------------------------------------------

/** The Effect-facing ledger surface. */
export interface Interface {
  /** False when a reduced graph supplied no usable `WorkerDb`; callers skip the ledger. */
  readonly available: boolean
  /** The synchronous adapter for direct callers (tests and the legacy seam). */
  readonly ledger: ToolCallLedgerHandle
  readonly startStandalone: (input: StartInput) => Effect.Effect<ToolCallRow, ServiceError>
  readonly start: (input: StartInput) => Effect.Effect<ToolCallRow, ServiceError>
  readonly finish: (input: FinishInput) => Effect.Effect<ToolCallRow, ServiceError>
  readonly finishStandalone: (input: FinishInput) => Effect.Effect<ToolCallRow, ServiceError>
  readonly finishWithResult: <T>(input: FinishInput, result: () => T) => Effect.Effect<T, ServiceError>
  readonly indeterminate: (sessionID: string) => Effect.Effect<readonly ToolCallRow[], ServiceError>
  readonly list: (sessionID: string) => Effect.Effect<readonly ToolCallRow[], ServiceError>
  readonly hasSideEffectingStarted: (sessionID: string) => Effect.Effect<boolean, ServiceError>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/ToolCallLedger") {}

function makeService(
  repository: WorkerRepository,
  options: ToolCallLedgerOptions,
  currentFence?: () => ToolCallFence | undefined,
): Interface {
  const fenceFor = (): ToolCallFence | undefined => (currentFence ?? (() => options.fence))()
  const run = <A>(
    operation: string,
    write: () => LedgerResult<A>,
    standalone: boolean,
  ): Effect.Effect<A, ServiceError> =>
    Effect.fn(`ToolCallLedger.${operation}`)(function* () {
      const attempt =
        standalone && options.exclusive !== undefined
          ? Effect.suspend(() => options.exclusive!(write))
          : Effect.sync(write)
      const result = yield* attempt.pipe(
        Effect.catchDefect((defect) => {
          const failure = serviceError(operation, defect)
          return failure === undefined ? Effect.die(defect) : Effect.fail(failure)
        }),
      )
      if (result.ok) return result.value
      return yield* failureServiceError(operation, result.error)
    })()
  return {
    available: true,
    ledger: createToolCallLedger(repository, options),
    startStandalone: (input) => run("startStandalone", () => attemptStart(repository, input, fenceFor()), true),
    start: (input) => run("start", () => attemptStart(repository, input, fenceFor()), false),
    finish: (input) => run("finish", () => attemptFinish(repository, input, fenceFor()), false),
    finishStandalone: (input) => run("finishStandalone", () => attemptFinish(repository, input, fenceFor()), true),
    finishWithResult: (input, result) =>
      run("finishWithResult", () => attemptFinishWithResult(repository, input, result, fenceFor()), false),
    indeterminate: (sessionID) => run("indeterminate", () => attemptIndeterminate(repository, sessionID), false),
    list: (sessionID) => run("list", () => attemptList(repository, sessionID), false),
    hasSideEffectingStarted: (sessionID) =>
      run("hasSideEffectingStarted", () => attemptHasSideEffectingStarted(repository, sessionID), false),
  }
}

/** Adapt a synchronous handle to the Effect surface (compatibility seam). */
export function fromHandle(ledger: ToolCallLedgerHandle): Interface {
  const startStandalone = Effect.fn("ToolCallLedger.startStandalone")(function* (input: StartInput) {
    // `suspend` defers handle construction into the Effect so a synchronous throw
    // from an `exclusive` acquirer is mapped like any other legacy failure.
    return yield* mapLegacyErrors("startStandalone", Effect.suspend(() => ledger.startStandalone(input)))
  })
  const start = Effect.fn("ToolCallLedger.start")(function* (input: StartInput) {
    return yield* mapLegacyErrors(
      "start",
      Effect.sync(() => ledger.start(input)),
    )
  })
  const finish = Effect.fn("ToolCallLedger.finish")(function* (input: FinishInput) {
    return yield* mapLegacyErrors(
      "finish",
      Effect.sync(() => ledger.finish(input)),
    )
  })
  const finishStandalone = Effect.fn("ToolCallLedger.finishStandalone")(function* (input: FinishInput) {
    // `suspend` defers handle construction into the Effect so a synchronous throw
    // from an `exclusive` acquirer is mapped like any other legacy failure.
    return yield* mapLegacyErrors("finishStandalone", Effect.suspend(() => ledger.finishStandalone(input)))
  })
  const finishWithResult = Effect.fn("ToolCallLedger.finishWithResult")(function* <T>(
    input: FinishInput,
    result: () => T,
  ) {
    return yield* mapLegacyErrors(
      "finishWithResult",
      Effect.sync(() => ledger.finishWithResult(input, result)),
    )
  })
  const indeterminate = Effect.fn("ToolCallLedger.indeterminate")(function* (sessionID: string) {
    return yield* mapLegacyErrors(
      "indeterminate",
      Effect.sync(() => ledger.indeterminate(sessionID)),
    )
  })
  const list = Effect.fn("ToolCallLedger.list")(function* (sessionID: string) {
    return yield* mapLegacyErrors(
      "list",
      Effect.sync(() => ledger.list(sessionID)),
    )
  })
  const hasSideEffectingStarted = Effect.fn("ToolCallLedger.hasSideEffectingStarted")(function* (sessionID: string) {
    return yield* mapLegacyErrors(
      "hasSideEffectingStarted",
      Effect.sync(() => ledger.hasSideEffectingStarted(sessionID)),
    )
  })
  return {
    available: true,
    ledger,
    startStandalone,
    start,
    finish,
    finishStandalone,
    finishWithResult,
    indeterminate,
    list,
    hasSideEffectingStarted,
  }
}

/** Build a ledger over the given source; no resource needs a scope. */
export function make(source: ToolCallLedgerSource, options: ToolCallLedgerOptions = {}): Effect.Effect<Interface> {
  return Effect.sync(() => makeService(repositoryFor(source), options))
}

/** The worker-database portion the ledger needs; the live lease owner is read per write. */
export interface WorkerLedgerSource {
  readonly repository: WorkerRepository
  readonly eventBinding: { leaseOwner?: string }
}

/**
 * Build the ledger service from a worker database. The fence follows the
 * worker scope's lease owner on every write, so a lease claimed after the graph
 * is built is still enforced.
 */
export function makeFromWorker(
  workerDb: WorkerLedgerSource,
  options: Omit<ToolCallLedgerOptions, "fence"> = {},
): Effect.Effect<Interface> {
  const fence = (): ToolCallFence | undefined => {
    const ownerID = workerDb.eventBinding.leaseOwner
    return ownerID === undefined ? undefined : { ownerID }
  }
  return Effect.sync(() => makeService(workerDb.repository, options, fence))
}

/** Install a `ToolCallLedger` implementation into the context. */
export function layer(source: ToolCallLedgerSource, options: ToolCallLedgerOptions = {}): Layer.Layer<Service> {
  return Layer.effect(Service, make(source, options))
}

/** The unbound graph node for {@link Service} over an explicit test source. */
export function node(
  source: ToolCallLedgerSource,
  options: ToolCallLedgerOptions = {},
): LayerNode.Node<Service, never, undefined> {
  return LayerNode.make({ service: Service, layer: layer(source, options), deps: [] })
}

function isWorkerDatabase(value: object): value is {
  readonly filename: string
  readonly repository: WorkerRepository
  readonly eventBinding: { leaseOwner?: string }
} {
  const candidate = value as {
    readonly filename?: unknown
    readonly repository?: Partial<WorkerRepository>
    readonly eventBinding?: { leaseOwner?: string }
  }
  return (
    typeof candidate.filename === "string" &&
    typeof candidate.repository?.insert === "function" &&
    typeof candidate.repository?.select === "function"
  )
}

function unavailableService(): Interface {
  const cause = new ToolCallLedgerError("tool-call ledger is unavailable without a worker database")
  const fail = (operation: string): Effect.Effect<never, ServiceError> =>
    Effect.fail(
      new ToolCallLedgerServiceError({
        operation,
        message: cause.message,
        legacyTag: cause.tag,
        cause,
      }),
    )
  const throwIt = (): never => {
    throw cause
  }
  return {
    available: false,
    ledger: {
      startStandalone: () => Effect.die(cause),
      start: throwIt,
      finish: throwIt,
      finishStandalone: () => Effect.die(cause),
      finishWithResult: throwIt,
      indeterminate: throwIt,
      list: throwIt,
      hasSideEffectingStarted: throwIt,
    },
    startStandalone: () => fail("startStandalone"),
    start: () => fail("start"),
    finish: () => fail("finish"),
    finishStandalone: () => fail("finishStandalone"),
    finishWithResult: () => fail("finishWithResult"),
    indeterminate: () => fail("indeterminate"),
    list: () => fail("list"),
    hasSideEffectingStarted: () => fail("hasSideEffectingStarted"),
  }
}

/**
 * The production layer. The repository and the generation fence come from
 * `WorkerDb.Service`; the connection's exclusive transaction comes from the
 * optional `Database.Service`. A reduced graph with no usable worker database
 * yields an unavailable service so callers keep their no-ledger behavior.
 */
export const workerLayer: Layer.Layer<Service> = Layer.effect(
  Service,
  Effect.gen(function* () {
    const workerDb = yield* Effect.serviceOption(WorkerDb)
    if (Option.isNone(workerDb) || !isWorkerDatabase(workerDb.value)) return unavailableService()
    const database = yield* Effect.serviceOption(Database.Service)
    const exclusive: LedgerExclusive | undefined = Option.isSome(database)
      ? <A>(operation: () => A) =>
          database.value.db
            .transaction(() => Effect.sync(operation), { behavior: "immediate" })
            .pipe(Effect.orDie)
      : undefined
    // The lease owner is read per operation: a worker claims its lease after the
    // graph is built, so capturing it here would miss the first write.
    const fenceFor = (): ToolCallFence | undefined => {
      const ownerID = workerDb.value.eventBinding.leaseOwner
      return ownerID === undefined ? undefined : { ownerID }
    }
    return makeService(
      workerDb.value.repository,
      exclusive === undefined ? {} : { exclusive },
      fenceFor,
    )
  }),
)

/**
 * The production graph node. `WorkerDb` is read through `serviceOption` so the
 * node adds no hard dependency and a reduced graph still builds.
 */
export const workerNode: LayerNode.Node<Service, never, undefined> = LayerNode.make({
  service: Service,
  layer: workerLayer,
  deps: [],
})

export * as ToolCallLedger from "./tool-call"
