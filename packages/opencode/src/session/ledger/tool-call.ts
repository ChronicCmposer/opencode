import { readFileSync } from "node:fs"
import { sql, type SQL } from "drizzle-orm"
import { Context, Effect, Layer, Option, Schema } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Database } from "@opencode-ai/core/database/database"
import type { Connection, WriteOutcome } from "@opencode-ai/core/database/connection"
import type { RepositoryValues, WorkerRepository } from "@/session/worker/repository/types"
import { connectedRepository } from "@/session/worker/repository/write"
import { WorkerDb } from "@/session/worker/db"
import { LEDGER_SQL_FALLBACK } from "./sql/fallback.gen"

/** The ledger key for one tool call: `<owning assistant message>:<provider call id>`. */
export const toolCallKey = (messageID: string, callID: string): string => `${messageID}:${callID}`

/** The message prefix of a ledger key, for finding calls owned by one message. */
export const toolCallMessagePrefix = (messageID: string): string => `${messageID}:`

/** The owning message id of a ledger key, or `undefined` when it is not a ledger key. */
export function toolCallMessageID(callID: string): string | undefined {
  const index = callID.indexOf(":")
  return index <= 0 ? undefined : callID.slice(0, index)
}

/** The drizzle raw-run surface the hub discard path already holds. */
export interface LedgerSqlDatabase {
  run(query: SQL): Effect.Effect<unknown, unknown>
  all<T = unknown>(query: SQL): Effect.Effect<T[], unknown>
}

/** The worker owns the tool-call table; a hub that never opened a worker has nothing to abandon. */
function hasToolCallTable(db: LedgerSqlDatabase): Effect.Effect<boolean> {
  return db
    .all(sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'tool_call' LIMIT 1`)
    .pipe(Effect.map((rows) => rows.length > 0), Effect.orDie)
}

/** The driver's row-change count, when it reports one. */
function changedRows(result: unknown): number {
  if (typeof result !== "object" || result === null || !("changes" in result)) return 0
  return typeof result.changes === "number" ? result.changes : 0
}

/**
 * Mark every `started` tool-call record owned by the discarded messages as
 * finished/abandoned in the caller's transaction. Abandoning only stops the
 * record from blocking automatic resume; it never turns the call into a success.
 */
export function abandonForMessages(
  db: LedgerSqlDatabase,
  sessionID: string,
  messageIDs: readonly string[],
): Effect.Effect<number> {
  return Effect.gen(function* () {
    if (!(yield* hasToolCallTable(db))) return 0
    let changes = 0
    for (const messageID of messageIDs) {
      const result = yield* db.run(sql`
        UPDATE tool_call
        SET state = 'finished', outcome = 'abandoned', time_finished = ${Date.now()}
        WHERE session_id = ${sessionID}
          AND state = 'started'
          AND substr(call_id, 1, ${toolCallMessagePrefix(messageID).length}) = ${toolCallMessagePrefix(messageID)}
      `).pipe(Effect.orDie)
      changes += changedRows(result)
    }
    return changes
  })
}

export const TOOL_CALL_STATES = ["started", "finished", "indeterminate"] as const

export type ToolCallState = (typeof TOOL_CALL_STATES)[number]

/** The ledger's raw connection shape. */
export type LedgerConnection = Connection

/** The typed ledger surface. */
export type ToolCallLedgerSource = WorkerRepository | LedgerConnection

/** The hub-only schema. */

export const LEDGER_SQL = (() => {
  try {
    return readFileSync(new URL("./sql/001-tool-call.sql", import.meta.url), "utf8")
  } catch {
    return LEDGER_SQL_FALLBACK
  }
})()

/** The generation-fence column is additive. */
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

/** Serialize a standalone ledger write with the worker's other connection owner. */
export type LedgerExclusive = <A>(operation: () => A) => Effect.Effect<A>

export interface ToolCallLedgerOptions {
  readonly exclusive?: LedgerExclusive
  /** Bind every write to one worker generation. */
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

/** A write from a superseded worker generation. */
export class StaleWorkerGenerationError extends ToolCallLedgerError {
  constructor(sessionID: string, expected: string, actual: string | undefined) {
    super(`worker generation ${expected} is stale for session ${sessionID}: current lease owner is ${actual ?? "none"}`)
    Object.defineProperty(this, "_tag", { value: "ToolCallLedger.StaleGeneration", enumerable: true, configurable: true })
    this.name = "StaleWorkerGenerationError"
  }
}

/** The service-level failure for a ledger operation. */
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

/** A synchronous outcome with no thrown control flow. */
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

/** Map a legacy throw to its tagged service error. */
function serviceError(operation: string, error: unknown): ServiceError | undefined {
  if (!(error instanceof ToolCallLedgerError)) return undefined
  const fields = { operation, message: error.message, legacyTag: error.tag, cause: error }
  if (error instanceof ToolCallAlreadyStartedError) return new ToolCallAlreadyStartedServiceError(fields)
  if (error instanceof ToolCallNotStartedError) return new ToolCallNotStartedServiceError(fields)
  if (error instanceof StaleWorkerGenerationError) return new ToolCallStaleGenerationServiceError(fields)
  return new ToolCallLedgerServiceError(fields)
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
 * Resolve the typed write surface: a worker repository, or an unbound repository over a raw connection.
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

/** The lease-generation fence. */
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

/** Mark a call started. */
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

/** Finish a started call. */
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

/** Run the result write and the ledger finish atomically. */
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

/** The synchronous adapter. */
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

// --------------------------------------------------------------------------- Effect service shape.

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

/** Build a ledger over the given source; no resource needs a scope. */
export function make(source: ToolCallLedgerSource, options: ToolCallLedgerOptions = {}): Effect.Effect<Interface> {
  return Effect.sync(() => makeService(repositoryFor(source), options))
}

/** The worker-database portion the ledger needs; the live lease owner is read per write. */
export interface WorkerLedgerSource {
  readonly repository: WorkerRepository
  readonly eventBinding: { leaseOwner?: string }
}

/** Build the ledger service from a worker database. */
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

/** The production layer. */
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

/** The production graph node. */
export const workerNode: LayerNode.Node<Service, never, undefined> = LayerNode.make({
  service: Service,
  layer: workerLayer,
  deps: [],
})

export * as ToolCallLedger from "./tool-call"
