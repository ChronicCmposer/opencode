import { randomUUID } from "node:crypto"
import { Schema } from "effect"
import { LEDGER_SQL, type LedgerConnection } from "./tool-call"

export const WORKER_CRASH_PHASES = ["boot", "materialize", "stream", "tool", "commit"] as const

export type WorkerCrashPhase = (typeof WORKER_CRASH_PHASES)[number]

/** The D8 crash payload. The same value is persisted and returned; never retried. */
export interface WorkerCrashed {
  readonly sessionID: string
  readonly workerID: number
  readonly phase: WorkerCrashPhase
  readonly lastCommittedMessageID: string | null
  readonly inFlightCallIDs: readonly string[]
  readonly retryable: false
  readonly reason: string
}

export interface CrashRow extends WorkerCrashed {
  readonly crashID: string
  readonly timeCreated: number
}

export interface RecordCrashInput extends Omit<WorkerCrashed, "retryable"> {
  readonly crashID?: string
  readonly timeCreated?: number
}

export class WorkerCrashRecordError extends Schema.TaggedErrorClass<WorkerCrashRecordError>(
  "WorkerCrashRecordError",
)("WorkerCrash.Record", { message: Schema.String }) {
  readonly tag = "WORKER_CRASH_RECORD"
  constructor(message: string) {
    super({ message })
    this.name = "WorkerCrashRecordError"
  }
}

export function isWorkerCrashPhase(value: unknown): value is WorkerCrashPhase {
  return typeof value === "string" && (WORKER_CRASH_PHASES as readonly string[]).includes(value)
}

function requireText(value: string, field: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new WorkerCrashRecordError(`WorkerCrashed.${field} must be a non-empty string`)
  }
  return value
}

/** Build a complete D8 payload; every field is validated at this single boundary. */
export function workerCrashed(input: Omit<WorkerCrashed, "retryable"> & { readonly retryable?: false }): WorkerCrashed {
  if (!isWorkerCrashPhase(input.phase)) {
    throw new WorkerCrashRecordError(
      `WorkerCrashed.phase must be one of ${WORKER_CRASH_PHASES.join(", ")}, received ${String(input.phase)}`,
    )
  }
  if (!Number.isInteger(input.workerID) || input.workerID < 0) {
    throw new WorkerCrashRecordError(
      `WorkerCrashed.workerID must be a non-negative integer, received ${String(input.workerID)}`,
    )
  }
  if (input.lastCommittedMessageID !== null && typeof input.lastCommittedMessageID !== "string") {
    throw new WorkerCrashRecordError("WorkerCrashed.lastCommittedMessageID must be a string or null")
  }
  if (!input.inFlightCallIDs.every((id) => typeof id === "string" && id.length > 0)) {
    throw new WorkerCrashRecordError("WorkerCrashed.inFlightCallIDs must contain only non-empty strings")
  }
  return {
    sessionID: requireText(input.sessionID, "sessionID"),
    workerID: input.workerID,
    phase: input.phase,
    lastCommittedMessageID: input.lastCommittedMessageID,
    inFlightCallIDs: [...input.inFlightCallIDs],
    retryable: false,
    reason: requireText(input.reason, "reason"),
  }
}

function parseCallIDs(serialized: unknown): readonly string[] {
  if (typeof serialized !== "string") throw new WorkerCrashRecordError("worker_crash.in_flight_call_ids is not text")
  const parsed: unknown = JSON.parse(serialized)
  if (!Array.isArray(parsed) || !parsed.every((value) => typeof value === "string")) {
    throw new WorkerCrashRecordError("worker_crash.in_flight_call_ids is not a string array")
  }
  return parsed
}

function rowFromRecord(record: Record<string, unknown>): CrashRow {
  return {
    crashID: String(record.crash_id),
    sessionID: String(record.session_id),
    workerID: Number(record.worker_id),
    phase: record.phase as WorkerCrashPhase,
    lastCommittedMessageID:
      record.last_committed_message_id === null || record.last_committed_message_id === undefined
        ? null
        : String(record.last_committed_message_id),
    inFlightCallIDs: parseCallIDs(record.in_flight_call_ids),
    retryable: false,
    reason: String(record.reason),
    timeCreated: Number(record.time_created),
  }
}

/** Persist the payload and return the same value. */
export function record(connection: LedgerConnection, input: RecordCrashInput): WorkerCrashed {
  const payload = workerCrashed(input)
  connection.run(
    "INSERT INTO worker_crash (crash_id, session_id, worker_id, phase, last_committed_message_id, in_flight_call_ids, retryable, reason, time_created) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)",
    [
      input.crashID ?? randomUUID(),
      payload.sessionID,
      payload.workerID,
      payload.phase,
      payload.lastCommittedMessageID,
      JSON.stringify(payload.inFlightCallIDs),
      payload.reason,
      input.timeCreated ?? Date.now(),
    ],
  )
  return payload
}

export function crashes(connection: LedgerConnection, sessionID: string): readonly CrashRow[] {
  if (typeof sessionID !== "string" || sessionID.length === 0) {
    throw new WorkerCrashRecordError("sessionID must be a non-empty string")
  }
  return connection
    .all("SELECT * FROM worker_crash WHERE session_id = ? ORDER BY time_created, crash_id", [sessionID])
    .map(rowFromRecord)
}

/** The ledger schema is hub-owned; a test or migration applies it explicitly. */
export const SCHEMA_SQL = LEDGER_SQL


/** The async executor surface the hub uses for off-loop crash persistence. */
export interface AsyncLedgerExecutor {
  run(sql: string, params?: ReadonlyArray<unknown>): Promise<unknown>
}

/** Persist the payload through an async executor (the hub's DB thread) and return the same value. */
export async function recordAsync(
  executor: AsyncLedgerExecutor,
  input: RecordCrashInput,
): Promise<WorkerCrashed> {
  const payload = workerCrashed(input)
  await executor.run(
    "INSERT INTO worker_crash (crash_id, session_id, worker_id, phase, last_committed_message_id, in_flight_call_ids, retryable, reason, time_created) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)",
    [
      input.crashID ?? randomUUID(),
      payload.sessionID,
      payload.workerID,
      payload.phase,
      payload.lastCommittedMessageID,
      JSON.stringify(payload.inFlightCallIDs),
      payload.reason,
      input.timeCreated ?? Date.now(),
    ],
  )
  return payload
}

export * as LedgerCrash from "./crash"
