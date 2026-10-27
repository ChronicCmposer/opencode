// Typed durable-log primitives (interface for `typed-durable`).
//
// The Bus/EventV2 path owns these operations today. This interface is the typed
// seam that replaces its raw SQL: `event`, `event_sequence`, and `event_blob`
// are scoped by `aggregate_id` and may only be written for the bound session.
// The repository core deliberately does not implement `core/src/event.ts` or
// `core/src/bus.ts`.

import type { WriteOutcome } from "@opencode-ai/core/database/connection"
import type { RepositoryValues } from "./types"

export interface DurableSequenceRow {
  readonly aggregate_id: string
  readonly seq: number
  readonly owner_id?: string | null
  readonly worker_owner_id?: string | null
  readonly worker_heartbeat_at?: number | null
  readonly worker_expires_at?: number | null
}

export interface DurableEventRow {
  readonly id: string
  readonly aggregate_id: string
  readonly seq: number
  readonly type: string
  readonly data: string
}

export interface DurableBlobRow {
  readonly aggregate_id: string
  readonly seq: number
  readonly type: string
  readonly hash: string
  readonly size: number
  readonly data?: string | null
  readonly file_path?: string | null
}

/**
 * Typed durable primitives. Implementations must bind `aggregate_id` to the
 * worker's session and refuse any other aggregate.
 */
export interface DurablePrimitives {
  readSequence(aggregateID: string): DurableSequenceRow | undefined
  insertSequence(row: DurableSequenceRow): WriteOutcome
  updateSequence(aggregateID: string, values: RepositoryValues<"event_sequence">): WriteOutcome
  insertEvent(row: DurableEventRow): WriteOutcome
  insertBlob(row: DurableBlobRow): WriteOutcome
  deleteEvents(aggregateID: string, beforeSeq?: number): WriteOutcome
}

export * as DurablePrimitivesContract from "./durable"
