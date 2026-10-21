// Shared writer test fixtures (plan finding #184).
//
// The writer suites each rebuilt the same in-memory schema, fault-free
// environment overlay and event shape. Keeping them here gives the schema and
// the event contract a single definition.

import { FAULTS_ENV } from "@/testing/faults"

/** Schema statements for the writer test database (ordering matters). */
export const SCHEMA: ReadonlyArray<string> = [
  "CREATE TABLE event_sequence (aggregate_id TEXT PRIMARY KEY, seq INTEGER NOT NULL, owner_id TEXT)",
  "CREATE TABLE event (id TEXT PRIMARY KEY, aggregate_id TEXT NOT NULL, seq INTEGER NOT NULL, type TEXT NOT NULL, data TEXT NOT NULL)",
  "CREATE TABLE projector_log (aggregate_id TEXT, seq INTEGER NOT NULL, type TEXT)",
]

/** {@link SCHEMA} as one SQL string, for `connection.exec`-style setup. */
export const SCHEMA_SQL = SCHEMA.map((statement) => `${statement};`).join("\n")

/** A client environment whose writer isolate never sees an armed fault. */
export const healthyEnv: Record<string, string | undefined> = { [FAULTS_ENV]: undefined }

let eventCounter = 0

/** Build a writer event with a unique id. */
export const ev = (type: string, aggregateID: string, data: Record<string, unknown> = {}) => ({
  id: `evt-${++eventCounter}`,
  type,
  aggregateID,
  data,
})
