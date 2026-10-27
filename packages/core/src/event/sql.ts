import { sqliteTable, text, integer, index, uniqueIndex, primaryKey } from "drizzle-orm/sqlite-core"
import type { EventV2 } from "../event"

export const EventSequenceTable = sqliteTable("event_sequence", {
  aggregate_id: text().notNull().primaryKey(),
  seq: integer().notNull(),
  /** v1 sync owner (workspace id). Written only by `EventV2.claim`/sync replay. */
  owner_id: text(),
  /** Worker-generation lease owner; independent of the sync owner. */
  worker_owner_id: text(),
  worker_heartbeat_at: integer(),
  worker_expires_at: integer(),
})

export const EventTable = sqliteTable(
  "event",
  {
    id: text().$type<EventV2.ID>().primaryKey(),
    aggregate_id: text()
      .notNull()
      .references(() => EventSequenceTable.aggregate_id, { onDelete: "cascade" }),
    seq: integer().notNull(),
    type: text().notNull(),
    data: text({ mode: "json" }).$type<Record<string, unknown>>().notNull(),
  },
  (table) => [
    uniqueIndex("event_aggregate_seq_idx").on(table.aggregate_id, table.seq),
    index("event_aggregate_type_seq_idx").on(table.aggregate_id, table.type, table.seq),
  ],
)

/**
 * D16: a durable payload stored by reference. A row exists only when a payload
 * exceeded the inline bound, and it is keyed by the durable position. `file_path`
 * names the content-addressed file; `data` remains for rows written by an older
 * inline-blob schema. Multiple rows may share one hash, so a file is unlinked
 * only when the last referencing row is collected.
 */
export const EventBlobTable = sqliteTable(
  "event_blob",
  {
    aggregate_id: text()
      .notNull()
      .references(() => EventSequenceTable.aggregate_id, { onDelete: "cascade" }),
    seq: integer().notNull(),
    type: text().notNull(),
    hash: text().notNull(),
    size: integer().notNull(),
    data: text({ mode: "json" }).$type<Record<string, unknown>>(),
    file_path: text(),
  },
  (table) => [
    primaryKey({ columns: [table.aggregate_id, table.seq] }),
    // Refcount GC probes `WHERE hash = ?` across every aggregate; without this
    // index each probe scans the whole blob table.
    index("event_blob_hash_idx").on(table.hash),
  ],
)
