import { sqliteTable, text, integer, index, uniqueIndex } from "drizzle-orm/sqlite-core"
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
