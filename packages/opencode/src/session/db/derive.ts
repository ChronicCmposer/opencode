// Schema-derived table ownership (D6).
//
// A table is worker-writable exactly when it has a real foreign-key path to the
// `session` aggregate: it carries a `session_id` foreign key, or it references a
// table that does (`part.message_id` is the one transitive case). A bare
// `session_id` column with no foreign key is NOT ownership evidence, so adding
// the column to an unrelated table cannot silently widen the allowlist.
//
// This is derived from the live schema, never maintained by hand, so adding or
// removing the key without regenerating the allowlist is a drift failure.

export interface SqliteQuery {
  all<T = Record<string, unknown>>(sql: string, params?: ReadonlyArray<unknown>): ReadonlyArray<T>
}

interface MasterRow {
  readonly name: string
}

interface ForeignKeyRow {
  readonly table: string
  readonly from: string
}

function quoteIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`
}

/** Every user table, sorted; `sqlite_*` internals are excluded. */
export function listSchemaTables(connection: SqliteQuery): readonly string[] {
  return connection
    .all<MasterRow>("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
    .map((row) => row.name)
}

export function tableColumns(connection: SqliteQuery, table: string): readonly string[] {
  return connection
    .all<{ readonly name: string }>(`PRAGMA table_info(${quoteIdentifier(table)})`)
    .map((row) => row.name)
}

/** The declared foreign keys of one table (target table + source column). */
export function tableForeignKeys(connection: SqliteQuery, table: string): readonly ForeignKeyRow[] {
  return connection
    .all<ForeignKeyRow>(`PRAGMA foreign_key_list(${quoteIdentifier(table)})`)
    .map((row) => ({ table: row.table, from: row.from }))
}

/**
 * Tables that own session-scoped rows and are therefore worker-writable. The
 * `session` root is itself excluded; every other table reachable through a
 * foreign key is included, in sorted order.
 */
export function deriveSessionScopedTables(connection: SqliteQuery): readonly string[] {
  const tables = listSchemaTables(connection)
  const scoped = new Set<string>(["session"])
  let changed = true
  while (changed) {
    changed = false
    for (const table of tables) {
      if (scoped.has(table)) continue
      if (tableForeignKeys(connection, table).some((key) => scoped.has(key.table))) {
        scoped.add(table)
        changed = true
      }
    }
  }
  scoped.delete("session")
  return tables.filter((table) => scoped.has(table))
}

export * as TableDerive from "./derive"
