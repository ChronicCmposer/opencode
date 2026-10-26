// Schema-derived table ownership (D6).
//
// A table is worker-writable exactly when it carries a `session_id` column. This
// is derived from the live schema, never maintained by hand, so adding or
// removing `session_id` without regenerating the allowlist is a drift failure.

export interface SqliteQuery {
  all<T = Record<string, unknown>>(sql: string, params?: ReadonlyArray<unknown>): ReadonlyArray<T>
}

interface MasterRow {
  readonly name: string
}

interface ColumnRow {
  readonly name: string
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
    .all<ColumnRow>(`PRAGMA table_info(${quoteIdentifier(table)})`)
    .map((row) => row.name)
}

/** Tables that own session-scoped rows and are therefore worker-writable. */
export function deriveSessionScopedTables(connection: SqliteQuery): readonly string[] {
  return listSchemaTables(connection)
    .filter((table) => tableColumns(connection, table).includes("session_id"))
    .sort()
}
