// Table ownership split (D6).
//
// `WORKER_WRITABLE_TABLES` and `SCHEMA_TABLES` are generated from the shipped
// schema (`script/migration-statements.ts`). Runtime classification is derived
// from SQLite itself so the generated allowlist can be checked for drift.

import { SCHEMA_TABLES, WORKER_WRITABLE_TABLES } from "./allowlist.gen"
import { deriveSessionScopedTables, listSchemaTables, type SqliteQuery } from "./derive"

export { SCHEMA_TABLES, WORKER_WRITABLE_TABLES }

/**
 * The durable log is written by the Bus, not by session-scoped DML, so it is not
 * part of the generated `session_id` allowlist. The Bus owns its access rule.
 */
export const DURABLE_LOG_TABLES: readonly string[] = Object.freeze(["event", "event_sequence", "event_blob"])

const workerTables = new Set(WORKER_WRITABLE_TABLES)
const durableTables = new Set(DURABLE_LOG_TABLES)

/** Every other table is hub-only, including leases, auth, and schema state. */
export const HUB_ONLY_TABLES: readonly string[] = Object.freeze(
  SCHEMA_TABLES.filter((table) => !workerTables.has(table) && !durableTables.has(table)),
)

export interface TableOwnership {
  readonly tables: readonly string[]
  readonly workerWritable: readonly string[]
  readonly hubOnly: readonly string[]
}

export interface Drift {
  readonly missing: readonly string[]
  readonly unexpected: readonly string[]
}

export class TableAllowlistDriftError extends Error {
  readonly tag = "WorkerDb.TableAllowlistDrift"
  readonly sessionScoped: Drift
  readonly tables: Drift

  constructor(input: { readonly sessionScoped: Drift; readonly tables: Drift }) {
    super(
      `session_id tables changed without regenerating the allowlist ` +
        `(missing: [${input.sessionScoped.missing.join(", ")}], ` +
        `unexpected: [${input.sessionScoped.unexpected.join(", ")}]; ` +
        `schema tables missing: [${input.tables.missing.join(", ")}], ` +
        `unexpected: [${input.tables.unexpected.join(", ")}])`,
    )
    this.name = "TableAllowlistDriftError"
    this.sessionScoped = input.sessionScoped
    this.tables = input.tables
  }
}

function drift(expected: readonly string[], observed: readonly string[]): Drift {
  const expectedSet = new Set(expected)
  const observedSet = new Set(observed)
  return {
    missing: expected.filter((value) => !observedSet.has(value)),
    unexpected: observed.filter((value) => !expectedSet.has(value)),
  }
}

/** Derive both sides of the split from a live (migrated) database. */
export function classifyTables(connection: SqliteQuery): TableOwnership {
  const tables = listSchemaTables(connection)
  const workerWritable = deriveSessionScopedTables(connection)
  const writable = new Set(workerWritable)
  const durable = new Set(DURABLE_LOG_TABLES)
  return {
    tables,
    workerWritable,
    hubOnly: tables.filter((table) => !writable.has(table) && !durable.has(table)),
  }
}

/** The gate: a live schema whose ownership differs from the generated allowlist fails. */
export function assertTableAllowlist(connection: SqliteQuery): void {
  const observed = classifyTables(connection)
  const sessionScoped = drift(WORKER_WRITABLE_TABLES, observed.workerWritable)
  const tables = drift(SCHEMA_TABLES, observed.tables)
  if (sessionScoped.missing.length > 0 || sessionScoped.unexpected.length > 0) {
    throw new TableAllowlistDriftError({ sessionScoped, tables })
  }
  if (tables.missing.length > 0 || tables.unexpected.length > 0) {
    throw new TableAllowlistDriftError({ sessionScoped, tables })
  }
}

/**
 * Boot/migration parity gate for the generated worker table set. Unlike the full
 * allowlist assertion this tolerates unrelated installer tables, but any drift in
 * the session-scoped (worker-writable) set is fatal.
 */
export function assertWorkerTableParity(connection: SqliteQuery): void {
  const observed = deriveSessionScopedTables(connection)
  const sessionScoped = drift(WORKER_WRITABLE_TABLES, observed)
  if (sessionScoped.missing.length > 0 || sessionScoped.unexpected.length > 0) {
    throw new TableAllowlistDriftError({ sessionScoped, tables: { missing: [], unexpected: [] } })
  }
}

/** Session-scoped DML target. Durable-log writes are governed by the Bus contract. */
export function isWorkerWritable(table: string): boolean {
  return workerTables.has(table)
}

export type { SqliteQuery }

export * as Ownership from "./ownership"
