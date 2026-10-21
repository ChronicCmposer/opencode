// Phase 7.2 — writer connection surface (plan task 7.2).
//
// The writer core is deliberately connection-agnostic: it depends only on the
// minimal synchronous surface below, which is exactly what the Effect SQL
// `Connection` reduces to (`execute`/`executeValues`/`executeRaw`). Production
// wires the `bun:sqlite` adapter; tests wire the same adapter over an in-memory
// database so transaction and sequence semantics are exercised for real.

import { Database } from "bun:sqlite"

export interface WriteOutcome {
  readonly changes: number
  readonly lastInsertRowid: number
}

/** Positional (`?`) or named (`$name`) bindings; the Effect SQL path uses positional. */
export type QueryParams = ReadonlyArray<unknown> | Record<string, unknown>

function spread(params: QueryParams): unknown[] {
  return Array.isArray(params) ? [...params] : [params]
}

export interface WriterConnection {
  /** `SELECT`-shaped: rows as records. */
  all(sql: string, params?: QueryParams): ReadonlyArray<Record<string, unknown>>
  /** `SELECT`-shaped: rows as positional arrays. */
  values(sql: string, params?: QueryParams): ReadonlyArray<ReadonlyArray<unknown>>
  /** Mutating statement; returns the change count. */
  run(sql: string, params?: QueryParams): WriteOutcome
  /** Raw multi-statement execution with no parameters. */
  exec(sql: string): void
  close(): void
}

/** `SQLITE_BUSY` marker carried by forced faults and by the native driver. */
const BUSY_CODES: ReadonlySet<string> = new Set(["SQLITE_BUSY", "SQLITE_LOCKED"])

/**
 * True when `error` represents a lock/busy failure that a whole-transaction retry
 * can recover from. Recognizes the native driver message and the injected
 * `CommitFault` from `@/testing/faults` (which carries `sqliteCode`).
 */
export function isSqliteBusy(error: unknown): boolean {
  if (typeof error !== "object" || error === null) return false
  const code = (error as { sqliteCode?: unknown }).sqliteCode
  if (typeof code === "string" && BUSY_CODES.has(code)) return true
  const message = (error as { message?: unknown }).message
  return typeof message === "string" && (message.includes("SQLITE_BUSY") || message.includes("database is locked"))
}

export interface BunSqliteConnectionOptions {
  readonly filename: string
  /** Applied after open, before any other statement (e.g. `PRAGMA journal_mode`). */
  readonly pragmas?: ReadonlyArray<string>
}

/**
 * Open a synchronous `bun:sqlite` connection. This is the production adapter; it is
 * only ever constructed inside the writer isolate (no other service opens one).
 */
export function createBunSqliteConnection(options: BunSqliteConnectionOptions): WriterConnection {
  const native = new Database(options.filename, { create: true })
  for (const pragma of options.pragmas ?? []) native.exec(pragma)

  return {
    all: (sql, params = []) =>
      native.query(sql).all(...(spread(params) as never[])) as ReadonlyArray<Record<string, unknown>>,
    values: (sql, params = []) =>
      native.query(sql).values(...(spread(params) as never[])) as ReadonlyArray<ReadonlyArray<unknown>>,
    run: (sql, params = []) => {
      const result = native.query(sql).run(...(spread(params) as never[]))
      return { changes: Number(result.changes), lastInsertRowid: Number(result.lastInsertRowid) }
    },
    exec: (sql) => native.exec(sql),
    close: () => native.close(),
  }
}

export * as WriterConnection from "./connection"
