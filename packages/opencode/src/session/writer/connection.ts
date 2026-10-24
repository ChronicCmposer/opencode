// Phase 7.2 — writer connection surface (plan task 7.2).
//
// The writer core is deliberately connection-agnostic: it depends only on the
// minimal synchronous surface below, which is exactly what the Effect SQL
// `Connection` reduces to (`execute`/`executeValues`/`executeRaw`). Production
// wires the `bun:sqlite` adapter; tests wire the same adapter over an in-memory
// database so transaction and sequence semantics are exercised for real.

import { resolveWalConfig, walPragmas, type WalConfig } from "@opencode-ai/core/database/pragmas"
import { Database, type SQLQueryBindings } from "bun:sqlite"

/**
 * A synchronous driver must never stall the writer isolate on a lock, so the
 * writer keeps a much shorter busy timeout than the read connection.
 */
export const WRITER_BUSY_TIMEOUT_MS = 250

export interface WriteOutcome {
  readonly changes: number
  readonly lastInsertRowid: number
}

/** Positional (`?`) or named (`$name`) bindings; the Effect SQL path uses positional. */
export type QueryParams = ReadonlyArray<unknown> | Record<string, unknown>

/**
 * Flatten a binding list into the native driver's variadic form. `QueryParams` is
 * intentionally broad (the Effect SQL path passes positional `unknown[]`), so this
 * is the single point where the values are handed to `bun:sqlite`.
 */
function spread(params: QueryParams): unknown[] {
  return Array.isArray(params) ? [...params] : [params]
}

/** Native variadic bindings; the driver validates each value at execution time. */
function nativeBindings(params: QueryParams): SQLQueryBindings[] {
  return spread(params) as SQLQueryBindings[]
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
  try {
    for (const pragma of options.pragmas ?? []) native.exec(pragma)
  } catch (error) {
    // A rejected pragma leaves an open handle (and its file descriptor) behind:
    // close it before surfacing the failure so a bad config never leaks the
    // connection it just opened.
    native.close()
    throw error
  }

  return {
    all: (sql, params = []) => native.query(sql).all(...nativeBindings(params)) as ReadonlyArray<Record<string, unknown>>,
    values: (sql, params = []) =>
      native.query(sql).values(...nativeBindings(params)) as ReadonlyArray<ReadonlyArray<unknown>>,
    run: (sql, params = []) => {
      const result = native.query(sql).run(...nativeBindings(params))
      return { changes: Number(result.changes), lastInsertRowid: Number(result.lastInsertRowid) }
    },
    exec: (sql) => native.exec(sql),
    close: () => native.close(),
  }
}

/** Writer-side pragmas: the shared WAL tuning plus the isolate's short timeout. */
export function writerPragmas(config: WalConfig): ReadonlyArray<string> {
  return walPragmas({ journalSizeLimitBytes: config.journalSizeLimitBytes, busyTimeoutMs: WRITER_BUSY_TIMEOUT_MS })
}

/**
 * Open the production writer connection. The shared WAL pragmas (including
 * `journal_size_limit`) are applied before the first statement, so the WAL is
 * capped from the moment the isolate starts writing.
 */
export function openWriterConnection(filename: string, config: WalConfig = resolveWalConfig()): WriterConnection {
  return createBunSqliteConnection({ filename, pragmas: writerPragmas(config) })
}

export * as WriterConnection from "./connection"
