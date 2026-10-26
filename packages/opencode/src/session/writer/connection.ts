// Per-worker WAL connection helper.
//
// Each Session worker opens its own `bun:sqlite` handle through this module;
// there is no writer isolate and no writer RPC. The implementation lives in
// `@opencode-ai/core/database/connection` so the Bus, db-core, and the worker
// share one ownership/pragma/close contract. This module keeps the retained
// names and the writer-specific pragma tuning.

import {
  openConnection,
  type Connection,
  type QueryParams,
  type WriteOutcome,
} from "@opencode-ai/core/database/connection"
import { isBusyError } from "@opencode-ai/core/database/transaction"
import { resolveWalConfig, walPragmas, type WalConfig } from "@opencode-ai/core/database/pragmas"

/**
 * A synchronous driver must not stall a Session worker on a lock, so the worker
 * connection keeps a much shorter busy timeout than a read connection.
 */
export const WRITER_BUSY_TIMEOUT_MS = 250

export type { QueryParams, WriteOutcome }

export type WriterConnection = Connection

/** `SQLITE_BUSY`/`SQLITE_LOCKED` marker, including injected fault `sqliteCode`. */
export const isSqliteBusy = isBusyError

export interface BunSqliteConnectionOptions {
  readonly filename: string
  /** Applied after open, before any other statement (e.g. `PRAGMA journal_mode`). */
  readonly pragmas?: ReadonlyArray<string>
}

/** Open a synchronous connection; when `pragmas` is absent, open it bare. */
export function createBunSqliteConnection(options: BunSqliteConnectionOptions): WriterConnection {
  return openConnection({ filename: options.filename, pragmas: options.pragmas ?? [] })
}

/** Worker-side pragmas: the shared WAL tuning plus the short busy timeout. */
export function writerPragmas(config: WalConfig): ReadonlyArray<string> {
  return walPragmas({ journalSizeLimitBytes: config.journalSizeLimitBytes, busyTimeoutMs: WRITER_BUSY_TIMEOUT_MS })
}

/**
 * Open a worker connection. The shared WAL pragmas (including
 * `journal_size_limit`) are applied before the first statement, so the WAL is
 * capped from the moment the worker starts writing.
 */
export function openWriterConnection(filename: string, config: WalConfig = resolveWalConfig()): WriterConnection {
  return openConnection({ filename, pragmas: writerPragmas(config) })
}

export * as WriterConnection from "./connection"
