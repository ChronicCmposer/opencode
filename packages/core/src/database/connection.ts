// Per-connection SQLite helper for Session workers (D5).
//
// There is no writer isolate and no singleton. Every worker opens its own
// handle through this module; the shared WAL tuning is applied before the first
// statement and a rejected pragma closes the handle again. The default driver is
// `bun:sqlite`; `openNodeConnection` adds authorizer and pre-authorizer setup
// support from `node:sqlite` for the per-session scoped handle (live bound
// views plus a repository-only write path). Foreign keys stay ON there so a
// cross-session parent reference cannot be inserted. The exported surface
// deliberately matches the Bus `Connection` contract (`all`/`run`/`exec`) and
// the retained `session/writer/connection.ts` helper.

import { randomUUID } from "node:crypto"
import { Database, type SQLQueryBindings } from "bun:sqlite"
import { DatabaseSync, type SQLInputValue, type SQLOutputValue } from "node:sqlite"
import { resolveWorkerCacheSizeKib, walPragmas } from "./pragmas"

/** SQLite stays locked for this long before the sync driver reports busy. */
export const WORKER_BUSY_TIMEOUT_MS = 250

export interface WriteOutcome {
  readonly changes: number
  readonly lastInsertRowid: number
}

/** Positional (`?`) or named (`$name`) bindings. */
export type QueryParams = ReadonlyArray<unknown> | Record<string, unknown>

export interface Connection {
  readonly owner: string
  readonly filename: string
  readonly closed: boolean
  all<T = Record<string, unknown>>(sql: string, params?: QueryParams): ReadonlyArray<T>
  values<T = ReadonlyArray<unknown>>(sql: string, params?: QueryParams): ReadonlyArray<T>
  run(sql: string, params?: QueryParams): WriteOutcome
  exec(sql: string): void
  pragma(name: string): unknown
  journalMode(): string
  close(): void
}

export class ConnectionClosedError extends Error {
  readonly tag = "DB_CORE_CONNECTION_CLOSED"
  constructor(filename: string) {
    super(`database connection is closed: ${filename}`)
    this.name = "ConnectionClosedError"
  }
}

export interface ConnectionOptions {
  readonly filename: string
  /**
   * Applied verbatim after open, before any other statement. When present this
   * replaces the default WAL pragmas (an empty list opens a bare connection).
   */
  readonly pragmas?: ReadonlyArray<string>
  /** Default WAL tuning when `pragmas` is absent. */
  readonly journalSizeLimitBytes?: number
  readonly busyTimeoutMs?: number
  /** Page-cache budget in KiB; defaults to the worker budget, not the hub's 64 MiB. */
  readonly cacheSizeKib?: number
  readonly create?: boolean
}

/** One SQLite authorizer decision. Throw from the callback to deny the action. */
export interface AuthorizerRequest {
  /** One of the `SQLITE_*` authorizer action codes. */
  readonly action: number
  /** The table, view, index, trigger, or pragma the action names. */
  readonly table: string | null
  /** The column for `SQLITE_READ`, the argument for `SQLITE_PRAGMA`, else null. */
  readonly column: string | null
  /** `main`, `temp`, or an attached schema name. */
  readonly database: string | null
  /** The trigger (or view) currently expanding, null for a top-level statement. */
  readonly trigger: string | null
}

export type Authorizer = (request: AuthorizerRequest) => number | void

export interface NodeConnectionOptions extends ConnectionOptions {
  /** Enable SQLite foreign-key enforcement for this handle. Defaults to true. */
  readonly foreignKeys?: boolean
  /** Runs after open, before the authorizer is installed (schema gate + TEMP views). */
  readonly initialize?: (connection: NodeConnection) => void
  /** Installed after {@link initialize}; throw to deny an action. */
  readonly authorizer?: Authorizer
}

/**
 * `node:sqlite` exposes `setAuthorizer` where `bun:sqlite` does not. The worker
 * boundary needs it, so the scoped handle is a node handle while the rest of the
 * process keeps the bun driver.
 */
export interface NodeConnection extends Connection {
  /** Register a deterministic SQL function used by the bound views/triggers. */
  function(name: string, fn: (...args: unknown[]) => unknown): void
  /** Replace (or clear) the authorizer. */
  setAuthorizer(authorizer: Authorizer | undefined): void
}

/** A unique ownership token for one opened handle; never shared across opens. */
let nextOwner = 0

function nextOwnerID(): string {
  nextOwner += 1
  // The counter is module-local, so worker threads (separate module realms)
  // would otherwise collide on pid + counter. The UUID keeps ownership unique.
  return `${process.pid}:${nextOwner}:${randomUUID()}`
}

function spread(params: QueryParams): unknown[] {
  return Array.isArray(params) ? [...params] : [params]
}

function nativeBindings(params: QueryParams): SQLQueryBindings[] {
  return spread(params) as SQLQueryBindings[]
}

const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/

/**
 * Open an independent connection. `journal_mode=WAL`, `synchronous=NORMAL`,
 * `foreign_keys=ON`, and a short busy timeout are the default worker tuning;
 * every caller owns (`owner`) and closes its own handle.
 */
export function openConnection(options: ConnectionOptions): Connection {
  if (options.filename.length === 0) throw new Error("openConnection requires a filename")

  const pragmas =
    options.pragmas ??
    walPragmas({
      ...(options.journalSizeLimitBytes === undefined ? {} : { journalSizeLimitBytes: options.journalSizeLimitBytes }),
      busyTimeoutMs: options.busyTimeoutMs ?? WORKER_BUSY_TIMEOUT_MS,
      // A per-session worker gets its own bounded page cache (default 8 MiB), so
      // eight workers cannot reserve eight copies of the hub's 64 MiB budget.
      cacheSizeKib: options.cacheSizeKib ?? resolveWorkerCacheSizeKib(),
    })

  const native = new Database(options.filename, { create: options.create ?? true })
  try {
    for (const pragma of pragmas) native.exec(pragma)
  } catch (error) {
    native.close()
    throw error
  }

  const owner = nextOwnerID()
  let closed = false

  const assertOpen = () => {
    if (closed) throw new ConnectionClosedError(options.filename)
  }

  const pragma = (name: string): unknown => {
    assertOpen()
    if (!IDENTIFIER.test(name)) throw new Error(`invalid pragma name: ${name}`)
    const row = native.query(`PRAGMA ${name}`).get() as Record<string, unknown> | null
    return row === null ? undefined : Object.values(row)[0]
  }

  return {
    owner,
    filename: options.filename,
    get closed() {
      return closed
    },
    all<T = Record<string, unknown>>(sql: string, params: QueryParams = []) {
      assertOpen()
      return native.query(sql).all(...nativeBindings(params)) as ReadonlyArray<T>
    },
    values<T = ReadonlyArray<unknown>>(sql: string, params: QueryParams = []) {
      assertOpen()
      return native.query(sql).values(...nativeBindings(params)) as unknown as ReadonlyArray<T>
    },
    run(sql: string, params: QueryParams = []) {
      assertOpen()
      const result = native.query(sql).run(...nativeBindings(params))
      return { changes: Number(result.changes), lastInsertRowid: Number(result.lastInsertRowid) }
    },
    exec(sql: string) {
      assertOpen()
      native.exec(sql)
    },
    pragma,
    journalMode() {
      return String(pragma("journal_mode")).toLowerCase()
    },
    close() {
      if (closed) return
      closed = true
      native.close()
    },
  }
}

/**
 * Open a synchronous SQLite handle backed by `node:sqlite`. Same pragma and
 * surface contract as {@link openConnection}; additionally supports an
 * authorizer and a pre-authorizer initializer, which the per-session boundary
 * uses to gate the schema and install the live filtered TEMP views.
 */
export function openNodeConnection(options: NodeConnectionOptions): NodeConnection {
  if (options.filename.length === 0) throw new Error("openNodeConnection requires a filename")

  const pragmas =
    options.pragmas ??
    walPragmas({
      ...(options.journalSizeLimitBytes === undefined ? {} : { journalSizeLimitBytes: options.journalSizeLimitBytes }),
      busyTimeoutMs: options.busyTimeoutMs ?? WORKER_BUSY_TIMEOUT_MS,
      cacheSizeKib: options.cacheSizeKib ?? resolveWorkerCacheSizeKib(),
    })

  const native = new DatabaseSync(options.filename, {
    open: true,
    enableForeignKeyConstraints: options.foreignKeys ?? true,
  })
  try {
    for (const pragma of pragmas) native.exec(pragma)
    // The shared WAL tuning enables foreign keys. Callers may explicitly opt out
    // for a private test handle, but the worker's scoped handle keeps them ON so
    // a child row cannot reference another session's parent.
    if (options.foreignKeys === false) native.exec("PRAGMA foreign_keys = OFF")
  } catch (error) {
    native.close()
    throw error
  }

  const owner = nextOwnerID()
  let closed = false

  const assertOpen = () => {
    if (closed) throw new ConnectionClosedError(options.filename)
  }

  const pragma = (name: string): unknown => {
    assertOpen()
    if (!IDENTIFIER.test(name)) throw new Error(`invalid pragma name: ${name}`)
    const row = native.prepare(`PRAGMA ${name}`).get() as Record<string, unknown> | null | undefined
    return row === null || row === undefined ? undefined : Object.values(row)[0]
  }

  const connection: NodeConnection = {
    owner,
    filename: options.filename,
    get closed() {
      return closed
    },
    all<T = Record<string, unknown>>(sql: string, params: QueryParams = []) {
      assertOpen()
      const statement = native.prepare(sql)
      return statement.all(...(spread(params) as unknown as SQLInputValue[])) as unknown as ReadonlyArray<T>
    },
    values<T = ReadonlyArray<unknown>>(sql: string, params: QueryParams = []) {
      assertOpen()
      const statement = native.prepare(sql)
      statement.setReturnArrays(true)
      return statement.all(...(spread(params) as unknown as SQLInputValue[])) as unknown as ReadonlyArray<T>
    },
    run(sql: string, params: QueryParams = []) {
      assertOpen()
      const result = native.prepare(sql).run(...(spread(params) as unknown as SQLInputValue[]))
      return { changes: Number(result.changes), lastInsertRowid: Number(result.lastInsertRowid) }
    },
    exec(sql: string) {
      assertOpen()
      native.exec(sql)
    },
    pragma,
    journalMode() {
      return String(pragma("journal_mode")).toLowerCase()
    },
    close() {
      if (closed) return
      closed = true
      native.close()
    },
    function(name, fn) {
      assertOpen()
      native.function(name, fn as unknown as (...args: SQLOutputValue[]) => SQLInputValue)
    },
    setAuthorizer(authorizer) {
      assertOpen()
      native.setAuthorizer(
        authorizer === undefined
          ? null
          : (action, table, column, database, trigger) => authorizer({ action, table, column, database, trigger }) ?? 0,
      )
    },
  }

  try {
    options.initialize?.(connection)
    connection.setAuthorizer(options.authorizer)
  } catch (error) {
    native.close()
    throw error
  }

  return connection
}
