// Per-connection SQLite helper for Session workers (D5).
//
// There is no writer isolate and no singleton. Every worker opens its own
// `bun:sqlite` handle through this module; the shared WAL tuning is applied
// before the first statement and a rejected pragma closes the handle again. The
// exported surface deliberately matches the Bus `Connection` contract
// (`all`/`run`/`exec`) and the retained `session/writer/connection.ts` helper.

import { randomUUID } from "node:crypto"
import { Database, type SQLQueryBindings } from "bun:sqlite"
import { walPragmas } from "./pragmas"

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
  readonly create?: boolean
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
      ...(options.journalSizeLimitBytes === undefined
        ? {}
        : { journalSizeLimitBytes: options.journalSizeLimitBytes }),
      busyTimeoutMs: options.busyTimeoutMs ?? WORKER_BUSY_TIMEOUT_MS,
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
