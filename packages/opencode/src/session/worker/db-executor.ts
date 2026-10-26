// Serialized, off-loop DB executor for the hub.
//
// The executor forwards `run` / `query` / `transaction` calls to one worker
// thread that owns the synchronous `bun:sqlite` handle. It deliberately owns no
// ordering, cache, migration, schema, or lease policy: callers pass SQL and
// params, and the executor returns the driver result. The hub uses it only as a
// measured fallback; workers keep their own WAL connection.

import { Database, type SQLQueryBindings } from "bun:sqlite"
import { isMainThread, parentPort, workerData } from "node:worker_threads"
import { resolveWorkerEntry } from "./port"

export type DbParams = ReadonlyArray<unknown> | Record<string, unknown>

export interface DbRunResult {
  readonly changes: number
  readonly lastInsertRowid: number
}

export interface DbStatement {
  readonly sql: string
  readonly params?: DbParams
}

export interface DbTransactionResult {
  readonly changes: number
  readonly lastInsertRowid: number
}

/** The hub-side executor surface. Nothing else is forwarded across the thread. */
export interface DbExecutor {
  run(sql: string, params?: DbParams): Promise<DbRunResult>
  query(sql: string, params?: DbParams): Promise<ReadonlyArray<Record<string, unknown>>>
  transaction(statements: ReadonlyArray<DbStatement>): Promise<ReadonlyArray<DbTransactionResult>>
  close(): Promise<void>
}

type DbExecutorRequest =
  | { readonly type: "run"; readonly id: number; readonly sql: string; readonly params?: DbParams }
  | { readonly type: "query"; readonly id: number; readonly sql: string; readonly params?: DbParams }
  | {
      readonly type: "transaction"
      readonly id: number
      readonly statements: ReadonlyArray<DbStatement>
    }

interface DbExecutorFailure {
  readonly name: string
  readonly message: string
  readonly code?: string | number
  readonly sqliteCode?: string
}

type DbExecutorResponse =
  | { readonly type: "ready"; readonly threadId: number }
  | { readonly type: "result"; readonly id: number; readonly value: unknown }
  | { readonly type: "failure"; readonly id: number; readonly error: DbExecutorFailure }

export class DbExecutorCrashedError extends Error {
  readonly tag = "DB_EXECUTOR_CRASHED"
  constructor(message: string) {
    super(message)
    this.name = "DbExecutorCrashedError"
  }
}

export class DbExecutorClosedError extends Error {
  readonly tag = "DB_EXECUTOR_CLOSED"
  constructor() {
    super("database executor is closed")
    this.name = "DbExecutorClosedError"
  }
}

export class DbExecutorRemoteError extends Error {
  readonly tag = "DB_EXECUTOR_REMOTE"
  readonly code: string | number | undefined
  readonly sqliteCode: string | undefined
  constructor(failure: DbExecutorFailure) {
    super(failure.message)
    this.name = failure.name
    this.code = failure.code
    this.sqliteCode = failure.sqliteCode
  }
}

export interface DbExecutorOptions {
  /** SQLite filename opened inside the executor thread. */
  readonly filename: string
  /** Applied at open, before any request (e.g. WAL pragmas). */
  readonly pragmas?: ReadonlyArray<string>
  /** Per-request deadline; `0` disables it. Defaults to 30s. */
  readonly timeoutMs?: number
  /** Worker entry override (tests / packaged builds). */
  readonly entry?: string | URL
  readonly env?: Record<string, string | undefined>
  readonly name?: string
}

export interface DbExecutorWorkerData {
  readonly kind: "db-executor"
  readonly filename: string
  readonly pragmas?: ReadonlyArray<string>
}

const DEFAULT_TIMEOUT_MS = 30_000

function nativeBindings(params: DbParams | undefined): SQLQueryBindings[] {
  if (params === undefined) return []
  return (Array.isArray(params) ? [...params] : [params]) as SQLQueryBindings[]
}

function failureOf(error: unknown): DbExecutorFailure {
  if (typeof error !== "object" || error === null) return { name: "Error", message: String(error) }
  const candidate = error as {
    readonly name?: unknown
    readonly message?: unknown
    readonly code?: unknown
    readonly sqliteCode?: unknown
  }
  return {
    name: typeof candidate.name === "string" ? candidate.name : "Error",
    message: typeof candidate.message === "string" ? candidate.message : String(error),
    ...(typeof candidate.code === "string" || typeof candidate.code === "number" ? { code: candidate.code } : {}),
    ...(typeof candidate.sqliteCode === "string" ? { sqliteCode: candidate.sqliteCode } : {}),
  }
}

function runStatement(db: Database, statement: DbStatement): DbRunResult {
  const result = db.query(statement.sql).run(...nativeBindings(statement.params))
  return { changes: Number(result.changes), lastInsertRowid: Number(result.lastInsertRowid) }
}

function runTransaction(db: Database, statements: ReadonlyArray<DbStatement>): ReadonlyArray<DbTransactionResult> {
  db.exec("BEGIN IMMEDIATE")
  try {
    const results = statements.map((statement) => runStatement(db, statement))
    db.exec("COMMIT")
    return results
  } catch (error) {
    db.exec("ROLLBACK")
    throw error
  }
}

function execute(db: Database, request: DbExecutorRequest): unknown {
  switch (request.type) {
    case "run":
      return runStatement(db, { sql: request.sql, ...(request.params === undefined ? {} : { params: request.params }) })
    case "query":
      return db.query(request.sql).all(...nativeBindings(request.params))
    case "transaction":
      return runTransaction(db, request.statements)
  }
}

function startWorkerThread(data: DbExecutorWorkerData): void {
  const transport = parentPort
  if (transport === null) throw new Error("db executor worker started without a parent port")
  const db = new Database(data.filename, { create: true })
  try {
    for (const pragma of data.pragmas ?? []) db.exec(pragma)
    transport.postMessage({ type: "ready", threadId: 0 } satisfies DbExecutorResponse)
    transport.on("message", (message: DbExecutorRequest) => {
      try {
        const value = execute(db, message)
        transport.postMessage({
          type: "result",
          id: message.id,
          value,
        } satisfies DbExecutorResponse)
      } catch (error) {
        transport.postMessage({
          type: "failure",
          id: message.id,
          error: failureOf(error),
        } satisfies DbExecutorResponse)
      }
    })
  } catch (error) {
    db.close()
    throw error
  }
}

function isDbExecutorWorkerData(value: unknown): value is DbExecutorWorkerData {
  if (typeof value !== "object" || value === null) return false
  const data = value as Partial<DbExecutorWorkerData>
  return data.kind === "db-executor" && typeof data.filename === "string"
}

if (!isMainThread && isDbExecutorWorkerData(workerData)) {
  startWorkerThread(workerData)
}

/** Resolve the executor isolate entry (env override, build define, source sibling). */
export function resolveDbExecutorEntry(options: { readonly env?: Record<string, string | undefined> } = {}):
  | string
  | URL {
  return resolveWorkerEntry({
    env: options.env,
    definedPath:
      typeof OPENCODE_DB_EXECUTOR_PATH !== "undefined" ? OPENCODE_DB_EXECUTOR_PATH : undefined,
    envVar: "OPENCODE_DB_EXECUTOR_PATH",
    fallback: new URL("./db-executor.ts", import.meta.url),
  })
}

declare global {
  const OPENCODE_DB_EXECUTOR_PATH: string | undefined
}

interface PendingRequest {
  readonly resolve: (value: unknown) => void
  readonly reject: (error: unknown) => void
  readonly timer: ReturnType<typeof setTimeout> | undefined
}

const terminateWorker = async (worker: Worker | undefined): Promise<void> => {
  if (worker === undefined) return
  try {
    await worker.terminate()
  } catch {
    // The thread is already gone; there is nothing left to release.
  }
}

export function createDbExecutor(options: DbExecutorOptions): DbExecutor {
  const entry = options.entry ?? resolveDbExecutorEntry({ env: options.env })
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS
  let worker: Worker | undefined
  let ready: Promise<void> | undefined
  let closed = false
  let nextId = 1
  const pending = new Map<number, PendingRequest>()

  const failPending = (error: unknown): void => {
    for (const request of pending.values()) {
      if (request.timer !== undefined) clearTimeout(request.timer)
      request.reject(error)
    }
    pending.clear()
  }

  const retire = (error: Error): void => {
    const current = worker
    worker = undefined
    ready = undefined
    failPending(error)
    void terminateWorker(current)
  }

  const ensureReady = (): Promise<void> => {
    if (closed) return Promise.reject(new DbExecutorClosedError())
    if (ready !== undefined) return ready
    ready = new Promise<void>((resolve, reject) => {
      const workerOptions = {
        ...(options.name === undefined ? {} : { name: options.name }),
        workerData: {
          kind: "db-executor",
          filename: options.filename,
          ...(options.pragmas === undefined ? {} : { pragmas: options.pragmas }),
        } satisfies DbExecutorWorkerData,
      } as unknown as WorkerOptions
      const spawned = new Worker(entry, workerOptions)
      worker = spawned
      spawned.onmessage = (event: MessageEvent<DbExecutorResponse>) => {
        const message = event.data
        if (message.type === "ready") {
          resolve()
          return
        }
        const request = pending.get(message.id)
        if (request === undefined) return
        pending.delete(message.id)
        if (request.timer !== undefined) clearTimeout(request.timer)
        if (message.type === "result") request.resolve(message.value)
        else request.reject(new DbExecutorRemoteError(message.error))
      }
      spawned.onerror = (event) => {
        const error = new DbExecutorCrashedError(
          `db executor worker failed: ${event.message || "unknown worker error"}`,
        )
        retire(error)
        reject(error)
      }
      spawned.addEventListener("close", () => {
        if (closed) return
        const error = new DbExecutorCrashedError("db executor worker exited")
        retire(error)
        reject(error)
      })
    })
    return ready
  }

  const request = async (build: (id: number) => DbExecutorRequest): Promise<unknown> => {
    await ensureReady()
    if (closed) throw new DbExecutorClosedError()
    const id = nextId++
    const current = worker
    if (current === undefined) throw new DbExecutorCrashedError("db executor worker is not running")
    return new Promise<unknown>((resolve, reject) => {
      const timer =
        timeoutMs > 0
          ? setTimeout(() => {
              pending.delete(id)
              reject(new DbExecutorCrashedError(`db executor request ${id} timed out after ${timeoutMs}ms`))
            }, timeoutMs)
          : undefined
      pending.set(id, { resolve, reject, timer })
      try {
        current.postMessage(build(id))
      } catch (error) {
        pending.delete(id)
        if (timer !== undefined) clearTimeout(timer)
        reject(error)
      }
    })
  }

  return {
    run: (sql, params) =>
      request((id) => ({
        type: "run",
        id,
        sql,
        ...(params === undefined ? {} : { params }),
      })) as Promise<DbRunResult>,
    query: (sql, params) =>
      request((id) => ({
        type: "query",
        id,
        sql,
        ...(params === undefined ? {} : { params }),
      })) as Promise<ReadonlyArray<Record<string, unknown>>>,
    transaction: (statements) =>
      request((id) => ({ type: "transaction", id, statements })) as Promise<ReadonlyArray<DbTransactionResult>>,
    async close() {
      if (closed) return
      closed = true
      const current = worker
      worker = undefined
      ready = undefined
      failPending(new DbExecutorClosedError())
      await terminateWorker(current)
    },
  }
}

export * as DbExecutorModule from "./db-executor"
