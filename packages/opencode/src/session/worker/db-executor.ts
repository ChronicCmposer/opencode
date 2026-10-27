// Serialized, off-loop DB executor for the hub.
//
// The executor forwards `run` / `query` / `transaction` calls to one worker
// thread that owns the synchronous `bun:sqlite` handle. It deliberately owns no
// ordering, cache, migration, schema, or lease policy: callers pass SQL and
// params, and the executor returns the driver result. The hub uses it only as a
// measured fallback; workers keep their own WAL connection.

import { Database, type SQLQueryBindings } from "bun:sqlite"
import { isMainThread, parentPort, threadId, workerData } from "node:worker_threads"
import { resolveWalConfig, walPragmas } from "@opencode-ai/core/database/pragmas"
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
  /** The isolate's real thread id; `0` until the ready frame arrives. */
  readonly threadId: number
}

type DbExecutorRequest =
  | { readonly type: "run"; readonly id: number; readonly sql: string; readonly params?: DbParams }
  | { readonly type: "query"; readonly id: number; readonly sql: string; readonly params?: DbParams }
  | {
      readonly type: "transaction"
      readonly id: number
      readonly statements: ReadonlyArray<DbStatement>
    }
  /**
   * Ask the isolate to close its native handle before the thread is terminated.
   * `Worker.terminate()` destroys the JS realm without running finalizers, so a
   * live `bun:sqlite` connection would keep its WAL locks until process exit.
   */
  | { readonly type: "close"; readonly id: number }

interface DbExecutorFailure {
  readonly name: string
  readonly message: string
  readonly code?: string | number
  readonly sqliteCode?: string
  /** The original transaction failure, when a rollback also failed. */
  readonly cause?: DbExecutorFailure
  /** The follow-up ROLLBACK failure, when a rollback also failed. */
  readonly rollbackCause?: DbExecutorFailure
}

type DbExecutorResponse =
  | { readonly type: "ready"; readonly threadId: number }
  | { readonly type: "result"; readonly id: number; readonly value: unknown }
  | { readonly type: "failure"; readonly id: number; readonly error: DbExecutorFailure }
  | { readonly type: "closed"; readonly id: number }

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

/**
 * A request deadline elapsed. The transaction may have committed before the
 * response was lost, so the result is indeterminate: the thread is retired and
 * the caller must not blindly retry a non-idempotent body.
 */
export class DbExecutorTimeoutError extends Error {
  readonly tag = "DB_EXECUTOR_TIMEOUT"
  readonly indeterminate = true
  constructor(message: string) {
    super(message)
    this.name = "DbExecutorTimeoutError"
  }
}

/**
 * A transaction body failed and the follow-up `ROLLBACK` also failed. The
 * original failure is preserved as `cause`; the rollback failure is attached as
 * `rollbackCause`; the executor is poisoned because the transaction state is
 * unknown.
 */
export class DbExecutorRollbackError extends Error {
  readonly tag = "DB_EXECUTOR_ROLLBACK_FAILED"
  override readonly cause: Error
  readonly rollbackCause: DbExecutorRemoteError
  constructor(original: DbExecutorFailure, rollback: DbExecutorFailure) {
    super(original.message)
    this.name = "DbExecutorRollbackError"
    this.cause = new DbExecutorRemoteError(original)
    this.rollbackCause = new DbExecutorRemoteError(rollback)
  }
}

/** A prior failed rollback left the connection unusable; no work may follow. */
export class DbExecutorPoisonedError extends Error {
  readonly tag = "DB_EXECUTOR_POISONED"
  constructor() {
    super("database executor is poisoned by a failed rollback")
    this.name = "DbExecutorPoisonedError"
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
  /**
   * Create the database file when absent. A gated probe passes `false` so a
   * missing or unmigrated database is refused without leaving a stray file.
   */
  readonly create?: boolean
}

export interface DbExecutorWorkerData {
  readonly kind: "db-executor"
  readonly filename: string
  readonly pragmas?: ReadonlyArray<string>
  readonly create?: boolean
}

const DEFAULT_TIMEOUT_MS = 30_000

function nativeBindings(params: DbParams | undefined): SQLQueryBindings[] {
  if (params === undefined) return []
  return (Array.isArray(params) ? [...params] : [params]) as SQLQueryBindings[]
}

class RollbackFailure extends Error {
  readonly tag = "DB_EXECUTOR_ROLLBACK_FAILED"
  constructor(
    readonly original: unknown,
    readonly rollback: unknown,
  ) {
    super(errorMessage(original))
    this.name = "DbExecutorRollbackError"
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

function failureOf(error: unknown): DbExecutorFailure {
  if (typeof error !== "object" || error === null) return { name: "Error", message: String(error) }
  const candidate = error as {
    readonly name?: unknown
    readonly message?: unknown
    readonly code?: unknown
    readonly sqliteCode?: unknown
  }
  const rollback =
    error instanceof RollbackFailure
      ? { cause: failureOf(error.original), rollbackCause: failureOf(error.rollback) }
      : {}
  return {
    name: typeof candidate.name === "string" ? candidate.name : "Error",
    message: typeof candidate.message === "string" ? candidate.message : String(error),
    ...(typeof candidate.code === "string" || typeof candidate.code === "number" ? { code: candidate.code } : {}),
    ...(typeof candidate.sqliteCode === "string" ? { sqliteCode: candidate.sqliteCode } : {}),
    ...rollback,
  }
}

function remoteErrorOf(failure: DbExecutorFailure): Error {
  if (failure.cause !== undefined && failure.rollbackCause !== undefined) {
    return new DbExecutorRollbackError(failure.cause, failure.rollbackCause)
  }
  return new DbExecutorRemoteError(failure)
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
    try {
      db.exec("ROLLBACK")
    } catch (rollbackError) {
      // The original failure is the cause; the failed rollback is attached so
      // neither is lost. The message handler poisons the thread afterwards.
      throw new RollbackFailure(error, rollbackError)
    }
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
    case "close":
      return undefined
  }
}

function startWorkerThread(data: DbExecutorWorkerData): void {
  const transport = parentPort
  if (transport === null) throw new Error("db executor worker started without a parent port")
  const db = new Database(data.filename, { create: data.create ?? true })
  try {
    for (const pragma of data.pragmas ?? []) db.exec(pragma)
    let poisoned = false
    transport.postMessage({ type: "ready", threadId } satisfies DbExecutorResponse)
    transport.on("message", (message: DbExecutorRequest) => {
      // A close is honoured even by a poisoned handle: releasing the native
      // handle is the whole point, and it must never be answered with a retry.
      if (message.type === "close") {
        try {
          db.close()
        } catch {
          // The handle may already be unusable; the lock is released either way.
        }
        transport.postMessage({ type: "closed", id: message.id } satisfies DbExecutorResponse)
        return
      }
      if (poisoned) {
        transport.postMessage({
          type: "failure",
          id: message.id,
          error: failureOf(new DbExecutorPoisonedError()),
        } satisfies DbExecutorResponse)
        return
      }
      try {
        const value = execute(db, message)
        transport.postMessage({
          type: "result",
          id: message.id,
          value,
        } satisfies DbExecutorResponse)
      } catch (error) {
        if (error instanceof RollbackFailure) {
          // The transaction state is unknown; never reuse this handle.
          poisoned = true
          try {
            db.close()
          } catch {
            // The handle may already be unusable.
          }
        }
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

/** How long a closing executor may take to acknowledge its native-handle close. */
const CLOSE_ACK_TIMEOUT_MS = 1_000

/**
 * Ask the isolate to close its `bun:sqlite` handle and wait (bounded) for the
 * acknowledgement before terminating the thread. Without this the native handle
 * outlives the JS realm and its WAL lock survives until the process exits.
 */
const closeWorkerDatabase = async (worker: Worker | undefined, id: number): Promise<void> => {
  if (worker === undefined) return
  await new Promise<void>((resolve) => {
    let settled = false
    let timer: ReturnType<typeof setTimeout>
    const onMessage = (event: MessageEvent<DbExecutorResponse>): void => {
      if (event.data?.type === "closed" && event.data.id === id) finish()
    }
    const finish = (): void => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      worker.removeEventListener?.("message", onMessage)
      resolve()
    }
    timer = setTimeout(finish, CLOSE_ACK_TIMEOUT_MS)
    timer.unref?.()
    worker.addEventListener?.("message", onMessage)
    try {
      worker.postMessage({ type: "close", id } satisfies DbExecutorRequest)
    } catch {
      finish()
    }
  })
}

export function createDbExecutor(options: DbExecutorOptions): DbExecutor {
  const entry = options.entry ?? resolveDbExecutorEntry({ env: options.env })
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS
  // Applied inside the worker before the ready frame; the default is the shared
  // WAL tuning (journal mode, synchronous, cache, foreign keys, size cap) plus a
  // driver-level busy timeout, so a concurrently held write lock waits instead
  // of surfacing SQLITE_BUSY immediately.
  const pragmas = options.pragmas ?? walPragmas(resolveWalConfig(options.env))
  let worker: Worker | undefined
  let ready: Promise<void> | undefined
  let closed = false
  let nextId = 1
  let executorThreadId = 0
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
          pragmas,
          ...(options.create === undefined ? {} : { create: options.create }),
        } satisfies DbExecutorWorkerData,
      } as unknown as WorkerOptions
      const spawned = new Worker(entry, workerOptions)
      worker = spawned
      spawned.onmessage = (event: MessageEvent<DbExecutorResponse>) => {
        // A retired worker's late frames must never settle a request for, or
        // resolve, a replacement thread.
        if (worker !== spawned) return
        const message = event.data
        if (message.type === "ready") {
          executorThreadId = message.threadId
          resolve()
          return
        }
        // A close acknowledgement has no request to settle; `close()` waits for
        // it through its own listener.
        if (message.type === "closed") return
        const request = pending.get(message.id)
        if (request === undefined) return
        pending.delete(message.id)
        if (request.timer !== undefined) clearTimeout(request.timer)
        if (message.type === "result") request.resolve(message.value)
        else request.reject(remoteErrorOf(message.error))
      }
      spawned.onerror = (event) => {
        if (worker !== spawned) return
        const error = new DbExecutorCrashedError(
          `db executor worker failed: ${event.message || "unknown worker error"}`,
        )
        retire(error)
        reject(error)
      }
      spawned.addEventListener("close", () => {
        if (closed || worker !== spawned) return
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
              // A deadline is indeterminate: the transaction may have committed
              // before the response was lost. Retire the whole thread so a retry
              // starts on a fresh connection and no half-open transaction is
              // reused. Unlike a crash, callers can tell this apart by type.
              retire(
                new DbExecutorTimeoutError(
                  `db executor request ${id} timed out after ${timeoutMs}ms; the transaction may have committed`,
                ),
              )
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
    get threadId() {
      return executorThreadId
    },
    async close() {
      if (closed) return
      closed = true
      const current = worker
      worker = undefined
      ready = undefined
      failPending(new DbExecutorClosedError())
      // The isolate must close its own native handle while its realm still
      // exists; `terminate()` alone would strand the WAL lock.
      await closeWorkerDatabase(current, nextId++)
      await terminateWorker(current)
    },
  }
}

export * as DbExecutorModule from "./db-executor"
