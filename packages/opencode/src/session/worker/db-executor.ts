// Serialized, off-loop DB executor for the hub.
//
// The executor forwards `run` / `query` / `transaction` calls to one worker
// thread that owns the synchronous `bun:sqlite` handle. It deliberately owns no
// ordering, cache, migration, schema, or lease policy: callers pass SQL and
// params, and the executor returns the driver result. The hub uses it only as a
// measured fallback; workers keep their own WAL connection.
//
// The Effect service in this file is the implementation. There is no Promise
// executor underneath it: every operation is an `Effect`, and the worker
// handshake is an `Effect.callback`. The exported `makePromiseExecutor` is the
// single temporary edge for callers that are not Effect yet (the async schema
// gate); the production hub consumes the service directly and adapts only at
// its Promise-shaped `SessionHubOptions.db` boundary.

import { Database, type SQLQueryBindings } from "bun:sqlite"
import { isMainThread, parentPort, threadId, workerData } from "node:worker_threads"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { executorPragmas, resolveWalConfig } from "@opencode-ai/core/database/pragmas"
import { EXECUTOR_BUSY_RETRY_CONFIG, retryBusySync } from "@opencode-ai/core/database/transaction"
import { withBusyRetry, type BusyRetriesExhaustedError } from "@opencode-ai/core/database/busy"
import { Context, Deferred, Duration, Effect, Exit, Layer, Schedule, Schema, Scope, Semaphore } from "effect"
import { resolveWorkerEntry } from "./port"
import { WorkerRuntime } from "./runtime-edge"

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
  /**
   * Same as {@link query} but a busy failure is returned to the caller instead
   * of replaying the statement. The migration path opens one `BEGIN IMMEDIATE`
   * transaction and must fail the whole batch, never re-issue a statement
   * inside a still-open transaction.
   */
  queryInTransaction?(sql: string, params?: DbParams): Promise<ReadonlyArray<Record<string, unknown>>>
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

export class DbExecutorCrashedError extends Schema.TaggedErrorClass<DbExecutorCrashedError>()(
  "DbExecutorCrashedError",
  {
    message: Schema.String,
  },
) {}

export class DbExecutorClosedError extends Schema.TaggedErrorClass<DbExecutorClosedError>()("DbExecutorClosedError", {
  message: Schema.String,
}) {}

export class DbExecutorRemoteError extends Schema.TaggedErrorClass<DbExecutorRemoteError>()("DbExecutorRemoteError", {
  message: Schema.String,
  /** The original driver error name (for example `SQLiteError`). */
  errorName: Schema.optional(Schema.String),
  code: Schema.optional(Schema.Union([Schema.String, Schema.Number])),
  sqliteCode: Schema.optional(Schema.String),
}) {}

/**
 * A request deadline elapsed. The transaction may have committed before the
 * response was lost, so the result is indeterminate: the thread is retired and
 * the caller must not blindly retry a non-idempotent body.
 */
export class DbExecutorTimeoutError extends Schema.TaggedErrorClass<DbExecutorTimeoutError>()(
  "DbExecutorTimeoutError",
  {
    message: Schema.String,
  },
) {
  get indeterminate(): true {
    return true
  }
}

/**
 * A transaction body failed and the follow-up `ROLLBACK` also failed. The
 * original failure is preserved as `cause`; the rollback failure is attached as
 * `rollbackCause`; the executor is poisoned because the transaction state is
 * unknown.
 */
export class DbExecutorRollbackError extends Schema.TaggedErrorClass<DbExecutorRollbackError>()(
  "DbExecutorRollbackError",
  {
    message: Schema.String,
    cause: DbExecutorRemoteError,
    rollbackCause: DbExecutorRemoteError,
  },
) {}

/** A prior failed rollback left the connection unusable; no work may follow. */
export class DbExecutorPoisonedError extends Schema.TaggedErrorClass<DbExecutorPoisonedError>()(
  "DbExecutorPoisonedError",
  {
    message: Schema.String,
  },
) {}

export type DbExecutorError =
  | DbExecutorCrashedError
  | DbExecutorClosedError
  | DbExecutorRemoteError
  | DbExecutorTimeoutError
  | DbExecutorRollbackError
  | DbExecutorPoisonedError

/** A service failure: executor errors plus the shared busy-retry terminal error. */
export type DbExecutorFailureError = DbExecutorError | BusyRetriesExhaustedError

export interface DbExecutorOptions {
  /** SQLite filename opened inside the executor thread. */
  readonly filename: string
  /** Applied at open, before any request (e.g. WAL pragmas). */
  readonly pragmas?: ReadonlyArray<string>
  /** Per-request deadline; `0` disables it. Defaults to 30s. */
  readonly timeoutMs?: number
  /**
   * Idle WAL checkpoint cadence. `0` disables it; defaults to
   * `resolveWalConfig().checkpointIntervalMs`. The checkpoint is issued through
   * the executor so the truncating checkpoint runs on the executor thread and
   * never blocks the hub loop.
   */
  readonly checkpointIntervalMs?: number
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

function remoteErrorOf(
  failure: DbExecutorFailure,
): DbExecutorRemoteError | DbExecutorRollbackError | DbExecutorPoisonedError {
  // The isolate reports poisoning as its own failure kind; keep the typed
  // surface instead of folding it into a generic remote error.
  if (failure.name === "DbExecutorPoisonedError") {
    return new DbExecutorPoisonedError({ message: failure.message })
  }
  if (failure.cause !== undefined && failure.rollbackCause !== undefined) {
    return new DbExecutorRollbackError({
      message: failure.cause.message,
      cause: new DbExecutorRemoteError(remoteFields(failure.cause)),
      rollbackCause: new DbExecutorRemoteError(remoteFields(failure.rollbackCause)),
    })
  }
  return new DbExecutorRemoteError(remoteFields(failure))
}

function remoteFields(failure: DbExecutorFailure): {
  message: string
  errorName: string
  code?: string | number
  sqliteCode?: string
} {
  return {
    message: failure.message,
    errorName: failure.name,
    ...(failure.code === undefined ? {} : { code: failure.code }),
    ...(failure.sqliteCode === undefined ? {} : { sqliteCode: failure.sqliteCode }),
  }
}

function runStatement(db: Database, statement: DbStatement): DbRunResult {
  const result = db.query(statement.sql).run(...nativeBindings(statement.params))
  return { changes: Number(result.changes), lastInsertRowid: Number(result.lastInsertRowid) }
}

/**
 * Run `work` inside one `BEGIN IMMEDIATE` transaction. A busy failure at any
 * point rolls back and retries the whole body, never a single statement: a
 * half-applied transaction is not observable to the caller.
 *
 * This is the synchronous caller on the executor thread; it reuses the shared
 * busy-retry primitive rather than owning a second backoff loop.
 */
function runTransaction(db: Database, statements: ReadonlyArray<DbStatement>): ReadonlyArray<DbTransactionResult> {
  return retryBusySync(() => {
    db.exec("BEGIN IMMEDIATE")
    try {
      const results = statements.map((statement) => runStatement(db, statement))
      // A busy COMMIT did not commit; the transaction is still open, so retry
      // the COMMIT in place instead of replaying the whole body.
      retryBusySync(() => db.exec("COMMIT"), EXECUTOR_BUSY_RETRY_CONFIG)
      return results
    } catch (error) {
      try {
        // A busy ROLLBACK did not roll back; re-issue it in place on the same
        // bounded budget before treating the transaction state as unknown.
        retryBusySync(() => db.exec("ROLLBACK"), EXECUTOR_BUSY_RETRY_CONFIG)
      } catch (rollbackError) {
        // The original failure is the cause; the failed rollback is attached so
        // neither is lost. The message handler poisons the thread afterwards.
        throw new RollbackFailure(error, rollbackError)
      }
      throw error
    }
  }, EXECUTOR_BUSY_RETRY_CONFIG)
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

/** Minimal handle the checkpoint scheduler needs; a fake satisfies it in tests. */
export interface CheckpointTarget {
  exec(sql: string): void
}

/**
 * Start a periodic passive WAL checkpoint. Returns a stop function.
 *
 * The loop is an Effect fiber in its own scope, not `setInterval`: `stop()`
 * closes the scope, which interrupts and joins the fiber. A checkpoint failure
 * is swallowed: the next interval retries, and a busy database must not crash
 * the executor.
 */
export function scheduleCheckpoint(target: CheckpointTarget, intervalMs: number): () => void {
  if (!Number.isFinite(intervalMs) || intervalMs <= 0) return () => {}
  const scope = Scope.makeUnsafe()
  const tick = Effect.sync(() => {
    try {
      target.exec("PRAGMA wal_checkpoint(PASSIVE)")
    } catch {
      // A concurrently held write lock is expected; the next tick retries.
    }
  })
  WorkerRuntime.runSync(tick.pipe(Effect.repeat(Schedule.spaced(Duration.millis(intervalMs))), Effect.forkIn(scope)))
  let stopped = false
  return () => {
    if (stopped) return
    stopped = true
    WorkerRuntime.runSync(Scope.close(scope, Exit.void))
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
          error: failureOf(
            new DbExecutorPoisonedError({ message: "database executor is poisoned by a failed rollback" }),
          ),
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
export function resolveDbExecutorEntry(
  options: { readonly env?: Record<string, string | undefined> } = {},
): string | URL {
  return resolveWorkerEntry({
    env: options.env,
    definedPath: typeof OPENCODE_DB_EXECUTOR_PATH !== "undefined" ? OPENCODE_DB_EXECUTOR_PATH : undefined,
    envVar: "OPENCODE_DB_EXECUTOR_PATH",
    fallback: new URL("./db-executor.ts", import.meta.url),
  })
}

declare global {
  const OPENCODE_DB_EXECUTOR_PATH: string | undefined
}

interface PendingRequest {
  readonly resolve: (value: unknown) => void
  readonly reject: (error: DbExecutorError) => void
}

const terminateWorker = (worker: Worker | undefined): Effect.Effect<void> => {
  if (worker === undefined) return Effect.void
  return Effect.tryPromise({
    try: async () => {
      await worker.terminate()
    },
    catch: () => undefined,
  }).pipe(Effect.ignore)
}

/** How long a closing executor may take to acknowledge its native-handle close. */
const CLOSE_ACK_TIMEOUT_MS = 1_000

/**
 * Ask the isolate to close its `bun:sqlite` handle and wait (bounded) for the
 * acknowledgement before terminating the thread. Without this the native handle
 * outlives the JS realm and its WAL lock survives until the process exits.
 */
const closeWorkerDatabase = (worker: Worker | undefined, id: number): Effect.Effect<void> => {
  if (worker === undefined) return Effect.void
  return Effect.callback<void>((resume) => {
    let settled = false
    let timer: ReturnType<typeof setTimeout> | undefined
    let onMessage: (event: MessageEvent<DbExecutorResponse>) => void
    const finish = (): void => {
      if (settled) return
      settled = true
      if (timer !== undefined) clearTimeout(timer)
      worker.removeEventListener?.("message", onMessage)
      resume(Effect.void)
    }
    onMessage = (event: MessageEvent<DbExecutorResponse>): void => {
      if (event.data?.type === "closed" && event.data.id === id) finish()
    }
    timer = setTimeout(finish, CLOSE_ACK_TIMEOUT_MS)
    timer.unref?.()
    worker.addEventListener?.("message", onMessage)
    try {
      worker.postMessage({ type: "close", id } satisfies DbExecutorRequest)
    } catch {
      finish()
    }
    return Effect.sync(() => {
      if (settled) return
      settled = true
      if (timer !== undefined) clearTimeout(timer)
      worker.removeEventListener?.("message", onMessage)
    })
  })
}

interface ExecutorState {
  worker: Worker | undefined
  /** A crashed/exited worker that still needs `terminate()` before a respawn. */
  staleWorker: Worker | undefined
  ready: Deferred.Deferred<void, DbExecutorError> | undefined
  closed: boolean
  nextId: number
  executorThreadId: number
  readonly pending: Map<number, PendingRequest>
}

interface ExecutorCore extends Interface {
  readonly checkpoint: () => Effect.Effect<void>
  readonly checkpointIntervalMs: number
}

function closedError(): DbExecutorClosedError {
  return new DbExecutorClosedError({ message: "database executor is closed" })
}

function crashedError(message: string): DbExecutorCrashedError {
  return new DbExecutorCrashedError({ message })
}

/**
 * Build the Effect-native executor state. Construction is synchronous and
 * spawns nothing; the worker starts lazily on the first operation (or the first
 * checkpoint after a request has started the worker).
 */
function createExecutorCore(options: DbExecutorOptions, scope: Scope.Scope): ExecutorCore {
  const entry = options.entry ?? resolveDbExecutorEntry({ env: options.env })
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS
  const resolvedWal = resolveWalConfig(options.env)
  const checkpointIntervalMs = options.checkpointIntervalMs ?? resolvedWal.checkpointIntervalMs
  // Applied inside the worker before the ready frame; the default is the shared
  // WAL tuning plus the executor-specific busy budget (1s) and page cache
  // (16 MiB), so a concurrently held write lock waits briefly instead of
  // surfacing SQLITE_BUSY immediately, and the executor does not duplicate the
  // hub's 64 MiB cache.
  const pragmas = options.pragmas ?? executorPragmas(options.env)
  // Serializes spawn with teardown. A timeout retires the old thread
  // asynchronously; the replacement must await that retirement or two SQLite
  // handles (and their WAL locks) would overlap during the handover.
  const workerGate = Semaphore.makeUnsafe(1)
  const state: ExecutorState = {
    worker: undefined,
    staleWorker: undefined,
    ready: undefined,
    closed: false,
    nextId: 1,
    executorThreadId: 0,
    pending: new Map(),
  }

  const failPending = (error: DbExecutorError): void => {
    for (const request of [...state.pending.values()]) request.reject(error)
    state.pending.clear()
  }

  const failBoot = (error: DbExecutorError): void => {
    const ready = state.ready
    state.ready = undefined
    if (ready !== undefined) Deferred.doneUnsafe(ready, Effect.fail(error))
  }

  const retire = (error: DbExecutorError, orderly: boolean): Effect.Effect<void> =>
    workerGate.withPermits(1)(
      Effect.suspend(() => {
        const live = state.worker
        const stale = state.staleWorker
        state.worker = undefined
        state.staleWorker = undefined
        failBoot(error)
        failPending(error)
        return Effect.gen(function* () {
          if (live !== undefined) {
            if (orderly) yield* closeWorkerDatabase(live, state.nextId++)
            yield* terminateWorker(live)
          }
          if (stale !== undefined) yield* terminateWorker(stale)
        })
      }),
    )

  /**
   * Wait for a prior retirement to finish, then spawn the isolate. The boot is
   * forked into the service scope, so interrupting one caller does not abandon
   * the shared handshake.
   */
  const bootWorker = (ready: Deferred.Deferred<void, DbExecutorError>): Effect.Effect<void> =>
    Effect.gen(function* () {
      yield* workerGate
        .withPermits(1)(
          Effect.gen(function* () {
            if (state.closed) return yield* closedError()
            const stale = state.staleWorker
            state.staleWorker = undefined
            if (stale !== undefined) yield* terminateWorker(stale)
            const spawned = yield* Effect.try({
              try: () => {
                const workerOptions = {
                  ...(options.name === undefined ? {} : { name: options.name }),
                  workerData: {
                    kind: "db-executor",
                    filename: options.filename,
                    pragmas,
                    ...(options.create === undefined ? {} : { create: options.create }),
                  } satisfies DbExecutorWorkerData,
                } as unknown as WorkerOptions
                return new Worker(entry, workerOptions)
              },
              catch: (error) => crashedError(`db executor worker failed to start: ${errorMessage(error)}`),
            })
            state.worker = spawned
            spawned.onmessage = (event: MessageEvent<DbExecutorResponse>) => {
              // A retired worker's late frames must never settle a request for, or
              // resolve, a replacement thread.
              if (state.worker !== spawned) return
              const message = event.data
              if (message.type === "ready") {
                state.executorThreadId = message.threadId
                Deferred.doneUnsafe(ready, Effect.void)
                return
              }
              // A close acknowledgement has no request to settle; `close()` waits
              // for it through its own listener.
              if (message.type === "closed") return
              const request = state.pending.get(message.id)
              if (request === undefined) return
              if (message.type === "result") request.resolve(message.value)
              else request.reject(remoteErrorOf(message.error))
            }
            spawned.onerror = (event) => {
              if (state.worker !== spawned) return
              crash(spawned, crashedError(`db executor worker failed: ${event.message || "unknown worker error"}`))
            }
            spawned.addEventListener("close", () => {
              if (state.closed || state.worker !== spawned) return
              crash(spawned, crashedError("db executor worker exited"))
            })
          }),
        )
        .pipe(
          Effect.catch((error) =>
            Effect.sync(() => {
              Deferred.doneUnsafe(ready, Effect.fail(error))
            }),
          ),
        )
      // The deferred carries success or failure to every waiter; the boot fiber
      // itself has nothing left to report.
      yield* Deferred.await(ready).pipe(Effect.ignore)
    })

  const crash = (spawned: Worker, error: DbExecutorCrashedError): void => {
    if (state.worker !== spawned) return
    state.worker = undefined
    state.staleWorker = spawned
    failBoot(error)
    failPending(error)
  }

  const ensureReady = (): Effect.Effect<void, DbExecutorError> =>
    Effect.suspend(() => {
      if (state.closed) return Effect.fail(closedError())
      if (state.ready !== undefined) return Deferred.await(state.ready)
      const ready = Deferred.makeUnsafe<void, DbExecutorError>()
      state.ready = ready
      return Effect.forkIn(bootWorker(ready), scope).pipe(Effect.flatMap(() => Deferred.await(ready)))
    })

  const requestOnce = (build: (id: number) => DbExecutorRequest): Effect.Effect<unknown, DbExecutorError> =>
    Effect.gen(function* () {
      yield* ensureReady()
      if (state.closed) return yield* closedError()
      const current = state.worker
      if (current === undefined) return yield* crashedError("db executor worker is not running")
      const id = state.nextId++
      return yield* Effect.callback<unknown, DbExecutorError>((resume) => {
        let done = false
        let timer: ReturnType<typeof setTimeout> | undefined
        const clearTimer = (): void => {
          if (timer !== undefined) clearTimeout(timer)
        }
        const finish = (effect: Effect.Effect<unknown, DbExecutorError>): void => {
          if (done) return
          done = true
          clearTimer()
          state.pending.delete(id)
          resume(effect)
        }
        if (timeoutMs > 0) {
          timer = setTimeout(() => {
            if (done) return
            done = true
            clearTimer()
            state.pending.delete(id)
            // A deadline is indeterminate: the transaction may have committed
            // before the response was lost. Retire the whole thread so a retry
            // starts on a fresh connection and no half-open transaction is
            // reused. Unlike a crash, callers can tell this apart by type.
            const error = new DbExecutorTimeoutError({
              message: `db executor request ${id} timed out after ${timeoutMs}ms; the transaction may have committed`,
            })
            resume(
              Effect.gen(function* () {
                yield* retire(error, true)
                return yield* Effect.fail(error)
              }),
            )
          }, timeoutMs)
        }
        state.pending.set(id, {
          resolve: (value) => finish(Effect.succeed(value)),
          reject: (error) => finish(Effect.fail(error)),
        })
        try {
          current.postMessage(build(id))
        } catch (error) {
          finish(Effect.fail(crashedError(`db executor request failed: ${errorMessage(error)}`)))
        }
        // Interruption removes the pending entry; the statement itself may still
        // finish on the executor thread, and its late result is ignored.
        return Effect.sync(() => {
          if (done) return
          done = true
          clearTimer()
          state.pending.delete(id)
        })
      })
    })

  const busy = <A>(effect: Effect.Effect<A, DbExecutorError>): Effect.Effect<A, DbExecutorFailureError> =>
    withBusyRetry(effect, EXECUTOR_BUSY_RETRY_CONFIG)

  return {
    run: (sql, params) =>
      busy(
        requestOnce((id) => ({
          type: "run",
          id,
          sql,
          ...(params === undefined ? {} : { params }),
        })) as Effect.Effect<DbRunResult, DbExecutorError>,
      ),
    query: (sql, params) =>
      busy(
        requestOnce((id) => ({
          type: "query",
          id,
          sql,
          ...(params === undefined ? {} : { params }),
        })) as Effect.Effect<ReadonlyArray<Record<string, unknown>>, DbExecutorError>,
      ),
    queryInTransaction: (sql, params) =>
      requestOnce((id) => ({
        type: "query",
        id,
        sql,
        ...(params === undefined ? {} : { params }),
      })) as Effect.Effect<ReadonlyArray<Record<string, unknown>>, DbExecutorError>,
    transaction: (statements) =>
      requestOnce((id) => ({ type: "transaction", id, statements })) as Effect.Effect<
        ReadonlyArray<DbTransactionResult>,
        DbExecutorError
      >,
    close: () =>
      Effect.suspend(() => {
        if (state.closed) return Effect.void
        state.closed = true
        const error = closedError()
        failBoot(error)
        failPending(error)
        return workerGate.withPermits(1)(
          Effect.gen(function* () {
            // Read the current handles after acquiring the gate: a boot that was
            // between its closed check and its spawn cannot be missed.
            const live = state.worker
            const stale = state.staleWorker
            state.worker = undefined
            state.staleWorker = undefined
            // The isolate must close its own native handle while its realm still
            // exists; `terminate()` alone would strand the WAL lock.
            if (live !== undefined) {
              yield* closeWorkerDatabase(live, state.nextId++)
              yield* terminateWorker(live)
            }
            if (stale !== undefined) yield* terminateWorker(stale)
          }),
        )
      }),
    get threadId() {
      return state.executorThreadId
    },
    checkpoint: () =>
      Effect.suspend(() => {
        if (state.closed || state.worker === undefined) return Effect.void
        return requestOnce((id) => ({ type: "query", id, sql: "PRAGMA wal_checkpoint(PASSIVE)" })).pipe(
          Effect.asVoid,
          Effect.catchCause(() => Effect.logWarning("db executor passive checkpoint failed")),
        )
      }),
    checkpointIntervalMs,
  }
}

// ---------------------------------------------------------------------------
// Effect service shape.
// ---------------------------------------------------------------------------

/**
 * The Effect-facing executor surface. Every method is the implementation: it
 * owns the worker handshake, deadline, retirement, and detach semantics.
 */
export interface Interface {
  readonly run: (sql: string, params?: DbParams) => Effect.Effect<DbRunResult, DbExecutorFailureError>
  readonly query: (
    sql: string,
    params?: DbParams,
  ) => Effect.Effect<ReadonlyArray<Record<string, unknown>>, DbExecutorFailureError>
  /** Query without busy replay; the migration batch owns its transaction. */
  readonly queryInTransaction: (
    sql: string,
    params?: DbParams,
  ) => Effect.Effect<ReadonlyArray<Record<string, unknown>>, DbExecutorError>
  readonly transaction: (
    statements: ReadonlyArray<DbStatement>,
  ) => Effect.Effect<ReadonlyArray<DbTransactionResult>, DbExecutorError>
  readonly close: () => Effect.Effect<void>
  /** The isolate's real thread id; `0` until the ready frame arrives. */
  readonly threadId: number
}

export class Service extends Context.Service<Service, Interface>()("@opencode/DbExecutor") {}

/**
 * Acquire the executor and fork its passive-checkpoint loop into the current
 * scope. Closing the scope interrupts and joins the checkpoint before the
 * service's release finalizer closes the worker.
 */
export function make(options: DbExecutorOptions): Effect.Effect<Interface, never, Scope.Scope> {
  return Effect.gen(function* () {
    const scope = yield* Scope.Scope
    const core = yield* Effect.acquireRelease(
      Effect.sync(() => createExecutorCore(options, scope)),
      (executor) => executor.close(),
    )
    if (core.checkpointIntervalMs > 0) {
      yield* core
        .checkpoint()
        .pipe(Effect.repeat(Schedule.spaced(Duration.millis(core.checkpointIntervalMs))), Effect.forkScoped)
    }
    return core
  })
}

/** Install a `DbExecutor` implementation into the context. */
export function layer(options: DbExecutorOptions): Layer.Layer<Service> {
  return Layer.effect(Service, make(options))
}

/** The unbound graph node for {@link Service}; this lane does not wire a consumer. */
export function node(options: DbExecutorOptions): LayerNode.Node<Service, never, undefined> {
  return LayerNode.make({ service: Service, layer: layer(options), deps: [] })
}

// ---------------------------------------------------------------------------
// Temporary Promise edge.
//
// The async schema gate and some legacy tests are not Effect yet. These helpers
// acquire the same service and expose its methods as Promises. The production
// hub uses the service directly; it only adapts at `SessionHubOptions.db`.
// ---------------------------------------------------------------------------

function promiseFacade(service: Interface, close: () => Promise<void>): DbExecutor {
  return {
    run: (sql, params) => WorkerRuntime.runPromise(service.run(sql, params)),
    query: (sql, params) => WorkerRuntime.runPromise(service.query(sql, params)),
    queryInTransaction: (sql, params) => WorkerRuntime.runPromise(service.queryInTransaction(sql, params)),
    transaction: (statements) => WorkerRuntime.runPromise(service.transaction(statements)),
    close,
    get threadId() {
      return service.threadId
    },
  }
}

/** Adapt a live service to the legacy Promise surface (hub internals only). */
export function toPromiseExecutor(service: Interface): DbExecutor {
  return promiseFacade(service, () => WorkerRuntime.runPromise(service.close()))
}

/**
 * Acquire the service on a private scope and return its Promise facade.
 *
 * This is the single temporary edge for non-Effect callers. The returned
 * handle must be closed; that closes the service scope (worker + checkpoint).
 */
export function makePromiseExecutor(options: DbExecutorOptions): DbExecutor {
  const scope = Scope.makeUnsafe()
  const service = WorkerRuntime.runSync(Scope.provide(scope)(make(options)))
  let closing: Promise<void> | undefined
  return promiseFacade(service, () => (closing ??= WorkerRuntime.runPromise(Scope.close(scope, Exit.void))))
}

/** @deprecated Use {@link Service} / {@link make} in production; kept for legacy tests. */
export function createDbExecutor(options: DbExecutorOptions): DbExecutor {
  return makePromiseExecutor(options)
}

export * as DbExecutor from "./db-executor"
