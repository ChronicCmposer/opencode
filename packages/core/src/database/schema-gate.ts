// Cross-process schema gate (D7).
//
// One row in a sidecar SQLite file is the database-level lock. It is acquired,
// renewed, and released in `BEGIN IMMEDIATE`, carries an owner + lease deadline
// so a crashed process cannot block forever, and is held across the whole
// migrate/verify/mark sequence. A sidecar file is used because the main database
// is empty until core's migration creates its first table, and core refuses a
// non-empty file with no session table.
//
// Two callers share this implementation:
//   * core's `Database` layer uses {@link withSchemaGate}, which forks a scoped
//     renewal fiber for the migration duration and releases the lease in a
//     finalizer on success, failure, and interruption.
//   * the session hub's synchronous/async schema-ensure uses the plain helpers
//     ({@link waitForSchemaGate}, {@link startSchemaGateRenewal}, …), so the
//     lease rules can never drift between the two paths.

export * as SchemaGate from "./schema-gate"

import { randomUUID } from "node:crypto"
import { Deferred, Effect, Schedule, type Scope } from "effect"
import { openConnection, sleepSync, type Connection } from "./connection"
import {
  BusyExhaustedError,
  inImmediateTransaction,
  retryBusySync,
  type BusyRetryConfig,
} from "./transaction"

const SCHEMA_GATE_TABLE_SQL =
  "CREATE TABLE IF NOT EXISTS schema_gate (id INTEGER PRIMARY KEY CHECK (id = 1), owner TEXT NOT NULL, expires_at INTEGER NOT NULL)"

/** How long one acquired lease is valid without a renewal. */
export const SCHEMA_GATE_TTL_MS = 2 * 60 * 1000

/** Renewal cadence: four renewals fit inside one TTL. */
export const SCHEMA_GATE_RENEW_MS = 30 * 1000

/** Pause between acquisition attempts while another process holds the gate. */
export const SCHEMA_GATE_POLL_MS = 25

/** Bounded wait for the gate; a stuck peer eventually expires instead. */
export const SCHEMA_GATE_TIMEOUT_MS = 2 * 60 * 1000

/** Busy budget for opening/applying the sidecar's WAL pragmas. */
export const SCHEMA_GATE_BUSY_TIMEOUT_MS = 5_000

export class SchemaGateTimeoutError extends Error {
  readonly tag = "WorkerDb.SchemaGateTimeout"

  constructor() {
    super("timed out waiting for the cross-process schema gate")
    this.name = "SchemaGateTimeoutError"
  }
}

export class SchemaGateLostError extends Error {
  readonly tag = "WorkerDb.SchemaGateLost"

  constructor(options: { readonly cause?: unknown } = {}) {
    super(
      "the cross-process schema gate lease was lost before completion",
      options.cause === undefined ? undefined : { cause: options.cause },
    )
    this.name = "SchemaGateLostError"
  }
}

/** Short bounded retry for the lease's own maintenance writes. */
const SCHEMA_GATE_MAINTENANCE_RETRY: BusyRetryConfig = Object.freeze({
  retries: 3,
  baseDelayMs: 5,
  maxDelayMs: 50,
})

export interface GateToken {
  readonly owner: string
  readonly ttlMs: number
  expiresAt: number
}

export function newGateToken(ttlMs: number = SCHEMA_GATE_TTL_MS): GateToken {
  return { owner: `${process.pid}:${randomUUID()}`, ttlMs, expiresAt: Date.now() + ttlMs }
}

/** The lock database is a sibling of the real database; in-memory has no peers. */
export function schemaGateFilename(filename: string): string {
  return `${filename}.schema-gate`
}

export function isMemoryDatabase(filename: string): boolean {
  return filename === ":memory:" || filename.startsWith("file::memory:")
}

/**
 * Open the sidecar gate handle. The driver applies WAL pragmas before this
 * returns; `openConnection` retries a busy open, and the five-second busy budget
 * covers a peer that is between its own open and `BEGIN IMMEDIATE`.
 */
export function openSchemaGate(filename: string): Connection {
  return openConnection({
    filename: schemaGateFilename(filename),
    busyTimeoutMs: SCHEMA_GATE_BUSY_TIMEOUT_MS,
  })
}

interface GateRow {
  readonly owner: string
  readonly expires_at: number
}

function readGateExpiry(connection: Pick<Connection, "all">): number | undefined {
  const row = connection.all<GateRow>("SELECT owner, expires_at FROM schema_gate WHERE id = 1")[0]
  return row === undefined ? undefined : Number(row.expires_at)
}

function holdGate(connection: Pick<Connection, "run">, token: GateToken): void {
  connection.run("INSERT OR REPLACE INTO schema_gate (id, owner, expires_at) VALUES (1, ?, ?)", [
    token.owner,
    token.expiresAt,
  ])
}

/**
 * One acquisition attempt. Returns `false` when a live lease is held elsewhere
 * or when the sidecar's write lock is busy after its bounded retries; both mean
 * "poll again", never "fail the migration".
 */
export function acquireSchemaGate(connection: Connection, token: GateToken): boolean {
  try {
    return inImmediateTransaction(connection, (tx) => {
      tx.exec(SCHEMA_GATE_TABLE_SQL)
      const heldUntil = readGateExpiry(tx)
      if (heldUntil !== undefined && heldUntil > Date.now()) return false
      // Start the lease at acquisition, not at token creation: a caller that
      // waited out a peer's migration must not hold an already-expired row.
      token.expiresAt = Date.now() + token.ttlMs
      holdGate(tx, token)
      return true
    })
  } catch (error) {
    if (error instanceof BusyExhaustedError) return false
    throw error
  }
}

export function releaseSchemaGate(connection: Pick<Connection, "run">, token: GateToken): void {
  const deleteLease = (): void => {
    connection.run("DELETE FROM schema_gate WHERE id = 1 AND owner = ?", [token.owner])
  }
  try {
    retryBusySync(deleteLease, SCHEMA_GATE_MAINTENANCE_RETRY)
    return
  } catch (deleteError) {
    // A busy DELETE leaves the row live for a full TTL, stalling the next
    // acquirer even though this owner is done. Expire it instead, retrying the
    // expiry write on the same bounded budget before giving up loudly.
    try {
      retryBusySync(
        () =>
          connection.run("UPDATE schema_gate SET expires_at = 0 WHERE id = 1 AND owner = ?", [token.owner]),
        SCHEMA_GATE_MAINTENANCE_RETRY,
      )
    } catch (expireError) {
      console.warn(
        `[schema-gate] failed to release lease ${token.owner}: ${describeError(deleteError)}; ` +
          `fallback expiry failed: ${describeError(expireError)}`,
      )
    }
  }
}

function describeError(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

/**
 * Extend a live lease. Zero rows changed means another process took the expired
 * lease; the caller must stop trusting its ownership instead of writing on.
 */
export function renewSchemaGate(connection: Pick<Connection, "run">, token: GateToken): void {
  const expiresAt = Date.now() + token.ttlMs
  const { changes } = retryBusySync(
    () =>
      connection.run("UPDATE schema_gate SET expires_at = ? WHERE id = 1 AND owner = ?", [
        expiresAt,
        token.owner,
      ]),
    SCHEMA_GATE_MAINTENANCE_RETRY,
  )
  if (changes === 0) throw new SchemaGateLostError()
  token.expiresAt = expiresAt
}

/**
 * The renewal signal for the non-Effect paths (the session hub's executor boot).
 * `lost` rejects as soon as a renewal can no longer prove ownership; the caller
 * must race it against the migration and abort before its final commit. `stop`
 * is idempotent and never rejects `lost` afterwards.
 */
export interface SchemaGateRenewal {
  readonly lost: Promise<never>
  stop(): void
}

export function startSchemaGateRenewal(
  connection: Pick<Connection, "run">,
  token: GateToken,
  intervalMs: number = SCHEMA_GATE_RENEW_MS,
): SchemaGateRenewal {
  let stopped = false
  let settleLost: ((error: SchemaGateLostError) => void) | undefined
  const lost = new Promise<never>((_, reject) => {
    settleLost = reject
  })
  // The signal is optional: a caller that only stops the loop must not observe
  // an unhandled rejection when the lease is lost first.
  lost.catch(() => {})

  const stop = (): void => {
    if (stopped) return
    stopped = true
    clearInterval(timer)
  }
  const timer = setInterval(() => {
    if (stopped) return
    try {
      renewSchemaGate(connection, token)
    } catch (cause) {
      stop()
      settleLost?.(
        cause instanceof SchemaGateLostError ? cause : new SchemaGateLostError({ cause }),
      )
    }
  }, intervalMs)
  timer.unref?.()
  return { lost, stop }
}

export function waitForSchemaGate(
  connection: Connection,
  token: GateToken,
  timeoutMs: number = SCHEMA_GATE_TIMEOUT_MS,
): void {
  const deadline = Date.now() + timeoutMs
  while (!acquireSchemaGate(connection, token)) {
    if (Date.now() > deadline) throw new SchemaGateTimeoutError()
    sleepSync(SCHEMA_GATE_POLL_MS)
  }
}

export async function waitForSchemaGateAsync(
  connection: Connection,
  token: GateToken,
  timeoutMs: number = SCHEMA_GATE_TIMEOUT_MS,
  onWait?: () => void,
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!acquireSchemaGate(connection, token)) {
    onWait?.()
    if (Date.now() > deadline) throw new SchemaGateTimeoutError()
    await new Promise((resolve) => setTimeout(resolve, SCHEMA_GATE_POLL_MS))
  }
}

export interface SchemaGateLease {
  readonly connection: Connection
  readonly token: GateToken
  renew(): Effect.Effect<void, SchemaGateLostError>
  release(): Effect.Effect<void>
}

function makeLease(
  connection: Connection,
  token: GateToken,
  releaseOwnership: () => void,
): SchemaGateLease {
  return {
    connection,
    token,
    renew: () =>
      Effect.try({
        try: () => renewSchemaGate(connection, token),
        catch: (cause) =>
          cause instanceof SchemaGateLostError ? cause : new SchemaGateLostError({ cause }),
      }),
    release: () =>
      Effect.sync(() => {
        releaseOwnership()
        connection.close()
      }),
  }
}

export interface SchemaGateOptions {
  /** Lease validity window; tests shorten it to exercise renewal. */
  readonly ttlMs?: number
  /** Renewal cadence while the migration runs. */
  readonly renewIntervalMs?: number
  readonly pollMs?: number
  readonly timeoutMs?: number
  /** Test seam: fires once an acquisition attempt finds another live lease. */
  readonly onWaitForLease?: () => void
  /** Test seam: receives the sidecar handle as soon as it opens. */
  readonly onLeaseOpened?: (connection: Connection) => void
}

function acquireLease(
  filename: string,
  options: SchemaGateOptions,
): Effect.Effect<SchemaGateLease, SchemaGateTimeoutError, Scope.Scope> {
  const ttlMs = options.ttlMs ?? SCHEMA_GATE_TTL_MS
  const pollMs = options.pollMs ?? SCHEMA_GATE_POLL_MS
  const deadline = Date.now() + (options.timeoutMs ?? SCHEMA_GATE_TIMEOUT_MS)

  return Effect.gen(function* () {
    const token = newGateToken(ttlMs)
    // Ownership is a boolean on the handle's lifetime, so a timeout or an
    // interruption during polling closes the sidecar connection and a success
    // transfers release responsibility to the scope. Keeping the polling loop
    // outside `Effect.acquireRelease`'s uninterruptible acquire is what makes
    // the interruption observable at all.
    let ownsLease = false
    // One release path: the lease's explicit release and the scope finalizer
    // share it, so a double call cannot emit a spurious release warning.
    const releaseOwnership = (opened: Connection): void => {
      if (!ownsLease) return
      ownsLease = false
      releaseSchemaGate(opened, token)
    }
    const connection = yield* Effect.acquireRelease(
      Effect.sync(() => openSchemaGate(filename)),
      (opened) =>
        Effect.sync(() => {
          releaseOwnership(opened)
          opened.close()
        }),
    )
    options.onLeaseOpened?.(connection)
    while (true) {
      const acquired = yield* Effect.sync(() => {
        const won = acquireSchemaGate(connection, token)
        if (won) ownsLease = true
        return won
      })
      if (acquired) return makeLease(connection, token, () => releaseOwnership(connection))
      options.onWaitForLease?.()
      if (Date.now() > deadline) return yield* Effect.fail(new SchemaGateTimeoutError())
      yield* Effect.sleep(pollMs)
    }
  })
}

/**
 * Run `work` while holding the gate. The lease is renewed by a scoped fiber for
 * exactly the duration of `work`; the release finalizer runs on success, failure,
 * and interruption. An in-memory database has no cross-process peer and skips
 * the gate entirely.
 */
export function withSchemaGate<A, E, R>(
  filename: string,
  work: () => Effect.Effect<A, E, R>,
  options: SchemaGateOptions = {},
): Effect.Effect<A, E | SchemaGateTimeoutError | SchemaGateLostError, R> {
  if (isMemoryDatabase(filename)) return work()

  return Effect.scoped(
    Effect.gen(function* () {
      const lease = yield* acquireLease(filename, options)
      // Loss is authoritative: the renewal fiber fails the deferred, and racing
      // it against `work` interrupts the migration instead of letting it write
      // on a lease another process now owns. The scoped release finalizer still
      // runs on either branch.
      const lost = yield* Deferred.make<never, SchemaGateLostError>()
      yield* lease.renew().pipe(
        Effect.repeat(Schedule.spaced(options.renewIntervalMs ?? SCHEMA_GATE_RENEW_MS)),
        Effect.catch((error) => Deferred.fail(lost, error)),
        Effect.forkScoped,
      )
      return yield* Effect.raceFirst(work(), Deferred.await(lost))
    }),
  )
}
