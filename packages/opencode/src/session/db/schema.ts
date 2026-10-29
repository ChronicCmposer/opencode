// Schema gate (D7).
//
// Identity is `PRAGMA user_version` plus the ordered migration journal ids plus
// the hash of the shipped SQL bodies (persisted in `schema_meta`). Only the hub
// migrates, and only with zero live workers; it applies every statement, writes
// the body hash, verifies the journal, and marks `user_version` inside ONE
// `BEGIN IMMEDIATE` transaction. A worker probes read-only and refuses a mismatch
// with `SchemaMismatch`: it never runs DDL and never marks schema state.

import { Effect, Schema, Stream } from "effect"
import { Reactivity } from "effect/unstable/reactivity"
import { make, SqlClient } from "effect/unstable/sql/SqlClient"
import type { Connection as SqlConnection } from "effect/unstable/sql/SqlConnection"
import { classifySqliteError, SqlError } from "effect/unstable/sql/SqlError"
import { Statement } from "effect/unstable/sql"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import path from "node:path"
import { DatabaseMigration } from "@opencode-ai/core/database/migration"
import { openConnection, type Connection, type ConnectionOptions } from "@opencode-ai/core/database/connection"
import { isBusyError, inImmediateTransaction, retryBusySync } from "@opencode-ai/core/database/transaction"
import { transactionControl } from "@opencode-ai/core/database/sqlite-adapter"
import {
  SCHEMA_GATE_RENEW_MS,
  SchemaGateTimeoutError,
  newGateToken,
  openSchemaGate,
  releaseSchemaGate,
  renewSchemaGate,
  schemaGateFilename,
  startSchemaGateRenewal,
  waitForSchemaGate,
  waitForSchemaGateAsync,
  type SchemaGateRenewal,
} from "@opencode-ai/core/database/schema-gate"
import { ledgerStatements, migrationStatements } from "@/session/writer/migration.statements.gen"
import { toolCallUpgradeStatements, toolCallUpgradeStatementsAsync } from "@/session/ledger/tool-call"
import { makePromiseExecutor, type DbExecutor } from "@/session/worker/db-executor"
import { SCHEMA_BODY_HASH, SCHEMA_MIGRATION_IDS, SCHEMA_VERSION } from "./allowlist.gen"
import { hashIdentity, readPersistedBodyHash, writePersistedBodyHash } from "./identity"
import { assertWorkerTableParity, type SqliteQuery } from "./ownership"

export interface SchemaIdentity {
  readonly version: number
  readonly contentHash: string
}

interface MigrationRow {
  readonly id: string
}

export class SchemaMismatch extends Schema.TaggedErrorClass<SchemaMismatch>("SchemaMismatch")(
  "WorkerDb.SchemaMismatch",
  {
    expected: Schema.Struct({ version: Schema.Number, contentHash: Schema.String }),
    observed: Schema.Struct({ version: Schema.Number, contentHash: Schema.String }),
  },
) {
  readonly tag = "WorkerDb.SchemaMismatch"

  constructor(expected: SchemaIdentity, observed: SchemaIdentity) {
    super({ expected, observed })
    this.name = "SchemaMismatch"
  }

  override get message() {
    return (
      `schema mismatch: expected version ${this.expected.version}/${this.expected.contentHash}, ` +
      `observed ${this.observed.version}/${this.observed.contentHash}`
    )
  }
}

/**
 * The migration path opens one `BEGIN IMMEDIATE` batch and must fail the whole
 * batch on a busy statement, never replay a single DML statement inside it. A
 * custom executor without `queryInTransaction` cannot honour that (F1), so the
 * path fails fast instead of silently falling back to retrying `query`.
 */
export class MissingQueryInTransactionError extends Schema.TaggedErrorClass<MissingQueryInTransactionError>(
  "MissingQueryInTransactionError",
)("WorkerDb.MissingQueryInTransaction", {}) {
  readonly tag = "WorkerDb.MissingQueryInTransaction"

  constructor() {
    super({})
    this.name = "MissingQueryInTransactionError"
  }

  override get message() {
    return "schema migration requires an executor with queryInTransaction; a busy in-transaction statement must fail the batch, not replay DML"
  }
}

export class LiveWorkersError extends Schema.TaggedErrorClass<LiveWorkersError>("LiveWorkersError")(
  "WorkerDb.LiveWorkers",
  { liveWorkers: Schema.Number },
) {
  readonly tag = "WorkerDb.LiveWorkers"

  constructor(liveWorkers: number) {
    super({ liveWorkers })
    this.name = "LiveWorkersError"
  }

  override get message() {
    return `schema migration requires zero live workers, found ${this.liveWorkers}`
  }
}

/** Stable identity over version, SQL bodies, and the journal id set. */
export function migrationContentHash(input: {
  readonly version: number
  readonly ids: readonly string[]
  readonly bodyHash: string
}): string {
  return hashIdentity(input)
}

function hasMigrationTable(connection: Connection): boolean {
  return connection.all("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'migration'").length > 0
}

function readMigrationIDs(connection: Connection): readonly string[] {
  return hasMigrationTable(connection)
    ? connection.all<MigrationRow>("SELECT id FROM migration ORDER BY id").map((row) => row.id)
    : []
}

/** Identity this build expects after a complete hub migration. */
export function currentSchemaIdentity(): SchemaIdentity {
  return {
    version: SCHEMA_VERSION,
    contentHash: migrationContentHash({
      version: SCHEMA_VERSION,
      ids: SCHEMA_MIGRATION_IDS,
      bodyHash: SCHEMA_BODY_HASH,
    }),
  }
}

/** Read-only probe: the observed version, journal hash, and persisted body hash. */
export function schemaIdentity(connection: Connection): SchemaIdentity {
  const version = Number(connection.pragma("user_version"))
  return {
    version,
    contentHash: migrationContentHash({
      version,
      ids: readMigrationIDs(connection),
      bodyHash: readPersistedBodyHash(connection),
    }),
  }
}

export function sameSchemaIdentity(left: SchemaIdentity, right: SchemaIdentity): boolean {
  return left.version === right.version && left.contentHash === right.contentHash
}

/** Worker gate: refuse anything that is not exactly this build's schema. */
export function assertSchemaIdentity(connection: Connection): SchemaIdentity {
  const observed = schemaIdentity(connection)
  const expected = currentSchemaIdentity()
  if (!sameSchemaIdentity(expected, observed)) throw new SchemaMismatch(expected, observed)
  return observed
}

/**
 * Hub-only marker. It persists the body hash, verifies the migration journal,
 * then writes `PRAGMA user_version`, so an incomplete migration cannot be marked
 * complete. The caller owns the transaction when atomicity with the DDL matters.
 */
export function markSchemaIdentity(connection: Connection): SchemaIdentity {
  const expected = currentSchemaIdentity()
  writePersistedBodyHash(connection, SCHEMA_BODY_HASH)
  // Verify the journal and body at the *expected* version, then write the
  // version: the marker is what makes the real version match, so including the
  // old version in this comparison would refuse every fresh migration.
  const content = {
    version: expected.version,
    contentHash: migrationContentHash({
      version: expected.version,
      ids: readMigrationIDs(connection),
      bodyHash: readPersistedBodyHash(connection),
    }),
  }
  if (content.contentHash !== expected.contentHash) {
    throw new SchemaMismatch(expected, schemaIdentity(connection))
  }
  connection.exec(`PRAGMA user_version = ${expected.version}`)
  return expected
}

export interface MigrateOptions {
  readonly connection: Connection
  /** The hub must have drained/stopped every worker before migrating. */
  readonly liveWorkers: number
  readonly statements?: ReadonlyArray<string>
}

/**
 * Apply the shipped statements on the hub connection, then mark the schema. The
 * zero-live-worker guard runs before any statement, and the whole read → DDL →
 * verify → mark sequence is one `BEGIN IMMEDIATE` transaction: a crash between
 * the DDL and the marker rolls both back, so the next boot retries consistently.
 */
export function migrate(options: MigrateOptions): SchemaIdentity {
  if (options.liveWorkers !== 0) throw new LiveWorkersError(options.liveWorkers)
  const statements = options.statements ?? migrationStatements
  inImmediateTransaction(options.connection, (tx) => {
    for (const statement of statements) tx.exec(statement)
    // `CREATE TABLE IF NOT EXISTS` cannot add the fence column to a table an
    // earlier build created; upgrade that table before the schema is marked.
    for (const statement of toolCallUpgradeStatements(tx)) tx.exec(statement)
    markSchemaIdentity(tx)
  })
  return currentSchemaIdentity()
}

export interface EnsureSchemaOptions {
  readonly filename: string
  /** The hub must have zero live workers while it verifies and migrates. */
  readonly liveWorkers: number
  /** Test seam: fires when a migration batch is busy and will be replayed. */
  readonly onMigrationBusy?: () => void
}

/**
 * Databases this process has already ensured. Core's migration path and the
 * opencode ledger are process-wide one-time work; repeating them on every hub
 * construction re-opens the file and replays the journal. Keyed by the resolved
 * filename so a relative/absolute alias of the same file shares one entry.
 *
 * Deliberately not persisted: a new process must re-probe and apply anything a
 * previous process did not finish.
 */
const ensuredDatabases = new Map<string, SchemaIdentity>()

/** Test seam: forget process-level ensure results (each test owns fresh files). */
export function resetSchemaEnsureCache(): void {
  ensuredDatabases.clear()
}

/** Open an existing database read/write without creating one; absent/failed probes are not an error. */
function openExistingConnection(filename: string): Connection | undefined {
  try {
    return openConnection({ filename, create: false })
  } catch {
    return undefined
  }
}

// --- Cross-process schema gate ---------------------------------------------
//
// The lease lives in core (`@opencode-ai/core/database/schema-gate`) so core's
// `Database` layer and this hub-side ensure share one acquisition, renewal, and
// release implementation. `schemaGateFilename` is re-exported for callers that
// asserted on the sidecar path before the move.

export { SchemaGateTimeoutError, schemaGateFilename }

/**
 * Hub boot schema-ensure. A fresh app-migrated database is upgraded with the
 * shipped statements (the opencode-owned ledger and the identity marker) and
 * marked. An already-current database is left untouched. The zero-live-worker
 * guard and `BEGIN IMMEDIATE` make the check and the write one atomic step, so
 * two concurrent hubs cannot interleave read/verify/mark. Once a file is
 * ensured in this process, later hubs reuse that result instead of re-running
 * core's migration journal and the ledger.
 */
export function ensureProductionSchema(options: EnsureSchemaOptions): SchemaIdentity {
  // A private in-memory database cannot share core's migration connection, so it
  // uses the self-contained statement list on the one handle opened below. A real
  // file is upgraded incrementally through the normal `DatabaseMigration.apply`
  // path; replaying the final schema there would stamp every migration id with
  // `INSERT OR IGNORE` and skip real data migrations.
  const inMemory = options.filename === ":memory:"
  const expected = currentSchemaIdentity()
  const cacheKey = inMemory ? undefined : path.resolve(options.filename)
  if (cacheKey !== undefined) {
    const ensured = ensuredDatabases.get(cacheKey)
    if (ensured !== undefined) return ensured
    // An already-current file needs no core migration at all. Probing first also
    // keeps a second hub in this process from re-opening and replaying the
    // journal; only a genuinely legacy/missing file takes the migration path.
    const existing = openExistingConnection(options.filename)
    if (existing !== undefined) {
      try {
        if (sameSchemaIdentity(expected, schemaIdentity(existing))) {
          assertWorkerTableParity(existing)
          ensuredDatabases.set(cacheKey, expected)
          return expected
        }
      } finally {
        existing.close()
      }
    }
  }
  // The zero-live-worker guard protects DDL/migration only. A cached or already
  // current database is probe-and-return above, so a second hub sharing an
  // ensured file does not fail just because workers are now live.
  if (options.liveWorkers !== 0) throw new LiveWorkersError(options.liveWorkers)
  const connection = openConnection({ filename: options.filename })
  const gate = inMemory ? undefined : openSchemaGate(options.filename)
  const token = newGateToken()
  try {
    if (inMemory) {
      inImmediateTransaction(connection, (tx) => {
        for (const statement of migrationStatements) tx.exec(statement)
        markSchemaIdentity(tx)
        assertWorkerTableParity(tx)
      })
      return currentSchemaIdentity()
    }
    // Two processes now serialize on the database row, not on a process-local
    // cache. Core's incremental journal, the ledger, and the marker all run
    // while the gate is held.
    waitForSchemaGate(gate!, token)
    try {
      if (sameSchemaIdentity(expected, schemaIdentity(connection))) {
        assertWorkerTableParity(connection)
        if (cacheKey !== undefined) ensuredDatabases.set(cacheKey, expected)
        return expected
      }
      applyCoreMigrations(options.filename, options.onMigrationBusy)
      // Ownership is authoritative through the whole sequence: re-prove it
      // immediately before the ledger/marker commit so a lease stolen during
      // core's migration cannot be used to write the final marker.
      renewSchemaGate(gate!, token)
      inImmediateTransaction(connection, (tx) => {
        // On a file, only the opencode-owned ledger and marker are ours; core
        // already applied its schema.
        for (const statement of ledgerStatements) tx.exec(statement)
        // A P0 database already has `tool_call`; the create above is a no-op
        // there, so add the additive fence column in place.
        for (const statement of toolCallUpgradeStatements(tx)) tx.exec(statement)
        markSchemaIdentity(tx)
        assertWorkerTableParity(tx)
      })
      const identity = currentSchemaIdentity()
      if (cacheKey !== undefined) ensuredDatabases.set(cacheKey, identity)
      return identity
    } finally {
      releaseSchemaGate(gate!, token)
    }
  } finally {
    gate?.close()
    connection.close()
  }
}

/**
 * Off-loop variant of {@link ensureProductionSchema} for the hub's construction
 * path. Every probe, DDL statement, ledger write, and marker runs on the supplied
 * `DbExecutor` thread, so a slow or busy file never blocks the hub's main loop.
 *
 * Semantics match the synchronous gate: an already-current file is probe-and-
 * return (no live-worker refusal); a file that needs migration refuses a
 * non-zero live-worker count, holds the same cross-process gate, runs core's
 * incremental Effect journal through the executor connection, then applies the
 * ledger and marker in one `BEGIN IMMEDIATE` and verifies the same identity and
 * worker table parity. No migration SQL runs on the hub's main thread.
 */
export interface EnsureSchemaAsyncOptions extends EnsureSchemaOptions {
  /** Reuse a hub executor; one is created and closed when omitted. */
  readonly executor?: DbExecutor
  /** Renewal cadence; tests shorten it to observe a lost lease promptly. */
  readonly renewIntervalMs?: number
  /** Test seam: fires when the gate is held by a peer and this call waits. */
  readonly onGateWait?: () => void
}

async function readIdentityAsync(executor: DbExecutor): Promise<SchemaIdentity> {
  const version = Number(((await executor.query("PRAGMA user_version"))[0]?.user_version as number | undefined) ?? 0)
  const hasMigration =
    (await executor.query("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'migration'")).length > 0
  const ids = hasMigration
    ? (await executor.query("SELECT id FROM migration ORDER BY id")).map((row) => String(row.id))
    : []
  let bodyHash = ""
  try {
    const rows = await executor.query("SELECT content_hash FROM schema_meta WHERE id = 'schema'")
    bodyHash = String(rows[0]?.content_hash ?? "")
  } catch {
    bodyHash = ""
  }
  return { version, contentHash: migrationContentHash({ version, ids, bodyHash }) }
}

/**
 * `assertWorkerTableParity` over the executor. The schema walk is done with
 * awaited queries and replayed through the shared derivation, so the async gate
 * cannot drift from the synchronous one.
 */
async function assertWorkerTableParityAsync(executor: DbExecutor): Promise<void> {
  const tables = await executor.query(
    "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
  )
  const foreignKeys = new Map<string, ReadonlyArray<Record<string, unknown>>>()
  for (const row of tables) {
    const table = String(row.name)
    foreignKeys.set(table, await executor.query(`PRAGMA foreign_key_list("${table.replaceAll('"', '""')}")`))
  }
  const adapter: SqliteQuery = {
    all: <T = Record<string, unknown>>(sql: string): ReadonlyArray<T> => {
      if (sql.startsWith("SELECT name FROM sqlite_master")) return tables as ReadonlyArray<T>
      const match = /PRAGMA foreign_key_list\("(.+)"\)/.exec(sql)
      if (match === null) return []
      return (foreignKeys.get(match[1]!.replaceAll('""', '"')) ?? []) as ReadonlyArray<T>
    },
  }
  assertWorkerTableParity(adapter)
}

export async function ensureProductionSchemaAsync(options: EnsureSchemaAsyncOptions): Promise<SchemaIdentity> {
  const inMemory = options.filename === ":memory:"
  const expected = currentSchemaIdentity()
  const cacheKey = inMemory ? undefined : path.resolve(options.filename)
  if (cacheKey !== undefined) {
    const ensured = ensuredDatabases.get(cacheKey)
    if (ensured !== undefined) return ensured
  }

  // A live-worker refusal must not spawn an executor thread: the common case is a
  // second hub sharing an already-provisioned file, and a spawn would itself be
  // work the gate is meant to refuse. Probe the existing file synchronously (a
  // read-only identity check, exactly like the sync gate's cache probe) and only
  // migrate when zero workers are live.
  if (options.executor === undefined && options.liveWorkers !== 0) {
    const existing = inMemory ? undefined : openExistingConnection(options.filename)
    if (existing !== undefined) {
      try {
        const observed = schemaIdentity(existing)
        if (sameSchemaIdentity(expected, observed)) {
          assertWorkerTableParity(existing)
          if (cacheKey !== undefined) ensuredDatabases.set(cacheKey, expected)
          return expected
        }
      } finally {
        existing.close()
      }
    }
    throw new LiveWorkersError(options.liveWorkers)
  }

  const openExecutor = (): { readonly executor: DbExecutor; readonly owned: boolean } => {
    if (options.executor !== undefined) return { executor: options.executor, owned: false }
    // TEMPORARY EDGE: this async gate is not Effect yet, so it acquires the
    // DbExecutor service through the Promise facade and closes its scope in the
    // gate's `finally`. The service remains the single implementation.
    return {
      owned: true,
      executor: makePromiseExecutor({
        filename: options.filename,
        ...(inMemory ? { pragmas: [] } : {}),
        name: "hub-schema-ensure",
      }),
    }
  }

  if (inMemory) {
    const { executor, owned } = openExecutor()
    try {
      const observed = await readIdentityAsync(executor)
      if (sameSchemaIdentity(expected, observed)) {
        await assertWorkerTableParityAsync(executor)
        if (cacheKey !== undefined) ensuredDatabases.set(cacheKey, expected)
        return expected
      }
      // The in-memory schema is self-contained: one transaction builds it, marks
      // it, and advances the version.
      await executor.transaction([
        ...migrationStatements.map((sql) => ({ sql })),
        {
          sql: `INSERT OR REPLACE INTO schema_meta (id, content_hash) VALUES ('schema', '${SCHEMA_BODY_HASH.replaceAll("'", "''")}')`,
        },
        { sql: `PRAGMA user_version = ${expected.version}` },
      ])
      return await verifyIdentityAsync(executor, expected, cacheKey)
    } finally {
      if (owned) await executor.close()
    }
  }

  // Take the cross-process gate BEFORE opening the executor. A peer is then not
  // switching WAL/pragma on the same file while this handle opens, which is the
  // open-time busy window that used to flake when two processes booted together.
  const gate = openSchemaGate(options.filename)
  const token = newGateToken()
  let executor: DbExecutor | undefined
  let ownsExecutor = false
  try {
    await waitForSchemaGateAsync(gate, token, undefined, options.onGateWait)
    // Everything after acquisition is inside the finally that releases the
    // lease: a throw opening the executor or starting renewal must not leave the
    // row live for the full TTL. Renewal starts last, once setup can no longer
    // throw, and the executor is closed here (after the migration settles) so a
    // lost lease still closes it before the gate is released.
    try {
      const opened = openExecutor()
      executor = opened.executor
      ownsExecutor = opened.owned
      const renewal = startSchemaGateRenewal(gate, token, options.renewIntervalMs ?? SCHEMA_GATE_RENEW_MS)
      try {
        return await raceLeaseLoss(renewal, migrateUnderGate(options, executor, expected, cacheKey, gate, token))
      } finally {
        renewal.stop()
      }
    } finally {
      releaseSchemaGate(gate, token)
      if (ownsExecutor) await executor?.close()
    }
  } finally {
    gate.close()
  }
}

/**
 * The migration body while the gate is held. Every phase is preceded by an
 * ownership re-proof so a lease lost mid-migration fails here instead of writing
 * the ledger/marker on a lock another process now owns.
 *
 * Lease-loss guarantee (deliberately not "the core batch is aborted mid-flight"):
 * a loss is observed after the core batch and again immediately before the one
 * commit that writes the ledger/marker, so that commit never happens on a stolen
 * lease. The core batch itself is allowed to finish before the executor closes;
 * it is serialized by `BEGIN IMMEDIATE` and re-reads the journal in the same
 * transaction (`applyBatched`), so a peer cannot interleave with it.
 */
async function migrateUnderGate(
  options: EnsureSchemaAsyncOptions,
  executor: DbExecutor,
  expected: SchemaIdentity,
  cacheKey: string | undefined,
  gate: Connection,
  token: ReturnType<typeof newGateToken>,
): Promise<SchemaIdentity> {
  // Re-probe under the gate: a peer may have completed the migration while
  // this process was waiting.
  const observed = await readIdentityAsync(executor)
  if (sameSchemaIdentity(expected, observed)) {
    await assertWorkerTableParityAsync(executor)
    if (cacheKey !== undefined) ensuredDatabases.set(cacheKey, expected)
    return expected
  }

  if (options.liveWorkers !== 0) throw new LiveWorkersError(options.liveWorkers)

  renewSchemaGate(gate, token)
  // Core's incremental Effect migrations run through the executor-backed
  // client as one `BEGIN IMMEDIATE` batch, so no statement is ever replayed
  // separately and a legacy file is only marked after core has caught up.
  await applyCoreMigrationsOnExecutor(executor, options.onMigrationBusy)
  renewSchemaGate(gate, token)
  const journal = (await executor.query("SELECT id FROM migration ORDER BY id")).map((row) => String(row.id))
  const expectedIds = new Set(SCHEMA_MIGRATION_IDS)
  const unknown = journal.filter((id) => !expectedIds.has(id))
  if (unknown.length > 0) {
    const content = migrationContentHash({ version: expected.version, ids: journal, bodyHash: SCHEMA_BODY_HASH })
    throw new SchemaMismatch(expected, { version: expected.version, contentHash: content })
  }
  // The minimum guarantee: prove the lease immediately before the one commit
  // that writes the ledger and the version marker.
  renewSchemaGate(gate, token)
  await executor.transaction([
    ...ledgerStatements.map((sql) => ({ sql })),
    // Probed under the schema gate, so the ALTER is applied at most once
    // even when two hubs boot the same legacy file.
    ...(await toolCallUpgradeStatementsAsync(executor)).map((sql) => ({ sql })),
    {
      sql: `INSERT OR REPLACE INTO schema_meta (id, content_hash) VALUES ('schema', '${SCHEMA_BODY_HASH.replaceAll("'", "''")}')`,
    },
    { sql: `PRAGMA user_version = ${expected.version}` },
  ])
  return await verifyIdentityAsync(executor, expected, cacheKey)
}

/**
 * Resolve when either the migration settles or the lease is lost. A loss fails
 * the ensure, but the already-started body is awaited first so its executor is
 * closed before the gate is released. The body observes the loss at its next
 * ownership re-proof, which is always before the ledger/marker commit; the core
 * batch is not interrupted mid-flight (see {@link migrateUnderGate}).
 */
async function raceLeaseLoss<T>(renewal: SchemaGateRenewal, work: Promise<T>): Promise<T> {
  try {
    return await Promise.race([work, renewal.lost])
  } catch (error) {
    await work.catch(() => {})
    throw error
  }
}

/** Read the committed identity, enforce parity, and only then cache success. */
async function verifyIdentityAsync(
  executor: DbExecutor,
  expected: SchemaIdentity,
  cacheKey: string | undefined,
): Promise<SchemaIdentity> {
  const identity = await readIdentityAsync(executor)
  if (!sameSchemaIdentity(expected, identity)) throw new SchemaMismatch(expected, identity)
  await assertWorkerTableParityAsync(executor)
  if (cacheKey !== undefined) ensuredDatabases.set(cacheKey, expected)
  return expected
}

/**
 * Run core's incremental journal on `filename` on the calling thread. The
 * synchronous helper cannot suspend, so it never uses the adapter's async
 * `Effect.sleep` backoff: transaction control is retried at its own boundary and
 * the whole batch is replayed with a blocking bounded retry. The batch is one
 * `BEGIN IMMEDIATE`, so a busy failure rolled it back and replay is safe.
 */
function applyCoreMigrations(filename: string, onBusy?: () => void): void {
  const connection = openConnection({ filename })
  try {
    const client = Effect.runSync(
      make({
        acquirer: Effect.succeed(synchronousConnection(connection, onBusy)),
        compiler: Statement.makeCompilerSqlite(),
        spanAttributes: [["db.system.name", "sqlite"]],
      }).pipe(Effect.provide(Reactivity.layer)),
    )
    const db = Effect.runSync(
      EffectDrizzleSqlite.makeWithDefaults().pipe(Effect.provideService(SqlClient, client)),
    )
    retryBusySync(() => Effect.runSync(DatabaseMigration.applyBatched(db)), CORE_MIGRATION_RETRY)
  } finally {
    connection.close()
  }
}

/**
 * The synchronous twin of {@link executorConnection}: every statement runs on
 * the calling thread. Transaction-control statements are retried in place (a
 * busy BEGIN did not begin, a busy COMMIT did not commit); ordinary DML is not.
 */
function synchronousConnection(connection: Connection, onBusy?: () => void): SqlConnection {
  const executeRaw = (sql: string, params: ReadonlyArray<unknown>) =>
    Effect.try({
      try: () => {
        const control = transactionControl(sql)
        const retryAtBoundary = control === "begin" || control === "commit" || control === "rollback"
        if (!retryAtBoundary) return connection.all(sql, params)
        return retryBusySync(() => {
          try {
            return connection.all(sql, params)
          } catch (error) {
            // Publish the observed busy before the in-place retry so a test can
            // release the contended lock deterministically (no fixed hold).
            if (isBusyError(error)) onBusy?.()
            throw error
          }
        }, CORE_MIGRATION_RETRY)
      },
      catch: executorSqlError,
    })
  const rows = (sql: string, params: ReadonlyArray<unknown>) =>
    Effect.map(executeRaw(sql, params), (result) => result as ReadonlyArray<Record<string, unknown>>)
  const execute = (
    sql: string,
    params: ReadonlyArray<unknown>,
    transformRows: (<A extends object>(row: ReadonlyArray<A>) => ReadonlyArray<A>) | undefined,
  ) =>
    transformRows === undefined
      ? rows(sql, params)
      : Effect.map(rows(sql, params), (result) => transformRows(result as ReadonlyArray<Record<string, unknown>>))
  return {
    execute,
    executeRaw: executeRaw as SqlConnection["executeRaw"],
    executeUnprepared: execute,
    executeValues: (sql, params) =>
      Effect.map(executeRaw(sql, params), (result) => result.map((row) => Object.values(row))),
    executeStream: () => Stream.die("schema migration does not stream"),
  }
}

function executorSqlError(cause: unknown): SqlError {
  return new SqlError({
    reason: classifySqliteError(cause, { message: "Failed to execute schema statement", operation: "execute" }),
  })
}

/**
 * An Effect SQL connection whose statements are forwarded to the hub executor
 * thread. Core's Effect migration bodies are tiny; the SQL they issue (including
 * their transactions) runs off the hub loop on the executor's handle.
 */
function executorConnection(executor: DbExecutor): SqlConnection {
  const queryInTransaction = executor.queryInTransaction
  if (queryInTransaction === undefined) throw new MissingQueryInTransactionError()
  // Inside the migration transaction a busy statement must fail the whole batch
  // (which is then replayed atomically), never be retried on its own.
  const query = (sql: string, params: ReadonlyArray<unknown>) =>
    Effect.tryPromise({
      try: () => queryInTransaction.call(executor, sql, params),
      catch: executorSqlError,
    })
  const rows = (sql: string, params: ReadonlyArray<unknown>) =>
    Effect.map(query(sql, params), (result) => result as ReadonlyArray<Record<string, unknown>>)
  const execute = (
    sql: string,
    params: ReadonlyArray<unknown>,
    transformRows: (<A extends object>(row: ReadonlyArray<A>) => ReadonlyArray<A>) | undefined,
  ) =>
    transformRows === undefined
      ? rows(sql, params)
      : Effect.map(rows(sql, params), (result) => transformRows(result as ReadonlyArray<Record<string, unknown>>))
  return {
    execute,
    executeRaw: query,
    executeUnprepared: execute,
    executeValues: (sql, params) => Effect.map(query(sql, params), (result) => result.map((row) => Object.values(row))),
    executeStream: () => Stream.die("schema migration does not stream"),
  }
}

const CORE_MIGRATION_RETRY = { retries: 4, baseDelayMs: 25, maxDelayMs: 500 }

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms)
    timer.unref?.()
  })
}

/**
 * Run core's incremental journal off-loop over the executor connection as ONE
 * `BEGIN IMMEDIATE` transaction (`applyBatched`): read the journal, apply every
 * pending migration, and record it in the same batch. A busy failure rolled the
 * batch back, so replaying the whole batch is safe and no single statement is
 * ever retried inside an open transaction.
 */
async function applyCoreMigrationsOnExecutor(executor: DbExecutor, onBusy?: () => void): Promise<void> {
  const client = await Effect.runPromise(
    make({
      acquirer: Effect.succeed(executorConnection(executor)),
      compiler: Statement.makeCompilerSqlite(),
      spanAttributes: [["db.system.name", "sqlite"]],
    }).pipe(Effect.provide(Reactivity.layer)),
  )
  const db = await Effect.runPromise(
    EffectDrizzleSqlite.makeWithDefaults().pipe(Effect.provideService(SqlClient, client)),
  )
  for (let attempt = 0; ; attempt += 1) {
    try {
      await Effect.runPromise(DatabaseMigration.applyBatched(db))
      return
    } catch (error) {
      if (!isBusyError(error) || attempt >= CORE_MIGRATION_RETRY.retries) throw error
      onBusy?.()
      await sleep(Math.min(CORE_MIGRATION_RETRY.baseDelayMs * 2 ** attempt, CORE_MIGRATION_RETRY.maxDelayMs))
    }
  }
}

export interface GatedConnection {
  readonly connection: Connection
  readonly identity: SchemaIdentity
}

/**
 * Open a per-worker WAL connection and refuse a mixed schema. The probe and the
 * refusal are read-only, and a missing database file is never created as a side
 * effect of the probe; the handle is closed before the typed error escapes.
 */
export function openGatedConnection(options: ConnectionOptions): GatedConnection {
  const connection = openConnection({ ...options, create: false })
  try {
    const identity = assertSchemaIdentity(connection)
    return { connection, identity }
  } catch (error) {
    connection.close()
    throw error
  }
}

export * as SchemaGate from "./schema"
