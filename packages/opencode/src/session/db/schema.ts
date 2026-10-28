// Schema gate (D7).
//
// Identity is `PRAGMA user_version` plus the ordered migration journal ids plus
// the hash of the shipped SQL bodies (persisted in `schema_meta`). Only the hub
// migrates, and only with zero live workers; it applies every statement, writes
// the body hash, verifies the journal, and marks `user_version` inside ONE
// `BEGIN IMMEDIATE` transaction. A worker probes read-only and refuses a mismatch
// with `SchemaMismatch`: it never runs DDL and never marks schema state.

import { Effect } from "effect"
import path from "node:path"
import { Database } from "@opencode-ai/core/database/database"
import { openConnection, type Connection, type ConnectionOptions } from "@opencode-ai/core/database/connection"
import { inImmediateTransaction } from "@opencode-ai/core/database/transaction"
import { ledgerStatements, migrationStatements } from "@/session/writer/migration.statements.gen"
import { createDbExecutor, type DbExecutor } from "@/session/worker/db-executor"
import {
  SCHEMA_BODY_HASH,
  SCHEMA_MIGRATION_IDS,
  SCHEMA_VERSION,
} from "./allowlist.gen"
import { hashIdentity, readPersistedBodyHash, writePersistedBodyHash } from "./identity"
import { assertWorkerTableParity, type SqliteQuery } from "./ownership"

export interface SchemaIdentity {
  readonly version: number
  readonly contentHash: string
}

interface MigrationRow {
  readonly id: string
}

export class SchemaMismatch extends Error {
  readonly tag = "WorkerDb.SchemaMismatch"
  readonly expected: SchemaIdentity
  readonly observed: SchemaIdentity

  constructor(expected: SchemaIdentity, observed: SchemaIdentity) {
    super(
      `schema mismatch: expected version ${expected.version}/${expected.contentHash}, ` +
        `observed ${observed.version}/${observed.contentHash}`,
    )
    this.name = "SchemaMismatch"
    this.expected = expected
    this.observed = observed
  }
}

export class LiveWorkersError extends Error {
  readonly tag = "WorkerDb.LiveWorkers"
  readonly liveWorkers: number

  constructor(liveWorkers: number) {
    super(`schema migration requires zero live workers, found ${liveWorkers}`)
    this.name = "LiveWorkersError"
    this.liveWorkers = liveWorkers
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
    markSchemaIdentity(tx)
  })
  return currentSchemaIdentity()
}

export interface EnsureSchemaOptions {
  readonly filename: string
  /** The hub must have zero live workers while it verifies and migrates. */
  readonly liveWorkers: number
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
  if (!inMemory) applyCoreMigrations(options.filename)
  const connection = openConnection({ filename: options.filename })
  try {
    if (sameSchemaIdentity(expected, schemaIdentity(connection))) {
      assertWorkerTableParity(connection)
      if (cacheKey !== undefined) ensuredDatabases.set(cacheKey, expected)
      return expected
    }
    inImmediateTransaction(connection, (tx) => {
      // On a file, only the opencode-owned ledger and marker are ours; core
      // already applied its schema. An in-memory DB is self-contained.
      for (const statement of inMemory ? migrationStatements : ledgerStatements) tx.exec(statement)
      markSchemaIdentity(tx)
      assertWorkerTableParity(tx)
    })
    const identity = currentSchemaIdentity()
    if (cacheKey !== undefined) ensuredDatabases.set(cacheKey, identity)
    return identity
  } finally {
    connection.close()
  }
}

/**
 * Off-loop variant of {@link ensureProductionSchema} for the hub's construction
 * path. Every probe, DDL statement, ledger write, and marker runs on the supplied
 * `DbExecutor` thread, so a slow or busy file never blocks the hub's main loop.
 *
 * Semantics match the synchronous gate: an already-current file is probe-and-
 * return (no live-worker refusal); a file that needs the opencode ledger refuses
 * a non-zero live-worker count, applies the ledger and marker in one
 * `BEGIN IMMEDIATE`, then verifies the same identity and worker table parity.
 * A genuinely legacy file still needs core's incremental Effect migrations,
 * which the hub has already run through `Database.layerFromPath` before this
 * gate; the async path applies only the opencode-owned ledger, exactly like the
 * sync path on a file.
 */
export interface EnsureSchemaAsyncOptions extends EnsureSchemaOptions {
  /** Reuse a hub executor; one is created and closed when omitted. */
  readonly executor?: DbExecutor
}

async function readIdentityAsync(executor: DbExecutor): Promise<SchemaIdentity> {
  const version = Number(((await executor.query("PRAGMA user_version"))[0]?.user_version as number | undefined) ?? 0)
  const hasMigration = (
    await executor.query("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'migration'")
  ).length > 0
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

  const owned = options.executor === undefined
  const executor =
    options.executor ??
    createDbExecutor({
      filename: options.filename,
      ...(inMemory ? { pragmas: [] } : {}),
      name: "hub-schema-ensure",
    })
  try {
    const observed = await readIdentityAsync(executor)
    if (sameSchemaIdentity(expected, observed)) {
      await assertWorkerTableParityAsync(executor)
      if (cacheKey !== undefined) ensuredDatabases.set(cacheKey, expected)
      return expected
    }

    if (options.liveWorkers !== 0) throw new LiveWorkersError(options.liveWorkers)

    if (inMemory) {
      // The in-memory schema is self-contained: one transaction builds it, marks
      // it, and advances the version. Verification happens below from the
      // committed journal.
      await executor.transaction([
        ...migrationStatements.map((sql) => ({ sql })),
        {
          sql: `INSERT OR REPLACE INTO schema_meta (id, content_hash) VALUES ('schema', '${SCHEMA_BODY_HASH.replaceAll("'", "''")}')`,
        },
        { sql: `PRAGMA user_version = ${expected.version}` },
      ])
    } else {
      // Core's incremental Effect migrations are idempotent; a current file is a
      // quick probe-and-return. A legacy file may only be marked after core has
      // caught up, and its journal may not contain an id this build does not
      // ship (that is drift, not an upgrade).
      applyCoreMigrations(options.filename)
      const journal = (await executor.query("SELECT id FROM migration ORDER BY id")).map((row) => String(row.id))
      const expectedIds = new Set(SCHEMA_MIGRATION_IDS)
      const unknown = journal.filter((id) => !expectedIds.has(id))
      if (unknown.length > 0) {
        const content = migrationContentHash({ version: expected.version, ids: journal, bodyHash: SCHEMA_BODY_HASH })
        throw new SchemaMismatch(expected, { version: expected.version, contentHash: content })
      }
      await executor.transaction([
        ...ledgerStatements.map((sql) => ({ sql })),
        {
          sql: `INSERT OR REPLACE INTO schema_meta (id, content_hash) VALUES ('schema', '${SCHEMA_BODY_HASH.replaceAll("'", "''")}')`,
        },
        { sql: `PRAGMA user_version = ${expected.version}` },
      ])
    }

    const identity = await readIdentityAsync(executor)
    if (!sameSchemaIdentity(expected, identity)) throw new SchemaMismatch(expected, identity)
    await assertWorkerTableParityAsync(executor)
    if (cacheKey !== undefined) ensuredDatabases.set(cacheKey, expected)
    return expected
  } finally {
    if (owned) await executor.close()
  }
}

/**
 * Run the normal app migration path on `filename`. `Database.layerFromPath`
 * opens its own connection, applies core's incremental journal (or the current
 * schema on a fresh file), and closes the layer scope before returning.
 */
function applyCoreMigrations(filename: string): void {
  Effect.runSync(
    Effect.scoped(
      Effect.gen(function* () {
        yield* Database.Service
      }),
    ).pipe(Effect.provide(Database.layerFromPath(filename)), Effect.orDie),
  )
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
