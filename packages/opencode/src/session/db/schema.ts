// Schema gate (D7 / M4 / M5).
//
// Identity is `PRAGMA user_version` plus the ordered migration journal ids plus
// the hash of the shipped SQL bodies (persisted in `schema_meta`). Only the hub
// migrates, and only with zero live workers; it applies every statement, writes
// the body hash, verifies the journal, and marks `user_version` inside ONE
// `BEGIN IMMEDIATE` transaction. A worker probes read-only and refuses a mismatch
// with `SchemaMismatch`: it never runs DDL and never marks schema state.

import { openConnection, type Connection, type ConnectionOptions } from "@opencode-ai/core/database/connection"
import { inImmediateTransaction } from "@opencode-ai/core/database/transaction"
import { migrationStatements } from "@/session/writer/migration.statements.gen"
import {
  SCHEMA_BODY_HASH,
  SCHEMA_MIGRATION_IDS,
  SCHEMA_VERSION,
} from "./allowlist.gen"
import { hashIdentity, readPersistedBodyHash, writePersistedBodyHash } from "./identity"
import { assertWorkerTableParity } from "./ownership"

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
 * Hub boot schema-ensure. A fresh app-migrated database is upgraded with the
 * shipped statements (the opencode-owned ledger and the identity marker) and
 * marked. An already-current database is left untouched. The zero-live-worker
 * guard and `BEGIN IMMEDIATE` make the check and the write one atomic step, so
 * two concurrent hubs cannot interleave read/verify/mark.
 */
export function ensureProductionSchema(options: EnsureSchemaOptions): SchemaIdentity {
  if (options.liveWorkers !== 0) throw new LiveWorkersError(options.liveWorkers)
  const connection = openConnection({ filename: options.filename })
  try {
    const expected = currentSchemaIdentity()
    if (sameSchemaIdentity(expected, schemaIdentity(connection))) {
      assertWorkerTableParity(connection)
      return expected
    }
    inImmediateTransaction(connection, (tx) => {
      for (const statement of migrationStatements) tx.exec(statement)
      markSchemaIdentity(tx)
      assertWorkerTableParity(tx)
    })
    return currentSchemaIdentity()
  } finally {
    connection.close()
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
