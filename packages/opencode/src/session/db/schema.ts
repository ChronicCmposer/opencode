// Schema gate (D7).
//
// Identity is `PRAGMA user_version` plus a hash of the migration journal. Only
// the hub migrates, and only with zero live workers; it then marks the version
// and opens the spawn gate. A worker probes read-only and refuses a mismatch
// with `SchemaMismatch`: it never runs DDL and never marks schema state.

import { createHash } from "node:crypto"
import { openConnection, type Connection, type ConnectionOptions } from "@opencode-ai/core/database/connection"
import { inImmediateTransaction } from "@opencode-ai/core/database/transaction"
import { migrationStatements } from "@/session/writer/migration.statements.gen"
import { SCHEMA_MIGRATION_IDS, SCHEMA_VERSION } from "./allowlist.gen"

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

/** Stable content hash of the migration journal (ids only; timestamps are bookkeeping). */
export function migrationContentHash(ids: readonly string[]): string {
  const ordered = [...ids].sort()
  const hash = createHash("sha256")
  for (const id of ordered) hash.update(id).update("\n")
  return hash.digest("hex")
}

/** Identity this build expects after a complete hub migration. */
export function currentSchemaIdentity(): SchemaIdentity {
  return { version: SCHEMA_VERSION, contentHash: migrationContentHash(SCHEMA_MIGRATION_IDS) }
}

function hasMigrationTable(connection: Connection): boolean {
  return (
    connection.all("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'migration'").length > 0
  )
}

/** Read-only probe: the observed version and journal hash, with no writes. */
export function schemaIdentity(connection: Connection): SchemaIdentity {
  const version = Number(connection.pragma("user_version"))
  const ids = hasMigrationTable(connection)
    ? connection.all<MigrationRow>("SELECT id FROM migration ORDER BY id").map((row) => row.id)
    : []
  return { version, contentHash: migrationContentHash(ids) }
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
 * Hub-only marker. It verifies the migration journal first, so an incomplete
 * migration cannot be marked complete, then writes `PRAGMA user_version`.
 */
export function markSchemaIdentity(connection: Connection): SchemaIdentity {
  const observed = schemaIdentity(connection)
  const expected = currentSchemaIdentity()
  if (observed.contentHash !== expected.contentHash) throw new SchemaMismatch(expected, observed)
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
 * zero-live-worker guard runs before any statement is issued.
 */
export function migrate(options: MigrateOptions): SchemaIdentity {
  if (options.liveWorkers !== 0) throw new LiveWorkersError(options.liveWorkers)
  const statements = options.statements ?? migrationStatements
  inImmediateTransaction(options.connection, (tx) => {
    for (const statement of statements) tx.exec(statement)
  })
  return markSchemaIdentity(options.connection)
}

export interface GatedConnection {
  readonly connection: Connection
  readonly identity: SchemaIdentity
}

/**
 * Open a per-worker WAL connection and refuse a mixed schema. The probe and the
 * refusal are read-only; the handle is closed before the typed error escapes.
 */
export function openGatedConnection(options: ConnectionOptions): GatedConnection {
  const connection = openConnection(options)
  try {
    const identity = assertSchemaIdentity(connection)
    return { connection, identity }
  } catch (error) {
    connection.close()
    throw error
  }
}
