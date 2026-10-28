// Per-worker SQLite connection, schema gate, and table ownership.
//
// A session worker owns exactly one WAL connection, opened through the shared
// db-core helper. There is no writer isolate and no writer RPC. The hub is the
// only process that migrates; a worker refuses a schema whose `user_version` or
// migration journal differs from the build it runs.
//
// The session boundary is structural, not textual:
//
// * every session-scoped table is shadowed by a live TEMP VIEW filtered to the
//   bound session/aggregate (`worker_scope_session()`), so unqualified reads
//   cannot observe another session and no history is copied at open;
// * every write is built by the typed repository from the generated column
//   allowlist, binds the session column/aggregate, checks FK parent ownership,
//   and executes against `main` through an explicit capability;
// * the SQLite authorizer refuses DDL/ATTACH/PRAGMA, hub-only writes, raw
//   writes to scoped tables, secret hub-table reads, and direct `main`
//   scoped-table access from the public handles. The Bus/EventV2 path receives
//   narrow operations, never a public raw handle.
//
// There is no SQL-text statement parser/rewriter in this module. Public handles
// are read-only (the writer seam may open/commit a transaction); the typed
// repository is the only write surface. The in-worker Drizzle graph keeps a
// private runner that routes its own trusted writes to `main` without exposing
// a connection.

import path from "node:path"
import { Context, Effect, Layer, Schema } from "effect"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { Database as DatabaseService } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { Bus, createDurableRepository } from "@opencode-ai/core/bus"
import { makeSqliteClient, type SqliteNativeRunner } from "@opencode-ai/core/database/sqlite-adapter"
import {
  openNodeConnection,
  type Authorizer,
  type Connection,
  type NodeConnection,
  type QueryParams,
  type WriteOutcome,
} from "@opencode-ai/core/database/connection"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import type { SQLQueryBindings } from "bun:sqlite"
import {
  SCHEMA_BODY_HASH,
  SCHEMA_MIGRATION_IDS,
  SCHEMA_TABLES,
  SCHEMA_VERSION,
  WORKER_WRITABLE_TABLES,
} from "@/session/db/allowlist.gen"
import { hashIdentity, readPersistedBodyHash } from "@/session/db/identity"
import { deriveSessionScopedTables } from "@/session/db/derive"
import { createWriteCapability } from "./repository/capability"
import { assertTableRegistry } from "./repository/registry"
import { makeRepository } from "./repository/write"
import type { WorkerRepository } from "./repository/types"
import { DURABLE_LOG_TABLES, SESSION_IMMUTABLE_COLUMNS, WORKER_READABLE_HUB_TABLES } from "./repository/policy"
import { WORKER_DB_KEY } from "./node-table"

export { SCHEMA_VERSION, WORKER_WRITABLE_TABLES, deriveSessionScopedTables }

export interface SchemaIdentity {
  readonly version: number
  readonly contentHash: string
}

const SchemaIdentityStruct = Schema.Struct({
  version: Schema.Number,
  contentHash: Schema.String,
})

/** The worker observed a schema that does not match the build it runs. */
export class SchemaMismatch extends Schema.TaggedErrorClass<SchemaMismatch>()("WorkerDb.SchemaMismatch", {
  expected: SchemaIdentityStruct,
  observed: SchemaIdentityStruct,
}) {
  override get message() {
    return `worker schema mismatch: expected version ${this.expected.version}/${this.expected.contentHash}, observed ${this.observed.version}/${this.observed.contentHash}`
  }
}

/** A worker attempted to write a hub-owned table. */
export class HubOnlyTableError extends Schema.TaggedErrorClass<HubOnlyTableError>()("WorkerDb.HubOnlyTableError", {
  table: Schema.String,
}) {
  override get message() {
    return `worker may not write hub-owned table "${this.table}"`
  }
}

/** A worker attempted DDL or a write shape it cannot execute safely. */
export class WorkerStatementRejectedError extends Schema.TaggedErrorClass<WorkerStatementRejectedError>()(
  "WorkerDb.WorkerStatementRejectedError",
  {
    reason: Schema.String,
  },
) {
  override get message() {
    return `worker statement rejected: ${this.reason}`
  }
}

/** The shipped schema no longer matches the worker's generated table allowlist. */
export class WorkerTableOwnershipError extends Schema.TaggedErrorClass<WorkerTableOwnershipError>()(
  "WorkerDb.WorkerTableOwnershipError",
  {
    observed: Schema.Array(Schema.String),
    expected: Schema.Array(Schema.String),
  },
) {
  override get message() {
    return `session_id tables changed: expected ${this.expected.join(", ")}, observed ${this.observed.join(", ")}`
  }
}

/**
 * A worker statement referenced (or mutated) another session's rows. The worker
 * is bound to exactly one session, so cross-session access is refused before it
 * can observe another session.
 */
export class CrossSessionAccessError extends Schema.TaggedErrorClass<CrossSessionAccessError>()(
  "WorkerDb.CrossSessionAccessError",
  {
    table: Schema.String,
    sessionID: Schema.String,
  },
) {
  override get message() {
    return `worker for session "${this.sessionID}" may not access "${this.table}" outside its scope`
  }
}

/** A worker connection could not be opened. */
export class WorkerDatabaseOpenError extends Schema.TaggedErrorClass<WorkerDatabaseOpenError>()(
  "WorkerDb.WorkerDatabaseOpenError",
  {
    reason: Schema.String,
  },
) {
  override get message() {
    return `worker database could not be opened: ${this.reason}`
  }
}

export interface WorkerStatement<Row = Record<string, unknown>> {
  all(...params: SQLQueryBindings[]): ReadonlyArray<Row>
  values(...params: SQLQueryBindings[]): ReadonlyArray<ReadonlyArray<unknown>>
  get(...params: SQLQueryBindings[]): Row | null
  run(...params: SQLQueryBindings[]): WriteOutcome
}

/** The synchronous surface the per-worker Bus also consumes. */
export interface WorkerConnection {
  readonly owner: string
  readonly filename: string
  readonly closed: boolean
  query<Row = Record<string, unknown>>(sql: string): WorkerStatement<Row>
  all<Row = Record<string, unknown>>(sql: string, params?: QueryParams): ReadonlyArray<Row>
  values<Row = ReadonlyArray<unknown>>(sql: string, params?: QueryParams): ReadonlyArray<Row>
  run(sql: string, params?: QueryParams): WriteOutcome
  exec(sql: string): void
  pragma(name: string): unknown
  journalMode(): string
  close(): void
}

export type WorkerDatabaseError =
  | SchemaMismatch
  | HubOnlyTableError
  | WorkerStatementRejectedError
  | WorkerTableOwnershipError
  | CrossSessionAccessError
  | WorkerDatabaseOpenError

/** A table the worker repository may write (`session` plus the derived set). */
export type WorkerWritableTable = "session" | (typeof WORKER_WRITABLE_TABLES)[number]

export type { RepositoryConflict, RepositoryInsert, WorkerRepository } from "./repository/types"
export {
  ForeignKeyOwnershipError,
  HubOnlyWriteError,
  ImmutableColumnError,
  ScopeViolationError,
  TableRegistryDriftError,
  UnknownColumnError,
  UnsupportedWriteError,
} from "./repository/errors"
export { TABLE_REGISTRY, assertTableRegistry, tableRegistry } from "./repository/registry"
export { TABLE_SCHEMA } from "./repository/tables.gen"

export interface WorkerBusCreateOptions {
  readonly ownerID: string
  readonly aggregateID?: string
  readonly definitions?: Bus.Options["definitions"]
  readonly blobDir?: string
  readonly blobLimitBytes?: number
  readonly leaseTimeoutMs?: number
}

/** Bus operations a worker may use; the raw `connection`/transaction handles are absent. */
export type WorkerBusHandle = Omit<Bus.Interface, "connection" | "transaction" | "transactionAsync">

export interface WorkerBus extends WorkerBusHandle {
  /** Build a scoped Bus for tests/custom callers; the returned handle has no raw connection. */
  readonly create: (options: WorkerBusCreateOptions) => WorkerBusHandle
  /** Delete one aggregate's durable log, optionally with an explicit D16 directory. */
  readonly deleteAggregate: (aggregateID: string, options?: { readonly blobDir?: string }) => number
}

export interface WorkerDatabase {
  readonly filename: string
  readonly identity: SchemaIdentity
  /** The one session this worker is bound to; absent only for degraded/test handles. */
  readonly sessionID?: string
  /**
   * Read-only scoped capability. Reads resolve through the bound TEMP views;
   * the writer seam may open/commit a transaction, but every write to a scoped
   * table is refused. It is not a raw `main` write handle.
   */
  readonly connection: WorkerConnection
  /** Read-only query connection. Every write is rejected. */
  readonly strictConnection: WorkerConnection
  /** The Drizzle graph's public connection: reads only; writes are repository-only. */
  readonly drizzleConnection: WorkerConnection
  /** The narrow typed write API; the only write path by contract. */
  readonly repository: WorkerRepository
  /** Narrow Bus/EventV2 operations; no public raw trusted connection. */
  readonly bus: WorkerBus
  /** Per-database EventV2 worker binding (lease generation, aggregate fence). */
  readonly eventBinding: EventV2.WorkerScope
  /** Read rows. Reads of hub-owned state are allowed; writes are not. */
  readonly all: <Row = Record<string, unknown>>(
    sql: string,
    params?: QueryParams,
  ) => Effect.Effect<ReadonlyArray<Row>, WorkerDatabaseError>
  /**
   * Legacy unbound/test write executor. A bound worker omits this property at
   * runtime; its only write surface is `repository`.
   */
  readonly run: (sql: string, params?: QueryParams) => Effect.Effect<WriteOutcome, WorkerDatabaseError>
  /** Close the native handle. Idempotent so termination can call it twice. */
  readonly close: Effect.Effect<void>
}

/** The read-only surface schema identity and ownership need. */
export type SchemaConnection = Pick<Connection, "all" | "exec"> & Partial<Pick<Connection, "pragma">>

const DURABLE_LOG_TABLE_SET: ReadonlySet<string> = new Set(DURABLE_LOG_TABLES)
const WORKER_WRITABLE_TABLE_SET: ReadonlySet<string> = new Set(WORKER_WRITABLE_TABLES)
export const HUB_ONLY_TABLES: readonly string[] = Object.freeze(
  SCHEMA_TABLES.filter((table) => !WORKER_WRITABLE_TABLE_SET.has(table) && !DURABLE_LOG_TABLE_SET.has(table)),
)
const HUB_ONLY_TABLE_SET: ReadonlySet<string> = new Set(HUB_ONLY_TABLES)

export { WORKER_READABLE_HUB_TABLES, SESSION_IMMUTABLE_COLUMNS }
const WORKER_READABLE_HUB_TABLE_SET: ReadonlySet<string> = new Set(WORKER_READABLE_HUB_TABLES)
export { DURABLE_LOG_TABLES }

const SCOPED_TABLE_SET: ReadonlySet<string> = new Set(["session", ...WORKER_WRITABLE_TABLES, ...DURABLE_LOG_TABLES])

/** A private in-memory database is never shared with a hub, so it has no schema gate. */
const IN_MEMORY_FILENAME = ":memory:"

/**
 * The private, trusted Drizzle runner. The in-worker projector and EventV2
 * lease write through `Database.Service`; those writes are routed to `main`
 * under the same authorizer capability, but the runner is never exposed as a
 * public connection. Reads still resolve through the bound TEMP views.
 */
const drizzleRunners = new WeakMap<object, WorkerConnection>()

const WRITE_KEYWORDS: ReadonlySet<string> = new Set(["delete", "insert", "replace", "update"])
const TRANSACTION_KEYWORDS: ReadonlySet<string> = new Set([
  "begin",
  "commit",
  "end",
  "release",
  "rollback",
  "savepoint",
])

export function currentSchemaIdentity(): SchemaIdentity {
  return {
    version: SCHEMA_VERSION,
    contentHash: hashIdentity({
      version: SCHEMA_VERSION,
      ids: SCHEMA_MIGRATION_IDS,
      bodyHash: SCHEMA_BODY_HASH,
    }),
  }
}

export function schemaIdentity(connection: SchemaConnection): SchemaIdentity {
  const version = readUserVersion(connection)
  return {
    version,
    contentHash: hashIdentity({
      version,
      ids: readMigrationIDs(connection),
      bodyHash: readPersistedBodyHash(connection),
    }),
  }
}

/** The hub's migration completion marker: `user_version` becomes the build version. */
export function markSchemaIdentity(connection: SchemaConnection): void {
  const expected = currentSchemaIdentity()
  connection.exec(`INSERT OR REPLACE INTO schema_meta (id, content_hash) VALUES ('schema', '${SCHEMA_BODY_HASH}')`)
  const content = {
    version: expected.version,
    contentHash: hashIdentity({
      version: expected.version,
      ids: readMigrationIDs(connection),
      bodyHash: readPersistedBodyHash(connection),
    }),
  }
  if (expected.contentHash !== content.contentHash) {
    throw new SchemaMismatch({ expected, observed: schemaIdentity(connection) })
  }
  connection.exec(`PRAGMA user_version = ${expected.version}`)
}

/** Fail loud when the generated worker allowlist no longer matches the schema. */
export function assertWorkerTableOwnership(connection: SchemaConnection): void {
  const observed = deriveSessionScopedTables(connection)
  if (!sameStrings(observed, WORKER_WRITABLE_TABLES)) {
    throw new WorkerTableOwnershipError({
      observed: [...observed],
      expected: [...WORKER_WRITABLE_TABLES],
    })
  }
}

/** The schema gate: read-only, before TEMP views exist, so it sees the real tables/FKs. */
function assertSchema(connection: SchemaConnection, filename: string): void {
  if (filename === IN_MEMORY_FILENAME) return
  const expected = currentSchemaIdentity()
  const observed = schemaIdentity(connection)
  if (!sameIdentity(expected, observed)) throw new SchemaMismatch({ expected, observed })
  assertWorkerTableOwnership(connection)
}

export interface OpenWorkerDatabaseOptions {
  readonly filename: string
  /** Overrides at the test boundary; production uses the shared WAL tuning. */
  readonly pragmas?: ReadonlyArray<string>
  /**
   * The one session this worker owns. When present, reads resolve through the
   * bound TEMP views and every write is repository-scoped. Absent is a
   * degraded/test handle with no session boundary.
   */
  readonly sessionID?: string
}

export function openWorkerDatabase(
  options: OpenWorkerDatabaseOptions,
): Effect.Effect<WorkerDatabase, WorkerDatabaseError> {
  return Effect.gen(function* () {
    const expected = currentSchemaIdentity()
    const sessionID = options.sessionID
    let mainAccess = 0
    const withMain = <T>(operation: () => T): T => {
      mainAccess += 1
      try {
        return operation()
      } finally {
        mainAccess -= 1
      }
    }

    const connection = yield* Effect.try({
      try: () =>
        openNodeConnection({
          filename: options.filename,
          ...(options.pragmas === undefined ? {} : { pragmas: options.pragmas }),
          foreignKeys: true,
          initialize: (scoped) => {
            // Gate before the TEMP views exist: `PRAGMA table_info`/`foreign_key_list`
            // must inspect the real schema, not view projections.
            assertSchema(scoped, options.filename)
            if (options.filename !== IN_MEMORY_FILENAME) assertTableRegistry(scoped)
            if (sessionID !== undefined) installBoundViews(scoped, sessionID)
            // The typed repository already binds/validates its own writes. The
            // Drizzle graph is a second, trusted write path (the in-worker
            // projector and the EventV2 lease/commit); these per-connection TEMP
            // triggers apply the same D6 scope fence to it, including deletes that
            // cascade. They are never shared with another connection/process.
            if (sessionID !== undefined) installScopeTriggers(scoped, SCOPED_TABLE_SET)
          },
          authorizer:
            sessionID === undefined
              ? makeUnboundWorkerAuthorizer()
              : makeWorkerAuthorizer({
                  sessionID,
                  canAccessMain: () => mainAccess > 0,
                }),
        }),
      catch: asWorkerDatabaseError,
    })

    // The Bus/EventV2 path needs main-table SQL (its own BEGIN IMMEDIATE and
    // blob refcount probe). It gets a second, viewless handle with an authorizer
    // restricted to the durable log, so it is never a public raw connection and
    // cannot touch the bound session's projection tables.
    const busConnection =
      sessionID === undefined
        ? connection
        : yield* Effect.try({
            try: () =>
              openNodeConnection({
                filename: options.filename,
                ...(options.pragmas === undefined ? {} : { pragmas: options.pragmas }),
                foreignKeys: true,
                initialize: (scoped) => {
                  scoped.function(SCOPE_FUNCTION, () => sessionID)
                  installScopeTriggers(scoped, DURABLE_LOG_TABLE_SET)
                },
                authorizer: makeBusAuthorizer(sessionID),
              }),
            catch: (error) => {
              connection.close()
              return asWorkerDatabaseError(error)
            },
          })

    return makeWorkerDatabase({
      filename: options.filename,
      identity: expected,
      connection,
      busConnection,
      ...(sessionID === undefined ? {} : { sessionID }),
      withMain,
    })
  })
}

/** Build the worker `Database.Service` over the private validated Drizzle runner. */
export function databaseLayer(database: WorkerDatabase): Layer.Layer<DatabaseService.Service> {
  const client = makeSqliteClient(
    { spanAttributes: { "db.system.name": "sqlite" } },
    workerRunner(drizzleRunners.get(database) ?? database.drizzleConnection),
  )
  return Layer.effect(
    DatabaseService.Service,
    Effect.gen(function* () {
      const operations = {
        deleteAggregate: (aggregateID: string, options?: { readonly blobDir?: string }) =>
          database.bus.deleteAggregate(aggregateID, options),
      }
      database.eventBinding.durable = operations
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          if (database.eventBinding.durable === operations) database.eventBinding.durable = undefined
        }).pipe(Effect.andThen(database.close)),
      )
      const sql = Object.assign(yield* client, { config: { filename: database.filename } })
      const db = yield* EffectDrizzleSqlite.makeWithDefaults().pipe(Effect.provideService(SqlClient.SqlClient, sql))
      EventV2.bindWorkerScope(db, database.eventBinding)
      return { db } satisfies DatabaseService.Interface
    }),
  ).pipe(Layer.provide(Reactivity.layer))
}

/** The per-worker DB service. One instance per worker graph. */
export class WorkerDb extends Context.Service<WorkerDb, WorkerDatabase>()(WORKER_DB_KEY) {}

export const layer = (database: WorkerDatabase): Layer.Layer<WorkerDb> => Layer.succeed(WorkerDb, database)

interface MakeWorkerDatabaseOptions {
  readonly filename: string
  readonly identity: SchemaIdentity
  readonly connection: NodeConnection
  /** Viewless trusted durable-log handle; identical to `connection` when unbound. */
  readonly busConnection: NodeConnection
  readonly sessionID?: string
  readonly withMain: <T>(operation: () => T) => T
}

function makeWorkerDatabase(options: MakeWorkerDatabaseOptions): WorkerDatabase {
  const { connection, filename, sessionID } = options
  const bound = sessionID !== undefined
  const repository = makeRepository(
    createWriteCapability({
      connection,
      ...(sessionID === undefined ? {} : { sessionID }),
      scopeFunction: SCOPE_FUNCTION,
      withMain: options.withMain,
    }),
  )
  // A bound writer/query handle may open/commit a transaction, but every write
  // to a scoped table is refused: the repository is the only write surface.
  const connectionGuard = bound ? guardBoundConnection(connection) : guardUnboundConnection(connection)
  // The writer seam uses the read-only capability for reads and transaction
  // control (`BEGIN`/`COMMIT`/`ROLLBACK`); direct writes remain rejected.
  const strict = guardBoundConnection(connection)
  const drizzle = guardReadOnly(connection)
  const drizzleRunner = trustedRunner(connection, options.withMain, sessionID)
  const bus = makeWorkerBus(options.busConnection, filename, sessionID)

  const all = <Row = Record<string, unknown>>(sql: string, params: QueryParams = []) =>
    Effect.try({
      try: () => strict.all<Row>(sql, params),
      catch: asWorkerDatabaseError,
    })

  const run = (sql: string, params: QueryParams = []) =>
    Effect.try({
      try: () => connectionGuard.run(sql, params),
      catch: asWorkerDatabaseError,
    })

  const eventBinding = EventV2.createWorkerScope(sessionID)
  const database = {
    filename,
    identity: options.identity,
    eventBinding,
    ...(sessionID === undefined ? {} : { sessionID }),
    connection: connectionGuard,
    strictConnection: strict,
    drizzleConnection: drizzle,
    repository,
    bus,
    all,
    ...(bound ? {} : { run }),
    close: Effect.sync(() => {
      if (options.busConnection !== connection) options.busConnection.close()
      connection.close()
    }),
  }
  drizzleRunners.set(database, drizzleRunner)
  // A bound worker has no public raw write surface; the property is omitted at
  // runtime even though legacy unbound callers still type-check against it.
  return database as WorkerDatabase
}

// ---------------------------------------------------------------------------
// Live bound views + authorizers.
// ---------------------------------------------------------------------------

const SCOPE_FUNCTION = "worker_scope_session"

/** Sentinel carried by the TEMP scope triggers; mapped to a typed denial. */
const SCOPE_TRIGGER_PREFIX = "worker scope violation"

/** SQLite authorizer action codes (sqlite3.h). */
const SQLITE_DELETE = 9
const SQLITE_INSERT = 18
const SQLITE_PRAGMA = 19
const SQLITE_READ = 20
const SQLITE_UPDATE = 23

const SCOPED_ACTIONS: ReadonlySet<number> = new Set([SQLITE_DELETE, SQLITE_INSERT, SQLITE_READ, SQLITE_UPDATE])
const WRITE_ACTIONS: ReadonlySet<number> = new Set([SQLITE_DELETE, SQLITE_INSERT, SQLITE_UPDATE])
const DENIED_ACTIONS: ReadonlySet<number> = new Set([
  1, // SQLITE_CREATE_INDEX
  2, // SQLITE_CREATE_TABLE
  3, // SQLITE_CREATE_TEMP_INDEX
  4, // SQLITE_CREATE_TEMP_TABLE
  5, // SQLITE_CREATE_TEMP_TRIGGER
  6, // SQLITE_CREATE_TEMP_VIEW
  7, // SQLITE_CREATE_TRIGGER
  8, // SQLITE_CREATE_VIEW
  10, // SQLITE_DROP_INDEX
  11, // SQLITE_DROP_TABLE
  12, // SQLITE_DROP_TEMP_INDEX
  13, // SQLITE_DROP_TEMP_TABLE
  14, // SQLITE_DROP_TEMP_TRIGGER
  15, // SQLITE_DROP_TEMP_VIEW
  16, // SQLITE_DROP_TRIGGER
  17, // SQLITE_DROP_VIEW
  24, // SQLITE_ATTACH
  25, // SQLITE_DETACH
  26, // SQLITE_ALTER_TABLE
  27, // SQLITE_REINDEX
  28, // SQLITE_ANALYZE
  29, // SQLITE_CREATE_VTABLE
  30, // SQLITE_DROP_VTABLE
])

/**
 * Install one live TEMP VIEW per scoped table. The view is already filtered to
 * the bound session/aggregate and reads through to `main`, so hub/other-process
 * writes are visible immediately and opening a long session copies no rows.
 */
function installBoundViews(connection: NodeConnection, sessionID: string): void {
  connection.function(SCOPE_FUNCTION, () => sessionID)
  for (const table of SCOPED_TABLE_SET) {
    const columns = tableColumns(connection, table)
    const scope = scopeColumn(table)
    if (scope === undefined || columns.length === 0) continue
    connection.exec(
      `CREATE TEMP VIEW ${quoteIdentifier(table)} AS SELECT ${columns
        .map(quoteIdentifier)
        .join(", ")} FROM main.${quoteIdentifier(table)} WHERE ${quoteIdentifier(scope)} = ${SCOPE_FUNCTION}()`,
    )
  }
}

/**
 * Install a per-connection TEMP trigger for every scoped table. Unlike a `main`
 * trigger these live only on this worker's handle (never the hub or a sibling),
 * and they fire for any write the connection performs — the typed repository,
 * the private Drizzle runner, or a cascade. A row whose scope column is not the
 * bound session aborts the whole statement, so the Drizzle graph cannot delete,
 * update, or insert across the D6 boundary even though it holds the trusted
 * `main` route.
 */
function installScopeTriggers(connection: NodeConnection, tables: Iterable<string>): void {
  for (const table of tables) {
    const scope = scopeColumn(table)
    if (scope === undefined) continue
    // A private/degraded in-memory handle has no shipped tables yet; a missing
    // table is not a trigger target (and must not fail the open).
    if (tableColumns(connection, table).length === 0) continue
    const target = `main.${quoteIdentifier(table)}`
    const column = quoteIdentifier(scope)
    const message = `${SCOPE_TRIGGER_PREFIX}: ${table}`.replaceAll("'", "''")
    const trigger = (event: string) => quoteIdentifier(`worker_scope_${table}_${event}`)
    const abort = `BEGIN SELECT RAISE(ABORT, '${message}'); END`
    connection.exec(
      `CREATE TEMP TRIGGER ${trigger("insert")} BEFORE INSERT ON ${target} WHEN NEW.${column} IS NOT ${SCOPE_FUNCTION}() ${abort}`,
    )
    connection.exec(
      `CREATE TEMP TRIGGER ${trigger("update")} BEFORE UPDATE ON ${target} WHEN OLD.${column} IS NOT ${SCOPE_FUNCTION}() OR NEW.${column} IS NOT ${SCOPE_FUNCTION}() ${abort}`,
    )
    connection.exec(
      `CREATE TEMP TRIGGER ${trigger("delete")} BEFORE DELETE ON ${target} WHEN OLD.${column} IS NOT ${SCOPE_FUNCTION}() ${abort}`,
    )
  }
}

export function makeWorkerAuthorizer(options: {
  readonly sessionID: string
  /** True only while the repository/private runner owns the statement. */
  readonly canAccessMain: () => boolean
}): Authorizer {
  return ({ action, table, column, database, trigger }) => {
    if (action === SQLITE_PRAGMA) return authorizePragma(table, column)
    if (DENIED_ACTIONS.has(action)) {
      throw new WorkerStatementRejectedError({ reason: `DDL or schema change "${table ?? ""}" is hub-only` })
    }
    if (!SCOPED_ACTIONS.has(action)) return 0

    const name = table ?? ""
    if (database === "temp") {
      // TEMP objects are only the read-only bound views; a write to them is a
      // scope bypass, not a legitimate statement.
      if (WRITE_ACTIONS.has(action) && SCOPED_TABLE_SET.has(name)) {
        throw new CrossSessionAccessError({ table: name, sessionID: options.sessionID })
      }
      return 0
    }
    if (database !== "main") return 0

    if (!SCOPED_TABLE_SET.has(name)) {
      assertHubOnlyAccess(name, action)
      return 0
    }
    // View expansion reads are attributed to the view that owns them. Any other
    // `main` access is either the private capability or a bypass.
    if (action === SQLITE_READ && trigger === name) return 0
    if (options.canAccessMain()) return 0
    throw new CrossSessionAccessError({ table: name, sessionID: options.sessionID })
  }
}

/**
 * An unbound/degraded handle has no session fence, so it may write scoped and
 * durable tables directly. It still refuses DDL/ATTACH/PRAGMA, hub-only writes,
 * and secret hub-table reads.
 */
export function makeUnboundWorkerAuthorizer(): Authorizer {
  return ({ action, table, column, database }) => {
    if (action === SQLITE_PRAGMA) return authorizePragma(table, column)
    if (DENIED_ACTIONS.has(action)) {
      throw new WorkerStatementRejectedError({ reason: `DDL or schema change "${table ?? ""}" is hub-only` })
    }
    if (!SCOPED_ACTIONS.has(action)) return 0
    if (database !== "main") return 0
    const name = table ?? ""
    if (SCOPED_TABLE_SET.has(name)) return 0
    assertHubOnlyAccess(name, action)
    return 0
  }
}

/**
 * The Bus/EventV2 handle is viewless and may read/write only the durable log;
 * its Bus is scoped to one aggregate by construction. Projection tables remain
 * unreachable even on this trusted handle.
 */
export function makeBusAuthorizer(sessionID: string): Authorizer {
  return ({ action, table, column, database }) => {
    if (action === SQLITE_PRAGMA) return authorizePragma(table, column)
    if (DENIED_ACTIONS.has(action)) {
      throw new WorkerStatementRejectedError({ reason: `DDL or schema change "${table ?? ""}" is hub-only` })
    }
    if (!SCOPED_ACTIONS.has(action)) return 0
    const name = table ?? ""
    if (database !== "main") return 0
    if (DURABLE_LOG_TABLE_SET.has(name)) return 0
    if (SCOPED_TABLE_SET.has(name)) {
      throw new CrossSessionAccessError({ table: name, sessionID })
    }
    assertHubOnlyAccess(name, action)
    return 0
  }
}

function authorizePragma(table: string | null, column: string | null): number {
  const name = (table ?? "").toLowerCase()
  if (!READ_ONLY_PRAGMAS.has(name)) {
    throw new WorkerStatementRejectedError({ reason: `pragma "${name}" is not worker-readable` })
  }
  if (column !== null && column !== undefined && !PRAGMA_ARGUMENT_PRAGMAS.has(name)) {
    throw new WorkerStatementRejectedError({ reason: `pragma "${name}" is read-only in a worker` })
  }
  return 0
}

/**
 * Writes to any hub-only table are refused, and reads are limited to the
 * non-secret allowlist. Secret-bearing auth tables therefore fail closed on
 * every worker authorizer even when the statement is only a `SELECT`.
 */
function assertHubOnlyAccess(table: string, action: number): void {
  if (!HUB_ONLY_TABLE_SET.has(table)) return
  if (WRITE_ACTIONS.has(action)) throw new HubOnlyTableError({ table })
  if (!WORKER_READABLE_HUB_TABLE_SET.has(table)) {
    throw new HubOnlyTableError({ table })
  }
}

// ---------------------------------------------------------------------------
// Public read/write capabilities and the private Drizzle runner.
// ---------------------------------------------------------------------------

const RAW_WRITE_REJECTED = "raw writes are repository-only; use WorkerRepository"

/** One statement only. Multi-statement SQL is never a valid worker statement. */
function assertSingleStatement(sql: string): void {
  if (hasMultipleStatements(sql)) {
    throw new WorkerStatementRejectedError({ reason: "multiple statements are hub-only" })
  }
}

/** Unbound/test handles keep direct scoped writes, but never multi-statement SQL. */
function guardUnboundConnection(connection: NodeConnection): WorkerConnection {
  const check = (sql: string): void => assertSingleStatement(sql)
  return {
    owner: connection.owner,
    filename: connection.filename,
    get closed() {
      return connection.closed
    },
    query: <Row>(sql: string) => ({
      all: (...params: SQLQueryBindings[]) => {
        check(sql)
        return connection.all<Row>(sql, params)
      },
      values: (...params: SQLQueryBindings[]) => {
        check(sql)
        return connection.values(sql, params)
      },
      get: (...params: SQLQueryBindings[]) => {
        check(sql)
        return connection.all<Row>(sql, params)[0] ?? null
      },
      run: (...params: SQLQueryBindings[]) => {
        check(sql)
        return connection.run(sql, params)
      },
    }),
    all: <Row>(sql: string, params: QueryParams = []) => {
      check(sql)
      return connection.all<Row>(sql, params)
    },
    values: <Row>(sql: string, params: QueryParams = []) => {
      check(sql)
      return connection.values(sql, params)
    },
    run: (sql, params = []) => {
      check(sql)
      return connection.run(sql, params)
    },
    exec: (sql) => {
      check(sql)
      connection.exec(sql)
    },
    pragma: (name) => {
      assertReadOnlyPragma(name)
      return connection.pragma(name)
    },
    journalMode: () => connection.journalMode(),
    close: () => connection.close(),
  }
}

/**
 * A bound writer/query handle. Reads resolve through the bound views; the
 * writer seam may open/commit transactions, but every scoped write is refused.
 */
function guardBoundConnection(connection: NodeConnection): WorkerConnection {
  const read = <Row>(sql: string, params: QueryParams): ReadonlyArray<Row> => {
    assertSingleStatement(sql)
    return connection.all<Row>(sql, params)
  }
  const readValues = <Row>(sql: string, params: QueryParams): ReadonlyArray<Row> => {
    assertSingleStatement(sql)
    return connection.values<Row>(sql, params)
  }
  const rejectWrite = (): never => {
    throw new WorkerStatementRejectedError({ reason: RAW_WRITE_REJECTED })
  }
  return {
    owner: connection.owner,
    filename: connection.filename,
    get closed() {
      return connection.closed
    },
    query: <Row>(sql: string) => ({
      all: (...params: SQLQueryBindings[]) => read<Row>(sql, params),
      values: (...params: SQLQueryBindings[]) => readValues<ReadonlyArray<unknown>>(sql, params),
      get: (...params: SQLQueryBindings[]) => read<Row>(sql, params)[0] ?? null,
      run: (..._params: SQLQueryBindings[]) => rejectWrite(),
    }),
    all: <Row>(sql: string, params: QueryParams = []) => read<Row>(sql, params),
    values: <Row>(sql: string, params: QueryParams = []) => readValues<Row>(sql, params),
    run: (_sql, _params = []) => rejectWrite(),
    exec: (sql) => {
      assertSingleStatement(sql)
      const stripped = stripComments(sql)
      const keyword = leadingKeyword(stripped)
      if (TRANSACTION_KEYWORDS.has(keyword)) {
        connection.exec(sql)
        return
      }
      if (keyword === "pragma") {
        assertReadOnlyPragma(pragmaName(stripped))
        connection.exec(sql)
        return
      }
      rejectWrite()
    },
    pragma: (name) => {
      assertReadOnlyPragma(name)
      return connection.pragma(name)
    },
    journalMode: () => connection.journalMode(),
    close: () => connection.close(),
  }
}

/** Read-only public connection: every write and every DDL statement is refused. */
function guardReadOnly(connection: NodeConnection): WorkerConnection {
  const check = (sql: string): void => {
    assertSingleStatement(sql)
  }
  const rejectWrite = (): never => {
    throw new WorkerStatementRejectedError({ reason: "the worker query connection is read-only" })
  }
  return {
    owner: connection.owner,
    filename: connection.filename,
    get closed() {
      return connection.closed
    },
    query: <Row>(sql: string) => ({
      all: (...params: SQLQueryBindings[]) => {
        check(sql)
        return connection.all<Row>(sql, params)
      },
      values: (...params: SQLQueryBindings[]) => {
        check(sql)
        return connection.values<ReadonlyArray<unknown>>(sql, params)
      },
      get: (...params: SQLQueryBindings[]) => {
        check(sql)
        return connection.all<Row>(sql, params)[0] ?? null
      },
      run: (..._params: SQLQueryBindings[]) => rejectWrite(),
    }),
    all: <Row>(sql: string, params: QueryParams = []) => {
      check(sql)
      return connection.all<Row>(sql, params)
    },
    values: <Row>(sql: string, params: QueryParams = []) => {
      check(sql)
      return connection.values(sql, params)
    },
    run: (_sql, _params = []) => rejectWrite(),
    exec: () => rejectWrite(),
    pragma: (name) => {
      assertReadOnlyPragma(name)
      return connection.pragma(name)
    },
    journalMode: () => connection.journalMode(),
    close: () => connection.close(),
  }
}

/**
 * The private Drizzle runner. Reads use the live bound views; writes are routed
 * to `main` and bracketed by the authorizer capability. It is not a public
 * handle and it performs no SQL validation/rewriting beyond qualifying the
 * write target.
 */
function trustedRunner(
  connection: NodeConnection,
  withMain: <T>(operation: () => T) => T,
  sessionID: string | undefined,
): WorkerConnection {
  // A D6 trigger abort surfaces as a SQLite constraint error. Map it back to the
  // same typed denial the repository/authorizer raises, so a trusted-path caller
  // (projector, EventV2) never has to parse SQL text.
  const guard = <T>(operation: () => T): T => {
    try {
      return operation()
    } catch (error) {
      throw mapScopeTriggerError(error, sessionID)
    }
  }
  // Drizzle's SqlClient executes writes through `query(...).all/values` too, so
  // every mode must honour the write route, not only `run`.
  const execute = (
    sql: string,
    params: QueryParams,
    mode: "all" | "values" | "run",
  ): WriteOutcome | ReadonlyArray<Record<string, unknown>> | ReadonlyArray<ReadonlyArray<unknown>> => {
    if (!WRITE_KEYWORDS.has(leadingKeyword(stripComments(sql)))) {
      if (mode === "run") {
        connection.all(sql, params)
        return { changes: 0, lastInsertRowid: 0 }
      }
      return mode === "values"
        ? (connection.values<ReadonlyArray<unknown>>(sql, params) as ReadonlyArray<ReadonlyArray<unknown>>)
        : (connection.all(sql, params) as ReadonlyArray<Record<string, unknown>>)
    }
    return guard(() =>
      withMain(() => {
        const routed = routeWriteToMain(sql)
        if (mode === "run") return connection.run(routed, params)
        return mode === "values"
          ? (connection.values<ReadonlyArray<unknown>>(routed, params) as ReadonlyArray<ReadonlyArray<unknown>>)
          : (connection.all(routed, params) as ReadonlyArray<Record<string, unknown>>)
      }),
    )
  }
  return {
    owner: connection.owner,
    filename: connection.filename,
    get closed() {
      return connection.closed
    },
    query: <Row>(sql: string) => ({
      all: (...params: SQLQueryBindings[]) => execute(sql, params, "all") as ReadonlyArray<Row>,
      values: (...params: SQLQueryBindings[]) =>
        execute(sql, params, "values") as ReadonlyArray<ReadonlyArray<unknown>>,
      get: (...params: SQLQueryBindings[]) => (execute(sql, params, "all") as ReadonlyArray<Row>)[0] ?? null,
      run: (...params: SQLQueryBindings[]) => execute(sql, params, "run") as WriteOutcome,
    }),
    all: <Row>(sql: string, params: QueryParams = []) => execute(sql, params, "all") as ReadonlyArray<Row>,
    values: <Row>(sql: string, params: QueryParams = []) => execute(sql, params, "values") as ReadonlyArray<Row>,
    run: (sql: string, params: QueryParams = []) => execute(sql, params, "run") as WriteOutcome,
    exec: (sql) => {
      const stripped = stripComments(sql)
      const keyword = leadingKeyword(stripped)
      if (TRANSACTION_KEYWORDS.has(keyword)) {
        connection.exec(sql)
        return
      }
      if (keyword === "pragma") {
        assertReadOnlyPragma(pragmaName(stripped))
        connection.exec(sql)
        return
      }
      withMain(() => guard(() => connection.exec(routeWriteToMain(sql))))
    },
    pragma: (name) => {
      assertReadOnlyPragma(name)
      return connection.pragma(name)
    },
    journalMode: () => connection.journalMode(),
    close: () => connection.close(),
  }
}

const WRITE_TARGET =
  /\b(insert(?:\s+or\s+\w+)?\s+into|replace\s+(?:into\s+)?|update(?:\s+or\s+\w+)?|delete\s+from)\s+([`"\[\]\w.]+)/i

/** Qualify the first write target with `main.` so Drizzle writes bypass the TEMP view. */
function routeWriteToMain(sql: string): string {
  const match = WRITE_TARGET.exec(sql)
  const token = match?.[2]
  if (match?.index === undefined || token === undefined) return sql
  if (/^(?:main|temp)\s*\./i.test(token.replace(/[`"[\]]/g, ""))) return sql
  const start = match.index + match[0].lastIndexOf(token)
  return `${sql.slice(0, start)}main.${token}${sql.slice(start + token.length)}`
}

// ---------------------------------------------------------------------------
// Bus capability.
// ---------------------------------------------------------------------------

let nextBusOwner = 0

function makeWorkerBus(connection: NodeConnection, filename: string, sessionID: string | undefined): WorkerBus {
  const create = (options: WorkerBusCreateOptions): WorkerBusHandle => {
    const aggregateID = options.aggregateID ?? sessionID
    if (sessionID !== undefined && options.aggregateID !== undefined && options.aggregateID !== sessionID) {
      throw new CrossSessionAccessError({ table: "event", sessionID })
    }
    const blobDir =
      options.blobDir ??
      (filename === IN_MEMORY_FILENAME ? undefined : path.join(path.dirname(filename), "event-blobs"))
    const bus = Bus.create({
      repository: createDurableRepository(capabilityConnection(connection, (operation) => operation())),
      ownerID: options.ownerID,
      ...(options.definitions === undefined ? {} : { definitions: options.definitions }),
      ...(aggregateID === undefined ? {} : { aggregateID }),
      ...(blobDir === undefined ? {} : { blobDir }),
      ...(options.blobLimitBytes === undefined ? {} : { blobLimitBytes: options.blobLimitBytes }),
      ...(options.leaseTimeoutMs === undefined ? {} : { leaseTimeoutMs: options.leaseTimeoutMs }),
    })
    return safeBusHandle(bus)
  }
  nextBusOwner += 1
  const defaultBus = create({
    ownerID: `worker-bus-${process.pid}-${nextBusOwner}`,
    ...(sessionID === undefined ? {} : { aggregateID: sessionID }),
  })
  const deleteAggregate = (aggregateID: string, options: { readonly blobDir?: string } = {}): number => {
    if (sessionID !== undefined && aggregateID !== sessionID) {
      throw new CrossSessionAccessError({ table: "event", sessionID })
    }
    if (options.blobDir === undefined) return defaultBus.deleteAggregate(aggregateID)
    const bus = create({
      ownerID: `worker-delete-${process.pid}-${nextBusOwner}`,
      aggregateID,
      blobDir: options.blobDir,
    })
    return bus.deleteAggregate(aggregateID)
  }
  return Object.assign(defaultBus, { create, deleteAggregate })
}

function capabilityConnection(connection: NodeConnection, withMain: <T>(operation: () => T) => T): Bus.Connection {
  return {
    filename: connection.filename,
    all: (sql, params = []) => withMain(() => connection.all(sql, params)),
    run: (sql, params = []) => withMain(() => connection.run(sql, params)),
    exec: (sql) => withMain(() => connection.exec(sql)),
  }
}

function safeBusHandle(bus: Bus.Interface): WorkerBusHandle {
  const { connection: _connection, transaction: _transaction, transactionAsync: _transactionAsync, ...safe } = bus
  return safe
}

function workerRunner(connection: WorkerConnection): SqliteNativeRunner {
  return {
    all: (query, params) => [...connection.query(query).all(...(params as SQLQueryBindings[]))],
    values: (query, params) => [...connection.query(query).values(...(params as SQLQueryBindings[]))],
  }
}

// ---------------------------------------------------------------------------
// Small lexical helpers (no statement validation/rewriting).
// ---------------------------------------------------------------------------

/**
 * The scope column for a table the worker may observe: the session root by `id`,
 * the durable log by `aggregate_id`, and every session-scoped table by
 * `session_id`.
 */
function scopeColumn(table: string): string | undefined {
  if (table === "session") return "id"
  if (DURABLE_LOG_TABLE_SET.has(table)) return "aggregate_id"
  if (WORKER_WRITABLE_TABLE_SET.has(table)) return "session_id"
  return undefined
}

function tableColumns(connection: Connection, table: string): readonly string[] {
  return connection
    .all<{ readonly name: string }>(`PRAGMA main.table_info(${quoteIdentifier(table)})`)
    .map((row) => row.name)
}

function leadingKeyword(sql: string): string {
  return /^[\s(]*([A-Za-z_]+)/.exec(sql)?.[1]?.toLowerCase() ?? ""
}

/** Read-only pragmas whose optional parenthesized argument is a schema name. */
const PRAGMA_ARGUMENT_PRAGMAS: ReadonlySet<string> = new Set([
  "compile_options",
  "database_list",
  "foreign_key_list",
  "index_info",
  "index_list",
  "integrity_check",
  "table_info",
  "table_list",
])

const READ_ONLY_PRAGMAS: ReadonlySet<string> = new Set([
  "busy_timeout",
  "compile_options",
  "database_list",
  "foreign_key_list",
  "foreign_keys",
  "index_info",
  "index_list",
  "integrity_check",
  "journal_mode",
  "schema_version",
  "synchronous",
  "table_info",
  "table_list",
  "user_version",
  "wal_autocheckpoint",
])

function pragmaName(sql: string): string {
  return /^\s*pragma\s+([^;]+)/i.exec(sql)?.[1]?.trim() ?? ""
}

function assertReadOnlyPragma(name: string): void {
  const normalized = name.trim()
  if (normalized.includes("=")) {
    throw new WorkerStatementRejectedError({ reason: `pragma "${name}" is read-only in a worker` })
  }
  const key = normalized.split(/[\s(]/)[0]?.toLowerCase() ?? ""
  if (!READ_ONLY_PRAGMAS.has(key)) {
    throw new WorkerStatementRejectedError({ reason: `pragma "${key}" is not worker-readable` })
  }
}

function stripComments(sql: string): string {
  let out = ""
  let index = 0
  while (index < sql.length) {
    const char = sql[index]
    if (char === "'" || char === '"' || char === "`") {
      out += char
      index += 1
      while (index < sql.length) {
        const next = sql[index]
        out += next
        if (next === char) {
          if (sql[index + 1] === char) {
            out += sql[index + 1]
            index += 2
            continue
          }
          index += 1
          break
        }
        index += 1
      }
      continue
    }
    if (char === "[") {
      out += char
      index += 1
      while (index < sql.length) {
        const next = sql[index]
        out += next
        index += 1
        if (next === "]") break
      }
      continue
    }
    if (char === "-" && sql[index + 1] === "-") {
      while (index < sql.length && sql[index] !== "\n") index += 1
      continue
    }
    if (char === "/" && sql[index + 1] === "*") {
      index += 2
      while (index < sql.length && !(sql[index] === "*" && sql[index + 1] === "/")) index += 1
      index += 2
      continue
    }
    out += char
    index += 1
  }
  return out
}

/** True when a top-level `;` is followed by another statement. */
function hasMultipleStatements(sql: string): boolean {
  let depth = 0
  let index = 0
  while (index < sql.length) {
    const char = sql[index]
    if (char === "'" || char === '"' || char === "`") {
      index = skipQuoted(sql, index, char)
      continue
    }
    if (char === "[") {
      index += 1
      while (index < sql.length && sql[index] !== "]") index += 1
      index += 1
      continue
    }
    if (char === "-" && sql[index + 1] === "-") {
      while (index < sql.length && sql[index] !== "\n") index += 1
      continue
    }
    if (char === "/" && sql[index + 1] === "*") {
      index += 2
      while (index < sql.length && !(sql[index] === "*" && sql[index + 1] === "/")) index += 1
      index += 2
      continue
    }
    if (char === "(") depth += 1
    else if (char === ")") depth = Math.max(0, depth - 1)
    else if (char === ";" && depth === 0) {
      const rest = stripComments(sql.slice(index + 1)).replace(/[\s;]+/g, "")
      if (rest.length > 0) return true
    }
    index += 1
  }
  return false
}

function skipQuoted(sql: string, start: number, quote: string): number {
  let index = start + 1
  while (index < sql.length) {
    if (sql[index] === quote) {
      if (sql[index + 1] === quote) {
        index += 2
        continue
      }
      return index + 1
    }
    index += 1
  }
  return index
}

function readUserVersion(connection: SchemaConnection): number {
  if (connection.pragma !== undefined) return Number(connection.pragma("user_version") ?? 0)
  const rows = connection.all<{ readonly user_version: number }>("PRAGMA user_version")
  return Number(rows[0]?.user_version ?? 0)
}

function readMigrationIDs(connection: SchemaConnection): readonly string[] {
  try {
    const rows = connection.all<{ readonly id: string }>("SELECT id FROM migration ORDER BY id")
    return rows.map((row) => row.id)
  } catch {
    return []
  }
}

function sameIdentity(left: SchemaIdentity, right: SchemaIdentity): boolean {
  return left.version === right.version && left.contentHash === right.contentHash
}

function sameStrings(left: readonly string[], right: readonly string[]): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index])
}

function quoteIdentifier(value: string): string {
  return `"${value.replaceAll('"', '""')}"`
}

function reasonOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

/**
 * Translate a TEMP scope-trigger abort into the typed cross-session denial.
 * Any other error (including the repository's own typed errors) passes through
 * unchanged.
 */
function mapScopeTriggerError(error: unknown, sessionID: string | undefined): unknown {
  if (sessionID === undefined) return error
  const message = reasonOf(error)
  const marker = `${SCOPE_TRIGGER_PREFIX}: `
  const index = message.indexOf(marker)
  if (index < 0) return error
  const table = message.slice(index + marker.length).split(/[\s'"]/)[0] ?? "unknown"
  return new CrossSessionAccessError({ table, sessionID })
}

function asWorkerDatabaseError(error: unknown): WorkerDatabaseError {
  if (
    error instanceof SchemaMismatch ||
    error instanceof HubOnlyTableError ||
    error instanceof WorkerStatementRejectedError ||
    error instanceof WorkerTableOwnershipError ||
    error instanceof CrossSessionAccessError ||
    error instanceof WorkerDatabaseOpenError
  ) {
    return error
  }
  return new WorkerDatabaseOpenError({ reason: reasonOf(error) })
}

export * as WorkerDbModule from "./db"
