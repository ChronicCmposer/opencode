// Per-worker SQLite connection, schema gate, and table ownership.
//
// A session worker owns exactly one WAL connection, opened through the shared
// db-core helper. There is no writer isolate and no writer RPC. The hub is the
// only process that migrates; a worker refuses a schema whose `user_version` or
// migration journal differs from the build it runs.

import { Context, Effect, Layer, Schema } from "effect"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { Database as DatabaseService } from "@opencode-ai/core/database/database"
import { makeSqliteClient, type SqliteNativeRunner } from "@opencode-ai/core/database/sqlite-adapter"
import { openConnection, type Connection, type QueryParams, type WriteOutcome } from "@opencode-ai/core/database/connection"
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

/** A worker attempted DDL or a write shape it cannot classify safely. */
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

/** The synchronous surface a per-worker Bus also consumes. */
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
  | WorkerDatabaseOpenError

export interface WorkerDatabase {
  readonly filename: string
  readonly identity: SchemaIdentity
  readonly connection: WorkerConnection
  /** Read rows. Reads of hub-owned state are allowed; writes are not. */
  readonly all: <Row = Record<string, unknown>>(
    sql: string,
    params?: QueryParams,
  ) => Effect.Effect<ReadonlyArray<Row>, WorkerDatabaseError>
  /** Write one statement, refusing DDL and hub-only tables before SQLite runs it. */
  readonly run: (sql: string, params?: QueryParams) => Effect.Effect<WriteOutcome, WorkerDatabaseError>
  /** Close the native handle. Idempotent so termination can call it twice. */
  readonly close: Effect.Effect<void>
}

/** The read-only surface schema identity and ownership need. */
export type SchemaConnection = Pick<Connection, "all" | "exec"> &
  Partial<Pick<Connection, "pragma">>

// Tables that carry an actual foreign-key path to the `session` aggregate are
// worker-writable (D6). The generated allowlist is the single source; the live
// schema is re-derived at boot so a drifted table set fails the gate.
const DURABLE_LOG_TABLES: ReadonlySet<string> = new Set(["event", "event_sequence"])
const WORKER_WRITABLE_TABLE_SET: ReadonlySet<string> = new Set(WORKER_WRITABLE_TABLES)
export const HUB_ONLY_TABLES: readonly string[] = Object.freeze(
  SCHEMA_TABLES.filter((table) => !WORKER_WRITABLE_TABLE_SET.has(table) && !DURABLE_LOG_TABLES.has(table)),
)
const HUB_ONLY_TABLE_SET: ReadonlySet<string> = new Set(HUB_ONLY_TABLES)

/**
 * The session aggregate root is materialized by the in-worker `SessionProjector`
 * in the same transaction as its durable event, so the committing worker must
 * write this one hub-classified table. Every other hub-only table stays refused.
 */
const SESSION_ROOT_TABLES: ReadonlySet<string> = new Set(["session"])

/** A private in-memory database is never shared with a hub, so it has no schema gate. */
const IN_MEMORY_FILENAME = ":memory:"

const DDL_KEYWORDS: ReadonlySet<string> = new Set([
  "alter",
  "attach",
  "create",
  "detach",
  "drop",
  "pragma",
  "reindex",
  "truncate",
  "vacuum",
])

const WRITE_KEYWORDS: ReadonlySet<string> = new Set(["delete", "insert", "replace", "update"])

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
  connection.exec(
    `INSERT OR REPLACE INTO schema_meta (id, content_hash) VALUES ('schema', '${SCHEMA_BODY_HASH}')`,
  )
  // Verify the journal and body at the *expected* version, then write the
  // marker version itself.
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

export interface OpenWorkerDatabaseOptions {
  readonly filename: string
  /** Overrides at the test boundary; production uses the shared WAL tuning. */
  readonly pragmas?: ReadonlyArray<string>
}

export function openWorkerDatabase(
  options: OpenWorkerDatabaseOptions,
): Effect.Effect<WorkerDatabase, WorkerDatabaseError> {
  return Effect.gen(function* () {
    const connection = yield* Effect.try({
      try: () =>
        openConnection({
          filename: options.filename,
          ...(options.pragmas === undefined ? {} : { pragmas: options.pragmas }),
        }),
      catch: (error) => new WorkerDatabaseOpenError({ reason: reasonOf(error) }),
    })

    const expected = currentSchemaIdentity()
    // A shared file must match the hub's schema. A private in-memory database
    // cannot observe a mixed schema and is test/degraded-only, so it skips the
    // gate rather than rejecting an empty private handle.
    if (options.filename !== IN_MEMORY_FILENAME) {
      // The gate is read-only: a mismatch closes the handle without running DDL
      // or marking schema state.
      const observed = schemaIdentity(connection)
      if (!sameIdentity(expected, observed)) {
        connection.close()
        return yield* Effect.fail(new SchemaMismatch({ expected, observed }))
      }

      try {
        assertWorkerTableOwnership(connection)
      } catch (error) {
        connection.close()
        return yield* Effect.fail(asWorkerDatabaseError(error))
      }
    }

    return makeWorkerDatabase(options.filename, expected, connection)
  })
}

/** Build the worker `Database.Service` over this connection. No cast, no DDL. */
export function databaseLayer(database: WorkerDatabase): Layer.Layer<DatabaseService.Service> {
  const client = makeSqliteClient(
    { spanAttributes: { "db.system.name": "sqlite" } },
    workerRunner(database.connection),
  )
  return Layer.effect(
    DatabaseService.Service,
    Effect.gen(function* () {
      // Disposing the worker graph (termination) closes the WAL handle.
      yield* Effect.addFinalizer(() => database.close)
      const sql = yield* client
      const db = yield* EffectDrizzleSqlite.makeWithDefaults().pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
      )
      return { db } satisfies DatabaseService.Interface
    }),
  ).pipe(Layer.provide(Reactivity.layer))
}

/** The per-worker DB service. One instance per worker graph. */
export class WorkerDb extends Context.Service<WorkerDb, WorkerDatabase>()(WORKER_DB_KEY) {}

export const layer = (database: WorkerDatabase): Layer.Layer<WorkerDb> => Layer.succeed(WorkerDb, database)

function makeWorkerDatabase(filename: string, identity: SchemaIdentity, connection: Connection): WorkerDatabase {
  const guarded = guard(connection)

  const all = <Row = Record<string, unknown>>(sql: string, params: QueryParams = []) =>
    Effect.try({
      try: () => {
        assertWorkerStatement(sql)
        return guarded.all<Row>(sql, params)
      },
      catch: asWorkerDatabaseError,
    })

  const run = (sql: string, params: QueryParams = []) =>
    Effect.try({
      try: () => {
        assertWorkerStatement(sql)
        return guarded.run(sql, params)
      },
      catch: asWorkerDatabaseError,
    })

  return {
    filename,
    identity,
    connection: guarded,
    all,
    run,
    close: Effect.sync(() => connection.close()),
  }
}

/** Wrap the db-core connection with the worker statement/table guard. */
function guard(connection: Connection): WorkerConnection {
  return {
    owner: connection.owner,
    filename: connection.filename,
    get closed() {
      return connection.closed
    },
    query: <Row>(sql: string) => ({
      all: (...params: SQLQueryBindings[]) => {
        assertWorkerStatement(sql)
        return connection.all<Row>(sql, params)
      },
      values: (...params: SQLQueryBindings[]) => {
        assertWorkerStatement(sql)
        return connection.values(sql, params)
      },
      get: (...params: SQLQueryBindings[]) => {
        assertWorkerStatement(sql)
        return connection.all<Row>(sql, params)[0] ?? null
      },
      run: (...params: SQLQueryBindings[]) => {
        assertWorkerStatement(sql)
        return connection.run(sql, params)
      },
    }),
    all: <Row>(sql: string, params: QueryParams = []) => {
      assertWorkerStatement(sql)
      return connection.all<Row>(sql, params)
    },
    values: <Row>(sql: string, params: QueryParams = []) => {
      assertWorkerStatement(sql)
      return connection.values<Row>(sql, params)
    },
    run: (sql, params = []) => {
      assertWorkerStatement(sql)
      return connection.run(sql, params)
    },
    exec: (sql) => {
      assertWorkerStatement(sql)
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

function workerRunner(connection: WorkerConnection): SqliteNativeRunner {
  return {
    all: (query, params) => {
      assertWorkerStatement(query)
      return [...connection.query(query).all(...(params as SQLQueryBindings[]))]
    },
    values: (query, params) => {
      assertWorkerStatement(query)
      return [...connection.query(query).values(...(params as SQLQueryBindings[]))]
    },
  }
}

// The single worker SQL boundary: classify by statement kind (not the leading
// token), reject multi-statement SQL, CTE-DML, DDL, and non-read-only pragmas,
// then enforce table ownership on writes (C9/D14/D21).
function assertWorkerStatement(sql: string): void {
  if (hasMultipleStatements(sql)) {
    throw new WorkerStatementRejectedError({ reason: "multiple statements are hub-only" })
  }
  const stripped = stripComments(sql)
  const keyword = statementKeyword(stripped)
  if (keyword === "pragma") {
    assertReadOnlyPragma(pragmaName(stripped))
    return
  }
  if (DDL_KEYWORDS.has(keyword)) throw new WorkerStatementRejectedError({ reason: `DDL "${keyword}" is hub-only` })
  if (!WRITE_KEYWORDS.has(keyword)) return
  const table = writtenTable(stripped)
  if (table === undefined) {
    throw new WorkerStatementRejectedError({ reason: `cannot classify the table of "${sql.trim().slice(0, 80)}"` })
  }
  if (SESSION_ROOT_TABLES.has(table)) return
  // Durable log writes are the worker Bus's own connection (C2/D13); ownership
  // and sequence fencing are enforced by the Bus lease before it writes.
  if (DURABLE_LOG_TABLES.has(table)) return
  if (HUB_ONLY_TABLE_SET.has(table)) throw new HubOnlyTableError({ table })
  if (!WORKER_WRITABLE_TABLE_SET.has(table)) {
    throw new WorkerStatementRejectedError({ reason: `table "${table}" is not worker-writable` })
  }
}

const READ_ONLY_PRAGMAS: ReadonlySet<string> = new Set([
  "busy_timeout",
  "compile_options",
  "database_list",
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

/** Remove line/block comments outside quoted strings and identifiers. */
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
      index += 1
      while (index < sql.length) {
        if (sql[index] === char) {
          if (sql[index + 1] === char) {
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

/** Top-level words outside parentheses/strings; used for statement kind. */
function topLevelWords(sql: string): string[] {
  const words: string[] = []
  let depth = 0
  let index = 0
  let current = ""
  const flush = () => {
    if (current.length > 0 && depth === 0) words.push(current.toLowerCase())
    current = ""
  }
  while (index < sql.length) {
    const char = sql[index]
    if (char === "'" || char === '"' || char === "`") {
      flush()
      index += 1
      while (index < sql.length) {
        if (sql[index] === char) {
          if (sql[index + 1] === char) {
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
    if (char === "(") {
      flush()
      depth += 1
    } else if (char === ")") {
      flush()
      depth = Math.max(0, depth - 1)
    } else if (/[A-Za-z_]/.test(char)) {
      current += char
    } else {
      flush()
    }
    index += 1
  }
  flush()
  return words
}

const STATEMENT_KINDS: ReadonlySet<string> = new Set([...WRITE_KEYWORDS, "select"])

function statementKeyword(sql: string): string {
  const words = topLevelWords(sql)
  const first = words[0] ?? ""
  if (first !== "with") return first
  const kind = words.slice(1).find((word) => STATEMENT_KINDS.has(word))
  return kind ?? first
}

function writtenTable(sql: string): string | undefined {
  const normalized = sql.trim().replace(/\s+/g, " ")
  const patterns = [
    /\binsert\s+(?:or\s+\w+\s+)?into\s+([`"\[\].\w]+)/i,
    /\breplace\s+(?:into\s+)?([`"\[\].\w]+)/i,
    /\bupdate\s+(?:or\s+\w+\s+)?([`"\[\].\w]+)/i,
    /\bdelete\s+from\s+([`"\[\].\w]+)/i,
  ]
  for (const pattern of patterns) {
    const match = pattern.exec(normalized)
    if (match?.[1] !== undefined) return bareTable(match[1])
  }
  return undefined
}

function bareTable(token: string): string {
  const parts = token.replace(/[`"[\]]/g, "").split(".")
  return parts[parts.length - 1] ?? token
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
  return `"${value.replace(/"/g, '""')}"`
}

function reasonOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

function asWorkerDatabaseError(error: unknown): WorkerDatabaseError {
  if (
    error instanceof SchemaMismatch ||
    error instanceof HubOnlyTableError ||
    error instanceof WorkerStatementRejectedError ||
    error instanceof WorkerTableOwnershipError ||
    error instanceof WorkerDatabaseOpenError
  ) {
    return error
  }
  return new WorkerDatabaseOpenError({ reason: reasonOf(error) })
}

export * as WorkerDbModule from "./db"
