#!/usr/bin/env bun
//
// Generate the shipped migration statements and the derived table allowlist.
//
// The hub applies plain SQL strings because workers never run DDL (D7). Core
// defines its schema as `up(tx)` Effect functions, so this script flattens the
// schema into the frozen `migrationStatements` list the hub applies before it
// opens the spawn gate.
//
// The emitted list reproduces `DatabaseMigration.apply` on an empty database:
//   1. every `schema.gen.ts` `up` statement (the full current schema),
//   2. the `migration` journal table,
//   3. one journal seed row per core migration (its timestamp is never read).
//
// Every statement is emitted in an idempotent form (`IF NOT EXISTS` / `INSERT OR
// IGNORE`) so re-applying the already-migrated file is a safe no-op. The same
// run derives `session/db/allowlist.gen.ts`: the table set and the tables with a
// `session_id` column (D6).
//
// Run from `packages/opencode`: `bun script/migration-statements.ts`.
// `--check` verifies both committed files are current without writing them.

import { Effect } from "effect"
import fs from "node:fs/promises"
import path from "node:path"
import { Database } from "bun:sqlite"
import { fileURLToPath } from "node:url"
import schema from "@opencode-ai/core/database/schema.gen"
import { migrations } from "@opencode-ai/core/database/migration.gen"
import { deriveSessionScopedTables, listSchemaTables } from "../src/session/db/derive"
import { hashStatements } from "../src/session/db/identity"
import {
  DURABLE_LOG_TABLES,
  IMMUTABLE_COLUMNS,
  SECRET_HUB_TABLES,
  WORKER_READABLE_HUB_TABLES,
} from "../src/session/worker/repository/policy"

const output = path.resolve(
  fileURLToPath(new URL("../src/session/writer/migration.statements.gen.ts", import.meta.url)),
)
const allowlistOutput = path.resolve(fileURLToPath(new URL("../src/session/db/allowlist.gen.ts", import.meta.url)))
const registryOutput = path.resolve(
  fileURLToPath(new URL("../src/session/worker/repository/tables.gen.ts", import.meta.url)),
)
const ledgerSchemaPath = path.resolve(
  fileURLToPath(new URL("../src/session/ledger/sql/001-tool-call.sql", import.meta.url)),
)
const ledgerFallbackOutput = path.resolve(
  fileURLToPath(new URL("../src/session/ledger/sql/fallback.gen.ts", import.meta.url)),
)
const check = process.argv.includes("--check")

/** Journal timestamp: the value is bookkeeping only, so a constant keeps the file deterministic. */
const JOURNAL_TIME = 0

/**
 * The `tool_call`/`worker_crash` ledger is shipped by opencode rather than core,
 * so it needs its own journal id. Registering it here puts the ledger schema in
 * the same hub-only statement list and content hash every worker gate checks.
 */
const LEDGER_MIGRATION_ID = "20261026000000_tool_call_ledger"

/**
 * The persisted schema identity marker. The hub writes the body hash here inside
 * the same `BEGIN IMMEDIATE` transaction that applies the statements, so a
 * worker can verify the exact shipped SQL content, not just the journal ids.
 */
const SCHEMA_META_STATEMENT = "CREATE TABLE `schema_meta` (id TEXT PRIMARY KEY, content_hash TEXT NOT NULL)"

/**
 * Split the ledger SQL into statements. Every statement is a plain DDL form the
 * idempotent rewriter understands; a non-DDL/insert form would fail loud there.
 */
async function ledgerSchemaStatements(): Promise<string[]> {
  const sql = await fs.readFile(ledgerSchemaPath, "utf8")
  return sql
    .split(";")
    .map((statement) => statement.trim())
    .filter((statement) => statement.length > 0)
}

/**
 * Drive `schema.up` against a recording transaction double. Core's `up` only ever
 * calls `tx.run`, so the effect can run synchronously; a non-string statement is a
 * codegen gap and fails loud rather than emitting corrupted SQL.
 */
function schemaStatements(): string[] {
  const statements: string[] = []
  const tx = {
    run: (statement: unknown) => {
      if (typeof statement !== "string") {
        throw new Error(
          `core schema emitted a non-string statement (${typeof statement}); extend script/migration-statements.ts`,
        )
      }
      statements.push(statement)
      return Effect.void
    },
  }
  Effect.runSync(schema.up(tx as never))
  return statements
}

/**
 * One create/insert statement, split so the identifier is parsed once and can be
 * re-emitted verbatim rather than being matched by a chain of lookahead regexes.
 */
type ParsedStatement =
  | {
      readonly kind: "create"
      readonly indent: string
      readonly verb: string
      readonly identifier: string
      readonly tail: string
    }
  | { readonly kind: "insert"; readonly indent: string; readonly identifier: string; readonly tail: string }

/** A single SQL identifier: backticked, double-quoted, bracketed, or bare. */
const IDENTIFIER = '(?:`[^`]*`|"[^"]*"|\\[[^\\]]*\\]|[^\\s(]+)'

const CREATE_STATEMENT = new RegExp(
  `^(\\s*)CREATE\\s+(TABLE|UNIQUE\\s+INDEX|INDEX)\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?(${IDENTIFIER})(.*)$`,
  "is",
)
const INSERT_STATEMENT = new RegExp(`^(\\s*)INSERT\\s+(?:OR\\s+IGNORE\\s+)?INTO\\s+(${IDENTIFIER})(.*)$`, "is")

function parseStatement(statement: string): ParsedStatement | undefined {
  const create = CREATE_STATEMENT.exec(statement)
  if (create) {
    const [, indent, verb, identifier, tail] = create
    return { kind: "create", indent, verb: verb.toUpperCase().replace(/\s+/g, " "), identifier, tail }
  }
  const insert = INSERT_STATEMENT.exec(statement)
  if (insert) {
    const [, indent, identifier, tail] = insert
    return { kind: "insert", indent, identifier, tail }
  }
  return undefined
}

/**
 * Rewrite one statement into an idempotent form so re-applying the migration to a
 * database the main process already migrated is a safe no-op. The parsed
 * identifier is preserved exactly (quoting included) and a statement that is not
 * a recognized idempotent-able form fails loud: silently emitting non-idempotent
 * SQL would reintroduce the boot crash this list exists to prevent.
 */
function toIdempotent(statement: string): string {
  const parsed = parseStatement(statement)
  if (parsed === undefined) {
    throw new Error(
      `migration statement cannot be made idempotent (extend script/migration-statements.ts): ${JSON.stringify(statement)}`,
    )
  }
  if (parsed.kind === "create")
    return `${parsed.indent}CREATE ${parsed.verb} IF NOT EXISTS ${parsed.identifier}${parsed.tail}`
  return `${parsed.indent}INSERT OR IGNORE INTO ${parsed.identifier}${parsed.tail}`
}

function contentStatements(ledger: readonly string[]): string[] {
  return [...schemaStatements(), ...ledger, SCHEMA_META_STATEMENT]
}

function migrationStatements(ledger: readonly string[]): string[] {
  const statements = [
    ...contentStatements(ledger),
    "CREATE TABLE `migration` (id TEXT PRIMARY KEY, time_completed INTEGER NOT NULL)",
    ...[...migrations.map((migration) => migration.id), LEDGER_MIGRATION_ID].map((id) => {
      return `INSERT INTO \`migration\` (id, time_completed) VALUES ('${id.replaceAll("'", "''")}', ${JOURNAL_TIME})`
    }),
  ]
  return statements.map(toIdempotent)
}

/**
 * The opencode-owned subset the hub applies AFTER core's incremental migration
 * path. Core owns its own schema/DDL; replaying the final schema here would
 * skip real upgrades and `INSERT OR IGNORE`-mark every migration id without
 * running its body. Only the ledger, the identity marker, and the ledger's own
 * journal row belong to this list.
 */
function ledgerOwnedStatements(ledger: readonly string[]): string[] {
  return [
    ...ledger,
    SCHEMA_META_STATEMENT,
    `INSERT INTO \`migration\` (id, time_completed) VALUES ('${LEDGER_MIGRATION_ID}', ${JOURNAL_TIME})`,
  ].map(toIdempotent)
}

/** Escape a statement so it survives verbatim inside a generated template literal. */
function escapeTemplate(statement: string): string {
  return statement.replaceAll("\\", "\\\\").replaceAll("`", "\\`").replaceAll("${", "\\${")
}

function render(statements: readonly string[], ledger: readonly string[]): string {
  const body = statements.map((statement) => `  \`${escapeTemplate(statement)}\`,`).join("\n")
  const ledgerBody = ledger.map((statement) => `  \`${escapeTemplate(statement)}\`,`).join("\n")
  return `// AUTO-GENERATED by script/migration-statements.ts — do not edit by hand.
//
// \`migrationStatements\` is the full current schema (used by tests to build a
// migrated database in one step). \`ledgerStatements\` is the opencode-owned
// subset the hub applies after core's incremental migration path: the ledger,
// the identity marker, and the ledger journal row. The hub must never replay the
// full schema, because that would mark every migration id without running its
// body and skip real upgrades. The lists are frozen so no caller can mutate them.

export const migrationStatements: readonly string[] = Object.freeze([
${body}
])

export const ledgerStatements: readonly string[] = Object.freeze([
${ledgerBody}
])
`
}

async function format(source: string): Promise<string> {
  const prettier = await import("prettier")
  const typescript = await import("prettier/plugins/typescript")
  const estree = await import("prettier/plugins/estree")
  return prettier.format(source, {
    parser: "typescript",
    plugins: [typescript.default, estree.default],
    semi: false,
    printWidth: 120,
  })
}

/** The generated allowlist is derived from the same statements the hub applies. */
interface RegistryColumn {
  readonly name: string
  readonly pk: number
}

interface RegistryForeignKey {
  readonly id: number
  readonly seq: number
  readonly table: string
  readonly from: string
  readonly to: string
}

interface RegistryIndex {
  readonly name: string
  readonly unique: number
  readonly partial: number
}

interface RegistryEntry {
  readonly table: string
  readonly columns: readonly string[]
  readonly scope: string | null
  readonly primaryKey: readonly string[]
  readonly uniqueKeys: readonly (readonly string[])[]
  readonly foreignKeys: readonly { readonly table: string; readonly from: string; readonly to: string }[]
  readonly classification: "worker" | "hub" | "durable"
  readonly readable: boolean
  readonly immutable: readonly string[]
}

function quote(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`
}

/**
 * Structural registry derived from the same in-memory schema the hub applies.
 * SQLite supplies columns/PK/unique/FK; policy supplies classification and the
 * authority-bearing columns that cannot be inferred from DDL.
 */
function schemaRegistry(statements: readonly string[]): RegistryEntry[] {
  const db = new Database(":memory:")
  try {
    for (const statement of statements) db.exec(statement)
    const tables = listSchemaTables({
      all: <T = Record<string, unknown>>(sql: string, params: ReadonlyArray<unknown> = []) =>
        db.query(sql).all(...(params as never[])) as ReadonlyArray<T>,
    })
    const workerWritable = new Set(
      deriveSessionScopedTables({
        all: <T = Record<string, unknown>>(sql: string, params: ReadonlyArray<unknown> = []) =>
          db.query(sql).all(...(params as never[])) as ReadonlyArray<T>,
      }),
    )
    const durable = new Set(DURABLE_LOG_TABLES)
    const readableHub = new Set(WORKER_READABLE_HUB_TABLES)
    const secretHub = new Set(SECRET_HUB_TABLES)

    return tables.map((table) => {
      const columns = db.query(`PRAGMA table_info(${quote(table)})`).all() as RegistryColumn[]
      const primaryKey = columns
        .filter((column) => column.pk > 0)
        .sort((left, right) => left.pk - right.pk)
        .map((column) => column.name)
      const unique = new Map<string, readonly string[]>()
      if (primaryKey.length > 0) unique.set(primaryKey.join(","), primaryKey)
      for (const index of db.query(`PRAGMA index_list(${quote(table)})`).all() as RegistryIndex[]) {
        if (Number(index.unique) !== 1 || Number(index.partial) === 1) continue
        const columnsOfIndex = (db.query(`PRAGMA index_info(${quote(index.name)})`).all() as { name: string }[])
          .map((column) => column.name)
          .filter((name) => name.length > 0)
        if (columnsOfIndex.length > 0) unique.set(columnsOfIndex.join(","), columnsOfIndex)
      }
      const foreignKeys = (db.query(`PRAGMA foreign_key_list(${quote(table)})`).all() as RegistryForeignKey[])
        .sort((left, right) => left.id - right.id || left.seq - right.seq)
        .map((key) => ({ table: key.table, from: key.from, to: key.to }))
      const classification = durable.has(table)
        ? "durable"
        : table === "session" || workerWritable.has(table)
          ? "worker"
          : "hub"
      return {
        table,
        columns: columns.map((column) => column.name),
        scope:
          table === "session"
            ? "id"
            : durable.has(table)
              ? "aggregate_id"
              : workerWritable.has(table)
                ? "session_id"
                : null,
        primaryKey,
        uniqueKeys: [...unique.values()],
        foreignKeys,
        classification,
        readable: classification !== "hub" || (readableHub.has(table) && !secretHub.has(table)),
        immutable: IMMUTABLE_COLUMNS[table] ?? [],
      }
    })
  } finally {
    db.close()
  }
}

function renderRegistry(entries: readonly RegistryEntry[]): string {
  const body = entries
    .map((entry) => {
      return `  {
    table: ${JSON.stringify(entry.table)},
    columns: ${JSON.stringify(entry.columns)},
    scope: ${JSON.stringify(entry.scope)},
    primaryKey: ${JSON.stringify(entry.primaryKey)},
    uniqueKeys: ${JSON.stringify(entry.uniqueKeys)},
    foreignKeys: ${JSON.stringify(entry.foreignKeys)},
    classification: ${JSON.stringify(entry.classification)},
    readable: ${entry.readable},
    immutable: ${JSON.stringify(entry.immutable)},
  },`
    })
    .join("\n")
  return `// AUTO-GENERATED by script/migration-statements.ts — do not edit by hand.
//
// Structural table registry derived from core's \`schema.gen.ts\` statements.
// \`assertTableRegistry\` compares this generated shape with the live schema, so a
// schema change that is not regenerated fails the worker boot gate.

export interface GeneratedTable {
  readonly table: string
  readonly columns: readonly string[]
  readonly scope: string | null
  readonly primaryKey: readonly string[]
  readonly uniqueKeys: readonly (readonly string[])[]
  readonly foreignKeys: readonly {
    readonly table: string
    readonly from: string
    readonly to: string
  }[]
  readonly classification: "worker" | "hub" | "durable"
  readonly readable: boolean
  readonly immutable: readonly string[]
}

export const TABLE_SCHEMA = [
${body}
] as const satisfies readonly GeneratedTable[]
`
}

function renderLedgerFallback(sql: string): string {
  return `// AUTO-GENERATED by script/migration-statements.ts - do not edit by hand.
//
// The hand-written schema lives in \`sql/001-tool-call.sql\`; a compiled entry
// has no adjacent \`.sql\` file, so the bundled binary falls back to this exact
// copy. The emitted string equals the source file byte for byte.

export const LEDGER_SQL_FALLBACK = ${JSON.stringify(sql)}
`
}

function renderAllowlist(input: {
  tables: readonly string[]
  workerWritable: readonly string[]
  bodyHash: string
}): string {
  const tables = input.tables.map((table) => `  ${JSON.stringify(table)},`).join("\n")
  const worker = input.workerWritable.map((table) => `  ${JSON.stringify(table)},`).join("\n")
  const ids = [...migrations.map((migration) => migration.id), LEDGER_MIGRATION_ID]
    .map((id) => `  ${JSON.stringify(id)},`)
    .join("\n")
  return `// AUTO-GENERATED by script/migration-statements.ts — do not edit by hand.
//
// \`SCHEMA_TABLES\` is every table the shipped migration statements create;
// \`WORKER_WRITABLE_TABLES\` is the subset with a \`session_id\` column (D6).
// \`SCHEMA_MIGRATION_IDS\` is the migration journal the hub applies and a worker
// hashes for the schema gate (D7).

export const SCHEMA_VERSION = 1

/** Hash of the shipped SQL bodies; changing a statement changes this value. */
export const SCHEMA_BODY_HASH = ${JSON.stringify(input.bodyHash)}

export const SCHEMA_TABLES: readonly string[] = Object.freeze([
${tables}
])

export const WORKER_WRITABLE_TABLES: readonly string[] = Object.freeze([
${worker}
])

export const SCHEMA_MIGRATION_IDS: readonly string[] = Object.freeze([
${ids}
])

export * as TableAllowlist from "./allowlist.gen"
`
}

const ledgerSource = await fs.readFile(ledgerSchemaPath, "utf8")
const ledger = await ledgerSchemaStatements()
const statements = migrationStatements(ledger)
const renderedStatements = await format(render(statements, ledgerOwnedStatements(ledger)))
const registry = schemaRegistry(statements)
const ownership = {
  tables: registry.map((entry) => entry.table),
  workerWritable: registry
    .filter((entry) => entry.classification === "worker" && entry.table !== "session")
    .map((entry) => entry.table),
}
const renderedAllowlist = await format(
  renderAllowlist({ ...ownership, bodyHash: hashStatements(contentStatements(ledger)) }),
)
const renderedRegistry = await format(renderRegistry(registry))

async function emit(file: string, rendered: string, name: string): Promise<void> {
  if (!check) {
    await fs.writeFile(file, rendered)
    console.log(`Wrote ${path.relative(process.cwd(), file)}`)
    return
  }
  const current = await fs.readFile(file, "utf8").catch(() => "")
  if (current !== rendered) throw new Error(`${name} is stale. Run \`bun script/migration-statements.ts\`.`)
  console.log(`${name} is current`)
}

await emit(output, renderedStatements, "migration.statements.gen.ts")
await emit(allowlistOutput, renderedAllowlist, "allowlist.gen.ts")
await emit(registryOutput, renderedRegistry, "repository/tables.gen.ts")
await emit(ledgerFallbackOutput, await format(renderLedgerFallback(ledgerSource)), "sql/fallback.gen.ts")
