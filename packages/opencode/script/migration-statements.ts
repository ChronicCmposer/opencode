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
import { deriveSessionScopedTables, listSchemaTables, type SqliteQuery } from "../src/session/db/derive"

const output = path.resolve(fileURLToPath(new URL("../src/session/writer/migration.statements.gen.ts", import.meta.url)))
const allowlistOutput = path.resolve(fileURLToPath(new URL("../src/session/db/allowlist.gen.ts", import.meta.url)))
const ledgerSchemaPath = path.resolve(
  fileURLToPath(new URL("../src/session/ledger/sql/001-tool-call.sql", import.meta.url)),
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
  | { readonly kind: "create"; readonly indent: string; readonly verb: string; readonly identifier: string; readonly tail: string }
  | { readonly kind: "insert"; readonly indent: string; readonly identifier: string; readonly tail: string }

/** A single SQL identifier: backticked, double-quoted, bracketed, or bare. */
const IDENTIFIER = "(?:`[^`]*`|\"[^\"]*\"|\\[[^\\]]*\\]|[^\\s(]+)"

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
  if (parsed.kind === "create") return `${parsed.indent}CREATE ${parsed.verb} IF NOT EXISTS ${parsed.identifier}${parsed.tail}`
  return `${parsed.indent}INSERT OR IGNORE INTO ${parsed.identifier}${parsed.tail}`
}

function migrationStatements(ledger: readonly string[]): string[] {
  const statements = [
    ...schemaStatements(),
    ...ledger,
    "CREATE TABLE `migration` (id TEXT PRIMARY KEY, time_completed INTEGER NOT NULL)",
    ...[...migrations.map((migration) => migration.id), LEDGER_MIGRATION_ID].map((id) => {
      return `INSERT INTO \`migration\` (id, time_completed) VALUES ('${id.replaceAll("'", "''")}', ${JOURNAL_TIME})`
    }),
  ]
  return statements.map(toIdempotent)
}

/** Escape a statement so it survives verbatim inside a generated template literal. */
function escapeTemplate(statement: string): string {
  return statement.replaceAll("\\", "\\\\").replaceAll("`", "\\`").replaceAll("${", "\\${")
}

function render(statements: readonly string[]): string {
  const body = statements.map((statement) => `  \`${escapeTemplate(statement)}\`,`).join("\n")
  return `// AUTO-GENERATED by script/migration-statements.ts — do not edit by hand.
//
// The hub applies these statements on its own connection with zero live workers;
// workers never run DDL. They reproduce \`DatabaseMigration.apply\` on an empty
// database plus the opencode-owned ledger schema, followed by the migration
// journal. The list is frozen so no caller can mutate it.

export const migrationStatements: readonly string[] = Object.freeze([
${body}
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
function schemaOwnership(statements: readonly string[]): { tables: string[]; workerWritable: string[] } {
  const db = new Database(":memory:")
  try {
    for (const statement of statements) db.exec(statement)
    const query: SqliteQuery = {
      all: <T = Record<string, unknown>>(sql: string, params: ReadonlyArray<unknown> = []) =>
        db.query(sql).all(...(params as never[])) as ReadonlyArray<T>,
    }
    return {
      tables: [...listSchemaTables(query)],
      workerWritable: [...deriveSessionScopedTables(query)],
    }
  } finally {
    db.close()
  }
}

function renderAllowlist(input: { tables: readonly string[]; workerWritable: readonly string[] }): string {
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

export const SCHEMA_TABLES: readonly string[] = Object.freeze([
${tables}
])

export const WORKER_WRITABLE_TABLES: readonly string[] = Object.freeze([
${worker}
])

export const SCHEMA_MIGRATION_IDS: readonly string[] = Object.freeze([
${ids}
])
`
}

const ledger = await ledgerSchemaStatements()
const statements = migrationStatements(ledger)
const renderedStatements = await format(render(statements))
const renderedAllowlist = await format(renderAllowlist(schemaOwnership(statements)))

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
