#!/usr/bin/env bun
//
// Generate `src/session/writer/migration.statements.gen.ts` (Fix C).
//
// The writer isolate owns the sole SQLite connection but cannot import application
// code across the worker port: it applies plain SQL strings through
// `WriterCore.migrate` (`ReadonlyArray<string>`). Core defines its schema as
// `up(tx)` Effect functions and deletes the intermediate `.sql`, so this script
// flattens the schema into the frozen `migrationStatements` list the hub ships
// with `writer.migrate(migrationStatements)`.
//
// The emitted list reproduces `DatabaseMigration.apply` on an empty database:
//   1. every `schema.gen.ts` `up` statement (the full current schema),
//   2. the `migration` journal table,
//   3. one journal seed row per core migration (its timestamp is never read).
//
// Every statement is emitted in an idempotent form (`IF NOT EXISTS` / `INSERT OR
// IGNORE`). The writer opens the SAME database file the main process already
// migrated, so re-applying the schema must be a safe no-op rather than
// `table workspace already exists`.
//
// Run from `packages/opencode`: `bun script/migration-statements.ts`.
// `--check` verifies the committed file is current without writing it.

import { Effect } from "effect"
import fs from "node:fs/promises"
import path from "node:path"
import { fileURLToPath } from "node:url"
import schema from "@opencode-ai/core/database/schema.gen"
import { migrations } from "@opencode-ai/core/database/migration.gen"

const output = path.resolve(fileURLToPath(new URL("../src/session/writer/migration.statements.gen.ts", import.meta.url)))
const check = process.argv.includes("--check")

/** Journal timestamp: the value is bookkeeping only, so a constant keeps the file deterministic. */
const JOURNAL_TIME = 0

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

function migrationStatements(): string[] {
  const statements = [
    ...schemaStatements(),
    "CREATE TABLE `migration` (id TEXT PRIMARY KEY, time_completed INTEGER NOT NULL)",
    ...migrations.map((migration) => {
      const id = migration.id.replaceAll("'", "''")
      return `INSERT INTO \`migration\` (id, time_completed) VALUES ('${id}', ${JOURNAL_TIME})`
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
// The writer isolate's SQLite database has no schema of its own; the hub migrates
// it by shipping these statements over the worker port with \`writer.migrate(...)\`.
// They reproduce \`DatabaseMigration.apply\` on an empty database: the core schema
// followed by the migration journal. The list is frozen so no caller can mutate it.

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

const rendered = await format(render(migrationStatements()))

if (check) {
  const current = await fs.readFile(output, "utf8").catch(() => "")
  if (current !== rendered) {
    throw new Error("migration.statements.gen.ts is stale. Run `bun script/migration-statements.ts`.")
  }
  console.log("migration.statements.gen.ts is current")
} else {
  await fs.writeFile(output, rendered)
  console.log(`Wrote ${path.relative(process.cwd(), output)}`)
}
