// Typed write engine.
//
// Batches and writes that validate against the stored row (immutable fields,
// hub-owned metadata) run in one savepoint, so validation and the write are
// atomic and a nested call joins the caller's open transaction. Other
// single-statement writes skip the savepoint; see `insertOne`.

import type { WriteOutcome } from "@opencode-ai/core/database/connection"
import { createWriteCapability, type RepositoryConnection, type WriteCapability } from "./capability"
import {
  ForeignKeyOwnershipError,
  HubOnlyWriteError,
  ImmutableColumnError,
  ScopeViolationError,
  TransactionScopeError,
  UnknownColumnError,
  UnsupportedWriteError,
} from "./errors"
import {
  changedHubOwnedSessionColumns,
  HUB_ONLY_WRITE_TABLES,
  HUB_OWNED_METADATA,
  SESSION_METADATA_COLUMN,
} from "./policy"
import { tableRegistry, type TableRegistryEntry } from "./registry"
import type {
  ReadableTable,
  RepositoryConflict,
  RepositoryInsert,
  RepositoryRow,
  RepositoryUpsert,
  RepositoryValues,
  WorkerRepository,
  WritableTable,
} from "./types"

type Row = Record<string, unknown>

function quote(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

function sameSqlValue(left: unknown, right: unknown): boolean {
  if (left === right) return true
  if (left === null || left === undefined || right === null || right === undefined) return false
  return String(left) === String(right)
}

/** A plain object (not a Date/Map/class instance) is the only JSON object shape. */
function isJsonObject(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false
  const prototype = Object.getPrototypeOf(value)
  return prototype === Object.prototype || prototype === null
}

/** JSON omits undefined/function/symbol object members. */
function jsonMember(value: unknown): unknown {
  return value === undefined || typeof value === "function" || typeof value === "symbol" ? undefined : value
}

/** JSON turns undefined/function/symbol array items into null. */
function jsonElement(value: unknown): unknown {
  const member = jsonMember(value)
  return member === undefined ? null : member
}

/** JSON serializes non-finite numbers as null and collapses -0 to 0. */
function jsonNumber(value: number): number | null {
  return Number.isFinite(value) ? (value === 0 ? 0 : value) : null
}

/**
 * Compare one written JSON field with JSON semantics, without serializing it.
 * Metadata values are usually scalars (`depth`), so the scalar path avoids a
 * full `JSON.stringify` per row. Object key order is irrelevant; array order is
 * not. `undefined` object members are omitted, array holes/undefined become
 * `null`, non-finite numbers serialize as `null`, and `-0` equals `0`. Only
 * plain objects/arrays compare structurally: a Date/Map is never equal to an
 * empty object merely because both expose no enumerable keys.
 */
export function sameJsonValue(left: unknown, right: unknown): boolean {
  const normalizedLeft = typeof left === "number" ? jsonNumber(left) : left
  const normalizedRight = typeof right === "number" ? jsonNumber(right) : right
  if (normalizedLeft === normalizedRight) return true
  if (normalizedLeft === null || normalizedRight === null) return false
  if (typeof normalizedLeft !== typeof normalizedRight) return false
  if (typeof normalizedLeft !== "object") return false
  if (Array.isArray(normalizedLeft) || Array.isArray(normalizedRight)) {
    if (!Array.isArray(normalizedLeft) || !Array.isArray(normalizedRight)) return false
    if (normalizedLeft.length !== normalizedRight.length) return false
    for (let index = 0; index < normalizedLeft.length; index += 1) {
      if (!sameJsonValue(jsonElement(normalizedLeft[index]), jsonElement(normalizedRight[index]))) return false
    }
    return true
  }
  if (!isJsonObject(normalizedLeft) || !isJsonObject(normalizedRight)) return false
  const leftKeys = Object.keys(normalizedLeft).filter((key) => jsonMember(normalizedLeft[key]) !== undefined)
  const rightKeys = Object.keys(normalizedRight).filter((key) => jsonMember(normalizedRight[key]) !== undefined)
  return (
    leftKeys.length === rightKeys.length &&
    leftKeys.every(
      (key) =>
        Object.hasOwn(normalizedRight, key) &&
        jsonMember(normalizedRight[key]) !== undefined &&
        sameJsonValue(normalizedLeft[key], normalizedRight[key]),
    )
  )
}

function parseJsonObject(value: unknown): Record<string, unknown> | undefined {
  if (isPlainObject(value)) return value
  if (typeof value !== "string") return undefined
  try {
    const parsed: unknown = JSON.parse(value)
    return isPlainObject(parsed) ? parsed : undefined
  } catch {
    return undefined
  }
}

function columnsOf(entry: TableRegistryEntry, values: Row): string[] {
  const columns = Object.keys(values)
  for (const column of columns) {
    if (!entry.columns.includes(column)) throw new UnknownColumnError({ table: entry.table, column })
  }
  return columns
}

function predicates(where: Row): { readonly sql: string; readonly params: unknown[] } {
  const columns = Object.keys(where)
  if (columns.length === 0) return { sql: "", params: [] }
  return {
    sql: ` WHERE ${columns.map((column) => `${quote(column)} = ?`).join(" AND ")}`,
    params: columns.map((column) => where[column]),
  }
}

function boundScope(capability: WriteCapability, entry: TableRegistryEntry): string | undefined {
  return capability.sessionID === undefined || entry.scope === null ? undefined : capability.sessionID
}

/** The one authorizer bracket around a statement this engine itself emits. */
function runOnMain<T>(capability: WriteCapability, operation: () => T): T {
  return capability.withMain(operation)
}

const TRANSACTION_KEYWORDS: ReadonlySet<string> = new Set([
  "begin",
  "commit",
  "end",
  "release",
  "rollback",
  "savepoint",
])

/** Skip a quoted string/identifier whose opening quote is at `index`. */
function skipQuoted(sql: string, index: number, quote: string): number {
  let next = index + 1
  while (next < sql.length) {
    if (sql[next] === quote) {
      if (sql[next + 1] === quote) {
        next += 2
        continue
      }
      return next + 1
    }
    next += 1
  }
  return next
}

function skipBracket(sql: string, index: number): number {
  const end = sql.indexOf("]", index + 1)
  return end === -1 ? sql.length : end + 1
}

function skipLineComment(sql: string, index: number): number {
  const end = sql.indexOf("\n", index + 2)
  return end === -1 ? sql.length : end + 1
}

function skipBlockComment(sql: string, index: number): number {
  const end = sql.indexOf("*/", index + 2)
  return end === -1 ? sql.length : end + 2
}

/** Skip whitespace, comments, and the opening parentheses used as a statement prefix. */
function skipLeadingTrivia(sql: string): number {
  let index = 0
  while (index < sql.length) {
    const char = sql[index]!
    if (/\s/u.test(char)) {
      index += 1
      continue
    }
    if (char === "-" && sql[index + 1] === "-") {
      index = skipLineComment(sql, index)
      continue
    }
    if (char === "/" && sql[index + 1] === "*") {
      index = skipBlockComment(sql, index)
      continue
    }
    if (char === "(") {
      index += 1
      continue
    }
    break
  }
  return index
}

interface CteCursor {
  index: number
}

function skipCteTrivia(sql: string, cursor: CteCursor): void {
  while (cursor.index < sql.length) {
    const char = sql[cursor.index]!
    if (/\s/u.test(char)) {
      cursor.index += 1
      continue
    }
    if (char === "-" && sql[cursor.index + 1] === "-") {
      cursor.index = skipLineComment(sql, cursor.index)
      continue
    }
    if (char === "/" && sql[cursor.index + 1] === "*") {
      cursor.index = skipBlockComment(sql, cursor.index)
      continue
    }
    break
  }
}

function readCteWord(sql: string, cursor: CteCursor): string | undefined {
  skipCteTrivia(sql, cursor)
  const match = /^[A-Za-z_][A-Za-z0-9_]*/.exec(sql.slice(cursor.index))
  if (match === null) return undefined
  cursor.index += match[0].length
  return match[0].toLowerCase()
}

function readCteName(sql: string, cursor: CteCursor): boolean {
  skipCteTrivia(sql, cursor)
  const char = sql[cursor.index]
  if (char === '"' || char === "`") {
    cursor.index = skipQuoted(sql, cursor.index, char)
    return true
  }
  if (char === "[") {
    cursor.index = skipBracket(sql, cursor.index)
    return true
  }
  return readCteWord(sql, cursor) !== undefined
}

/** Skip one balanced parenthesis group, including strings/comments/quoted names. */
function skipCteParens(sql: string, cursor: CteCursor): boolean {
  skipCteTrivia(sql, cursor)
  if (sql[cursor.index] !== "(") return false
  let depth = 0
  while (cursor.index < sql.length) {
    const char = sql[cursor.index]!
    if (char === "'" || char === '"' || char === "`") {
      cursor.index = skipQuoted(sql, cursor.index, char)
      continue
    }
    if (char === "[") {
      cursor.index = skipBracket(sql, cursor.index)
      continue
    }
    if (char === "-" && sql[cursor.index + 1] === "-") {
      cursor.index = skipLineComment(sql, cursor.index)
      continue
    }
    if (char === "/" && sql[cursor.index + 1] === "*") {
      cursor.index = skipBlockComment(sql, cursor.index)
      continue
    }
    if (char === "(") {
      depth += 1
    } else if (char === ")") {
      depth -= 1
      cursor.index += 1
      if (depth === 0) return true
      continue
    }
    cursor.index += 1
  }
  return false
}

/**
 * Keyword of the statement after a `WITH` CTE list. SQLite CTE bodies are
 * parenthesized and the main statement follows the final body, so a small
 * grammar walk is both conservative and exact. Anything unparseable returns
 * undefined and the caller keeps `with` (a read).
 */
function cteStatementKeyword(sql: string): string | undefined {
  const cursor: CteCursor = { index: 0 }
  if (readCteWord(sql, cursor) !== "with") return undefined
  const afterWith = cursor.index
  if (readCteWord(sql, cursor) !== "recursive") cursor.index = afterWith
  while (true) {
    if (!readCteName(sql, cursor)) return undefined
    skipCteTrivia(sql, cursor)
    if (sql[cursor.index] === "(" && !skipCteParens(sql, cursor)) return undefined
    if (readCteWord(sql, cursor) !== "as") return undefined
    const afterAs = cursor.index
    const hint = readCteWord(sql, cursor)
    if (hint === "not") {
      if (readCteWord(sql, cursor) !== "materialized") return undefined
    } else if (hint !== "materialized") {
      cursor.index = afterAs
    }
    if (!skipCteParens(sql, cursor)) return undefined
    skipCteTrivia(sql, cursor)
    if (sql[cursor.index] !== ",") break
    cursor.index += 1
  }
  return readCteWord(sql, cursor)
}

/**
 * Classify a SQL statement by its leading keyword, ignoring leading whitespace,
 * comments, and opening parentheses. Lowercase; empty when there is no word.
 *
 * A `WITH` statement is classified by its top-level statement after the CTE
 * bodies: `WITH … INSERT/UPDATE/DELETE/REPLACE` is a write, while `WITH … SELECT`
 * stays a read. This is the one lexical classifier the worker uses, so the
 * statement router (`db.ts`) and the transaction fence cannot disagree about
 * what a statement does.
 */
export function leadingKeyword(sql: string): string {
  const start = skipLeadingTrivia(sql)
  const keyword = /^[A-Za-z_][A-Za-z0-9_]*/.exec(sql.slice(start))?.[0]?.toLowerCase() ?? ""
  return keyword === "with" ? (cteStatementKeyword(sql) ?? keyword) : keyword
}

function assertNoTransactionControl(sql: string): void {
  const keyword = leadingKeyword(sql)
  if (TRANSACTION_KEYWORDS.has(keyword)) {
    throw new TransactionScopeError({ keyword })
  }
}

interface TransactionFence {
  depth: number
  readonly original: RepositoryConnection
}

/**
 * Transaction control belongs to the savepoint engine, never to an `atomic`
 * callback. While a callback runs, every raw statement method on the shared
 * connection rejects COMMIT/ROLLBACK/etc before SQLite sees it, so a callback
 * cannot commit its write past the savepoint or roll back the caller's outer
 * transaction. Calls are synchronous, so the fence is a re-entrant bracket
 * (nested atomics share it) restored on the outermost exit.
 */
const transactionFences = new WeakMap<RepositoryConnection, TransactionFence>()

function installTransactionFence(connection: RepositoryConnection): TransactionFence {
  const existing = transactionFences.get(connection)
  if (existing !== undefined) {
    existing.depth += 1
    return existing
  }
  const original: RepositoryConnection = {
    all: connection.all,
    run: connection.run,
    exec: connection.exec,
    values: connection.values,
  }
  const fence: TransactionFence = { depth: 1, original }
  transactionFences.set(connection, fence)
  connection.all = ((sql: string, params?: unknown) => {
    assertNoTransactionControl(sql)
    return original.all(sql, params as never)
  }) as unknown as RepositoryConnection["all"]
  connection.run = ((sql: string, params?: unknown) => {
    assertNoTransactionControl(sql)
    return original.run(sql, params as never)
  }) as unknown as RepositoryConnection["run"]
  connection.exec = ((sql: string) => {
    assertNoTransactionControl(sql)
    original.exec(sql)
  }) as unknown as RepositoryConnection["exec"]
  connection.values = ((sql: string, params?: unknown) => {
    assertNoTransactionControl(sql)
    return original.values(sql, params as never)
  }) as unknown as RepositoryConnection["values"]
  return fence
}

function releaseTransactionFence(connection: RepositoryConnection, fence: TransactionFence): void {
  fence.depth -= 1
  if (fence.depth > 0) return
  connection.all = fence.original.all
  connection.run = fence.original.run
  connection.exec = fence.original.exec
  connection.values = fence.original.values
  transactionFences.delete(connection)
}

/**
 * Bind the session on insert. A caller may repeat the bound value but may never
 * supply a different one: the column is authoritative, not advisory.
 */
function insertValues(capability: WriteCapability, entry: TableRegistryEntry, input: Row, operation: string): Row {
  const values: Row = { ...input }
  columnsOf(entry, values)
  const scope = boundScope(capability, entry)
  if (scope !== undefined && entry.scope !== null) {
    const supplied = values[entry.scope]
    if (supplied !== undefined && supplied !== null && !sameSqlValue(supplied, scope)) {
      throw new ScopeViolationError({
        table: entry.table,
        sessionID: scope,
        reason: `${operation} another session's row`,
      })
    }
    values[entry.scope] = scope
  }
  return values
}

/** Bind the session predicate on update/delete/read, refusing an explicit foreign target. */
function scopedWhere(capability: WriteCapability, entry: TableRegistryEntry, input: Row): Row {
  const where: Row = { ...input }
  columnsOf(entry, where)
  const scope = boundScope(capability, entry)
  if (scope !== undefined && entry.scope !== null) {
    const supplied = where[entry.scope]
    if (supplied !== undefined && supplied !== null && !sameSqlValue(supplied, scope)) {
      throw new ScopeViolationError({
        table: entry.table,
        sessionID: scope,
        reason: "target another session's row",
      })
    }
    where[entry.scope] = scope
  }
  return where
}

function writeEntry(table: string, operation: string): TableRegistryEntry {
  const entry = tableRegistry(table)
  if (entry === undefined) {
    throw new UnsupportedWriteError({ operation, table, reason: "unknown table" })
  }
  if (entry.classification === "hub" || HUB_ONLY_WRITE_TABLES.includes(entry.table)) {
    throw new HubOnlyWriteError({ operation, table })
  }
  if (entry.classification === "durable") {
    throw new UnsupportedWriteError({
      operation,
      table,
      reason: "the durable log is written only through DurablePrimitives",
    })
  }
  return entry
}

function readEntry(table: string): TableRegistryEntry {
  const entry = tableRegistry(table)
  if (entry === undefined) throw new UnsupportedWriteError({ operation: "read", table, reason: "unknown table" })
  if (entry.classification === "hub" && !entry.readable) {
    throw new UnsupportedWriteError({ operation: "read", table, reason: "hub-owned table is not worker-readable" })
  }
  return entry
}

function all(entry: TableRegistryEntry, capability: WriteCapability, where: Row): Row[] {
  const predicate = predicates(where)
  const target = entry.classification === "hub" ? `main.${quote(entry.table)}` : quote(entry.table)
  return capability.withMain(() => [
    ...capability.connection.all<Row>(
      `SELECT ${entry.columns.map(quote).join(", ")} FROM ${target}${predicate.sql}`,
      predicate.params,
    ),
  ])
}

function assertForeignKeyOwnership(capability: WriteCapability, entry: TableRegistryEntry, values: Row): void {
  const sessionID = capability.sessionID
  if (sessionID === undefined) return
  for (const key of entry.foreignKeys) {
    if (!Object.hasOwn(values, key.from)) continue
    const value = values[key.from]
    if (value === null || value === undefined) continue
    const parent = tableRegistry(key.table)
    if (parent === undefined || parent.scope === null) continue
    const found = capability.withMain(() =>
      capability.connection.all<Row>(
        `SELECT ${quote(parent.scope!)} AS scope FROM main.${quote(parent.table)} WHERE ${quote(key.to)} = ? LIMIT 1`,
        [value],
      ),
    )[0]
    if (found === undefined) {
      throw new ForeignKeyOwnershipError({
        table: entry.table,
        column: key.from,
        parent: parent.table,
        sessionID,
        reason: "parent row is missing",
      })
    }
    if (!sameSqlValue(found.scope, sessionID)) {
      throw new ForeignKeyOwnershipError({
        table: entry.table,
        column: key.from,
        parent: parent.table,
        sessionID,
        reason: "parent row belongs to another session",
      })
    }
  }
}

function fullyPresent(values: Row, key: readonly string[]): boolean {
  return key.every((column) => Object.hasOwn(values, column))
}

function assertUniqueOwnership(capability: WriteCapability, entry: TableRegistryEntry, values: Row): void {
  const sessionID = capability.sessionID
  if (sessionID === undefined || entry.scope === null) return
  for (const key of entry.uniqueKeys) {
    if (!fullyPresent(values, key)) continue
    const predicate = key.map((column) => `${quote(column)} = ?`).join(" AND ")
    const params = key.map((column) => values[column])
    const found = capability.withMain(() =>
      capability.connection.all<Row>(
        `SELECT ${quote(entry.scope!)} AS scope FROM main.${quote(entry.table)} WHERE ${predicate} LIMIT 1`,
        params,
      ),
    )[0]
    if (found !== undefined && !sameSqlValue(found.scope, sessionID)) {
      throw new ScopeViolationError({
        table: entry.table,
        sessionID,
        reason: "claim another session's unique key",
      })
    }
  }
}

function assertImmutable(
  capability: WriteCapability,
  entry: TableRegistryEntry,
  values: Row,
  current: readonly Row[],
): void {
  const sessionID = capability.sessionID ?? "unbound"
  const immutable = entry.immutable.filter((column) => Object.hasOwn(values, column))
  const ownedMetadata = HUB_OWNED_METADATA[entry.table]
  const metadata =
    ownedMetadata !== undefined && Object.hasOwn(values, SESSION_METADATA_COLUMN) ? ownedMetadata : undefined

  if (immutable.length === 0 && metadata === undefined) return
  if (current.length === 0) return

  for (const row of current) {
    for (const column of immutable) {
      if (!sameSqlValue(row[column], values[column])) {
        throw new ImmutableColumnError({
          table: entry.table,
          column,
          sessionID,
          reason: "session identity is hub authority",
        })
      }
    }
    if (metadata === undefined) continue
    const before = parseJsonObject(row[SESSION_METADATA_COLUMN])
    const after = parseJsonObject(values[SESSION_METADATA_COLUMN])
    for (const key of metadata) {
      if (!sameJsonValue(before?.[key], after?.[key])) {
        throw new ImmutableColumnError({
          table: entry.table,
          column: `${SESSION_METADATA_COLUMN}.${key}`,
          sessionID,
          reason: "hub-owned metadata key",
        })
      }
    }
  }
}

/**
 * Session rows carry hub-owned billing/share/revert state in addition to the
 * immutable identity columns. A repository update may repeat an unchanged
 * identity value (the projector does), but it may only CHANGE a worker-owned
 * projection column; everything else is hub authority.
 */
function assertSessionUpdateColumns(
  capability: WriteCapability,
  entry: TableRegistryEntry,
  values: Row,
  current: readonly Row[],
): void {
  if (entry.table !== "session") return
  const sessionID = capability.sessionID ?? "unbound"
  const offending = changedHubOwnedSessionColumns(values, (column, value) =>
    current.length > 0 ? current.every((row) => sameSqlValue(row[column], value)) : false,
  )
  const column = offending[0]
  if (column === undefined) return
  throw new ImmutableColumnError({
    table: entry.table,
    column,
    sessionID,
    reason: "column is hub-owned",
  })
}

function assertAssignmentScope(
  capability: WriteCapability,
  entry: TableRegistryEntry,
  values: Row,
  reason: string,
): void {
  const scope = boundScope(capability, entry)
  if (scope === undefined || entry.scope === null) return
  const supplied = values[entry.scope]
  if (supplied !== undefined && supplied !== null && !sameSqlValue(supplied, scope)) {
    throw new ScopeViolationError({ table: entry.table, sessionID: scope, reason })
  }
}

function rejectImmutableConflict(capability: WriteCapability, entry: TableRegistryEntry, values: Row): void {
  const sessionID = capability.sessionID ?? "unbound"
  const immutable = entry.immutable.find((column) => Object.hasOwn(values, column))
  if (immutable === undefined) return
  throw new ImmutableColumnError({
    table: entry.table,
    column: immutable,
    sessionID,
    reason: "an upsert may not rewrite session identity",
  })
}

function conflictValues(
  entry: TableRegistryEntry,
  inserted: Row,
  conflict: RepositoryConflict | undefined,
): { readonly values: Row; readonly array: readonly string[] | undefined } {
  if (conflict === undefined || conflict.update === undefined) return { values: {}, array: undefined }
  if (Array.isArray(conflict.update)) {
    const columns = [...conflict.update]
    columnsOf(entry, Object.fromEntries(columns.map((column) => [column, inserted[column]])))
    return {
      values: Object.fromEntries(columns.map((column) => [column, inserted[column]])),
      array: columns,
    }
  }
  const values: Row = { ...conflict.update }
  columnsOf(entry, values)
  return { values, array: undefined }
}

function assertConflictKey(entry: TableRegistryEntry, conflict: RepositoryConflict, inserted: Row): void {
  const columns = [...conflict.columns]
  if (columns.length === 0) {
    throw new UnsupportedWriteError({ operation: "upsert", table: entry.table, reason: "conflict needs a key" })
  }
  columnsOf(entry, Object.fromEntries(columns.map((column) => [column, inserted[column]])))
  if (!fullyPresent(inserted, columns)) {
    throw new UnsupportedWriteError({
      operation: "upsert",
      table: entry.table,
      reason: "conflict key columns must all be bound",
    })
  }
  const matches = entry.uniqueKeys.some(
    (key) => key.length === columns.length && key.every((column) => (columns as readonly string[]).includes(column)),
  )
  if (!matches) {
    throw new UnsupportedWriteError({
      operation: "upsert",
      table: entry.table,
      reason: `conflict key [${columns.join(", ")}] is not a unique key`,
    })
  }
}

function conflictRow(
  capability: WriteCapability,
  entry: TableRegistryEntry,
  conflict: RepositoryConflict,
  inserted: Row,
): Row | undefined {
  const predicate = conflict.columns.map((column) => `${quote(column)} = ?`).join(" AND ")
  const params = conflict.columns.map((column) => inserted[column])
  const row = capability.withMain(() =>
    capability.connection.all<Row>(
      `SELECT ${entry.columns.map(quote).join(", ")} FROM main.${quote(entry.table)} WHERE ${predicate} LIMIT 1`,
      params,
    ),
  )[0]
  const sessionID = capability.sessionID
  if (row !== undefined && sessionID !== undefined && entry.scope !== null) {
    if (!sameSqlValue(row[entry.scope], sessionID)) {
      throw new ScopeViolationError({ table: entry.table, sessionID, reason: "upsert another session's row" })
    }
  }
  return row
}

function currentRows(capability: WriteCapability, entry: TableRegistryEntry, where: Row): readonly Row[] {
  return all(entry, capability, where)
}

/**
 * The ownership half of an upsert's conflict write. Validation reads the target
 * row first, but the row can change between that read and the statement; the
 * `DO UPDATE ... WHERE` predicate re-checks ownership inside SQLite's write so a
 * vanished or foreign row can never be repointed.
 */
function conflictOwnershipWhere(
  capability: WriteCapability,
  entry: TableRegistryEntry,
): { readonly sql: string; readonly params: unknown[] } {
  const scope = boundScope(capability, entry)
  if (scope === undefined || entry.scope === null) return { sql: "", params: [] }
  return { sql: ` WHERE ${quote(entry.scope)} = ?`, params: [scope] }
}

function insertStatement(
  capability: WriteCapability,
  entry: TableRegistryEntry,
  values: Row,
  conflict: RepositoryConflict | undefined,
): {
  readonly sql: string
  readonly params: unknown[]
} {
  const columns = Object.keys(values)
  if (columns.length === 0) {
    throw new UnsupportedWriteError({
      operation: "insert",
      table: entry.table,
      reason: "insert needs at least one column",
    })
  }
  const params = columns.map((column) => values[column])
  let sql = `INSERT INTO main.${quote(entry.table)} (${columns.map(quote).join(", ")}) VALUES (${columns
    .map(() => "?")
    .join(", ")})`
  if (conflict === undefined) return { sql, params }

  const conflictColumns = conflict.columns.map(quote).join(", ")
  if (conflict.update === undefined || (Array.isArray(conflict.update) && conflict.update.length === 0)) {
    return { sql: `${sql} ON CONFLICT (${conflictColumns}) DO NOTHING`, params }
  }
  const where = conflictOwnershipWhere(capability, entry)
  if (Array.isArray(conflict.update)) {
    const assignments = conflict.update.map((column) => `${quote(column)} = excluded.${quote(column)}`).join(", ")
    return {
      sql: `${sql} ON CONFLICT (${conflictColumns}) DO UPDATE SET ${assignments}${where.sql}`,
      params: [...params, ...where.params],
    }
  }
  const assignments = Object.entries(conflict.update)
  for (const [, value] of assignments) params.push(value)
  return {
    sql: `${sql} ON CONFLICT (${conflictColumns}) DO UPDATE SET ${assignments
      .map(([column]) => `${quote(column)} = ?`)
      .join(", ")}${where.sql}`,
    params: [...params, ...where.params],
  }
}

/**
 * The session root is update-only. A worker may project onto an existing root,
 * but the statement can never INSERT: if the hub removed the row, the UPDATE
 * matches nothing instead of re-minting it with the caller's depth/parent.
 */
function sessionUpsertStatement(
  capability: WriteCapability,
  entry: TableRegistryEntry,
  values: Row,
  conflict: RepositoryConflict,
): { readonly sql: string; readonly params: unknown[] } | undefined {
  if (conflict.update === undefined || (Array.isArray(conflict.update) && conflict.update.length === 0)) {
    return undefined
  }
  const where: Row = Object.fromEntries(conflict.columns.map((column) => [column, values[column]]))
  const scope = boundScope(capability, entry)
  if (scope !== undefined && entry.scope !== null) where[entry.scope] = scope
  const update = conflictValues(entry, values, conflict).values
  return updateStatement(entry, update, where)
}

function updateStatement(
  entry: TableRegistryEntry,
  values: Row,
  where: Row,
): { readonly sql: string; readonly params: unknown[] } {
  const columns = Object.keys(values)
  if (columns.length === 0) {
    throw new UnsupportedWriteError({
      operation: "update",
      table: entry.table,
      reason: "update needs at least one assignment",
    })
  }
  const params = columns.map((column) => values[column])
  const predicate = predicates(where)
  params.push(...predicate.params)
  return {
    sql: `UPDATE main.${quote(entry.table)} SET ${columns.map((column) => `${quote(column)} = ?`).join(", ")}${predicate.sql}`,
    params,
  }
}

function deleteStatement(entry: TableRegistryEntry, where: Row): { readonly sql: string; readonly params: unknown[] } {
  const predicate = predicates(where)
  return { sql: `DELETE FROM main.${quote(entry.table)}${predicate.sql}`, params: predicate.params }
}

let savepointCounter = 0

/**
 * Run validation+write atomically; nested calls join the caller's transaction.
 * The main-table authorizer bracket is applied per internal statement, never
 * around the caller-supplied callback: a callback keeps the bound read fence.
 */
function atomic<T>(capability: WriteCapability, work: () => T): T {
  const fence = installTransactionFence(capability.connection)
  const exec = fence.original.exec
  try {
    savepointCounter += 1
    const savepoint = `worker_repository_${savepointCounter}`
    exec(`SAVEPOINT ${savepoint}`)
    try {
      const result = work()
      exec(`RELEASE ${savepoint}`)
      return result
    } catch (error) {
      try {
        exec(`ROLLBACK TO ${savepoint}`)
        exec(`RELEASE ${savepoint}`)
      } catch {
        // Preserve the original failure; a savepoint cleanup fault is secondary.
      }
      throw error
    }
  } finally {
    releaseTransactionFence(capability.connection, fence)
  }
}

/**
 * Validation that compares assigned values against the stored row (immutable
 * columns, session-column rules) must see the same row the write changes.
 */
function validatesAgainstStoredRow(entry: TableRegistryEntry): boolean {
  return entry.immutable.length > 0 || entry.table === "session" || HUB_OWNED_METADATA[entry.table] !== undefined
}

/**
 * Single-statement writes are already atomic in SQLite, and validation only
 * reads, so the per-write SAVEPOINT/RELEASE round trip is pure overhead for
 * plain inserts and for updates and upserts on tables without immutable or
 * session columns. Updates and upserts that validate against the stored row run
 * inside the savepoint so the check and the write share one snapshot. Batches
 * and caller-supplied `atomic` callbacks keep the savepoint.
 *
 * For the remaining tables, validation (scope/ownership reads) and the write are
 * NOT snapshot-atomic against a concurrent hub writer. The SQLite statement
 * itself is atomic, and an upsert's ownership predicate is re-checked inside the
 * write, so a vanished/foreign row cannot be repointed; a caller that needs a
 * read-write snapshot must wrap the call in `atomic`/its own transaction.
 */
function insertOne(
  capability: WriteCapability,
  entry: TableRegistryEntry,
  input: Row,
  conflict: RepositoryConflict | undefined,
): WriteOutcome {
  const values = insertValues(capability, entry, input, "insert")
  // The session root is hub authority. A worker projects data onto an
  // existing row (update/upsert), but it may never mint the root itself: a
  // forged row could reset metadata.depth/parent_id and buy spawn headroom.
  if (entry.table === "session" && conflict === undefined) {
    throw new UnsupportedWriteError({
      operation: "insert",
      table: entry.table,
      reason: "the session root is created by the hub",
    })
  }
  assertForeignKeyOwnership(capability, entry, values)
  assertUniqueOwnership(capability, entry, values)
  if (conflict !== undefined) {
    assertConflictKey(entry, conflict, values)
    const existing = conflictRow(capability, entry, conflict, values)
    if (entry.table === "session" && existing === undefined) {
      throw new UnsupportedWriteError({
        operation: "upsert",
        table: entry.table,
        reason: "the session root is created by the hub",
      })
    }
    if (existing !== undefined) {
      const update = conflictValues(entry, values, conflict)
      rejectImmutableConflict(capability, entry, update.values)
      assertAssignmentScope(capability, entry, update.values, "move a row out of scope")
      assertImmutable(capability, entry, update.values, [existing])
      assertSessionUpdateColumns(capability, entry, update.values, [existing])
      assertForeignKeyOwnership(capability, entry, update.values)
      assertUniqueOwnership(capability, entry, { ...existing, ...update.values })
    }
    if (entry.table === "session") {
      const statement = sessionUpsertStatement(capability, entry, values, conflict)
      if (statement === undefined) return { changes: 0, lastInsertRowid: 0 }
      return runOnMain(capability, () => capability.connection.run(statement.sql, statement.params))
    }
  }
  const statement = insertStatement(capability, entry, values, conflict)
  return runOnMain(capability, () => capability.connection.run(statement.sql, statement.params))
}

function normalizeInsert<T extends WritableTable>(
  first: T | RepositoryInsert<T>,
  second?: RepositoryValues<T>,
): { readonly table: T; readonly values: RepositoryValues<T>; readonly conflict?: RepositoryConflict<T> } {
  if (typeof first === "string") {
    return { table: first, values: second ?? {} }
  }
  return {
    table: first.table,
    values: first.values,
    ...(first.onConflict === undefined ? {} : { conflict: first.onConflict }),
  }
}

function normalizeUpsert<T extends WritableTable>(
  first: T | RepositoryUpsert<T>,
  second?: RepositoryValues<T>,
  third?: RepositoryConflict<T>,
): { readonly table: T; readonly values: RepositoryValues<T>; readonly conflict: RepositoryConflict<T> } {
  if (typeof first === "string") {
    if (third === undefined) {
      throw new UnsupportedWriteError({ operation: "upsert", table: first, reason: "upsert needs a conflict target" })
    }
    return { table: first, values: second ?? {}, conflict: third }
  }
  return { table: first.table, values: first.values, conflict: first.onConflict }
}

export function makeRepository(capability: WriteCapability): WorkerRepository {
  return {
    insert(first: any, second?: any) {
      const input = normalizeInsert(first as never, second as never)
      const entry = writeEntry(input.table, "insert")
      const conflict = input.conflict as RepositoryConflict | undefined
      const write = () => insertOne(capability, entry, input.values as Row, conflict)
      return conflict !== undefined && validatesAgainstStoredRow(entry) ? atomic(capability, write) : write()
    },
    insertMany(first: any, second?: any) {
      const input =
        typeof first === "string"
          ? { table: first, rows: second ?? [] }
          : (first as unknown as {
              readonly table: string
              readonly values?: readonly Row[]
              readonly rows?: readonly Row[]
            })
      const rows = "values" in input ? (input.values ?? []) : (input.rows ?? [])
      const entry = writeEntry(input.table, "insertMany")
      if (rows.length === 0) {
        throw new UnsupportedWriteError({ operation: "insertMany", table: entry.table, reason: "batch is empty" })
      }
      // A batch bypasses `insertOne`; it must not become a minting path for the
      // hub-owned session root.
      if (entry.table === "session") {
        throw new UnsupportedWriteError({
          operation: "insertMany",
          table: entry.table,
          reason: "the session root is created by the hub",
        })
      }
      return atomic(capability, () => {
        let changes = 0
        let lastInsertRowid = 0
        for (const row of rows) {
          const values = insertValues(capability, entry, row, "insertMany")
          assertForeignKeyOwnership(capability, entry, values)
          assertUniqueOwnership(capability, entry, values)
          const statement = insertStatement(capability, entry, values, undefined)
          const outcome = runOnMain(capability, () => capability.connection.run(statement.sql, statement.params))
          changes += outcome.changes
          lastInsertRowid = outcome.lastInsertRowid
        }
        return { changes, lastInsertRowid }
      })
    },
    upsert(first: any, second?: any, third?: any) {
      const input = normalizeUpsert(first as never, second as never, third as never)
      const entry = writeEntry(input.table, "upsert")
      const write = () => insertOne(capability, entry, input.values as Row, input.conflict as unknown as RepositoryConflict)
      return validatesAgainstStoredRow(entry) ? atomic(capability, write) : write()
    },
    update(table: any, values: any, where: any) {
      const entry = writeEntry(table, "update")
      const assignments: Row = { ...(values as Row) }
      columnsOf(entry, assignments)
      const write = () => {
        const predicate = scopedWhere(capability, entry, where as Row)
        const current = currentRows(capability, entry, predicate)
        assertImmutable(capability, entry, assignments, current)
        assertSessionUpdateColumns(capability, entry, assignments, current)
        assertAssignmentScope(capability, entry, assignments, "move a row out of scope")
        assertForeignKeyOwnership(capability, entry, assignments)
        assertUniqueOwnership(capability, entry, assignments)
        const statement = updateStatement(entry, assignments, predicate)
        return runOnMain(capability, () => capability.connection.run(statement.sql, statement.params))
      }
      return validatesAgainstStoredRow(entry) ? atomic(capability, write) : write()
    },
    delete(table: any, where: any) {
      const entry = writeEntry(table, "delete")
      // The session root is hub authority: a worker projects data onto the row
      // but may never remove it (which would allow a forged re-insert to reset
      // metadata.depth/parent_id and misclassify the next spawn).
      if (entry.table === "session") {
        throw new UnsupportedWriteError({
          operation: "delete",
          table: entry.table,
          reason: "the session root may only be removed by the hub",
        })
      }
      const predicate = scopedWhere(capability, entry, where as Row)
      const statement = deleteStatement(entry, predicate)
      return runOnMain(capability, () => capability.connection.run(statement.sql, statement.params))
    },
    atomic(operation: any) {
      return atomic(capability, operation)
    },
    select(table: any, where?: any) {
      const entry = readEntry(table)
      const predicate = scopedWhere(capability, entry, (where ?? {}) as Row)
      const target = entry.classification === "hub" ? `main.${quote(entry.table)}` : quote(entry.table)
      const statement = predicates(predicate)
      return capability.withMain(() =>
        capability.connection.all<RepositoryRow<ReadableTable>>(
          `SELECT ${entry.columns.map(quote).join(", ")} FROM ${target}${statement.sql}`,
          statement.params,
        ),
      ) as readonly RepositoryRow<ReadableTable>[]
    },
  } as unknown as WorkerRepository
}

/** A typed repository over an unbound connection (ledger-only DBs and tests). */
export function connectedRepository(connection: RepositoryConnection): WorkerRepository {
  return makeRepository(
    createWriteCapability({
      connection,
      scopeFunction: "worker_scope_session",
      withMain: (operation) => operation(),
    }),
  )
}

export * as RepositoryWrite from "./write"
