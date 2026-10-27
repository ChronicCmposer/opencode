// Runtime view over the generated table registry.
//
// The registry is generated from core's `schema.gen.ts`; this module validates
// the generated shape against the live schema so a schema change that is not
// regenerated fails the worker boot gate instead of silently widening writes.

import type { SqliteQuery } from "@/session/db/derive"
import { TableRegistryDriftError } from "./errors"
import { TABLE_SCHEMA } from "./tables.gen"

export interface TableForeignKey {
  readonly table: string
  readonly from: string
  readonly to: string
}

export interface TableRegistryEntry {
  readonly table: string
  readonly columns: readonly string[]
  readonly scope: string | null
  readonly primaryKey: readonly string[]
  readonly uniqueKeys: readonly (readonly string[])[]
  readonly foreignKeys: readonly TableForeignKey[]
  readonly classification: "worker" | "hub" | "durable"
  readonly readable: boolean
  readonly immutable: readonly string[]
}

export const TABLE_REGISTRY: readonly TableRegistryEntry[] = Object.freeze(TABLE_SCHEMA.map((entry) => entry))

const BY_NAME = new Map<string, TableRegistryEntry>(TABLE_REGISTRY.map((entry) => [entry.table, entry]))

export function tableRegistry(table: string): TableRegistryEntry | undefined {
  return BY_NAME.get(table)
}

function tableInfo(connection: SqliteQuery, table: string): readonly string[] {
  return connection
    .all<{ readonly name: string }>(`PRAGMA main.table_info("${table.replaceAll('"', '""')}")`)
    .map((row) => row.name)
}

function tablePrimaryKey(connection: SqliteQuery, table: string): readonly string[] {
  return connection
    .all<{ readonly name: string; readonly pk: number }>(`PRAGMA main.table_info("${table.replaceAll('"', '""')}")`)
    .filter((row) => Number(row.pk) > 0)
    .sort((left, right) => Number(left.pk) - Number(right.pk))
    .map((row) => row.name)
}

function tableUniqueKeys(
  connection: SqliteQuery,
  table: string,
  primaryKey: readonly string[],
): readonly (readonly string[])[] {
  const unique = new Map<string, readonly string[]>()
  if (primaryKey.length > 0) unique.set(primaryKey.join(","), primaryKey)
  const quoted = `"${table.replaceAll('"', '""')}"`
  for (const index of connection.all<{ readonly name: string; readonly unique: number; readonly partial: number }>(
    `PRAGMA main.index_list(${quoted})`,
  )) {
    if (Number(index.unique) !== 1 || Number(index.partial) === 1) continue
    const columns = connection
      .all<{ readonly name: string }>(`PRAGMA main.index_info("${index.name.replaceAll('"', '""')}")`)
      .map((row) => row.name)
      .filter((name) => name.length > 0)
    if (columns.length > 0) unique.set(columns.join(","), columns)
  }
  return [...unique.values()]
}

function tableForeignKeys(connection: SqliteQuery, table: string): readonly TableForeignKey[] {
  return [
    ...connection.all<{
      readonly id: number
      readonly seq: number
      readonly table: string
      readonly from: string
      readonly to: string
    }>(`PRAGMA main.foreign_key_list("${table.replaceAll('"', '""')}")`),
  ]
    .sort((left, right) => Number(left.id) - Number(right.id) || Number(left.seq) - Number(right.seq))
    .map((row) => ({ table: row.table, from: row.from, to: row.to }))
}

function same(left: unknown, right: unknown): boolean {
  return JSON.stringify(left) === JSON.stringify(right)
}

function drift(table: string, expected: unknown, observed: unknown): never {
  throw new TableRegistryDriftError({
    table,
    expected: JSON.stringify(expected),
    observed: JSON.stringify(observed),
  })
}

interface ObservedTable {
  readonly table: string
  readonly columns: readonly string[]
  readonly primaryKey: readonly string[]
  readonly uniqueKeys: readonly (readonly string[])[]
  readonly foreignKeys: readonly TableForeignKey[]
}

function compare(table: string, expected: TableRegistryEntry, observed: ObservedTable): void {
  if (!same(expected.columns, observed.columns)) drift(table, expected.columns, observed.columns)
  if (!same(expected.primaryKey, observed.primaryKey)) drift(table, expected.primaryKey, observed.primaryKey)
  if (!same(expected.uniqueKeys, observed.uniqueKeys)) drift(table, expected.uniqueKeys, observed.uniqueKeys)
  if (!same(expected.foreignKeys, observed.foreignKeys)) drift(table, expected.foreignKeys, observed.foreignKeys)
}

/** Compare the generated registry with the live schema (columns/PK/unique/FK). */
export function assertTableRegistry(connection: SqliteQuery): void {
  const tables = connection
    .all<{
      readonly name: string
    }>("SELECT name FROM main.sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
    .map((row) => row.name)
  const generated = new Set(TABLE_REGISTRY.map((entry) => entry.table))
  const observed = new Set(tables)
  if (!same([...generated].sort(), [...observed].sort())) {
    drift("*", [...generated].sort(), [...observed].sort())
  }
  for (const entry of TABLE_REGISTRY) {
    const primaryKey = tablePrimaryKey(connection, entry.table)
    compare(entry.table, entry, {
      table: entry.table,
      columns: tableInfo(connection, entry.table),
      primaryKey,
      uniqueKeys: tableUniqueKeys(connection, entry.table, primaryKey),
      foreignKeys: tableForeignKeys(connection, entry.table),
    })
  }
}

export * as TableRegistry from "./registry"
