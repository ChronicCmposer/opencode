// Typed repository surface.
//
// Table and column names come from the generated registry, so a typo is a
// compile error at the call site and an `UnknownColumnError` at runtime (for
// untyped callers). Values stay `unknown` because SQLite stores JSON/text
// scalars and the repository validates shape, not application decoding.

import type { WriteOutcome } from "@opencode-ai/core/database/connection"
import type { TABLE_SCHEMA } from "./tables.gen"

type GeneratedEntry = (typeof TABLE_SCHEMA)[number]

export type TableName = GeneratedEntry["table"]
export type WritableTable = Extract<GeneratedEntry, { readonly classification: "worker" }>["table"]
export type DurableTable = Extract<GeneratedEntry, { readonly classification: "durable" }>["table"]
export type ReadableTable = Extract<GeneratedEntry, { readonly readable: true }>["table"]

export type ColumnOf<T extends TableName> = Extract<GeneratedEntry, { readonly table: T }>["columns"][number]
export type RepositoryValues<T extends TableName> = Partial<Record<ColumnOf<T>, unknown>>
export type RepositoryRow<T extends TableName> = Partial<Record<ColumnOf<T>, unknown>>

export interface RepositoryConflict<T extends WritableTable = WritableTable> {
  readonly columns: readonly ColumnOf<T>[]
  /**
   * Columns to update on conflict. An array carries the insert's `excluded`
   * values; a record carries explicit values for the same validation pipeline as
   * `update` (including immutable/depth checks).
   */
  readonly update?: readonly ColumnOf<T>[] | RepositoryValues<T>
}

export interface RepositoryInsert<T extends WritableTable = WritableTable> {
  readonly table: T
  readonly values: RepositoryValues<T>
  readonly onConflict?: RepositoryConflict<T>
}

export interface RepositoryUpsert<T extends WritableTable = WritableTable> extends RepositoryInsert<T> {
  readonly onConflict: RepositoryConflict<T>
}

export interface RepositoryBatch<T extends WritableTable = WritableTable> {
  readonly table: T
  readonly values: readonly RepositoryValues<T>[]
}

/**
 * The public typed repository. There is deliberately no `run`, `all`, `exec`,
 * `query`, `connection`, or transaction handle: every statement is generated
 * from the registry and every write binds the worker's session.
 */
export interface WorkerRepository {
  insert<T extends WritableTable>(table: T, values: RepositoryValues<T>): WriteOutcome
  insert<T extends WritableTable>(input: RepositoryInsert<T>): WriteOutcome
  insertMany<T extends WritableTable>(table: T, rows: readonly RepositoryValues<T>[]): WriteOutcome
  insertMany<T extends WritableTable>(input: RepositoryBatch<T>): WriteOutcome
  upsert<T extends WritableTable>(
    table: T,
    values: RepositoryValues<T>,
    onConflict: RepositoryConflict<T>,
  ): WriteOutcome
  upsert<T extends WritableTable>(input: RepositoryUpsert<T>): WriteOutcome
  update<T extends WritableTable>(table: T, values: RepositoryValues<T>, where: RepositoryValues<T>): WriteOutcome
  delete<T extends WritableTable>(table: T, where: RepositoryValues<T>): WriteOutcome
  select<T extends ReadableTable>(table: T, where?: RepositoryValues<T>): readonly RepositoryRow<T>[]
  /** A savepoint-scoped callback; joins an already-open outer transaction. */
  atomic<T>(operation: () => T): T
}

export * as RepositoryTypes from "./types"
