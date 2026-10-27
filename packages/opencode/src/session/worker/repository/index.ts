// Typed repository boundary.
//
// The public factory returns only typed methods; the raw connection and the
// `withMain` bracket live in the private write capability.

export {
  ForeignKeyOwnershipError,
  HubOnlyWriteError,
  ImmutableColumnError,
  ScopeViolationError,
  TableRegistryDriftError,
  TransactionScopeError,
  UnknownColumnError,
  UnsupportedWriteError,
} from "./errors"
export { createWriteCapability, type WriteCapability } from "./capability"
export {
  TABLE_REGISTRY,
  assertTableRegistry,
  tableRegistry,
  type TableForeignKey,
  type TableRegistryEntry,
} from "./registry"
export { TABLE_SCHEMA, type GeneratedTable } from "./tables.gen"
export type {
  ColumnOf,
  DurableTable,
  ReadableTable,
  RepositoryBatch,
  RepositoryConflict,
  RepositoryInsert,
  RepositoryRow,
  RepositoryUpsert,
  RepositoryValues,
  TableName,
  WorkerRepository,
  WritableTable,
} from "./types"
export type { DurableBlobRow, DurableEventRow, DurablePrimitives, DurableSequenceRow } from "./durable"
export { makeRepository } from "./write"

import { createWriteCapability, type RepositoryConnection } from "./capability"
import { makeRepository } from "./write"
import type { WorkerRepository } from "./types"

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
