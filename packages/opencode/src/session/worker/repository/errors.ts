// Typed repository errors.

import { Schema } from "effect"

/** A table/row shape the typed repository does not expose for writing. */
export class UnsupportedWriteError extends Schema.TaggedErrorClass<UnsupportedWriteError>()(
  "WorkerRepository.UnsupportedWriteError",
  {
    operation: Schema.String,
    table: Schema.String,
    reason: Schema.String,
  },
) {
  override get message() {
    return `worker repository cannot ${this.operation} table "${this.table}": ${this.reason}`
  }
}

/** A column is not part of the generated table definition. */
export class UnknownColumnError extends Schema.TaggedErrorClass<UnknownColumnError>()(
  "WorkerRepository.UnknownColumnError",
  {
    table: Schema.String,
    column: Schema.String,
  },
) {
  override get message() {
    return `unknown column "${this.column}" on table "${this.table}"`
  }
}

/** A hub-only (or secret) table was targeted by a worker write. */
export class HubOnlyWriteError extends Schema.TaggedErrorClass<HubOnlyWriteError>()(
  "WorkerRepository.HubOnlyWriteError",
  {
    operation: Schema.String,
    table: Schema.String,
  },
) {
  override get message() {
    return `worker may not ${this.operation} hub-owned table "${this.table}"`
  }
}

/** A bound write tried to read, move, or claim a row outside its session/aggregate. */
export class ScopeViolationError extends Schema.TaggedErrorClass<ScopeViolationError>()(
  "WorkerRepository.ScopeViolationError",
  {
    table: Schema.String,
    sessionID: Schema.String,
    reason: Schema.String,
  },
) {
  override get message() {
    return `worker for "${this.sessionID}" may not ${this.reason} on table "${this.table}"`
  }
}

/** An authority-bearing session column would change. */
export class ImmutableColumnError extends Schema.TaggedErrorClass<ImmutableColumnError>()(
  "WorkerRepository.ImmutableColumnError",
  {
    table: Schema.String,
    column: Schema.String,
    sessionID: Schema.String,
    reason: Schema.String,
  },
) {
  override get message() {
    return `worker may not change ${this.table}.${this.column}: ${this.reason}`
  }
}

/** A callback tried to issue transaction control inside a repository savepoint. */
export class TransactionScopeError extends Schema.TaggedErrorClass<TransactionScopeError>()(
  "WorkerRepository.TransactionScopeError",
  {
    keyword: Schema.String,
  },
) {
  override get message() {
    return `worker repository does not expose transaction control: "${this.keyword}" is repository-scoped`
  }
}

/** A repository transaction issued more statements than its budget allows. */
export class StatementBudgetExceededError extends Schema.TaggedErrorClass<StatementBudgetExceededError>()(
  "WorkerRepository.StatementBudgetExceededError",
  {
    limit: Schema.Number,
  },
) {
  override get message() {
    return `worker repository transaction exceeded its statement budget of ${this.limit}`
  }
}

/** A foreign-key parent is absent or owned by another session. */
export class ForeignKeyOwnershipError extends Schema.TaggedErrorClass<ForeignKeyOwnershipError>()(
  "WorkerRepository.ForeignKeyOwnershipError",
  {
    table: Schema.String,
    column: Schema.String,
    parent: Schema.String,
    sessionID: Schema.String,
    reason: Schema.String,
  },
) {
  override get message() {
    return `foreign key ${this.table}.${this.column} -> ${this.parent}: ${this.reason}`
  }
}

/** The generated table registry no longer matches the live schema. */
export class TableRegistryDriftError extends Schema.TaggedErrorClass<TableRegistryDriftError>()(
  "WorkerRepository.TableRegistryDriftError",
  {
    table: Schema.String,
    expected: Schema.String,
    observed: Schema.String,
  },
) {
  override get message() {
    return `table registry drift on "${this.table}": expected ${this.expected}, observed ${this.observed}`
  }
}

export * as RepositoryErrors from "./errors"
