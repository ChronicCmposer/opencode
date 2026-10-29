// Worker repository policy (semantic classification).

/** Durable log tables written by the DurableLog/EventV2 path, never by scoped DML. */
export const DURABLE_LOG_TABLES: readonly string[] = Object.freeze(["event", "event_blob", "event_sequence"])

/** Non-secret hub tables a bound worker may read. */
export const WORKER_READABLE_HUB_TABLES: readonly string[] = Object.freeze([
  "data_migration",
  "migration",
  "permission",
  "project",
  "project_directory",
  "schema_meta",
  "workspace",
])

/** Hub-owned tables whose rows contain authentication or credential material. */
export const SECRET_HUB_TABLES: readonly string[] = Object.freeze([
  "account",
  "account_state",
  "control_account",
  "credential",
])

/**
 * Hub-owned tables the registry still classifies as worker-writable because the generated structure predates
 * the authority split.
 */
export const HUB_ONLY_WRITE_TABLES: readonly string[] = Object.freeze(["session_share"])

/** Session columns a worker projection may never rewrite; identity is hub authority. */
export const SESSION_IMMUTABLE_COLUMNS: readonly string[] = Object.freeze([
  "directory",
  "id",
  "parent_id",
  "permission",
  "project_id",
  "slug",
  "workspace_id",
])

/**
 * Session columns a worker projection may UPDATE. The rest of the session row is hub authority: billing
 * (`cost`, `tokens_*`), share (`share_url`), and revert (`revert`) state are written by the hub/projector,
 * never by a repository caller.
 */
export const SESSION_WORKER_UPDATE_COLUMNS: readonly string[] = Object.freeze([
  "agent",
  "metadata",
  "model",
  "summary_additions",
  "summary_deletions",
  "summary_diffs",
  "summary_files",
  "time_archived",
  "time_compacting",
  "time_updated",
  "title",
])

/** Hub-owned session columns an update would change. */
export function changedHubOwnedSessionColumns(
  values: Readonly<Record<string, unknown>>,
  isUnchanged: (column: string, value: unknown) => boolean,
): readonly string[] {
  return Object.keys(values).filter(
    (column) =>
      values[column] !== undefined &&
      !SESSION_WORKER_UPDATE_COLUMNS.includes(column) &&
      !isUnchanged(column, values[column]),
  )
}

/** Per-table immutable columns. Only the session root carries authority state today. */
export const IMMUTABLE_COLUMNS: Readonly<Record<string, readonly string[]>> = Object.freeze({
  session: SESSION_IMMUTABLE_COLUMNS,
})

/** JSON columns whose keys are hub-owned. */
export const HUB_OWNED_METADATA: Readonly<Record<string, readonly string[]>> = Object.freeze({
  session: Object.freeze(["depth"]),
})

/** The session column storing `HUB_OWNED_METADATA`. */
export const SESSION_METADATA_COLUMN = "metadata"

export * as RepositoryPolicy from "./policy"
