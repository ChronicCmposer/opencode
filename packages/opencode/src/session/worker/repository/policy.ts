// Worker repository policy (semantic classification).
//
// The structural registry in `tables.gen.ts` is generated from core's
// `schema.gen.ts`. This module owns the few semantic decisions SQLite cannot
// express: which tables the durable log owns, which hub tables a worker may
// read, which carry secrets, and which columns are authority-bearing.

/** Durable log tables written by the Bus/EventV2 path, never by scoped DML. */
export const DURABLE_LOG_TABLES: readonly string[] = Object.freeze(["event", "event_blob", "event_sequence"])

/**
 * Non-secret hub tables a bound worker may read. Everything else hub-owned —
 * notably `credential`, `account`, `control_account`, and `account_state` — is
 * denied even for `SELECT`.
 */
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
 * Hub-owned tables the registry still classifies as worker-writable because the
 * generated structure predates the authority split. The share row is minted and
 * rotated by the hub, so the write boundary refuses every worker mutation while
 * leaving the worker's own-session row readable.
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
 * Session columns a worker projection may UPDATE. The rest of the session row is
 * hub authority: billing (`cost`, `tokens_*`), share (`share_url`), and revert
 * (`revert`) state are written by the hub/projector, never by a repository
 * caller. Identity columns above are already immutable, and `metadata.depth` is
 * checked separately through `HUB_OWNED_METADATA`.
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

/**
 * Hub-owned session columns an update would change. `undefined` means the column
 * is not written at all (`UPDATE ... SET` ignores undefined values in both the
 * projector and the repository), so it can never be a violation. A repeated
 * current value is likewise allowed: only an actual change outside
 * `SESSION_WORKER_UPDATE_COLUMNS` is rejected.
 */
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

/**
 * JSON columns whose keys are hub-owned. A worker may preserve the current value
 * but may not change it; `metadata.depth` drives spawn headroom and is stamped by
 * the hub.
 */
export const HUB_OWNED_METADATA: Readonly<Record<string, readonly string[]>> = Object.freeze({
  session: Object.freeze(["depth"]),
})

/** The session column storing `HUB_OWNED_METADATA`. */
export const SESSION_METADATA_COLUMN = "metadata"
