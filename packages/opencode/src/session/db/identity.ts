// Schema identity hashing (D7).
//
// The identity is `user_version` + the ordered migration journal ids + a hash of
// the actual shipped SQL bodies. Hashing ids alone let a changed statement keep
// the same identity, so the body hash is part of every comparison. The body hash
// is persisted in `schema_meta` by the hub migration; a worker never writes it.

import { createHash } from "node:crypto"

export interface SchemaIdentityParts {
  readonly version: number
  readonly ids: readonly string[]
  readonly bodyHash: string
}

/** Stable hash of the ordered SQL bodies the hub ships. */
export function hashStatements(statements: readonly string[]): string {
  const hash = createHash("sha256")
  for (const statement of statements) hash.update(statement).update("\n")
  return hash.digest("hex")
}

/** Stable identity hash over version, body content, and the journal id set. */
export function hashIdentity(parts: SchemaIdentityParts): string {
  const hash = createHash("sha256")
  hash.update(`version:${parts.version}\n`)
  hash.update(`body:${parts.bodyHash}\n`)
  for (const id of [...parts.ids].sort()) hash.update(`id:${id}\n`)
  return hash.digest("hex")
}

/** The read surface the persisted body hash needs. */
export interface IdentityQuery {
  all<T = Record<string, unknown>>(sql: string, params?: ReadonlyArray<unknown>): ReadonlyArray<T>
}

/** The hub-persisted body hash; absent on an unmigrated database. */
export function readPersistedBodyHash(connection: IdentityQuery): string {
  try {
    const rows = connection.all<{ readonly content_hash: string }>(
      "SELECT content_hash FROM schema_meta WHERE id = 'schema'",
    )
    return rows[0]?.content_hash ?? ""
  } catch {
    return ""
  }
}

/** Idempotent persisted-body-hash write, used by the hub marker. */
export function writePersistedBodyHash(connection: { exec(sql: string): void }, bodyHash: string): void {
  connection.exec(
    `INSERT OR REPLACE INTO schema_meta (id, content_hash) VALUES ('schema', '${bodyHash.replaceAll("'", "''")}')`,
  )
}

export * as SchemaIdentity from "./identity"
