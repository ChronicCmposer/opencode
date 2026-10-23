import { realpath } from "fs/promises"
import os from "os"
import path from "path"

/** True only when `target` resolves inside the OS temp directory. */
export async function isInsideTemp(target: string): Promise<boolean> {
  const root = await realpath(os.tmpdir()).catch(() => path.resolve(os.tmpdir()))
  const resolved = path.resolve(target)
  const rel = path.relative(root, resolved)
  return rel !== "" && !rel.startsWith("..") && !path.isAbsolute(rel)
}

/**
 * Refuse to operate on any database that is not isolated to the temp dir.
 * `:memory:` is always safe (nothing on disk).
 */
export async function assertIsolatedDatabasePath(dbPath: string): Promise<void> {
  if (dbPath === ":memory:") return
  if (await isInsideTemp(dbPath)) return
  throw new Error(
    `refusing to operate on a non-temp database: ${dbPath}\n` +
      `Tests must never touch the host database. Check OPENCODE_DB / XDG_DATA_HOME leakage.`,
  )
}
