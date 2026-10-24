import { rm } from "fs/promises"
import { Database } from "@opencode-ai/core/database/database"
import { disposeAllInstances } from "./fixture"
import { assertIsolatedDatabasePath } from "./isolation"

/** The main database file plus the SQLite WAL/SHM sidecars it may leave behind. */
function databaseFiles(dbPath: string): readonly string[] {
  return [dbPath, `${dbPath}-wal`, `${dbPath}-shm`]
}

/**
 * Reset the isolated test database, disposing live instances first so their
 * SQLite handles release before the files are removed. Failures are collected
 * rather than swallowed: a broken teardown must surface loudly instead of
 * silently leaking a database handle into the next test.
 */
export async function resetDatabase(): Promise<void> {
  const dbPath = Database.path()
  if (dbPath === ":memory:") return
  await assertIsolatedDatabasePath(dbPath)

  const failures: unknown[] = []
  await disposeAllInstances().catch((error) => failures.push(error))
  for (const file of databaseFiles(dbPath)) {
    await rm(file, { force: true }).catch((error) => failures.push(error))
  }

  if (failures.length === 1) throw failures[0]
  if (failures.length > 1) throw new AggregateError(failures, `failed to reset database at ${dbPath}`)
}
