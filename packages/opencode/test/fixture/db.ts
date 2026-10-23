import { rm } from "fs/promises"
import { Database } from "@opencode-ai/core/database/database"
import { disposeAllInstances } from "./fixture"
import { assertIsolatedDatabasePath } from "./isolation"

export async function resetDatabase() {
  const dbPath = Database.path()
  if (dbPath === ":memory:") return
  await assertIsolatedDatabasePath(dbPath)
  await disposeAllInstances().catch(() => undefined)
  await rm(dbPath, { force: true }).catch(() => undefined)
  await rm(`${dbPath}-wal`, { force: true }).catch(() => undefined)
  await rm(`${dbPath}-shm`, { force: true }).catch(() => undefined)
}
