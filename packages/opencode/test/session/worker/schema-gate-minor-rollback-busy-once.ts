// Fault injection for the executor's in-place ROLLBACK retry: the first
// `ROLLBACK` reports SQLITE_BUSY while the real worker transaction path still
// runs. The retry must re-issue ROLLBACK and succeed without poisoning.
import { Database } from "bun:sqlite"

const originalExec = Database.prototype.exec
let busyRollbacks = 1

Database.prototype.exec = function execWithOneBusyRollback(...args: Parameters<typeof originalExec>) {
  const sql = args[0]
  if (busyRollbacks > 0 && /^\s*rollback\b/i.test(sql)) {
    busyRollbacks -= 1
    const error = new Error("SQLITE_BUSY: database is locked")
    error.name = "SQLiteError"
    Object.assign(error, { code: "SQLITE_BUSY", errno: 5 })
    throw error
  }
  return originalExec.apply(this, args)
}

await import("../../../src/session/worker/db-executor")
