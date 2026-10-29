// Fault injection for ROLLBACK exhaustion: every ROLLBACK reports SQLITE_BUSY,
// so the bounded retry budget is spent and the worker must poison the handle
// and surface the typed rollback error.
import { Database } from "bun:sqlite"

const originalExec = Database.prototype.exec

Database.prototype.exec = function execWithBusyRollback(...args: Parameters<typeof originalExec>) {
  const sql = args[0]
  if (/^\s*rollback\b/i.test(sql)) {
    const error = new Error("SQLITE_BUSY: database is locked")
    error.name = "SQLiteError"
    Object.assign(error, { code: "SQLITE_BUSY", errno: 5 })
    throw error
  }
  return originalExec.apply(this, args)
}

await import("../../../src/session/worker/db-executor")
