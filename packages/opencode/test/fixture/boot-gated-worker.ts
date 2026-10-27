// Transport fixture: a real Bun worker whose executor boot is deliberately
// delayed, so a hub can cancel it during materialization. It announces the
// start of its boot delay, waits, then installs the executor. Any work that
// actually runs writes a marker file, so a cancelled boot can prove no run
// executed.

import { writeFileSync } from "node:fs"
import { Database } from "bun:sqlite"
import { parentPort, workerData } from "node:worker_threads"
import { Effect } from "effect"
import { installSessionExecutor, type SessionWorkFactory } from "@/session/worker/execute"
import { threadId } from "node:worker_threads"
import { scope } from "./worker-scope"

interface BootGatedData {
  readonly bootDelayMs?: number
  readonly markerPath?: string
  /** Open a real WAL database before `ready` and hold a write transaction. */
  readonly dbPath?: string
  /** Written once the WAL handle and write lock are held. */
  readonly dbReadyPath?: string
}

const data = (workerData ?? {}) as BootGatedData

parentPort?.postMessage({ type: "booting" })

// A production worker opens its `bun:sqlite` WAL handle before it announces
// `ready`; hold an open write transaction so a boot cancel that skips the
// orderly close strands the lock observably.
let database: Database | undefined
let closed = false
const closeResources = (): void => {
  if (closed) return
  closed = true
  try {
    database?.close()
  } catch {
    // The handle may already be unusable; the lock is released either way.
  }
}
const onShutdown = (event: { readonly data: unknown }): void => {
  const frame = event.data as { readonly type?: unknown } | undefined
  if (frame?.type !== "shutdown") return
  closeResources()
  scope.postMessage({ type: "shutdown-ack", id: 0 })
}
;(scope as unknown as { addEventListener?: (type: string, listener: (event: { data: unknown }) => void) => void })
  .addEventListener?.("message", onShutdown)

if (data.dbPath !== undefined) {
  database = new Database(data.dbPath, { create: true })
  database.exec("PRAGMA journal_mode = WAL")
  database.exec("PRAGMA busy_timeout = 0")
  database.exec("CREATE TABLE IF NOT EXISTS boot_probe (id INTEGER PRIMARY KEY)")
  database.exec("BEGIN IMMEDIATE")
  database.exec("INSERT OR REPLACE INTO boot_probe (id) VALUES (1)")
  if (data.dbReadyPath !== undefined) writeFileSync(data.dbReadyPath, "ready")
}

await new Promise<void>((resolve) => setTimeout(resolve, data.bootDelayMs ?? 60_000))

// A shutdown during the boot delay wins: never install (or announce) an
// executor for an isolate already being closed.
if (!closed) {
  const work: SessionWorkFactory = () => (sessionID) =>
    Effect.sync(() => {
      if (data.markerPath !== undefined) writeFileSync(data.markerPath, "executed")
      return { sessionID }
    })

  installSessionExecutor(scope, { work, threadId })
}
