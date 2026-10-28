// Hard-close matrix for the DB executor and the worker bootstrap.
//
// A deadline or forced teardown must not strand a native SQLite handle: the
// handle owns the WAL write lock, and `Worker.terminate()` alone does not run
// finalizers. These tests prove the executor asks the thread to close its handle
// before terminating, and that a bootstrap shutdown frame closes the worker's
// WAL even while a write transaction is open.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test"
import { Database } from "bun:sqlite"
import { DbExecutorTimeoutError, createDbExecutor } from "@/session/worker/db-executor"
import { REAL_WORKER_TEST_TIMEOUT_MS } from "../lib/worker-timeouts"
import { pollUntil } from "../lib/async"

setDefaultTimeout(REAL_WORKER_TEST_TIMEOUT_MS)

const directories: string[] = []

function workspace(): { filename: string } {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-worker-hard-close-"))
  directories.push(directory)
  return { filename: path.join(directory, "opencode.db") }
}

afterEach(() => {
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("db executor deadline hard-close", () => {
  test("a timed-out write releases the WAL lock before the thread is retired", async () => {
    const { filename } = workspace()
    const executor = createDbExecutor({ filename, timeoutMs: 50 })
    try {
      await executor.run("CREATE TABLE item (id INTEGER PRIMARY KEY)")
      // One statement that holds the write lock for ~1s; the 50ms deadline
      // retires the executor while it is mid-write.
      const slow =
        "INSERT INTO item SELECT x FROM (WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 2000000) SELECT x FROM c)"
      await expect(executor.run(slow)).rejects.toBeInstanceOf(DbExecutorTimeoutError)
    } finally {
      await executor.close()
    }

    // The isolate is gone; its native handle must be closed, so a new handle can
    // take the write lock without waiting for a process exit.
    const probe = new Database(filename)
    try {
      probe.exec("PRAGMA busy_timeout = 0")
      await pollUntil(
        () => {
          try {
            probe.exec("BEGIN IMMEDIATE")
            probe.exec("ROLLBACK")
            return true
          } catch {
            return false
          }
        },
        { message: "the timed-out executor never released the WAL write lock", timeoutMs: 5_000 },
      )
    } finally {
      probe.close()
    }
  })
})
