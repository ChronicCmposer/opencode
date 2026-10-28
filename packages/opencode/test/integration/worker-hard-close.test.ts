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
import { pathToFileURL } from "node:url"
import { DbExecutorClosedError, DbExecutorTimeoutError, createDbExecutor } from "@/session/worker/db-executor"
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

const NEVER_READY_WORKER_SOURCE = `
import { parentPort } from "node:worker_threads"

// Deliberately never sends "ready": the caller stays in the boot handshake
// until close() settles it.
parentPort!.on("message", (message: { type: string; id: number }) => {
  if (message.type === "close") parentPort!.postMessage({ type: "closed", id: message.id })
})
`

describe("db executor boot close", () => {
  test("close rejects a request still waiting on the boot handshake", async () => {
    const { filename } = workspace()
    const worker = path.join(path.dirname(filename), "never-ready-worker.ts")
    await Bun.write(worker, NEVER_READY_WORKER_SOURCE)

    const executor = createDbExecutor({
      filename: ":memory:",
      entry: pathToFileURL(worker),
      timeoutMs: 2_000,
    })
    const pending = executor.query("SELECT 1")
    // Attach a handler before close: the rejection is expected, and Bun should
    // not report it as unhandled while the test waits for close() itself.
    void pending.catch(() => undefined)
    await new Promise((resolve) => setTimeout(resolve, 25))
    await executor.close()

    const outcome = await Promise.race([
      pending.then(
        () => "resolved" as const,
        (error: unknown) => error,
      ),
      new Promise<"timeout">((resolve) => setTimeout(() => resolve("timeout"), 1_000)),
    ])
    // A close during boot must settle the waiter instead of dropping it.
    expect(outcome).toBeInstanceOf(DbExecutorClosedError)
  })
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
