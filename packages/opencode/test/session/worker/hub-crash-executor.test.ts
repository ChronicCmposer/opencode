import { describe, expect, test } from "bun:test"
import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { Database as BunDatabase } from "bun:sqlite"
import type { SessionID } from "@/session/schema"
import { WorkerCrashedError } from "@/session/worker/dispatch"
import type {
  ExecutorRun,
  ExecutorWorker,
  ExecutorWorkerPool,
  SessionExecutionResult,
} from "@/session/worker/execute"
import { createSessionHub, workerCrashedFrom, type WorkerCrashed } from "@/session/worker/hub"
import { createTestBarrier } from "../../lib/run-barrier-test"
import { createDbExecutor, type DbExecutor } from "@/session/worker/db-executor"
import { createBunSqliteConnection } from "@/session/writer/connection"
import { LEDGER_SQL } from "@/session/ledger/tool-call"
import { crashes } from "@/session/ledger/crash"

const crash = (sessionID: string): WorkerCrashed => ({
  sessionID,
  workerID: 7,
  phase: "commit",
  lastCommittedMessageID: "msg_committed",
  inFlightCallIDs: [],
  retryable: false,
  reason: "isolate exited after commit",
})

function crashPool(payload: WorkerCrashed): ExecutorWorkerPool {
  const results = new Map<SessionID, SessionExecutionResult>()
  const error = Object.assign(new WorkerCrashedError({ message: "worker crashed", workerID: payload.workerID }), {
    workerCrashed: payload,
  })
  const worker: ExecutorWorker = {
    id: payload.workerID,
    alive: true,
    async execute(_run: ExecutorRun) {
      throw error
    },
    async cancel() {},
    async assertNotBusy() {
      return false
    },
    resultFor: (sessionID) => results.get(sessionID),
    async terminate() {},
  }
  return {
    workers: [worker],
    spawn: () => worker,
    terminateAll: async () => {},
    byID: (id) => (id === worker.id ? worker : undefined),
    resultFor: (sessionID) => results.get(sessionID),
  }
}

describe("hub crash persistence through the DB executor", () => {
  test("a commit crash persists the returned payload through the executor under a held write lock", async () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-executor-crash-"))
    const filename = path.join(directory, "opencode.db")
    const sessionID = "ses_executor_commit_crash"
    const expected = crash(sessionID)

    const admin = createBunSqliteConnection({ filename })
    admin.exec("CREATE TABLE session (id TEXT PRIMARY KEY)")
    admin.exec(LEDGER_SQL)
    admin.run("INSERT INTO session (id) VALUES (?)", [sessionID])
    admin.close()

    // A real second connection holds the write lock while the crash is surfaced.
    // Without WAL + busy_timeout the executor reports SQLITE_BUSY immediately and
    // the crash is never persisted.
    const holder = new BunDatabase(filename)
    holder.exec("PRAGMA journal_mode = WAL")
    holder.exec("BEGIN IMMEDIATE")
    holder.exec("INSERT INTO worker_crash (crash_id, session_id, worker_id, phase, last_committed_message_id, in_flight_call_ids, retryable, reason, time_created) VALUES ('holder', 'holder', 0, 'commit', NULL, '[]', 0, 'holder', 0)")

    let executor: DbExecutor | undefined
    let hub: ReturnType<typeof createSessionHub> | undefined
    try {
      executor = createDbExecutor({ filename, timeoutMs: 10_000 })
      hub = createSessionHub({ ...{ barrier: createTestBarrier() }, pool: crashPool(expected), db: executor })

      const settled = hub.ensureRunning(sessionID as SessionID).then(
        () => ({ rejected: false as const }),
        (error: unknown) => ({ rejected: true as const, error }),
      )
      const release = setTimeout(() => {
        try {
          holder.exec("ROLLBACK")
        } catch {
          // The test already released/closed the holder.
        }
      }, 150)
      const outcome = await settled
      clearTimeout(release)

      if (!outcome.rejected) throw new Error("expected the crash to reject the run")
      expect(workerCrashedFrom(outcome.error)).toEqual(expected)
      const reader = createBunSqliteConnection({ filename })
      try {
        expect(crashes(reader, sessionID)).toEqual([
          expect.objectContaining({
            sessionID,
            workerID: expected.workerID,
            phase: "commit",
            lastCommittedMessageID: "msg_committed",
            inFlightCallIDs: [],
            retryable: false,
            reason: expected.reason,
          }),
        ])
      } finally {
        reader.close()
      }
    } finally {
      try {
        holder.exec("ROLLBACK")
      } catch {
        // Already released by the timer.
      }
      holder.close()
      await hub?.terminateAll().catch(() => undefined)
      await executor?.close().catch(() => undefined)
      fs.rmSync(directory, { recursive: true, force: true })
    }
  })
})
