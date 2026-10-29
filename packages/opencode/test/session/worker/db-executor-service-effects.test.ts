// The DbExecutor Effect service: typed failures, interruption, and the scoped
// checkpoint fiber. These tests exercise the real worker-thread executor, not a
// Promise adapter around it.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { describe, expect } from "bun:test"
import { Effect, Fiber, Layer } from "effect"
import {
  DbExecutorClosedError,
  DbExecutorCrashedError,
  DbExecutorRemoteError,
  DbExecutorTimeoutError,
  layer,
  make,
  Service,
} from "@/session/worker/db-executor"
import { pollWithTimeout, testEffect } from "../../lib/effect"

const HANGING_WORKER = new URL("./db-executor-service-hanging-worker.ts", import.meta.url)
const CHECKPOINT_WORKER = new URL("./db-executor-service-checkpoint-worker.ts", import.meta.url)

const layerIt = testEffect(layer({ filename: ":memory:" }))
const it = testEffect(Layer.empty)

function scratchDirectory(): { directory: string; cleanup: Effect.Effect<void> } {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-db-executor-service-"))
  return {
    directory,
    cleanup: Effect.sync(() => fs.rmSync(directory, { recursive: true, force: true })),
  }
}

describe("db executor service", () => {
  layerIt.live("provides the service through a layer and forwards every operation", () =>
    Effect.gen(function* () {
      const executor = yield* Service
      expect(executor.threadId).toBe(0)

      yield* executor.run("CREATE TABLE item (id INTEGER PRIMARY KEY, name TEXT)")
      expect(
        yield* executor.transaction([
          { sql: "INSERT INTO item (name) VALUES (?)", params: ["a"] },
          { sql: "INSERT INTO item (name) VALUES (?)", params: ["b"] },
        ]),
      ).toEqual([
        { changes: 1, lastInsertRowid: 1 },
        { changes: 1, lastInsertRowid: 2 },
      ])
      expect(yield* executor.query("SELECT name FROM item ORDER BY id")).toEqual([{ name: "a" }, { name: "b" }])
      expect(yield* executor.queryInTransaction("SELECT name FROM item ORDER BY id")).toEqual([
        { name: "a" },
        { name: "b" },
      ])
      expect(executor.threadId).toBeGreaterThan(0)
    }),
  )

  it.live("reports a closed executor as a typed Schema error", () =>
    Effect.gen(function* () {
      const executor = yield* make({ filename: ":memory:" })
      yield* executor.query("SELECT 1 AS n")
      yield* executor.close()

      const error = yield* Effect.flip(executor.query("SELECT 1 AS n"))
      expect(error).toBeInstanceOf(DbExecutorClosedError)
      expect(error._tag).toBe("DbExecutorClosedError")
    }),
  )

  it.live("reports a worker crash as a typed Schema error", () =>
    Effect.gen(function* () {
      const executor = yield* make({
        filename: ":memory:",
        timeoutMs: 1_000,
        entry: new URL("../../fixture/db-executor-crash.ts", import.meta.url),
      })

      const error = yield* Effect.flip(executor.query("SELECT 1 AS n"))
      expect(error).toBeInstanceOf(DbExecutorCrashedError)
      expect(error).not.toBeInstanceOf(DbExecutorTimeoutError)
      expect(error._tag).toBe("DbExecutorCrashedError")
    }),
  )

  it.live("reports a remote SQL failure as a typed Schema error", () =>
    Effect.gen(function* () {
      const executor = yield* make({ filename: ":memory:" })

      const error = yield* Effect.flip(executor.query("SELECT * FROM missing_table"))
      expect(error).toBeInstanceOf(DbExecutorRemoteError)
      expect(error._tag).toBe("DbExecutorRemoteError")
      expect((error as DbExecutorRemoteError).message).toContain("missing_table")
    }),
  )

  it.live("reports a deadline as an indeterminate typed Schema error", () =>
    Effect.gen(function* () {
      const executor = yield* make({
        filename: ":memory:",
        timeoutMs: 100,
        entry: HANGING_WORKER,
      })

      const error = yield* Effect.flip(executor.query("SLOW"))
      expect(error).toBeInstanceOf(DbExecutorTimeoutError)
      expect((error as DbExecutorTimeoutError).indeterminate).toBe(true)
      expect(error._tag).toBe("DbExecutorTimeoutError")
    }),
  )

  it.live("interruption reaches the executor instead of waiting for a response", () =>
    Effect.gen(function* () {
      const scratch = scratchDirectory()
      // The hanging fixture treats an absolute `filename` as a readiness marker;
      // the marker is published once `SLOW` is actually in flight.
      const received = path.join(scratch.directory, "slow-received")
      try {
        const executor = yield* make({
          filename: received,
          timeoutMs: 0,
          entry: HANGING_WORKER,
        })
        // The fixture never answers SLOW; only the request callback's
        // interruption cleanup can let this fiber finish.
        const fiber = yield* executor.query("SLOW").pipe(Effect.forkScoped({ startImmediately: true }))
        yield* pollWithTimeout(
          Effect.sync(() => (fs.existsSync(received) ? true : undefined)),
          "the hanging worker never received the SLOW request",
        )

        // `Fiber.interrupt` only returns once the request callback's interruption
        // cleanup has run; a Promise-only handshake would leave this suspended.
        yield* Fiber.interrupt(fiber)

        // Teardown still completes its orderly close handshake.
        yield* executor.close()
      } finally {
        yield* scratch.cleanup
      }
    }),
  )

  it.live("scopes the checkpoint fiber and stops it when the service scope closes", () =>
    Effect.gen(function* () {
      const scratch = scratchDirectory()
      const logPath = path.join(scratch.directory, "checkpoints.log")
      const observed = () => {
        try {
          return fs
            .readFileSync(logPath, "utf8")
            .split("\n")
            .filter((line) => line.length > 0).length
        } catch {
          return 0
        }
      }

      yield* Effect.scoped(
        Effect.gen(function* () {
          const executor = yield* make({
            filename: logPath,
            entry: CHECKPOINT_WORKER,
            checkpointIntervalMs: 10,
          })
          // Construction must stay lazy; the first checkpoint follows a request.
          expect(executor.threadId).toBe(0)
          yield* executor.query("SELECT 1 AS n")
          yield* pollWithTimeout(
            Effect.sync(() => (observed() > 0 ? observed() : undefined)),
            "the scoped checkpoint fiber never ran",
          )
        }),
      )

      const afterClose = observed()
      yield* Effect.sleep("60 millis")
      expect(observed()).toBe(afterClose)
      yield* scratch.cleanup
    }),
  )
})
