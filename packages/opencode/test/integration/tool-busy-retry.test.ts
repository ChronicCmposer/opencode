// A tool result commit survives another connection holding the write lock.
//
// The worker's driver busy timeout is deliberately short (250ms). A lock held
// longer than that must be absorbed by the bounded whole-unit retry instead of
// surfacing `database is locked` into the tool result or ledger. This test holds
// the lock from the test process, releases it inside the retry budget, and then
// proves the durable result, the ledger, and the connection all recover.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Database } from "@opencode-ai/core/database/database"
import { openConnection, type QueryParams } from "@opencode-ai/core/database/connection"
import { BusyExhaustedError } from "@opencode-ai/core/database/transaction"
import type { RepositoryConnection } from "@/session/worker/repository/capability"
import { connectedRepository } from "@/session/worker/repository/write"
import { MessageID } from "@/session/schema"
import { createProductionSessionHub, type SessionHub } from "@/session/worker/hub"
import { createDbExecutor } from "@/session/worker/db-executor"
import { sessionWorkerInit } from "../lib/session-worker-init"
import { workerSnapshot } from "../lib/snapshot"
import { TestLLMServer } from "../lib/llm-server"
import { pollWithTimeout, testEffect } from "../lib/effect"
import { createTestBarrier } from "../lib/run-barrier-test"
import { sid } from "../lib/async"
import {
  REAL_WORKER_IDLE_TIMEOUT_MS,
  REAL_WORKER_TEST_TIMEOUT_MS,
  REAL_WORKER_TRANSPORT_TIMEOUT_MS,
} from "../lib/worker-timeouts"
import { Effect } from "effect"
import { noMcpNoSnapshotHubProxy } from "../lib/hub-proxy-defaults"

const it = testEffect(TestLLMServer.layer)
const SESSION_WORKER = new URL("../../src/session/worker/session-worker.ts", import.meta.url)
const directories: string[] = []

afterEach(() => {
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

function freshWorkspace() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-tool-busy-"))
  directories.push(directory)
  return { filename: path.join(directory, "opencode.db"), directory }
}

function migrateLikeProduction(filename: string) {
  return Effect.scoped(Effect.gen(function* () {
    yield* Database.Service
  })).pipe(Effect.provide(Database.layerFromPath(filename)))
}

function seedSession(filename: string, sessionID: string): void {
  const connection = openConnection({ filename, busyTimeoutMs: 5000 })
  try {
    connection.run(
      "INSERT INTO project (id, name, worktree, sandboxes, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
      ["project-tool-busy", "tool-busy", "/tmp/tool-busy", "[]", 1, 1],
    )
    connection.run(
      "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      [sessionID, "project-tool-busy", "tool-busy", "/tmp/tool-busy", "tool-busy", "1", 1, 1],
    )
  } finally {
    connection.close()
  }
}

function readRows(filename: string, sessionID: string) {
  const connection = openConnection({ filename, busyTimeoutMs: 0 })
  try {
    return {
      tools: connection.all<{ readonly call_id: string; readonly state: string; readonly outcome: string | null }>(
        "SELECT call_id, state, outcome FROM tool_call WHERE session_id = ? ORDER BY time_started, call_id",
        [sessionID],
      ),
      parts: connection
        .all<{ readonly data: string }>("SELECT data FROM part WHERE session_id = ? ORDER BY time_created, id", [
          sessionID,
        ])
        .map((row) => JSON.parse(row.data) as { readonly type: string; readonly state?: { readonly status: string } }),
      messages: connection
        .all<{ readonly data: string }>("SELECT data FROM message WHERE session_id = ? ORDER BY time_created, id", [
          sessionID,
        ])
        .map((row) => row.data),
    }
  } finally {
    connection.close()
  }
}

interface RealHubFixture {
  readonly hub: SessionHub
  readonly dispose: () => Promise<void>
}

function startHub(input: {
  readonly llm: TestLLMServer["Service"]
  readonly filename: string
  readonly directory: string
}): RealHubFixture {
  const db = createDbExecutor({ filename: input.filename, timeoutMs: REAL_WORKER_TRANSPORT_TIMEOUT_MS })
  const hub = createProductionSessionHub({
    barrier: createTestBarrier(),
    db,
    init: () =>
      ({
        ...sessionWorkerInit({
          snapshot: workerSnapshot({ directory: input.directory }),
          model: { baseUrl: input.llm.url, apiKey: "test-key" },
        }),
      }) as ReturnType<typeof sessionWorkerInit>,
    entry: SESSION_WORKER,
    databasePath: input.filename,
    env: { OPENCODE_DB: input.filename },
    timeoutMs: REAL_WORKER_TRANSPORT_TIMEOUT_MS,
    runIdleTimeoutMs: REAL_WORKER_IDLE_TIMEOUT_MS,
    ports: {
      hubProxy: {
        ...noMcpNoSnapshotHubProxy,
        "sessionStatus.get": () => ({ type: "idle" }),
        "sessionStatus.list": () => new Map(),
        "sessionStatus.set": () => undefined,
        "permission.ask": () => undefined,
        "permission.reply": () => undefined,
        "permission.list": () => [],
      },
    },
  })
  return {
    hub,
    dispose: async () => {
      await hub.terminateAll()
      await db.close()
    },
  }
}

function dispatchPrompt(hub: SessionHub, sessionID: string, text: string): Promise<void> {
  return hub.dispatcher.dispatch({
    sessionID: sid(sessionID),
    prompt: {
      messageID: MessageID.ascending(),
      sessionID: sid(sessionID),
      agent: "build",
      parts: [{ type: "text", text }],
    },
  })
}

describe("tool result commit under a held write lock (real worker)", () => {
  it.live(
    "the result and ledger finish retry past the worker busy timeout",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const { filename, directory } = freshWorkspace()
        const sessionID = "ses_tool_busy_retry"
        yield* migrateLikeProduction(filename)
        seedSession(filename, sessionID)

        const started = path.join(directory, "tool-started")
        const release = path.join(directory, "tool-release")
        const command = `touch '${started}'; while [ ! -f '${release}' ]; do sleep 0.02; done; echo done`
        yield* llm.tool("bash", { command, description: "block until released", workdir: directory })
        yield* llm.text("done")

        const fixture = startHub({ llm, filename, directory })
        let disposed = false
        const dispose = async () => {
          if (disposed) return
          disposed = true
          await fixture.dispose()
        }

        try {
          const run = dispatchPrompt(fixture.hub, sessionID, "run the blocking tool")

          // Wait for the durable start row and the real bash process before
          // contending: the contended write must be the result/finish boundary.
          const reader = openConnection({ filename, busyTimeoutMs: 0 })
          try {
            // The worker creates the ledger schema during boot; wait for it
            // before querying the row so an early poll is not a hard error.
            yield* pollWithTimeout(
              Effect.sync(
                () =>
                  reader.all<{ readonly name: string }>(
                    "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'tool_call' LIMIT 1",
                  )[0]?.name,
              ),
              "tool_call table was never created",
            )
            yield* pollWithTimeout(
              Effect.sync(() => {
                const row = reader.all<{ readonly state: string }>(
                  "SELECT state FROM tool_call WHERE session_id = ? AND state = 'started' LIMIT 1",
                  [sessionID],
                )[0]
                return row === undefined ? undefined : row
              }),
              "tool call never reached started",
            )
          } finally {
            reader.close()
          }
          yield* pollWithTimeout(
            Effect.sync(() => (fs.existsSync(started) ? true : undefined)),
            "tool command never started",
          )

          const holder = openConnection({ filename, busyTimeoutMs: 0 })
          try {
            // The worker may still be writing a part; retry until the lock is ours.
            yield* pollWithTimeout(
              Effect.sync(() => {
                try {
                  holder.exec("BEGIN IMMEDIATE")
                  return true
                } catch {
                  return undefined
                }
              }),
              "could not take the write lock",
            )

            // Let the tool finish while the lock is held. The hold is the test:
            // it must outlast the worker's 250ms driver timeout but stay inside
            // the bounded retry budget (~1.5s of driver waits).
            fs.writeFileSync(release, "go")
            yield* Effect.promise(() => new Promise<void>((resolve) => setTimeout(resolve, 600)))
            holder.exec("COMMIT")
          } finally {
            holder.close()
          }

          yield* Effect.promise(() => run)

          const rows = readRows(filename, sessionID)
          expect(rows.tools).toHaveLength(1)
          expect(rows.tools[0]).toMatchObject({ state: "finished", outcome: "ok" })
          expect(rows.parts.filter((part) => part.type === "tool" && part.state?.status === "completed")).toHaveLength(1)
          expect(JSON.stringify(rows)).not.toMatch(/database is locked|SQLITE_BUSY|SQLITE_LOCKED|LockTimeoutError/)

          yield* Effect.promise(dispose)

          const probe = openConnection({ filename, busyTimeoutMs: 0 })
          try {
            probe.exec("BEGIN IMMEDIATE")
            probe.exec("COMMIT")
          } finally {
            probe.close()
          }
        } finally {
          yield* Effect.promise(dispose)
        }
      }),
    REAL_WORKER_TEST_TIMEOUT_MS,
  )
})

describe("repository whole-unit retry under a busy validation read", () => {
  const TODO_DDL = `
    CREATE TABLE todo (
      session_id TEXT NOT NULL,
      content TEXT NOT NULL,
      status TEXT NOT NULL,
      priority TEXT NOT NULL,
      position INTEGER NOT NULL,
      time_created INTEGER NOT NULL,
      time_updated INTEGER NOT NULL,
      PRIMARY KEY (session_id, position)
    );
  `
  const todo = {
    session_id: "ses_repository_busy",
    content: "retry me",
    status: "pending",
    priority: "high",
    position: 1,
    time_created: 1,
    time_updated: 1,
  }
  const busy = () => Object.assign(new Error("SQLITE_BUSY: database is locked"), { code: "SQLITE_BUSY" })

  test("a busy validation read is retried with the write it guards", () => {
    const { filename } = freshWorkspace()
    const native = openConnection({ filename, busyTimeoutMs: 250 })
    try {
      native.exec(TODO_DDL)
      native.run(
        "INSERT INTO todo (session_id, content, status, priority, position, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [todo.session_id, todo.content, todo.status, todo.priority, todo.position, todo.time_created, todo.time_updated],
      )

      let failed = false
      let reads = 0
      const scripted: RepositoryConnection = {
        all: ((sql: string, params?: QueryParams) => {
          reads += 1
          // The validation read is busy once; the retry must replay the read
          // and the write together, never the write alone.
          if (!failed) {
            failed = true
            throw busy()
          }
          return native.all(sql, params)
        }) as RepositoryConnection["all"],
        values: native.values.bind(native),
        run: native.run.bind(native),
        exec: native.exec.bind(native),
      }

      const outcome = connectedRepository(scripted).update(
        "todo",
        { content: "updated" },
        { session_id: todo.session_id, position: todo.position },
      )

      expect(outcome.changes).toBe(1)
      expect(reads).toBe(2)
      expect(native.all("SELECT content FROM todo")).toEqual([{ content: "updated" }])
    } finally {
      native.close()
    }
  })

  test("an exhausted repository unit is typed, never a raw database-is-locked failure", () => {
    const { filename } = freshWorkspace()
    const holder = openConnection({ filename, busyTimeoutMs: 0 })
    const contender = openConnection({ filename, busyTimeoutMs: 1 })
    try {
      holder.exec(TODO_DDL)
      holder.exec("BEGIN IMMEDIATE")

      let failure: unknown
      try {
        connectedRepository(contender).insert("todo", todo)
      } catch (error) {
        failure = error
      } finally {
        holder.exec("ROLLBACK")
      }

      expect(failure).toBeInstanceOf(BusyExhaustedError)
      expect((failure as BusyExhaustedError).tag).toBe("SQLITE_BUSY_EXHAUSTED")
      expect((failure as Error).message).not.toContain("database is locked")
      expect(contender.all("SELECT content FROM todo")).toEqual([])
    } finally {
      holder.close()
      contender.close()
    }
  })
})
