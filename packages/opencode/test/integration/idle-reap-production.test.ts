// Production idle-reap end to end.
//
// Three independent sessions run to completion through the real production hub
// and the real session-worker isolate. With a 50 ms reap window, every idle
// worker is retired and its per-session hub state dropped. The assertions:
//   * all hub diagnostics return to zero (registry, pids, spawn gate, lifecycle,
//     barrier, protocol errors);
//   * a brand-new session still launches and completes after the reap;
//   * a direct SQLite connection can take `BEGIN IMMEDIATE` with a zero busy
//     timeout, proving the retired isolates released the write lock.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect } from "bun:test"
import { Database } from "bun:sqlite"
import { Effect } from "effect"
import { Database as CoreDatabase } from "@opencode-ai/core/database/database"
import { openConnection } from "@opencode-ai/core/database/connection"
import { MessageID } from "@/session/schema"
import { createProductionSessionHub } from "@/session/worker/production-hub"
import { createDbExecutor } from "@/session/worker/db-executor"
import { liveSessionWorkerCount, makeSpawnGate } from "@/session/worker/worker-pools"
import { createTestBarrier } from "../lib/run-barrier-test"
import { pollUntil, sid } from "../lib/async"
import { testEffect } from "../lib/effect"
import { TestLLMServer } from "../lib/llm-server"
import { noMcpNoSnapshotHubProxy } from "../lib/hub-proxy-defaults"
import { sessionWorkerInit } from "../lib/session-worker-init"
import { workerSnapshot } from "../lib/snapshot"
import {
  REAL_WORKER_IDLE_TIMEOUT_MS,
  REAL_WORKER_TEST_TIMEOUT_MS,
  REAL_WORKER_TRANSPORT_TIMEOUT_MS,
} from "../lib/worker-timeouts"

const it = testEffect(TestLLMServer.layer)

const SESSION_WORKER = new URL("../../src/session/worker/session-worker.ts", import.meta.url)
const PROJECT = "project_idle_reap"
const REAP_MS = 50
const directories: string[] = []

function freshWorkspace(): { readonly filename: string; readonly directory: string } {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-idle-reap-"))
  directories.push(directory)
  return { filename: path.join(directory, "opencode.db"), directory }
}

function migrateLikeProduction(filename: string) {
  return Effect.scoped(
    Effect.gen(function* () {
      yield* CoreDatabase.Service
    }),
  ).pipe(Effect.provide(CoreDatabase.layerFromPath(filename)))
}

/** Seed one project and the given session rows into the production database. */
function seedSessions(filename: string, directory: string, sessionIDs: readonly string[]): void {
  const connection = openConnection({ filename, busyTimeoutMs: 5000 })
  try {
    connection.run(
      "INSERT OR IGNORE INTO project (id, name, worktree, sandboxes, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
      [PROJECT, "idle-reap", directory, "[]", 1, 1],
    )
    for (const sessionID of sessionIDs) {
      connection.run(
        "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [sessionID, PROJECT, sessionID, directory, sessionID, "1", 1, 1],
      )
    }
  } finally {
    connection.close()
  }
}

interface HubFixture {
  readonly hub: ReturnType<typeof createProductionSessionHub>
  dispose(): Promise<void>
}

function startHub(input: {
  readonly llm: TestLLMServer["Service"]
  readonly filename: string
  readonly directory: string
}): HubFixture {
  const db = createDbExecutor({ filename: input.filename, timeoutMs: REAL_WORKER_TRANSPORT_TIMEOUT_MS })
  const hub = createProductionSessionHub({
    barrier: createTestBarrier(),
    db,
    init: () =>
      sessionWorkerInit({
        snapshot: workerSnapshot({ directory: input.directory }),
        model: { baseUrl: input.llm.url, apiKey: "test-key" },
      }),
    entry: SESSION_WORKER,
    databasePath: input.filename,
    env: { OPENCODE_DB: input.filename },
    timeoutMs: REAL_WORKER_TRANSPORT_TIMEOUT_MS,
    runIdleTimeoutMs: REAL_WORKER_IDLE_TIMEOUT_MS,
    reapMs: REAP_MS,
    spawnGate: makeSpawnGate({ timeoutMs: REAL_WORKER_TRANSPORT_TIMEOUT_MS, capacity: 4 }),
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

function dispatchPrompt(fixture: HubFixture, sessionID: string, text: string): Promise<void> {
  return fixture.hub.dispatcher.dispatch({
    sessionID: sid(sessionID),
    prompt: {
      messageID: MessageID.ascending(),
      sessionID: sid(sessionID),
      agent: "build",
      parts: [{ type: "text", text }],
    },
  })
}

/**
 * Prove no retired worker still holds the write lock. Retry is bounded to the
 * two transient SQLite lock errors; every other failure is a real defect.
 */
async function expectImmediateWrite(filename: string): Promise<void> {
  let lastBusy: unknown
  for (let attempt = 0; attempt < 10; attempt++) {
    const probe = new Database(filename)
    try {
      probe.exec("PRAGMA busy_timeout = 0")
      try {
        probe.exec("BEGIN IMMEDIATE")
        probe.exec("ROLLBACK")
        return
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error)
        if (!/SQLITE_BUSY|database is locked/.test(message)) throw error
        lastBusy = error
      }
    } finally {
      probe.close()
    }
    await new Promise<void>((resolve) => setTimeout(resolve, 25))
  }
  throw new Error(
    `database stayed locked after the idle reap: ${lastBusy instanceof Error ? lastBusy.message : String(lastBusy)}`,
  )
}

afterEach(() => {
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("production idle reap", () => {
  it.live(
    "three real sessions reap to zero state, then a new session runs and the write lock is free",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const { filename, directory } = freshWorkspace()
        const sessions = ["ses_reap_parent", "ses_reap_a", "ses_reap_b"] as const
        yield* migrateLikeProduction(filename)
        seedSessions(filename, directory, sessions)
        for (const sessionID of sessions) yield* llm.text(`${sessionID} complete`)

        const fixture = startHub({ llm, filename, directory })
        try {
          for (const sessionID of sessions) {
            yield* Effect.promise(() => dispatchPrompt(fixture, sessionID, `run ${sessionID}`))
          }

          yield* Effect.promise(() =>
            pollUntil(
              () => fixture.hub.dispatcher.registry.size === 0 && liveSessionWorkerCount(filename) === 0,
              {
                timeoutMs: 30_000,
                message: "the idle reaper never retired every session worker",
              },
            ),
          )

          expect(fixture.hub.diagnostics()).toEqual({
            registry: 0,
            pids: 0,
            gateActive: 0,
            lifecycle: 0,
            barrier: 0,
            protocolErrors: 0,
          })

          // A session created after the reap launches a fresh isolate and runs.
          seedSessions(filename, directory, ["ses_after_reap"])
          yield* llm.text("after reap complete")
          yield* Effect.promise(() => dispatchPrompt(fixture, "ses_after_reap", "run after reap"))

          // The successor is reaped by the same window too, so the write-lock
          // probe below observes no live isolate for this database path.
          yield* Effect.promise(() =>
            pollUntil(
              () => fixture.hub.dispatcher.registry.size === 0 && liveSessionWorkerCount(filename) === 0,
              {
                timeoutMs: 30_000,
                message: "the post-reap session worker never retired",
              },
            ),
          )

          // The just-completed worker released its write lock; the hub still
          // owns no transaction, so a zero-busy-timeout writer succeeds.
          yield* Effect.promise(() => expectImmediateWrite(filename))
        } finally {
          yield* Effect.promise(() => fixture.dispose())
        }
      }),
    REAL_WORKER_TEST_TIMEOUT_MS,
  )
})
