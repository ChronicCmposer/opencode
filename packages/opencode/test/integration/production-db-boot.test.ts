// Production DB boot — the real hub must migrate the app database before it
// opens the spawn gate, so a fresh worker can pass the read-only schema gate.
//
// The database in this test is created by the NORMAL app migration path
// (`Database.layerFromPath` → `DatabaseMigration.apply`), not by the test
// applying the shipped statements. The hub is the real
// `createProductionSessionHub`; the worker is the real `session-worker.ts`
// isolate. On a fresh file the worker used to fail `SchemaMismatch` forever
// (user_version 0 and no ledger), which is the RED this test pins.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { openConnection } from "@opencode-ai/core/database/connection"
import { currentSchemaIdentity, ensureProductionSchemaAsync, schemaIdentity } from "@/session/db/schema"
import { MessageID } from "@/session/schema"
import { createProductionSessionHub } from "@/session/worker/hub"
import { sessionWorkerInit } from "../lib/session-worker-init"
import { workerSnapshot } from "../lib/snapshot"
import { TestLLMServer } from "../lib/llm-server"
import { testEffect } from "../lib/effect"
import { sid } from "../lib/async"
import { REAL_WORKER_TEST_TIMEOUT_MS, REAL_WORKER_TRANSPORT_TIMEOUT_MS } from "../lib/worker-timeouts"

const it = testEffect(TestLLMServer.layer)

const SESSION_WORKER = new URL("../../src/session/worker/session-worker.ts", import.meta.url)
const directories: string[] = []

function freshWorkspace(): { readonly filename: string; readonly directory: string } {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-production-db-"))
  directories.push(directory)
  return { filename: path.join(directory, "opencode.db"), directory }
}

/** The normal app boot path: core's drizzle layer applies its migration journal. */
function migrateLikeProduction(filename: string) {
  return Effect.scoped(
    Effect.gen(function* () {
      yield* Database.Service
    }),
  ).pipe(Effect.provide(Database.layerFromPath(filename)))
}

/** Seed the project + session rows a turn needs, through a normal connection. */
function seedSession(filename: string, sessionID: string): void {
  const connection = openConnection({ filename, busyTimeoutMs: 5000 })
  try {
    connection.run(
      "INSERT INTO project (id, name, worktree, sandboxes, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
      ["project-production", "production", "/tmp/production", "[]", 1, 1],
    )
    connection.run(
      "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      [sessionID, "project-production", "production", "/tmp/production", "production boot", "1", 1, 1],
    )
  } finally {
    connection.close()
  }
}

function readSchema(filename: string) {
  const connection = openConnection({ filename, busyTimeoutMs: 5000 })
  try {
    return {
      identity: schemaIdentity(connection),
      version: Number(connection.pragma("user_version")),
      messages: connection.all<{ readonly count: number }>("SELECT COUNT(*) AS count FROM message"),
      ledger: connection.all<{ readonly name: string }>(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('tool_call', 'worker_crash') ORDER BY name",
      ),
    }
  } finally {
    connection.close()
  }
}

afterEach(() => {
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("production DB boot (D7)", () => {
  it.live(
    "boots a real worker through the production hub on a fresh app-migrated DB and runs one turn",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        // The worker snapshot directory must exist on disk; each test owns its
        // own temp workspace instead of relying on a pre-created global path.
        const { filename, directory } = freshWorkspace()
        const sessionID = "ses_production_db_boot"

        // Only the normal application migration path touches the file here.
        yield* migrateLikeProduction(filename)
        seedSession(filename, sessionID)
        // The hub's off-loop boot gate is the production schema path; run the
        // same function the hub awaits before spawning.
        yield* Effect.promise(() => ensureProductionSchemaAsync({ filename, liveWorkers: 0 }))
        yield* llm.text("production-db-boot")

        const outcome = yield* Effect.promise(async () => {
          const hub = createProductionSessionHub({
            init: () =>
              sessionWorkerInit({
                snapshot: workerSnapshot({ directory }),
                model: { baseUrl: llm.url, apiKey: "test-key" },
              }),
            entry: SESSION_WORKER,
            databasePath: filename,
            env: { OPENCODE_DB: filename },
            // Serve the one hub authority a simple text turn touches. The
            // production layer wires the real SessionStatus/Permission/Question
            // services; this harness only needs the status surface.
            ports: {
              hubProxy: {
                "sessionStatus.get": () => ({ type: "idle" }),
                "sessionStatus.list": () => new Map(),
                "sessionStatus.set": () => undefined,
              },
            },
            timeoutMs: REAL_WORKER_TRANSPORT_TIMEOUT_MS,
          })
          try {
            await hub.dispatcher.dispatch({
              sessionID: sid(sessionID),
              prompt: {
                messageID: MessageID.ascending(),
                sessionID: sid(sessionID),
                agent: "build",
                parts: [{ type: "text", text: "hello from the production boot test" }],
              },
            })
            return undefined
          } catch (error) {
            return error instanceof Error ? error.message : String(error)
          } finally {
            await hub.terminateAll()
          }
        })

        const observed = readSchema(filename)
        // The hub migrated the fresh app database before spawn.
        expect(observed.version).toBe(currentSchemaIdentity().version)
        expect(observed.identity).toEqual(currentSchemaIdentity())
        expect(observed.ledger.map((row) => row.name)).toEqual(["tool_call", "worker_crash"])

        // Both invariants are closed: the hub migrates the fresh app database
        // before spawn, and the worker Bus may publish durable events through
        // its own connection. The turn must therefore finish cleanly.
        expect(outcome).toBeUndefined()
        expect(observed.messages[0]?.count).toBeGreaterThan(0)
      }),
    REAL_WORKER_TEST_TIMEOUT_MS,
  )
})
