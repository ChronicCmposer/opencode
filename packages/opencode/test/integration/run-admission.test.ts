// P0 run admission: a run that ends must settle its commit-side phases and
// report the durable message it actually wrote.
//
// The barrier used to wait for a `commits` report that a run without a model
// step can never produce: the 5 s bound expired into `timed-out` and every later
// prompt was refused with `RunBarrierTimeout` until the idle reaper retired the
// worker. The first fix settled every such exit with `noCommit()`, which lost the
// durable message id for the two exits that DO persist an assistant message: a
// shell run and a compaction stop. These cases prove both halves:
//
//   * a run with no durable message settles explicitly (`none`, never a
//     fabricated commit), and
//   * a shell run / compaction stop reports the persisted assistant message id,
//     so `lastCommittedMessageID` is correct for resume and auto-resume.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect } from "bun:test"
import { Effect, Fiber } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { openConnection } from "@opencode-ai/core/database/connection"
import { MessageID } from "@/session/schema"
import { createLifecycleTracker } from "@/session/ledger/lifecycle"
import { RunBarrier } from "@/session/run-barrier"
import { createProductionSessionHub, createSessionHub, type SessionHub } from "@/session/worker/hub"
import { createTestBarrier } from "../lib/run-barrier-test"
import { createDbExecutor } from "@/session/worker/db-executor"
import { createFakeExecutorPool } from "../lib/fake-worker-pool"
import { flush, sid } from "../lib/async"
import { testEffect } from "../lib/effect"
import { clockedRunBarrierLayer } from "../lib/run-barrier-test"
import { TestLLMServer } from "../lib/llm-server"
import { sessionWorkerInit } from "../lib/session-worker-init"
import { workerSnapshot } from "../lib/snapshot"
import {
  REAL_WORKER_IDLE_TIMEOUT_MS,
  REAL_WORKER_TEST_TIMEOUT_MS,
  REAL_WORKER_TRANSPORT_TIMEOUT_MS,
} from "../lib/worker-timeouts"
import { noMcpNoSnapshotHubProxy } from "../lib/hub-proxy-defaults"

function shellInput(sessionID: ReturnType<typeof sid>) {
  return { sessionID, agent: "build", command: "true" }
}

const it = testEffect(clockedRunBarrierLayer)

describe("run admission after a run that commits nothing (P0)", () => {
  it.effect("a shell run drains and admits the next prompt", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const pool = createFakeExecutorPool(1)
      const hub = createSessionHub({ pool, lifecycle: createLifecycleTracker(), barrier })
      const sessionID = sid("ses_admission_shell")

      try {
        const shellRun = yield* Effect.promise(() => hub.startShell(sessionID, shellInput(sessionID))).pipe(
          Effect.forkChild,
        )
        yield* Effect.promise(flush)
        expect(hub.barrier.state(sessionID)).toBe("drained")
        yield* Fiber.join(shellRun)

        yield* Effect.promise(() => hub.ensureRunning(sessionID))
        expect(hub.barrier.state(sessionID)).toBe("drained")
        expect(hub.barrier.state(sessionID)).not.toBe("timed-out")
      } finally {
        yield* Effect.promise(() => hub.terminateAll())
      }
    }),
  )

  it.effect("a prompt run that commits nothing drains and admits the next prompt", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const pool = createFakeExecutorPool(1)
      const hub = createSessionHub({ pool, lifecycle: createLifecycleTracker(), barrier })
      const sessionID = sid("ses_admission_no_commit")

      try {
        const first = yield* Effect.promise(() => hub.ensureRunning(sessionID)).pipe(Effect.forkChild)
        yield* Effect.promise(flush)
        expect(hub.barrier.state(sessionID)).toBe("drained")
        yield* Fiber.join(first)

        yield* Effect.promise(() => hub.ensureRunning(sessionID))
        expect(hub.barrier.state(sessionID)).toBe("drained")
        expect(hub.barrier.state(sessionID)).not.toBe("timed-out")
      } finally {
        yield* Effect.promise(() => hub.terminateAll())
      }
    }),
  )
})

// ---------------------------------------------------------------------------
// Production exits that DO persist an assistant message
// ---------------------------------------------------------------------------
//
// The cases below boot the real production hub + real session-worker isolate.
// The commit is observed through the worker's `lifecycle.commit` control-port
// report (the same seam a crash/resume reads), and the reported id is compared
// with the assistant row actually persisted in SQLite.

const production = testEffect(TestLLMServer.layer)
const productionDirectories: string[] = []

afterEach(() => {
  for (const directory of productionDirectories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

function freshWorkspace(): { readonly filename: string; readonly directory: string } {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-run-admission-"))
  productionDirectories.push(directory)
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
      ["project-run-admission", "run-admission", "/tmp/run-admission", "[]", 1, 1],
    )
    connection.run(
      "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      [sessionID, "project-run-admission", "run-admission", "/tmp/run-admission", "run-admission", "1", 1, 1],
    )
  } finally {
    connection.close()
  }
}

interface DurableMessage {
  readonly id: string
  readonly role: string | undefined
  readonly summary: boolean
  readonly finish: string | undefined
}

function durableMessages(filename: string, sessionID: string): DurableMessage[] {
  const connection = openConnection({ filename, busyTimeoutMs: 5000 })
  try {
    return connection
      .all<{ readonly id: string; readonly data: string }>(
        "SELECT id, data FROM message WHERE session_id = ? ORDER BY time_created, id",
        [sessionID],
      )
      .map((row) => {
        const data = JSON.parse(row.data) as { readonly role?: string; readonly summary?: boolean; readonly finish?: string }
        return { id: row.id, role: data.role, summary: data.summary === true, finish: data.finish }
      })
  } finally {
    connection.close()
  }
}

interface ProductionFixture {
  readonly hub: SessionHub
  readonly sessionID: string
  readonly filename: string
  readonly directory: string
  dispose(): Promise<void>
}

function startProductionHub(input: {
  readonly llm: TestLLMServer["Service"]
  readonly filename: string
  readonly directory: string
  readonly sessionID: string
}): ProductionFixture {
  const db = createDbExecutor({ filename: input.filename, timeoutMs: REAL_WORKER_TRANSPORT_TIMEOUT_MS })
  const hub = createProductionSessionHub({ ...{ barrier: createTestBarrier() },
    db,
    init: () =>
      sessionWorkerInit({
        snapshot: workerSnapshot({ directory: input.directory }),
        model: { baseUrl: input.llm.url, apiKey: "test-key" },
      }),
    entry: new URL("../../src/session/worker/session-worker.ts", import.meta.url),
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
    sessionID: input.sessionID,
    filename: input.filename,
    directory: input.directory,
    dispose: async () => {
      await hub.terminateAll()
      await db.close()
    },
  }
}

function dispatchPrompt(fixture: ProductionFixture, text: string): Promise<void> {
  return fixture.hub.dispatcher.dispatch({
    sessionID: sid(fixture.sessionID),
    prompt: {
      messageID: MessageID.ascending(),
      sessionID: sid(fixture.sessionID),
      agent: "build",
      parts: [{ type: "text", text }],
    },
  })
}

describe("run admission reports the durable message (P0)", () => {
  production.live(
    "a shell run reports the assistant message it persisted",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const { filename, directory } = freshWorkspace()
        const sessionID = "ses_admission_shell_durable"
        yield* migrateLikeProduction(filename)
        seedSession(filename, sessionID)
        const fixture = startProductionHub({ llm, filename, directory, sessionID })
        try {
          yield* Effect.promise(() =>
            fixture.hub.startShell(sid(sessionID), {
              sessionID: sid(sessionID),
              agent: "build",
              command: "printf shell-ok",
            }),
          )

          const assistant = durableMessages(filename, sessionID).filter((message) => message.role === "assistant")
          expect(assistant).toHaveLength(1)
          expect(fixture.hub.lifecycle.crashState(sessionID).lastCommittedMessageID).toBe(assistant[0]!.id)
          expect(fixture.hub.barrier.state(sessionID)).toBe("drained")

          yield* llm.text("after the shell turn")
          yield* Effect.promise(() => fixture.hub.ensureRunning(sid(sessionID)))
          expect(fixture.hub.barrier.state(sessionID)).toBe("drained")
          expect(fixture.hub.barrier.state(sessionID)).not.toBe("timed-out")
        } finally {
          yield* Effect.promise(() => fixture.dispose())
        }
      }),
    REAL_WORKER_TEST_TIMEOUT_MS,
  )

  production.live(
    "a compaction stop reports the summary message it persisted",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const { filename, directory } = freshWorkspace()
        const sessionID = "ses_admission_compaction_stop_durable"
        yield* migrateLikeProduction(filename)
        seedSession(filename, sessionID)
        // Both the model turn and the compaction turn overflow: the first starts
        // auto-compaction, the second makes the compaction itself stop.
        yield* llm.error(400, { type: "error", error: { code: "context_length_exceeded" } })
        yield* llm.error(400, { type: "error", error: { code: "context_length_exceeded" } })
        const fixture = startProductionHub({ llm, filename, directory, sessionID })
        try {
          yield* Effect.promise(() => dispatchPrompt(fixture, "overflow twice"))

          const summary = durableMessages(filename, sessionID).findLast(
            (message) => message.role === "assistant" && message.summary,
          )
          expect(summary).toBeDefined()
          expect(summary!.finish).toBe("error")
          expect(fixture.hub.lifecycle.crashState(sessionID).lastCommittedMessageID).toBe(summary!.id)
          expect(fixture.hub.barrier.state(sessionID)).toBe("drained")

          yield* llm.text("after the compaction stop")
          yield* Effect.promise(() => fixture.hub.ensureRunning(sid(sessionID)))
          expect(fixture.hub.barrier.state(sessionID)).toBe("drained")
          expect(fixture.hub.barrier.state(sessionID)).not.toBe("timed-out")
        } finally {
          yield* Effect.promise(() => fixture.dispose())
        }
      }),
    REAL_WORKER_TEST_TIMEOUT_MS,
  )
})
