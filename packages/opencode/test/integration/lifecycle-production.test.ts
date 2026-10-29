// Production lifecycle wiring (D8/D9/D10).
//
// These tests drive the REAL production path: the production hub
// (`createProductionSessionHub`), the real session-worker isolate (or a real
// worker fixture for pre-run phases), the real `SessionTools.resolve` tool path,
// and the durable SQLite ledger. Nothing injects a `crashState` or a fake
// lifecycle: the phase, committed message id and in-flight tool call must come
// from the worker/dispatcher reporting into the hub's lifecycle tracker.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { openConnection } from "@opencode-ai/core/database/connection"
import { MessageID } from "@/session/schema"
import { decideAutoResume } from "@/session/ledger/resume"
import { createProductionSessionHub, type SessionHub, type WorkerCrashed } from "@/session/worker/hub"
import { createTestBarrier } from "../lib/run-barrier-test"
import { createDbExecutor } from "@/session/worker/db-executor"
import { WorkerCrashedError } from "@/session/worker/dispatch"
import { sessionWorkerInit } from "../lib/session-worker-init"
import { workerSnapshot } from "../lib/snapshot"
import { TestLLMServer } from "../lib/llm-server"
import { testEffect } from "../lib/effect"
import { sid } from "../lib/async"
import {
  REAL_WORKER_IDLE_TIMEOUT_MS,
  REAL_WORKER_TEST_TIMEOUT_MS,
  REAL_WORKER_TRANSPORT_TIMEOUT_MS,
} from "../lib/worker-timeouts"

const it = testEffect(TestLLMServer.layer)

const SESSION_WORKER = new URL("../../src/session/worker/session-worker.ts", import.meta.url)
const PHASE_WORKER = new URL("../fixture/phase-crash-worker.ts", import.meta.url)
const directories: string[] = []

function freshWorkspace(): { readonly filename: string; readonly directory: string } {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-lifecycle-production-"))
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
      ["project-lifecycle", "lifecycle", "/tmp/lifecycle", "[]", 1, 1],
    )
    connection.run(
      "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      [sessionID, "project-lifecycle", "lifecycle", "/tmp/lifecycle", "lifecycle", "1", 1, 1],
    )
  } finally {
    connection.close()
  }
}

function readState(filename: string, sessionID: string) {
  const connection = openConnection({ filename, busyTimeoutMs: 5000 })
  try {
    return {
      messages: connection
        .all<{ readonly id: string; readonly data: string }>(
          "SELECT id, data FROM message WHERE session_id = ? ORDER BY time_created, id",
          [sessionID],
        )
        .map((row) => ({ id: row.id, role: JSON.parse(row.data).role as string })),
      tools: connection.all<{ readonly call_id: string; readonly state: string; readonly outcome: string | null }>(
        "SELECT call_id, state, outcome FROM tool_call WHERE session_id = ? ORDER BY time_started, call_id",
        [sessionID],
      ),
      crashes: connection.all<{ readonly phase: string; readonly last_committed_message_id: string | null }>(
        "SELECT phase, last_committed_message_id FROM worker_crash WHERE session_id = ?",
        [sessionID],
      ),
    }
  } finally {
    connection.close()
  }
}

interface HubFixture {
  readonly hub: SessionHub
  readonly sessionID: string
  readonly filename: string
  readonly directory: string
  dispose(): Promise<void>
}

function startHub(input: {
  readonly llm: TestLLMServer["Service"]
  readonly filename: string
  readonly directory: string
  readonly sessionID: string
  readonly env?: Record<string, string | undefined>
  readonly entry?: string | URL
  readonly workerData?: Record<string, unknown>
}): HubFixture {
  const db = createDbExecutor({ filename: input.filename, timeoutMs: REAL_WORKER_TRANSPORT_TIMEOUT_MS })
  const hub = createProductionSessionHub({ ...{ barrier: createTestBarrier() },
    db,
    init: () =>
      ({
        ...sessionWorkerInit({
          snapshot: workerSnapshot({ directory: input.directory }),
          model: { baseUrl: input.llm.url, apiKey: "test-key" },
        }),
        ...(input.workerData ?? {}),
      }) as ReturnType<typeof sessionWorkerInit>,
    entry: input.entry ?? SESSION_WORKER,
    databasePath: input.filename,
    env: { OPENCODE_DB: input.filename, ...(input.env ?? {}) },
    timeoutMs: REAL_WORKER_TRANSPORT_TIMEOUT_MS,
    runIdleTimeoutMs: REAL_WORKER_IDLE_TIMEOUT_MS,
    ports: {
      hubProxy: {
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

function dispatchPrompt(fixture: HubFixture, text: string): Promise<void> {
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

async function crashPayload(run: Promise<void>, timeoutMs = REAL_WORKER_TRANSPORT_TIMEOUT_MS): Promise<WorkerCrashed> {
  const outcome = await Promise.race([
    run.then(
      () => ({ kind: "completed" as const }),
      (error: unknown) => ({ kind: "failed" as const, error }),
    ),
    new Promise<{ kind: "timeout" }>((resolve) => setTimeout(() => resolve({ kind: "timeout" }), timeoutMs)),
  ])
  if (outcome.kind === "completed") throw new Error("expected the run to crash, but it completed")
  if (outcome.kind === "timeout") throw new Error("timed out waiting for the worker crash")
  if (!(outcome.error instanceof WorkerCrashedError)) {
    throw new Error(`expected WorkerCrashedError, received ${String(outcome.error)}`)
  }
  const payload = outcome.error.payload
  if (payload === undefined) throw new Error("crashed worker did not report a payload")
  return payload
}

afterEach(() => {
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("production lifecycle wiring", () => {
  it.live(
    "a real tool turn records the durable started row and the committed assistant message",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const { filename, directory } = freshWorkspace()
        const sessionID = "ses_lifecycle_tool_ok"
        const target = path.join(directory, "target.txt")
        fs.writeFileSync(target, "hello lifecycle\n")
        yield* migrateLikeProduction(filename)
        seedSession(filename, sessionID)
        yield* llm.tool("todowrite", {
          todos: [{ content: "verify lifecycle", status: "pending", priority: "high" }],
        })
        yield* llm.text("done")

        const fixture = startHub({ llm, filename, directory, sessionID })
        try {
          yield* Effect.promise(() => dispatchPrompt(fixture, "read the target"))
          const state = readState(filename, sessionID)
          expect(state.tools).toHaveLength(1)
          expect(state.tools[0]).toMatchObject({ state: "finished", outcome: "ok" })

          const assistant = state.messages.filter((message) => message.role === "assistant")
          expect(assistant.length).toBeGreaterThan(0)
          expect(fixture.hub.lifecycle.crashState(sessionID)).toMatchObject({
            lastCommittedMessageID: assistant.at(-1)!.id,
            inFlightCallIDs: [],
          })
        } finally {
          yield* Effect.promise(() => fixture.dispose())
        }
      }),
    REAL_WORKER_TEST_TIMEOUT_MS,
  )

  it.live(
    "crashing after the tool started leaves an indeterminate call that blocks auto-resume",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const { filename, directory } = freshWorkspace()
        const sessionID = "ses_lifecycle_tool_crash"
        const target = path.join(directory, "target.txt")
        fs.writeFileSync(target, "hello lifecycle\n")
        yield* migrateLikeProduction(filename)
        seedSession(filename, sessionID)
        yield* llm.text("first")
        yield* llm.tool("read", { filePath: target })

        const fixture = startHub({
          llm,
          filename,
          directory,
          sessionID,
          env: { OPENCODE_TEST_FAULTS: "crashWorker@tool:effect" },
        })
        try {
          yield* Effect.promise(() => dispatchPrompt(fixture, "first turn"))
          expect(fixture.hub.lifecycle.crashState(sessionID).lastCommittedMessageID).not.toBeNull()
          const payload = yield* Effect.promise(() => crashPayload(dispatchPrompt(fixture, "read the target")))
          const indeterminate = fixture.hub.lifecycle.indeterminate(sessionID)
          expect(indeterminate).toHaveLength(1)
          expect(indeterminate[0]!.sideEffecting).toBe(true)

          const state = readState(filename, sessionID)
          expect(state.tools).toHaveLength(1)
          expect(state.tools[0]).toMatchObject({ state: "started", outcome: null })

          const decision = decideAutoResume({
            sessionID,
            phase: payload.phase,
            attempts: fixture.hub.lifecycle.attempts(sessionID),
            cancelled: fixture.hub.lifecycle.cancelled(sessionID),
            lastCommittedMessageID: payload.lastCommittedMessageID,
            indeterminate,
          })
          expect(decision).toMatchObject({ resume: false, reason: "indeterminate" })
        } finally {
          yield* Effect.promise(() => fixture.dispose())
        }
      }),
    REAL_WORKER_TEST_TIMEOUT_MS,
  )

  it.live(
    "a crashed run with a real committed message resumes from that id, not a stub",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const { filename, directory } = freshWorkspace()
        const sessionID = "ses_lifecycle_resume"
        yield* migrateLikeProduction(filename)
        seedSession(filename, sessionID)
        yield* llm.text("first")
        yield* llm.hang

        const fixture = startHub({ llm, filename, directory, sessionID })
        try {
          yield* Effect.promise(() => dispatchPrompt(fixture, "first turn"))
          const committed = fixture.hub.lifecycle.crashState(sessionID).lastCommittedMessageID
          expect(committed).not.toBeNull()
          expect(readState(filename, sessionID).messages.some((message) => message.id === committed)).toBe(true)

          const before = yield* llm.calls
          const second = crashPayload(dispatchPrompt(fixture, "second turn"))
          yield* llm.wait(before + 1)
          const worker = fixture.hub.dispatcher.registry.worker(sid(sessionID))
          expect(worker).toBeDefined()
          yield* Effect.promise(() => worker!.terminate())

          const payload = yield* Effect.promise(() => second)
          expect(payload.lastCommittedMessageID).toBe(committed)

          const decision = decideAutoResume({
            sessionID,
            phase: payload.phase,
            attempts: fixture.hub.lifecycle.attempts(sessionID),
            cancelled: fixture.hub.lifecycle.cancelled(sessionID),
            lastCommittedMessageID: payload.lastCommittedMessageID,
            indeterminate: fixture.hub.lifecycle.indeterminate(sessionID),
          })
          expect(decision).toMatchObject({ resume: true, fromMessageID: committed })
        } finally {
          yield* Effect.promise(() => fixture.dispose())
        }
      }),
    REAL_WORKER_TEST_TIMEOUT_MS,
  )

  it.live(
    "real crashes report boot/materialize/stream/tool/commit phases and the committed id",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const cases = [
          { phase: "boot", entry: PHASE_WORKER, env: {}, tool: false },
          { phase: "materialize", entry: PHASE_WORKER, env: {}, tool: false },
          { phase: "stream", entry: SESSION_WORKER, env: { OPENCODE_TEST_FAULTS: "crashWorker@prompt:stream" }, tool: false },
          { phase: "tool", entry: SESSION_WORKER, env: { OPENCODE_TEST_FAULTS: "crashWorker@tool:effect" }, tool: true },
          { phase: "commit", entry: SESSION_WORKER, env: { OPENCODE_TEST_FAULTS: "crashWorker@prompt:commit" }, tool: false },
        ] as const

        for (const scenario of cases) {
          const { filename, directory } = freshWorkspace()
          const sessionID = `ses_phase_${scenario.phase}`
          const target = path.join(directory, "target.txt")
          fs.writeFileSync(target, "hello phase\n")
          yield* migrateLikeProduction(filename)
          seedSession(filename, sessionID)
          yield* llm.reset
          if (scenario.tool) yield* llm.tool("glob", { pattern: "*.txt", path: directory })
          else yield* llm.text("phase")

          const fixture = startHub({
            llm,
            filename,
            directory,
            sessionID,
            entry: scenario.entry,
            env: scenario.env,
            workerData: { phase: scenario.phase },
          })
          try {
            const payload = yield* Effect.promise(() =>
              crashPayload(dispatchPrompt(fixture, "phase turn")).catch((error: unknown) => {
                throw new Error(`${scenario.phase}: ${error instanceof Error ? error.message : String(error)}`)
              }),
            )
            expect(payload.phase).toBe(scenario.phase)
            if (scenario.phase === "commit") {
              expect(payload.lastCommittedMessageID).not.toBeNull()
              expect(
                readState(filename, sessionID).messages.some(
                  (message) => message.id === payload.lastCommittedMessageID,
                ),
              ).toBe(true)
            } else {
              expect(payload.lastCommittedMessageID).toBeNull()
            }
          } finally {
            yield* Effect.promise(() => fixture.dispose())
          }
        }
      }),
    REAL_WORKER_TEST_TIMEOUT_MS,
  )
})
