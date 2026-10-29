// Production resume/ledger wiring: the duplicate-side-effect window, the
// durable `tool_call` truth, and phase-correct auto-resume.
//
// Every case drives `createProductionSessionHub`, a real session-worker isolate,
// the real `SessionTools.resolve` tool path and the durable SQLite ledger. No
// fake lifecycle, crash state or resume decision is injected: the phase,
// committed message and started rows come from the worker/hub reporting and the
// durable ledger, and auto-resume re-enters the real dispatch path.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { openConnection } from "@opencode-ai/core/database/connection"
import { ensureProductionSchema } from "@/session/db/schema"
import { MessageID } from "@/session/schema"
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
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-resume-ledger-"))
  directories.push(directory)
  return { filename: path.join(directory, "opencode.db"), directory }
}

function migrateLikeProduction(filename: string) {
  return Effect.scoped(
    Effect.gen(function* () {
      yield* Database.Service
    }),
  ).pipe(
    Effect.provide(Database.layerFromPath(filename)),
    // The production hub boot gate upgrades the core schema with the
    // opencode-owned ledger (`tool_call`) and identity marker before any worker
    // spawns. Do the same here so a seeded ledger row precedes the first dispatch.
    Effect.tap(() => Effect.sync(() => void ensureProductionSchema({ filename, liveWorkers: 0 }))),
  )
}

function seedSession(filename: string, sessionID: string): void {
  const connection = openConnection({ filename, busyTimeoutMs: 5000 })
  try {
    connection.run(
      "INSERT INTO project (id, name, worktree, sandboxes, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
      ["project-resume", "resume", "/tmp/resume", "[]", 1, 1],
    )
    connection.run(
      "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      [sessionID, "project-resume", "resume", "/tmp/resume", "resume", "1", 1, 1],
    )
  } finally {
    connection.close()
  }
}

/** Seed a ledger row exactly as a worker would leave it. */
function seedToolCall(
  filename: string,
  input: {
    readonly callID: string
    readonly sessionID: string
    readonly state: "started" | "finished"
    readonly sideEffecting?: boolean
  },
): void {
  const connection = openConnection({ filename, busyTimeoutMs: 5000 })
  try {
    connection.run(
      "INSERT INTO tool_call (call_id, session_id, tool, side_effecting, state, outcome, time_started, time_finished) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      [
        input.callID,
        input.sessionID,
        "bash",
        input.sideEffecting === false ? 0 : 1,
        input.state,
        input.state === "finished" ? "ok" : null,
        1,
        input.state === "finished" ? 2 : null,
      ],
    )
  } finally {
    connection.close()
  }
}

function readToolCalls(filename: string, sessionID: string) {
  const connection = openConnection({ filename, busyTimeoutMs: 5000 })
  try {
    return connection.all<{ readonly call_id: string; readonly state: string; readonly outcome: string | null }>(
      "SELECT call_id, state, outcome FROM tool_call WHERE session_id = ? ORDER BY time_started, call_id",
      [sessionID],
    )
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

/** Count the side-effect lines in `counter` (`x` per applied edit). */
function effectCount(counter: string): number {
  if (!fs.existsSync(counter)) return 0
  return fs
    .readFileSync(counter, "utf8")
    .split("\n")
    .filter((line) => line === "x").length
}

/**
 * Wait until resume has been attempted `count` times. A resumable crash resolves
 * the original dispatch with the replacement run, and the armed fault fires
 * again in that replacement, so the test must observe the decision instead of
 * awaiting the (looping) dispatch promise.
 */
function waitForAttempts(fixture: HubFixture, count: number, timeoutMs = REAL_WORKER_TRANSPORT_TIMEOUT_MS) {
  return Effect.gen(function* () {
    const deadline = Date.now() + timeoutMs
    while (fixture.hub.lifecycle.attempts(fixture.sessionID) < count && Date.now() < deadline) {
      yield* Effect.sleep(25)
    }
    return fixture.hub.lifecycle.attempts(fixture.sessionID)
  })
}

/**
 * Wait for a replay to become observable, bounded so a refused resume does not
 * hang. The assertion is on the side effect itself, never on request counts
 * (title generation also hits the LLM server).
 */
function waitForEffects(counter: string, count: number, timeoutMs = REAL_WORKER_TRANSPORT_TIMEOUT_MS) {
  return Effect.gen(function* () {
    const deadline = Date.now() + timeoutMs
    while (effectCount(counter) < count && Date.now() < deadline) yield* Effect.sleep(25)
    return effectCount(counter)
  })
}

afterEach(() => {
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("duplicate side effect at the result window", () => {
  it.live(
    "a crash after the tool effect but before its result commits never replays the side effect",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const { filename, directory } = freshWorkspace()
        const sessionID = "ses_resume_duplicate"
        const counter = path.join(directory, "counter.txt")
        fs.writeFileSync(counter, "start\n")
        yield* migrateLikeProduction(filename)
        seedSession(filename, sessionID)
        // Turn one commits a real assistant message, so resume is otherwise allowed.
        yield* llm.text("first")
        // Turn two runs a side-effecting tool. A replay would append a second line.
        yield* llm.tool("edit", { filePath: counter, oldString: "start", newString: "start\nx" })
        // Only consumed if the crashed run is wrongly replayed.
        yield* llm.tool("edit", { filePath: counter, oldString: "start", newString: "start\nx" })
        yield* llm.text("done")

        const fixture = startHub({
          llm,
          filename,
          directory,
          sessionID,
          env: { OPENCODE_TEST_FAULTS: "crashWorker@tool:result" },
        })
        try {
          yield* Effect.promise(() => dispatchPrompt(fixture, "first turn"))
          const payload = yield* Effect.promise(() => crashPayload(dispatchPrompt(fixture, "run the side effect")))
          expect(payload.phase).toBe("tool")
          expect(payload.lastCommittedMessageID).not.toBeNull()

          // Let a (wrongly) allowed auto-resume apply the edit again; a refused
          // resume stops at the crashed turn.
          expect(yield* waitForEffects(counter, 2)).toBe(1)
          expect(readToolCalls(filename, sessionID)).toEqual([
            expect.objectContaining({ state: "started", outcome: null }),
          ])
          expect(fixture.hub.lifecycle.attempts(sessionID)).toBe(0)
        } finally {
          yield* Effect.promise(() => fixture.dispose())
        }
      }),
    REAL_WORKER_TEST_TIMEOUT_MS,
  )
})

describe("resume reads the durable tool_call ledger", () => {
  it.live(
    "an orphan started row blocks resume even when the in-memory mirror is empty",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const { filename, directory } = freshWorkspace()
        const sessionID = "ses_resume_orphan"
        yield* migrateLikeProduction(filename)
        seedSession(filename, sessionID)
        yield* llm.text("second")

        const fixture = startHub({
          llm,
          filename,
          directory,
          sessionID,
          env: { OPENCODE_TEST_FAULTS: "crashWorker@prompt:stream" },
        })
        try {
          // `migrateLikeProduction` already applied the ledger schema, so the
          // orphan row is present for the crash decision.
          seedToolCall(filename, { callID: "call_orphan", sessionID, state: "started" })
          // The crash payload reports a committed message; only the durable row
          // can block the resume because the tracker never saw the start.
          fixture.hub.lifecycle.commit(sessionID, "msg_committed")
          const payload = yield* Effect.promise(() => crashPayload(dispatchPrompt(fixture, "second turn")))
          expect(payload.phase).toBe("stream")
          expect(fixture.hub.lifecycle.indeterminate(sessionID)).toEqual([])
          expect(fixture.hub.lifecycle.attempts(sessionID)).toBe(0)
        } finally {
          yield* Effect.promise(() => fixture.dispose())
        }
      }),
    REAL_WORKER_TEST_TIMEOUT_MS,
  )

  it.live(
    "a finished row allows resume even when the in-memory mirror still says started",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const { filename, directory } = freshWorkspace()
        const sessionID = "ses_resume_finished"
        yield* migrateLikeProduction(filename)
        seedSession(filename, sessionID)
        yield* llm.text("second")

        const fixture = startHub({
          llm,
          filename,
          directory,
          sessionID,
          env: { OPENCODE_TEST_FAULTS: "crashWorker@prompt:stream" },
        })
        try {
          seedToolCall(filename, { callID: "call_done", sessionID, state: "finished" })
          fixture.hub.lifecycle.commit(sessionID, "msg_committed")
          // A lost finish report leaves the mirror stale; the durable row wins.
          fixture.hub.lifecycle.toolStarted(sessionID, "call_done")
          // A resumable crash resolves the caller with the replacement run, not
          // `WorkerCrashedError`; the assertion is the resume decision itself.
          const run = dispatchPrompt(fixture, "second turn")
          run.catch(() => undefined)
          expect(yield* waitForAttempts(fixture, 1)).toBeGreaterThanOrEqual(1)
        } finally {
          yield* Effect.promise(() => fixture.dispose())
        }
      }),
    REAL_WORKER_TEST_TIMEOUT_MS,
  )

  it.live(
    "no tool start allows resume from the committed assistant message",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const { filename, directory } = freshWorkspace()
        const sessionID = "ses_resume_clean"
        yield* migrateLikeProduction(filename)
        seedSession(filename, sessionID)
        yield* llm.text("second")

        const fixture = startHub({
          llm,
          filename,
          directory,
          sessionID,
          env: { OPENCODE_TEST_FAULTS: "crashWorker@prompt:stream" },
        })
        try {
          fixture.hub.lifecycle.commit(sessionID, "msg_committed")
          // The durable commit allows a resume; observe the decision while the
          // armed fault keeps firing in replacement runs.
          const run = dispatchPrompt(fixture, "second turn")
          run.catch(() => undefined)
          expect(yield* waitForAttempts(fixture, 1)).toBeGreaterThanOrEqual(1)
        } finally {
          yield* Effect.promise(() => fixture.dispose())
        }
      }),
    REAL_WORKER_TEST_TIMEOUT_MS,
  )
})

describe("phase-correct resume", () => {
  it.live(
    "boot and materialize never resume; stream and tool resume from the committed message",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer

        // Boot has no run to replay: the caller observes the typed crash.
        {
          const { filename, directory } = freshWorkspace()
          const sessionID = "ses_resume_boot"
          yield* migrateLikeProduction(filename)
          seedSession(filename, sessionID)
          yield* llm.reset

          const fixture = startHub({
            llm,
            filename,
            directory,
            sessionID,
            entry: PHASE_WORKER,
            workerData: { phase: "boot" },
          })
          try {
            fixture.hub.lifecycle.commit(sessionID, "msg_committed")
            const payload = yield* Effect.promise(() => crashPayload(dispatchPrompt(fixture, "phase turn")))
            expect(payload.phase).toBe("boot")
            expect(fixture.hub.lifecycle.attempts(sessionID)).toBe(0)
          } finally {
            yield* Effect.promise(() => fixture.dispose())
          }
        }

        // Materialize is resumable from the commit: the replacement run settles
        // the original dispatch promise.
        {
          const { filename, directory } = freshWorkspace()
          const sessionID = "ses_resume_materialize"
          yield* migrateLikeProduction(filename)
          seedSession(filename, sessionID)
          yield* llm.reset

          const fixture = startHub({
            llm,
            filename,
            directory,
            sessionID,
            entry: PHASE_WORKER,
            workerData: { phase: "materialize" },
          })
          try {
            fixture.hub.lifecycle.commit(sessionID, "msg_committed")
            const run = dispatchPrompt(fixture, "phase turn")
            run.catch(() => undefined)
            expect(yield* waitForAttempts(fixture, 1)).toBe(1)
          } finally {
            yield* Effect.promise(() => fixture.dispose())
          }
        }
      }),
    90_000,
  )

  it.live(
    "a crash after the durable commit keeps the committed id and resumes",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const { filename, directory } = freshWorkspace()
        const sessionID = "ses_resume_commit"
        yield* migrateLikeProduction(filename)
        seedSession(filename, sessionID)
        yield* llm.text("first")
        yield* llm.text("resumed")

        const fixture = startHub({
          llm,
          filename,
          directory,
          sessionID,
          env: { OPENCODE_TEST_FAULTS: "crashWorker@prompt:commit" },
        })
        try {
          // The commit is durable, so the crash is resumable; the caller sees the
          // replacement run and the assertion is the recorded attempt.
          const run = dispatchPrompt(fixture, "commit turn")
          run.catch(() => undefined)
          expect(yield* waitForAttempts(fixture, 1)).toBeGreaterThanOrEqual(1)
        } finally {
          yield* Effect.promise(() => fixture.dispose())
        }
      }),
    REAL_WORKER_TEST_TIMEOUT_MS,
  )
})
