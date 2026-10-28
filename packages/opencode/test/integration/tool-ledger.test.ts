// a cancelled/crashed tool must not wedge the session barrier.
//
// The red state: `withToolLedger` used `Effect.catch`, which observes typed
// failures only. An interrupted tool effect (session cancel / fiber interrupt)
// left the durable row `started` and the lifecycle call in flight, so the run
// barrier's `tools` phase never settled, expired as `timed-out`, and every
// later run was refused with `RunBarrierTimeout`.
//
// These tests drive the real production tool wrapper (`SessionTools.resolve`),
// a real SQLite ledger, the real lifecycle tracker and the real run barrier.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Database } from "@opencode-ai/core/database/database"
import { openConnection } from "@opencode-ai/core/database/connection"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Agent } from "@/agent/agent"
import { MCP } from "@/mcp"
import { Permission } from "@/permission"
import { Provider } from "@/provider/provider"
import { Session } from "@/session/session"
import { MessageID, SessionID } from "@/session/schema"
import { SessionProcessor } from "@/session/processor"
import { SessionTools } from "@/session/tools"
import { ToolRegistry, type RegisteredTool } from "@/tool/registry"
import { Truncate } from "@/tool/truncate"
import { Plugin } from "@/plugin"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { createBunSqliteConnection } from "@/session/writer/connection"
import { LEDGER_SQL, createToolCallLedger, type ToolCallLedgerHandle } from "@/session/ledger/tool-call"
import { createLifecycleTracker, type LifecycleTracker } from "@/session/ledger/lifecycle"
import {
  RUN_BARRIER_TIMEOUT_MS,
  RunBarrierTimeout,
  createRunBarrier,
  type RunBarrierPhases,
  type RunBarrierScheduler,
} from "@/session/run-barrier"
import { createProductionSessionHub, type SessionHub } from "@/session/worker/hub"
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
import { Effect, Layer, Schema } from "effect"

const callID = "call-p0"
const sessionID = SessionID.make("ses_tool_p0")
const messageID = MessageID.ascending()
// The production tool path keys the durable ledger by the owning assistant message.
const ledgerKey = `${messageID}:${callID}`

const agent: Agent.Info = {
  name: "build",
  mode: "primary",
  options: {},
  permission: [{ permission: "*", pattern: "*", action: "allow" }],
}

const model = {
  providerID: ProviderV2.ID.make("test"),
  api: { id: "test-model" },
} as Provider.Model

function fakeMcp() {
  return MCP.Service.of({
    tools: () => Effect.succeed({}),
    clients: () => Effect.succeed({}),
  } as Partial<MCP.Interface> as MCP.Interface)
}

const fakePlugin = Plugin.Service.of({
  init: () => Effect.void,
  list: () => Effect.succeed([]),
  trigger: (_name, _input, output) => Effect.succeed(output),
} satisfies Plugin.Interface)

const fakePermission = Permission.Service.of({
  ask: () => Effect.void,
  reply: () => Effect.void,
  list: () => Effect.succeed([]),
} satisfies Permission.Interface)

const fakeTruncate = Truncate.Service.of({
  cleanup: () => Effect.void,
  write: () => Effect.succeed("output.txt"),
  output: (text: string) => Effect.succeed({ content: text, truncated: false }),
  limits: () => Effect.succeed({ maxLines: 2000, maxBytes: 50 * 1024 }),
} satisfies Truncate.Interface)

function layerWith(tool: RegisteredTool) {
  return Layer.mergeAll(
    Layer.succeed(Plugin.Service, fakePlugin),
    Layer.succeed(Permission.Service, fakePermission),
    Layer.succeed(MCP.Service, fakeMcp()),
    Layer.succeed(Truncate.Service, fakeTruncate),
    RuntimeFlags.layer(),
    Layer.succeed(
      ToolRegistry.Service,
      ToolRegistry.Service.of({
        ids: () => Effect.succeed([tool.id]),
        all: () => Effect.succeed([tool]),
        named: () => Effect.die("unused"),
        tools: () => Effect.succeed([tool]),
      }),
    ),
  )
}

const processor = {
  message: {
    id: messageID,
    sessionID,
    role: "assistant",
    parentID: MessageID.ascending(),
    agent: "build",
    mode: "build",
    path: { cwd: "/tmp", root: "/tmp" },
    cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    modelID: ModelV2.ID.make("test-model"),
    providerID: ProviderV2.ID.make("test"),
    time: { created: 1 },
  } satisfies SessionV1.Assistant,
  updateToolCall: () => Effect.succeed(undefined),
  completeToolCall: () => Effect.void,
} satisfies Pick<SessionProcessor.Handle, "message" | "updateToolCall" | "completeToolCall">

function tool(execute: RegisteredTool["execute"]): RegisteredTool {
  return {
    id: "ledger",
    source: "builtin",
    description: "records ledger state",
    parameters: Schema.Struct({}),
    jsonSchema: { type: "object", properties: {} },
    execute,
  } satisfies RegisteredTool
}

interface FakeClock {
  readonly scheduler: RunBarrierScheduler
  advance(ms: number): void
}

/** Deterministic barrier clock: only an explicit `advance` fires the bound. */
function fakeClock(): FakeClock {
  let now = 0
  let nextID = 1
  const timers = new Map<number, { readonly at: number; readonly run: () => void }>()
  return {
    scheduler: {
      schedule: (run, delayMs) => {
        const id = nextID++
        timers.set(id, { at: now + delayMs, run })
        return id
      },
      cancel: (handle) => void timers.delete(handle as number),
    },
    advance(ms) {
      now += ms
      for (const [id, timer] of [...timers.entries()]) {
        if (timer.at <= now) {
          timers.delete(id)
          timer.run()
        }
      }
    },
  }
}

/** Drive the real lifecycle tracker through the promise-shaped reporting seam. */
function reportingFor(lifecycle: LifecycleTracker) {
  return {
    toolStarted: (session: string, call: string, sideEffecting = true) =>
      Promise.resolve(lifecycle.toolStarted(session, call, sideEffecting)),
    toolFinished: (session: string, call: string) => Promise.resolve(lifecycle.toolFinished(session, call)),
  }
}

function ledgerForTest() {
  const connection = createBunSqliteConnection({ filename: ":memory:" })
  connection.exec(LEDGER_SQL)
  return { connection, ledger: createToolCallLedger(connection) }
}

async function resolveTool(
  toolDefinition: RegisteredTool,
  ledger: ToolCallLedgerHandle,
  lifecycle: LifecycleTracker,
) {
  const tools = await Effect.runPromise(
    Effect.gen(function* () {
      return yield* SessionTools.resolve({
        agent,
        model,
        session: { id: sessionID, permission: [] } as unknown as Session.Info,
        processor,
        bypassAgentCheck: false,
        messages: [],
        promptOps: {} as never,
        ledger,
        lifecycle: reportingFor(lifecycle),
      })
    }).pipe(Effect.provide(layerWith(toolDefinition))),
  )
  const execute = tools[toolDefinition.id]?.execute
  if (!execute) throw new Error("tool is missing execute")
  return execute
}

function run(execute: Awaited<ReturnType<typeof resolveTool>>) {
  return Promise.resolve(execute({}, { toolCallId: callID, abortSignal: new AbortController().signal, messages: [] }))
}

function settleRunPhases(lifecycle: LifecycleTracker) {
  lifecycle.settlePhase(String(sessionID), "commits")
  lifecycle.settlePhase(String(sessionID), "projectors")
  lifecycle.settlePhase(String(sessionID), "promotion")
  if (lifecycle.indeterminate(String(sessionID)).length === 0) {
    lifecycle.settlePhase(String(sessionID), "tools")
  }
}

/**
 * A finished run waits on the real barrier. The fake clock is advanced only
 * after every phase microtask has had a chance to settle, so a drained barrier
 * cancels its timer instead of racing the timeout.
 */
async function finishRun(barrier: ReturnType<typeof createRunBarrier>, lifecycle: LifecycleTracker, clock: FakeClock) {
  settleRunPhases(lifecycle)
  const pending = barrier.wait(String(sessionID), lifecycle.phases(String(sessionID)) as RunBarrierPhases)
  await new Promise<void>((resolve) => setTimeout(resolve, 0))
  clock.advance(RUN_BARRIER_TIMEOUT_MS)
  return pending
}

describe("interrupted tool finishes and releases the run barrier", () => {
  test("an interrupted side-effecting tool finishes `error` and the next run is admitted", async () => {
    const { ledger } = ledgerForTest()
    const lifecycle = createLifecycleTracker()
    const clock = fakeClock()
    const barrier = createRunBarrier({ scheduler: clock.scheduler })
    const execute = await resolveTool(tool(() => Effect.interrupt), ledger, lifecycle)

    await expect(run(execute)).rejects.toThrow()

    // RED (before the fix): the row stays `started` and the call stays in flight.
    expect(ledger.list(String(sessionID))).toEqual([
      expect.objectContaining({ callID: ledgerKey, state: "finished", outcome: "error" }),
    ])
    expect(ledger.indeterminate(String(sessionID))).toEqual([])
    expect(lifecycle.indeterminate(String(sessionID))).toEqual([])

    await expect(finishRun(barrier, lifecycle, clock)).resolves.toEqual({
      sessionID: String(sessionID),
      status: "drained",
    })
    expect(barrier.state(String(sessionID))).toBe("drained")

    // The run after a cancelled tool is admitted; it is not refused forever.
    await expect(finishRun(barrier, lifecycle, clock)).resolves.toEqual({
      sessionID: String(sessionID),
      status: "drained",
    })
  })

  test("a defect also finishes the row instead of leaving it pending", async () => {
    const { ledger } = ledgerForTest()
    const lifecycle = createLifecycleTracker()
    const execute = await resolveTool(tool(() => Effect.die(new Error("simulated tool defect"))), ledger, lifecycle)

    await expect(run(execute)).rejects.toThrow()

    expect(ledger.list(String(sessionID))).toEqual([
      expect.objectContaining({ callID: ledgerKey, state: "finished", outcome: "error" }),
    ])
    expect(lifecycle.indeterminate(String(sessionID))).toEqual([])
  })

  test("an interrupted tool cannot leave a timed-out barrier behind", async () => {
    const { ledger } = ledgerForTest()
    const lifecycle = createLifecycleTracker()
    const clock = fakeClock()
    const barrier = createRunBarrier({ scheduler: clock.scheduler })
    const execute = await resolveTool(tool(() => Effect.interrupt), ledger, lifecycle)

    await expect(run(execute)).rejects.toThrow()
    const first = await finishRun(barrier, lifecycle, clock)
    expect(first.status).toBe("drained")
    expect(barrier.state(String(sessionID))).not.toBe("timed-out")

    const rejected = await finishRun(barrier, lifecycle, clock).catch((error: unknown) => error)
    expect(rejected).not.toBeInstanceOf(RunBarrierTimeout)
  })
})

describe("same-transaction result + finish", () => {
  test("a failure before the result rolls both back and leaves the call indeterminate", () => {
    const { connection, ledger } = ledgerForTest()
    connection.exec("CREATE TABLE note (id TEXT NOT NULL)")
    ledger.start({ callID, sessionID: String(sessionID), tool: "write" })

    expect(() =>
      ledger.finishWithResult({ callID, sessionID: String(sessionID), outcome: "ok" }, () => {
        connection.run("INSERT INTO note (id) VALUES (?)", ["result"])
        throw new Error("crash before the result committed")
      }),
    ).toThrow("crash before the result committed")

    expect(connection.all("SELECT id FROM note")).toEqual([])
    expect(ledger.indeterminate(String(sessionID))[0]).toMatchObject({ callID, state: "indeterminate" })
  })

  test("the result and the finish commit together; a crash after commits retry-safe", () => {
    const { connection, ledger } = ledgerForTest()
    connection.exec("CREATE TABLE note (id TEXT NOT NULL)")
    ledger.start({ callID, sessionID: String(sessionID), tool: "write" })

    const value = ledger.finishWithResult({ callID, sessionID: String(sessionID), outcome: "ok" }, () => {
      connection.run("INSERT INTO note (id) VALUES (?)", ["result"])
      return "committed"
    })

    expect(value).toBe("committed")
    expect(connection.all("SELECT id FROM note")).toEqual([{ id: "result" }])
    expect(ledger.list(String(sessionID))[0]).toMatchObject({ state: "finished", outcome: "ok" })
    expect(ledger.indeterminate(String(sessionID))).toEqual([])
  })

  test("an outer transaction owns the commit; the ledger never issues a nested BEGIN", () => {
    const { connection, ledger } = ledgerForTest()
    connection.exec("CREATE TABLE note (id TEXT NOT NULL)")
    ledger.start({ callID, sessionID: String(sessionID), tool: "write" })

    connection.exec("BEGIN IMMEDIATE")
    connection.run("INSERT INTO note (id) VALUES (?)", ["outer"])
    // A savepoint composes with the open transaction instead of raising
    // "cannot start a transaction within a transaction".
    ledger.finishWithResult({ callID, sessionID: String(sessionID), outcome: "ok" }, () => undefined)
    expect(ledger.list(String(sessionID))[0]?.state).toBe("finished")
    connection.exec("ROLLBACK")

    // The outer rollback takes the result and the ledger finish with it.
    expect(connection.all("SELECT id FROM note")).toEqual([])
    expect(ledger.indeterminate(String(sessionID))[0]).toMatchObject({ callID, state: "indeterminate" })
  })
})

// ---------------------------------------------------------------------------
// Real worker: the `tool:result` fault fires after the effect returns but
// before the durable result + finish boundary. A crash there must leave the
// durable row `started` (orphan) and no completed tool part, so auto-resume
// refuses instead of replaying a side-effecting effect whose result was lost.
// ---------------------------------------------------------------------------

const it = testEffect(TestLLMServer.layer)
const SESSION_WORKER = new URL("../../src/session/worker/session-worker.ts", import.meta.url)
const directories: string[] = []

function freshWorkspace() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-tool-ledger-p0-"))
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
      ["project-tool-p0", "tool-p0", "/tmp/tool-p0", "[]", 1, 1],
    )
    connection.run(
      "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      [sessionID, "project-tool-p0", "tool-p0", "/tmp/tool-p0", "tool-p0", "1", 1, 1],
    )
  } finally {
    connection.close()
  }
}

function readRows(filename: string, sessionID: string) {
  const connection = openConnection({ filename, busyTimeoutMs: 5000 })
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
    }
  } finally {
    connection.close()
  }
}

/** Count applied edits (`x` lines) in the side-effect counter file. */
function effectCount(counter: string): number {
  if (!fs.existsSync(counter)) return 0
  return fs
    .readFileSync(counter, "utf8")
    .split("\n")
    .filter((line) => line === "x").length
}

function dispatchPrompt(hub: SessionHub, sessionID: string, text: string): Promise<void> {  return hub.dispatcher.dispatch({
    sessionID: sid(sessionID),
    prompt: {
      messageID: MessageID.ascending(),
      sessionID: sid(sessionID),
      agent: "build",
      parts: [{ type: "text", text }],
    },
  })
}

async function crashPayload(run: Promise<void>, timeoutMs = REAL_WORKER_TRANSPORT_TIMEOUT_MS) {
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
  return outcome.error.payload
}

interface RealHubFixture {
  readonly hub: SessionHub
  readonly dispose: () => Promise<void>
}

function startHub(input: {
  readonly llm: TestLLMServer["Service"]
  readonly filename: string
  readonly directory: string
  readonly env?: Record<string, string | undefined>
}): RealHubFixture {
  const db = createDbExecutor({ filename: input.filename, timeoutMs: REAL_WORKER_TRANSPORT_TIMEOUT_MS })
  const hub = createProductionSessionHub({
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
    dispose: async () => {
      await hub.terminateAll()
      await db.close()
    },
  }
}

afterEach(() => {
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("effect-returned but result-not-committed crash (real worker)", () => {
  it.live(
    "a crash at tool:result leaves the row started and no completed result part",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const { filename, directory } = freshWorkspace()
        const sessionID = "ses_p0_tool_result_crash"
        yield* migrateLikeProduction(filename)
        seedSession(filename, sessionID)
        yield* llm.tool("todowrite", { todos: [{ content: "side effect", status: "pending", priority: "high" }] })
        yield* llm.text("done")

        const fixture = startHub({
          llm,
          filename,
          directory,
          env: { OPENCODE_TEST_FAULTS: "crashWorker@tool:result" },
        })
        try {
          const payload = yield* Effect.promise(() => crashPayload(dispatchPrompt(fixture.hub, sessionID, "write")))
          expect(payload?.phase).toBe("tool")

          const rows = readRows(filename, sessionID)
          expect(rows.tools).toHaveLength(1)
          expect(rows.tools[0]).toMatchObject({ state: "started", outcome: null })
          expect(rows.parts.filter((part) => part.type === "tool" && part.state?.status === "completed")).toEqual([])
          // The durable ledger key is message-scoped; lifecycle keeps the
          // provider call id. Both identify the same orphaned call.
          expect(rows.tools[0]!.call_id).toMatch(/:call_1$/)
          expect(fixture.hub.lifecycle.indeterminate(sessionID).map((call) => call.callID)).toEqual(["call_1"])
          // The durable orphan blocks auto-resume. Barrier/retirement recovery
          // for the dead generation is covered by `barrier-recovery`.
          expect(fixture.hub.lifecycle.attempts(sessionID)).toBe(0)
        } finally {
          yield* Effect.promise(() => fixture.dispose())
        }
      }),
    REAL_WORKER_TEST_TIMEOUT_MS,
  )

  it.live(
    "a crash at tool:settled rolls the result and settlement back together",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const { filename, directory } = freshWorkspace()
        const sessionID = "ses_p0_tool_settled_crash"
        const counter = path.join(directory, "counter.txt")
        fs.writeFileSync(counter, "start\n")
        yield* migrateLikeProduction(filename)
        seedSession(filename, sessionID)
        yield* llm.text("first")
        // The side effect applies once. The queued replay never runs because the
        // crash leaves an indeterminate row.
        yield* llm.tool("edit", { filePath: counter, oldString: "start", newString: "start\nx" })
        yield* llm.text("done")

        const fixture = startHub({
          llm,
          filename,
          directory,
          env: { OPENCODE_TEST_FAULTS: "crashWorker@tool:settled" },
        })
        try {
          yield* Effect.promise(() => dispatchPrompt(fixture.hub, sessionID, "first turn"))
          const payload = yield* Effect.promise(() => crashPayload(dispatchPrompt(fixture.hub, sessionID, "side effect")))
          expect(payload?.phase).toBe("tool")

          // Settlement runs inside the result transaction, so the crash rolls
          // both back: the row is `started`, not a finished call with no result.
          const rows = readRows(filename, sessionID)
          expect(rows.tools).toHaveLength(1)
          expect(rows.tools[0]).toMatchObject({ state: "started", outcome: null })
          expect(rows.parts.filter((part) => part.type === "tool" && part.state?.status === "completed")).toEqual([])
          expect(fixture.hub.lifecycle.attempts(sessionID)).toBe(0)
          // A wrongly allowed resume would consume the queued edit; the durable
          // orphan refuses it, so the side effect stays at exactly one.
          expect(effectCount(counter)).toBe(1)
        } finally {
          yield* Effect.promise(() => fixture.dispose())
        }
      }),
    REAL_WORKER_TEST_TIMEOUT_MS,
  )

  it.live(
    "a normal tool commits both the finished row and the result part",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const { filename, directory } = freshWorkspace()
        const sessionID = "ses_p0_tool_result_ok"
        yield* migrateLikeProduction(filename)
        seedSession(filename, sessionID)
        yield* llm.tool("todowrite", { todos: [{ content: "side effect", status: "pending", priority: "high" }] })
        yield* llm.text("done")

        const fixture = startHub({ llm, filename, directory })
        try {
          yield* Effect.promise(() => dispatchPrompt(fixture.hub, sessionID, "write"))

          const rows = readRows(filename, sessionID)
          expect(rows.tools).toHaveLength(1)
          expect(rows.tools[0]).toMatchObject({ state: "finished", outcome: "ok" })
          expect(rows.parts.filter((part) => part.type === "tool" && part.state?.status === "completed")).toHaveLength(1)
          expect(fixture.hub.lifecycle.indeterminate(sessionID)).toEqual([])
        } finally {
          yield* Effect.promise(() => fixture.dispose())
        }
      }),
    REAL_WORKER_TEST_TIMEOUT_MS,
  )
})
