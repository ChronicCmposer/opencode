// Production tool path ledger (D10).
//
// These tests drive the real `SessionTools.resolve` tool wrapper (not a mock of
// the wrapper): a real SQLite connection backs the ledger, a real registry tool
// runs, and the assertions read the `tool_call` rows the wrapper wrote.

import { describe, expect, test } from "bun:test"
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
import { LEDGER_SQL, createToolCallLedger } from "@/session/ledger/tool-call"
import { WorkerDb, markSchemaIdentity, openWorkerDatabase, type WorkerDatabase } from "@/session/worker/db"
import { migrationStatements } from "@/session/writer/migration.statements.gen"
import { openConnection } from "@opencode-ai/core/database/connection"
import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { Effect, Layer, Schema, Semaphore } from "effect"
import { testEffect } from "../lib/effect"

const callID = "call-ledger"
const sessionID = SessionID.make("ses_ledger")
const messageID = MessageID.ascending()
// The tool path keys the durable ledger by the owning assistant message.
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

type LedgerTestProcessor = Pick<
  SessionProcessor.Handle,
  "message" | "updateToolCall" | "completeToolCall" | "registerToolSettlement"
>

/**
 * One processor handle per assistant message. Each owns its settlement map, so
 * a provider call id reused across messages is a distinct ledger row while the
 * provider id stays the processor/lifecycle key.
 */
function makeProcessor(id: MessageID = messageID) {
  const local = new Map<string, SessionProcessor.ToolSettlement>()
  const value = {
    message: {
      id,
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
    // Mirrors the processor's durable result commit: the registration is drained
    // only when the result part is committed.
    completeToolCall: (toolCallID: string) =>
      Effect.gen(function* () {
        const settle = local.get(toolCallID)
        if (settle === undefined) return
        yield* settle("ok", "commit")
        local.delete(toolCallID)
        yield* settle("ok", "afterCommit")
      }),
    registerToolSettlement: (toolCallID: string, settle: SessionProcessor.ToolSettlement) => {
      local.set(toolCallID, settle)
    },
  } satisfies LedgerTestProcessor
  return { processor: value, settlements: local }
}

const { processor, settlements } = makeProcessor()

/** Commit the result part, which is what settles a successful call. */
function commitResult(toolCallID = callID) {
  return Effect.runPromise(processor.completeToolCall(toolCallID))
}

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

function ledgerForTest() {
  const connection = createBunSqliteConnection({ filename: ":memory:" })
  connection.exec(LEDGER_SQL)
  return { connection, ledger: createToolCallLedger(connection) }
}

async function resolveTool(
  toolDefinition: RegisteredTool,
  ledger: ReturnType<typeof createToolCallLedger>,
  proc: LedgerTestProcessor = processor,
) {
  const tools = await Effect.runPromise(
    Effect.gen(function* () {
      return yield* SessionTools.resolve({
        agent,
        model,
        session: { id: sessionID, permission: [] } as unknown as Session.Info,
        processor: proc,
        bypassAgentCheck: false,
        messages: [],
        promptOps: {} as never,
        ledger,
      })
    }).pipe(Effect.provide(layerWith(toolDefinition))),
  )
  const execute = tools[toolDefinition.id]?.execute
  if (!execute) throw new Error("tool is missing execute")
  return execute
}

function run(execute: Awaited<ReturnType<typeof resolveTool>>, abortSignal = new AbortController().signal) {
  return Promise.resolve(execute({}, { toolCallId: callID, abortSignal, messages: [] }))
}

describe("tool-call ledger on the production tool path", () => {
  test("derives the ledger from the per-worker WAL connection", async () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-tools-ledger-"))
    const filename = path.join(directory, "opencode.db")
    const seed = openConnection({ filename })
    try {
      for (const statement of migrationStatements) seed.exec(statement)
      markSchemaIdentity(seed)
      seed.run(
        "INSERT INTO project (id, name, worktree, sandboxes, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
        ["project-1", "project", "/tmp/project", "[]", 1, 1],
      )
      seed.run(
        "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [String(sessionID), "project-1", "ledger", "/tmp/project", "ledger", "1", 1, 1],
      )
    } finally {
      seed.close()
    }
    const database = await Effect.runPromise(openWorkerDatabase({ filename }))
    try {
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
          })
        }).pipe(
          Effect.provide(layerWith(tool(() => Effect.succeed({ title: "ledger", metadata: {}, output: "done" })))),
          Effect.provideService(WorkerDb, database),
        ),
      )
      const execute = tools.ledger?.execute
      if (!execute) throw new Error("tool is missing execute")
      await run(execute)

      // The effect returned, but no result part committed yet: the row must
      // still be `started` so a crash here is indeterminate, not replayable.
      expect(
        database.connection.all("SELECT state, outcome FROM tool_call WHERE call_id = ? AND session_id = ?", [
          ledgerKey,
          String(sessionID),
        ]),
      ).toEqual([{ state: "started", outcome: null }])

      await commitResult()

      expect(
        database.connection.all("SELECT state, outcome FROM tool_call WHERE call_id = ? AND session_id = ?", [
          ledgerKey,
          String(sessionID),
        ]),
      ).toEqual([{ state: "finished", outcome: "ok" }])
    } finally {
      await Effect.runPromise(database.close)
      fs.rmSync(directory, { recursive: true, force: true })
    }
  })

  test("records started before the effect and finishes only with the result commit", async () => {
    const { ledger } = ledgerForTest()
    let observed: string | undefined
    const execute = await resolveTool(
      tool(() =>
        Effect.sync(() => {
          observed = ledger.list(sessionID)[0]?.state
          return { title: "ledger", metadata: {}, output: "done" }
        }),
      ),
      ledger,
    )

    await run(execute)

    expect(observed).toBe("started")
    // The side effect happened but its result has not committed: a crash here
    // must leave the row `started` so recovery refuses to replay the effect.
    expect(ledger.list(sessionID)).toEqual([
      expect.objectContaining({ callID: ledgerKey, sessionID, tool: "ledger", state: "started", outcome: null }),
    ])
    expect(ledger.indeterminate(sessionID)).toEqual([
      expect.objectContaining({ callID: ledgerKey, state: "indeterminate", sideEffecting: true }),
    ])

    await commitResult()

    expect(ledger.list(sessionID)).toEqual([
      expect.objectContaining({ callID: ledgerKey, sessionID, tool: "ledger", state: "finished", outcome: "ok" }),
    ])
    expect(ledger.indeterminate(sessionID)).toEqual([])
  })

  test("a hard crash between start and finish leaves an indeterminate row", () => {
    const { ledger } = ledgerForTest()
    // A process that dies between `start` and the exit handler writes nothing;
    // the durable row is the only record, and recovery reads it as indeterminate.
    ledger.start({ callID, sessionID: String(sessionID), tool: "ledger", sideEffecting: true })

    expect(ledger.list(sessionID)).toEqual([expect.objectContaining({ callID, state: "started", outcome: null })])
    expect(ledger.indeterminate(sessionID)).toEqual([
      expect.objectContaining({ callID, state: "indeterminate", sideEffecting: true }),
    ])
  })

  test("a defect finishes the row as error instead of blocking the run forever", async () => {
    const { ledger } = ledgerForTest()
    const execute = await resolveTool(
      tool(() => Effect.die(new Error("simulated tool defect"))),
      ledger,
    )

    await expect(run(execute)).rejects.toThrow()
    expect(ledger.list(sessionID)).toEqual([
      expect.objectContaining({ callID: ledgerKey, state: "finished", outcome: "error" }),
    ])
    expect(ledger.indeterminate(sessionID)).toEqual([])
  })

  test("a pre-aborted signal drains the settlement inside the tool effect", async () => {
    const { ledger } = ledgerForTest()
    settlements.delete(callID)
    const execute = await resolveTool(
      tool(() => Effect.succeed({ title: "ledger", metadata: {}, output: "done" })),
      ledger,
    )

    const controller = new AbortController()
    controller.abort()
    await run(execute, controller.signal)

    // The registration exists before the effect runs, so the abort-path result
    // commit inside the effect drains it exactly once.
    expect(settlements.has(callID)).toBe(false)
    expect(ledger.list(sessionID)).toEqual([
      expect.objectContaining({ callID: ledgerKey, state: "finished", outcome: "ok" }),
    ])

    // A later AI SDK result event settles nothing a second time.
    await commitResult()
    expect(ledger.list(sessionID)).toEqual([
      expect.objectContaining({ callID: ledgerKey, state: "finished", outcome: "ok" }),
    ])
  })

  test("an abort settlement wins over a late success", async () => {
    const { ledger } = ledgerForTest()
    settlements.delete(callID)
    const execute = await resolveTool(
      tool(() => Effect.succeed({ title: "ledger", metadata: {}, output: "done" })),
      ledger,
    )

    // The effect returned, so the registration waits for a result commit.
    await run(execute)
    const settle = settlements.get(callID)
    if (settle === undefined) throw new Error("settlement was not registered")

    // Abort cleanup settles error and removes the registration. A later result
    // event must not overwrite the durable row or settle the hub twice.
    await Effect.runPromise(settle("error", "settle"))
    settlements.delete(callID)
    await commitResult()

    expect(ledger.list(sessionID)).toEqual([
      expect.objectContaining({ callID: ledgerKey, state: "finished", outcome: "error" }),
    ])
    expect(ledger.indeterminate(sessionID)).toEqual([])
  })

  test("a provider call id reused across messages gets a fresh ledger row", async () => {
    const { ledger } = ledgerForTest()
    const first = makeProcessor(MessageID.ascending())
    const second = makeProcessor(MessageID.ascending())
    const firstKey = `${first.processor.message.id}:${callID}`
    const secondKey = `${second.processor.message.id}:${callID}`

    const executeA = await resolveTool(
      tool(() => Effect.succeed({ title: "first", metadata: {}, output: "first" })),
      ledger,
      first.processor,
    )
    const executeB = await resolveTool(
      tool(() => Effect.succeed({ title: "second", metadata: {}, output: "second" })),
      ledger,
      second.processor,
    )

    // The provider reuses the id; the second call must not throw
    // ToolCallAlreadyStartedError nor touch the first call's row.
    await run(executeA)
    await run(executeB)

    expect(ledger.list(sessionID).map((row) => row.callID)).toEqual([firstKey, secondKey])
    expect(ledger.list(sessionID).every((row) => row.state === "started")).toBe(true)

    await Effect.runPromise(first.processor.completeToolCall(callID))
    await Effect.runPromise(second.processor.completeToolCall(callID))

    expect(ledger.list(sessionID)).toEqual([
      expect.objectContaining({ callID: firstKey, state: "finished", outcome: "ok" }),
      expect.objectContaining({ callID: secondKey, state: "finished", outcome: "ok" }),
    ])
    expect(ledger.indeterminate(sessionID)).toEqual([])
  })

  test("a parallel start survives another tool's rollback", async () => {
    const { connection } = ledgerForTest()
    const semaphore = Effect.runSync(Semaphore.make(1))
    const exclusive = <A>(operation: () => A) => semaphore.withPermits(1)(Effect.sync(operation))
    const ledger = createToolCallLedger(connection, { exclusive })
    settlements.delete(callID)
    const execute = await resolveTool(
      tool(() => Effect.succeed({ title: "ledger", metadata: {}, output: "done" })),
      ledger,
    )

    // Another tool owns the connection and its result transaction. The start of
    // this tool must wait for that owner instead of joining its transaction.
    Effect.runSync(semaphore.take(1))
    connection.exec("BEGIN IMMEDIATE")
    const running = run(execute)
    await new Promise((resolve) => setTimeout(resolve, 0))

    // The other tool rolls back. A joined start would be erased here.
    connection.exec("ROLLBACK")
    Effect.runSync(semaphore.release(1))
    await running

    expect(ledger.list(sessionID)).toEqual([
      expect.objectContaining({ callID: ledgerKey, sessionID, tool: "ledger", state: "started", outcome: null }),
    ])
    expect(ledger.indeterminate(sessionID)).toHaveLength(1)
  })

  test("finish joins an open result transaction instead of nesting one", async () => {
    const { connection, ledger } = ledgerForTest()
    connection.exec("CREATE TABLE note (id TEXT NOT NULL)")
    const execute = await resolveTool(
      tool(() =>
        Effect.sync(() => {
          // The tool owns the result transaction; the result-commit finish must
          // run inside it (a single UPDATE), never issue a nested BEGIN.
          connection.exec("BEGIN IMMEDIATE")
          connection.run("INSERT INTO note (id) VALUES (?)", ["result"])
          return { title: "ledger", metadata: {}, output: "done" }
        }),
      ),
      ledger,
    )

    await run(execute)
    expect(ledger.list(sessionID)[0]?.state).toBe("started")

    await commitResult()

    // The finish is visible on the same open transaction and commits with it.
    expect(ledger.list(sessionID)[0]?.state).toBe("finished")
    expect(connection.all("SELECT id FROM note")).toEqual([{ id: "result" }])
    connection.exec("COMMIT")
    expect(ledger.list(sessionID)[0]?.state).toBe("finished")
  })
})
