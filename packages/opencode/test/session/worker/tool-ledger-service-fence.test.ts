// F8: a released worker lease is fenced for its old generation, and the
// tool-call ledger refuses a retired generation through the real tool path.
//
// The lease check and the ledger write share one repository transaction. The
// tests drive both the core DurableLog fence and `SessionTools.resolve` so the closed
// loop is exercised where the tool path actually runs.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { randomUUID } from "node:crypto"
import { afterEach, describe, expect, test } from "bun:test"
import { Database as BunDatabase } from "bun:sqlite"
import { Effect, Layer, Schema } from "effect"
import { Event } from "@opencode-ai/schema/event"
import { DurableLog } from "@opencode-ai/core/durable-log"
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
import {
  LEDGER_SQL,
  ToolCallLedger,
  ToolCallStaleGenerationServiceError,
} from "@/session/ledger/tool-call"
import { connectedRepository } from "@/session/worker/repository/write"
import { WorkerDb, type WorkerDatabase } from "@/session/worker/db"

const Note = Event.define({
  type: "test.tool-ledger-fence.note",
  durable: { version: 1, aggregate: "sessionID" },
  schema: { sessionID: Schema.String, text: Schema.String },
})

const LEASE_DDL = `
  CREATE TABLE event_sequence (
    aggregate_id text PRIMARY KEY,
    seq integer NOT NULL,
    owner_id text,
    worker_owner_id text,
    worker_heartbeat_at integer,
    worker_expires_at integer
  );
  CREATE TABLE event (
    id text PRIMARY KEY,
    aggregate_id text NOT NULL,
    seq integer NOT NULL,
    type text NOT NULL,
    data text NOT NULL
  );
  CREATE TABLE event_blob (
    aggregate_id text NOT NULL,
    seq integer NOT NULL,
    type text NOT NULL,
    hash text NOT NULL,
    size integer NOT NULL,
    data text,
    file_path text,
    PRIMARY KEY (aggregate_id, seq)
  );
`

const EVENT_SEQUENCE_DDL = `
  CREATE TABLE event_sequence (
    aggregate_id TEXT PRIMARY KEY,
    seq INTEGER NOT NULL,
    owner_id TEXT,
    worker_owner_id TEXT,
    worker_heartbeat_at INTEGER,
    worker_expires_at INTEGER
  )
`

const directories: string[] = []
const natives: BunDatabase[] = []

afterEach(() => {
  for (const native of natives.splice(0)) native.close()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

function openBusDatabase() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-ledger-fence-"))
  directories.push(directory)
  const native = new BunDatabase(path.join(directory, "fence.db"), { create: true })
  native.exec("PRAGMA journal_mode = WAL")
  native.exec(LEASE_DDL)
  natives.push(native)
  const connection: DurableLog.Connection = {
    all: (sql, params = []) => native.query(sql).all(...(params as never[])) as Array<Record<string, unknown>>,
    run: (sql, params = []) => ({ changes: Number(native.query(sql).run(...(params as never[])).changes) }),
    exec: (sql) => native.exec(sql),
  }
  return { native, connection, directory }
}

function bus(database: ReturnType<typeof openBusDatabase>, ownerID: string) {
  return DurableLog.create({
    connection: database.connection,
    ownerID,
    definitions: [Note],
    leaseTimeoutMs: 30_000,
    blobDir: path.join(database.directory, "event-blobs"),
  })
}

function leaseRow(database: ReturnType<typeof openBusDatabase>, aggregateID: string) {
  return database.native
    .query("SELECT worker_owner_id, worker_expires_at FROM event_sequence WHERE aggregate_id = ?")
    .get(aggregateID) as { worker_owner_id: string | null; worker_expires_at: number } | null
}

/** A live-PID generation id (the liveness parser treats it as alive). */
const owner = (workerID: number) => `worker-${workerID}-${process.pid}-${randomUUID()}`

describe("F8 released worker lease", () => {
  test("releaseLease clears the owner and fences the old generation", () => {
    const database = openBusDatabase()
    const previousOwner = owner(1)
    const previous = bus(database, previousOwner)
    previous.claim("s1")

    previous.release("s1")

    expect(leaseRow(database, "s1")).toEqual({ worker_owner_id: null, worker_expires_at: 0 })
    expect(() =>
      previous.publish([{ definition: Note, data: { sessionID: "s1", text: "late" } }]),
    ).toThrow(DurableLog.StaleOwnerError)
  })

  test("a successor reclaims a cleared lease instead of being blocked by the old live PID", () => {
    const database = openBusDatabase()
    const previousOwner = owner(1)
    const nextOwner = owner(2)
    bus(database, previousOwner).claim("s1")

    bus(database, previousOwner).release("s1")

    const successor = bus(database, nextOwner)
    expect(
      successor.reclaim({
        aggregateID: "s1",
        expectedOwnerID: previousOwner,
        expectedSeq: -1,
        nextOwnerID: nextOwner,
      }),
    ).toMatchObject({ ownerID: nextOwner })
    expect(leaseRow(database, "s1")?.worker_owner_id).toBe(nextOwner)
  })

  test("expires=0 fences a live owner in assertWritableOwner and does not block reclaim", () => {
    const database = openBusDatabase()
    const previousOwner = owner(1)
    const nextOwner = owner(2)
    const previous = bus(database, previousOwner)
    previous.claim("s2")

    // A crash/retirement release zeroes the expiry but keeps the owner. The
    // live PID must not make the row writable, and must not block reclaim.
    database.native.query("UPDATE event_sequence SET worker_expires_at = 0 WHERE aggregate_id = 's2'").run()

    expect(() =>
      previous.publish([{ definition: Note, data: { sessionID: "s2", text: "late" } }]),
    ).toThrow(DurableLog.StaleOwnerError)

    const successor = bus(database, nextOwner)
    expect(
      successor.reclaim({
        aggregateID: "s2",
        expectedOwnerID: previousOwner,
        expectedSeq: -1,
        nextOwnerID: nextOwner,
      }),
    ).toMatchObject({ ownerID: nextOwner })
  })
})

// ---------------------------------------------------------------------------
// The ledger service through the real tool path.
// ---------------------------------------------------------------------------

const sessionID = SessionID.make("ses_ledger_fence")
const messageID = MessageID.ascending()
const callID = "call-fence"

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

function fakes(definition: RegisteredTool, database: WorkerDatabase) {
  return Layer.mergeAll(
    Layer.succeed(Plugin.Service, {
      init: () => Effect.void,
      list: () => Effect.succeed([]),
      trigger: (_name, _input, output) => Effect.succeed(output),
    } satisfies Plugin.Interface),
    Layer.succeed(Permission.Service, {
      ask: () => Effect.void,
      reply: () => Effect.void,
      list: () => Effect.succeed([]),
    } satisfies Permission.Interface),
    Layer.succeed(MCP.Service, {
      tools: () => Effect.succeed({}),
      clients: () => Effect.succeed({}),
    } as Partial<MCP.Interface> as MCP.Interface),
    Layer.succeed(Truncate.Service, {
      cleanup: () => Effect.void,
      write: () => Effect.succeed("output.txt"),
      output: (text: string) => Effect.succeed({ content: text, truncated: false }),
      limits: () => Effect.succeed({ maxLines: 2000, maxBytes: 50 * 1024 }),
    } satisfies Truncate.Interface),
    RuntimeFlags.layer(),
    Layer.succeed(
      ToolRegistry.Service,
      ToolRegistry.Service.of({
        ids: () => Effect.succeed([definition.id]),
        all: () => Effect.succeed([definition]),
        named: () => Effect.die("unused"),
        tools: () => Effect.succeed([definition]),
      }),
    ),
    ToolCallLedger.workerLayer.pipe(Layer.provide(Layer.succeed(WorkerDb, database))),
  )
}

function workerDatabase(dbOwner: string, leaseOwner: string = dbOwner): {
  connection: ReturnType<typeof createBunSqliteConnection>
  database: WorkerDatabase
} {
  const connection = createBunSqliteConnection({ filename: ":memory:" })
  connection.exec(LEDGER_SQL)
  connection.exec(EVENT_SEQUENCE_DDL)
  connection.run("INSERT INTO event_sequence (aggregate_id, seq, worker_owner_id) VALUES (?, -1, ?)", [
    String(sessionID),
    dbOwner,
  ])
  const database = {
    filename: ":memory:",
    repository: connectedRepository(connection),
    eventBinding: { leaseOwner },
  } as unknown as WorkerDatabase
  return { connection, database }
}

async function resolve(definition: RegisteredTool, database: WorkerDatabase) {
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
    }).pipe(Effect.provide(fakes(definition, database))),
  )
  const execute = tools[definition.id]?.execute
  if (!execute) throw new Error("tool is missing execute")
  return execute
}

function run(execute: Awaited<ReturnType<typeof resolve>>) {
  return Promise.resolve(execute({}, { toolCallId: callID, abortSignal: new AbortController().signal, messages: [] }))
}

async function failure(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise
    return undefined
  } catch (error) {
    return error
  }
}

describe("tool-call ledger fence through SessionTools.resolve", () => {
  test("a retired generation cannot start a call", async () => {
    const { connection, database } = workerDatabase("worker-new-generation", "worker-old-generation")
    let ran = false
    const execute = await resolve(
      tool(() => {
        ran = true
        return Effect.succeed({ title: "ledger", metadata: {}, output: "done" })
      }),
      database,
    )

    const error = await failure(run(execute))

    expect(error).toBeInstanceOf(ToolCallStaleGenerationServiceError)
    expect(ran).toBe(false)
    expect(connection.all("SELECT * FROM tool_call")).toEqual([])
  })

  test("a released (NULL owner) lease is fenced for a start", async () => {
    const { connection, database } = workerDatabase("worker-old-generation")
    connection.run("UPDATE event_sequence SET worker_owner_id = NULL, worker_expires_at = 0 WHERE aggregate_id = ?", [
      String(sessionID),
    ])
    const execute = await resolve(
      tool(() => Effect.succeed({ title: "ledger", metadata: {}, output: "done" })),
      database,
    )

    expect(await failure(run(execute))).toBeInstanceOf(ToolCallStaleGenerationServiceError)
    expect(connection.all("SELECT * FROM tool_call")).toEqual([])
  })

  test("expires=0 is fenced for a start", async () => {
    const { connection, database } = workerDatabase("worker-old-generation")
    connection.run("UPDATE event_sequence SET worker_expires_at = 0 WHERE aggregate_id = ?", [String(sessionID)])
    const execute = await resolve(
      tool(() => Effect.succeed({ title: "ledger", metadata: {}, output: "done" })),
      database,
    )

    expect(await failure(run(execute))).toBeInstanceOf(ToolCallStaleGenerationServiceError)
    expect(connection.all("SELECT * FROM tool_call")).toEqual([])
  })

  test("a generation retired between start and finish cannot settle the row", async () => {
    const { connection, database } = workerDatabase("worker-old-generation")
    const execute = await resolve(
      tool(() =>
        Effect.sync(() => {
          // The successor claims the lease while the tool effect is running.
          connection.run("UPDATE event_sequence SET worker_owner_id = ? WHERE aggregate_id = ?", [
            "worker-new-generation",
            String(sessionID),
          ])
          return { title: "ledger", metadata: {}, output: "done" }
        }),
      ),
      database,
    )

    await run(execute)

    // The finish was refused, so the row stays indeterminate instead of being
    // settled by a generation that no longer owns the session.
    expect(connection.all("SELECT state, outcome FROM tool_call")).toEqual([{ state: "started", outcome: null }])
  })
})
