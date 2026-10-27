// Session-hub integration fixture: a REAL bound worker isolate.
//
// The worker opens the production worker database bound to one session, builds
// the production reduced session graph over it, and reaches the hub through a
// real `MessageChannel` control port. Every successful worker SQL statement is
// recorded so the parent test can prove a child row is only ever reached through
// the hub, never through the worker's own connection.
//
// Protocol (default worker channel):
//   worker -> hub : { type: "ready" }
//   hub -> worker : { type: "hub-port", port }            (transfer)
//   worker -> hub : { type: "initialized" }
//   hub -> worker : { type: "call", id, op, arg? }
//   worker -> hub : { type: "result", id, ok, value?, error?, tag?, sql }
//   hub -> worker : { type: "close" }

import { dirname } from "node:path"
import { workerData } from "node:worker_threads"
import { Effect, Exit, Layer, Scope } from "effect"
import { InstanceRef } from "@/effect/instance-ref"
import { Session } from "@/session/session"
import { SessionPrompt } from "@/session/prompt"
import { MessageID, SessionID } from "@/session/schema"
import {
  openWorkerDatabase,
  type WorkerConnection,
  type WorkerDatabase,
  type WorkerStatement,
} from "@/session/worker/db"
import { ModelClient, buildSessionWorkerLayer, createHubProxyClient } from "@/session/worker/layer"
import { workerSnapshot } from "../lib/snapshot"
import { onMessage, postToParent } from "./worker-scope"

interface FixtureData {
  readonly filename: string
  readonly sessionID: string
}

type Inbound =
  | { readonly type: "hub-port"; readonly port: MessagePort }
  | { readonly type: "call"; readonly id: number; readonly op: string; readonly arg?: unknown }
  | { readonly type: "close" }

const data = workerData as FixtureData
const bound = SessionID.make(data.sessionID)
const sqlLog: string[] = []
const databases: WorkerDatabase[] = []

function note(sql: string): void {
  sqlLog.push(sql)
}

/** Record the statement only AFTER it executed: a guard-rejected read never lands. */
function recordConnection(inner: WorkerConnection): WorkerConnection {
  const recordStatement = <Row>(sql: string, statement: WorkerStatement<Row>) => ({
    all: (...params: Parameters<WorkerStatement<Row>["all"]>) => {
      const rows = statement.all(...params)
      note(sql)
      return rows
    },
    values: (...params: Parameters<WorkerStatement<Row>["values"]>) => {
      const rows = statement.values(...params)
      note(sql)
      return rows
    },
    get: (...params: Parameters<WorkerStatement<Row>["get"]>) => {
      const row = statement.get(...params)
      note(sql)
      return row
    },
    run: (...params: Parameters<WorkerStatement<Row>["run"]>) => {
      const outcome = statement.run(...params)
      note(sql)
      return outcome
    },
  })
  return {
    owner: inner.owner,
    filename: inner.filename,
    get closed() {
      return inner.closed
    },
    query: <Row = Record<string, unknown>>(sql: string) => recordStatement<Row>(sql, inner.query<Row>(sql)),
    all: <Row = Record<string, unknown>>(sql: string, params?: Parameters<WorkerConnection["all"]>[1]) => {
      const rows = inner.all<Row>(sql, params)
      note(sql)
      return rows
    },
    values: <Row = ReadonlyArray<unknown>>(sql: string, params?: Parameters<WorkerConnection["values"]>[1]) => {
      const rows = inner.values<Row>(sql, params)
      note(sql)
      return rows
    },
    run: (sql: string, params?: Parameters<WorkerConnection["run"]>[1]) => {
      const outcome = inner.run(sql, params)
      note(sql)
      return outcome
    },
    exec: (sql: string) => {
      inner.exec(sql)
      note(sql)
    },
    pragma: (name: string) => inner.pragma(name),
    journalMode: () => inner.journalMode(),
    close: () => inner.close(),
  }
}

/**
 * Wrap every worker connection; the Drizzle graph (session reads) included. A
 * bound worker has no raw `run` (the property is absent at runtime), so the
 * recorded surface must not synthesize one.
 */
function recordDatabase(inner: WorkerDatabase): WorkerDatabase {
  const { run: _rawRun, ...surface } = inner
  const recorded: Omit<WorkerDatabase, "run"> = {
    ...surface,
    connection: recordConnection(inner.connection),
    strictConnection: recordConnection(inner.strictConnection),
    drizzleConnection: recordConnection(inner.drizzleConnection),
    all: <Row = Record<string, unknown>>(sql: string, params?: Parameters<WorkerDatabase["all"]>[1]) =>
      inner.all<Row>(sql, params).pipe(Effect.tap(() => Effect.sync(() => note(sql)))),
  }
  // The legacy type still lists `run`; a bound worker is intentionally without it.
  return recorded as WorkerDatabase
}

let run: (<A, E>(effect: Effect.Effect<A, E, any>) => Promise<A>) | undefined
let direct: WorkerDatabase | undefined
let scope: Scope.Scope | undefined

function tagOf(error: unknown): string {
  if (error !== null && typeof error === "object" && "_tag" in error) return String((error as { _tag: unknown })._tag)
  return error instanceof Error ? error.name : String(error)
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

function childReply(input: { readonly sessionID: SessionID; readonly messageID?: unknown }) {
  const id = MessageID.ascending()
  return {
    info: {
      id,
      role: "assistant" as const,
      parentID: (input.messageID ?? MessageID.ascending()) as never,
      sessionID: input.sessionID,
      mode: "general",
      agent: "general",
      cost: 0,
      path: { cwd: "/tmp", root: "/tmp" },
      tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      modelID: "mock-model" as never,
      providerID: "mock" as never,
      time: { created: Date.now() },
      finish: "stop" as const,
    },
    parts: [
      {
        id: "part-session-hub" as never,
        messageID: id,
        sessionID: input.sessionID,
        type: "text" as const,
        text: "child done",
      },
    ],
  }
}

const operations: Record<string, (arg: any) => Effect.Effect<unknown, unknown, any>> = {
  create: (arg: { title?: string }) =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      return yield* sessions.create({
        parentID: bound,
        title: arg?.title ?? "hub child",
        agent: "general",
        permission: [],
      })
    }),
  get: (arg: { id: string }) =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      return yield* sessions.get(SessionID.make(arg.id))
    }),
  children: () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      return yield* sessions.children(bound)
    }),
  spawn: (arg: { childID: string; text: string }) =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      return yield* sessions.spawn({
        parentID: bound,
        childID: SessionID.make(arg.childID),
        type: "task",
        title: "hub child",
        depthLimit: 10,
        prompt: {
          messageID: MessageID.ascending(),
          sessionID: SessionID.make(arg.childID),
          agent: "general",
          parts: [{ type: "text", text: arg.text }],
        },
      })
    }),
  cancel: (arg: { id: string }) =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      return yield* prompt.cancel(SessionID.make(arg.id))
    }),
  directRead: (arg: { id: string }) =>
    direct!.all("SELECT id FROM session WHERE id = ?", [arg.id]).pipe(
      Effect.match({
        onFailure: (error) => ({ kind: "rejected" as const, tag: tagOf(error), message: messageOf(error) }),
        onSuccess: (rows) => ({ kind: "read" as const, rows }),
      }),
    ),
  directWrite: (arg: { id: string }) =>
    Effect.try({
      try: () => direct!.repository.update("session", { title: "hacked" }, { id: arg.id }),
      catch: (error) => error,
    }).pipe(
      Effect.match({
        onFailure: (error) => ({ kind: "rejected" as const, tag: tagOf(error), message: messageOf(error) }),
        onSuccess: (outcome) => ({ kind: "wrote" as const, outcome }),
      }),
    ),
}

async function initialize(port: MessagePort): Promise<void> {
  const client = createHubProxyClient(port)
  const snapshot = workerSnapshot({ directory: dirname(data.filename) })
  const database = await Effect.runPromise(openWorkerDatabase({ filename: data.filename, sessionID: data.sessionID }))
  databases.push(database)
  direct = database
  const layer = buildSessionWorkerLayer({
    snapshot,
    env: { values: {} },
    clients: {
      writer: { query: async () => [], publish: async () => ({ seq: null, committed: false }) },
      egress: { emit: () => {} },
      backgroundJob: { wait: async () => null, cancel: async () => {} },
      database: recordDatabase(database),
    },
    modelClient: Layer.succeed(ModelClient, { complete: () => Effect.succeed({ text: "unused" }) }),
    hubProxies: { sessionStatus: client, permission: client, question: client },
    boundSessionID: bound,
  }).layer
  scope = Effect.runSync(Scope.make())
  const context = await Effect.runPromise(Layer.buildWithScope(layer, scope))
  const instance = {
    directory: snapshot.directory,
    worktree: snapshot.worktree ?? snapshot.directory,
    project: { id: snapshot.projectId ?? "prj_session_hub", worktree: snapshot.worktree ?? snapshot.directory },
  } as never
  run = (effect) =>
    Effect.runPromise(effect.pipe(Effect.provide(context), Effect.provideService(InstanceRef, instance)))
}

async function handleCall(message: Extract<Inbound, { type: "call" }>): Promise<void> {
  const operation = operations[message.op]
  if (operation === undefined || run === undefined) {
    postToParent({
      type: "result",
      id: message.id,
      ok: false,
      error: `unknown session-hub operation "${message.op}"`,
      sql: [...sqlLog],
    })
    return
  }
  try {
    const value = await run(operation(message.arg))
    postToParent({ type: "result", id: message.id, ok: true, value, sql: [...sqlLog] })
  } catch (error) {
    postToParent({
      type: "result",
      id: message.id,
      ok: false,
      error: messageOf(error),
      tag: tagOf(error),
      sql: [...sqlLog],
    })
  }
}

async function close(): Promise<void> {
  if (scope !== undefined) await Effect.runPromise(Scope.close(scope, Exit.void))
  for (const database of databases.splice(0)) {
    try {
      await Effect.runPromise(database.close)
    } catch {
      // Already closed by the graph finalizer.
    }
  }
  postToParent({ type: "closed" })
}

onMessage<Inbound>((message) => {
  if (message.type === "hub-port") {
    void initialize(message.port).then(
      () => postToParent({ type: "initialized" }),
      (error: unknown) => postToParent({ type: "error", error: messageOf(error) }),
    )
    return
  }
  if (message.type === "call") {
    void handleCall(message)
    return
  }
  if (message.type === "close") void close()
})

postToParent({ type: "ready" })
