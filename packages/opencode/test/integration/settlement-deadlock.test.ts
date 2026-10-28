// tool settlement must never wait on the hub while holding the SQLite
// writer.
//
// The processor commits a tool result and its ledger finish in one
// `BEGIN IMMEDIATE` transaction. `lifecycle.toolFinished` is a hub-proxy call;
// awaiting it inside that transaction lets the hub's own write wait on the
// writer lock the transaction already holds. These tests drive the real
// processor and a real migrated file database, block the lifecycle report, and
// prove the result transaction commits and the connection is free while the
// report is still blocked.

import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Database } from "@opencode-ai/core/database/database"
import { openConnection } from "@opencode-ai/core/database/connection"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2Bridge } from "@/event-v2-bridge"
import { afterAll, expect } from "bun:test"
import { Deferred, Effect, Exit, Fiber, Layer, Stream } from "effect"
import fs from "node:fs"
import os from "node:os"
import path from "path"
import type { Agent } from "../../src/agent/agent"
import { Provider } from "@/provider/provider"
import { Session } from "@/session/session"
import { LLM } from "../../src/session/llm"
import { MessageV2 } from "../../src/session/message-v2"
import { SessionProcessor } from "../../src/session/processor"
import { MessageID, PartID, SessionID } from "../../src/session/schema"
import { SessionStatus } from "../../src/session/status"
import { SessionSummary } from "../../src/session/summary"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { provideTmpdirInstance } from "../fixture/fixture"
import { pollWithTimeout, testEffect } from "../lib/effect"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { LLMEvent } from "@opencode-ai/llm"

const CALL_ID = "call-settlement-deadlock"

// A real file database: an in-memory database cannot observe another
// connection's write lock, which is exactly what this test must prove.
const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-settlement-deadlock-"))
const filename = path.join(directory, "opencode.db")
afterAll(() => fs.rmSync(directory, { recursive: true, force: true }))

const summary = Layer.succeed(
  SessionSummary.Service,
  SessionSummary.Service.of({
    summarize: () => Effect.void,
    diff: () => Effect.succeed([]),
    computeDiff: () => Effect.succeed([]),
  }),
)

const ref = {
  providerID: ProviderV2.ID.make("test"),
  modelID: ModelV2.ID.make("test-model"),
}

const cfg = {
  provider: {
    test: {
      name: "Test",
      id: "test",
      env: [],
      npm: "@ai-sdk/openai-compatible",
      models: {
        "test-model": {
          id: "test-model",
          name: "Test Model",
          attachment: false,
          reasoning: false,
          temperature: false,
          tool_call: true,
          release_date: "2025-01-01",
          limit: { context: 100000, output: 10000 },
          cost: { input: 0, output: 0 },
          options: {},
        },
      },
      options: { apiKey: "test-key", baseURL: "http://localhost:1/v1" },
    },
  },
}

function agent(): Agent.Info {
  return {
    name: "build",
    mode: "primary",
    options: {},
    permission: [{ permission: "*", pattern: "*", action: "allow" }],
  }
}

const modalities = { text: true, audio: false, image: false, video: false, pdf: false }
const testModel: Provider.Model = {
  id: ModelV2.ID.make("test-model"),
  providerID: ProviderV2.ID.make("test"),
  api: { id: "test-model", npm: "@ai-sdk/openai-compatible", url: "" },
  name: "Test Model",
  capabilities: {
    temperature: false,
    reasoning: false,
    attachment: false,
    toolcall: true,
    input: { ...modalities },
    output: { ...modalities },
    interleaved: false,
  },
  cost: { input: 0, output: 0, cache: { read: 0, write: 0 } },
  limit: { context: 100000, output: 10000 },
  status: "active",
  options: {},
  headers: {},
  release_date: "2025-01-01",
}

const user = Effect.fn("TestSettlement.user")(function* (sessionID: SessionID, text: string) {
  const session = yield* Session.Service
  const msg = yield* session.updateMessage({
    id: MessageID.ascending(),
    role: "user",
    sessionID,
    agent: "build",
    model: ref,
    time: { created: Date.now() },
  })
  yield* session.updatePart({ id: PartID.ascending(), messageID: msg.id, sessionID, type: "text", text })
  return msg
})

const assistant = Effect.fn("TestSettlement.assistant")(function* (
  sessionID: SessionID,
  parentID: MessageID,
  root: string,
) {
  const session = yield* Session.Service
  const msg: SessionV1.Assistant = {
    id: MessageID.ascending(),
    role: "assistant",
    sessionID,
    mode: "build",
    agent: "build",
    path: { cwd: root, root },
    cost: 0,
    tokens: { total: 0, input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    modelID: ref.modelID,
    providerID: ref.providerID,
    parentID,
    time: { created: Date.now() },
  }
  yield* session.updateMessage(msg)
  return msg
})

function streamInput(sessionID: SessionID, parent: SessionV1.User, model: Provider.Model): LLM.StreamInput {
  return {
    user: {
      id: parent.id,
      sessionID,
      role: "user",
      time: parent.time,
      agent: parent.agent,
      model: { providerID: ref.providerID, modelID: ref.modelID },
    } satisfies SessionV1.User,
    sessionID,
    model,
    agent: agent(),
    system: [],
    messages: [{ role: "user", content: "hi" }],
    tools: {},
  }
}

// Emits one running tool call and then never completes: the test drives the
// result commit itself, exactly like an abort-path result.
const hangingToolLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () =>
      Stream.make(LLMEvent.toolCall({ id: CALL_ID, name: "ledger", input: {} })).pipe(
        Stream.concat(Stream.fromEffect(Effect.never)),
      ),
  }),
)

const root = LayerNode.group([
  SessionProcessor.node,
  Session.node,
  SessionProjector.node,
  Provider.node,
  Database.node,
  EventV2Bridge.node,
  SessionStatus.node,
  CrossSpawnSpawner.node,
])
const env = LayerNode.compile(root, [
  [SessionSummary.node, summary],
  [RuntimeFlags.node, RuntimeFlags.layer({ experimentalEventSystem: true })],
  [LLM.node, hangingToolLLM],
  [Database.node, Database.layerFromPath(filename)],
])

const it = testEffect(env)

function findingToolPart(messageID: MessageID) {
  return MessageV2.parts(messageID).pipe(
    Effect.map((parts) => parts.find((part): part is SessionV1.ToolPart => part.type === "tool")),
  )
}

function runningToolPart(messageID: MessageID) {
  return findingToolPart(messageID).pipe(Effect.map((part) => (part?.state.status === "running" ? part : undefined)))
}

it.live(
  "a blocked lifecycle report cannot hold the result transaction open",
  () =>
    provideTmpdirInstance(
      (dir) =>
        Effect.gen(function* () {
          const processors = yield* SessionProcessor.Service
          const session = yield* Session.Service

          const chat = yield* session.create({})
          const parent = yield* user(chat.id, "settlement deadlock")
          const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
          const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model: testModel })

          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const outcomes: Array<"ok" | "error"> = []

          handle.registerToolSettlement?.(CALL_ID, (outcome, phase) => {
            if (phase === "commit") return Effect.void
            return Effect.gen(function* () {
              outcomes.push(outcome)
              yield* Deferred.succeed(entered, undefined)
              yield* Deferred.await(release)
            })
          })

          const process = yield* handle.process(streamInput(chat.id, parent, testModel)).pipe(Effect.forkChild)
          yield* pollWithTimeout(runningToolPart(msg.id), "timed out waiting for the running tool part")

          const settling = yield* handle
            .completeToolCall(CALL_ID, { title: "ledger", metadata: {}, output: "done" })
            .pipe(Effect.forkChild)

          // The report is now blocked. Before the fix it is awaited inside
          // `BEGIN IMMEDIATE`, so the writer lock is still held here.
          yield* Deferred.await(entered).pipe(Effect.timeout("5 seconds"))

          // Another writer must be able to take the write lock, which is only
          // true if the result transaction committed before the report.
          const other = openConnection({ filename, busyTimeoutMs: 100 })
          const competing = yield* Effect.try({
            try: () => {
              other.exec("BEGIN IMMEDIATE")
              other.exec("COMMIT")
              return "committed"
            },
            catch: (error) => error,
          }).pipe(Effect.timeout("2 seconds"), Effect.exit)
          other.close()
          expect(Exit.isSuccess(competing)).toBe(true)

          yield* Deferred.succeed(release, undefined)
          yield* Fiber.join(settling)
          const part = yield* findingToolPart(msg.id)
          expect(part?.state.status).toBe("completed")
          expect(outcomes).toEqual(["ok"])

          yield* Fiber.interrupt(process)
        }),
      { config: cfg },
    ),
)
