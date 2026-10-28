import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2Bridge } from "@/event-v2-bridge"
import { expect } from "bun:test"
import { Effect, Exit, Fiber, Layer, Stream } from "effect"
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
      options: {
        apiKey: "test-key",
        baseURL: "http://localhost:1/v1",
      },
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

const user = Effect.fn("TestSession.user")(function* (sessionID: SessionID, text: string) {
  const session = yield* Session.Service
  const msg = yield* session.updateMessage({
    id: MessageID.ascending(),
    role: "user",
    sessionID,
    agent: "build",
    model: ref,
    time: { created: Date.now() },
  })
  yield* session.updatePart({
    id: PartID.ascending(),
    messageID: msg.id,
    sessionID,
    type: "text",
    text,
  })
  return msg
})

// Deliberately omits `finish` so a missing terminal `finish` handler is observable.
const assistant = Effect.fn("TestSession.assistant")(function* (
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
    tokens: {
      total: 0,
      input: 0,
      output: 0,
      reasoning: 0,
      cache: { read: 0, write: 0 },
    },
    modelID: ref.modelID,
    providerID: ref.providerID,
    parentID,
    time: { created: Date.now() },
  }
  yield* session.updateMessage(msg)
  return msg
})

// Built directly rather than via `provider.getModel`: the sandbox has no
// `test` provider in the models.dev fixture, so config providers are dropped
// and `getModel` fails with ProviderModelNotFoundError (pre-existing/env).
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

const boot = Effect.fn("test.boot")(function* () {
  const processors = yield* SessionProcessor.Service
  const session = yield* Session.Service
  return { processors, session }
})

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
const replacements = [
  [SessionSummary.node, summary],
  [RuntimeFlags.node, RuntimeFlags.layer({ experimentalEventSystem: true })],
] as const

// A stream whose only terminal event is a top-level `finish` (no `step-finish`).
const finishOnlyLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () => Stream.make(LLMEvent.finish({ reason: "stop" })),
  }),
)
const finishOnlyEnv = LayerNode.compile(root, [...replacements, [LLM.node, finishOnlyLLM]])
const itFinishOnly = testEffect(finishOnlyEnv)

// A stream where `step-finish` sets a reason before a differing top-level `finish`.
const stepThenFinishLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () =>
      Stream.make(LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }), LLMEvent.finish({ reason: "stop" })),
  }),
)
const stepThenFinishEnv = LayerNode.compile(root, [...replacements, [LLM.node, stepThenFinishLLM]])
const itStepThenFinish = testEffect(stepThenFinishEnv)

// Emits a tool call and then hangs without a result, so interrupting the run
// leaves a registered settlement pending when `cleanup` runs.
const interruptToolLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () =>
      Stream.make(LLMEvent.toolCall({ id: "call-interrupt", name: "ledger", input: {} })).pipe(
        Stream.concat(Stream.fromEffect(Effect.never)),
      ),
  }),
)
const interruptToolEnv = LayerNode.compile(root, [...replacements, [LLM.node, interruptToolLLM]])
const itInterruptTool = testEffect(interruptToolEnv)

// Emits a tool call and then hangs; the test commits its result late, after the
// old fixed abort window.
const delayedResultLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () =>
      Stream.make(LLMEvent.toolCall({ id: "call-delayed", name: "ledger", input: {} })).pipe(
        Stream.concat(Stream.fromEffect(Effect.never)),
      ),
  }),
)
const delayedResultEnv = LayerNode.compile(root, [...replacements, [LLM.node, delayedResultLLM]])
const itDelayedResult = testEffect(delayedResultEnv)

const streamInput = (sessionID: SessionID, parent: SessionV1.User, model: Provider.Model): LLM.StreamInput => ({
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
})

itFinishOnly.live("session.processor sets finish from a terminal finish event without step-finish", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const { processors, session } = yield* boot()

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "finish only")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = testModel
        const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model: mdl })

        const value = yield* handle.process(streamInput(chat.id, parent, mdl))
        const stored = yield* MessageV2.get({ sessionID: chat.id, messageID: msg.id })

        expect(value).toBe("continue")
        expect(handle.message.finish).toBe("stop")
        expect(stored.info.role).toBe("assistant")
        if (stored.info.role === "assistant") expect(stored.info.finish).toBe("stop")
      }),
    { config: cfg },
  ),
)

itStepThenFinish.live("session.processor does not overwrite a step-finish reason with a later finish reason", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const { processors, session } = yield* boot()

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "step then finish")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = testModel
        const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model: mdl })

        const value = yield* handle.process(streamInput(chat.id, parent, mdl))
        const stored = yield* MessageV2.get({ sessionID: chat.id, messageID: msg.id })

        expect(value).toBe("continue")
        expect(handle.message.finish).toBe("tool-calls")
        expect(stored.info.role).toBe("assistant")
        if (stored.info.role === "assistant") expect(stored.info.finish).toBe("tool-calls")
      }),
    { config: cfg },
  ),
)

itInterruptTool.live("session.processor cleanup drains a registered settlement interrupted before its result", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const database = yield* Database.Service
        const { processors, session } = yield* boot()

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "interrupt result")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = testModel
        const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model: mdl })

        const outcomes: Array<"ok" | "error"> = []
        handle.registerToolSettlement?.("call-interrupt", (outcome, phase) =>
          Effect.sync(() => {
            // Cleanup owns the abort/result-less path; commit/afterCommit are
            // reserved for a result that actually committed.
            if (phase === "settle") outcomes.push(outcome)
          }),
        )

        const run = yield* handle.process(streamInput(chat.id, parent, mdl)).pipe(Effect.forkChild)
        yield* pollWithTimeout(
          MessageV2.parts(msg.id).pipe(
            Effect.provideService(Database.Service, database),
            Effect.map((parts) => parts.find((part): part is SessionV1.ToolPart => part.type === "tool")),
          ),
          "timed out waiting for the tool part",
        )
        yield* Fiber.interrupt(run)

        const exit = yield* Fiber.await(run)
        const parts = yield* MessageV2.parts(msg.id)
        const call = parts.find((part): part is SessionV1.ToolPart => part.type === "tool")

        expect(Exit.isFailure(exit)).toBe(true)
        expect(outcomes).toEqual(["error"])
        expect(call?.state.status).toBe("error")
        if (call?.state.status === "error") {
          expect(call.state.error).toBe("Tool execution aborted")
          expect(call.state.metadata?.interrupted).toBe(true)
        }
      }),
    { config: cfg },
  ),
)

const delayedCallID = "call-delayed"

function findingToolPart(messageID: MessageID) {
  return MessageV2.parts(messageID).pipe(
    Effect.map((parts) => parts.find((part): part is SessionV1.ToolPart => part.type === "tool")),
  )
}

function runningToolPart(messageID: MessageID) {
  return findingToolPart(messageID).pipe(Effect.map((part) => (part?.state.status === "running" ? part : undefined)))
}

itDelayedResult.live(
  "session.processor cleanup drains a result committed just after the old abort window",
  () =>
    provideTmpdirInstance(
      (dir) =>
        Effect.gen(function* () {
          const { processors, session } = yield* boot()

          const chat = yield* session.create({})
          const parent = yield* user(chat.id, "late result")
          const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
          const mdl = testModel
          const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model: mdl })

          const outcomes: Array<"ok" | "error"> = []
          handle.registerToolSettlement?.(delayedCallID, (outcome, phase) =>
            Effect.sync(() => {
              // `commit` is the durable ledger write; the report/standalone
              // phases are the settlement decision cleanup observes.
              if (phase !== "commit") outcomes.push(outcome)
            }),
          )

          const run = yield* handle.process(streamInput(chat.id, parent, mdl)).pipe(Effect.forkChild)
          yield* pollWithTimeout(runningToolPart(msg.id), "timed out waiting for the running tool part")

          // A legitimately slow abort-path result: it lands after the old
          // fixed 250 ms window but before the settlement drain gives up.
          yield* handle
            .completeToolCall(delayedCallID, { title: "delayed", metadata: {}, output: "done" })
            .pipe(Effect.delay("350 millis"), Effect.forkChild)
          yield* Fiber.interrupt(run)

          const part = yield* findingToolPart(msg.id)
          expect(part?.state.status).toBe("completed")
          expect(outcomes).toEqual(["ok"])
        }),
      { config: cfg },
    ),
)

itDelayedResult.live(
  "session.processor cleanup keeps a result that landed before the abort",
  () =>
    provideTmpdirInstance(
      (dir) =>
        Effect.gen(function* () {
          const { processors, session } = yield* boot()

          const chat = yield* session.create({})
          const parent = yield* user(chat.id, "pre-settled")
          const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
          const mdl = testModel
          const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model: mdl })

          const outcomes: Array<"ok" | "error"> = []
          handle.registerToolSettlement?.(delayedCallID, (outcome, phase) =>
            Effect.sync(() => {
              if (phase !== "commit") outcomes.push(outcome)
            }),
          )

          const run = yield* handle.process(streamInput(chat.id, parent, mdl)).pipe(Effect.forkChild)
          yield* pollWithTimeout(runningToolPart(msg.id), "timed out waiting for the running tool part")

          yield* handle.completeToolCall(delayedCallID, { title: "done", metadata: {}, output: "done" })
          yield* Fiber.interrupt(run)

          const part = yield* findingToolPart(msg.id)
          expect(part?.state.status).toBe("completed")
          expect(outcomes).toEqual(["ok"])
        }),
      { config: cfg },
    ),
)
