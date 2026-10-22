import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2Bridge } from "@/event-v2-bridge"
import { expect } from "bun:test"
import { Effect, Layer, Stream } from "effect"
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
import { testEffect } from "../lib/effect"
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
