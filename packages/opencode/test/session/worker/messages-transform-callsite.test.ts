// The two `experimental.chat.messages.transform` call sites must CONSUME the
// trigger's returned `{ messages }`, not rely on in-place mutation.
//
// The hook contract is "mutate `output.messages` in place". In-process the
// trigger mutates and returns the SAME array, so discarding the return is
// harmless. Under session workers the rewrite crosses the hub control port and
// comes back as a FRESH `{ messages }` — a discarded return silently drops the
// context pruning before the LLM request. The existing
// `experimental.chat.system.transform` site already reconciles this
// (`src/session/llm/request.ts`); these tests pin the same reconciliation at:
//   * `src/session/prompt.ts` (the main loop), and
//   * `src/session/compaction.ts` (the summarization prompt).
//
// A REAL path test: the full worker graph (`buildSessionWorkerLayer`) runs over
// the `createE2EContext` writer with a `WorkerPluginHost` whose `messagesTransform`
// returns a freshly built `{ messages }` (structuredClone + a marker) — exactly
// what a hub-proxied worker trigger returns. The outbound mock-provider request
// must reflect the mutation.

import { describe, expect, test } from "bun:test"
import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { Effect, Layer } from "effect"
import { MessageID, PartID, type SessionID } from "@/session/schema"
import { Session } from "@/session/session"
import { SessionPrompt } from "@/session/prompt"
import { SessionCompaction } from "@/session/compaction"
import { InstanceRef } from "@/effect/instance-ref"
import { createWorkerPluginHost, type WorkerPluginHost } from "@/session/worker/plugin-host"
import type { WriterClientHandle } from "@/session/worker/layer"
import {
  buildSessionWorkerLayer,
  llmModelClientLayer,
  reducedLlmLayer,
  type WorkerModelConfig,
  type WorkerSnapshot,
} from "@/session/worker/layer"
import { workerGraphReplacements } from "@/session/worker/session-worker"
import { createE2EContext, type E2EContext } from "../../lib/parity-harness"
import { pluginPassThroughReplacement } from "../../lib/worker-plugin-routes"
import { startMockProvider, type MockProviderHandle } from "../../lib/mock-provider"
import { workerSnapshot } from "../../lib/snapshot"

/** A sentinel the transform appends, proving the transformed array reached the model. */
const MARKER = "PLUGIN_MESSAGES_TRANSFORM_MARKER"

/** One `{ messages }` payload element this test stamps/mutates. */
type MessageLike = { parts?: Array<{ type?: string; text?: string }> }

/** The serializable model bootstrap the worker talks to (mirrors layer-session.test.ts). */
const modelConfig = (provider: MockProviderHandle): WorkerModelConfig => ({
  providerID: "mock",
  modelID: provider.model,
  apiId: provider.model,
  npm: "@ai-sdk/openai-compatible",
  baseUrl: provider.baseUrl,
  apiKey: provider.apiKey,
  headers: {},
})

/** Append {@link MARKER} to every text part of every message (mutates in place). */
function stampMarker(messages: MessageLike[]): void {
  for (const message of messages) {
    for (const part of message.parts ?? []) {
      if (part.type === "text" && typeof part.text === "string") part.text = `${part.text} ${MARKER}`
    }
  }
}

/**
 * A `WorkerPluginHost` whose `messagesTransform` simulates the hub-proxy outcome:
 * it cannot rewrite the caller's array across the control port, so it returns a
 * FRESH `{ messages }` (a structuredClone with the marker stamped in).
 *
 * `onMessages` receives the exact `msgs` array the call site handed in, letting a
 * test prove the call site reconciled the returned array back into it.
 */
function freshArrayHostCapturing(onMessages: (messages: MessageLike[]) => void = () => {}): WorkerPluginHost {
  return createWorkerPluginHost([], {
    messagesTransform: async (_input, output) => {
      onMessages(output.messages as MessageLike[])
      const cloned = structuredClone(output.messages) as MessageLike[]
      stampMarker(cloned)
      return { messages: cloned }
    },
  })
}

/** {@link freshArrayHostCapturing} without the capture, for the real-loop tests. */
function freshArrayHost(): WorkerPluginHost {
  return freshArrayHostCapturing()
}

/** The full worker graph over the E2E writer, with a supplied plugin host. */
function workerLayer(
  snapshot: WorkerSnapshot,
  provider: MockProviderHandle,
  writer: WriterClientHandle,
  plugins: WorkerPluginHost,
): Layer.Layer<unknown, unknown> {
  const config = modelConfig(provider)
  return buildSessionWorkerLayer({
    snapshot,
    env: { values: {} },
    clients: {
      writer,
      egress: { emit: () => {}, emitDurable: () => {} },
      backgroundJob: { wait: async () => null, cancel: async () => {} },
    },
    modelClient: llmModelClientLayer(config).pipe(Layer.provide(reducedLlmLayer(snapshot, config))),
    plugins,
    replacements: [...workerGraphReplacements(config), pluginPassThroughReplacement(plugins)],
  }).layer
}

/** Create a session with one user text message and return its id. */
function seed(
  layer: Layer.Layer<unknown, unknown>,
  instance: E2EContext["instance"],
  modelID: string,
  text: string,
) {
  return Effect.gen(function* () {
    const sessions = yield* Session.Service
    const session = yield* sessions.create({
      title: "a5",
      agent: "build",
      permission: [{ permission: "*", pattern: "*", action: "allow" }],
    })
    const user = (yield* sessions.updateMessage({
      id: MessageID.ascending(),
      role: "user",
      sessionID: session.id,
      agent: "build",
      model: { providerID: "mock", modelID },
      time: { created: Date.now() },
    } as never)) as { id: MessageID }
    yield* sessions.updatePart({
      id: PartID.ascending(),
      messageID: user.id,
      sessionID: session.id,
      type: "text",
      text,
    } as never)
    return session.id
  }).pipe(Effect.provide(layer), Effect.provideService(InstanceRef, instance))
}

/** Run the prompt loop once; resolves after the assistant message is produced. */
function runLoop(layer: Layer.Layer<unknown, unknown>, instance: E2EContext["instance"], sessionID: SessionID) {
  return Effect.gen(function* () {
    const prompt = yield* SessionPrompt.Service
    return yield* prompt.loop({ sessionID })
  }).pipe(Effect.provide(layer), Effect.provideService(InstanceRef, instance))
}

/** A temp directory + E2E writer + mock provider shared by one test. */
async function harness() {
  const provider = startMockProvider({ chunks: ["ok"] })
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "oc-a5-"))
  const snapshot = workerSnapshot({ directory })
  const ctx = createE2EContext({ snapshot, modelConfig: modelConfig(provider) })
  const dispose = async () => {
    await ctx.dispose()
    await provider.stop()
    fs.rmSync(directory, { recursive: true, force: true })
  }
  return { provider, snapshot, ctx, dispose }
}

describe("experimental.chat.messages.transform call sites consume the returned array", () => {
  test("prompt.ts applies a hub-proxied (fresh-array) messages transform to the LLM request", async () => {
    const { provider, snapshot, ctx, dispose } = await harness()
    try {
      const layer = workerLayer(snapshot, provider, ctx.writer, freshArrayHost())
      const sessionID = await Effect.runPromise(seed(layer, ctx.instance, provider.model, "hello-original"))
      await Effect.runPromise(runLoop(layer, ctx.instance, sessionID))

      expect(provider.requests.length).toBeGreaterThanOrEqual(1)
      expect(JSON.stringify(provider.requests.map((request) => request.body))).toContain(MARKER)
    } finally {
      await dispose()
    }
  }, 60_000)

  test("compaction.ts applies a hub-proxied (fresh-array) messages transform to the summary prompt", async () => {
    const { provider, snapshot, ctx, dispose } = await harness()
    try {
      const layer = workerLayer(snapshot, provider, ctx.writer, freshArrayHost())
      const sessionID = await Effect.runPromise(seed(layer, ctx.instance, provider.model, "hello-original"))
      await Effect.runPromise(
        Effect.gen(function* () {
          const sessions = yield* Session.Service
          const compaction = yield* SessionCompaction.Service
          const messages = yield* sessions.messages({ sessionID })
          const parent = messages.findLast((message) => message.info.role === "user")
          if (parent === undefined) throw new Error("seed produced no user message")
          yield* compaction.process({ parentID: parent.info.id, messages, sessionID, auto: false })
        }).pipe(Effect.provide(layer), Effect.provideService(InstanceRef, ctx.instance)),
      )

      expect(JSON.stringify(provider.requests.map((request) => request.body))).toContain(MARKER)
    } finally {
      await dispose()
    }
  }, 60_000)

  test("the fresh-array (hub-proxied) path is the one reconciled into `msgs`", async () => {
    const { provider, snapshot, ctx, dispose } = await harness()
    try {
      // Capture the exact `msgs` array the call site hands to the trigger. A
      // hub-proxied trigger returns a FRESH array across the port, so the ONLY
      // way `msgs` can carry the marker is if the call site spliced the returned
      // array back into it. Remove that reconciliation and this test goes red.
      let msgsSeenByTrigger: MessageLike[] | undefined
      const host = freshArrayHostCapturing((messages) => {
        msgsSeenByTrigger = messages
      })
      const layer = workerLayer(snapshot, provider, ctx.writer, host)
      const sessionID = await Effect.runPromise(seed(layer, ctx.instance, provider.model, "hello-original"))
      await Effect.runPromise(runLoop(layer, ctx.instance, sessionID))

      expect(msgsSeenByTrigger).toBeDefined()
      expect(JSON.stringify(msgsSeenByTrigger)).toContain(MARKER)
      expect(JSON.stringify(provider.requests.map((request) => request.body))).toContain(MARKER)
    } finally {
      await dispose()
    }
  }, 60_000)
})
