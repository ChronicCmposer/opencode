// B1 — DCP regression guard for `experimental.chat.messages.transform` under workers.
//
// The original bug: `@tarquinen/opencode-dcp` prunes old tool results via
// `experimental.chat.messages.transform`. With `OPENCODE_EXPERIMENTAL_SESSION_WORKERS=true`
// the hook is a hub proxy that returns a FRESH `{ messages }`; the call sites used
// to discard that return and rely on in-place mutation, so the pruning silently
// vanished before the provider request.
//
// This test encodes that bug: a stub hub plugin replaces an old tool-result's
// content with the exact DCP sentinel, a real prompt turn runs through the full
// worker graph (dispatch → hub-proxy host → call-site reconciliation → LLM), and
// the sentinel must reach the outbound provider request. The persisted session
// keeps the original output, proving the sentinel came from the transform.

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
import { startMockProvider, type MockProviderHandle } from "../../lib/mock-provider"
import { workerSnapshot } from "../../lib/snapshot"

/** The exact sentinel `@tarquinen/opencode-dcp` writes in place of pruned output. */
const SENTINEL = "[Output removed to save context - information superseded or no longer needed]"

/** The seeded tool output the stub plugin must prune. */
const OLD_OUTPUT = "TOOL_OUTPUT_ORIGINAL"

/** The user text of the turn the loop runs; used to locate the outbound request. */
const TURN_TEXT = "second"

type ToolStateLike = { status?: string; output?: string; error?: string }
type PartLike = { type?: string; state?: ToolStateLike }
type MessageLike = { parts?: PartLike[] }

/** The serializable model bootstrap the worker talks to. */
const modelConfig = (provider: MockProviderHandle): WorkerModelConfig => ({
  providerID: "mock",
  modelID: provider.model,
  apiId: provider.model,
  npm: "@ai-sdk/openai-compatible",
  baseUrl: provider.baseUrl,
  apiKey: provider.apiKey,
  headers: {},
})

/**
 * The DCP-shaped prune: clear completed tool-result content outside the most
 * recent message. Mutates the list in place (the hub handler clones first).
 */
function pruneOldToolResults(messages: MessageLike[]): void {
  const keepFrom = Math.max(0, messages.length - 1)
  for (let index = 0; index < keepFrom; index++) {
    for (const part of messages[index].parts ?? []) {
      if (part.type !== "tool") continue
      if (part.state?.status === "completed") part.state.output = SENTINEL
      else if (part.state?.status === "error") part.state.error = SENTINEL
    }
  }
}

/**
 * The stub DCP plugin, hosted on the hub: it returns a fresh cloned `{ messages }`
 * carrying the pruned content, exactly like `hubProxyMessagesTransform`.
 */
function dcpStubHost(): WorkerPluginHost {
  return createWorkerPluginHost([], {
    messagesTransform: async (_input, output) => {
      const messages = structuredClone(output.messages) as MessageLike[]
      pruneOldToolResults(messages)
      return { messages }
    },
  })
}

/** The full worker graph over the E2E writer, with the stub plugin host. */
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
      broker: { call: async () => null },
      backgroundJob: { wait: async () => null, cancel: async () => {} },
    },
    modelClient: llmModelClientLayer(config).pipe(Layer.provide(reducedLlmLayer(snapshot, config))),
    plugins,
    replacements: workerGraphReplacements(config),
  }).layer
}

/**
 * Seed a session whose history contains one completed tool result followed by a
 * fresh user turn, so the DCP-shaped prune has an "old" result to clear.
 */
function seed(
  layer: Layer.Layer<unknown, unknown>,
  instance: E2EContext["instance"],
  directory: string,
  modelID: string,
) {
  return Effect.gen(function* () {
    const sessions = yield* Session.Service
    const session = yield* sessions.create({
      title: "b1-dcp",
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
      text: "first",
    } as never)
    const assistant = (yield* sessions.updateMessage({
      id: MessageID.ascending(),
      role: "assistant",
      sessionID: session.id,
      parentID: user.id,
      agent: "build",
      mode: "build",
      modelID,
      providerID: "mock",
      path: { cwd: directory, root: directory },
      cost: 0,
      tokens: { output: 0, input: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      time: { created: Date.now() },
      finish: "end_turn",
    } as never)) as { id: MessageID }
    yield* sessions.updatePart({
      id: PartID.ascending(),
      messageID: assistant.id,
      sessionID: session.id,
      type: "tool",
      callID: "call-fixed-1",
      tool: "bash",
      state: {
        status: "completed",
        input: {},
        output: OLD_OUTPUT,
        title: "done",
        metadata: {},
        time: { start: Date.now(), end: Date.now() },
      },
    } as never)
    const turn = (yield* sessions.updateMessage({
      id: MessageID.ascending(),
      role: "user",
      sessionID: session.id,
      agent: "build",
      model: { providerID: "mock", modelID },
      time: { created: Date.now() },
    } as never)) as { id: MessageID }
    yield* sessions.updatePart({
      id: PartID.ascending(),
      messageID: turn.id,
      sessionID: session.id,
      type: "text",
      text: TURN_TEXT,
    } as never)
    return session.id
  }).pipe(Effect.provide(layer), Effect.provideService(InstanceRef, instance))
}

/** The outbound request bodies that belong to the seeded turn. */
function turnBodies(provider: MockProviderHandle): ReadonlyArray<Record<string, unknown>> {
  return provider.requests
    .map((request) => request.body)
    .filter((body) => JSON.stringify(body).includes(TURN_TEXT))
}

/** A temp directory shared by the harness and the seeded session's `path`. */
async function withDirectory(run: (snapshot: WorkerSnapshot) => Promise<void>): Promise<void> {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "oc-b1-dcp-"))
  try {
    await run(workerSnapshot({ directory }))
  } finally {
    fs.rmSync(directory, { recursive: true, force: true })
  }
}

describe("DCP sentinel survives a worker prompt turn", () => {
  test("the pruned tool result reaches the provider request (not the original output)", async () => {
    await withDirectory(async (snapshot) => {
      const provider = startMockProvider({ chunks: ["ok"] })
      const ctx = createE2EContext({ snapshot, modelConfig: modelConfig(provider) })
      try {
        const layer = workerLayer(snapshot, provider, ctx.writer, dcpStubHost())
        const sessionID = await Effect.runPromise(seed(layer, ctx.instance, snapshot.directory, provider.model))
        const persisted = await Effect.runPromise(
          Effect.gen(function* () {
            const prompt = yield* SessionPrompt.Service
            yield* prompt.loop({ sessionID })
            const sessions = yield* Session.Service
            return yield* sessions.messages({ sessionID })
          }).pipe(Effect.provide(layer), Effect.provideService(InstanceRef, ctx.instance)),
        )

        const bodies = turnBodies(provider)
        expect(bodies.length).toBeGreaterThanOrEqual(1)
        const serialized = JSON.stringify(bodies)
        expect(serialized).toContain(SENTINEL)
        expect(serialized).not.toContain(OLD_OUTPUT)
        // The transform only rewrites the outbound request; the durable history
        // keeps the real tool output.
        expect(JSON.stringify(persisted)).toContain(OLD_OUTPUT)
      } finally {
        await ctx.dispose()
        await provider.stop()
      }
    })
  }, 60_000)

  test("the pruned tool result reaches the compaction conversation", async () => {
    await withDirectory(async (snapshot) => {
      const provider = startMockProvider({ chunks: ["summary"] })
      const ctx = createE2EContext({ snapshot, modelConfig: modelConfig(provider) })
      try {
        const layer = workerLayer(snapshot, provider, ctx.writer, dcpStubHost())
        const sessionID = await Effect.runPromise(seed(layer, ctx.instance, snapshot.directory, provider.model))
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

        const bodies = turnBodies(provider)
        expect(bodies.length).toBeGreaterThanOrEqual(1)
        const serialized = JSON.stringify(bodies)
        expect(serialized).toContain(`[Tool result]: ${SENTINEL}`)
        expect(serialized).not.toContain(OLD_OUTPUT)
      } finally {
        await ctx.dispose()
        await provider.stop()
      }
    })
  }, 60_000)
})
