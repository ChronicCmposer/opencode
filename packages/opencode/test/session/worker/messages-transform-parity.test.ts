// B1/C1 — faithfulness contract for `experimental.chat.messages.transform`.
//
// The SAME DCP-shaped transform must produce the SAME outbound LLM request (and
// the same compaction conversation) whether session workers are ON or OFF:
//
//   * ON  (hub-proxy, the REAL wire): the transform is a hub authority. The
//     worker forwards `{ messages }` over a `MessageChannel` to
//     `serveHubProxy(buildHubProxyHandlers(…))`, where the hub stub plugin prunes
//     and the reply crosses back as a FRESH structuredClone'd `{ messages }` —
//     exactly what `hubProxyMessagesTransform` + `createHubProxyClient` yield.
//   * OFF (in-process semantics): true flag-off has no worker at all — the
//     plugin hook mutates `output.messages` in place and `Plugin.trigger`
//     returns the SAME object. The worker's function-hook path reproduces those
//     semantics here so the two arms are comparable.
//
// The call sites reconcile both shapes (`prompt.ts`, `compaction.ts`), so the
// observable request must be identical. This file pins that equivalence: the
// real hub-proxy round trip is a faithful stand-in for the in-process mutation.

import { describe, expect, test } from "bun:test"
import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { Effect, Layer, Option } from "effect"
import { MessageID, PartID } from "@/session/schema"
import { Session } from "@/session/session"
import { SessionPrompt } from "@/session/prompt"
import { SessionCompaction } from "@/session/compaction"
import { InstanceRef } from "@/effect/instance-ref"
import type { Plugin } from "@/plugin"
import { buildHubProxyHandlers, messagesTransformAuthority, type HubProxyAuthorities } from "@/session/worker/hub"
import {
  createWorkerPluginHost,
  type MessagesTransformInput,
  type MessagesTransformOutput,
  type WorkerPluginHost,
} from "@/session/worker/plugin-host"
import { hubProxyMessagesTransform } from "@/session/worker/session-worker"
import type { WriterClientHandle } from "@/session/worker/layer"
import {
  buildSessionWorkerLayer,
  createHubProxyClient,
  llmModelClientLayer,
  reducedLlmLayer,
  serveHubProxy,
  type WorkerModelConfig,
  type WorkerSnapshot,
} from "@/session/worker/layer"
import { workerGraphReplacements } from "@/session/worker/session-worker"
import { instantRunner, recordingHubAuthorities } from "../../lib/hub-proxy"
import { createE2EContext, type E2EContext } from "../../lib/parity-harness"
import { startMockProvider, type MockProviderHandle } from "../../lib/mock-provider"
import { workerSnapshot } from "../../lib/snapshot"

/** The exact DCP sentinel the real plugin writes in place of pruned output. */
const SENTINEL = "[Output removed to save context - information superseded or no longer needed]"

/** The seeded tool output the transform must replace. */
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
 * DCP-shaped pruning: replace completed tool-result content outside the most
 * recent message with {@link SENTINEL}. Mutates the given message list in place
 * so the ON path's hub hook and the OFF path's worker hook prune identically.
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
 * The hub-resident stub DCP plugin. Its hook prunes `output.messages` in place
 * and `trigger` resolves the mutated `{ messages }`; the hub-proxy wire
 * structured-clones it on the way back, so the worker sees a FRESH array.
 */
function dcpStubPlugin(): Plugin.Interface {
  return {
    trigger: (_name: string, _input: MessagesTransformInput, output: MessagesTransformOutput) => {
      pruneOldToolResults(output.messages as MessageLike[])
      return Effect.succeed(output)
    },
    list: () => Effect.succeed([]),
    init: () => Effect.void,
  } as unknown as Plugin.Interface
}

/** The production hub authority: resolve the stub plugin, then trigger its hook. */
function dcpMessagesAuthority(): NonNullable<HubProxyAuthorities["messagesTransform"]> {
  return messagesTransformAuthority({ plugin: () => Option.some(dcpStubPlugin()), run: instantRunner })
}

/**
 * ON: the REAL hub-proxy shape. A real `MessageChannel` serves
 * `buildHubProxyHandlers(authorities)` on port2; the worker-side plugin host
 * carries `hubProxyMessagesTransform(createHubProxyClient(port1))`, so the prune
 * genuinely crosses the isolate boundary. The returned disposer closes both ends.
 */
function hubProxyHost(): { host: WorkerPluginHost; dispose: () => void } {
  const { authorities } = recordingHubAuthorities({ messagesTransform: dcpMessagesAuthority() })
  const channel = new MessageChannel()
  const disposeServer = serveHubProxy(buildHubProxyHandlers(authorities, instantRunner), channel.port2)
  const client = createHubProxyClient(channel.port1)
  const bridge = hubProxyMessagesTransform(client)
  if (bridge === undefined) throw new Error("hub proxy client did not yield a messages transform bridge")
  return {
    host: createWorkerPluginHost([], { messagesTransform: bridge }),
    dispose() {
      disposeServer()
      client.dispose()
      channel.port1.close()
      channel.port2.close()
    },
  }
}

/**
 * OFF: the in-process shape (what flag-off semantics look like inside a worker).
 * The worker-hosted plugin hook mutates `output.messages` in place and the host
 * returns that same object — no port, no clone.
 */
function inProcessHost(): WorkerPluginHost {
  return createWorkerPluginHost([
    {
      name: "dcp-stub",
      hooks: {
        "experimental.chat.messages.transform": (_input, output) => {
          pruneOldToolResults(output.messages as MessageLike[])
        },
      },
    },
  ])
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
      title: "b1-parity",
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

/** Boot a fresh provider + worker graph with `host` and run the seeded prompt turn. */
async function runPromptWithHost(
  buildHost: () => { host: WorkerPluginHost; dispose: () => void },
  snapshot: WorkerSnapshot,
): Promise<ReadonlyArray<Record<string, unknown>>> {
  const provider = startMockProvider({ chunks: ["ok"] })
  const ctx = createE2EContext({ snapshot, modelConfig: modelConfig(provider) })
  const built = buildHost()
  try {
    const layer = workerLayer(snapshot, provider, ctx.writer, built.host)
    const sessionID = await Effect.runPromise(seed(layer, ctx.instance, snapshot.directory, provider.model))
    await Effect.runPromise(
      Effect.gen(function* () {
        const prompt = yield* SessionPrompt.Service
        return yield* prompt.loop({ sessionID })
      }).pipe(Effect.provide(layer), Effect.provideService(InstanceRef, ctx.instance)),
    )
    // Readiness signal, no sleep: the provider request is recorded before the
    // loop resolves, but wait on it explicitly to make the ordering explicit.
    await provider.wait(1)
    return turnBodies(provider)
  } finally {
    built.dispose()
    await ctx.dispose()
    await provider.stop()
  }
}

/** Run one prompt turn through the REAL hub-proxy boundary. */
function runPrompt(snapshot: WorkerSnapshot): Promise<ReadonlyArray<Record<string, unknown>>> {
  return runPromptWithHost(hubProxyHost, snapshot)
}

/** Run one compaction through a freshly booted worker graph; return its request bodies. */
async function runCompaction(
  buildHost: () => { host: WorkerPluginHost; dispose: () => void },
  snapshot: WorkerSnapshot,
): Promise<ReadonlyArray<Record<string, unknown>>> {
  const provider = startMockProvider({ chunks: ["summary"] })
  const ctx = createE2EContext({ snapshot, modelConfig: modelConfig(provider) })
  const built = buildHost()
  try {
    const layer = workerLayer(snapshot, provider, ctx.writer, built.host)
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
    await provider.wait(1)
    return turnBodies(provider)
  } finally {
    built.dispose()
    await ctx.dispose()
    await provider.stop()
  }
}

/** The OFF arm's host builder (in-process semantics; a true flag-off has no worker). */
function inProcessHostBuilder(): { host: WorkerPluginHost; dispose: () => void } {
  return { host: inProcessHost(), dispose: () => {} }
}

/** A shared directory keeps the system prompt (cwd/date) identical across both paths. */
async function withSharedDirectory(run: (snapshot: WorkerSnapshot) => Promise<void>): Promise<void> {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "oc-b1-parity-"))
  try {
    await run(workerSnapshot({ directory }))
  } finally {
    fs.rmSync(directory, { recursive: true, force: true })
  }
}

describe("experimental.chat.messages.transform: worker ON ≡ OFF", () => {
  test("a DCP-shaped transform yields the same prompt request through the hub proxy and in-process", async () => {
    await withSharedDirectory(async (snapshot) => {
      const on = await runPrompt(snapshot)
      const off = await runPromptWithHost(inProcessHostBuilder, snapshot)

      expect(on.length).toBeGreaterThanOrEqual(1)
      expect(on).toEqual(off)
      const serialized = JSON.stringify(on)
      expect(serialized).toContain(SENTINEL)
      expect(serialized).not.toContain(OLD_OUTPUT)
    })
  }, 60_000)

  test("a DCP-shaped transform yields the same compaction conversation through the hub proxy and in-process", async () => {
    await withSharedDirectory(async (snapshot) => {
      const on = await runCompaction(hubProxyHost, snapshot)
      const off = await runCompaction(inProcessHostBuilder, snapshot)

      expect(on.length).toBeGreaterThanOrEqual(1)
      expect(on).toEqual(off)
      const serialized = JSON.stringify(on)
      expect(serialized).toContain(SENTINEL)
      expect(serialized).not.toContain(OLD_OUTPUT)
    })
  }, 60_000)
})
