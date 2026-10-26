// C1 (finding M1) — the REAL hub-proxy boundary for
// `experimental.chat.messages.transform`.
//
// The "ON" suites historically simulated the hub-proxy outcome with an
// in-worker stand-in: `createWorkerPluginHost([], { messagesTransform: … })`
// doing a local `structuredClone` + prune. That proves the call-site
// reconciliation but never exercises the actual wire. This file removes the
// stand-in and runs the genuine path end to end:
//
//   prompt.ts / compaction.ts call site
//     → worker `createWorkerPluginHost` (no worker-local transform hooks)
//       → `hubProxyMessagesTransform(createHubProxyClient(port1))`  (real worker bridge)
//         → `MessageChannel`                                    (real structured-clone boundary)
//           → `serveHubProxy(buildHubProxyHandlers(authorities, run), port2)`
//             → `messagesTransformAuthority` → stub plugin `Plugin.trigger(
//                  "experimental.chat.messages.transform", …)` (real hub authority)
//
// A stub hub plugin prunes an old completed tool-result's output to the exact
// DCP sentinel; the pruned `{ messages }` must cross the port as a FRESH array
// (never mutate the caller in place) and reach the outbound provider request,
// while the persisted session keeps the original output.

import { describe, expect, test } from "bun:test"
import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { Effect, Layer, Option } from "effect"
import { MessageID, PartID, type SessionID } from "@/session/schema"
import { Session } from "@/session/session"
import { SessionPrompt } from "@/session/prompt"
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
import { instantRunner, recordingHubAuthorities, type HubAuthorityCall } from "../../lib/hub-proxy"
import { createE2EContext, type E2EContext } from "../../lib/parity-harness"
import { pluginPassThroughReplacement } from "../../lib/worker-plugin-routes"
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
 * recent message. The real plugin hook mutates `output.messages` in place; the
 * clone that makes it safe happens when the reply crosses the control port.
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
 * A hub-resident stub plugin: its `experimental.chat.messages.transform` hook
 * prunes in place and `trigger` resolves the mutated `{ messages }`. This is the
 * shape `Plugin.trigger` returns in production; the hub authority runs it.
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

/** A served hub-proxy pair with the real messages bridge wired into a worker host. */
interface RealHubProxyHost {
  readonly host: WorkerPluginHost
  /** Every authority call the hub recorded; proves the port was actually crossed. */
  readonly calls: ReadonlyArray<HubAuthorityCall>
  dispose(): void
}

/**
 * Stand up the genuine boundary: a real `MessageChannel` with `serveHubProxy`
 * on port2 and `createHubProxyClient` on port1. The worker's plugin host then
 * carries the REAL `hubProxyMessagesTransform` bridge over that client (never a
 * local stand-in), so the hub stub plugin's prune must cross the port to apply.
 */
function realHubProxyHost(): RealHubProxyHost {
  const { authorities, calls } = recordingHubAuthorities({ messagesTransform: dcpMessagesAuthority() })
  const channel = new MessageChannel()
  const disposeServer = serveHubProxy(buildHubProxyHandlers(authorities, instantRunner), channel.port2)
  const client = createHubProxyClient(channel.port1)
  const bridge = hubProxyMessagesTransform(client)
  if (bridge === undefined) throw new Error("hub proxy client did not yield a messages transform bridge")
  return {
    host: createWorkerPluginHost([], { messagesTransform: bridge }),
    calls,
    dispose() {
      disposeServer()
      client.dispose()
      channel.port1.close()
      channel.port2.close()
    },
  }
}

/** The full worker graph over the E2E writer, with the real hub-proxy host. */
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
      title: "c1-hub-proxy-e2e",
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

/** Run one prompt turn, then read back the persisted history. */
function runTurn(layer: Layer.Layer<unknown, unknown>, instance: E2EContext["instance"], sessionID: SessionID) {
  return Effect.gen(function* () {
    const prompt = yield* SessionPrompt.Service
    yield* prompt.loop({ sessionID })
    const sessions = yield* Session.Service
    return yield* sessions.messages({ sessionID })
  }).pipe(Effect.provide(layer), Effect.provideService(InstanceRef, instance))
}

/** The outbound request bodies that belong to the seeded turn. */
function turnBodies(provider: MockProviderHandle): ReadonlyArray<Record<string, unknown>> {
  return provider.requests
    .map((request) => request.body)
    .filter((body) => JSON.stringify(body).includes(TURN_TEXT))
}

describe("messages.transform crosses the REAL hub-proxy boundary", () => {
  test("a hub stub plugin's DCP prune reaches the provider request over the control port", async () => {
    const provider = startMockProvider({ chunks: ["ok"] })
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "oc-c1-hub-proxy-"))
    const snapshot = workerSnapshot({ directory })
    const ctx = createE2EContext({ snapshot, modelConfig: modelConfig(provider) })
    const proxy = realHubProxyHost()
    try {
      const layer = workerLayer(snapshot, provider, ctx.writer, proxy.host)
      const sessionID = await Effect.runPromise(seed(layer, ctx.instance, directory, provider.model))
      const persisted = await Effect.runPromise(runTurn(layer, ctx.instance, sessionID))

      // Readiness signal: the provider only records after it answers, and the
      // loop only resolves after that reply — no sleep, wait on the request.
      await provider.wait(1)

      // The stub authority ran on the hub, reached over the port. Remove the
      // real bridge (or the served handler) and this call list is empty.
      expect(proxy.calls.some((call) => call.method === "messagesTransform")).toBe(true)

      const bodies = turnBodies(provider)
      expect(bodies.length).toBeGreaterThanOrEqual(1)
      const serialized = JSON.stringify(bodies)
      expect(serialized).toContain(SENTINEL)
      expect(serialized).not.toContain(OLD_OUTPUT)
      // The transform rewrites only the outbound request; the durable history
      // keeps the real tool output.
      expect(JSON.stringify(persisted)).toContain(OLD_OUTPUT)
    } finally {
      proxy.dispose()
      await ctx.dispose()
      await provider.stop()
      fs.rmSync(directory, { recursive: true, force: true })
    }
  }, 60_000)
})
