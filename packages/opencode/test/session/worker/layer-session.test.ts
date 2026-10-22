// Wave 4 — session-worker production wiring tests.
//
//   * the full `RUNNABLE_SESSION_NODES` graph BOOTS against the functional
//     boundary layers (BackgroundJob + the 9 excluded nodes);
//   * a real session runs against `test/lib/mock-provider.ts` through the worker
//     `SessionWork`, asserting a durable publish + an egress event + non-empty
//     assistant text;
//   * the hub-proxy control wire satisfies `SessionStatus` over a port.

import { describe, expect, test } from "bun:test"
import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { Effect, Layer } from "effect"
import { MessageID, PartID, SessionID } from "@/session/schema"
import { Session } from "@/session/session"
import { SessionPrompt } from "@/session/prompt"
import { SessionProcessor } from "@/session/processor"
import { LLM } from "@/session/llm"
import { SessionStatus } from "@/session/status"
import { InstanceRef } from "@/effect/instance-ref"
import type { InstanceContext } from "@/project/instance-context"
import {
  ModelClient,
  PIPELINE_SESSION_NODES,
  RUNNABLE_SESSION_NODES,
  buildSessionWorkerLayer,
  createHubProxyClient,
  llmModelClientLayer,
  reducedLlmLayer,
  serveHubProxy,
  type SessionLayerOptions,
  type WorkerModelConfig,
} from "@/session/worker/layer"
import { createSessionWork, workerGraphReplacements, workerInstanceContext } from "@/session/worker/session-worker"
import { collectNodeNames } from "@/session/worker/node-table"
import {
  buildHubProxyHandlers,
  type HubProxyAuthorities,
  type HubProxyRunner,
} from "@/session/worker/hub"
import { Permission } from "@/permission"
import { Question } from "@/question"
import { startMockProvider, type MockProviderHandle } from "../../lib/mock-provider"
import { workerSnapshot } from "../../lib/snapshot"
import { createE2EContext, type E2EContext } from "../../lib/parity-harness"

const sid = (value: string) => SessionID.make(value)

const modelConfig = (provider: MockProviderHandle): WorkerModelConfig => ({
  providerID: "mock",
  modelID: provider.model,
  apiId: provider.model,
  npm: "@ai-sdk/openai-compatible",
  baseUrl: provider.baseUrl,
  apiKey: provider.apiKey,
  headers: {},
})

const stubClients = (): SessionLayerOptions["clients"] => ({
  writer: { query: async () => [], publish: async () => ({ seq: 1, committed: true }) },
  egress: { emit: () => {} },
  broker: { call: async () => null },
  backgroundJob: { wait: async () => null, cancel: async () => {} },
})

const stubModel = () => Layer.succeed(ModelClient, { complete: () => Effect.succeed({ text: "stub" }) })

describe("full worker graph boot", () => {
  test("RUNNABLE_SESSION_NODES boots with functional boundary layers", async () => {
    const built = buildSessionWorkerLayer({
      snapshot: workerSnapshot(),
      env: { values: {} },
      clients: stubClients(),
      modelClient: stubModel(),
    })
    // Booting the layer builds every node eagerly; an absent boundary service
    // (the previous `Service not found: @opencode/BackgroundJob`) would reject.
    await Effect.runPromise(Effect.void.pipe(Effect.provide(built.layer)))
    expect(true).toBe(true)
  })

  test("the default graph reaches the real SessionPrompt/SessionProcessor/LLM path", async () => {
    const built = buildSessionWorkerLayer({
      snapshot: workerSnapshot(),
      env: { values: {} },
      clients: stubClients(),
      modelClient: stubModel(),
    })
    const services = await Effect.runPromise(
      Effect.gen(function* () {
        return {
          prompt: yield* Effect.serviceOption(SessionPrompt.Service),
          processor: yield* Effect.serviceOption(SessionProcessor.Service),
          llm: yield* Effect.serviceOption(LLM.Service),
        }
      }).pipe(Effect.provide(built.layer)),
    )
    // The production `createSessionWork` swap relies on the whole prompt path
    // being present, not just the entry service.
    expect(services.prompt._tag).toBe("Some")
    expect(services.processor._tag).toBe("Some")
    expect(services.llm._tag).toBe("Some")
  })

  test("the default production graph materializes its own durable rows (SessionProjector in-worker)", () => {
    // Production must not rely on a test-added projector: the default graph
    // carries `SessionProjector` and the boundary layer must not blank it out.
    const names = collectNodeNames(RUNNABLE_SESSION_NODES)
    expect(names.has("session-projector")).toBe(true)
    const built = buildSessionWorkerLayer({
      snapshot: workerSnapshot(),
      env: { values: {} },
      clients: stubClients(),
      modelClient: stubModel(),
    })
    const replaced = new Set(built.replacements.map(([node]) => node.name))
    expect(replaced.has("session-projector")).toBe(false)
  })

  test("the bootable graph is the full runnable graph, not the pipeline subset", () => {
    const names = new Set<string>()
    const walk = (node: { readonly kind: string; readonly name: string; readonly dependencies: readonly unknown[] }) => {
      if (node.kind !== "group") names.add(node.name)
      for (const dep of node.dependencies) walk(dep as typeof node)
    }
    walk(RUNNABLE_SESSION_NODES as never)
    expect(names.has("@opencode/SessionPrompt")).toBe(true)
    expect(names.has("@opencode/BackgroundJob")).toBe(true)
  })
})

describe("real session against the mock provider", () => {
  test("runs the worker SessionWork, publishes durably and egresses the text", async () => {
    await using provider = startMockProvider({ chunks: ["hel", "lo ", "worker"] })
    const config = modelConfig(provider)

    const published: Array<{ type: string; committed: boolean }> = []
    const emitted: Array<{ type: string; text: string }> = []

    const built = buildSessionWorkerLayer({
      nodes: PIPELINE_SESSION_NODES,
      snapshot: workerSnapshot(),
      env: { values: {} },
      clients: {
        writer: {
          query: async () => [],
          publish: async (event) => {
            published.push({ type: event.type, committed: true })
            return { seq: 42, committed: true }
          },
        },
        egress: {
          emit: (event) =>
            void emitted.push({ type: event.type, text: (event.properties as { text: string }).text }),
        },
        broker: { call: async () => null },
        backgroundJob: { wait: async () => null, cancel: async () => {} },
      },
      // The LLM-backed ModelClient makes a real streaming call to the provider.
      modelClient: llmModelClientLayer(config).pipe(Layer.provide(reducedLlmLayer(workerSnapshot(), config))),
    })

    const work = createSessionWork({
      layer: built.layer,
      writer: { query: async () => [], publish: async () => ({ seq: 0, committed: false }) },
      threadId: 3,
      readMessages: () => Effect.succeed([{ role: "user", content: "say hello" }]),
    })

    const result = await Effect.runPromise(work(sid("ses_real")))
    const value = result.value as { text: string; publishedSeq: number | null; emitted: number; threadId: number }

    // The assistant text is non-empty and comes from the real streamed reply.
    expect(value.text).toBe("hello worker")
    expect(value.threadId).toBe(3)
    // Durable publish + egress event.
    expect(value.publishedSeq).toBe(42)
    expect(value.emitted).toBe(1)
    expect(published).toEqual([{ type: "message.assistant", committed: true }])
    expect(emitted).toEqual([{ type: "session.text", text: "hello worker" }])
    expect(provider.requests).toHaveLength(1)
  })
})

describe("hub-proxy control wire (production node wiring)", () => {
  test("buildSessionWorkerLayer routes SessionStatus over the hub-proxy port", async () => {
    const channel = new MessageChannel()
    const store = new Map<string, SessionStatus.Info>()
    serveHubProxy(
      {
        get: (sessionID) => store.get(sessionID as string) ?? { type: "idle" },
        set: (sessionID, status) => {
          store.set(sessionID as string, status as SessionStatus.Info)
        },
        list: () => [...store.entries()],
      },
      channel.port2,
    )
    const hubProxyClient = createHubProxyClient(channel.port1)

    const built = buildSessionWorkerLayer({
      snapshot: workerSnapshot(),
      env: { values: {} },
      clients: stubClients(),
      modelClient: stubModel(),
      hubProxies: { sessionStatus: hubProxyClient, permission: hubProxyClient, question: hubProxyClient },
    })

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const status = yield* SessionStatus.Service
        yield* status.set(sid("ses_proxy"), { type: "busy" })
        return yield* status.get(sid("ses_proxy"))
      }).pipe(Effect.provide(built.layer)),
    )
    expect(result).toEqual({ type: "busy" })
    // The hub-side store is the authority: its population proves the call
    // crossed the hub-proxy port rather than hitting an in-process store.
    expect(store.get("ses_proxy")).toEqual({ type: "busy" })
  })

  test("the production handler map routes the multiplexed wire to the right authority", async () => {
    // One worker control port multiplexes SessionStatus / Permission / Question,
    // so `ask`/`reply` (Permission+Question) and `list` (all three) collide. The
    // production map disambiguates `ask`/`reply` by request shape.
    const calls: string[] = []
    const authorities: HubProxyAuthorities = {
      sessionStatus: {
        get: (sessionID) =>
          Effect.sync(() => {
            calls.push(`sessionStatus.get:${sessionID}`)
            return { type: "idle" } as SessionStatus.Info
          }),
        list: () =>
          Effect.sync(() => {
            calls.push("sessionStatus.list")
            return new Map()
          }),
        set: (sessionID, status) =>
          Effect.sync(() => {
            calls.push(`sessionStatus.set:${sessionID}:${status.type}`)
          }),
      },
      permission: {
        ask: (input) =>
          Effect.sync(() => {
            calls.push(`permission.ask:${input.permission}`)
          }),
        reply: (input) =>
          Effect.sync(() => {
            calls.push(`permission.reply:${input.requestID}`)
          }),
        list: () =>
          Effect.sync(() => {
            calls.push("permission.list")
            return []
          }),
      },
      question: {
        ask: (input) =>
          Effect.sync(() => {
            calls.push(`question.ask:${input.questions.length}`)
            return []
          }),
        reply: (input) =>
          Effect.sync(() => {
            calls.push(`question.reply:${input.requestID}`)
          }),
        reject: (requestID) =>
          Effect.sync(() => {
            calls.push(`question.reject:${requestID}`)
          }),
        list: () =>
          Effect.sync(() => {
            calls.push("question.list")
            return []
          }),
      },
    }
    const run: HubProxyRunner = (effect) => Effect.runPromise(effect)
    const handlers = buildHubProxyHandlers(authorities, run)

    expect(await handlers.get(sid("ses_a"))).toEqual({ type: "idle" })
    await handlers.ask({ permission: "bash", patterns: ["*"] })
    await handlers.ask({ sessionID: sid("ses_q"), questions: [] })
    await handlers.reply({ requestID: "req_p", reply: "once" })
    await handlers.reply({ requestID: "req_q", answers: [["yes"]] })
    await handlers.reject("req_q")

    expect(calls).toEqual([
      "sessionStatus.get:ses_a",
      "permission.ask:bash",
      "question.ask:0",
      "permission.reply:req_p",
      "question.reply:req_q",
      "question.reject:req_q",
    ])
  })
})

// ---------------------------------------------------------------------------
// The full graph runs the REAL prompt path (not the reduced `SessionPipeline`):
// a mock session completes end-to-end over the writer-backed `Database`.
// ---------------------------------------------------------------------------

describe("full-graph prompt path against the mock provider", () => {
  const modelConfig = (provider: MockProviderHandle) => ({
    providerID: "mock",
    modelID: provider.model,
    apiId: provider.model,
    npm: "@ai-sdk/openai-compatible",
    baseUrl: provider.baseUrl,
    apiKey: provider.apiKey,
    headers: {},
  })

  const seed = (ctx: E2EContext, modelID: string, prompt: string) =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const session = yield* sessions.create({
        title: "full-graph",
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
        text: prompt,
      } as never)
      return session.id
    }).pipe(Effect.provide(ctx.layer), Effect.provideService(InstanceRef, ctx.instance))

  test("runs SessionPrompt.loop and completes a mock session", async () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "oc-layer-e2e-"))
    await using provider = startMockProvider({ chunks: ["wor", "ld"] })
    const ctx = createE2EContext({
      snapshot: workerSnapshot({ directory }),
      modelConfig: modelConfig(provider),
    })
    try {
      const sessionID = await Effect.runPromise(seed(ctx, provider.model, "hi"))
      const result = await Effect.runPromise(
        Effect.gen(function* () {
          const prompt = yield* SessionPrompt.Service
          return yield* prompt.loop({ sessionID })
        }).pipe(Effect.provide(ctx.layer), Effect.provideService(InstanceRef, ctx.instance)),
      )
      const value = result as { parts: ReadonlyArray<{ type: string; text?: string }> }
      const text = value.parts
        .filter((part) => part.type === "text")
        .map((part) => part.text ?? "")
        .join("")
      expect(text).toBe("world")
      expect(provider.requests.length).toBeGreaterThanOrEqual(1)
      // The durable rows prove the writer-backed Database actually ran.
      expect(ctx.rows("message")).toHaveLength(2)
      expect(ctx.rows("part").length).toBeGreaterThanOrEqual(2)
    } finally {
      await ctx.dispose()
      fs.rmSync(directory, { recursive: true, force: true })
    }
  }, 60_000)

  test("the DEFAULT production boundary carries SessionPrompt.loop past the mock boundary (f-w5)", async () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "oc-layer-default-"))
    await using provider = startMockProvider({ chunks: ["pas", "t"] })
    const snapshot = workerSnapshot({ directory })
    const ctx = createE2EContext({ snapshot, modelConfig: modelConfig(provider) })
    try {
      const sessionID = await Effect.runPromise(seed(ctx, provider.model, "hi"))
      // A second layer built ONLY from production defaults: unlike `ctx.layer`
      // it omits the harness's functional MCP/LSP overrides, so it exercises the
      // real `boundaryReplacements` — the exact path the packaged gate bundles.
      const config = modelConfig(provider)
      const built = buildSessionWorkerLayer({
        snapshot,
        env: { values: {} },
        clients: {
          writer: ctx.writer,
          egress: { emit: () => {}, emitDurable: () => {} },
          broker: { call: async () => null },
          backgroundJob: { wait: async () => null, cancel: async () => {} },
        },
        modelClient: llmModelClientLayer(config).pipe(Layer.provide(reducedLlmLayer(snapshot, config))),
        // The hub-shipped provider/snapshot the production worker installs; MCP
        // and LSP are intentionally left to `boundaryReplacements`.
        replacements: workerGraphReplacements(config),
      })
      const result = await Effect.runPromise(
        Effect.gen(function* () {
          const prompt = yield* SessionPrompt.Service
          return yield* prompt.loop({ sessionID })
        }).pipe(Effect.provide(built.layer), Effect.provideService(InstanceRef, ctx.instance)),
      )
      const value = result as { parts: ReadonlyArray<{ type: string; text?: string }> }
      const text = value.parts
        .filter((part) => part.type === "text")
        .map((part) => part.text ?? "")
        .join("")
      expect(text).toBe("past")
      // The durable rows prove the writer-backed Database ran through the
      // production graph, not just the harness's overridden one.
      expect(ctx.rows("message")).toHaveLength(2)
    } finally {
      await ctx.dispose()
      fs.rmSync(directory, { recursive: true, force: true })
    }
  }, 60_000)
})

// ---------------------------------------------------------------------------
// Fix B — the worker's ambient `InstanceRef` carries a REAL project id (the hub
// ships it on the snapshot), so `SessionPrompt.loop` never hits the
// `ctx.project.id` `TypeError` that the old `project: undefined as never` caused.
// ---------------------------------------------------------------------------

describe("real project ambient InstanceRef (Fix B)", () => {
  const seedWith = (ctx: E2EContext, instance: InstanceContext, modelID: string, prompt: string) =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const session = yield* sessions.create({
        title: "real-project",
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
        text: prompt,
      } as never)
      return session.id
    }).pipe(Effect.provide(ctx.layer), Effect.provideService(InstanceRef, instance))

  test("SessionPrompt.loop completes a mock session from a real project without a TypeError", async () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "oc-realproj-"))
    await using provider = startMockProvider({ chunks: ["re", "al"] })
    // No `vcs`: the project is real (a stable id + worktree) but not a git
    // checkout, so `Snapshot.track` short-circuits instead of shelling out.
    const snapshot = workerSnapshot({ directory, projectId: "prj_real", worktree: directory })
    const ctx = createE2EContext({ snapshot, modelConfig: modelConfig(provider) })
    const instance = workerInstanceContext(snapshot)
    try {
      const sessionID = await Effect.runPromise(seedWith(ctx, instance, provider.model, "hi"))
      const result = await Effect.runPromise(
        Effect.gen(function* () {
          const prompt = yield* SessionPrompt.Service
          return yield* prompt.loop({ sessionID })
        }).pipe(Effect.provide(ctx.layer), Effect.provideService(InstanceRef, instance)),
      )
      const value = result as { parts: ReadonlyArray<{ type: string; text?: string }> }
      const text = value.parts
        .filter((part) => part.type === "text")
        .map((part) => part.text ?? "")
        .join("")
      expect(text).toBe("real")
      expect(ctx.rows("message")).toHaveLength(2)
    } finally {
      await ctx.dispose()
      fs.rmSync(directory, { recursive: true, force: true })
    }
  }, 60_000)
})
