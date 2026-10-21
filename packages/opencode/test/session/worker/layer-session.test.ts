// Wave 4 — session-worker production wiring tests.
//
//   * the full `RUNNABLE_SESSION_NODES` graph BOOTS against the functional
//     boundary layers (BackgroundJob + the 9 excluded nodes);
//   * a real session runs against `test/lib/mock-provider.ts` through the worker
//     `SessionWork`, asserting a durable publish + an egress event + non-empty
//     assistant text;
//   * the hub-proxy control wire satisfies `SessionStatus` over a port.

import { describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import { SessionID } from "@/session/schema"
import { SessionPrompt } from "@/session/prompt"
import { SessionStatus } from "@/session/status"
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
import { createSessionWork } from "@/session/worker/session-worker"
import {
  buildHubProxyHandlers,
  type HubProxyAuthorities,
  type HubProxyRunner,
} from "@/session/worker/hub"
import { Permission } from "@/permission"
import { Question } from "@/question"
import { startMockProvider, type MockProviderHandle } from "../../lib/mock-provider"
import { workerSnapshot } from "../../lib/snapshot"

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

  test("the default graph reaches the real SessionPrompt loop", async () => {
    const built = buildSessionWorkerLayer({
      snapshot: workerSnapshot(),
      env: { values: {} },
      clients: stubClients(),
      modelClient: stubModel(),
    })
    const provided = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* Effect.serviceOption(SessionPrompt.Service)
      }).pipe(Effect.provide(built.layer)),
    )
    // The production `createSessionWork` swap relies on this being present.
    expect(provided._tag).toBe("Some")
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
