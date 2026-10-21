// Wave 2 — session-worker runtime tests (TDD plan steps 22, 27, 28).
//
// The production entry is import-safe outside a worker (it only bootstraps when
// `workerData` is present), so the pure aggregation / registry / work builders
// are exercised in-process here.

import { describe, expect, test } from "bun:test"
import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { Effect, Latch, Layer, Schema } from "effect"
import { EventV2 } from "@opencode-ai/core/event"
import { EventV2Bridge } from "@/event-v2-bridge"
import { MessageID, PartID, SessionID } from "@/session/schema"
import { Session } from "@/session/session"
import { SessionRunState } from "@/session/run-state"
import { SessionStatus } from "@/session/status"
import { InstanceRef } from "@/effect/instance-ref"
import { serveWriterPort, type HubWriter } from "@/session/writer/client"
import type { EventV2Event } from "@/session/worker/frame"
import { collectNodeNames } from "@/session/worker/node-table"
import {
  EventEgressClientNode,
  EventEgressClientService,
  ModelClient,
  PIPELINE_SESSION_NODES,
  RUNNABLE_SESSION_NODES,
  buildSessionWorkerLayer,
  createHubProxyClient,
  serveHubProxy,
  type SessionLayerOptions,
} from "@/session/worker/layer"
import {
  DEFAULT_WORK_KIND,
  UnknownWorkKindError,
  type SessionWork,
} from "@/session/worker/execute"
import {
  MissingWorkerPortError,
  bootstrapSessionWorker,
  buildWorkerClients,
  createSessionWork,
  createWorkRegistry,
  productionWorkRegistry,
  writerReadMessages,
} from "@/session/worker/session-worker"
import { createHubEventSink } from "@/session/worker/hub"
import { workerSnapshot } from "../../lib/snapshot"
import { withParts } from "../../lib/session-parts"
import { startMockProvider } from "../../lib/mock-provider"
import { createE2EContext, type E2EContext } from "../../lib/parity-harness"

const sid = (value: string) => SessionID.make(value)

const stubClients = (): SessionLayerOptions["clients"] => ({
  writer: { query: async () => [], publish: async () => ({ seq: 1, committed: true }) },
  egress: { emit: () => {} },
  broker: { call: async () => null },
  backgroundJob: { wait: async () => null, cancel: async () => {} },
})

const modelLayer = (text: string) =>
  Layer.succeed(ModelClient, {
    complete: () => Effect.succeed({ text }),
  })

const buildLayer = (overrides: Partial<SessionLayerOptions> = {}) =>
  buildSessionWorkerLayer({
    // The pipeline tests exercise the reduced one-shot seam explicitly; the
    // production default is the full runnable graph.
    nodes: PIPELINE_SESSION_NODES,
    snapshot: workerSnapshot(),
    env: { values: {} },
    clients: stubClients(),
    modelClient: modelLayer("worker text"),
    ...overrides,
  })

describe("bootstrapSessionWorker control-port transfer (steps 22 / Wave 4)", () => {
  const fakeScope = () => {
    const frames: Array<{ data: unknown; transfer: readonly Transferable[] }> = []
    const scope = {
      postMessage(data: unknown, transfer?: readonly Transferable[]) {
        frames.push({ data, transfer: transfer ?? [] })
      },
      onmessage: null as ((event: { data: unknown }) => void) | null,
    }
    return { frames, scope }
  }

  const init = (capabilities?: { sessionWorkerPorts?: boolean }) => ({
    snapshot: workerSnapshot(),
    env: { values: {} },
    model: { baseUrl: "http://127.0.0.1:1/v1", apiKey: "k", model: "m" },
    ...(capabilities === undefined ? {} : { capabilities }),
  })

  const frameTypes = (frames: ReadonlyArray<{ data: unknown }>): string[] =>
    frames.map((frame) => (frame.data as { type?: string }).type ?? "")

  test("transfers job, worker and hub-proxy ports when the capability is advertised", () => {
    const { frames, scope } = fakeScope()
    bootstrapSessionWorker(init({ sessionWorkerPorts: true }), scope)
    const types = frameTypes(frames)
    // The dedicated job control link is always transferred; the writer/egress/
    // broker bundle and the hub-proxy control port join it for a full hub.
    expect(types).toContain("job-port")
    expect(types).toContain("ports")
    expect(types).toContain("hub-proxy")
    expect(types).toContain("ready")
    expect(typeof scope.onmessage).toBe("function")
  })

  test("the hub-proxy frame carries a transferred MessagePort", () => {
    const { frames, scope } = fakeScope()
    bootstrapSessionWorker(init({ sessionWorkerPorts: true }), scope)
    const hubProxy = frames.find((frame) => (frame.data as { type?: string }).type === "hub-proxy")
    expect(hubProxy).toBeDefined()
    const port = (hubProxy!.data as { port?: unknown }).port
    expect(port).toBeInstanceOf(MessagePort)
    expect(hubProxy!.transfer).toHaveLength(1)
  })

  test("degrades to the reduced pipeline without the capability", () => {
    const { frames, scope } = fakeScope()
    bootstrapSessionWorker(init(), scope)
    const types = frameTypes(frames)
    expect(types).toContain("job-port")
    expect(types).toContain("ready")
    // No served ports: transferring one nobody serves would hang a session.
    expect(types).not.toContain("ports")
    expect(types).not.toContain("hub-proxy")
  })
})

// ---------------------------------------------------------------------------
// Step 22 — client aggregation.
// ---------------------------------------------------------------------------

describe("buildWorkerClients (step 22)", () => {
  const emptyChannels = () => {
    const writer = new MessageChannel()
    const egress = new MessageChannel()
    const broker = new MessageChannel()
    const backgroundJob = new MessageChannel()
    return { writer, egress, broker, backgroundJob }
  }

  test("every port is required and a missing one fails loud", () => {
    expect(() => buildWorkerClients({})).toThrow(MissingWorkerPortError)
    expect(() => buildWorkerClients({})).toThrow(/writer/)

    const channels = emptyChannels()
    expect(() => buildWorkerClients({ writer: channels.writer.port1 })).toThrow(/egress/)
    expect(() => buildWorkerClients({ writer: channels.writer.port1, egress: channels.egress.port1 })).toThrow(/broker/)
    expect(() =>
      buildWorkerClients({
        writer: channels.writer.port1,
        egress: channels.egress.port1,
        broker: channels.broker.port1,
      }),
    ).toThrow(/backgroundJob/)
  })

  test("wires the four clients and round-trips writer publish/query over the port", async () => {
    const channels = emptyChannels()
    const published: string[] = []
    const hubWriter: HubWriter = {
      query: async () => [{ value: 1 }],
      publish: async (event) => {
        published.push(event.type)
        return { seq: 9, committed: true }
      },
      begin: async () => {
        throw new Error("begin was not expected in this test")
      },
      txQuery: async () => {
        throw new Error("txQuery was not expected in this test")
      },
      commit: async () => {
        throw new Error("commit was not expected in this test")
      },
      rollback: async () => {
        throw new Error("rollback was not expected in this test")
      },
    }
    serveWriterPort(hubWriter, channels.writer.port2)

    const clients = buildWorkerClients({
      writer: channels.writer.port1,
      egress: channels.egress.port1,
      broker: channels.broker.port1,
      backgroundJob: channels.backgroundJob.port1,
    })

    expect(typeof clients.writer.publish).toBe("function")
    expect(typeof clients.egress.emit).toBe("function")
    expect(typeof clients.broker.call).toBe("function")
    expect(typeof clients.backgroundJob.wait).toBe("function")

    const result = await clients.writer.publish({
      id: "e1",
      type: "message.assistant",
      aggregateID: "s1",
      data: { text: "hi" },
    })
    expect(result).toEqual({ seq: 9, committed: true })
    expect(published).toEqual(["message.assistant"])

    expect(await clients.writer.query("all", "SELECT 1")).toEqual([{ value: 1 }])
  })
})

// ---------------------------------------------------------------------------
// Step 28 — work-kind registry.
// ---------------------------------------------------------------------------

describe("createWorkRegistry (step 28)", () => {
  const marker: SessionWork = (sessionID) => Effect.succeed({ sessionID, value: { kind: "session" } })

  test("resolves a registered kind to its factory", () => {
    const registry = createWorkRegistry({ session: marker })
    expect(registry("session")).toBe(marker)
  })

  test("an unknown kind throws a typed UnknownWorkKindError", () => {
    const registry = createWorkRegistry({ session: marker })
    expect(() => registry("nope")).toThrow(UnknownWorkKindError)
    try {
      registry("nope")
    } catch (error) {
      expect((error as UnknownWorkKindError).kind).toBe("nope")
    }
  })

  test("the production registry runs session work for the session and legacy kinds", () => {
    const registry = productionWorkRegistry(marker)
    expect(registry("session")).toBe(marker)
    expect(registry(DEFAULT_WORK_KIND)).toBe(marker)
    expect(() => registry("other")).toThrow(UnknownWorkKindError)
  })
})

// ---------------------------------------------------------------------------
// Step 27 — real session work.
// ---------------------------------------------------------------------------

describe("createSessionWork (step 27)", () => {
  test("reads messages from the writer seam and returns the pipeline result", async () => {
    const built = buildLayer()
    const asked: string[] = []
    const work = createSessionWork({
      layer: built.layer,
      writer: stubClients().writer,
      threadId: 7,
      readMessages: (sessionID) => {
        asked.push(sessionID)
        return Effect.succeed([{ role: "user", content: "hello" }])
      },
    })

    const result = await Effect.runPromise(work(sid("ses_a")))
    expect(asked).toEqual([sid("ses_a")])
    expect(result.sessionID).toBe(sid("ses_a"))
    expect(result.value).toMatchObject({ text: "worker text", publishedSeq: 1, emitted: 1, threadId: 7 })
  })

  test("defaults to reading persisted messages through the writer client", async () => {
    const built = buildLayer()
    const queries: Array<{ method: string; sql: string; params?: readonly unknown[] }> = []
    const writer = {
      query: async (method: string, sql: string, params?: readonly unknown[]) => {
        queries.push({ method, sql, params })
        return [{ data: JSON.stringify({ role: "user", parts: [{ text: "from-db" }] }) }]
      },
      publish: async () => ({ seq: 1, committed: true }),
    }
    const work = createSessionWork({ layer: built.layer, writer, threadId: 0 })
    const result = await Effect.runPromise(work(sid("ses_b")))
    expect(queries.length).toBe(1)
    expect(queries[0].method).toBe("all")
    expect(queries[0].params).toEqual([sid("ses_b")])
    expect(result.value).toMatchObject({ text: "worker text" })
  })
})

// ---------------------------------------------------------------------------
// Worker EventV2 bridge wiring (spec Q6/Q13/Q15).
// ---------------------------------------------------------------------------

describe("worker event bridge wiring (Q6/Q13/Q15)", () => {
  const SessionThing = EventV2.define({
    type: "test.worker.session",
    schema: { sessionID: Schema.String, text: Schema.String },
  })

  test("the runnable graph is part of the worker and includes the bridge node", () => {
    const names = collectNodeNames(RUNNABLE_SESSION_NODES)
    expect(names.has("@opencode/session-worker/EventBridge")).toBe(true)
    expect(names.has("@opencode/EventV2Bridge")).toBe(true)
    expect(names.has("@opencode/Event")).toBe(true)
  })

  test("the production runnable graph carries the in-worker SessionProjector", () => {
    // Durable `message.updated` / `message.part.updated` events only become
    // `session`/`message`/`part` rows when the projector runs. Production must
    // include it; a test-added projector is no longer required.
    expect(collectNodeNames(RUNNABLE_SESSION_NODES).has("session-projector")).toBe(true)
  })

  test("the bridge forwards a worker-published session event to the egress sink verbatim", async () => {
    const forwarded: EventV2Event[] = []
    const built = buildSessionWorkerLayer({
      snapshot: workerSnapshot(),
      env: { values: {} },
      clients: {
        writer: { query: async () => [], publish: async () => ({ seq: 1, committed: true }) },
        // The bridge's sink; the egress transport frames the verbatim event.
        egress: { emit: (event) => forwarded.push(event) },
        broker: { call: async () => null },
        backgroundJob: { wait: async () => null, cancel: async () => {} },
      },
      modelClient: modelLayer("worker text"),
    })

    await Effect.runPromise(
      Effect.gen(function* () {
        const events = yield* EventV2Bridge.Service
        yield* events.publish(SessionThing, { sessionID: "ses_1", text: "hi" })
      }).pipe(Effect.provide(built.layer)),
    )

    expect(forwarded).toHaveLength(1)
    expect(forwarded[0]).toMatchObject({
      type: "test.worker.session",
      properties: { sessionID: "ses_1", text: "hi" },
    })
    expect(forwarded[0].id).toStartWith("evt_")
  })

  test("bootstrap transfers the egress port the bridge sink rides on", () => {
    const frames: Array<{ data: unknown; transfer: readonly Transferable[] }> = []
    const scope = {
      postMessage(data: unknown, transfer?: readonly Transferable[]) {
        frames.push({ data, transfer: transfer ?? [] })
      },
      onmessage: null as ((event: { data: unknown }) => void) | null,
    }
    bootstrapSessionWorker(
      {
        snapshot: workerSnapshot(),
        env: { values: {} },
        model: { baseUrl: "http://127.0.0.1:1/v1", apiKey: "k", model: "m" },
        capabilities: { sessionWorkerPorts: true },
      },
      scope,
    )
    const ports = frames.find((frame) => (frame.data as { type?: string }).type === "ports")
    expect(ports).toBeDefined()
    expect((ports!.data as { egress?: unknown }).egress).toBeInstanceOf(MessagePort)
  })
})

describe("writerReadMessages", () => {
  test("extracts role + text parts and tolerates a read failure", async () => {
    const reader = writerReadMessages({
      query: async () => [
        { data: JSON.stringify({ role: "user", parts: [{ text: "a" }, { text: "b" }] }) },
        { data: { role: "assistant", content: "c" } },
        { data: "not-json" },
      ],
      publish: async () => ({ seq: 0, committed: false }),
    })
    expect(await Effect.runPromise(reader("s1"))).toEqual([
      { role: "user", content: "ab" },
      { role: "assistant", content: "c" },
    ])

    const failing = writerReadMessages({
      query: async () => {
        throw new Error("writer down")
      },
      publish: async () => ({ seq: 0, committed: false }),
    })
    expect(await Effect.runPromise(failing("s1"))).toEqual([])
  })
})

// ---------------------------------------------------------------------------
// Flag-on status parity: the worker-local run-state reaches idle via the
// hub-proxy `SessionStatus`, the worker half of the flag-off cancel/completion
// transition. (The hub half — cancel after `hub.cancel` — lives in
// `hub-wiring.test.ts`.)
// ---------------------------------------------------------------------------

describe("worker run-state idle parity (flag-on)", () => {
  /**
   * The worker's `InstanceState`-backed services (e.g. `SessionRunState`) need an
   * ambient `InstanceRef`; a worker owns no project loader, so the snapshot
   * directory is both the working directory and the worktree.
   */
  const instance = {
    directory: workerSnapshot().directory,
    worktree: workerSnapshot().directory,
    project: undefined as never,
  }

  /** Build the worker graph with a `SessionStatus` recorded on the hub side. */
  const workerWithStatusStore = () => {
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
      modelClient: modelLayer("worker text"),
      hubProxies: { sessionStatus: hubProxyClient, permission: hubProxyClient, question: hubProxyClient },
    })
    return { built, store }
  }

  test("normal completion publishes session.status idle through the hub-proxy", async () => {
    const { built, store } = workerWithStatusStore()

    await Effect.runPromise(
      Effect.gen(function* () {
        const runState = yield* SessionRunState.Service
        yield* runState.ensureRunning(
          sid("ses_done"),
          Effect.succeed(withParts(sid("ses_done"))),
          Effect.succeed(withParts(sid("ses_done"))),
        )
      }).pipe(Effect.provide(built.layer), Effect.provideService(InstanceRef, instance)),
    )

    // The worker-local runner's `onIdle` crossed the hub-proxy port, so the hub
    // observed the terminal idle — the same transition flag-off reaches through
    // its in-process runner's `onIdle`.
    expect(store.get("ses_done")).toEqual({ type: "idle" })
  })


  test("cancelling the worker-local runner also publishes idle through the hub-proxy", async () => {
    const { built, store } = workerWithStatusStore()

    await Effect.runPromise(
      Effect.gen(function* () {
        const runState = yield* SessionRunState.Service
        const started = yield* Latch.make()
        // A run that never settles keeps the runner busy until cancel interrupts it.
        const work = Effect.andThen(started.open, Effect.never)
        yield* runState
          .ensureRunning(sid("ses_run"), Effect.succeed(withParts(sid("ses_run"))), work)
          .pipe(Effect.forkChild)
        // Wait for the runner to be registered before cancelling it.
        yield* started.await
        yield* runState.cancel(sid("ses_run"))
      }).pipe(Effect.provide(built.layer), Effect.provideService(InstanceRef, instance)),
    )

    expect(store.get("ses_run")).toEqual({ type: "idle" })
  })
})

// ---------------------------------------------------------------------------
// End-to-end: the REAL `SessionPrompt.loop` inside the worker over the real
// writer-backed `Database`. This is the acceptance test for the worker DB fix:
// before it, the worker's raw-SQL `Database` had no Drizzle methods, so the
// loop died on `db.select`/`db.transaction` and wrote nothing.
//
// The production default graph (`RUNNABLE_SESSION_NODES`) now carries
// `SessionProjector`: `Session.updateMessage` / `updatePart` publish durable
// events, and the projector materializes the `session`/`message`/`part` rows the
// loop reads back — inside the same writer-pinned transaction as the event
// insert. The harness `createE2EContext` also names the projector explicitly, but
// that is the SAME node object, so it runs exactly once (verified).
// ---------------------------------------------------------------------------

const mkModelConfig = (provider: { baseUrl: string; apiKey: string; model: string }) => ({
  providerID: "mock",
  modelID: provider.model,
  apiId: provider.model,
  npm: "@ai-sdk/openai-compatible",
  baseUrl: provider.baseUrl,
  apiKey: provider.apiKey,
  headers: {},
})

/** Seed a session + one user message through the worker's own services. */
function seedSession(ctx: E2EContext, modelID: string, prompt: string) {
  return Effect.gen(function* () {
    const sessions = yield* Session.Service
    const session = yield* sessions.create({
      title: "e2e",
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
}

describe("real SessionPrompt.loop over the writer-backed worker Database (E2E)", () => {
  test("runs the loop against the mock provider and persists each durable row exactly once", async () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "oc-e2e-"))
    await using provider = startMockProvider({ chunks: ["hel", "lo"] })
    const ctx = createE2EContext({
      snapshot: workerSnapshot({ directory }),
      modelConfig: mkModelConfig(provider),
    })
    try {
      const sessionID = await Effect.runPromise(seedSession(ctx, provider.model, "hi"))
      const work = createSessionWork({
        layer: ctx.layer,
        writer: ctx.writer,
        threadId: 7,
        prompt: true,
        instance: ctx.instance,
      })
      const result = await Effect.runPromise(work(sessionID))
      const value = result.value as { parts: ReadonlyArray<{ type: string; text?: string }> }
      const text = value.parts
        .filter((part) => part.type === "text")
        .map((part) => part.text ?? "")
        .join("")

      // 1. Non-empty assistant text from the real streamed reply.
      expect(text).toBe("hello")
      expect(provider.requests.length).toBeGreaterThanOrEqual(1)

      // 2. Durable rows written through the writer, exactly once. `data` is
      // stored as JSON text by Drizzle, so parse it before inspecting.
      const json = (value: unknown): Record<string, unknown> =>
        typeof value === "string" ? (JSON.parse(value) as Record<string, unknown>) : (value as Record<string, unknown>)
      const messages = ctx.rows("message")
      expect(messages).toHaveLength(2)
      const roles = messages.map((row) => json(row.data).role)
      expect(roles.filter((role) => role === "assistant")).toHaveLength(1)
      expect(roles.filter((role) => role === "user")).toHaveLength(1)
      const texts = ctx
        .rows("part")
        .map((row) => json(row.data))
        .filter((data) => data.type === "text")
        .map((data) => data.text)
      expect(texts.filter((value) => value === "hello")).toHaveLength(1)
      expect(ctx.rows("event").length).toBeGreaterThan(0)

      // 3. session.status busy -> idle (the worker half of completion parity).
      const statuses = ctx.statusLog.filter((entry) => entry.sessionID === sessionID).map((entry) => entry.status)
      expect(statuses[0]).toBe("busy")
      expect(statuses[statuses.length - 1]).toBe("idle")

      // 4. Every durable write serialized through the single writer.
      expect(ctx.writerOps.length).toBeGreaterThan(0)
      expect(ctx.writerOps.some((op) => op.startsWith("tx:") || op === "begin" || op === "commit")).toBe(true)
      const eventIDs = ctx.rows("event").map((row) => row.id)
      expect(new Set(eventIDs).size).toBe(eventIDs.length)
    } finally {
      await ctx.dispose()
      fs.rmSync(directory, { recursive: true, force: true })
    }
  }, 60_000)
})

// ---------------------------------------------------------------------------
// End-to-end: a real worker `SessionPrompt.loop` run that ERRORS. The processor
// halts on the provider failure and publishes a NON-durable `session.error`,
// which the worker's EventV2→egress bridge forwards verbatim across the isolate
// to the hub-side sink. The audit (`.parallel-implement/o1-w4-audit.md`, phase 5)
// flagged that `session.error` was only unit-covered at the hub sink, never over
// a real worker error run.
//
// The verbatim shape is the flag-OFF EventV2 wire shape `{ id, type, properties }`
// (`properties` is the event `data`), NOT the writer-style `{ id, type, data }`.
// ---------------------------------------------------------------------------

describe("real SessionPrompt.loop error over the worker (E2E)", () => {
  test("a provider failure crosses the isolate as one verbatim session.error and persists one error row", async () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "oc-e2e-err-"))
    // A non-retryable status (400) makes the processor halt on the first failure
    // instead of backing off through the 5xx retry ladder (~60s).
    await using provider = startMockProvider({
      status: 400,
      errorBody: { error: { message: "bad request from mock provider" } },
    })

    // Observe the hub-side events-port sink: the worker's egress handle IS that
    // sink, so replacing the egress client node captures exactly what crosses.
    const crossed: EventV2Event[] = []
    const ctx = createE2EContext({
      snapshot: workerSnapshot({ directory }),
      modelConfig: mkModelConfig(provider),
      replacements: [
        [
          EventEgressClientNode,
          Layer.succeed(EventEgressClientService, { emit: (event) => void crossed.push(event) }),
        ],
      ],
    })
    try {
      const sessionID = await Effect.runPromise(seedSession(ctx, provider.model, "hi"))
      const work = createSessionWork({
        layer: ctx.layer,
        writer: ctx.writer,
        threadId: 7,
        prompt: true,
        instance: ctx.instance,
      })
      await Effect.runPromise(work(sessionID))

      // 1. The error crossed the isolate exactly once, verbatim.
      const errors = crossed.filter((event) => event.type === "session.error")
      expect(errors).toHaveLength(1)
      const error = errors[0]
      expect(Object.keys(error).sort()).toEqual(["id", "properties", "type"])
      expect(error.id).toStartWith("evt_")
      const properties = error.properties as { sessionID?: string; error?: { name?: string; message?: string } }
      expect(properties.sessionID).toBe(sessionID)
      expect(properties.error).toBeDefined()

      // The hub sink accepts that same verbatim event and republishes it
      // live-only — closing the "real run -> hub sink" gap the audit flagged.
      const republished: Array<{ id?: string; type: string; data: unknown }> = []
      createHubEventSink({
        publisher: {
          publish: (event) => void republished.push(event),
          publishDefinition: (definition, data) => void republished.push({ type: definition.type, data }),
        },
      }).event(error)
      expect(republished).toEqual([{ id: error.id, type: "session.error", data: properties }])

      // 2. The durable error row is persisted exactly once: one assistant message
      //    carrying the error, and no duplicate of it.
      const json = (value: unknown): Record<string, unknown> =>
        typeof value === "string" ? (JSON.parse(value) as Record<string, unknown>) : (value as Record<string, unknown>)
      const assistants = ctx
        .rows("message")
        .map((row) => json(row.data))
        .filter((data) => data.role === "assistant")
      expect(assistants).toHaveLength(1)
      expect(assistants[0].error).toBeDefined()
    } finally {
      await ctx.dispose()
      fs.rmSync(directory, { recursive: true, force: true })
    }
  }, 60_000)
})
