// Wave 2 — session-worker runtime tests (TDD plan steps 22, 27, 28).
//
// The production entry is import-safe outside a worker (it only bootstraps when
// `workerData` is present), so the pure aggregation / registry / work builders
// are exercised in-process here.

import { describe, expect, test } from "bun:test"
import { Effect, Layer, Schema } from "effect"
import { EventV2 } from "@opencode-ai/core/event"
import { EventV2Bridge } from "@/event-v2-bridge"
import { SessionID } from "@/session/schema"
import { serveWriterPort, type HubWriter } from "@/session/writer/client"
import type { EventV2Event } from "@/session/worker/frame"
import { collectNodeNames } from "@/session/worker/node-table"
import {
  ModelClient,
  PIPELINE_SESSION_NODES,
  RUNNABLE_SESSION_NODES,
  buildSessionWorkerLayer,
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
import { workerSnapshot } from "../../lib/snapshot"

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
