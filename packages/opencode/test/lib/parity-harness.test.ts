// Wave 1/3 — `parity-harness` tests (spec Q1/Q12/Q15).
//
// The harness runs ONE identical session script through an injected runner twice:
// `flag:false` (in-process / flag-OFF) and `flag:true` (worker / flag-ON),
// capturing each side's event sequence with `event-sink`.
//
// Wave 3 wires the assertion to REAL production paths:
//
//   * flag-OFF runs the full `RUNNABLE_SESSION_NODES` worker graph in-process and
//     captures its `EventV2` directly — the non-worker baseline;
//   * flag-ON runs the SAME graph, but the real `WorkerEventBridge` forwards each
//     session-scoped event over a REAL `MessagePort` (`createWorkerEgressClient`)
//     into `serveEventsPort` → `createHubEventSink` → `createLiveEventPublisher`,
//     which republishes live-only onto a separate hub `EventV2` the sink observes.
//
// `session.status` stays hub-proxied (Q15): the worker never emits it; the hub
// authority does, exactly once. The duplication probe proves the bridge's
// `HUB_PROXIED_EVENT_TYPES` drop, not just the absence of a local emit.

import { describe, expect, it } from "bun:test"
import { Effect, Layer, ManagedRuntime, Schema } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { SessionStatusEvent } from "@opencode-ai/schema/session-status-event"
import { EventV2Bridge, createLiveEventPublisher, type LiveEventPublisher } from "@/event-v2-bridge"
import { InstanceRef } from "@/effect/instance-ref"
import { Provider } from "@/provider/provider"
import { SessionStatus } from "@/session/status"
import { SessionID } from "@/session/schema"
import {
  PIPELINE_SESSION_NODES,
  buildSessionWorkerLayer,
  createHubProxyClient,
  llmModelClientLayer,
  reducedLlmLayer,
  reducedModelProviderLayer,
  serveHubProxy,
  type HubProxyClients,
  type WorkerModelConfig,
} from "@/session/worker/layer"
import { createWorkerEgressClient } from "@/session/worker/egress-client"
import {
  buildHubProxyHandlers,
  createHubEventSink,
  serveEventsPort,
  type HubProxyAuthorities,
  type HubProxyRunner,
} from "@/session/worker/hub"
import { createSessionWork } from "@/session/worker/session-worker"
import {
  compareSequences,
  compareToGolden,
  goldenFromParity,
  readGolden,
  recordGolden,
  runParitySession,
  type ParityResult,
  type ParityRunContext,
  type ParityRunner,
  type SessionScript,
} from "./parity-harness"
import { createEventSink, type EventSink } from "./event-sink"
import { startMockProvider, type MockProviderHandle } from "./mock-provider"
import { pollUntil } from "./poll"
import { workerSnapshot } from "./snapshot"
import { tmpdir } from "../fixture/fixture"

const LiveDelta = EventV2.define({
  type: "message.part.delta",
  schema: { delta: Schema.String },
})

const databaseStub = Layer.succeed(Database.Service, { db: {} as never })
const eventLayer = EventV2.layerWith().pipe(Layer.provide(databaseStub))

const script: SessionScript = { name: "deterministic-stream", prompt: "hello" }

/** Publish a deterministic delta stream through a sink bound to a fresh runtime. */
async function runDeltas(context: ParityRunContext, deltas: ReadonlyArray<string>): Promise<void> {
  const runtime = ManagedRuntime.make(eventLayer)
  try {
    const sink = context.capture(runtime)
    for (const delta of deltas) {
      await runtime.runPromise(EventV2.Service.use((events) => events.publish(LiveDelta, { delta })))
    }
    expect(sink.events()).toHaveLength(deltas.length)
  } finally {
    await runtime.dispose()
  }
}

describe("parity-harness", () => {
  it("runs both flag states without throwing and captures both sequences", async () => {
    const result = await runParitySession({
      script,
      // The flag-ON side is intentionally shorter: the known-red gap this harness
      // exists to expose once worker events cross the isolate.
      run: (context) => runDeltas(context, context.flag ? ["a"] : ["a", "b"]),
    })

    expect(result.script).toBe("deterministic-stream")
    expect(result.off.flag).toBe(false)
    expect(result.on.flag).toBe(true)
    expect(result.off.error).toBeUndefined()
    expect(result.on.error).toBeUndefined()
    expect(result.off.types).toEqual(["message.part.delta", "message.part.delta"])
    expect(result.on.types).toEqual(["message.part.delta"])
  })

  it("captures a runner failure as side.error instead of throwing", async () => {
    const result = await runParitySession({
      script,
      run: async (context) => {
        if (context.flag) throw new Error("worker events do not cross the isolate yet")
        await runDeltas(context, ["a"])
      },
    })

    expect(result.off.error).toBeUndefined()
    expect(result.off.types).toEqual(["message.part.delta"])
    expect(result.on.error).toBe("worker events do not cross the isolate yet")
    expect(result.on.types).toEqual([])
  })

  it("captures a hung flag-ON run as a timeout error without throwing", async () => {
    const result = await runParitySession({
      script,
      timeoutMs: 50,
      run: async (context) => {
        if (context.flag) await new Promise(() => {})
        await runDeltas(context, ["a"])
      },
    })

    expect(result.off.types).toEqual(["message.part.delta"])
    expect(result.on.error).toContain("timed out")
  })

  it("compareSequences reports equal for identical sequences (ids ignored)", () => {
    const off = { events: [event("evt_1", "message.part.delta", { delta: "a" })] }
    const on = { events: [event("evt_2", "message.part.delta", { delta: "a" })] }

    const comparison = compareSequences(off, on)
    expect(comparison.equal).toBe(true)
    expect(comparison.missingInOn).toEqual([])
    expect(comparison.missingInOff).toEqual([])
    expect(comparison.mismatches).toEqual([])
  })

  it("compareSequences detects a synthetic difference", () => {
    const off = {
      events: [
        event("evt_1", "message.part.delta", { delta: "a" }),
        event("evt_2", "message.part.updated", { text: "hi" }),
      ],
    }
    const on = { events: [event("evt_3", "message.part.delta", { delta: "a" })] }

    const comparison = compareSequences(off, on)
    expect(comparison.equal).toBe(false)
    expect(comparison.offCount).toBe(2)
    expect(comparison.onCount).toBe(1)
    expect(comparison.missingInOn.map((e) => e.type)).toEqual(["message.part.updated"])
    expect(comparison.missingInOff).toEqual([])
    expect(comparison.mismatches).toHaveLength(1)
    expect(comparison.mismatches[0]?.index).toBe(1)
    expect(comparison.mismatches[0]?.off?.type).toBe("message.part.updated")
    expect(comparison.mismatches[0]?.on).toBeUndefined()
  })

  it("records and reads back the flag-OFF reference as a golden fixture", async () => {
    await using tmp = await tmpdir()
    const result: ParityResult = await runParitySession({
      script,
      run: (context) => runDeltas(context, context.flag ? ["a"] : ["a", "b"]),
    })

    const golden = goldenFromParity(result, "reference")
    expect(golden.name).toBe("reference")
    expect(golden.events).toEqual(result.off.events)

    const path = `${tmp.path}/reference.json`
    await recordGolden(path, result, "reference")
    const read = await readGolden(path)
    expect(read).toEqual(golden)

    const match = compareToGolden(result.off, read)
    expect(match.equal).toBe(true)
    const mismatch = compareToGolden(result.on, read)
    expect(mismatch.equal).toBe(false)
  })
})

// ---------------------------------------------------------------------------
// Real flag-OFF / flag-ON runners (Q1/Q12/Q15).
// ---------------------------------------------------------------------------

/** The live-output deltas the scripted session streams; the Q1/Q12 "core". */
const DELTAS: ReadonlyArray<string> = ["hel", "lo ", "stream"]

/** The mock provider the runners were started with, wired as the worker's model. */
const modelConfig = (provider: MockProviderHandle): WorkerModelConfig => ({
  providerID: "mock",
  modelID: provider.model,
  apiId: provider.model,
  npm: "@ai-sdk/openai-compatible",
  baseUrl: provider.baseUrl,
  apiKey: provider.apiKey,
  headers: {},
})

/** The stub writer/egress clients a runner starts from; egress is overridden per side. */
function stubClients() {
  return {
    writer: { query: async () => [], publish: async () => ({ seq: 1, committed: true }) },
    broker: { call: async () => null },
    backgroundJob: { wait: async () => null, cancel: async () => {} },
  }
}

/** The hub half of the flag-ON path: a live-only publisher + the `session.status` authority. */
interface HubBridge {
  readonly runtime: ManagedRuntime.ManagedRuntime<
    EventV2.Service | EventV2Bridge.Service | SessionStatus.Service,
    never
  >
  readonly egressPort: MessagePort
  readonly hubProxyPort: MessagePort
  readonly sink: EventSink
  dispose(): Promise<void>
}

/**
 * Build the hub half: a dedicated `EventV2` (+ real `EventV2Bridge` so republishes
 * reach `GlobalBus`) with the real `SessionStatus` authority, plus the events port
 * (republishing live-only) and the hub-proxy control port the worker calls.
 */
async function createHubBridge(context: ParityRunContext): Promise<HubBridge> {
  const layer = AppNodeBuilder.build(LayerNode.group([EventV2.node, EventV2Bridge.node, SessionStatus.node]), [
    [Database.node, databaseStub],
  ])
  const runtime = ManagedRuntime.make(layer)
  // Pre-warm so the `EventV2` subscription is synchronous (no missed events).
  await runtime.runPromise(Effect.void)

  const events = runtime.runSync(EventV2.Service)
  const publisher: LiveEventPublisher = createLiveEventPublisher(events)
  const egressChannel = new MessageChannel()
  const disposeEvents = serveEventsPort(egressChannel.port2, createHubEventSink({ publisher }))

  // The hub-proxy authorities run under the dispatched instance's context, exactly
  // as `productionSessionHubLayer` provides it (`hub.ts` `HubProxyRunner`).
  const sessionStatus = runtime.runSync(SessionStatus.Service)
  const run: HubProxyRunner = (effect) =>
    runtime.runPromise(effect.pipe(Effect.provideService(InstanceRef, context.instance)))
  const authorities: HubProxyAuthorities = {
    sessionStatus,
    // A worker turn here never asks a permission/question; the handler map is only
    // built so the control port resolves.
    permission: {} as never,
    question: {} as never,
  }
  const hubProxyChannel = new MessageChannel()
  const disposeProxy = serveHubProxy(buildHubProxyHandlers(authorities, run), hubProxyChannel.port2)

  return {
    runtime,
    egressPort: egressChannel.port1,
    hubProxyPort: hubProxyChannel.port1,
    // The sink only needs the `EventV2.Service` surface; the runtime carries more.
    sink: context.capture(runtime as unknown as ManagedRuntime.ManagedRuntime<EventV2.Service, never>),
    async dispose() {
      disposeEvents()
      disposeProxy()
      egressChannel.port1.close()
      egressChannel.port2.close()
      hubProxyChannel.port1.close()
      hubProxyChannel.port2.close()
      await runtime.dispose()
    },
  }
}

export interface BridgeSideOptions {
  /**
   * Publish an extra `session.status` directly from the worker's `EventV2`. The
   * bridge must DROP it (Q15 hub-proxied) so the hub still sees exactly one status.
   */
  readonly directStatus?: boolean
}

/**
 * One flag state of the session. Both sides emit the same live deltas and set
 * `session.status` busy exactly once; flag-ON routes them through the real bridge.
 */
async function runBridgeSide(context: ParityRunContext, options: BridgeSideOptions = {}): Promise<void> {
  const config = modelConfig(context.provider)
  const snapshot = workerSnapshot()
  const bridge = context.flag ? await createHubBridge(context) : undefined
  const workerSink: EventSink | undefined = bridge === undefined ? undefined : bridge.sink

  const egress =
    bridge === undefined
      ? { emit: () => {}, emitDurable: () => {}, end: () => {}, dispose: () => {} }
      : createWorkerEgressClient(bridge.egressPort, { windowMs: 0 })
  const hubProxyClient = bridge === undefined ? undefined : createHubProxyClient(bridge.hubProxyPort)
  const hubProxies: HubProxyClients | undefined =
    hubProxyClient === undefined
      ? undefined
      : {
          // One multiplexed client backs all three authorities (see `session-worker.ts`):
          // separate clients on one port would steal each other's replies.
          sessionStatus: hubProxyClient,
          permission: hubProxyClient,
          question: hubProxyClient,
        }

  const built = buildSessionWorkerLayer({
    snapshot,
    env: { values: {} },
    clients: { ...stubClients(), egress },
    // A real LLM-backed model client over the mock provider (the graph's `Provider`
    // is replaced by the reduced bootstrap below).
    modelClient: llmModelClientLayer(config).pipe(Layer.provide(reducedLlmLayer(snapshot, config, hubProxies))),
    replacements: [[Provider.node, reducedModelProviderLayer(config)]],
    ...(hubProxies === undefined ? {} : { hubProxies }),
  })

  const runtime = ManagedRuntime.make(built.layer)
  try {
    await runtime.runPromise(Effect.void)
    // The flag-OFF baseline observes the graph's own `EventV2`; flag-ON observes the
    // hub (its sink is created in `createHubBridge`, before any event flows). The
    // worker layer is typed `unknown`; the sink only needs its `EventV2.Service`.
    const workerRuntime = runtime as unknown as ManagedRuntime.ManagedRuntime<EventV2.Service, never>
    const sink = workerSink ?? context.capture(workerRuntime)

    const expectedHubEvents = DELTAS.length + 1
    await runtime.runPromise(
      Effect.gen(function* () {
        const events = yield* EventV2Bridge.Service
        const status = yield* SessionStatus.Service
        const sessionID = context.sessionID
        const messageID = context.messageID
        const partID = context.partID
        if (options.directStatus === true) {
          yield* events.publish(SessionStatusEvent.Status, { sessionID, status: { type: "busy" } })
        }
        for (const delta of DELTAS) {
          yield* events.publish(SessionV1.Event.PartDelta, { sessionID, messageID, partID, field: "text", delta })
        }
        // Let the egress port flush the deltas before the hub-proxied status, so
        // both sides observe the same order (the port is a different channel).
        yield* Effect.sleep("25 millis")
        yield* status.set(sessionID, { type: "busy" })
      }).pipe(Effect.provideService(InstanceRef, context.instance)),
    )

    if (bridge !== undefined) {
      egress.end()
      await pollUntil(() => sink.types().length >= expectedHubEvents, {
        message: `flag-ON hub never received the republished sequence (saw ${sink.types().length})`,
      })
    }
  } finally {
    egress.dispose()
    await runtime.dispose()
    if (bridge !== undefined) await bridge.dispose()
  }
}

/** The runner `runParitySession` injects: the real flag-OFF / flag-ON paths. */
const runRealParity: ParityRunner = (context) => runBridgeSide(context)

// ---------------------------------------------------------------------------
// Q1/Q12 — flag-ON ≡ flag-OFF (id-agnostic).
// ---------------------------------------------------------------------------

describe("real flag-ON ≡ flag-OFF parity", () => {
  it("the flag-ON event sequence equals flag-OFF (live output visible in both)", async () => {
    const result = await runParitySession({ script, run: runRealParity, timeoutMs: 30_000 })

    expect(result.off.error).toBeUndefined()
    expect(result.on.error).toBeUndefined()
    // The live output crossed the isolate: 3 deltas + one hub-published status.
    expect(result.off.types).toEqual([
      "message.part.delta",
      "message.part.delta",
      "message.part.delta",
      "session.status",
    ])
    expect(result.on.types).toEqual(result.off.types)

    const comparison = compareSequences(result.off, result.on)
    expect(comparison.mismatches).toEqual([])
    expect(comparison.missingInOn).toEqual([])
    expect(comparison.missingInOff).toEqual([])
    expect(comparison.equal).toBe(true)
  }, 30_000)
})

// ---------------------------------------------------------------------------
// Q15 — `session.status` is hub-proxied and republished exactly once.
// ---------------------------------------------------------------------------

describe("real flag-ON session.status (Q15)", () => {
  it("publishes session.status exactly once even when the worker also emits one", async () => {
    const result = await runParitySession({
      script,
      run: (context) => runBridgeSide(context, { directStatus: true }),
      timeoutMs: 30_000,
    })

    expect(result.on.error).toBeUndefined()
    const statuses = result.on.types.filter((type) => type === "session.status")
    // One from the hub authority; the worker's own (hub-proxied) event was dropped
    // by the bridge — never duplicated onto the hub.
    expect(statuses).toHaveLength(1)
    // The live deltas still crossed.
    expect(result.on.types.filter((type) => type === "message.part.delta")).toHaveLength(DELTAS.length)
  }, 30_000)
})

// ---------------------------------------------------------------------------
// Sanity: the real worker graph runs a session against the mock provider.
// ---------------------------------------------------------------------------

describe("real worker session against the mock provider", () => {
  it("runs the reduced session pipeline in the real worker graph", async () => {
    await using provider = startMockProvider({ chunks: ["hel", "lo ", "provider"] })
    const config = modelConfig(provider)
    const snapshot = workerSnapshot()
    const emitted: string[] = []

    const built = buildSessionWorkerLayer({
      nodes: PIPELINE_SESSION_NODES,
      snapshot,
      env: { values: {} },
      clients: {
        ...stubClients(),
        egress: { emit: (event) => void emitted.push(event.type) },
      },
      modelClient: llmModelClientLayer(config).pipe(Layer.provide(reducedLlmLayer(snapshot, config))),
    })

    const work = createSessionWork({
      layer: built.layer,
      writer: stubClients().writer,
      threadId: 7,
      prompt: false,
      readMessages: () => Effect.succeed([{ role: "user", content: "say hello" }]),
    })

    const result = await Effect.runPromise(work(SessionID.descending()))
    const value = result.value as { text: string; threadId: number }
    expect(value.text).toBe("hello provider")
    expect(value.threadId).toBe(7)
    expect(emitted).toEqual(["session.text"])
    expect(provider.requests).toHaveLength(1)
  }, 30_000)
})

function event(id: string, type: string, properties: unknown) {
  return { id, type, properties }
}
