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

import { describe, expect, it, spyOn } from "bun:test"
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
  type HubEventSink,
  type HubProxyAuthorities,
  type HubProxyRunner,
} from "@/session/worker/hub"
import { HUB_PROXIED_EVENT_TYPES } from "@/session/worker/event-bridge"
import { recordingHubAuthorities } from "./hub-proxy"
import { createSessionWork } from "@/session/worker/session-worker"
import { Session } from "@/session/session"
import { SessionPrompt } from "@/session/prompt"
import { SessionSummary } from "@/session/summary"
import { createE2EContext, type E2EContextOptions } from "./parity-harness"
import {
  compareDurable,
  compareSequences,
  compareToGolden,
  goldenFromParity,
  readGolden,
  recordGolden,
  runParitySession,
  type ParityDurableRow,
  type ParityResult,
  type ParityRunContext,
  type ParityRunner,
  type SessionScript,
} from "./parity-session"
import { createBunSqliteConnection } from "@/session/writer/connection"
import { createWriterCore } from "@/session/writer/core"
import { serveWriterPort, type HubWriter } from "@/session/writer/client"
import { createWorkerWriterClient } from "@/session/worker/writer-client"
import { createEventSink, type EventSink, type SinkEvent } from "./event-sink"
import { startMockProvider, type MockProviderHandle } from "./mock-provider"
import { withDeadline } from "./async"
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
  /** The hub-side events-port sink; `ended` signals the worker flushed its stream. */
  readonly hubSink: HubEventSink
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
  const hubSink = createHubEventSink({ publisher })
  const disposeEvents = serveEventsPort(egressChannel.port2, hubSink)

  // The hub-proxy authorities run under the dispatched instance's context, exactly
  // as `productionSessionHubLayer` provides it (`hub.ts` `HubProxyRunner`).
  const sessionStatus = runtime.runSync(SessionStatus.Service)
  const run: HubProxyRunner = (effect) =>
    runtime.runPromise(effect.pipe(Effect.provideService(InstanceRef, context.instance)))
  // A worker turn here never asks a permission/question, but the handler map must
  // be complete so the control port resolves; the shared recorder supplies inert
  // (and observable) stubs instead of empty objects.
  const authorities: HubProxyAuthorities = recordingHubAuthorities({ sessionStatus }).authorities
  const hubProxyChannel = new MessageChannel()
  const disposeProxy = serveHubProxy(buildHubProxyHandlers(authorities, run), hubProxyChannel.port2)

  return {
    runtime,
    egressPort: egressChannel.port1,
    hubProxyPort: hubProxyChannel.port1,
    // The sink only needs the `EventV2.Service` surface; the runtime carries more.
    sink: context.capture(runtime as unknown as ManagedRuntime.ManagedRuntime<EventV2.Service, never>),
    hubSink,
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
    const { sessionID, messageID, partID } = context

    // Publish the deltas first, on their own run, then wait on the sink (not a
    // fixed sleep) until they have been observed before setting status. The
    // events port is an independent channel, so only the observed readiness
    // guarantees both sides order `session.status` last.
    await runtime.runPromise(
      Effect.gen(function* () {
        const events = yield* EventV2Bridge.Service
        if (options.directStatus === true) {
          yield* events.publish(SessionStatusEvent.Status, { sessionID, status: { type: "busy" } })
        }
        for (const delta of DELTAS) {
          yield* events.publish(SessionV1.Event.PartDelta, { sessionID, messageID, partID, field: "text", delta })
        }
      }).pipe(Effect.provideService(InstanceRef, context.instance)),
    )

    await pollUntil(() => sink.types().length >= DELTAS.length, {
      message: `parity deltas never reached the sink (saw ${sink.types().length})`,
    })

    await runtime.runPromise(
      Effect.gen(function* () {
        const status = yield* SessionStatus.Service
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
    // One stub-client set shared by the layer graph and the work scope, so the
    // worker's writer is the exact one the test owns.
    const clients = stubClients()

    const built = buildSessionWorkerLayer({
      nodes: PIPELINE_SESSION_NODES,
      snapshot,
      env: { values: {} },
      clients: {
        ...clients,
        egress: { emit: (event) => void emitted.push(event.type) },
      },
      modelClient: llmModelClientLayer(config).pipe(Layer.provide(reducedLlmLayer(snapshot, config))),
    })

    const work = createSessionWork({
      layer: built.layer,
      writer: clients.writer,
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

// ---------------------------------------------------------------------------
// Real `SessionPrompt.loop` flag-ON ≡ flag-OFF (o1-w4 audit gap 1).
//
// The synthetic `runBridgeSide` above publishes deltas directly; this section
// runs the ACTUAL `SessionPrompt.loop` over the writer-backed Database with the
// in-graph `SessionProjector`, once in-process (flag-OFF, captured from the
// graph's own `EventV2`) and once with the worker event bridge forwarding every
// session-scoped event to a hub (`event-sink` observes the hub surface).
//
// Why the loop runs directly here (not via `createSessionWork`): the harness must
// observe the SAME `EventV2` instance the graph publishes on, and
// `createSessionWork` builds its own layer scope internally. Calling
// `SessionPrompt.loop` on a runtime built from `createE2EContext().layer` is the
// identical loop the worker's work function invokes, over the identical graph.
// ---------------------------------------------------------------------------

/** The deterministic mock-provider reply both real-loop sides run against. */
const loopScript: SessionScript = {
  name: "real-loop",
  prompt: "hi",
  provider: { chunks: ["hel", "lo"] },
}

/**
 * The worker bridge's own projection: only events whose payload carries a
 * `sessionID` cross the isolate (`isSessionScopedEvent`). The flag-OFF graph also
 * publishes hub-local telemetry (`plugin.added`, `catalog.updated`, …) that never
 * crosses, so both sides are compared on this projection.
 */
function sessionScoped(events: ReadonlyArray<SinkEvent>): ReadonlyArray<SinkEvent> {
  return events.filter((entry) => {
    const properties = entry.properties
    if (properties === null || typeof properties !== "object") return false
    return typeof (properties as { readonly sessionID?: unknown }).sessionID === "string"
  })
}

/** Count event types into a multiset, for exactly-once comparison. */
function typeCounts(types: ReadonlyArray<string>): Record<string, number> {
  const counts: Record<string, number> = {}
  for (const type of types) counts[type] = (counts[type] ?? 0) + 1
  return counts
}

/**
 * Event types the hub owns (hub-proxied): the worker never forwards them over the
 * events port, the hub authority publishes them on the hub surface instead. The
 * base set comes from production's `HUB_PROXIED_EVENT_TYPES` (the exact set the
 * worker bridge drops); `SessionStatus.set(idle)` also publishes the deprecated
 * `session.idle` on the hub, so it rides the same hub-proxy channel.
 */
const HUB_OWNED_TYPES: ReadonlySet<string> = new Set([
  ...HUB_PROXIED_EVENT_TYPES,
  SessionStatusEvent.Idle.type,
])

/**
 * Split a captured sequence by the channel it crossed. The worker event bridge
 * (events port) and the hub-proxy control port are independent transports, so the
 * two subsets are each internally ordered but their interleaving is not
 * deterministic. Within a channel, order is preserved and must match exactly.
 */
function channels(events: ReadonlyArray<SinkEvent>): {
  readonly bridged: ReadonlyArray<SinkEvent>
  readonly hubOwned: ReadonlyArray<SinkEvent>
} {
  return {
    bridged: events.filter((entry) => !HUB_OWNED_TYPES.has(entry.type)),
    hubOwned: events.filter((entry) => HUB_OWNED_TYPES.has(entry.type)),
  }
}

/**
 * Run one flag state of the real loop:
 *
 *  * flag-OFF captures the worker graph's own `EventV2`;
 *  * flag-ON routes worker events over `createWorkerEgressClient` → `serveEventsPort`
 *    → `createHubEventSink` (live-only republish), and routes `session.status`
 *    through the hub authority, so the sink observes the full hub sequence.
 */
function loopRunner(directory: string): ParityRunner {
  return async (context) => {
    const config = modelConfig(context.provider)
    const bridge = context.flag ? await createHubBridge(context) : undefined
    const egress = bridge === undefined ? undefined : createWorkerEgressClient(bridge.egressPort, { windowMs: 0 })
    const hubProxyClient = bridge === undefined ? undefined : createHubProxyClient(bridge.hubProxyPort)
    const hubProxies: E2EContextOptions["hubProxies"] =
      hubProxyClient === undefined
        ? "in-process"
        : { sessionStatus: hubProxyClient, permission: hubProxyClient, question: hubProxyClient }

    const ctx = createE2EContext({
      snapshot: workerSnapshot({ directory }),
      modelConfig: config,
      ...(egress === undefined ? {} : { egress }),
      hubProxies,
      // `SessionPrompt` forks `SessionSummary.summarize` into the service scope
      // after the turn ends, so it can publish a `session.updated` AFTER the loop
      // resolves (and, on the worker, after `egress.end()`), which makes the
      // routed sequence nondeterministic. Neutralize that background work on BOTH
      // sides: the loop's own event route — what this parity test proves — is
      // otherwise unchanged.
      replacements: [[SessionSummary.node, Layer.mock(SessionSummary.Service, { summarize: () => Effect.void })]],
    })
    const runtime = ManagedRuntime.make(ctx.layer)

    try {
      // Warm the runtime so the sink subscribes to the graph's live `EventV2`.
      await runtime.runPromise(Effect.void)
      const sink =
        bridge === undefined
          ? context.capture(runtime as unknown as ManagedRuntime.ManagedRuntime<EventV2.Service, never>)
          : bridge.sink

      // Seed the scripted session + one user text part under the harness-shared
      // ids, so both sides run the SAME logical session.
      await runtime.runPromise(
        Effect.gen(function* () {
          const events = yield* EventV2Bridge.Service
          const sessions = yield* Session.Service
          const now = Date.now()
          const info = SessionV1.SessionInfo.make({
            id: context.sessionID,
            slug: "parity",
            version: "0.0.0",
            // `projectID` is branded; the graph's instance is the synthetic "e2e"
            // project (see `createE2EContext`).
            projectID: "e2e" as never,
            directory,
            title: "parity",
            agent: "build",
            permission: [{ permission: "*", pattern: "*", action: "allow" }] as never,
            cost: 0,
            tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
            time: { created: now, updated: now },
          })
          yield* events.publish(SessionV1.Event.Created, { sessionID: context.sessionID, info })
          yield* sessions.updateMessage({
            id: context.messageID,
            role: "user",
            sessionID: context.sessionID,
            agent: "build",
            model: { providerID: config.providerID, modelID: config.modelID },
            time: { created: now },
          } as never)
          yield* sessions.updatePart({
            id: context.partID,
            messageID: context.messageID,
            sessionID: context.sessionID,
            type: "text",
            text: context.script.prompt,
          } as never)
        }).pipe(Effect.provideService(InstanceRef, ctx.instance)),
      )

      // The real worker session loop, on the same runtime the sink observes.
      await runtime.runPromise(
        Effect.gen(function* () {
          const prompt = yield* SessionPrompt.Service
          yield* prompt.loop(new SessionPrompt.LoopInput({ sessionID: context.sessionID }))
        }).pipe(Effect.provideService(InstanceRef, ctx.instance)),
      )

      if (bridge !== undefined && egress !== undefined) {
        // Flush buffered deltas and signal end-of-stream, then wait on the HUB's
        // confirmation (not a fixed sleep) that the last frame was dispatched.
        egress.end()
        await pollUntil(() => bridge.hubSink.ended, {
          timeoutMs: 10_000,
          message: `flag-ON hub never received the end frame (saw ${sink.types().length} events)`,
        })
      }
    } finally {
      egress?.dispose()
      await runtime.dispose()
      await ctx.dispose()
      if (bridge !== undefined) await bridge.dispose()
    }
  }
}

describe("real SessionPrompt.loop flag-ON ≡ flag-OFF (o1-w4 gap 1)", () => {
  it("the hub event sequence equals the in-process graph sequence, per delivery channel", async () => {
    await using tmp = await tmpdir()
    const result = await runParitySession({
      script: loopScript,
      run: loopRunner(tmp.path),
      timeoutMs: 60_000,
    })

    expect(result.off.error).toBeUndefined()
    expect(result.on.error).toBeUndefined()

    // Both sides on the bridge's projection: session-scoped events only.
    const off = channels(sessionScoped(result.off.events))
    const on = channels(sessionScoped(result.on.events))
    expect(off.bridged.length + off.hubOwned.length).toBeGreaterThan(0)

    // The worker events port preserves order exactly...
    const bridged = compareSequences(off.bridged, on.bridged, { key: (e) => e.type })
    expect(bridged.mismatches).toEqual([])
    expect(bridged.missingInOn).toEqual([])
    expect(bridged.missingInOff).toEqual([])
    expect(bridged.equal).toBe(true)

    // ...and so does the hub-proxy control port (`session.status`/`session.idle`).
    const hubOwned = compareSequences(off.hubOwned, on.hubOwned, { key: (e) => e.type })
    expect(hubOwned.mismatches).toEqual([])
    expect(hubOwned.equal).toBe(true)

    // The two transports are independent, so their INTERLEAVING is not
    // deterministic (e.g. `session.status` and `session.diff` can swap). The full
    // sequence is therefore compared as a multiset, not positionally.
    const offTypes = sessionScoped(result.off.events).map((entry) => entry.type)
    expect(typeCounts(result.on.types)).toEqual(typeCounts(offTypes))
  }, 60_000)

  it("delivers every durable event to the hub exactly once (no writer+republish duplicate)", async () => {
    await using tmp = await tmpdir()
    const result = await runParitySession({
      script: loopScript,
      run: loopRunner(tmp.path),
      timeoutMs: 60_000,
    })

    expect(result.off.error).toBeUndefined()
    expect(result.on.error).toBeUndefined()

    // The hub only ever saw session-scoped events: the bridge's projection is
    // exact — no worker-local telemetry (`plugin.added`, …) leaked onto the hub.
    expect(sessionScoped(result.on.events)).toHaveLength(result.on.events.length)

    // No event id is delivered twice: a durable event that both committed in the
    // worker AND was republished would surface with the same id twice.
    const ids = result.on.events.map((entry) => entry.id)
    expect(new Set(ids).size).toBe(ids.length)

    // The loop created exactly one session; the bridge forwarded it once and the
    // hub never re-committed/duplicated it.
    expect(result.on.types.filter((type) => type === "session.created")).toHaveLength(1)

    // Exactly-once at the hub mirrors the flag-OFF session-scoped multiset
    // one-for-one (so no event is missing AND none is doubled).
    const offTypes = sessionScoped(result.off.events).map((entry) => entry.type)
    expect(typeCounts(result.on.types)).toEqual(typeCounts(offTypes))
  }, 60_000)
})

// ---------------------------------------------------------------------------
// Durable-row capture + flag-ON ≡ flag-OFF (the committed-state twin of the
// event-sequence assertion above).
// ---------------------------------------------------------------------------

describe("durable-row parity", () => {
  const rows: ReadonlyArray<ParityDurableRow> = [
    { table: "message", row: { id: "m1", role: "user", text: "hi" } },
    { table: "message", row: { id: "m2", role: "assistant", text: "hello" } },
    { table: "event", row: { id: "e1", type: "message.updated" } },
  ]

  const durableRunner =
    (transform: (context: ParityRunContext) => ReadonlyArray<ParityDurableRow>): ParityRunner =>
    (context) => {
      for (const entry of transform(context)) context.durable.add(entry.table, [entry.row])
    }

  it("captures durable rows per side and compares equal for identical writes", async () => {
    const result = await runParitySession({ script, run: durableRunner(() => rows) })

    expect(result.off.durable).toHaveLength(rows.length)
    expect(result.on.durable).toHaveLength(rows.length)
    const comparison = compareDurable(result.off, result.on)
    expect(comparison.equal).toBe(true)
    expect(comparison.missingInOn).toEqual([])
    expect(comparison.missingInOff).toEqual([])
  })

  it("reports a duplicate durable write as a divergence (exactly-once)", async () => {
    const result = await runParitySession({
      script,
      run: durableRunner((context) => (context.flag ? [...rows, rows[0]!] : rows)),
    })

    const comparison = compareDurable(result.off, result.on)
    expect(comparison.equal).toBe(false)
    expect(comparison.onCount).toBe(rows.length + 1)
    expect(comparison.missingInOff.map((entry) => entry.table)).toEqual(["message"])
  })

  it("normalizes volatile columns before comparing", () => {
    const off: ReadonlyArray<ParityDurableRow> = [{ table: "message", row: { id: "m1", time: 1, text: "hi" } }]
    const on: ReadonlyArray<ParityDurableRow> = [{ table: "message", row: { id: "m2", time: 99, text: "hi" } }]
    const comparison = compareDurable(off, on, {
      normalize: ({ table, row }) => ({ table, text: row.text }),
    })
    expect(comparison.equal).toBe(true)
  })
})

// ---------------------------------------------------------------------------
// Head-of-line / backpressure: a read queued behind an in-flight durable write
// (a pinned writer transaction) neither deadlocks nor exceeds a bounded
// latency once the transaction commits.
// ---------------------------------------------------------------------------

describe("writer head-of-line and backpressure", () => {
  it("a read queued behind a pinned transaction resolves after commit within a bound", async () => {
    const connection = createBunSqliteConnection({ filename: ":memory:" })
    connection.exec(
      [
        "CREATE TABLE event_sequence (aggregate_id TEXT PRIMARY KEY, seq INTEGER NOT NULL, owner_id TEXT)",
        "CREATE TABLE event (id TEXT PRIMARY KEY, aggregate_id TEXT NOT NULL, seq INTEGER NOT NULL, type TEXT NOT NULL, data TEXT NOT NULL)",
      ].join(";\n"),
    )
    const core = createWriterCore({ connection, sleep: async () => {} })
    const hub: HubWriter = {
      query: (method, sql, params) => core.query(method, sql, params),
      publish: (writerEvent, options) => core.publish({ event: writerEvent, durable: options.durable }),
      begin: () => core.beginTransaction(),
      txQuery: (handle, method, sql, params) => core.txQuery(handle, method, sql, params),
      commit: (handle) => core.commitTransaction(handle),
      rollback: (handle) => core.rollbackTransaction(handle),
    }
    const channel = new MessageChannel()
    const dispose = serveWriterPort(hub, channel.port1)
    const writer = createWorkerWriterClient(channel.port2)
    try {
      // Pin a transaction: every non-control job queues behind it.
      const handle = await writer.begin()
      const startedAt = performance.now()
      let readSettled = false
      const read = writer.query("all", "SELECT COUNT(*) AS n FROM event").then((value) => {
        readSettled = true
        return value
      })

      // The read must NOT resolve while the transaction is pinned (HOL).
      await new Promise((resolve) => setTimeout(resolve, 25))
      expect(readSettled).toBe(false)

      // Commit: the queued read drains promptly and never deadlocks.
      await writer.commit(handle)
      const value = await withDeadline(read, { timeoutMs: 2_000, message: "queued read deadlocked" })
      expect(readSettled).toBe(true)
      expect(Array.isArray(value)).toBe(true)
      expect(performance.now() - startedAt).toBeLessThan(2_000)
    } finally {
      writer.dispose()
      dispose()
      channel.port1.close()
      channel.port2.close()
    }
  })
})

// ---------------------------------------------------------------------------
// Resource ownership: the E2E context owns a writer port, a hub-proxy channel
// and an SQLite handle. Teardown must be idempotent and available to the
// `await using` idiom, so a suite never leaks an isolate handle on the error
// path either.
// ---------------------------------------------------------------------------

describe("E2E context resource ownership", () => {
  it("close is idempotent and releases the writer exactly once", async () => {
    await using tmp = await tmpdir()
    const provider = { model: "mock", baseUrl: "http://127.0.0.1:1", apiKey: "test-key" } as MockProviderHandle
    const ctx = createE2EContext({
      snapshot: workerSnapshot({ directory: tmp.path }),
      modelConfig: modelConfig(provider),
    })
    const writerDispose = spyOn(ctx.writer, "dispose")

    await ctx.close()
    await ctx.close()

    expect(writerDispose).toHaveBeenCalledTimes(1)
  })

  it("is async-disposable so a failing test still tears it down", async () => {
    await using tmp = await tmpdir()
    const provider = { model: "mock", baseUrl: "http://127.0.0.1:1", apiKey: "test-key" } as MockProviderHandle
    const ctx = createE2EContext({
      snapshot: workerSnapshot({ directory: tmp.path }),
      modelConfig: modelConfig(provider),
    })
    const writerDispose = spyOn(ctx.writer, "dispose")

    await ctx[Symbol.asyncDispose]()

    expect(writerDispose).toHaveBeenCalledTimes(1)
  })
})
